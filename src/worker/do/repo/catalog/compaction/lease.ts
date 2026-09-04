/**
 * Queue-facing compaction state transitions: begin, commit, abort, and alarm rearm.
 *
 * These operations manage the compaction lease lifecycle. The queue consumer
 * acquires a lease via `beginCompactionState`, performs the pack rewrite in
 * worker code, and then atomically commits the result via `commitCompactionState`.
 */
import type { Logger } from "@/worker/common/logger";
import { GC_OPERATION_KEY, type GcOperation } from "@/worker/git/maintenance/gcOperation";
import { gcOwnsMaintenance } from "../gcCoordination";
import type { PackCatalogRow } from "../../db/schema";
import type { RepoLease, RepoStateSchema } from "../../repoState";

import { asTypedStorage } from "../../repoState";
import {
  getDb,
  getPackCatalogRow,
  listActivePackCatalog,
  supersedePackCatalogRows,
  upsertPackCatalogRow,
} from "../../db";
import { getActivePackCatalogSnapshot } from "../state";
import {
  bumpPacksetVersion,
  clearCompactionSchedule,
  compactionStartAt,
  COMPACT_LEASE_TTL_MS,
  COMPACTION_REARM_DELAY_MS,
  ensureRepoMetadataDefaults,
  LEASE_RETRY_AFTER_SECONDS,
  markCompactionActivity,
} from "../shared";
import { activeLeaseOrUndefined } from "../activity";
import {
  listActiveStockReceiveOperations,
  listActiveStockReceivePreparationLeases,
} from "../../nativeReceiveActivity";
import { scheduleAlarmIfSooner } from "../../scheduler";
import {
  selectCompactionPlan,
  catalogNeedsCompaction,
  scheduleCompactionWake,
  scheduleCompactionAlarm,
  rowsMatchForCommit,
  type CompactionBusyReason,
  type BeginCompactionResult,
  type CommitCompactionResult,
} from "./plan";

type CompactionLeaseAcquisition =
  | { status: "acquired" }
  | {
      status: "busy";
      reason: CompactionBusyReason;
      retryAfter: number;
    }
  | { status: "no-work" };

/**
 * Acquire a compaction lease and select source packs for compaction.
 *
 * Rejects when: no compaction request is recorded, repository activity is
 * inside its bounded quiet window, a receive or compaction lease is already
 * active, or the active catalog is already within the compaction policy.
 */
export async function beginCompactionState(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  logger?: Logger;
}): Promise<BeginCompactionResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  if (await store.get("repositoryDeleting")) {
    return {
      ok: false,
      status: "busy",
      retryAfter: LEASE_RETRY_AFTER_SECONDS,
      reason: "repository-deleting",
      message: "Repository deletion has started, so compaction cannot begin.",
    };
  }
  if (gcOwnsMaintenance(await args.ctx.storage.get<GcOperation>(GC_OPERATION_KEY))) {
    return {
      ok: false,
      status: "busy",
      retryAfter: LEASE_RETRY_AFTER_SECONDS,
      reason: "compact-active",
      message: "GC still owns its source catalog; compaction remains queued.",
    };
  }
  const observedWantedAt = await store.get("compactionWantedAt");
  if (typeof observedWantedAt !== "number") {
    return {
      ok: false,
      status: "no_work",
      reason: "not-requested",
      message: "No compaction request is currently recorded for this repository.",
    };
  }

  const now = Date.now();
  const activeCatalog = await getActivePackCatalogSnapshot(args.ctx);
  const plan = selectCompactionPlan(activeCatalog);
  if (!plan) {
    await args.ctx.storage.transaction(async (transaction) => {
      const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
      if (
        !(await transactionStore.get("repositoryDeleting")) &&
        (await transactionStore.get("compactionWantedAt")) === observedWantedAt
      ) {
        await clearCompactionSchedule(transactionStore);
      }
    });
    args.logger?.info("compaction:begin-no-work", {
      reason: "below-threshold",
    });
    return {
      ok: false,
      status: "no_work",
      reason: "below-threshold",
      message: "The active pack catalog is already within the compaction policy.",
    };
  }

  const lease: RepoLease = {
    token: crypto.randomUUID(),
    createdAt: now,
    expiresAt: now + COMPACT_LEASE_TTL_MS,
    operation: "compaction",
  };
  const acquisition: CompactionLeaseAcquisition = await args.ctx.storage.transaction(
    async (transaction) => {
      const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
      if (await transactionStore.get("repositoryDeleting")) {
        return {
          status: "busy",
          reason: "repository-deleting",
          retryAfter: LEASE_RETRY_AFTER_SECONDS,
        };
      }
      if (gcOwnsMaintenance(await transaction.get<GcOperation>(GC_OPERATION_KEY)))
        return {
          status: "busy",
          reason: "compact-active",
          retryAfter: LEASE_RETRY_AFTER_SECONDS,
        };
      if (activeLeaseOrUndefined(await transactionStore.get("receiveLease"), now)) {
        return {
          status: "busy",
          reason: "receive-active",
          retryAfter: LEASE_RETRY_AFTER_SECONDS,
        };
      }
      if ((await listActiveStockReceivePreparationLeases(transactionStore, now)).length > 0) {
        return {
          status: "busy",
          reason: "receive-active",
          retryAfter: LEASE_RETRY_AFTER_SECONDS,
        };
      }
      if ((await listActiveStockReceiveOperations(transactionStore)).length > 0) {
        return {
          status: "busy",
          reason: "receive-active",
          retryAfter: LEASE_RETRY_AFTER_SECONDS,
        };
      }
      if (activeLeaseOrUndefined(await transactionStore.get("compactLease"), now)) {
        return {
          status: "busy",
          reason: "compact-active",
          retryAfter: LEASE_RETRY_AFTER_SECONDS,
        };
      }

      // Re-read activity in the same transaction that publishes the lease. A
      // receive that commits first moves the deadline. A receive that publishes
      // later changes the packset version, so the compaction commit fence retries.
      const wantedAt = await transactionStore.get("compactionWantedAt");
      if (typeof wantedAt !== "number") return { status: "no-work" };
      const pendingSince = (await transactionStore.get("compactionPendingSince")) ?? wantedAt;
      const startAt = compactionStartAt(wantedAt, pendingSince);
      if (now < startAt) {
        return {
          status: "busy",
          reason: "recent-activity",
          retryAfter: Math.max(1, Math.ceil((startAt - now) / 1000)),
        };
      }
      await transactionStore.put("compactLease", lease);
      return { status: "acquired" };
    }
  );
  if (acquisition.status === "no-work") {
    return {
      ok: false,
      status: "no_work",
      reason: "not-requested",
      message: "No compaction request is currently recorded for this repository.",
    };
  }
  if (acquisition.status === "busy") {
    return {
      ok: false,
      status: "busy",
      retryAfter: acquisition.retryAfter,
      reason: acquisition.reason,
      message:
        acquisition.reason === "repository-deleting"
          ? "Repository deletion has started, so compaction cannot begin."
          : acquisition.reason === "receive-active"
            ? "A receive lease is active, so compaction must retry later."
            : acquisition.reason === "recent-activity"
              ? "Repository activity is still inside the bounded compaction quiet period."
              : "A compaction lease is already active for this repository.",
    };
  }

  args.logger?.info("compaction:begin", {
    leaseToken: lease.token,
    sourceTier: plan.sourceTier,
    targetTier: plan.targetTier,
    sourceCount: plan.sourcePacks.length,
  });
  return {
    ok: true,
    lease,
    packsetVersion: (await store.get("packsetVersion")) || 0,
    activeCatalog,
    sourcePacks: plan.sourcePacks,
    targetTier: plan.targetTier,
  };
}

/**
 * Atomically commit a compaction result: insert the new pack, supersede source
 * packs, bump the packset version, and mirror legacy keys.
 *
 * Rejects with `status: "retry"` when the lease is stale, a receive lease
 * appeared, the packset version changed, or source packs were modified since
 * `beginCompactionState`.
 */
export async function commitCompactionState(args: {
  ctx: DurableObjectState;
  env: Env;
  token: string;
  sourcePacks: PackCatalogRow[];
  targetTier: number;
  packsetVersion: number;
  stagedPack: {
    packKey: string;
    packBytes: number;
    idxBytes: number;
    objectCount: number;
  };
  logger?: Logger;
}): Promise<CommitCompactionResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  await ensureRepoMetadataDefaults(store);
  const fence = await args.ctx.storage.transaction(async (transaction) => {
    const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
    if (await transactionStore.get("repositoryDeleting")) return "repository-deleting";
    if (gcOwnsMaintenance(await transaction.get<GcOperation>(GC_OPERATION_KEY)))
      return "lease-mismatch";
    const now = Date.now();
    const lease = await transactionStore.get("compactLease");
    if (!lease || lease.token !== args.token || lease.expiresAt <= now) {
      if (lease?.token === args.token) await transactionStore.delete("compactLease");
      return "lease-mismatch";
    }
    await transactionStore.put("compactLease", {
      ...lease,
      expiresAt: now + COMPACT_LEASE_TTL_MS,
    });
    return "held";
  });
  if (fence === "repository-deleting") {
    return {
      status: "retry",
      reason: "repository-deleting",
      message: "Repository deletion started before compaction could commit.",
    };
  }
  if (fence === "lease-mismatch") {
    return {
      status: "retry",
      reason: "lease-mismatch",
      message: "Compaction lease is no longer active for this request.",
    };
  }

  const receiveLease = activeLeaseOrUndefined(await store.get("receiveLease"), Date.now());
  if (receiveLease) {
    await store.delete("compactLease");
    return {
      status: "retry",
      reason: "receive-active",
      message: "A receive lease became active before compaction could commit.",
    };
  }
  if ((await listActiveStockReceivePreparationLeases(store)).length > 0) {
    await store.delete("compactLease");
    return {
      status: "retry",
      reason: "receive-active",
      message: "Stock receive input staging became active before compaction could commit.",
    };
  }
  if ((await listActiveStockReceiveOperations(store)).length > 0) {
    await store.delete("compactLease");
    return {
      status: "retry",
      reason: "receive-active",
      message: "Stock receive preparation became active before compaction could commit.",
    };
  }

  const currentPacksetVersion = (await store.get("packsetVersion")) || 0;
  if (currentPacksetVersion !== args.packsetVersion) {
    await store.delete("compactLease");
    return {
      status: "retry",
      reason: "packset-changed",
      message: "The active pack catalog changed before compaction could commit.",
    };
  }

  const db = getDb(args.ctx.storage);
  const currentRows: PackCatalogRow[] = [];
  for (const sourcePack of args.sourcePacks) {
    const row = await getPackCatalogRow(db, sourcePack.packKey);
    if (row) currentRows.push(row);
  }
  if (!rowsMatchForCommit(args.sourcePacks, currentRows)) {
    await store.delete("compactLease");
    return {
      status: "retry",
      reason: "source-changed",
      message: "One or more source packs changed before compaction could commit.",
    };
  }

  let seqLo = args.sourcePacks[0]!.seqLo;
  let seqHi = args.sourcePacks[0]!.seqHi;
  for (const sourcePack of args.sourcePacks) {
    if (sourcePack.seqLo < seqLo) seqLo = sourcePack.seqLo;
    if (sourcePack.seqHi > seqHi) seqHi = sourcePack.seqHi;
  }

  await upsertPackCatalogRow(db, {
    packKey: args.stagedPack.packKey,
    kind: "compact",
    state: "active",
    tier: args.targetTier,
    seqLo,
    seqHi,
    objectCount: args.stagedPack.objectCount,
    packBytes: args.stagedPack.packBytes,
    idxBytes: args.stagedPack.idxBytes,
    createdAt: Date.now(),
    supersededBy: null,
  });
  await supersedePackCatalogRows(
    db,
    args.sourcePacks.map((row) => row.packKey),
    args.stagedPack.packKey
  );

  const activeCatalog = await listActivePackCatalog(db);
  const nextPackCatalogVersion = await bumpPacksetVersion(store);
  await store.put("generationPublicationPending", {
    generation: nextPackCatalogVersion,
    activePackKeys: activeCatalog.map((row) => row.packKey),
  });

  const shouldRequeue = catalogNeedsCompaction(activeCatalog);
  if (shouldRequeue) {
    // Preserve both timestamps across internal passes. If an operator cleared
    // the request mid-pass, record a fresh schedule for the remaining work.
    const wantedAt = await store.get("compactionWantedAt");
    if (typeof wantedAt !== "number") {
      await markCompactionActivity(store, Date.now());
    } else if (typeof (await store.get("compactionPendingSince")) !== "number") {
      // Backfill repositories whose request predates the bounded-window key.
      await store.put("compactionPendingSince", wantedAt);
    }
    await scheduleCompactionWake(args.ctx, args.env);
  } else {
    await clearCompactionSchedule(store);
  }

  await store.delete("compactLease");
  args.logger?.info("compaction:commit", {
    targetPackKey: args.stagedPack.packKey,
    supersededCount: args.sourcePacks.length,
    shouldRequeue,
    packCatalogVersion: nextPackCatalogVersion,
  });
  return {
    status: "committed",
    packCatalogVersion: nextPackCatalogVersion,
    shouldRequeue,
    supersededPackKeys: args.sourcePacks.map((row) => row.packKey),
    targetPackKey: args.stagedPack.packKey,
  };
}

/**
 * Called from the DO alarm handler for streaming repos. A request inside its
 * activity window re-arms the exact eligibility deadline without enqueueing.
 * Once eligible and no lease is active, the alarm enqueues maintenance work;
 * a queue-send failure re-arms the bounded recovery cadence.
 */
export async function rearmCompactionQueueFromAlarm(args: {
  ctx: DurableObjectState;
  env: Env;
  logger?: Logger;
}): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  await ensureRepoMetadataDefaults(store);

  const wantedAt = await store.get("compactionWantedAt");
  if (typeof wantedAt !== "number") return false;

  const now = Date.now();
  const pendingSince = (await store.get("compactionPendingSince")) ?? wantedAt;
  const startAt = compactionStartAt(wantedAt, pendingSince);
  if (startAt > now) {
    await scheduleAlarmIfSooner(args.ctx, args.env, startAt, now);
    args.logger?.info("compaction:alarm-rearm-deferred", {
      startAt,
      retryAfter: Math.max(1, Math.ceil((startAt - now) / 1000)),
    });
    return true;
  }

  if (gcOwnsMaintenance(await args.ctx.storage.get<GcOperation>(GC_OPERATION_KEY))) return false;
  if (activeLeaseOrUndefined(await store.get("receiveLease"), now)) return false;
  if ((await listActiveStockReceivePreparationLeases(store, now)).length > 0) return false;
  if (activeLeaseOrUndefined(await store.get("compactLease"), now)) return false;

  const doId = args.ctx.id.toString();
  try {
    await args.env.REPO_TASKS_QUEUE.send({
      kind: "compaction",
      doId,
    });
    args.logger?.info("compaction:alarm-rearm-enqueued", { doId });
    return true;
  } catch (error) {
    args.logger?.warn("compaction:alarm-rearm-failed", {
      doId,
      error: String(error),
    });
    await scheduleCompactionAlarm(args.ctx, args.env, COMPACTION_REARM_DELAY_MS);
    return true;
  }
}
