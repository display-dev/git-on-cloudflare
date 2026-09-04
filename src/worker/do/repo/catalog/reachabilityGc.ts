import type { Logger } from "@/worker/common/logger";
import type { PackCatalogRow } from "../db/schema";
import type { Ref, RepoLease, RepoStateSchema } from "../repoState";

import { asTypedStorage } from "../repoState";
import {
  getDb,
  getPackCatalogRow,
  listActivePackCatalog,
  listSupersededPackCatalog,
  deletePackCatalogRows,
  replaceActivePackCatalog,
} from "../db";
import { getActivePackCatalogSnapshot } from "./state";
import {
  bumpPacksetVersion,
  clearCompactionSchedule,
  COMPACT_LEASE_TTL_MS,
  ensureRepoMetadataDefaults,
  LEASE_RETRY_AFTER_SECONDS,
} from "./shared";
import { activeLeaseOrUndefined } from "./activity";
import {
  listActiveStockReceiveOperations,
  listActiveStockReceivePreparationLeases,
} from "../nativeReceiveActivity";
import { gcOwnsSource } from "./gcCoordination";
import { rowsMatchForCommit } from "./compaction/plan";
import { EXPIRED_WRITER_DRAIN_MS } from "../repositoryLifecycle";
import {
  GC_OPERATION_KEY,
  GC_WAKE_DELAY_MS,
  type GcOperation,
  type GcCommit,
} from "@/worker/git/maintenance/gcOperation";

export type BeginReachabilityGcResult =
  | {
      ok: true;
      lease: RepoLease;
      refs: Ref[];
      refsVersion: number;
      packsetVersion: number;
      activeCatalog: PackCatalogRow[];
    }
  | {
      ok: false;
      status: "busy";
      retryAfter: number;
      reason: "receive-active" | "compact-active" | "repository-deleting";
    };

export type CommitReachabilityGcResult =
  | {
      status: "committed";
      packCatalogVersion: number;
      supersededPackKeys: string[];
      targetPackKey?: string;
    }
  | {
      status: "retry";
      reason:
        | "lease-mismatch"
        | "receive-active"
        | "refs-changed"
        | "packset-changed"
        | "source-changed"
        | "catalog-replacement-failed"
        | "pending-mismatch"
        | "repository-deleting";
    };

export type RecordReachabilityGcPendingResult =
  | { status: "recorded" }
  | { status: "retry"; reason: "lease-mismatch" | "repository-deleting" };

export type ReconcileReachabilityGcPendingResult =
  | { status: "none" }
  | { status: "committed"; packKey: string }
  | { status: "uncommitted"; token: string; packKey: string }
  | { status: "wait"; retryAfter: number };

export type RemoveSupersededGcPacksResult =
  | { status: "removed"; packKeys: string[] }
  | { status: "retry"; reason: "repository-deleting" | "pack-became-active" };

export type ClaimSupersededGcPacksResult =
  | { status: "claimed"; packKeys: string[] }
  | { status: "retry"; reason: "repository-deleting" | "pack-became-active" };

export async function listSupersededGcPacksState(
  ctx: DurableObjectState,
  cursor?: { seqHi: number; tier: number; packKey: string },
  limit = 250
): Promise<PackCatalogRow[]> {
  return await listSupersededPackCatalog(getDb(ctx.storage), limit, cursor);
}

export async function removeSupersededGcPacksState(args: {
  ctx: DurableObjectState;
  packKeys: string[];
}): Promise<RemoveSupersededGcPacksResult> {
  if (await args.ctx.storage.get<boolean>("repositoryDeleting")) {
    return { status: "retry", reason: "repository-deleting" };
  }
  const db = getDb(args.ctx.storage);
  const removable: string[] = [];
  for (const packKey of args.packKeys) {
    const row = await getPackCatalogRow(db, packKey);
    if (!row) continue;
    if (row.state !== "superseded") {
      return { status: "retry", reason: "pack-became-active" };
    }
    removable.push(packKey);
  }
  await deletePackCatalogRows(db, removable);
  return { status: "removed", packKeys: removable };
}

export async function claimSupersededGcPacksState(args: {
  ctx: DurableObjectState;
  packKeys: string[];
}): Promise<ClaimSupersededGcPacksResult> {
  if (await args.ctx.storage.get<boolean>("repositoryDeleting")) {
    return { status: "retry", reason: "repository-deleting" };
  }
  const db = getDb(args.ctx.storage);
  const claimed: string[] = [];
  for (const packKey of args.packKeys) {
    const row = await getPackCatalogRow(db, packKey);
    if (!row) continue;
    if (row.state !== "superseded") {
      return { status: "retry", reason: "pack-became-active" };
    }
    claimed.push(packKey);
  }
  return { status: "claimed", packKeys: claimed };
}

export async function beginReachabilityGcState(args: {
  ctx: DurableObjectState;
  logger?: Logger;
}): Promise<BeginReachabilityGcResult> {
  const now = Date.now();
  const lease: RepoLease = {
    token: crypto.randomUUID(),
    createdAt: now,
    expiresAt: now + COMPACT_LEASE_TTL_MS,
    operation: "reachability-gc",
  };
  const acquisition = await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return "repository-deleting";
    if (gcOwnsSource(await transaction.get<GcOperation>(GC_OPERATION_KEY))) return "compact-active";
    if (await store.get("reachabilityGcPending")) return "compact-active";
    if (activeLeaseOrUndefined(await store.get("receiveLease"), now)) return "receive-active";
    if ((await listActiveStockReceivePreparationLeases(store, now)).length > 0)
      return "receive-active";
    if ((await listActiveStockReceiveOperations(store)).length > 0) return "receive-active";
    if (activeLeaseOrUndefined(await store.get("compactLease"), now)) return "compact-active";
    await store.put("compactLease", lease);
    return "acquired";
  });
  if (acquisition !== "acquired") {
    return {
      ok: false,
      status: "busy",
      retryAfter: LEASE_RETRY_AFTER_SECONDS,
      reason: acquisition,
    };
  }

  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  await ensureRepoMetadataDefaults(store);
  const activeCatalog = await getActivePackCatalogSnapshot(args.ctx);
  const refs = (await store.get("refs")) ?? [];
  const result = {
    ok: true as const,
    lease,
    refs,
    refsVersion: (await store.get("refsVersion")) || 0,
    packsetVersion: (await store.get("packsetVersion")) || 0,
    activeCatalog,
  };
  args.logger?.info("reachability-gc:begin", {
    refCount: refs.length,
    sourcePackCount: activeCatalog.length,
    refsVersion: result.refsVersion,
    packsetVersion: result.packsetVersion,
  });
  return result;
}

async function clearGcAttemptState(ctx: DurableObjectState, token: string): Promise<void> {
  await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const pending = await store.get("reachabilityGcPending");
    if (pending?.token === token) await store.delete("reachabilityGcPending");
    const lease = await store.get("compactLease");
    if (lease?.token === token) await store.delete("compactLease");
  });
}

export async function recordReachabilityGcPendingState(args: {
  ctx: DurableObjectState;
  token: string;
  packKey: string;
}): Promise<RecordReachabilityGcPendingResult> {
  return await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) {
      return { status: "retry" as const, reason: "repository-deleting" as const };
    }
    const lease = await store.get("compactLease");
    if (
      !lease ||
      lease.operation !== "reachability-gc" ||
      lease.token !== args.token ||
      lease.expiresAt <= Date.now()
    ) {
      return { status: "retry" as const, reason: "lease-mismatch" as const };
    }
    await store.put("reachabilityGcPending", {
      token: args.token,
      packKey: args.packKey,
      state: "staged",
    });
    return { status: "recorded" as const };
  });
}

export async function reconcileReachabilityGcPendingState(args: {
  ctx: DurableObjectState;
}): Promise<ReconcileReachabilityGcPendingResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const pending = await store.get("reachabilityGcPending");
  if (!pending) return { status: "none" };

  const target = await getPackCatalogRow(getDb(args.ctx.storage), pending.packKey);
  if (target) {
    await clearGcAttemptState(args.ctx, pending.token);
    return { status: "committed", packKey: pending.packKey };
  }
  if (pending.state === "committing" && (pending.safeCleanupAt ?? Infinity) > Date.now()) {
    return {
      status: "wait",
      retryAfter: Math.max(
        1,
        Math.ceil(((pending.safeCleanupAt ?? Date.now()) - Date.now()) / 1000)
      ),
    };
  }

  const abandoned = await args.ctx.storage.transaction(async (transaction) => {
    const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
    const current = await transactionStore.get("reachabilityGcPending");
    if (!current || current.token !== pending.token || current.packKey !== pending.packKey) {
      return false;
    }
    if (current.state === "committing" && (current.safeCleanupAt ?? Infinity) > Date.now()) {
      return false;
    }
    await transactionStore.put("reachabilityGcPending", {
      ...current,
      state: "cleanup",
      safeCleanupAt: undefined,
    });
    const lease = await transactionStore.get("compactLease");
    if (lease?.token === current.token) await transactionStore.delete("compactLease");
    return true;
  });
  return abandoned
    ? { status: "uncommitted", token: pending.token, packKey: pending.packKey }
    : { status: "wait", retryAfter: LEASE_RETRY_AFTER_SECONDS };
}

export async function completeReachabilityGcPendingCleanupState(args: {
  ctx: DurableObjectState;
  token: string;
  packKey: string;
}): Promise<boolean> {
  return await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const pending = await store.get("reachabilityGcPending");
    if (
      !pending ||
      pending.state !== "cleanup" ||
      pending.token !== args.token ||
      pending.packKey !== args.packKey
    ) {
      return false;
    }
    await store.delete("reachabilityGcPending");
    return true;
  });
}

export async function commitReachabilityGcState(args: {
  ctx: DurableObjectState;
  token: string;
  refsVersion: number;
  packsetVersion: number;
  sourcePacks: PackCatalogRow[];
  retainedPackKey?: string;
  stagedPack?: {
    packKey: string;
    packBytes: number;
    idxBytes: number;
    objectCount: number;
  };
  logger?: Logger;
  gcOperationId?: string;
  gcClaimId?: string;
}): Promise<CommitReachabilityGcResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const db = getDb(args.ctx.storage);
  const retainedSource = args.retainedPackKey
    ? args.sourcePacks.find((row) => row.packKey === args.retainedPackKey)
    : undefined;
  if (args.retainedPackKey && (!retainedSource || args.stagedPack)) {
    return { status: "retry", reason: "source-changed" };
  }
  const sourcePackKeys = args.sourcePacks
    .map((row) => row.packKey)
    .filter((key) => key !== args.retainedPackKey);

  // The SQL catalog is the durable outcome record. A Worker may lose the RPC
  // response after the atomic catalog replacement but before the lease and KV
  // version cleanup completes. Repeating the exact operation recognizes that
  // committed state and finishes the remaining bookkeeping without ever
  // telling the Worker to delete the active staged pack.
  const currentSourceRows: PackCatalogRow[] = [];
  for (const sourcePack of args.sourcePacks) {
    const row = await getPackCatalogRow(db, sourcePack.packKey);
    if (row) currentSourceRows.push(row);
  }
  const expectedTargetKey = args.stagedPack?.packKey ?? args.retainedPackKey ?? null;
  const targetRow = expectedTargetKey ? await getPackCatalogRow(db, expectedTargetKey) : undefined;
  const catalogAlreadyCommitted =
    currentSourceRows.length === args.sourcePacks.length &&
    // With no superseded sources, the active target alone cannot prove a
    // previous commit. Let the normal lease/version fence handle that case.
    sourcePackKeys.length > 0 &&
    currentSourceRows.every((row) =>
      row.packKey === args.retainedPackKey
        ? row.state === "active" && rowsMatchForCommit([retainedSource!], [row])
        : row.state === "superseded" && row.supersededBy === expectedTargetKey
    ) &&
    (expectedTargetKey ? targetRow !== undefined : true);
  if (catalogAlreadyCommitted) {
    const currentVersion = (await store.get("packsetVersion")) || 0;
    const packCatalogVersion =
      currentVersion > args.packsetVersion ? currentVersion : await bumpPacksetVersion(store);
    await store.put("generationPublicationPending", {
      generation: packCatalogVersion,
      activePackKeys: (await listActivePackCatalog(db)).map((row) => row.packKey),
    });
    // The original GC owns only the next catalog version. If receives or a
    // later maintenance operation advanced it again while the Worker retried
    // a lost response, their compaction request is newer authority and must
    // survive reconciliation.
    if (currentVersion <= args.packsetVersion + 1) {
      await clearCompactionSchedule(store);
    }
    const committed: GcCommit = {
      status: "committed",
      packCatalogVersion,
      supersededPackKeys: sourcePackKeys,
      targetPackKey: expectedTargetKey ?? undefined,
    };
    await retainGcCommitReceipt(args.ctx, args.gcOperationId, args.token, committed);
    await clearGcAttemptState(args.ctx, args.token);
    args.logger?.info("reachability-gc:commit-reconciled", {
      sourcePackCount: args.sourcePacks.length,
      targetPackKey: expectedTargetKey ?? undefined,
      packCatalogVersion,
    });
    return committed;
  }

  const fence = await args.ctx.storage.transaction(async (transaction) => {
    const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
    if (await transactionStore.get("repositoryDeleting")) return "repository-deleting";
    const now = Date.now();
    if (args.gcOperationId) {
      const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
      if (
        operation?.id !== args.gcOperationId ||
        operation.phase !== "publish" ||
        !operation.claim ||
        operation.claim.id !== args.gcClaimId ||
        operation.claim.expiresAt <= now
      )
        return "lease-mismatch";
      // Reconciliation above already recognizes a committed replacement. If
      // another writer advanced an uncommitted source after our lease expired,
      // report the conclusive conflict rather than retrying a missing lease.
      if (((await transactionStore.get("refsVersion")) ?? 0) !== args.refsVersion)
        return "refs-changed";
      if (((await transactionStore.get("packsetVersion")) ?? 0) !== args.packsetVersion)
        return "packset-changed";
    }
    const lease = await transactionStore.get("compactLease");
    if (
      !lease ||
      lease.operation !== "reachability-gc" ||
      lease.token !== args.token ||
      lease.expiresAt <= now
    ) {
      if (lease?.token === args.token) await transactionStore.delete("compactLease");
      return "lease-mismatch";
    }
    if (args.stagedPack) {
      const pending = await transactionStore.get("reachabilityGcPending");
      if (!pending || pending.token !== args.token || pending.packKey !== args.stagedPack.packKey) {
        return "pending-mismatch";
      }
      await transactionStore.put("reachabilityGcPending", {
        ...pending,
        state: "committing",
        safeCleanupAt: now + COMPACT_LEASE_TTL_MS + EXPIRED_WRITER_DRAIN_MS,
      });
    }
    await transactionStore.put("compactLease", {
      ...lease,
      expiresAt: now + COMPACT_LEASE_TTL_MS,
    });
    return "held";
  });
  if (fence !== "held") {
    return { status: "retry", reason: fence };
  }

  if (activeLeaseOrUndefined(await store.get("receiveLease"), Date.now())) {
    await clearGcAttemptState(args.ctx, args.token);
    return { status: "retry", reason: "receive-active" };
  }
  if ((await listActiveStockReceivePreparationLeases(store)).length > 0) {
    await clearGcAttemptState(args.ctx, args.token);
    return { status: "retry", reason: "receive-active" };
  }
  if (((await store.get("refsVersion")) || 0) !== args.refsVersion) {
    await clearGcAttemptState(args.ctx, args.token);
    return { status: "retry", reason: "refs-changed" };
  }
  if (((await store.get("packsetVersion")) || 0) !== args.packsetVersion) {
    await clearGcAttemptState(args.ctx, args.token);
    return { status: "retry", reason: "packset-changed" };
  }

  const currentRows: PackCatalogRow[] = [];
  for (const sourcePack of args.sourcePacks) {
    const row = await getPackCatalogRow(db, sourcePack.packKey);
    if (row) currentRows.push(row);
  }
  if (!rowsMatchForCommit(args.sourcePacks, currentRows)) {
    await clearGcAttemptState(args.ctx, args.token);
    return { status: "retry", reason: "source-changed" };
  }

  let targetPack: PackCatalogRow | undefined = retainedSource;
  if (sourcePackKeys.length === 0 && !args.stagedPack) {
    // An already exact catalog is a physical no-op. In particular, do not
    // advance a version that would make replay of this no-op look stale.
    const committed: GcCommit = {
      status: "committed",
      packCatalogVersion: args.packsetVersion,
      supersededPackKeys: [],
      targetPackKey: retainedSource?.packKey,
    };
    await retainGcCommitReceipt(args.ctx, args.gcOperationId, args.token, committed);
    await clearGcAttemptState(args.ctx, args.token);
    return committed;
  }
  if (args.stagedPack) {
    let seqLo = args.sourcePacks[0]?.seqLo ?? 0;
    let seqHi = args.sourcePacks[0]?.seqHi ?? 0;
    let targetTier = 1;
    for (const sourcePack of args.sourcePacks) {
      if (sourcePack.seqLo < seqLo) seqLo = sourcePack.seqLo;
      if (sourcePack.seqHi > seqHi) seqHi = sourcePack.seqHi;
      if (sourcePack.tier >= targetTier) targetTier = sourcePack.tier + 1;
    }
    targetPack = {
      packKey: args.stagedPack.packKey,
      kind: "compact",
      state: "active",
      tier: targetTier,
      seqLo,
      seqHi,
      objectCount: args.stagedPack.objectCount,
      packBytes: args.stagedPack.packBytes,
      idxBytes: args.stagedPack.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    };
  }

  try {
    replaceActivePackCatalog({ db, sourcePackKeys, targetPack });
  } catch (error) {
    args.logger?.error("reachability-gc:catalog-replacement-failed", {
      error: String(error),
      sourcePackCount: sourcePackKeys.length,
      targetPackKey: targetPack?.packKey,
    });
    await clearGcAttemptState(args.ctx, args.token);
    return { status: "retry", reason: "catalog-replacement-failed" };
  }
  const committedActiveCatalog = await listActivePackCatalog(db);
  if (committedActiveCatalog.length !== (targetPack ? 1 : 0)) {
    throw new Error("reachability GC catalog commit produced an invalid active pack count");
  }
  const packCatalogVersion = await bumpPacksetVersion(store);
  await store.put("generationPublicationPending", {
    generation: packCatalogVersion,
    activePackKeys: committedActiveCatalog.map((row) => row.packKey),
  });
  await clearCompactionSchedule(store);
  const committed: GcCommit = {
    status: "committed",
    packCatalogVersion,
    supersededPackKeys: sourcePackKeys,
    targetPackKey: targetPack?.packKey,
  };
  await retainGcCommitReceipt(args.ctx, args.gcOperationId, args.token, committed);
  await clearGcAttemptState(args.ctx, args.token);
  args.logger?.info("reachability-gc:commit", {
    sourcePackCount: args.sourcePacks.length,
    reachableObjectCount: targetPack?.objectCount ?? 0,
    targetPackKey: targetPack?.packKey,
    packCatalogVersion,
  });
  return committed;
}

async function retainGcCommitReceipt(
  ctx: DurableObjectState,
  operationId: string | undefined,
  token: string,
  committed: GcCommit
): Promise<void> {
  if (!operationId) return;
  // Do this before releasing the source lease or pending-output ownership.
  // A crash before this write retains the existing catalog reconciliation
  // evidence; a crash after it has an independent durable outcome receipt.
  await ctx.storage.transaction(async (transaction) => {
    const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    if (!operation || operation.id !== operationId || operation.snapshot?.token !== token)
      throw new Error("GC committed without its registered owner");
    if (operation.commit) return;
    operation.commit = committed;
    operation.phase = "reclaim";
    operation.updatedAt = Date.now();
    const measurement = operation.measurements.publish;
    if (measurement)
      operation.measurements.publish = {
        ...measurement,
        completedAt: operation.updatedAt,
        elapsedMs: operation.updatedAt - measurement.startedAt,
      };
    delete operation.claim;
    await transaction.put(GC_OPERATION_KEY, operation);
    const alarm = await transaction.getAlarm();
    if (alarm === null || alarm > Date.now() + GC_WAKE_DELAY_MS)
      await transaction.setAlarm(Date.now() + GC_WAKE_DELAY_MS);
  });
}
