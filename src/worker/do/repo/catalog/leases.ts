import type { Logger } from "@/worker/common/logger";
import { GC_OPERATION_KEY, type GcOperation } from "@/worker/git/maintenance/gcOperation";
import { gcOwnsSource } from "./gcCoordination";
import { helpPreparedGcPublication } from "./gcPublication";
import {
  isNativeReceiveTerminal,
  type NativeReceiveOperation,
} from "@/worker/git/nativeReceive/types";
import { MAX_SIMULTANEOUS_CONNECTIONS, SubrequestLimiter } from "@/worker/git/operations/limits";
import { doPrefix, nativeReceiveInputPackKey, nativeReceiveInputRequestKey } from "@/worker/keys";

import { asTypedStorage, nativeReceiveOperationKey, receiveFinalizeIntentKey } from "../repoState";
import type { RepoLease, RepoStateSchema, StockReceiveRecoveryLease } from "../repoState";
import { scheduleAlarmIfSooner } from "../scheduler";
import { getActivePackCatalogSnapshot } from "./state";
import { activeLeaseOrUndefined } from "./activity";
import type { BeginReceiveResult, BeginStockReceiveRecoveryResult } from "./shared";
import {
  activeStockReceivePreparationLeases,
  listActiveNativeReceiveOperations,
  listActiveStockReceivePreparationLeases,
  removeStockReceivePreparationLease,
} from "../nativeReceiveActivity";
import { EXPIRED_WRITER_DRAIN_MS } from "../repositoryLifecycle";
import {
  DEFAULT_HEAD,
  LEASE_RETRY_AFTER_SECONDS,
  ensureRepoMetadataDefaults,
  RECEIVE_LEASE_TTL_MS,
} from "./shared";

export async function clearExpiredLeases(
  ctx: DurableObjectState,
  env: Env,
  logger?: Logger,
  now: number = Date.now()
): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  let writerCleanupPending = false;
  const preparationLeases = await store.get("stockReceivePreparationLeases");
  const expiredPreparationLeases = (preparationLeases ?? []).filter(
    (lease) => lease.expiresAt + EXPIRED_WRITER_DRAIN_MS <= now
  );
  const drainingPreparationLeases = (preparationLeases ?? []).filter(
    (lease) => lease.expiresAt <= now && lease.expiresAt + EXPIRED_WRITER_DRAIN_MS > now
  );
  if (drainingPreparationLeases.length > 0) {
    writerCleanupPending = true;
    await scheduleAlarmIfSooner(
      ctx,
      env,
      Math.min(
        ...drainingPreparationLeases.map((lease) => lease.expiresAt + EXPIRED_WRITER_DRAIN_MS)
      ),
      now
    );
  }
  if (expiredPreparationLeases.length > 0) {
    const prefix = doPrefix(ctx.id.toString());
    try {
      const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
      await limiter.run("r2:delete-expired-stock-preparation-inputs", () =>
        env.REPO_BUCKET.delete(
          expiredPreparationLeases.flatMap((lease) => [
            nativeReceiveInputPackKey(prefix, lease.token),
            nativeReceiveInputRequestKey(prefix, lease.token),
          ])
        )
      );
      const expiredTokens = new Set(expiredPreparationLeases.map((lease) => lease.token));
      await ctx.storage.transaction(async (transaction) => {
        const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
        const current = await transactionStore.get("stockReceivePreparationLeases");
        const retained = (current ?? []).filter(
          (lease) => lease.expiresAt > now || !expiredTokens.has(lease.token)
        );
        if (retained.length > 0) {
          await transactionStore.put("stockReceivePreparationLeases", retained);
        } else {
          await transactionStore.delete("stockReceivePreparationLeases");
        }
      });
      logger?.info("stock-receive:expired-preparations-cleaned", {
        count: expiredPreparationLeases.length,
      });
    } catch {
      writerCleanupPending = true;
      logger?.warn("stock-receive:expired-preparation-cleanup-failed", {
        count: expiredPreparationLeases.length,
        retryable: true,
      });
      await scheduleAlarmIfSooner(ctx, env, now + 1_000, now);
    }
  }
  const receiveLease = await store.get("receiveLease");
  if (receiveLease && receiveLease.expiresAt <= now) {
    const intent = await store.get(receiveFinalizeIntentKey(receiveLease.token));
    if (intent) {
      await store.put("receiveLease", {
        ...receiveLease,
        expiresAt: now + RECEIVE_LEASE_TTL_MS,
      });
      logger?.debug("lease:recovery-pending", { kind: "receive" });
    } else {
      const nativeOperationIds = (await store.get("nativeReceiveOperationIndex")) ?? [];
      let nativeOperation: NativeReceiveOperation | undefined;
      for (const operationId of nativeOperationIds) {
        const candidate = await store.get(nativeReceiveOperationKey(operationId));
        if (
          candidate?.leaseToken === receiveLease.token &&
          !isNativeReceiveTerminal(candidate.state)
        ) {
          nativeOperation = candidate;
          break;
        }
      }
      if (nativeOperation) {
        await store.put("receiveLease", {
          ...receiveLease,
          expiresAt: now + RECEIVE_LEASE_TTL_MS,
        });
        logger?.debug("lease:native-recovery-pending", { kind: "receive" });
        await scheduleAlarmIfSooner(ctx, env, now + 1_000, now);
      } else {
        const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
        try {
          await limiter.run("r2:delete-orphan-native-receive-input", async () => {
            const prefix = doPrefix(ctx.id.toString());
            await env.REPO_BUCKET.delete([
              nativeReceiveInputPackKey(prefix, receiveLease.token),
              nativeReceiveInputRequestKey(prefix, receiveLease.token),
            ]);
          });
          logger?.info("lease:orphan-input-cleaned", { kind: "receive" });
        } catch {
          await store.put("receiveLease", {
            ...receiveLease,
            expiresAt: now + RECEIVE_LEASE_TTL_MS,
          });
          logger?.warn("lease:orphan-input-cleanup-failed", {
            kind: "receive",
            retryable: true,
          });
          await scheduleAlarmIfSooner(ctx, env, now + 1_000, now);
          return true;
        }
        await store.delete("receiveLease");
        logger?.debug("lease:expired", { kind: "receive" });
      }
    }
  }

  const compactLease = await store.get("compactLease");
  const stockPublicationLease = await store.get("stockReceivePublicationLease");
  if (stockPublicationLease && stockPublicationLease.expiresAt <= now) {
    await store.delete("stockReceivePublicationLease");
  }
  const gc = await ctx.storage.get<GcOperation>(GC_OPERATION_KEY);
  if (
    compactLease &&
    compactLease.expiresAt <= now &&
    !(gcOwnsSource(gc) && gc?.snapshot?.token === compactLease.token)
  ) {
    await store.delete("compactLease");
    logger?.debug("lease:expired", { kind: "compact" });
  }
  return writerCleanupPending;
}

let beforeAdmissionForTesting: ((ctx: DurableObjectState) => Promise<void>) | undefined;
let afterStockPreparationHandoffForTesting:
  | ((ctx: DurableObjectState) => Promise<void>)
  | undefined;
export const __admissionTest = {
  beforeAdmissionOnce(callback: (ctx: DurableObjectState) => Promise<void>) {
    beforeAdmissionForTesting = callback;
  },
  afterStockPreparationHandoffOnce(callback: (ctx: DurableObjectState) => Promise<void>) {
    afterStockPreparationHandoffForTesting = callback;
  },
  reset() {
    beforeAdmissionForTesting = undefined;
    afterStockPreparationHandoffForTesting = undefined;
  },
};

export async function beginReceiveLease(
  ctx: DurableObjectState,
  logger?: Logger,
  options?: { stockPreparationConcurrency?: number | undefined }
): Promise<BeginReceiveResult> {
  const now = Date.now();
  const lease: RepoLease = {
    token: crypto.randomUUID(),
    createdAt: now,
    expiresAt: now + RECEIVE_LEASE_TTL_MS,
    operation: "receive",
  };
  let helpedClaimId: string | undefined;
  const acquire = () =>
    ctx.storage.transaction(async (transaction) => {
      const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
      if (await transactionStore.get("repositoryDeleting")) return false;
      const activeOperations = await listActiveNativeReceiveOperations(transactionStore);
      const activePreparations = await listActiveStockReceivePreparationLeases(
        transactionStore,
        now
      );
      const concurrency = options?.stockPreparationConcurrency;
      const activeCount = activeOperations.length + activePreparations.length;
      if (
        activeCount > 0 &&
        (concurrency === undefined ||
          activeCount >= concurrency ||
          activeOperations.some(
            (operation) => !operation.stockReceive || operation.state !== "processing"
          ))
      ) {
        return {
          status: "busy" as const,
          reason:
            concurrency !== undefined && activeCount >= concurrency
              ? ("stock-preparation-capacity" as const)
              : ("native-operation-active" as const),
          activeCount,
          limit: concurrency,
        };
      }
      const existing = await transactionStore.get("receiveLease");
      if (existing && existing.expiresAt > now) return false;
      if (existing && (await transactionStore.get(receiveFinalizeIntentKey(existing.token)))) {
        await transactionStore.put("receiveLease", {
          ...existing,
          expiresAt: now + RECEIVE_LEASE_TTL_MS,
        });
        return false;
      }
      const compactLease = activeLeaseOrUndefined(await transactionStore.get("compactLease"), now);
      const gc = await transaction.get<GcOperation>(GC_OPERATION_KEY);
      // Generic receives retain their existing priority and force compaction
      // to retry at commit. A stock receive entering the parallel pool releases
      // this snapshot lease, so it must not do so after compaction owns the fence.
      if (
        compactLease?.operation === "compaction" &&
        options?.stockPreparationConcurrency !== undefined
      )
        return false;
      if (
        compactLease?.operation === "reachability-gc" &&
        !(gc?.coordination && gc.snapshot?.token === compactLease.token)
      )
        return false;
      // Decide whether publication gets a turn in the same transaction that
      // would grant this lease. An earlier helper check can race the preceding
      // receive's completion and allow a sustained writer stream to bypass GC.
      const preparedClaim = gc?.coordination?.publicationClaimId;
      if (
        gc?.phase === "publish" &&
        preparedClaim &&
        gc.claim?.id === preparedClaim &&
        gc.claim.expiresAt > now &&
        preparedClaim !== helpedClaimId
      )
        return preparedClaim;
      if (existing) logger?.debug("lease:expired", { kind: "receive" });
      await transactionStore.put("receiveLease", lease);
      return true;
    });
  const beforeAdmission = beforeAdmissionForTesting;
  beforeAdmissionForTesting = undefined;
  await beforeAdmission?.(ctx);
  let acquired = await acquire();
  while (typeof acquired === "string") {
    helpedClaimId = acquired;
    await helpPreparedGcPublication(ctx);
    // One metadata attempt per prepared claim: an invalid GC source or a
    // transaction failure must not create indefinite foreground exclusion.
    acquired = await acquire();
  }
  if (typeof acquired === "object") {
    logger?.debug("receive:admission-busy", {
      reason: acquired.reason,
      activeCount: acquired.activeCount,
      preparationLimit: acquired.limit,
    });
    return { ok: false, retryAfter: LEASE_RETRY_AFTER_SECONDS };
  }
  if (!acquired) return { ok: false, retryAfter: LEASE_RETRY_AFTER_SECONDS };

  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const activeCatalog = await getActivePackCatalogSnapshot(ctx);
  await ensureRepoMetadataDefaults(store);
  const refs = (await store.get("refs")) ?? [];
  const head = (await store.get("head")) ?? DEFAULT_HEAD;
  const refsVersion = (await store.get("refsVersion")) || 0;
  const packsetVersion = (await store.get("packsetVersion")) || 0;
  const nextPackSeq = (await store.get("nextPackSeq")) || 1;

  let concurrentStockPreparation: boolean | undefined;
  const preparationConcurrency = options?.stockPreparationConcurrency;
  let stockPreparationReserved: true | undefined;
  if (preparationConcurrency !== undefined) {
    const handoff = await ctx.storage.transaction(async (transaction) => {
      const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
      const current = await transactionStore.get("receiveLease");
      if (current?.token !== lease.token || current.expiresAt <= Date.now()) return false;
      const compactLease = activeLeaseOrUndefined(
        await transactionStore.get("compactLease"),
        Date.now()
      );
      const gc = await transaction.get<GcOperation>(GC_OPERATION_KEY);
      if (
        compactLease?.operation === "reachability-gc" &&
        gc?.coordination &&
        gc.snapshot?.token === compactLease.token
      ) {
        return "exclusive" as const;
      }
      const existing = (await transactionStore.get("stockReceivePreparationLeases")) ?? [];
      const active = activeStockReceivePreparationLeases(existing);
      const activeOperations = await listActiveNativeReceiveOperations(transactionStore);
      if (
        active.length + activeOperations.length >= preparationConcurrency ||
        activeOperations.some(
          (operation) => !operation.stockReceive || operation.state !== "processing"
        )
      )
        return false;
      await transactionStore.put("stockReceivePreparationLeases", [...existing, lease]);
      await transactionStore.delete("receiveLease");
      const alarm = await transaction.getAlarm();
      if (alarm === null || alarm > lease.expiresAt) await transaction.setAlarm(lease.expiresAt);
      concurrentStockPreparation = active.length + activeOperations.length > 0 ? true : undefined;
      return "reserved" as const;
    });
    if (!handoff) {
      await abortReceiveLease(ctx, lease.token);
      return { ok: false, retryAfter: LEASE_RETRY_AFTER_SECONDS };
    }
    if (handoff === "reserved") {
      stockPreparationReserved = true;
      logger?.info("stock-receive:preparation-reserved", {
        concurrent: concurrentStockPreparation === true,
        limit: preparationConcurrency,
      });
      const afterHandoff = afterStockPreparationHandoffForTesting;
      afterStockPreparationHandoffForTesting = undefined;
      await afterHandoff?.(ctx);
    }
  }

  return {
    ok: true,
    lease,
    refs,
    head,
    refsVersion,
    packsetVersion,
    nextPackSeq,
    activeCatalog,
    stockPreparationReserved,
    concurrentStockPreparation:
      concurrentStockPreparation ??
      ((await listActiveNativeReceiveOperations(store)).length > 0 || undefined),
  };
}

const STOCK_RECEIVE_RECOVERY_LEASE_TTL_MS = 5 * 60_000;

/**
 * Issues a bounded retry-only staging lease for the exact retained stock
 * operation. It never relaxes the generic authority lease: other operation
 * IDs and every ordinary/legacy receive remain fenced by beginReceiveLease.
 */
export async function beginStockReceiveRecoveryLease(
  ctx: DurableObjectState,
  operationId: string
): Promise<BeginStockReceiveRecoveryResult> {
  const now = Date.now();
  const selected = await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) {
      return { status: "busy", retryAfter: LEASE_RETRY_AFTER_SECONDS } as const;
    }
    const operation = await store.get(nativeReceiveOperationKey(operationId));
    if (
      !operation?.stockReceive ||
      (operation.state !== "ready" && operation.state !== "finalizing")
    ) {
      return { status: "not_found" } as const;
    }
    const retained = await store.get("stockReceiveRecoveryLease");
    if (retained) {
      if (retained.expiresAt <= now) {
        return {
          status: "cleanup_required",
          operationId: retained.operationId,
          token: retained.token,
        } as const;
      }
      return { status: "busy", retryAfter: LEASE_RETRY_AFTER_SECONDS } as const;
    }
    const lease: StockReceiveRecoveryLease = {
      token: crypto.randomUUID(),
      operationId,
      operation: "receive",
      createdAt: now,
      expiresAt: now + STOCK_RECEIVE_RECOVERY_LEASE_TTL_MS,
    };
    await store.put("stockReceiveRecoveryLease", lease);
    return { status: "selected", operation, lease } as const;
  });
  if (selected.status !== "selected") return selected;

  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  await ensureRepoMetadataDefaults(store);
  return {
    status: "recovery",
    begin: {
      ok: true,
      lease: selected.lease,
      refs: selected.operation.stockReceive!.advertisedRefs,
      head: (await store.get("head")) ?? DEFAULT_HEAD,
      refsVersion: (await store.get("refsVersion")) ?? 0,
      packsetVersion: selected.operation.catalogGeneration,
      nextPackSeq: (await store.get("nextPackSeq")) ?? 1,
      activeCatalog: selected.operation.activeCatalog,
      stockRecovery: { operationId, token: selected.lease.token },
    },
  };
}

export async function completeStockReceiveRecoveryLease(
  ctx: DurableObjectState,
  operationId: string,
  token: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const retained = await store.get("stockReceiveRecoveryLease");
    if (!retained) return true;
    if (retained.operationId !== operationId || retained.token !== token) return false;
    await store.delete("stockReceiveRecoveryLease");
    return true;
  });
}

export async function abortReceiveLease(ctx: DurableObjectState, token: string): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const recovery = await store.get("stockReceiveRecoveryLease");
    if (recovery?.token === token) {
      await store.delete("stockReceiveRecoveryLease");
      return true;
    }
    if (await removeStockReceivePreparationLease(store, token)) return true;
    const existing = await store.get("receiveLease");
    if (!existing || existing.token !== token) return false;
    await store.delete("receiveLease");
    return true;
  });
}

export async function promoteStockPreparationLease(
  ctx: DurableObjectState,
  token: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    if (await store.get("receiveLease")) return false;
    const compactLease = activeLeaseOrUndefined(await store.get("compactLease"), Date.now());
    if (compactLease) {
      const gc = await transaction.get<GcOperation>(GC_OPERATION_KEY);
      if (
        compactLease.operation !== "reachability-gc" ||
        !gc?.coordination ||
        gc.snapshot?.token !== compactLease.token
      )
        return false;
    }
    const preparations = await store.get("stockReceivePreparationLeases");
    const selected = preparations?.find(
      (lease) => lease.token === token && lease.expiresAt > Date.now()
    );
    if (!selected || preparations?.some((lease) => lease.token !== token)) return false;
    if ((await listActiveNativeReceiveOperations(store)).length > 0) return false;
    await store.delete("stockReceivePreparationLeases");
    await store.put("receiveLease", selected);
    return true;
  });
}

export async function abortCompactionLease(
  ctx: DurableObjectState,
  token: string
): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const existing = await store.get("compactLease");
  if (!existing || existing.token !== token) return false;
  await store.delete("compactLease");
  return true;
}
