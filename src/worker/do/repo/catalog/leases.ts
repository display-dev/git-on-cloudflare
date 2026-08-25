import type { Logger } from "@/worker/common/logger";
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
): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
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
          return;
        }
        await store.delete("receiveLease");
        logger?.debug("lease:expired", { kind: "receive" });
      }
    }
  }

  const compactLease = await store.get("compactLease");
  if (compactLease && compactLease.expiresAt <= now) {
    await store.delete("compactLease");
    logger?.debug("lease:expired", { kind: "compact" });
  }
}

export async function beginReceiveLease(
  ctx: DurableObjectState,
  logger?: Logger
): Promise<BeginReceiveResult> {
  const now = Date.now();
  const lease: RepoLease = {
    token: crypto.randomUUID(),
    createdAt: now,
    expiresAt: now + RECEIVE_LEASE_TTL_MS,
    operation: "receive",
  };
  const acquired = await ctx.storage.transaction(async (transaction) => {
    const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
    if (await transactionStore.get("repositoryDeleting")) return false;
    const nativeOperationIds = (await transactionStore.get("nativeReceiveOperationIndex")) ?? [];
    for (const operationId of nativeOperationIds) {
      const operation = await transactionStore.get(nativeReceiveOperationKey(operationId));
      if (operation && !isNativeReceiveTerminal(operation.state)) return false;
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
    if (compactLease?.operation === "reachability-gc") return false;
    if (existing) logger?.debug("lease:expired", { kind: "receive" });
    await transactionStore.put("receiveLease", lease);
    return true;
  });
  if (!acquired) return { ok: false, retryAfter: LEASE_RETRY_AFTER_SECONDS };

  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const activeCatalog = await getActivePackCatalogSnapshot(ctx);
  await ensureRepoMetadataDefaults(store);
  const refs = (await store.get("refs")) ?? [];
  const head = (await store.get("head")) ?? DEFAULT_HEAD;

  return {
    ok: true,
    lease,
    refs,
    head,
    refsVersion: (await store.get("refsVersion")) || 0,
    packsetVersion: (await store.get("packsetVersion")) || 0,
    nextPackSeq: (await store.get("nextPackSeq")) || 1,
    activeCatalog,
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
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const recovery = await store.get("stockReceiveRecoveryLease");
  if (recovery?.token === token) {
    await store.delete("stockReceiveRecoveryLease");
    return true;
  }
  const existing = await store.get("receiveLease");
  if (!existing || existing.token !== token) return false;
  await store.delete("receiveLease");
  return true;
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
