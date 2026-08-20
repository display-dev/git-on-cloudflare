import type { Logger } from "@/worker/common/logger";
import { isNativeReceiveTerminal } from "@/worker/git/nativeReceive/types";
import { MAX_SIMULTANEOUS_CONNECTIONS, SubrequestLimiter } from "@/worker/git/operations/limits";
import { doPrefix, nativeReceiveInputPackKey } from "@/worker/keys";

import { asTypedStorage, nativeReceiveOperationKey, receiveFinalizeIntentKey } from "../repoState";
import type { RepoLease, RepoStateSchema } from "../repoState";
import { scheduleAlarmIfSooner } from "../scheduler";
import { getActivePackCatalogSnapshot } from "./state";
import { activeLeaseOrUndefined } from "./activity";
import type { BeginReceiveResult } from "./shared";
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
      const nativeOperation = await store.get(nativeReceiveOperationKey(receiveLease.token));
      if (!nativeOperation) {
        const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
        try {
          await limiter.run("r2:delete-orphan-native-receive-input", async () => {
            await env.REPO_BUCKET.delete(
              nativeReceiveInputPackKey(doPrefix(ctx.id.toString()), receiveLease.token)
            );
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
      }
      await store.delete("receiveLease");
      logger?.debug("lease:expired", { kind: "receive" });
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

export async function abortReceiveLease(ctx: DurableObjectState, token: string): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
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
