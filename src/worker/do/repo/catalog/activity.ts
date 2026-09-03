import { asTypedStorage } from "../repoState";
import type { RepoLease, RepoStateSchema } from "../repoState";
import {
  listActiveStockReceiveOperations,
  listActiveStockReceivePreparationLeases,
} from "../nativeReceiveActivity";

export type RepoActivitySnapshot =
  | { state: "idle"; compactionWantedAt?: number }
  | { state: "receiving"; lease: RepoLease }
  | { state: "compacting"; lease: RepoLease; compactionWantedAt?: number };

export function activeLeaseOrUndefined(
  lease: RepoLease | undefined,
  now: number
): RepoLease | undefined {
  if (!lease) return undefined;
  return lease.expiresAt > now ? lease : undefined;
}

export async function getRepoActivitySnapshot(
  ctx: DurableObjectState
): Promise<RepoActivitySnapshot> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const now = Date.now();

  const receiveLease = activeLeaseOrUndefined(await store.get("receiveLease"), now);
  if (receiveLease) {
    return { state: "receiving", lease: receiveLease };
  }

  const preparationLease = (await listActiveStockReceivePreparationLeases(store, now)).sort(
    (a, b) => a.createdAt - b.createdAt
  )[0];
  if (preparationLease) return { state: "receiving", lease: preparationLease };

  const activeNative = await listActiveStockReceiveOperations(store);
  const oldestNative = activeNative.sort((a, b) => a.createdAt - b.createdAt)[0];
  if (oldestNative) {
    return {
      state: "receiving",
      lease: {
        token: oldestNative.leaseToken,
        operation: "receive",
        createdAt: oldestNative.createdAt,
        expiresAt: oldestNative.claimExpiresAt ?? oldestNative.updatedAt,
      },
    };
  }

  const compactLease = activeLeaseOrUndefined(await store.get("compactLease"), now);
  const compactionWantedAt = await store.get("compactionWantedAt");
  if (compactLease) {
    return {
      state: "compacting",
      lease: compactLease,
      compactionWantedAt,
    };
  }

  return {
    state: "idle",
    compactionWantedAt,
  };
}
