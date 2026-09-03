import {
  isNativeReceiveTerminal,
  type NativeReceiveOperation,
} from "@/worker/git/nativeReceive/types";

import {
  nativeReceiveOperationKey,
  type RepoLease,
  type RepoStateSchema,
  type TypedStorage,
} from "./repoState";

export const STOCK_RECEIVE_EXECUTION_CLAIM_MS = 15 * 60_000;

export function activeStockReceivePreparationLeases(
  leases: RepoLease[] | undefined,
  now: number = Date.now()
): RepoLease[] {
  return (leases ?? []).filter((lease) => lease.expiresAt > now);
}

export async function listActiveStockReceivePreparationLeases(
  store: TypedStorage<RepoStateSchema>,
  now: number = Date.now()
): Promise<RepoLease[]> {
  return activeStockReceivePreparationLeases(await store.get("stockReceivePreparationLeases"), now);
}

export async function removeStockReceivePreparationLease(
  store: TypedStorage<RepoStateSchema>,
  token: string
): Promise<boolean> {
  const leases = await store.get("stockReceivePreparationLeases");
  if (!leases?.some((lease) => lease.token === token)) return false;
  const retained = leases.filter((lease) => lease.token !== token);
  if (retained.length > 0) await store.put("stockReceivePreparationLeases", retained);
  else await store.delete("stockReceivePreparationLeases");
  return true;
}

export async function listActiveNativeReceiveOperations(
  store: TypedStorage<RepoStateSchema>
): Promise<NativeReceiveOperation[]> {
  const operations: NativeReceiveOperation[] = [];
  for (const id of (await store.get("nativeReceiveOperationIndex")) ?? []) {
    const operation = await store.get(nativeReceiveOperationKey(id));
    if (operation && !isNativeReceiveTerminal(operation.state)) operations.push(operation);
  }
  return operations;
}

export async function listActiveStockReceiveOperations(
  store: TypedStorage<RepoStateSchema>
): Promise<NativeReceiveOperation[]> {
  return (await listActiveNativeReceiveOperations(store)).filter(
    (operation) => operation.stockReceive !== undefined
  );
}

export async function hasStockReceiveCleanupOrActivity(
  store: TypedStorage<RepoStateSchema>,
  deletionDrain?: { now: number; drainMs: number } | undefined
): Promise<boolean> {
  for (const id of (await store.get("nativeReceiveOperationIndex")) ?? []) {
    const operation = await store.get(nativeReceiveOperationKey(id));
    if (!operation?.stockReceive) continue;
    const needsAttention = !isNativeReceiveTerminal(operation.state) || operation.cleanupPending;
    if (!needsAttention) continue;
    if (!deletionDrain) return true;

    // Once deletion has durably fenced new work, orphaned stock state may be
    // ignored only after the operation's longest possible R2 activity plus the
    // writer drain. The repository-wide R2 sweep owns cleanup from that point.
    const activityExpiresAt = isNativeReceiveTerminal(operation.state)
      ? operation.updatedAt
      : (operation.claimExpiresAt ?? operation.updatedAt + STOCK_RECEIVE_EXECUTION_CLAIM_MS);
    if (activityExpiresAt + deletionDrain.drainMs > deletionDrain.now) return true;
  }
  return false;
}
