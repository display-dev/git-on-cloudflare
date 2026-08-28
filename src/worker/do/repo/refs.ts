/**
 * Git refs and HEAD management
 *
 * This module handles Git references (branches, tags) and HEAD state,
 * including resolution and updates with consistency guarantees.
 */

import type { RepoStateSchema, Head, TypedStorage } from "./repoState";
import { GC_OPERATION_KEY, type GcOperation } from "@/worker/git/maintenance/gcOperation";
import { gcOwnsSource } from "./catalog/gcCoordination";

import { asTypedStorage, nativeReceiveOperationKey, receiveFinalizeIntentKey } from "./repoState";
import { activeLeaseOrUndefined } from "./catalog/activity";
import { isNativeReceiveTerminal } from "@/worker/git/nativeReceive/types";

async function hasActiveReceiveMutation(store: TypedStorage<RepoStateSchema>): Promise<boolean> {
  const receiveLease = await store.get("receiveLease");
  if (receiveLease?.expiresAt && receiveLease.expiresAt > Date.now()) return true;
  if (
    receiveLease &&
    (await store.get(receiveFinalizeIntentKey(receiveLease.token))) !== undefined
  ) {
    return true;
  }
  const operationIds = (await store.get("nativeReceiveOperationIndex")) ?? [];
  for (const operationId of operationIds) {
    const operation = await store.get(nativeReceiveOperationKey(operationId));
    if (operation && !isNativeReceiveTerminal(operation.state)) return true;
  }
  if (receiveLease) await store.delete("receiveLease");
  return false;
}

/**
 * Retrieves all refs from storage
 * @param ctx - Durable Object state context
 * @returns Array of ref objects with name and oid, or empty array if none exist
 */
export async function getRefs(ctx: DurableObjectState): Promise<{ name: string; oid: string }[]> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  return (await store.get("refs")) ?? [];
}

/**
 * Updates refs in storage
 * @param ctx - Durable Object state context
 * @param refs - New refs array to store
 */
export async function setRefs(
  ctx: DurableObjectState,
  refs: { name: string; oid: string }[]
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    if (gcOwnsSource(await transaction.get<GcOperation>(GC_OPERATION_KEY))) return false;
    if (await hasActiveReceiveMutation(store)) return false;
    const compactLease = activeLeaseOrUndefined(await store.get("compactLease"), Date.now());
    if (compactLease?.operation === "reachability-gc") return false;
    await store.put("refs", refs);
    await store.put("refsVersion", ((await store.get("refsVersion")) || 0) + 1);
    return true;
  });
}

/**
 * Resolves the current HEAD state by looking up the target ref
 * @param ctx - Durable Object state context
 * @returns The resolved HEAD object with target and either oid or unborn flag
 */
export async function resolveHead(ctx: DurableObjectState): Promise<Head> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  if (await store.get("repositoryDeleting")) {
    return { target: "refs/heads/main", unborn: true };
  }
  const stored = await store.get("head");
  const refs = await getRefs(ctx);

  // Determine target (default to main)
  const target = stored?.target || "refs/heads/main";
  const match = refs.find((r) => r.name === target);
  const resolved = match
    ? ({ target, oid: match.oid } as Head)
    : ({ target, unborn: true } as Head);

  // Persist resolved head only if it changed
  await updateHeadIfChanged(store, stored, resolved);

  return resolved;
}

/**
 * Sets HEAD to a new value
 * @param ctx - Durable Object state context
 * @param head - New HEAD value
 */
export async function setHead(ctx: DurableObjectState, head: Head): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    if (await hasActiveReceiveMutation(store)) return false;
    await store.put("head", head);
    return true;
  });
}

/**
 * Get HEAD and refs in a single operation
 * @param ctx - Durable Object state context
 * @returns Object containing HEAD and refs
 */
export async function getHeadAndRefs(
  ctx: DurableObjectState
): Promise<{ head: Head; refs: { name: string; oid: string }[] }> {
  const [head, refs] = await Promise.all([resolveHead(ctx), getRefs(ctx)]);
  return { head, refs };
}

/**
 * Updates HEAD in storage only if the resolved value differs semantically
 * Handles normalization of legacy HEAD shapes (e.g., both oid and unborn present)
 * @param store - The typed storage instance
 * @param stored - The currently stored HEAD value
 * @param resolved - The newly resolved HEAD value
 */
async function updateHeadIfChanged(
  store: TypedStorage<RepoStateSchema>,
  stored: Head | undefined,
  resolved: Head
): Promise<void> {
  try {
    const storedOid = stored?.oid ?? undefined;
    const resolvedOid = resolved.oid ?? undefined;
    const sameTarget = !!stored && stored.target === resolved.target;
    const sameOid = storedOid === resolvedOid;
    const sameUnborn =
      storedOid || resolvedOid ? true : (stored?.unborn === true) === (resolved.unborn === true);
    const same = !!stored && sameTarget && sameOid && sameUnborn;

    if (!same) {
      await store.put("head", resolved);
    }
  } catch {}
}
