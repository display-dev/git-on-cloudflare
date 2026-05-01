/**
 * Git refs and HEAD management
 *
 * This module handles Git references (branches, tags) and HEAD state,
 * including resolution and updates with consistency guarantees.
 */

import type { RepoStateSchema, Head } from "./repoState.ts";

import { asTypedStorage } from "./repoState.ts";

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
): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  await store.put("refs", refs);
}

/**
 * Resolves the current HEAD state by looking up the target ref
 * @param ctx - Durable Object state context
 * @returns The resolved HEAD object with target and either oid or unborn flag
 */
export async function resolveHead(ctx: DurableObjectState): Promise<Head> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
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
export async function setHead(ctx: DurableObjectState, head: Head): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  await store.put("head", head);
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
  store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
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
