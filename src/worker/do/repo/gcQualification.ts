import {
  GC_OPERATION_KEY,
  type GcFault,
  type GcOperation,
} from "@/worker/git/maintenance/gcOperation";
import type { RepositoryReadLease, Ref, RepoLease } from "./repoState";
import { getDb, listActivePackCatalog } from "./db";

const READER_HOLD_MS = 15 * 60_000;

// Receives publish to the authoritative catalog before a maintenance generation
// is materialized in R2. Read the catalog and its version together, not through
// the potentially older maintenance-generation manifest.
export async function getQualificationGcSource(ctx: DurableObjectState, env: Env) {
  if (env.QUALIFICATION_MODE !== "1" || !env.QUALIFICATION_SECRET) return null;
  return ctx.storage.transaction(async (transaction) => {
    if (await transaction.get<boolean>("repositoryDeleting")) return null;
    for (const key of ["receiveLease", "compactLease"]) {
      const lease = await transaction.get<RepoLease>(key);
      // An expired writer may still have a durable finalization intent. Let
      // normal recovery clear it before exposing a qualification snapshot.
      if (lease) return null;
    }
    return {
      generation: (await transaction.get<number>("packsetVersion")) ?? 0,
      refs: (await transaction.get<Ref[]>("refs")) ?? [],
      packs: await listActivePackCatalog(getDb(ctx.storage)),
    };
  });
}

// These methods are reached only through the exact-target authenticated
// qualification route or internal execution. No control is active by default.
export async function consumeGcFault(
  ctx: DurableObjectState,
  env: Env,
  operationId: string,
  fault: GcFault
): Promise<boolean> {
  if (env.QUALIFICATION_MODE !== "1" || !env.QUALIFICATION_SECRET) return false;
  return ctx.storage.transaction(async (transaction) => {
    if (await transaction.get<boolean>("repositoryDeleting")) return false;
    const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    if (operation?.id !== operationId) return false;
    const configured = operation.qualification?.faults[fault];
    if (!configured || configured.triggeredAt) return false;
    const expected = {
      "after-rewrite": "index",
      "during-native": "index",
      "before-publication": "publish",
      "after-publication": "reclaim",
    };
    if (operation.phase !== expected[fault]) return false;
    configured.triggeredAt = Date.now();
    await transaction.put(GC_OPERATION_KEY, operation);
    return true;
  });
}

export async function gcReaderLatch(
  ctx: DurableObjectState,
  env: Env,
  token: string,
  packKeys: string[],
  readerOperationId?: string
): Promise<boolean> {
  if (env.QUALIFICATION_MODE !== "1" || !env.QUALIFICATION_SECRET) return false;
  return ctx.storage.transaction(async (transaction) => {
    if (await transaction.get<boolean>("repositoryDeleting")) return false;
    const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    const reader = operation?.qualification?.reader;
    if (!operation || !reader || reader.releasedAt || reader.expired || !operation.snapshot)
      return false;
    // Ordinary foreground reads must never be captured by a qualification
    // latch. This marker selects a reader, not authority: the real read lease
    // and exact-repository qualification configuration are still required.
    if (readerOperationId !== `${operation.id}-reader`) return false;
    const now = Date.now();
    const lease = (
      (await transaction.get<RepositoryReadLease[]>("repositoryReadLeases")) ?? []
    ).find((item) => item.token === token && item.expiresAt > now);
    if (!lease || (reader.token && reader.token !== token)) return false;
    if (!reader.token) {
      if (
        !["index", "publish"].includes(operation.phase) ||
        !operation.snapshot.sourcePacks.every((pack) => packKeys.includes(pack.packKey))
      )
        return false;
      reader.token = token;
      reader.startedAt = now;
      reader.expiresAt = now + READER_HOLD_MS;
    }
    if ((reader.expiresAt ?? 0) <= now) reader.expired = true;
    await transaction.put(GC_OPERATION_KEY, operation);
    return !reader.expired;
  });
}

export async function releaseGcReader(
  ctx: DurableObjectState,
  env: Env,
  operationId: string
): Promise<boolean> {
  if (env.QUALIFICATION_MODE !== "1" || !env.QUALIFICATION_SECRET) return false;
  return ctx.storage.transaction(async (transaction) => {
    const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    const reader = operation?.qualification?.reader;
    if (operation?.id !== operationId || !reader) return false;
    reader.releasedAt ??= Date.now();
    await transaction.put(GC_OPERATION_KEY, operation);
    return true;
  });
}

/** Called only after the real deletion fence returned protected. The record
 * proves an attempted reclamation overlapped this actual Git reader. */
export async function observeGcReaderProtection(
  ctx: DurableObjectState,
  env: Env,
  generation?: number
): Promise<void> {
  if (env.QUALIFICATION_MODE !== "1" || !env.QUALIFICATION_SECRET) return;
  await ctx.storage.transaction(async (transaction) => {
    const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    const reader = operation?.qualification?.reader;
    if (
      !operation?.commit ||
      operation.commit.packCatalogVersion !== generation ||
      !reader?.token ||
      reader.expired ||
      reader.releasedAt
    )
      return;
    const active = (
      (await transaction.get<RepositoryReadLease[]>("repositoryReadLeases")) ?? []
    ).some((lease) => lease.token === reader.token && lease.expiresAt > Date.now());
    if (!active) return;
    reader.deletionAttemptAt ??= Date.now();
    reader.generation = generation;
    await transaction.put(GC_OPERATION_KEY, operation);
  });
}
