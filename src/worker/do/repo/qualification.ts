import { getDb, listPackCatalog } from "./db";
import { getActivePackCatalogCount } from "./db/dal/packCatalog";
import { getRepoActivitySnapshot } from "./catalog/activity";
import { getRefs } from "./refs";
import { clearQualificationSnapshotProjectionState } from "./acceptedWrites";
import { pruneRepositoryActivityLeases } from "./repositoryLifecycle";
import { asTypedStorage, type RepoStateSchema } from "./repoState";
import { COMPACT_LEASE_TTL_MS } from "./catalog/shared";
import type { GcOperation } from "@/worker/git/maintenance/gcOperation";
import { GC_OPERATION_KEY, isGcTerminal } from "./catalog/gcOperation";

export const QUALIFICATION_REPOSITORY_SCHEMA_VERSION = 1;

const TRANSIENT_PREFIXES = [
  "acceptedWrite:",
  "acceptedWriteHead:",
  "ingestionReceipt:",
  "nativeReceiveOperation:",
  "receiveFinalizeIntent:",
  "receiveOutcome:",
] as const;

const TRANSIENT_KEYS = [
  "acceptedWriteHeadSequence",
  "ingestionReceiptIndex",
  "nativeReceiveOperationIndex",
  "receiveOutcomeIndex",
  "stockReceiveRecoveryLease",
  "stockReceivePublicationLease",
  "stockReceivePreparationLeases",
  "reachabilityGcPending",
  "compactionWantedAt",
  "nativeInputHold:foreground",
  "nativeInputHold:maintenance",
  GC_OPERATION_KEY,
] as const;

export type QualificationRepositoryInventory = {
  schemaVersion: 1;
  state: "idle" | "active";
  refCount: number;
  refStateDigest: string;
  activePackCount: number;
  transientStateCount: number;
};

export type QualificationResetResult =
  | { schemaVersion: 1; status: "reset" | "already_reset"; deletedStateCount: number }
  | { schemaVersion: 1; status: "conflict"; reason: "active" | "ref_state_mismatch" };

// Setup/teardown only: stop queued compaction retries without cancelling a
// processor or releasing its lease. Measured GC always retains pending demand.
export async function settleQualificationCompaction(
  ctx: DurableObjectState,
  env: Env,
  expectedRefStateDigest: string
) {
  if (env.QUALIFICATION_MODE !== "1" || !env.QUALIFICATION_SECRET)
    return { schemaVersion: 1 as const, status: "conflict" as const };
  return ctx.storage.transaction(async (transaction) => {
    const refs = (await transaction.get<Array<{ name: string; oid: string }>>("refs")) ?? [];
    const gc = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    if (
      (await transaction.get("repositoryDeleting")) ||
      (gc && !isGcTerminal(gc)) ||
      (await refStateDigest(refs)) !== expectedRefStateDigest
    )
      return { schemaVersion: 1 as const, status: "conflict" as const };
    const cleared = (await transaction.get("compactionWantedAt")) !== undefined;
    await transaction.delete("compactionWantedAt");
    return {
      schemaVersion: 1 as const,
      status: "request-cleared" as const,
      cleared,
      writerActive: Boolean(
        (await transaction.get("receiveLease")) ||
        (await transaction.get("stockReceivePreparationLeases")) ||
        (await transaction.get("compactLease"))
      ),
    };
  });
}

async function refStateDigest(refs: Array<{ name: string; oid: string }>): Promise<string> {
  const canonical = refs
    .map((ref) => `${ref.name}\0${ref.oid}\n`)
    .sort()
    .join("");
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(canonical));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

type QualificationStorageReader = {
  list<T = unknown>(options: { prefix: string }): Promise<Map<string, T>>;
  get<T = unknown>(key: string): Promise<T | undefined>;
};

async function transientKeys(storage: QualificationStorageReader): Promise<string[]> {
  const keys = new Set<string>();
  for (const prefix of TRANSIENT_PREFIXES) {
    for (const key of (await storage.list({ prefix })).keys()) keys.add(key);
  }
  for (const key of TRANSIENT_KEYS) {
    if (key === GC_OPERATION_KEY) {
      const operation = await storage.get<GcOperation>(key);
      // The bounded terminal receipt and admission tombstones are durable
      // replay metadata, not an unfinished writer or staging obligation.
      if (operation && !isGcTerminal(operation)) keys.add(key);
      continue;
    }
    if ((await storage.get(key)) !== undefined) keys.add(key);
  }
  return [...keys].sort();
}

export async function getQualificationRepositoryInventory(
  ctx: DurableObjectState
): Promise<QualificationRepositoryInventory> {
  const refs = await getRefs(ctx);
  const activity = await getRepoActivitySnapshot(ctx);
  return {
    schemaVersion: QUALIFICATION_REPOSITORY_SCHEMA_VERSION,
    state: activity.state === "idle" ? "idle" : "active",
    refCount: refs.length,
    refStateDigest: await refStateDigest(refs),
    activePackCount: await getActivePackCatalogCount(getDb(ctx.storage)),
    transientStateCount: (await transientKeys(ctx.storage)).length,
  };
}

export async function resetQualificationRepositoryState(
  ctx: DurableObjectState,
  expectedRefStateDigest: string
): Promise<QualificationResetResult> {
  return await ctx.storage.transaction(async (transaction) => {
    const refs = (await transaction.get<Array<{ name: string; oid: string }>>("refs")) ?? [];
    if ((await refStateDigest(refs)) !== expectedRefStateDigest) {
      return { schemaVersion: 1, status: "conflict", reason: "ref_state_mismatch" };
    }
    if (!(await pruneRepositoryActivityLeases(asTypedStorage<RepoStateSchema>(transaction)))) {
      return { schemaVersion: 1, status: "conflict", reason: "active" };
    }
    if ((await transaction.get("reachabilityGcPending")) !== undefined) {
      return { schemaVersion: 1, status: "conflict", reason: "active" };
    }
    const gc = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    if (gc && !isGcTerminal(gc)) return { schemaVersion: 1, status: "conflict", reason: "active" };
    if (gc) await transaction.delete(GC_OPERATION_KEY);
    // A queued request has no authority once every repository lease is idle.
    // Clear it in this exact-ref transaction so a waiting queue message sees
    // no work; an already-started worker would have held a lease above.
    await transaction.delete("compactionWantedAt");
    const deletedProjectionCount = await clearQualificationSnapshotProjectionState(
      transaction,
      refs
    );
    const keys = await transientKeys(transaction);
    if (keys.length === 0) {
      if (deletedProjectionCount === 0 && !gc) {
        return { schemaVersion: 1, status: "already_reset", deletedStateCount: 0 };
      }
      return {
        schemaVersion: 1,
        status: "reset",
        deletedStateCount: deletedProjectionCount + (gc ? 1 : 0),
      };
    }
    await transaction.delete(keys);
    return {
      schemaVersion: 1,
      status: "reset",
      deletedStateCount: keys.length + deletedProjectionCount + (gc ? 1 : 0),
    };
  });
}

/** Fence a synthetic-only orphan sweep after all writer/reader drain windows. */
export async function beginQualificationStorageRecovery(
  ctx: DurableObjectState,
  expectedRefStateDigest: string
) {
  const lease = await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const refs = (await store.get("refs")) ?? [];
    if ((await refStateDigest(refs)) !== expectedRefStateDigest) return null;
    if (await store.get("repositoryDeleting")) return null;
    if (!(await pruneRepositoryActivityLeases(store))) return null;
    if ((await transientKeys(transaction)).length !== 0) return null;
    if (await store.get("generationPublicationPending")) return null;
    const now = Date.now();
    const lease = {
      token: crypto.randomUUID(),
      operation: "reachability-gc" as const,
      createdAt: now,
      expiresAt: now + COMPACT_LEASE_TTL_MS,
    };
    await store.put("compactLease", lease);
    return { lease, refs, packsetVersion: (await store.get("packsetVersion")) ?? 0 };
  });
  if (!lease) return { status: "conflict" as const };
  return {
    status: "held" as const,
    ...lease,
    catalog: await listPackCatalog(getDb(ctx.storage)),
  };
}
