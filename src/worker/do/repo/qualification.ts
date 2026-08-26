import { getDb } from "./db";
import { getActivePackCatalogCount } from "./db/dal/packCatalog";
import { getRepoActivitySnapshot } from "./catalog/activity";
import { getRefs } from "./refs";
import { clearQualificationSnapshotProjectionState } from "./acceptedWrites";
import { pruneRepositoryActivityLeases } from "./repositoryLifecycle";
import { asTypedStorage, type RepoStateSchema } from "./repoState";

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
  "reachabilityGcPending",
  "compactionWantedAt",
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
      if (deletedProjectionCount === 0) {
        return { schemaVersion: 1, status: "already_reset", deletedStateCount: 0 };
      }
      return { schemaVersion: 1, status: "reset", deletedStateCount: deletedProjectionCount };
    }
    await transaction.delete(keys);
    return {
      schemaVersion: 1,
      status: "reset",
      deletedStateCount: keys.length + deletedProjectionCount,
    };
  });
}
