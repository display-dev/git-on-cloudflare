// Typed schema for Repo Durable Object storage
// Provides a light wrapper to get strong typing on storage keys/values in tests and code.

import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";

export type ObjKey = `obj:${string}`;
export type ReceiveOutcomeKey = `receiveOutcome:${string}`;
export type IngestionReceiptKey = `ingestionReceipt:${string}`;
export type AcceptedWriteJournalKey = `acceptedWrite:${string}`;
export type AcceptedWriteHeadKey = `acceptedWriteHead:${string}`;
export type MaterializedSnapshotKey = `materializedSnapshot:${string}`;
export type SnapshotCurrentKey = `snapshotCurrent:${string}`;

export type Ref = { name: string; oid: string };
export type Head = { target: string; oid?: string; unborn?: boolean };
export type RepoLease = {
  token: string;
  createdAt: number;
  expiresAt: number;
  operation?: "receive" | "compaction" | "reachability-gc" | "pack-ref-backfill";
};

export type SnapshotMaterializationLease = RepoLease & {
  prefix: string;
};

export type RepositoryMaintenanceLease = RepoLease & {
  operation: "pack-ref-backfill";
};

export type ReachabilityGcPending = {
  token: string;
  packKey: string;
  state: "staged" | "committing" | "cleanup";
  safeCleanupAt?: number;
};

export type IngestionReceipt = {
  keyHash: string;
  fingerprint: string;
  acceptedWrite: AcceptedWriteFact;
  treeSha: string;
  createdAt: number;
};

export type ReceiveCommitOutcome = {
  token: string;
  statuses: Array<{ ref: string; ok: boolean; msg?: string }>;
  changed: boolean;
  empty: boolean;
  shouldQueueCompaction: boolean;
};

export type AcceptedWriteJournalEntry = {
  id: string;
  sequence: number;
  fact: AcceptedWriteFact;
  acceptedAt: number;
  materializedAt?: number;
};

export type AcceptedWriteHead = {
  ref: string;
  beforeSha: string;
  afterSha: string;
  sequence: number;
};

export type MaterializedSnapshot = {
  commitSha: string;
  firstSequence: number;
  materializedAt: number;
};

export type SnapshotCurrent = {
  ref: string;
  commitSha: string;
  sequence: number;
  updatedAt: number;
};

export type RepoStateSchema = {
  refs: Ref[];
  head: Head;
  refsVersion: number;
  packsetVersion: number;
  nextPackSeq: number;
  receiveLease: RepoLease | undefined;
  receiveOutcomeIndex: string[] | undefined;
  ingestionReceiptIndex: string[] | undefined;
  compactLease: RepoLease | undefined;
  reachabilityGcPending: ReachabilityGcPending | undefined;
  snapshotMaterializationLeases: SnapshotMaterializationLease[] | undefined;
  repositoryMaintenanceLeases: RepositoryMaintenanceLease[] | undefined;
  snapshotPrefixes: string[] | undefined;
  repositoryDeleting: boolean | undefined;
  compactionWantedAt: number | undefined;
  lastAccessMs: number;
} & Record<ObjKey, Uint8Array | ArrayBuffer> &
  Record<ReceiveOutcomeKey, ReceiveCommitOutcome> &
  Record<IngestionReceiptKey, IngestionReceipt> &
  Record<AcceptedWriteJournalKey, AcceptedWriteJournalEntry> &
  Record<AcceptedWriteHeadKey, AcceptedWriteHead> &
  Record<MaterializedSnapshotKey, MaterializedSnapshot> &
  Record<SnapshotCurrentKey, SnapshotCurrent>;

export type TypedStorage<S> = {
  get<K extends keyof S & string>(key: K): Promise<S[K] | undefined>;
  get<K extends keyof S & string>(keys: K[]): Promise<Map<K, S[K] | undefined>>;
  put<K extends keyof S & string>(key: K, value: S[K]): Promise<void>;
  delete<K extends keyof S & string>(key: K): Promise<boolean | void>;
};

export function asTypedStorage<S>(
  storage: DurableObjectStorage | DurableObjectTransaction
): TypedStorage<S> {
  async function get<K extends keyof S & string>(key: K): Promise<S[K] | undefined>;
  async function get<K extends keyof S & string>(keys: K[]): Promise<Map<K, S[K] | undefined>>;
  async function get(keyOrKeys: any): Promise<any> {
    return storage.get(keyOrKeys as any);
  }
  const put = <K extends keyof S & string>(key: K, value: S[K]) =>
    storage.put(key as string, value);
  const del = <K extends keyof S & string>(key: K) => storage.delete(key as string);
  return { get: get as any, put: put as any, delete: del as any };
}

// Key helpers for template-literal key families
export function objKey(oid: string): ObjKey {
  return `obj:${oid}` as ObjKey;
}

export function receiveOutcomeKey(token: string): ReceiveOutcomeKey {
  return `receiveOutcome:${token}`;
}

export function ingestionReceiptKey(keyHash: string): IngestionReceiptKey {
  return `ingestionReceipt:${keyHash}`;
}

export function acceptedWriteJournalKey(id: string): AcceptedWriteJournalKey {
  return `acceptedWrite:${id}` as AcceptedWriteJournalKey;
}

export function acceptedWriteHeadKey(ref: string): AcceptedWriteHeadKey {
  return `acceptedWriteHead:${encodeURIComponent(ref)}` as AcceptedWriteHeadKey;
}

export function materializedSnapshotKey(commitSha: string): MaterializedSnapshotKey {
  return `materializedSnapshot:${commitSha}` as MaterializedSnapshotKey;
}

export function snapshotCurrentKey(ref: string): SnapshotCurrentKey {
  return `snapshotCurrent:${encodeURIComponent(ref)}` as SnapshotCurrentKey;
}
