// Typed schema for Repo Durable Object storage
// Provides a light wrapper to get strong typing on storage keys/values in tests and code.

import type { AcceptedWriteContext, AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type { NativeReceiveOperation } from "@/worker/git/nativeReceive/types";

export type ObjKey = `obj:${string}`;
export type ReceiveOutcomeKey = `receiveOutcome:${string}`;
export type ReceiveFinalizeIntentKey = `receiveFinalizeIntent:${string}`;
export type IngestionReceiptKey = `ingestionReceipt:${string}`;
export type AcceptedWriteJournalKey = `acceptedWrite:${string}`;
export type AcceptedWriteHeadKey = `acceptedWriteHead:${string}`;
export type MaterializedSnapshotKey = `materializedSnapshot:${string}`;
export type SnapshotCurrentKey = `snapshotCurrent:${string}`;
export type NativeReceiveOperationKey = `nativeReceiveOperation:${string}`;

export type Ref = { name: string; oid: string };
export type Head = { target: string; oid?: string; unborn?: boolean };
export type RepoLease = {
  token: string;
  createdAt: number;
  expiresAt: number;
  operation?:
    | "receive"
    | "compaction"
    | "reachability-gc"
    | "pack-ref-backfill"
    | "generation-publication";
};

export type StockReceiveRecoveryLease = RepoLease & {
  operationId: string;
};

export type SnapshotMaterializationLease = RepoLease & {
  prefix: string;
};

export type RepositoryMaintenanceLease = RepoLease & {
  operation: "pack-ref-backfill" | "generation-publication";
};

export type NativeCatalogReaderLease = {
  token: string;
  createdAt: number;
  expiresAt: number;
  operation: "native-reader";
  generation: number;
};

export type RepositoryReadLease = {
  token: string;
  createdAt: number;
  expiresAt: number;
  operation: "git-fetch";
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

export type ReceiveRefPublication = {
  at: number;
  refsVersion: number;
};

export type ReceiveCommitOutcome = {
  token: string;
  statuses: Array<{ ref: string; ok: boolean; msg?: string }>;
  changed: boolean;
  empty: boolean;
  shouldQueueCompaction: boolean;
  outputValidationBytes?: number;
  outputValidationRequests?: number;
  outputEtags?: { pack: string; idx: string; refs: string };
  refPublication?: ReceiveRefPublication;
};

export type ReceiveFinalizeIntent = {
  token: string;
  refPublication?: ReceiveRefPublication;
  commands: Array<{ oldOid: string; newOid: string; ref: string }>;
  expectedRefsVersion: number;
  nextHead: Head;
  nextRefsVersion: number;
  stagedPack?: {
    packKey: string;
    packBytes: number;
    idxBytes: number;
    objectCount: number;
    integrity?: {
      packSha256: string;
      idxSha256: string;
      refsSha256: string;
      refsBytes: number;
      packEtag?: string;
      idxEtag?: string;
      refsEtag?: string;
    };
  };
  packSequence?: number;
  nextPacksetVersion?: number;
  ingestionReceipt?: IngestionReceipt;
  acceptedWriteContext?: AcceptedWriteContext;
  recoveryAttempts?: number;
  recoveryEscalated?: boolean;
  createdAt: number;
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
  "nativeExecution:foreground":
    | import("@/worker/git/nativeReceive/execution").NativeExecutionRecord
    | undefined;
  "nativeExecution:maintenance":
    | import("@/worker/git/nativeReceive/execution").NativeExecutionRecord
    | undefined;
  refs: Ref[];
  head: Head;
  refsVersion: number;
  packsetVersion: number;
  generationPublicationPending:
    | {
        generation: number;
        activePackKeys: string[];
      }
    | undefined;
  nativeCatalogReaderGenerationFloor: number | undefined;
  nextPackSeq: number;
  receiveLease: RepoLease | undefined;
  stockReceiveRecoveryLease: StockReceiveRecoveryLease | undefined;
  receiveOutcomeIndex: string[] | undefined;
  nativeReceiveOperationIndex: string[] | undefined;
  nativeCatalogReaderLease: NativeCatalogReaderLease | undefined;
  repositoryReadLeases: RepositoryReadLease[] | undefined;
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
  Record<ReceiveFinalizeIntentKey, ReceiveFinalizeIntent> &
  Record<NativeReceiveOperationKey, NativeReceiveOperation> &
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

export function receiveFinalizeIntentKey(token: string): ReceiveFinalizeIntentKey {
  return `receiveFinalizeIntent:${token}` as ReceiveFinalizeIntentKey;
}

export function nativeReceiveOperationKey(operationId: string): NativeReceiveOperationKey {
  return `nativeReceiveOperation:${operationId}` as NativeReceiveOperationKey;
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
