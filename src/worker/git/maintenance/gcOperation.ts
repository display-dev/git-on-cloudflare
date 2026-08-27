import type { PackCatalogRow } from "@/worker/do/repo/db/schema";
import type { Ref } from "@/worker/do/repo/repoState";
import type { NativeReceiveProcessResult } from "@/worker/git/nativeReceive/types";
import type { CommitReachabilityGcResult } from "@/worker/do/repo/catalog/reachabilityGc";

/** GC owns physical storage only. There are deliberately no receive commands
 * or accepted-write facts in this operation. */
export type GcPhase =
  | "queued"
  | "rewrite"
  | "index"
  | "publish"
  | "reclaim"
  | "discard"
  | "complete"
  | "blocked";

export const GC_OPERATION_KEY = "gcOperation";
export const GC_WAKE_DELAY_MS = 30_000;
// Bound non-converging unpublished work without shortening execution claims,
// writer drain, or committed-generation reconciliation. Not a repository quota.
export const GC_UNPUBLISHED_LIFETIME_MS = 24 * 60 * 60_000;

export const GC_FAULTS = [
  "after-rewrite",
  "during-native",
  "before-publication",
  "after-publication",
] as const;
export type GcFault = (typeof GC_FAULTS)[number];
export type GcQualificationOptions = { faults: GcFault[]; holdReader: boolean; deadlineAt: number };
export type GcQualificationState = {
  deadlineAt: number;
  faults: Partial<Record<GcFault, { triggeredAt?: number; containerStoppedAt?: number }>>;
  reader?: {
    token?: string;
    startedAt?: number;
    expiresAt?: number;
    releasedAt?: number;
    deletionAttemptAt?: number;
    generation?: number;
    expired?: boolean;
  };
};

export type GcSnapshot = {
  token: string;
  refs: Ref[];
  refsVersion: number;
  packsetVersion: number;
  sourcePacks: PackCatalogRow[];
};

export type GcClosure = { objectCount: number; objectSetSha256: string };
export type GcRewriteIdentity = { packBytes: number; packSha1: string };
export type GcCommit = Extract<CommitReachabilityGcResult, { status: "committed" }>;

export type GcPhaseMeasurement = {
  attempts: number;
  startedAt: number;
  completedAt?: number;
  elapsedMs?: number;
  requests?: number;
  readBytes?: number;
  writtenBytes?: number;
};
export type GcStep =
  | "closure-planning"
  | "rewrite-selection"
  | "rewrite-upload"
  | "output-validation"
  | "generation-publication";
export type GcStepMeasurement = {
  completedAttempts: number;
  elapsedMs: number;
  observedRequests: number | null;
  writtenBytes: number | null;
};

export type GcOperation = {
  schemaVersion: 1;
  id: string;
  repositoryId: string;
  phase: GcPhase;
  createdAt: number;
  updatedAt: number;
  claim?: { id: string; expiresAt: number; safeRetryAt: number };
  snapshot?: GcSnapshot;
  closure?: GcClosure;
  retainedPackKey?: string;
  inputPackKey: string;
  outputPackKey: string;
  outputResultKey: string;
  // The upload intent is durable before R2 completion, so a lost completion
  // response is reconciled against immutable identity instead of overwritten.
  rewriteIntent?: GcRewriteIdentity;
  rewrite?: GcRewriteIdentity & { etag: string };
  nativeResult?: NativeReceiveProcessResult;
  nativeStartedClaimId?: string;
  commit?: GcCommit;
  blockedReason?: string;
  discardAfter?: number;
  qualification?: GcQualificationState;
  nativeReadyAt?: number;
  nativeWasRunning?: boolean;
  measurements: Partial<Record<GcPhase, GcPhaseMeasurement>>;
  stepMeasurements?: Partial<Record<GcStep, GcStepMeasurement>>;
};

export type GcOperationResult =
  | { status: "ready"; operation: GcOperation }
  | { status: "busy"; retryAt: number }
  | {
      status: "rejected";
      reason:
        | "repository-deleting"
        | "operation-mismatch"
        | "claim-mismatch"
        | "phase-mismatch"
        | "source-changed"
        | "invalid-input";
    };

/** Closed phase-specific transitions; callers cannot patch ownership, source
 * versions, or publication receipts through an arbitrary state update. */
export type GcProgress =
  | {
      kind: "step";
      step: GcStep;
      elapsedMs: number;
      observedRequests: number | null;
      writtenBytes: number | null;
    }
  | { kind: "snapshot"; snapshot: GcSnapshot }
  | { kind: "plan"; closure: GcClosure; retainedPackKey?: string }
  | { kind: "rewrite-intent"; identity: GcRewriteIdentity }
  | { kind: "rewrite-complete"; identity: GcRewriteIdentity; etag: string }
  | { kind: "native-complete"; result: NativeReceiveProcessResult }
  | { kind: "reclaimed" }
  | { kind: "discard"; reason: "source-changed" | "native-rejected" | "rewrite-rejected" }
  | { kind: "discarded" }
  | { kind: "yield" }
  | { kind: "blocked"; reason: string };
