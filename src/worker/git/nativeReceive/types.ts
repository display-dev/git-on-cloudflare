import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type { ReceiveCommand, ReceiveStatus } from "@/worker/git/operations/validation";
import type { PackCatalogRow } from "@/worker/do/repo/db/schema";

export type NativeReceiveOperationState =
  | "staged"
  | "processing"
  | "ready"
  | "finalizing"
  | "committed"
  | "aborted"
  | "failed";

export type NativeReceiveTerminalResult = {
  statuses: ReceiveStatus[];
  changed: boolean;
  empty: boolean;
  packKey?: string | undefined;
  packBytes?: number | undefined;
};

export type NativeReceiveOperation = {
  id: string;
  fingerprint: string;
  leaseToken: string;
  repositoryId: string;
  state: NativeReceiveOperationState;
  inputPackKey: string;
  inputBytes: number;
  inputEtag: string;
  outputPackKey: string;
  outputIdxKey: string;
  outputRefsKey: string;
  commands: ReceiveCommand[];
  acceptedWrites: AcceptedWriteFact[];
  activeCatalog: PackCatalogRow[];
  catalogGeneration: number;
  createdAt: number;
  updatedAt: number;
  attempts: number;
  cleanupPending: boolean;
  finalizeAttempts?: number | undefined;
  finalizeEscalated?: boolean | undefined;
  claimId?: string | undefined;
  claimExpiresAt?: number | undefined;
  errorCode?: string | undefined;
  processorResult?: NativeReceiveProcessResult | undefined;
  result?: NativeReceiveTerminalResult | undefined;
};

export type NativeReceiveOperationMetrics = Pick<
  NativeReceiveProcessResult,
  "elapsedMs" | "scratchBytes" | "hydratedBytes" | "downloadedBytes" | "cacheHitBytes"
>;

export type NativeReceiveOperationView = Pick<
  NativeReceiveOperation,
  "id" | "state" | "createdAt" | "updatedAt" | "attempts" | "errorCode" | "result"
> & { metrics?: NativeReceiveOperationMetrics | undefined };

export type EnqueueNativeReceiveResult =
  | { status: "queued"; operation: NativeReceiveOperationView }
  | { status: "replayed"; operation: NativeReceiveOperationView }
  | { status: "conflict"; message: string }
  | { status: "lease_mismatch"; message: string }
  | { status: "repository_deleting"; message: string }
  | { status: "dispatch_failed"; message: string };

export type MatchNativeReceiveOperationResult =
  | { status: "not_found" }
  | { status: "match"; operation: NativeReceiveOperationView }
  | { status: "conflict" };

export type NativeReceiveProcessRequest = {
  operationId: string;
  inputPackKey: string;
  inputBytes: number;
  activePacks: Array<{ packKey: string; packBytes: number; idxBytes: number }>;
  commands: ReceiveCommand[];
  outputPackKey: string;
  outputIdxKey: string;
  outputRefsKey: string;
};

export type NativeReceiveProcessResult = {
  operationId: string;
  packBytes: number;
  idxBytes: number;
  refsBytes: number;
  objectCount: number;
  packSha1: string;
  elapsedMs: number;
  scratchBytes: number;
  hydratedBytes: number;
  downloadedBytes: number;
  cacheHitBytes: number;
};

export type RepositoryContainerBridgeProps = {
  operationId: string;
  readKeys: Array<{ key: string; expectedBytes: number; expectedEtag?: string | undefined }>;
  writeKeys: Array<{ key: string; maxBytes: number }>;
};

export function nativeReceiveOperationView(
  operation: NativeReceiveOperation
): NativeReceiveOperationView {
  return {
    id: operation.id,
    state: operation.state,
    createdAt: operation.createdAt,
    updatedAt: operation.updatedAt,
    attempts: operation.attempts,
    errorCode: operation.errorCode,
    result: operation.result,
    metrics: operation.processorResult
      ? {
          elapsedMs: operation.processorResult.elapsedMs,
          scratchBytes: operation.processorResult.scratchBytes,
          hydratedBytes: operation.processorResult.hydratedBytes,
          downloadedBytes: operation.processorResult.downloadedBytes,
          cacheHitBytes: operation.processorResult.cacheHitBytes,
        }
      : undefined,
  };
}

export function isNativeReceiveTerminal(state: NativeReceiveOperationState): boolean {
  return state === "committed" || state === "aborted" || state === "failed";
}

export function isValidNativeReceiveOperationId(value: string): boolean {
  return /^[A-Za-z0-9_-]{1,100}$/.test(value);
}
