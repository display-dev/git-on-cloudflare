import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type { ReceiveCommand, ReceiveStatus } from "@/worker/git/operations/validation";
import type { PackCatalogRow } from "@/worker/do/repo/db/schema";
import type { ReceiveRefPublication } from "@/worker/do/repo/repoState";
import type { StockPhysicalDependencyEdge, StockPhysicalNode } from "./physicalDependencyPlan";

export type NativeReceiveOperationState =
  | "staged"
  | "processing"
  | "ready"
  | "finalizing"
  | "committed"
  | "aborted"
  | "failed";

export const STOCK_PROCESSOR_RESULT_MAX_BYTES = 256 * 1024;
export const STOCK_PLANNER_REJECTION_METRICS_MAX_BYTES = 64 * 1024;
export const STOCK_ACTIVE_PACK_MAX_COUNT = 64;
export const STOCK_METADATA_MAX_BYTES = 16 * 1024 * 1024;

export type NativeReceiveTerminalResult = {
  refPublication?: ReceiveRefPublication | undefined;
  statuses: ReceiveStatus[];
  changed: boolean;
  empty: boolean;
  packKey?: string | undefined;
  packBytes?: number | undefined;
  receivePackResponse?: string | undefined;
  stockTrace?: NativeReceiveStockTraceEvent[] | undefined;
  authorityPublication?: NativeReceiveAuthorityPublication | undefined;
};

export type NativeReceiveAuthorityR2Object = {
  key: string;
  bytes: number;
  sha256: string;
  etag: string;
};

export type NativeReceiveAuthorityPublication = {
  refs: Array<
    NativeReceiveAuthorityR2Object & {
      name: string;
      oid: string;
    }
  >;
  receipt: NativeReceiveAuthorityR2Object & {
    disposition: "committed";
    refName: string;
    newOid: string;
    digest: string;
  };
};

export type NativeReceiveStockTraceEvent = { sequence: number; event: string };

export type NativeReceiveStockPlanningPhases = {
  activeMetadataMs: number;
  advertisedClosureMs: number;
  inputStagingMs: number;
  incomingAnalysisMs: number;
  boundaryValidationMs: number;
  physicalPlanMs: number;
  canonicalizationMs: number;
  manifestPublishMs: number;
  postManifestCleanupAndOverheadMs: number;
};

export type NativeReceiveStockTiming = {
  planningMs: number;
  planningPhases?: NativeReceiveStockPlanningPhases | undefined;
  bundleReadMs: number;
  containerRpcMs: number;
  containerProcessMs: number;
  containerReadinessMs: number;
  outputUploadMs: number;
  outputVerificationMs: number;
  proofValidationMs: number;
  containerStartAttempts: number;
  containerProbeAttempts: number;
  containerWasRunning: boolean;
};

export type NativeReceiveEvidenceEvent = {
  sequence: number;
  phase: string;
  at?: number | undefined;
  durable?: boolean | undefined;
  bytes?: number | undefined;
  digest?: string | undefined;
  detailCode?: string | undefined;
};

export type NativeReceiveStockRange = {
  entryId?: string | undefined;
  packChecksum: string;
  start: number;
  end: number;
  reason: "required-object";
  requiredOid: string;
  semanticRootOids?: string[] | undefined;
};

export type NativeReceiveStockActivePackRead =
  | {
      packChecksum: string;
      start: number;
      end: number;
      returnedBytes: number;
      kind: "trailer";
    }
  | {
      packChecksum: string;
      start: number;
      end: number;
      returnedBytes: number;
      kind: "required-object";
      requiredOid: string;
    }
  | {
      packChecksum: string;
      start: number;
      end: number;
      returnedBytes: number;
      kind: "whole";
    };

export type NativeReceiveActiveMetadataBundleProof = {
  key: string;
  bytes: number;
  sha256: string;
  etag: string;
  catalogFingerprint: string;
};

export type NativeReceiveStockClosureProof = {
  planSha256: string;
  incomingOids: string[];
  semanticExternalOids: string[];
  visitedIncomingObjectCount: number;
  logicalEdgeCount: number;
  internalEdgeCount: number;
  externalEdgeCount: number;
  missingObjectCount: number;
  objectTypeCounts: { commit: number; tree: number; blob: number; tag: number };
};

export type NativeReceiveStockInput = {
  inputRequestSha256: string;
  packOffset: number;
  packBytes: number;
  advertisedRefs: Array<{ name: string; oid: string }>;
  sideBand64k?: boolean | undefined;
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
  stockReceive?: NativeReceiveStockInput | undefined;
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
  events?: NativeReceiveEvidenceEvent[] | undefined;
  clientAckReadyAt?: number | undefined;
  finalizeAttempts?: number | undefined;
  finalizeEscalated?: boolean | undefined;
  claimId?: string | undefined;
  claimExpiresAt?: number | undefined;
  errorCode?: string | undefined;
  processorResult?: NativeReceiveProcessResult | undefined;
  rejectionMetrics?: NativeReceiveOperationMetrics | undefined;
  publicationPlan?: NativeReceiveAuthorityPublicationPlan | undefined;
  result?: NativeReceiveTerminalResult | undefined;
};

/**
 * Immutable R2 authority objects planned by RepoDO after its exact-old CAS.
 * The Worker writes these bytes and returns the observed object proof; RepoDO
 * never receives an R2 binding or a callback capable of crossing that bound.
 */
export type NativeReceiveAuthorityPublicationObjectPlan = {
  key: string;
  bytes: number;
  sha256: string;
  json: string;
};

export type NativeReceiveAuthorityPublicationPlan = {
  token: string;
  operationId: string;
  fingerprint: string;
  refs: Array<
    NativeReceiveAuthorityPublicationObjectPlan & {
      name: string;
      oid: string;
    }
  >;
  receipt: NativeReceiveAuthorityPublicationObjectPlan & {
    disposition: "committed";
    refName: string;
    newOid: string;
    digest: string;
  };
};

export type NativeReceivePrepared = {
  operationId: string;
  fingerprint: string;
  processorResult: NativeReceiveProcessResult;
};

export type NativeReceiveExecutionRejection = {
  code:
    | "r2-transient"
    | "replacement-closure-invalid"
    | "output-integrity-invalid"
    | "native-data-plane-failed"
    | "finalize-rejected";
  processorResult?: NativeReceiveProcessResult | undefined;
  metrics?: NativeReceiveOperationMetrics | undefined;
  /** Bounded service-owned phase code; never provider prose. */
  diagnosticCode?: string | undefined;
};

export type NativeReceiveCleanupDescriptor = {
  operationId: string;
  fingerprint: string;
  inputPackKey: string;
  inputRequestSha256: string;
  outputPackKey: string;
  outputIdxKey: string;
  outputRefsKey: string;
};

export type AdmitStockReceiveResult =
  | { status: "admitted"; executionToken: string; operation: NativeReceiveOperation }
  | { status: "finalize_pending"; executionToken: string }
  | {
      status: "cleanup_pending";
      operation: NativeReceiveOperationView;
      cleanup: NativeReceiveCleanupDescriptor;
      includeOutputs: boolean;
    }
  | {
      status: "publication_pending";
      publicationToken: string;
      publication: NativeReceiveAuthorityPublicationPlan;
      cleanup: NativeReceiveCleanupDescriptor;
    }
  | {
      status: "replayed";
      operation: NativeReceiveOperationView;
      cleanup: NativeReceiveCleanupDescriptor;
    }
  | { status: "conflict"; code: "operation-id-conflict" }
  | {
      status: "rejected";
      code: "lease-mismatch" | "repository-deleting" | "operation-ledger-full" | string;
    };

export type FinalizeStockReceiveResult =
  | { status: "busy"; retryAfter: number }
  | {
      status: "publication_pending";
      publicationToken: string;
      publication: NativeReceiveAuthorityPublicationPlan;
      cleanup: NativeReceiveCleanupDescriptor;
    }
  | {
      status: "replayed";
      operation: NativeReceiveOperationView;
      cleanup: NativeReceiveCleanupDescriptor;
    }
  | {
      status: "ref_conflict";
      code: "exact-old-ref-conflict";
      cleanup: NativeReceiveCleanupDescriptor;
    }
  | { status: "rejected"; code: string; cleanup?: NativeReceiveCleanupDescriptor | undefined };

export type ConfirmStockReceivePublicationResult =
  | {
      status: "committed";
      operation: NativeReceiveOperationView;
      cleanup: NativeReceiveCleanupDescriptor;
    }
  | {
      status: "replayed";
      operation: NativeReceiveOperationView;
      cleanup: NativeReceiveCleanupDescriptor;
    }
  | { status: "rejected"; code: string };

export type RejectStockReceiveExecutionResult =
  | { status: "failed"; operation: NativeReceiveOperationView }
  | { status: "replayed"; operation: NativeReceiveOperationView }
  | { status: "rejected"; code: string };

export type CompleteStockReceiveCleanupResult =
  | { status: "complete"; operation: NativeReceiveOperationView }
  | { status: "rejected"; code: string };

export type RecoverStockReceivePublicationResult =
  | {
      status: "publication_pending";
      publicationToken: string;
      publication: NativeReceiveAuthorityPublicationPlan;
      cleanup: NativeReceiveCleanupDescriptor;
    }
  | {
      status: "cleanup_pending";
      cleanup: NativeReceiveCleanupDescriptor;
      includeOutputs: boolean;
    }
  | { status: "none" };

export type NativeReceiveOperationMetrics = Pick<
  NativeReceiveProcessResult,
  | "executionMode"
  | "elapsedMs"
  | "processorStartedAt"
  | "stockTiming"
  | "scratchBytes"
  | "hydratedBytes"
  | "downloadedBytes"
  | "cacheHitBytes"
  | "metadataBytes"
  | "metadataRequests"
  | "inputBytesRead"
  | "inputRequests"
  | "rangeBytes"
  | "rangeRequests"
  | "packsTouched"
  | "ranges"
  | "activePackReads"
  | "activeMetadataBundle"
  | "activePackTrailerBytes"
  | "activePackTrailerRequests"
  | "activePackRangeBytes"
  | "activePackRangeRequests"
  | "activePackWholeBytes"
  | "activePackWholeRequests"
  | "activePackUnattributedBytes"
  | "activePackUnattributedRequests"
  | "selectedPackBytes"
  | "activePackCount"
  | "outputValidationBytes"
  | "outputValidationRequests"
  | "outputBytesWritten"
  | "outputRequests"
>;

export type NativeReceiveOperationView = Pick<
  NativeReceiveOperation,
  | "id"
  | "state"
  | "createdAt"
  | "updatedAt"
  | "attempts"
  | "errorCode"
  | "result"
  | "clientAckReadyAt"
  | "events"
> & { schemaVersion: 1; metrics?: NativeReceiveOperationMetrics | undefined };

export type NativeReceiveOperationEvidenceView = Pick<
  NativeReceiveOperationView,
  | "schemaVersion"
  | "id"
  | "state"
  | "createdAt"
  | "updatedAt"
  | "attempts"
  | "errorCode"
  | "clientAckReadyAt"
  | "events"
  | "result"
  | "metrics"
>;

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
  stockReceive?: NativeReceiveStockInput | undefined;
  maintenance?: {
    roots: string[];
    objectCount: number;
    objectSetSha256: string;
    packSha1: string;
    resultKey: string;
  };
};

export type NativeReceiveProcessResult = {
  operationId: string;
  /** Which bounded processor established the immutable output proof. */
  executionMode?: "stock-container" | "direct-pack" | undefined;
  /** Native receive either produced immutable artifacts or only validated a ref transaction. */
  /** Required by the stock-receive host protocol; absent on non-stock native work. */
  resultKind?: "artifacts" | "ref-only" | undefined;
  packBytes: number;
  idxBytes: number;
  refsBytes: number;
  objectCount: number;
  inputPackObjectCount?: number | undefined;
  packSha1: string;
  elapsedMs: number;
  processorStartedAt?: number | undefined;
  stockTiming?: NativeReceiveStockTiming | undefined;
  scratchBytes: number;
  hydratedBytes: number;
  downloadedBytes: number;
  cacheHitBytes: number;
  maintenance?: {
    objectSetSha256: string;
    downloadMs: number;
    indexMs: number;
    validationMs: number;
    referenceMs: number;
    uploadMs: number;
    downloadBytes: number;
    uploadBytes: number;
    downloadRequests: number;
    uploadRequests: number;
  };
  receivePackResponse?: string | undefined;
  inputRequestSha256?: string | undefined;
  packSha256?: string | undefined;
  idxSha256?: string | undefined;
  refsSha256?: string | undefined;
  stockTrace?: NativeReceiveStockTraceEvent[] | undefined;
  metadataBytes?: number | undefined;
  metadataRequests?: number | undefined;
  inputBytesRead?: number | undefined;
  inputRequests?: number | undefined;
  rangeBytes?: number | undefined;
  rangeRequests?: number | undefined;
  packsTouched?: number | undefined;
  quarantinePathInsideOwnedWorkRoot?: boolean | undefined;
  quarantineRemovedAfterReceive?: boolean | undefined;
  quarantinePathNonEmpty?: boolean | undefined;
  freshWorkDirectory?: boolean | undefined;
  repositoryPackBytesBeforeHydration?: number | undefined;
  sharedObjectCacheDisabled?: boolean | undefined;
  skipConnectivityCheck?: boolean | undefined;
  planSha256?: string | undefined;
  closureProof?: NativeReceiveStockClosureProof | undefined;
  semanticExternalOids?: string[] | undefined;
  thinDeltaBaseOids?: string[] | undefined;
  requiredRootOids?: string[] | undefined;
  prerequisiteObjectOids?: string[] | undefined;
  physicalNodes?: StockPhysicalNode[] | undefined;
  physicalDependencies?: StockPhysicalDependencyEdge[] | undefined;
  topologicalEntryIds?: string[] | undefined;
  selectedPackChecksums?: string[] | undefined;
  activePackBindings?: NativeReceiveActivePackBinding[] | undefined;
  activeMetadataBundle?: NativeReceiveActiveMetadataBundleProof | undefined;
  ranges?: NativeReceiveStockRange[] | undefined;
  activePackReads?: NativeReceiveStockActivePackRead[] | undefined;
  activePackTrailerBytes?: number | undefined;
  activePackTrailerRequests?: number | undefined;
  activePackRangeBytes?: number | undefined;
  activePackRangeRequests?: number | undefined;
  activePackWholeBytes?: number | undefined;
  activePackWholeRequests?: number | undefined;
  activePackUnattributedBytes?: number | undefined;
  activePackUnattributedRequests?: number | undefined;
  closureManifestKey?: string | undefined;
  closureManifestBytes?: number | undefined;
  closureManifestSha256?: string | undefined;
  closureManifestEtag?: string | undefined;
  prerequisitePackKey?: string | undefined;
  prerequisitePackBytes?: number | undefined;
  prerequisitePackSha256?: string | undefined;
  prerequisitePackEtag?: string | undefined;
  incomingObjectCount?: number | undefined;
  visitedIncomingObjectCount?: number | undefined;
  logicalEdgeCount?: number | undefined;
  internalEdgeCount?: number | undefined;
  externalEdgeCount?: number | undefined;
  missingObjectCount?: number | undefined;
  objectTypeCounts?: { commit: number; tree: number; blob: number; tag: number } | undefined;
  selectedPackBytes?: number | undefined;
  activePackCount?: number | undefined;
  outputValidationBytes?: number | undefined;
  outputValidationRequests?: number | undefined;
  outputBytesWritten?: number | undefined;
  outputRequests?: number | undefined;
  outputPackEtag?: string | undefined;
  outputIdxEtag?: string | undefined;
  outputRefsEtag?: string | undefined;
  outputIntegrityRejectedRole?: "pack" | "index" | "references" | undefined;
  outputIntegrityRejectedAt?: "body" | "head" | undefined;
};

export type NativeReceiveActivePackBinding = {
  packKey: string;
  packBytes: number;
  idxBytes: number;
  packChecksum: string;
  idxSha256: string;
  prefSha256: string;
};

export type RepositoryContainerBridgeProps = {
  execution?: import("./execution").NativeExecutionIdentity;
  operationId: string;
  readKeys: Array<{ key: string; expectedBytes: number; expectedEtag?: string | undefined }>;
  writeKeys: Array<{ key: string; maxBytes: number }>;
  requireWriteSha256?: boolean | undefined;
  durableOutputOwner?: boolean;
};

export function nativeReceiveOperationView(
  operation: NativeReceiveOperation
): NativeReceiveOperationView {
  const processorMetrics: NativeReceiveOperationMetrics | undefined = operation.processorResult
    ? {
        executionMode: operation.processorResult.executionMode,
        elapsedMs: operation.processorResult.elapsedMs,
        processorStartedAt: operation.processorResult.processorStartedAt,
        stockTiming: operation.processorResult.stockTiming,
        scratchBytes: operation.processorResult.scratchBytes,
        hydratedBytes: operation.processorResult.hydratedBytes,
        downloadedBytes: operation.processorResult.downloadedBytes,
        cacheHitBytes: operation.processorResult.cacheHitBytes,
        metadataBytes: operation.processorResult.metadataBytes,
        metadataRequests: operation.processorResult.metadataRequests,
        inputBytesRead: operation.processorResult.inputBytesRead,
        inputRequests: operation.processorResult.inputRequests,
        rangeBytes: operation.processorResult.rangeBytes,
        rangeRequests: operation.processorResult.rangeRequests,
        packsTouched: operation.processorResult.packsTouched,
        ranges: operation.processorResult.ranges,
        activePackReads: operation.processorResult.activePackReads,
        activeMetadataBundle: operation.processorResult.activeMetadataBundle,
        activePackTrailerBytes: operation.processorResult.activePackTrailerBytes,
        activePackTrailerRequests: operation.processorResult.activePackTrailerRequests,
        activePackRangeBytes: operation.processorResult.activePackRangeBytes,
        activePackRangeRequests: operation.processorResult.activePackRangeRequests,
        activePackWholeBytes: operation.processorResult.activePackWholeBytes,
        activePackWholeRequests: operation.processorResult.activePackWholeRequests,
        activePackUnattributedBytes: operation.processorResult.activePackUnattributedBytes,
        activePackUnattributedRequests: operation.processorResult.activePackUnattributedRequests,
        selectedPackBytes: operation.processorResult.selectedPackBytes,
        activePackCount: operation.processorResult.activePackCount,
        outputValidationBytes: operation.processorResult.outputValidationBytes,
        outputValidationRequests: operation.processorResult.outputValidationRequests,
        outputBytesWritten: operation.processorResult.outputBytesWritten,
        outputRequests: operation.processorResult.outputRequests,
      }
    : operation.rejectionMetrics;
  return {
    schemaVersion: 1,
    id: operation.id,
    state: operation.state,
    createdAt: operation.createdAt,
    updatedAt: operation.updatedAt,
    attempts: operation.attempts,
    errorCode: operation.errorCode,
    clientAckReadyAt: operation.clientAckReadyAt,
    events: operation.events,
    result: operation.result,
    metrics: processorMetrics,
  };
}

/**
 * Canonical evidence projection shared by the authenticated operation route
 * and direct Durable Object inspection in the disposable qualification lane.
 * The terminal timestamps are included so the authenticated route must expose
 * the exact same durable operation projection as direct qualification access.
 */
export function nativeReceiveOperationEvidenceView(
  operation: NativeReceiveOperationView
): NativeReceiveOperationEvidenceView {
  return {
    schemaVersion: 1,
    id: operation.id,
    state: operation.state,
    createdAt: operation.createdAt,
    updatedAt: operation.updatedAt,
    attempts: operation.attempts,
    errorCode: operation.errorCode,
    clientAckReadyAt: operation.clientAckReadyAt,
    events: operation.events,
    result: operation.result,
    metrics: operation.metrics,
  };
}

function canonicalEvidenceValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalEvidenceValue);
  if (!value || typeof value !== "object") return value;
  const canonical: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    const member = (value as Record<string, unknown>)[key];
    if (member !== undefined) canonical[key] = canonicalEvidenceValue(member);
  }
  return canonical;
}

export function nativeReceiveOperationEvidenceMatches(
  route: NativeReceiveOperationView,
  durable: NativeReceiveOperationView
): boolean {
  return (
    JSON.stringify(canonicalEvidenceValue(nativeReceiveOperationEvidenceView(route))) ===
    JSON.stringify(canonicalEvidenceValue(nativeReceiveOperationEvidenceView(durable)))
  );
}

export function isNativeReceiveTerminal(state: NativeReceiveOperationState): boolean {
  return state === "committed" || state === "aborted" || state === "failed";
}

export function isValidNativeReceiveOperationId(value: string): boolean {
  return /^[A-Za-z0-9_-]{1,100}$/.test(value);
}
