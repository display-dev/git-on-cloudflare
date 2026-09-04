import type { Logger } from "@/worker/common/logger";
import type { NativeExecutionIdentity } from "@/worker/git/nativeReceive/execution";
import {
  beginNativeExecution,
  authorizeNativeExecution,
  finishNativeExecution,
  nativeExecutionKey,
  acknowledgeNativeStop,
} from "./nativeExecution";
import {
  startNativeProcessorSlot,
  finishNativeProcessorSlot,
  stopNativeProcessorSlot,
  deleteNativeProcessorSlot,
} from "../nativeProcessorSlot";
import type {
  NativeExecutionRecord,
  NativeExecutionLane,
} from "@/worker/git/nativeReceive/execution";
import type {
  EnqueueNativeReceiveResult,
  MatchNativeReceiveOperationResult,
  NativeReceiveAuthorityPublication,
  NativeReceiveEvidenceEvent,
  NativeReceiveOperation,
  NativeReceiveOperationView,
  NativeReceiveProcessRequest,
  NativeReceiveProcessResult,
  RepositoryContainerBridgeProps,
} from "@/worker/git/nativeReceive/types";
import { asBufferSource, bytesToHex } from "@/worker/common";
import { z } from "zod";
import {
  isNativeReceiveTerminal,
  nativeReceiveOperationView,
} from "@/worker/git/nativeReceive/types";
import {
  nativeReceiveAuthorityReceiptKey,
  nativeReceiveAuthorityRefKey,
  packIndexKey,
} from "@/worker/keys";
import { MAX_SIMULTANEOUS_CONNECTIONS, SubrequestLimiter } from "@/worker/git/operations/limits";

import {
  finalizeReceiveState,
  ReceiveOutputIntegrityError,
  type ReceiveFinalizeMilestone,
} from "./catalog/receive";
import { RECEIVE_LEASE_TTL_MS } from "./catalog/shared";
import { abortReceiveLease } from "./catalog/leases";
import {
  asTypedStorage,
  nativeReceiveOperationKey,
  receiveFinalizeIntentKey,
  type RepoStateSchema,
  type TypedStorage,
} from "./repoState";
import {
  RECOVERY_ESCALATION_ATTEMPTS,
  recoveryRetryDelayMs,
  scheduleAlarmIfSooner,
} from "./scheduler";

const MAX_RETAINED_OPERATIONS = 128;
const MAX_EVIDENCE_EVENTS = 128;
const MAX_PROCESS_ATTEMPTS = 5;
const CONTAINER_PORT = 8080;
const CONTAINER_READY_ATTEMPTS = 120;
const CONTAINER_READY_INTERVAL_MS = 250;
const CONTAINER_RESPONSE_MAX_BYTES = 64 * 1024;
const OUTPUT_SIDECAR_MAX_BYTES = 1_000_000_000;
const STOCK_OUTPUT_PACK_MAX_BYTES = 32 * 1024 * 1024;
const STOCK_OUTPUT_SIDECAR_MAX_BYTES = 32 * 1024 * 1024;
const LEASE_HEARTBEAT_MS = 30_000;
const NATIVE_READER_LEASE_TTL_MS = 2 * 60_000;
const PROCESSING_CLAIM_TTL_MS = 3 * 60_000;
const CONTAINER_PROCESS_TIMEOUT_MS = 14 * 60_000;

const stockOidSchema = z.string().regex(/^[0-9a-f]{40}$/);
const stockSha256Schema = z.string().regex(/^[0-9a-f]{64}$/);
const stockTypeCountsSchema = z
  .object({
    commit: z.number().int().nonnegative(),
    tree: z.number().int().nonnegative(),
    blob: z.number().int().nonnegative(),
    tag: z.number().int().nonnegative(),
  })
  .strict();
const stockClosureProofSchema = z
  .object({
    planSha256: stockSha256Schema,
    incomingOids: z.array(stockOidSchema).max(100_000),
    semanticExternalOids: z.array(stockOidSchema).max(100_000),
    visitedIncomingObjectCount: z.number().int().nonnegative().max(100_000),
    logicalEdgeCount: z.number().int().nonnegative().max(500_000),
    internalEdgeCount: z.number().int().nonnegative().max(500_000),
    externalEdgeCount: z.number().int().nonnegative().max(500_000),
    missingObjectCount: z.number().int().nonnegative().max(500_000),
    objectTypeCounts: stockTypeCountsSchema,
  })
  .strict();
const stockActivePackReadSchema = z.discriminatedUnion("kind", [
  z
    .object({
      packChecksum: stockOidSchema,
      start: z.number().int().nonnegative(),
      end: z.number().int().positive(),
      returnedBytes: z.number().int().positive(),
      kind: z.literal("trailer"),
    })
    .strict(),
  z
    .object({
      packChecksum: stockOidSchema,
      start: z.number().int().nonnegative(),
      end: z.number().int().positive(),
      returnedBytes: z.number().int().positive(),
      kind: z.literal("required-object"),
      requiredOid: stockOidSchema,
    })
    .strict(),
]);

const stockTimingMsSchema = z
  .number()
  .int()
  .nonnegative()
  .max(5 * 60 * 1000);

const nativeReceiveStockTimingSchema = z
  .object({
    planningMs: stockTimingMsSchema,
    planningPhases: z
      .object({
        activeMetadataMs: stockTimingMsSchema,
        advertisedClosureMs: stockTimingMsSchema,
        inputStagingMs: stockTimingMsSchema,
        incomingAnalysisMs: stockTimingMsSchema,
        boundaryValidationMs: stockTimingMsSchema,
        physicalPlanMs: stockTimingMsSchema,
        manifestPublishMs: stockTimingMsSchema,
        postManifestCleanupAndOverheadMs: stockTimingMsSchema,
      })
      .strict()
      .optional(),
    bundleReadMs: stockTimingMsSchema,
    containerRpcMs: stockTimingMsSchema,
    containerProcessMs: stockTimingMsSchema,
    containerReadinessMs: stockTimingMsSchema,
    outputUploadMs: stockTimingMsSchema,
    outputVerificationMs: stockTimingMsSchema,
    proofValidationMs: stockTimingMsSchema,
    containerStartAttempts: z.number().int().nonnegative().max(120),
    containerProbeAttempts: z.number().int().positive().max(120),
    containerWasRunning: z.boolean(),
  })
  .strict();

export const nativeReceiveProcessResultSchema = z
  .object({
    operationId: z.string().min(1),
    packBytes: z.number().int().positive(),
    idxBytes: z.number().int().positive(),
    refsBytes: z.number().int().positive(),
    objectCount: z.number().int().nonnegative(),
    inputPackObjectCount: z.number().int().positive().max(100_000).optional(),
    packSha1: z.string().regex(/^[0-9a-f]{40}$/),
    elapsedMs: z.number().int().nonnegative(),
    processorStartedAt: z.number().int().nonnegative().optional(),
    stockTiming: nativeReceiveStockTimingSchema.optional(),
    scratchBytes: z.number().int().nonnegative(),
    hydratedBytes: z.number().int().nonnegative().default(0),
    downloadedBytes: z.number().int().nonnegative().default(0),
    cacheHitBytes: z.number().int().nonnegative().default(0),
    maintenance: z
      .object({
        objectSetSha256: z.string().regex(/^[a-f0-9]{64}$/),
        downloadMs: z.number().int().nonnegative(),
        indexMs: z.number().int().nonnegative(),
        validationMs: z.number().int().nonnegative(),
        referenceMs: z.number().int().nonnegative(),
        uploadMs: z.number().int().nonnegative(),
        downloadBytes: z.number().int().nonnegative(),
        uploadBytes: z.number().int().nonnegative(),
        downloadRequests: z.number().int().nonnegative(),
        uploadRequests: z.number().int().nonnegative(),
      })
      .strict()
      .optional(),
    receivePackResponse: z.string().max(1_400_000).optional(),
    inputRequestSha256: z
      .string()
      .regex(/^[0-9a-f]{64}$/)
      .optional(),
    packSha256: z
      .string()
      .regex(/^[0-9a-f]{64}$/)
      .optional(),
    idxSha256: z
      .string()
      .regex(/^[0-9a-f]{64}$/)
      .optional(),
    refsSha256: z
      .string()
      .regex(/^[0-9a-f]{64}$/)
      .optional(),
    stockTrace: z
      .array(
        z
          .object({
            sequence: z.number().int().positive(),
            event: z.string().min(1).max(100),
          })
          .strict()
      )
      .max(128)
      .optional(),
    metadataBytes: z.number().int().nonnegative().optional(),
    rangeBytes: z.number().int().nonnegative().optional(),
    rangeRequests: z.number().int().nonnegative().optional(),
    packsTouched: z.number().int().nonnegative().optional(),
    quarantinePathInsideOwnedWorkRoot: z.boolean().optional(),
    quarantineRemovedAfterReceive: z.boolean().optional(),
    quarantinePathNonEmpty: z.boolean().optional(),
    planSha256: stockSha256Schema.optional(),
    closureProof: stockClosureProofSchema.optional(),
    semanticExternalOids: z.array(stockOidSchema).max(100_000).optional(),
    thinDeltaBaseOids: z.array(stockOidSchema).max(256).optional(),
    requiredRootOids: z.array(stockOidSchema).max(256).optional(),
    ranges: z
      .array(
        z
          .object({
            packChecksum: stockOidSchema,
            start: z.number().int().nonnegative(),
            end: z.number().int().positive(),
            reason: z.literal("required-object"),
            requiredOid: stockOidSchema,
          })
          .strict()
      )
      .max(256)
      .optional(),
    activePackReads: z.array(stockActivePackReadSchema).max(320).optional(),
    activePackTrailerBytes: z.number().int().nonnegative().optional(),
    activePackTrailerRequests: z.number().int().nonnegative().optional(),
    activePackRangeBytes: z.number().int().nonnegative().optional(),
    activePackRangeRequests: z.number().int().nonnegative().optional(),
    activePackWholeBytes: z.number().int().nonnegative().optional(),
    activePackWholeRequests: z.number().int().nonnegative().optional(),
    activePackUnattributedBytes: z.number().int().nonnegative().optional(),
    activePackUnattributedRequests: z.number().int().nonnegative().optional(),
    closureManifestKey: z.string().min(1).max(1_024).optional(),
    closureManifestBytes: z
      .number()
      .int()
      .positive()
      .max(16 * 1024 * 1024)
      .optional(),
    closureManifestSha256: stockSha256Schema.optional(),
    closureManifestEtag: z.string().min(1).max(256).optional(),
    prerequisitePackKey: z.string().min(1).max(1_024).optional(),
    prerequisitePackBytes: z
      .number()
      .int()
      .positive()
      .max(16 * 1024 * 1024)
      .optional(),
    prerequisitePackSha256: stockSha256Schema.optional(),
    prerequisitePackEtag: z.string().min(1).max(256).optional(),
    incomingObjectCount: z.number().int().nonnegative().max(100_000).optional(),
    visitedIncomingObjectCount: z.number().int().nonnegative().max(100_000).optional(),
    logicalEdgeCount: z.number().int().nonnegative().max(500_000).optional(),
    internalEdgeCount: z.number().int().nonnegative().max(500_000).optional(),
    externalEdgeCount: z.number().int().nonnegative().max(500_000).optional(),
    missingObjectCount: z.number().int().nonnegative().max(500_000).optional(),
    objectTypeCounts: stockTypeCountsSchema.optional(),
    selectedPackBytes: z.number().int().nonnegative().optional(),
    activePackCount: z.number().int().nonnegative().max(64).optional(),
  })
  .strict();

type NativeProcessor = (args: {
  ctx: DurableObjectState;
  request: NativeReceiveProcessRequest;
  bridgeProps: RepositoryContainerBridgeProps;
  signal: AbortSignal;
}) => Promise<NativeReceiveProcessResult>;

let nativeProcessorForTesting: NativeProcessor | undefined;
let pauseNextBeforeFinalizationForTesting = false;
let manualWakeupsForTesting = false;
let failNextAfterEnqueueStoreForTesting = false;

export const __test = {
  setNativeProcessor(processor: NativeProcessor): void {
    nativeProcessorForTesting = processor;
  },
  pauseNextBeforeFinalization(): void {
    pauseNextBeforeFinalizationForTesting = true;
  },
  useManualWakeups(): void {
    manualWakeupsForTesting = true;
  },
  failNextAfterEnqueueStore(): void {
    failNextAfterEnqueueStoreForTesting = true;
  },
  retryableProcessorError(code: "r2-transient"): Error {
    return new NativeProcessorError(code, "Host Git processor reported a retryable R2 read.", true);
  },
  processorError(
    code: "r2-transient" | "stock-receive-rejected" | "stock-plan-wrong-range"
  ): Error {
    return new NativeProcessorError(
      code,
      code === "r2-transient"
        ? "Host Git processor reported a retryable R2 read."
        : code === "stock-plan-wrong-range"
          ? "Stock receive planner rejected an injected wrong range."
          : "Host Git processor rejected the stock receive.",
      code === "r2-transient"
    );
  },
  reset(): void {
    nativeProcessorForTesting = undefined;
    pauseNextBeforeFinalizationForTesting = false;
    manualWakeupsForTesting = false;
    failNextAfterEnqueueStoreForTesting = false;
  },
};

export class NativeProcessorError extends Error {
  readonly retryable: boolean;
  readonly code: string;

  constructor(code: string, message: string, retryable: boolean) {
    super(message);
    this.name = "NativeProcessorError";
    this.retryable = retryable;
    this.code = code;
  }
}

function repositoryContainer(ctx: DurableObjectState): Container {
  if (!ctx.container) {
    throw new NativeProcessorError(
      "container_unavailable",
      "Repository Container binding is unavailable.",
      true
    );
  }
  return ctx.container;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function scheduleNativeWake(
  ctx: DurableObjectState,
  env: Env,
  scheduledTimeMs: number
): Promise<void> {
  if (manualWakeupsForTesting) return;
  await scheduleAlarmIfSooner(ctx, env, scheduledTimeMs);
}

function operationFingerprintMatches(
  existing: NativeReceiveOperation,
  input: NativeReceiveOperation
): boolean {
  return existing.fingerprint === input.fingerprint;
}

function isSortedUnique(values: string[]): boolean {
  return values.every((value, index) => index === 0 || values[index - 1]! < value);
}

function equalStrings(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function stockRangeKey(range: {
  packChecksum: string;
  start: number;
  end: number;
  requiredOid: string;
}): string {
  return `${range.packChecksum}:${range.start}:${range.end}:${range.requiredOid}`;
}

function validateStockProcessorProof(
  operation: NativeReceiveOperation,
  result: NativeReceiveProcessResult
): boolean {
  const proof = result.closureProof;
  const semantic = result.semanticExternalOids;
  const thin = result.thinDeltaBaseOids;
  const required = result.requiredRootOids;
  const ranges = result.ranges;
  const activePackReads = result.activePackReads;
  if (
    !proof ||
    !semantic ||
    !thin ||
    !required ||
    !ranges ||
    !activePackReads ||
    !result.planSha256 ||
    result.planSha256 !== result.closureManifestSha256 ||
    proof.planSha256 !== result.planSha256 ||
    !result.closureManifestKey ||
    !result.closureManifestBytes ||
    !result.closureManifestEtag ||
    !result.prerequisitePackKey ||
    !result.prerequisitePackBytes ||
    !result.prerequisitePackSha256 ||
    !result.prerequisitePackEtag ||
    result.quarantinePathNonEmpty !== true ||
    !isSortedUnique(proof.incomingOids) ||
    !isSortedUnique(semantic) ||
    !isSortedUnique(thin) ||
    !isSortedUnique(required) ||
    !equalStrings(proof.semanticExternalOids, semantic) ||
    proof.visitedIncomingObjectCount !== result.visitedIncomingObjectCount ||
    proof.visitedIncomingObjectCount !== result.incomingObjectCount ||
    proof.incomingOids.length !== result.incomingObjectCount ||
    result.inputPackObjectCount !== result.incomingObjectCount ||
    result.objectCount !== result.incomingObjectCount + thin.length ||
    proof.logicalEdgeCount !== result.logicalEdgeCount ||
    proof.internalEdgeCount !== result.internalEdgeCount ||
    proof.externalEdgeCount !== result.externalEdgeCount ||
    proof.internalEdgeCount + proof.externalEdgeCount !== proof.logicalEdgeCount ||
    proof.missingObjectCount !== 0 ||
    result.missingObjectCount !== 0 ||
    JSON.stringify(proof.objectTypeCounts) !== JSON.stringify(result.objectTypeCounts) ||
    Object.values(proof.objectTypeCounts).reduce((total, count) => total + count, 0) !==
      proof.visitedIncomingObjectCount ||
    result.activePackCount !== operation.activeCatalog.length
  ) {
    return false;
  }
  const union = [...new Set([...semantic, ...thin])].sort();
  if (!equalStrings(union, required) || ranges.length !== required.length) return false;
  const rangeOids = ranges.map((range) => range.requiredOid).sort();
  if (!equalStrings(rangeOids, required)) return false;
  if (
    ranges.some((range) => range.end <= range.start) ||
    ranges.reduce((total, range) => total + range.end - range.start, 0) !== result.rangeBytes ||
    result.rangeRequests !== ranges.length ||
    result.packsTouched !== new Set(ranges.map((range) => range.packChecksum)).size
  ) {
    return false;
  }
  const trailerReads = activePackReads.filter((read) => read.kind === "trailer");
  const rangeReads = activePackReads.filter((read) => read.kind === "required-object");
  const observedRangeKeys = rangeReads.map(stockRangeKey).sort();
  const plannedRangeKeys = ranges.map(stockRangeKey).sort();
  if (
    activePackReads.length !== trailerReads.length + rangeReads.length ||
    trailerReads.length !== operation.activeCatalog.length ||
    new Set(trailerReads.map((read) => read.packChecksum)).size !== trailerReads.length ||
    trailerReads.some((read) => read.end - read.start !== 20 || read.returnedBytes !== 20) ||
    result.activePackTrailerRequests !== trailerReads.length ||
    result.activePackTrailerBytes !== trailerReads.length * 20 ||
    result.activePackRangeRequests !== rangeReads.length ||
    result.activePackRangeRequests !== result.rangeRequests ||
    result.activePackRangeBytes !== result.rangeBytes ||
    rangeReads.some((read) => read.end - read.start !== read.returnedBytes) ||
    !equalStrings(observedRangeKeys, plannedRangeKeys) ||
    result.activePackWholeBytes !== 0 ||
    result.activePackWholeRequests !== 0 ||
    result.activePackUnattributedBytes !== 0 ||
    result.activePackUnattributedRequests !== 0
  ) {
    return false;
  }
  return true;
}

async function sha256Bytes(bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

async function putImmutableAuthorityObject(args: {
  env: Env;
  limiter: SubrequestLimiter;
  key: string;
  bytes: Uint8Array;
  sha256: string;
}): Promise<string> {
  const created = await args.limiter.run("r2:put-native-authority", () =>
    args.env.REPO_BUCKET.put(args.key, args.bytes, {
      onlyIf: { etagDoesNotMatch: "*" },
      httpMetadata: { contentType: "application/json" },
      customMetadata: { sha256: args.sha256 },
    })
  );
  if (created) return created.etag;
  const existing = await args.limiter.run("r2:get-native-authority", () =>
    args.env.REPO_BUCKET.get(args.key)
  );
  if (
    !existing ||
    existing.size !== args.bytes.byteLength ||
    existing.customMetadata?.sha256 !== args.sha256
  ) {
    throw new Error("native authority object conflicts with immutable publication");
  }
  const actual = new Uint8Array(await existing.arrayBuffer());
  if ((await sha256Bytes(actual)) !== args.sha256) {
    throw new Error("native authority object digest mismatch");
  }
  return existing.etag;
}

async function publishStockAuthority(args: {
  env: Env;
  operation: NativeReceiveOperation;
  processorResult: NativeReceiveProcessResult;
}): Promise<NativeReceiveAuthorityPublication> {
  if (
    args.operation.commands.length !== 1 ||
    args.operation.commands[0]!.newOid === "0".repeat(40)
  ) {
    throw new Error("stock authority publication requires one non-delete ref transition");
  }
  const command = args.operation.commands[0]!;
  if (
    args.processorResult.outputValidationBytes === undefined ||
    args.processorResult.outputValidationRequests === undefined ||
    !args.processorResult.outputPackEtag ||
    !args.processorResult.outputIdxEtag ||
    !args.processorResult.outputRefsEtag
  ) {
    throw new Error("stock authority publication requires durable output validation proof");
  }
  const limiter = new SubrequestLimiter(3);
  const refBytes = new TextEncoder().encode(
    JSON.stringify({
      schemaVersion: 1,
      kind: "authoritative-ref",
      name: command.ref,
      oid: command.newOid,
    })
  );
  const refSha256 = await sha256Bytes(refBytes);
  const refKey = nativeReceiveAuthorityRefKey(
    args.operation.outputPackKey,
    args.operation.id,
    args.operation.fingerprint,
    0
  );
  const refEtag = await putImmutableAuthorityObject({
    env: args.env,
    limiter,
    key: refKey,
    bytes: refBytes,
    sha256: refSha256,
  });
  const receiptDigest = await sha256Bytes(
    new TextEncoder().encode(
      JSON.stringify({
        operationId: args.operation.id,
        fingerprint: args.operation.fingerprint,
        refName: command.ref,
        oldOid: command.oldOid,
        newOid: command.newOid,
        packSha256: args.processorResult.packSha256,
        idxSha256: args.processorResult.idxSha256,
        refsSha256: args.processorResult.refsSha256,
        planSha256: args.processorResult.planSha256,
        outputValidationBytes: args.processorResult.outputValidationBytes,
        outputValidationRequests: args.processorResult.outputValidationRequests,
        outputPackEtag: args.processorResult.outputPackEtag,
        outputIdxEtag: args.processorResult.outputIdxEtag,
        outputRefsEtag: args.processorResult.outputRefsEtag,
      })
    )
  );
  const receiptBytes = new TextEncoder().encode(
    JSON.stringify({
      schemaVersion: 1,
      kind: "operation-receipt",
      disposition: "committed",
      refName: command.ref,
      newOid: command.newOid,
      digest: receiptDigest,
    })
  );
  const receiptSha256 = await sha256Bytes(receiptBytes);
  const receiptKey = nativeReceiveAuthorityReceiptKey(
    args.operation.outputPackKey,
    args.operation.id,
    args.operation.fingerprint
  );
  const receiptEtag = await putImmutableAuthorityObject({
    env: args.env,
    limiter,
    key: receiptKey,
    bytes: receiptBytes,
    sha256: receiptSha256,
  });
  return {
    refs: [
      {
        name: command.ref,
        oid: command.newOid,
        key: refKey,
        bytes: refBytes.byteLength,
        sha256: refSha256,
        etag: refEtag,
      },
    ],
    receipt: {
      disposition: "committed",
      refName: command.ref,
      newOid: command.newOid,
      digest: receiptDigest,
      key: receiptKey,
      bytes: receiptBytes.byteLength,
      sha256: receiptSha256,
      etag: receiptEtag,
    },
  };
}

function withEvidenceEvents(
  operation: NativeReceiveOperation,
  additions: Array<Omit<NativeReceiveEvidenceEvent, "sequence">>
): NativeReceiveOperation {
  let events = operation.events ?? [];
  for (const addition of additions) {
    if (events.some((event) => event.phase === addition.phase)) continue;
    if (events.length >= MAX_EVIDENCE_EVENTS) {
      throw new Error("native receive evidence event bound exceeded");
    }
    events = [
      ...events,
      {
        ...addition,
        sequence: (events.at(-1)?.sequence ?? 0) + 1,
      },
    ];
  }
  return events === operation.events ? operation : { ...operation, events };
}

async function appendClaimedEvidenceEvents(args: {
  ctx: DurableObjectState;
  operationId: string;
  claimId: string;
  events: Array<Omit<NativeReceiveEvidenceEvent, "sequence">>;
}): Promise<NativeReceiveOperation> {
  return await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const current = await store.get(nativeReceiveOperationKey(args.operationId));
    if (!current || current.claimId !== args.claimId) {
      throw new Error("native receive operation claim was lost while recording evidence");
    }
    const updated = withEvidenceEvents(current, args.events);
    if (updated !== current) await store.put(nativeReceiveOperationKey(args.operationId), updated);
    return updated;
  });
}

async function findOldestTerminalOperationIndex(
  store: TypedStorage<RepoStateSchema>,
  operationIds: string[]
): Promise<number> {
  for (let index = 0; index < operationIds.length; index++) {
    const operationId = operationIds[index];
    if (!operationId) continue;
    const operation = await store.get(nativeReceiveOperationKey(operationId));
    if (operation && isNativeReceiveTerminal(operation.state) && !operation.cleanupPending) {
      return index;
    }
  }
  return -1;
}

export async function getNativeReceiveOperationState(
  ctx: DurableObjectState,
  operationId: string
): Promise<NativeReceiveOperation | null> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  return (await store.get(nativeReceiveOperationKey(operationId))) ?? null;
}

export async function recordNativeReceiveClientAckState(
  ctx: DurableObjectState,
  operationId: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const operation = await store.get(nativeReceiveOperationKey(operationId));
    if (
      !operation ||
      operation.state !== "committed" ||
      !operation.stockReceive ||
      !operation.result?.receivePackResponse
    ) {
      return false;
    }
    if (operation.clientAckReadyAt !== undefined) return true;
    const acknowledgedAt = Date.now();
    const acknowledged = withEvidenceEvents(operation, [
      { phase: "worker-response-ack", at: acknowledgedAt },
    ]);
    await store.put(nativeReceiveOperationKey(operationId), {
      ...acknowledged,
      clientAckReadyAt: acknowledgedAt,
      updatedAt: acknowledgedAt,
    });
    return true;
  });
}

export async function matchNativeReceiveOperationState(
  ctx: DurableObjectState,
  operationId: string,
  fingerprint: string
): Promise<MatchNativeReceiveOperationResult> {
  const operation = await getNativeReceiveOperationState(ctx, operationId);
  if (!operation) return { status: "not_found" };
  if (operation.fingerprint !== fingerprint) return { status: "conflict" };
  return { status: "match", operation: nativeReceiveOperationView(operation) };
}

export async function enqueueNativeReceiveState(args: {
  ctx: DurableObjectState;
  env: Env;
  operation: NativeReceiveOperation;
  logger: Logger;
}): Promise<EnqueueNativeReceiveResult> {
  if (args.operation.stockReceive) {
    return {
      status: "dispatch_failed",
      message: "Stock receive requires tagged admission.",
    };
  }
  const result = await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) {
      return {
        status: "repository_deleting",
        message: "Repository deletion is in progress.",
      } satisfies EnqueueNativeReceiveResult;
    }

    const stagedOperation = args.operation.stockReceive
      ? withEvidenceEvents(args.operation, [{ phase: "repo-do-operation-staged", durable: true }])
      : args.operation;
    const existing = await store.get(nativeReceiveOperationKey(args.operation.id));
    if (existing) {
      if (!operationFingerprintMatches(existing, args.operation)) {
        return {
          status: "conflict",
          message: "Operation id is already bound to a different receive.",
        } satisfies EnqueueNativeReceiveResult;
      }
      if (
        existing.stockReceive &&
        existing.state === "failed" &&
        existing.errorCode === "r2-transient" &&
        !existing.cleanupPending
      ) {
        await store.put(nativeReceiveOperationKey(args.operation.id), stagedOperation);
        return {
          status: "queued",
          operation: nativeReceiveOperationView(stagedOperation),
        } satisfies EnqueueNativeReceiveResult;
      }
      return {
        status: "replayed",
        operation: nativeReceiveOperationView(existing),
      } satisfies EnqueueNativeReceiveResult;
    }

    const lease = await store.get("receiveLease");
    if (!lease || lease.token !== args.operation.leaseToken || lease.expiresAt <= Date.now()) {
      return {
        status: "lease_mismatch",
        message: "Receive lease is no longer active.",
      } satisfies EnqueueNativeReceiveResult;
    }

    const index = (await store.get("nativeReceiveOperationIndex")) ?? [];
    const nextIndex = [...index.filter((id) => id !== args.operation.id), args.operation.id];
    while (nextIndex.length > MAX_RETAINED_OPERATIONS) {
      const removableIndex = await findOldestTerminalOperationIndex(store, nextIndex);
      if (removableIndex < 0) {
        return {
          status: "conflict",
          message: "Native receive operation ledger is full.",
        } satisfies EnqueueNativeReceiveResult;
      }
      const [removedId] = nextIndex.splice(removableIndex, 1);
      if (removedId) await store.delete(nativeReceiveOperationKey(removedId));
    }
    await store.put(nativeReceiveOperationKey(args.operation.id), stagedOperation);
    await store.put("nativeReceiveOperationIndex", nextIndex);
    return {
      status: "queued",
      operation: nativeReceiveOperationView(stagedOperation),
    } satisfies EnqueueNativeReceiveResult;
  });

  if (failNextAfterEnqueueStoreForTesting) {
    failNextAfterEnqueueStoreForTesting = false;
    throw new Error("injected lost enqueue response");
  }

  if (manualWakeupsForTesting) {
    return result;
  }
  if (
    (result.status === "queued" || result.status === "replayed") &&
    !isNativeReceiveTerminal(result.operation.state)
  ) {
    const dispatchAt = Date.now();
    const alarm = await scheduleAlarmIfSooner(args.ctx, args.env, dispatchAt);
    const alarmViable =
      alarm.scheduled || (alarm.prev !== null && alarm.prev <= dispatchAt + 1_000);
    let queueViable = false;
    try {
      await args.env.REPO_TASKS_QUEUE.send({
        kind: "native-receive",
        doId: args.ctx.id.toString(),
        operationId: args.operation.id,
      });
      queueViable = true;
    } catch (error) {
      args.logger.warn("native-receive:queue-dispatch-failed", {
        operationId: args.operation.id,
        error: String(error),
      });
    }
    if (!alarmViable && !queueViable) {
      const durableOperation = await args.ctx.storage.transaction(async (transaction) => {
        const store = asTypedStorage<RepoStateSchema>(transaction);
        const current = await store.get(nativeReceiveOperationKey(args.operation.id));
        if (
          current?.fingerprint === args.operation.fingerprint &&
          current.state === "staged" &&
          !current.claimId
        ) {
          await store.delete(nativeReceiveOperationKey(args.operation.id));
          const index = (await store.get("nativeReceiveOperationIndex")) ?? [];
          await store.put(
            "nativeReceiveOperationIndex",
            index.filter((id) => id !== args.operation.id)
          );
          return null;
        }
        return current ? nativeReceiveOperationView(current) : null;
      });
      if (durableOperation) {
        return { status: "replayed", operation: durableOperation };
      }
      return {
        status: "dispatch_failed",
        message: "Native receive was staged but durable dispatch could not be scheduled.",
      };
    }
    args.logger.info("native-receive:queued", {
      operationId: args.operation.id,
      inputBytes: args.operation.inputBytes,
      activePackCount: args.operation.activeCatalog.length,
    });
  }
  return result;
}

type ClaimOperationAttemptResult =
  | { status: "claimed"; operation: NativeReceiveOperation; claimId: string }
  | { status: "current"; operation: NativeReceiveOperation }
  | { status: "missing" };

async function claimOperationAttempt(
  ctx: DurableObjectState,
  operationId: string
): Promise<ClaimOperationAttemptResult> {
  const claimId = crypto.randomUUID();
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return { status: "missing" };
    const operation = await store.get(nativeReceiveOperationKey(operationId));
    if (!operation) return { status: "missing" };
    if (isNativeReceiveTerminal(operation.state)) return { status: "current", operation };
    const now = Date.now();
    if (
      operation.claimId &&
      operation.claimExpiresAt !== undefined &&
      operation.claimExpiresAt > now
    ) {
      return { status: "current", operation };
    }
    const lease = await store.get("receiveLease");
    if (!lease || lease.token !== operation.leaseToken) {
      const intent = await store.get(receiveFinalizeIntentKey(operation.leaseToken));
      if (operation.state !== "ready" && operation.state !== "finalizing" && !intent) {
        return { status: "current", operation };
      }
    }

    const claimed: NativeReceiveOperation = {
      ...operation,
      state:
        operation.state === "ready" || operation.state === "finalizing"
          ? "finalizing"
          : "processing",
      attempts:
        operation.state === "ready" || operation.state === "finalizing"
          ? operation.attempts
          : operation.attempts + 1,
      updatedAt: now,
      errorCode: undefined,
      claimId,
      claimExpiresAt: now + PROCESSING_CLAIM_TTL_MS,
    };
    await store.put("receiveLease", {
      token: operation.leaseToken,
      createdAt: lease?.token === operation.leaseToken ? lease.createdAt : now,
      expiresAt: now + RECEIVE_LEASE_TTL_MS,
    });
    await store.put(nativeReceiveOperationKey(operationId), claimed);
    return { status: "claimed", operation: claimed, claimId };
  });
}

async function renewOperationLease(
  ctx: DurableObjectState,
  operation: NativeReceiveOperation,
  claimId: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    const lease = await store.get("receiveLease");
    if (!lease || lease.token !== operation.leaseToken) return false;
    const current = await store.get(nativeReceiveOperationKey(operation.id));
    if (!current || current.claimId !== claimId) return false;
    const now = Date.now();
    await store.put("receiveLease", {
      ...lease,
      expiresAt: now + RECEIVE_LEASE_TTL_MS,
    });
    await store.put(nativeReceiveOperationKey(operation.id), {
      ...current,
      claimExpiresAt: now + PROCESSING_CLAIM_TTL_MS,
    });
    return true;
  });
}

async function storeClaimedOperation(
  ctx: DurableObjectState,
  operation: NativeReceiveOperation,
  claimId: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const current = await store.get(nativeReceiveOperationKey(operation.id));
    if (!current || current.claimId !== claimId) return false;
    await store.put(nativeReceiveOperationKey(operation.id), operation);
    return true;
  });
}

async function releaseOperationClaim(
  ctx: DurableObjectState,
  operation: NativeReceiveOperation,
  claimId: string
): Promise<NativeReceiveOperation> {
  const released: NativeReceiveOperation = {
    ...operation,
    claimId: undefined,
    claimExpiresAt: undefined,
    updatedAt: Date.now(),
  };
  if (await storeClaimedOperation(ctx, released, claimId)) return released;
  return (await getNativeReceiveOperationState(ctx, operation.id)) ?? released;
}

async function acquireNativeCatalogReaderLease(
  ctx: DurableObjectState,
  operation: NativeReceiveOperation
): Promise<"acquired" | "catalog_superseded" | "reader_busy" | "repository_deleting"> {
  const now = Date.now();
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return "repository_deleting";
    const generationFloor = await store.get("nativeCatalogReaderGenerationFloor");
    if (typeof generationFloor === "number" && operation.catalogGeneration < generationFloor) {
      return "catalog_superseded";
    }
    const existing = await store.get("nativeCatalogReaderLease");
    if (existing && existing.expiresAt > now && existing.token !== operation.id) {
      return "reader_busy";
    }
    await store.put("nativeCatalogReaderLease", {
      token: operation.id,
      createdAt: existing?.token === operation.id ? existing.createdAt : now,
      expiresAt: now + NATIVE_READER_LEASE_TTL_MS,
      operation: "native-reader",
      generation: operation.catalogGeneration,
    });
    return "acquired";
  });
}

async function renewNativeCatalogReaderLease(
  ctx: DurableObjectState,
  operation: NativeReceiveOperation
): Promise<boolean> {
  const now = Date.now();
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    const existing = await store.get("nativeCatalogReaderLease");
    if (
      !existing ||
      existing.token !== operation.id ||
      existing.generation !== operation.catalogGeneration ||
      existing.expiresAt <= now
    ) {
      return false;
    }
    await store.put("nativeCatalogReaderLease", {
      ...existing,
      expiresAt: now + NATIVE_READER_LEASE_TTL_MS,
    });
    return true;
  });
}

async function releaseNativeCatalogReaderLease(
  ctx: DurableObjectState,
  operationId: string
): Promise<void> {
  await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const existing = await store.get("nativeCatalogReaderLease");
    if (existing?.token === operationId) await store.delete("nativeCatalogReaderLease");
  });
}

export async function canDeleteSupersededGenerationState(
  ctx: DurableObjectState,
  generation?: number
): Promise<{ safe: boolean; retryAfterSeconds?: number }> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const lease = await store.get("nativeCatalogReaderLease");
    const now = Date.now();
    const repositoryReaders = ((await store.get("repositoryReadLeases")) ?? []).filter(
      (reader) => reader.expiresAt > now
    );
    if (repositoryReaders.length > 0) {
      await store.put("repositoryReadLeases", repositoryReaders);
      return {
        safe: false,
        retryAfterSeconds: Math.max(
          1,
          Math.ceil((Math.max(...repositoryReaders.map((reader) => reader.expiresAt)) - now) / 1000)
        ),
      };
    }
    await store.delete("repositoryReadLeases");
    if (
      lease &&
      lease.expiresAt > now &&
      (typeof generation !== "number" || lease.generation < generation)
    ) {
      return {
        safe: false,
        retryAfterSeconds: Math.max(1, Math.ceil((lease.expiresAt - now) / 1000)),
      };
    }
    if (typeof generation === "number") {
      const floor = await store.get("nativeCatalogReaderGenerationFloor");
      if (floor === undefined || floor < generation) {
        await store.put("nativeCatalogReaderGenerationFloor", generation);
      }
    }
    return { safe: true };
  });
}

function bridgeProps(operation: NativeReceiveOperation): RepositoryContainerBridgeProps {
  const activeBytes = operation.activeCatalog.reduce((total, pack) => total + pack.packBytes, 0);
  const maximumOutputPackBytes = operation.stockReceive
    ? Math.min(STOCK_OUTPUT_PACK_MAX_BYTES, operation.inputBytes + 16 * 1024 * 1024)
    : operation.inputBytes + activeBytes;
  const maximumSidecarBytes = operation.stockReceive
    ? STOCK_OUTPUT_SIDECAR_MAX_BYTES
    : OUTPUT_SIDECAR_MAX_BYTES;
  return {
    operationId: operation.id,
    readKeys: [
      {
        key: operation.inputPackKey,
        expectedBytes: operation.inputBytes,
        expectedEtag: operation.inputEtag,
      },
      ...(operation.stockReceive
        ? []
        : operation.activeCatalog.flatMap((pack) => [
            { key: pack.packKey, expectedBytes: pack.packBytes },
            { key: packIndexKey(pack.packKey), expectedBytes: pack.idxBytes },
          ])),
    ],
    writeKeys: [
      { key: operation.outputPackKey, maxBytes: maximumOutputPackBytes },
      { key: operation.outputIdxKey, maxBytes: maximumSidecarBytes },
      { key: operation.outputRefsKey, maxBytes: maximumSidecarBytes },
    ],
    requireWriteSha256: operation.stockReceive !== undefined,
  };
}

function processRequest(operation: NativeReceiveOperation): NativeReceiveProcessRequest {
  return {
    operationId: operation.id,
    inputPackKey: operation.inputPackKey,
    inputBytes: operation.inputBytes,
    activePacks: operation.activeCatalog.map((pack) => ({
      packKey: pack.packKey,
      packBytes: pack.packBytes,
      idxBytes: pack.idxBytes,
    })),
    commands: operation.commands,
    outputPackKey: operation.outputPackKey,
    outputIdxKey: operation.outputIdxKey,
    outputRefsKey: operation.outputRefsKey,
    stockReceive: operation.stockReceive,
  };
}

async function waitForContainerReady(container: Container): Promise<void> {
  const port = container.getTcpPort(CONTAINER_PORT);
  for (let attempt = 0; attempt < CONTAINER_READY_ATTEMPTS; attempt++) {
    try {
      const response = await port.fetch("http://container/ready", { method: "GET" });
      if (response.status === 200) {
        await response.body?.cancel();
        return;
      }
      await response.body?.cancel();
    } catch {}
    await delay(CONTAINER_READY_INTERVAL_MS);
  }
  throw new NativeProcessorError(
    "container_not_ready",
    "Repository Container did not become ready.",
    true
  );
}

async function parseBoundedJSON(response: Response): Promise<unknown> {
  const contentLength = Number(response.headers.get("Content-Length"));
  if (Number.isFinite(contentLength) && contentLength > CONTAINER_RESPONSE_MAX_BYTES) {
    throw new NativeProcessorError(
      "container_response_too_large",
      "Repository Container response exceeded its bound.",
      false
    );
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > CONTAINER_RESPONSE_MAX_BYTES) {
    throw new NativeProcessorError(
      "container_response_too_large",
      "Repository Container response exceeded its bound.",
      false
    );
  }
  try {
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new NativeProcessorError(
      "container_invalid_response",
      "Repository Container returned invalid JSON.",
      false
    );
  }
}

export async function runContainerProcessor(args: {
  ctx: DurableObjectState;
  execution: NativeExecutionIdentity;
  request: NativeReceiveProcessRequest;
  bridgeProps: RepositoryContainerBridgeProps;
  onReady?: (wasRunning: boolean) => Promise<void>;
}): Promise<NativeReceiveProcessResult> {
  if (args.request.stockReceive && !nativeProcessorForTesting) {
    throw new NativeProcessorError(
      "host_stock_adapter_required",
      "Stock receive requires the host Git adapter.",
      false
    );
  }
  const wasRunning = args.ctx.container?.running ?? false;
  const slotSignal = await startNativeProcessorSlot(args.ctx, args.execution, async () => {
    if (nativeProcessorForTesting) return;
    const container = repositoryContainer(args.ctx);
    const bridge = args.ctx.exports.RepositoryContainerBridge({
      props: { ...args.bridgeProps, execution: args.execution },
    });
    await container.interceptOutboundHttp("repo-r2.internal", bridge);
    if (!container.running) container.start({ enableInternet: false });
  });
  if (!slotSignal)
    throw new NativeProcessorError(
      "native_execution_busy",
      "Native execution slot is already owned.",
      true
    );
  const signal = AbortSignal.any([slotSignal, AbortSignal.timeout(CONTAINER_PROCESS_TIMEOUT_MS)]);
  if (nativeProcessorForTesting) {
    try {
      await args.onReady?.(false);
      const result = await nativeProcessorForTesting({ ...args, signal });
      const parsed = nativeReceiveProcessResultSchema.safeParse(result);
      if (!parsed.success || parsed.data.operationId !== args.request.operationId) {
        throw new NativeProcessorError(
          "host_processor_invalid_response",
          "Host Git processor returned an invalid result.",
          false
        );
      }
      return parsed.data;
    } finally {
      await finishNativeProcessorSlot(args.ctx, args.execution);
    }
  }

  const container = repositoryContainer(args.ctx);

  try {
    await waitForContainerReady(container);
    await args.onReady?.(wasRunning);
    const response = await container.getTcpPort(CONTAINER_PORT).fetch("http://container/process", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args.request),
      signal,
    });
    const payload = await parseBoundedJSON(response);
    if (response.status !== 200) {
      throw new NativeProcessorError(
        response.status >= 500 ? "container_transient_failure" : "native_git_rejected",
        "Repository Container rejected the receive.",
        response.status >= 500
      );
    }
    const parsed = nativeReceiveProcessResultSchema.safeParse(payload);
    if (!parsed.success || parsed.data.operationId !== args.request.operationId) {
      throw new NativeProcessorError(
        "container_invalid_response",
        "Repository Container returned an invalid result.",
        false
      );
    }
    return parsed.data;
  } catch (error) {
    if (error instanceof NativeProcessorError) throw error;
    await stopNativeProcessorSlot(args.ctx, args.execution);
    throw new NativeProcessorError(
      "container_transport_failure",
      "Repository Container request failed.",
      true
    );
  } finally {
    await finishNativeProcessorSlot(args.ctx, args.execution);
  }
}

export async function cancelNativeExecution(
  ctx: DurableObjectState,
  env: Env,
  identity: NativeExecutionIdentity
): Promise<void> {
  await finishNativeExecution(ctx, identity, "revoked");
  if (identity.lane === "foreground") await stopNativeProcessorSlot(ctx, identity);
  else
    await new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS).run("do:maintenance-cancel", () =>
      env.MAINTENANCE_CONTAINER_HOST.getByName(ctx.id.toString()).cancel(identity)
    );
  await acknowledgeNativeStop(ctx, identity);
}

export async function reconcileNativeExecutionLeases(
  ctx: DurableObjectState,
  env: Env,
  lane: NativeExecutionLane
): Promise<void> {
  const record = await ctx.storage.get<NativeExecutionRecord>(nativeExecutionKey(lane));
  if (
    record &&
    (record.stopPending ||
      (record.state === "active" && !(await authorizeNativeExecution(ctx, record.identity))))
  )
    await cancelNativeExecution(ctx, env, record.identity);
}

export async function runNativeExecution(args: {
  ctx: DurableObjectState;
  env: Env;
  lane: NativeExecutionLane;
  claimId: string;
  request: NativeReceiveProcessRequest;
  bridgeProps: RepositoryContainerBridgeProps;
  onReady?: (wasRunning: boolean) => Promise<void>;
}): Promise<NativeReceiveProcessResult> {
  const identity = await beginNativeExecution(
    args.ctx,
    args.lane,
    args.request.operationId,
    args.claimId,
    args.bridgeProps
  );
  if (!identity)
    throw new NativeProcessorError(
      "native_execution_busy",
      "Native execution is unavailable.",
      true
    );
  const onReady = async (wasRunning: boolean) => {
    if (!(await authorizeNativeExecution(args.ctx, identity)))
      throw new Error("Native execution authority expired");
    await args.onReady?.(wasRunning);
  };
  try {
    let result: NativeReceiveProcessResult;
    if (args.lane === "foreground") {
      result = await runContainerProcessor({ ...args, execution: identity, onReady });
    } else {
      const outcome = await new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS).run(
        "do:maintenance-process",
        async () =>
          await args.env.MAINTENANCE_CONTAINER_HOST.getByName(args.ctx.id.toString()).process(
            args.request,
            args.bridgeProps,
            identity,
            onReady
          )
      );
      if (outcome.status === "failed")
        throw new NativeProcessorError(
          outcome.code,
          "Maintenance native execution failed.",
          outcome.retryable
        );
      result = outcome.result;
    }
    // Settlement and authority validation share one transaction. A revocation
    // that wins this transition must never return output for publication.
    if (!(await finishNativeExecution(args.ctx, identity, "completed")))
      throw new Error("Native execution authority expired");
    return result;
  } catch (error) {
    if (
      error instanceof NativeProcessorError &&
      !["container_transport_failure", "native_execution_busy"].includes(error.code)
    ) {
      await finishNativeExecution(args.ctx, identity, "completed");
    } else {
      await cancelNativeExecution(args.ctx, args.env, identity);
    }
    throw error;
  }
}

export async function stopNativeReceiveContainerState(
  ctx: DurableObjectState,
  env: Env
): Promise<void> {
  // Repository deletion is deliberately distinct from cancelling one job.
  const executions: NativeExecutionIdentity[] = [];
  for (const lane of ["foreground", "maintenance"] as const) {
    const record = await ctx.storage.get<NativeExecutionRecord>(nativeExecutionKey(lane));
    if (record) executions.push(record.identity);
    if (record?.state === "active") await finishNativeExecution(ctx, record.identity, "revoked");
  }
  await deleteNativeProcessorSlot(ctx);
  await new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS).run("do:maintenance-delete", () =>
    env.MAINTENANCE_CONTAINER_HOST.getByName(ctx.id.toString()).deleteRepositoryExecution()
  );
  for (const identity of executions) await acknowledgeNativeStop(ctx, identity);
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  await store.delete("nativeCatalogReaderLease");
}

async function deleteOperationObjects(args: {
  env: Env;
  operation: NativeReceiveOperation;
  includeOutputs: boolean;
  logger: Logger;
}): Promise<void> {
  const baseKeys = args.includeOutputs
    ? [
        args.operation.inputPackKey,
        args.operation.outputPackKey,
        args.operation.outputIdxKey,
        args.operation.outputRefsKey,
      ]
    : [args.operation.inputPackKey];
  const keys = [
    ...baseKeys,
    args.operation.processorResult?.prerequisitePackKey,
    args.operation.processorResult?.closureManifestKey,
  ].filter((key, index, all): key is string => Boolean(key) && all.indexOf(key) === index);
  const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
  await limiter.run("r2:delete-native-receive-objects", () => args.env.REPO_BUCKET.delete(keys));
  args.logger.info("native-receive:cleanup", {
    operationId: args.operation.id,
    inputDeleted: true,
    outputsDeleted: args.includeOutputs,
  });
}

async function markTerminal(args: {
  ctx: DurableObjectState;
  operation: NativeReceiveOperation;
  claimId: string;
  state: "committed" | "aborted" | "failed";
  result?: NativeReceiveOperation["result"];
  errorCode?: string;
}): Promise<NativeReceiveOperation> {
  const terminal: NativeReceiveOperation = {
    ...args.operation,
    state: args.state,
    updatedAt: Date.now(),
    result: args.result,
    errorCode: args.errorCode,
    cleanupPending: true,
    claimId: undefined,
    claimExpiresAt: undefined,
  };
  if (!(await storeClaimedOperation(args.ctx, terminal, args.claimId))) {
    const current = await getNativeReceiveOperationState(args.ctx, args.operation.id);
    if (current && isNativeReceiveTerminal(current.state)) return current;
    throw new Error("FUBAR: native receive operation claim was lost before terminal state");
  }
  return terminal;
}

async function completeOperationCleanup(args: {
  ctx: DurableObjectState;
  env: Env;
  operation: NativeReceiveOperation;
  includeOutputs: boolean;
  logger: Logger;
}): Promise<NativeReceiveOperation> {
  try {
    await deleteOperationObjects(args);
    return await args.ctx.storage.transaction(async (transaction) => {
      const store = asTypedStorage<RepoStateSchema>(transaction);
      const current = await store.get(nativeReceiveOperationKey(args.operation.id));
      // An alarm and the completing request can finish the same cleanup. Never
      // replace newer terminal evidence with the caller's pre-cleanup snapshot.
      if (!current || !current.cleanupPending) return current ?? args.operation;
      const cleaned = { ...current, cleanupPending: false, updatedAt: Date.now() };
      await store.put(nativeReceiveOperationKey(current.id), cleaned);
      return cleaned;
    });
  } catch (error) {
    args.logger.warn("native-receive:cleanup-deferred", {
      operationId: args.operation.id,
      error: String(error),
    });
    await scheduleNativeWake(args.ctx, args.env, Date.now() + 1_000);
    return args.operation;
  }
}

async function reconcileFinalizingOperation(args: {
  ctx: DurableObjectState;
  env: Env;
  operation: NativeReceiveOperation;
  claimId: string;
  logger: Logger;
}): Promise<NativeReceiveOperation | null> {
  let operation = args.operation;
  const processorResult = operation.processorResult;
  if (!processorResult) return null;
  const onMilestone = operation.stockReceive
    ? async (milestone: ReceiveFinalizeMilestone): Promise<void> => {
        operation = await appendClaimedEvidenceEvents({
          ctx: args.ctx,
          operationId: operation.id,
          claimId: args.claimId,
          events: [milestone],
        });
      }
    : undefined;
  const finalized = await finalizeReceiveState({
    ctx: args.ctx,
    env: args.env,
    token: operation.leaseToken,
    commands: operation.commands,
    stagedPack: {
      packKey: operation.outputPackKey,
      packBytes: processorResult.packBytes,
      idxBytes: processorResult.idxBytes,
      objectCount: processorResult.objectCount,
      integrity: operation.stockReceive
        ? {
            packSha256: processorResult.packSha256!,
            idxSha256: processorResult.idxSha256!,
            refsSha256: processorResult.refsSha256!,
            refsBytes: processorResult.refsBytes,
          }
        : undefined,
    },
    acceptedWrites: operation.acceptedWrites.length > 0 ? operation.acceptedWrites : undefined,
    logger: args.logger,
    onMilestone,
  });
  if (finalized.status === "committed") {
    const committedProcessorResult: NativeReceiveProcessResult = {
      ...processorResult,
      outputValidationBytes: finalized.outputValidationBytes,
      outputValidationRequests: finalized.outputValidationRequests,
      outputPackEtag: finalized.outputEtags?.pack,
      outputIdxEtag: finalized.outputEtags?.idx,
      outputRefsEtag: finalized.outputEtags?.refs,
    };
    let committedOperation: NativeReceiveOperation = {
      ...operation,
      processorResult: committedProcessorResult,
    };
    const authorityPublication = operation.stockReceive
      ? await publishStockAuthority({
          env: args.env,
          operation: committedOperation,
          processorResult: committedProcessorResult,
        })
      : undefined;
    if (authorityPublication) {
      const operationWithReceipt = await appendClaimedEvidenceEvents({
        ctx: args.ctx,
        operationId: operation.id,
        claimId: args.claimId,
        events: [
          {
            phase: "receipt-committed",
            durable: true,
            digest: authorityPublication.receipt.digest,
          },
        ],
      });
      committedOperation = {
        ...operationWithReceipt,
        processorResult: committedProcessorResult,
      };
    }
    const committed = await markTerminal({
      ctx: args.ctx,
      operation: committedOperation,
      claimId: args.claimId,
      state: "committed",
      result: {
        statuses: finalized.statuses,
        refPublication: finalized.refPublication,
        changed: finalized.changed,
        empty: finalized.empty,
        packKey: operation.outputPackKey,
        packBytes: processorResult.packBytes,
        receivePackResponse: processorResult.receivePackResponse,
        stockTrace: processorResult.stockTrace,
        authorityPublication,
      },
    });
    return await completeOperationCleanup({
      ctx: args.ctx,
      env: args.env,
      operation: committed,
      includeOutputs: false,
      logger: args.logger,
    });
  }

  const aborted = await markTerminal({
    ctx: args.ctx,
    operation,
    claimId: args.claimId,
    state: "aborted",
    result: {
      statuses: finalized.status === "ref_conflict" ? finalized.statuses : [],
      changed: false,
      empty: false,
    },
    errorCode: finalized.status,
  });
  return await completeOperationCleanup({
    ctx: args.ctx,
    env: args.env,
    operation: aborted,
    includeOutputs: true,
    logger: args.logger,
  });
}

export async function runNativeReceiveOperationState(args: {
  ctx: DurableObjectState;
  env: Env;
  operationId: string;
  logger: Logger;
}): Promise<NativeReceiveOperationView | null> {
  const retained = await getNativeReceiveOperationState(args.ctx, args.operationId);
  if (retained && ["staged", "processing"].includes(retained.state)) {
    await reconcileNativeExecutionLeases(args.ctx, args.env, "foreground");
    const execution = await args.ctx.storage.get<NativeExecutionRecord>(
      nativeExecutionKey("foreground")
    );
    if (execution?.state === "active" || (execution?.drainUntil ?? 0) > Date.now()) {
      const retryAt =
        execution?.state === "active" ? execution.identity.expiresAt : execution!.drainUntil;
      await scheduleNativeWake(args.ctx, args.env, retryAt + 1);
      args.logger.info("native-execution:foreground-waiting", { operationId: args.operationId });
      return nativeReceiveOperationView(retained);
    }
  }
  if (retained?.stockReceive) {
    args.logger.warn("native-receive:legacy-stock-dispatch-rejected", {
      operationId: args.operationId,
    });
    return nativeReceiveOperationView(retained);
  }
  const claim = await claimOperationAttempt(args.ctx, args.operationId);
  if (claim.status === "missing") return null;
  if (claim.status === "current") {
    if (!isNativeReceiveTerminal(claim.operation.state) && claim.operation.claimExpiresAt) {
      await scheduleNativeWake(args.ctx, args.env, claim.operation.claimExpiresAt + 1);
    }
    return nativeReceiveOperationView(claim.operation);
  }
  let operation = claim.operation;
  const { claimId } = claim;

  if (operation.state === "finalizing") {
    try {
      const reconciled = await reconcileFinalizingOperation({ ...args, operation, claimId });
      if (reconciled) return nativeReceiveOperationView(reconciled);
    } catch (error) {
      if (error instanceof ReceiveOutputIntegrityError) {
        const failedProcessorResult = operation.processorResult
          ? {
              ...operation.processorResult,
              outputValidationBytes: error.bytes,
              outputValidationRequests: error.requests,
              outputIntegrityRejectedRole: error.role,
            }
          : undefined;
        if (operation.stockReceive) {
          operation = await appendClaimedEvidenceEvents({
            ctx: args.ctx,
            operationId: operation.id,
            claimId,
            events: [
              {
                phase: "output-integrity-rejected",
                bytes: error.bytes,
                detailCode: `${error.role}-digest-mismatch`,
              },
            ],
          });
        }
        if (failedProcessorResult) {
          operation = { ...operation, processorResult: failedProcessorResult };
        }
        const failed = await markTerminal({
          ctx: args.ctx,
          operation,
          claimId,
          state: "failed",
          errorCode: "stock_output_integrity_rejected",
        });
        await abortReceiveLease(args.ctx, operation.leaseToken);
        const cleaned = await completeOperationCleanup({
          ctx: args.ctx,
          env: args.env,
          operation: failed,
          includeOutputs: true,
          logger: args.logger,
        });
        return nativeReceiveOperationView(cleaned);
      }
      // The durable operation and finalize intent contain everything needed
      // for recovery. Keep ordinary logs closed over bounded state only.
    }
    const finalizeAttempts = (operation.finalizeAttempts ?? 0) + 1;
    const escalate =
      finalizeAttempts >= RECOVERY_ESCALATION_ATTEMPTS && !operation.finalizeEscalated;
    const released = await releaseOperationClaim(
      args.ctx,
      {
        ...operation,
        finalizeAttempts,
        finalizeEscalated: operation.finalizeEscalated || escalate,
      },
      claimId
    );
    const fields = { operationId: operation.id, attempts: finalizeAttempts, retryable: true };
    if (escalate) {
      args.logger.error("native-receive:finalization-recovery-escalated", fields);
    } else {
      args.logger.warn("native-receive:finalization-recovery-pending", fields);
    }
    await scheduleNativeWake(
      args.ctx,
      args.env,
      Date.now() + recoveryRetryDelayMs(finalizeAttempts)
    );
    return nativeReceiveOperationView(released);
  }

  args.logger.info("native-receive:processing", {
    operationId: operation.id,
    attempt: operation.attempts,
    inputBytes: operation.inputBytes,
    activePackCount: operation.activeCatalog.length,
  });

  let nativeResult: NativeReceiveProcessResult;
  const readerLeaseResult = await acquireNativeCatalogReaderLease(args.ctx, operation);
  if (readerLeaseResult === "catalog_superseded") {
    args.logger.warn("native-receive:catalog-superseded", {
      operationId: operation.id,
      catalogGeneration: operation.catalogGeneration,
      retryable: false,
    });
    const failed = await markTerminal({
      ctx: args.ctx,
      operation,
      claimId,
      state: "failed",
      errorCode: "catalog_superseded",
    });
    await abortReceiveLease(args.ctx, operation.leaseToken);
    const cleaned = await completeOperationCleanup({
      ctx: args.ctx,
      env: args.env,
      operation: failed,
      includeOutputs: true,
      logger: args.logger,
    });
    return nativeReceiveOperationView(cleaned);
  }
  if (readerLeaseResult !== "acquired") {
    args.logger.info("native-receive:reader-pending", {
      operationId: operation.id,
      reason: readerLeaseResult,
      retryable: true,
    });
    const released = await releaseOperationClaim(
      args.ctx,
      {
        ...operation,
        state: "staged",
        attempts: Math.max(0, operation.attempts - 1),
      },
      claimId
    );
    await scheduleNativeWake(args.ctx, args.env, Date.now() + 1_000);
    return nativeReceiveOperationView(released);
  }
  if (operation.stockReceive) {
    operation = await appendClaimedEvidenceEvents({
      ctx: args.ctx,
      operationId: operation.id,
      claimId,
      events: [{ phase: "go-processor-start" }],
    });
  }
  let heartbeatFailure: Error | undefined;
  let heartbeatTail = Promise.resolve();
  const heartbeat = setInterval(() => {
    heartbeatTail = heartbeatTail
      .then(async () => {
        const operationRenewed = await renewOperationLease(args.ctx, operation, claimId);
        const readerRenewed = await renewNativeCatalogReaderLease(args.ctx, operation);
        if (!operationRenewed || !readerRenewed) {
          heartbeatFailure = new Error("native receive lease ownership was lost");
          const execution = await args.ctx.storage.get<NativeExecutionRecord>(
            nativeExecutionKey("foreground")
          );
          if (
            execution?.identity.operationId === operation.id &&
            execution.identity.claimId === claimId
          )
            await cancelNativeExecution(args.ctx, args.env, execution.identity);
        }
      })
      .catch((error) => {
        heartbeatFailure = error instanceof Error ? error : new Error(String(error));
      });
  }, LEASE_HEARTBEAT_MS);
  try {
    nativeResult = await runNativeExecution({
      ctx: args.ctx,
      env: args.env,
      lane: "foreground",
      claimId,
      request: processRequest(operation),
      bridgeProps: bridgeProps(operation),
    });
    await heartbeatTail;
    if (heartbeatFailure) throw heartbeatFailure;
  } catch (error) {
    const processorError =
      error instanceof NativeProcessorError
        ? error
        : new NativeProcessorError("native_processor_failed", String(error), true);
    args.logger.warn("native-receive:processing-failed", {
      operationId: operation.id,
      attempt: operation.attempts,
      code: processorError.code,
      retryable: processorError.retryable,
    });
    if (
      operation.stockReceive &&
      processorError.retryable &&
      processorError.code === "r2-transient"
    ) {
      operation = await appendClaimedEvidenceEvents({
        ctx: args.ctx,
        operationId: operation.id,
        claimId,
        events: [
          {
            phase: "r2-read-retryable",
            detailCode: "r2-transient",
          },
        ],
      });
      const retryable = await markTerminal({
        ctx: args.ctx,
        operation,
        claimId,
        state: "failed",
        errorCode: "r2-transient",
      });
      await abortReceiveLease(args.ctx, operation.leaseToken);
      const cleaned = await completeOperationCleanup({
        ctx: args.ctx,
        env: args.env,
        operation: retryable,
        includeOutputs: true,
        logger: args.logger,
      });
      return nativeReceiveOperationView(cleaned);
    }
    if (operation.stockReceive && processorError.code === "stock-plan-wrong-range") {
      operation = await appendClaimedEvidenceEvents({
        ctx: args.ctx,
        operationId: operation.id,
        claimId,
        events: [
          {
            phase: "replacement-closure-rejected",
            detailCode: "wrong-prerequisite-range",
          },
        ],
      });
      const rejected = await markTerminal({
        ctx: args.ctx,
        operation,
        claimId,
        state: "failed",
        errorCode: "wrong-prerequisite-range",
      });
      await abortReceiveLease(args.ctx, operation.leaseToken);
      const cleaned = await completeOperationCleanup({
        ctx: args.ctx,
        env: args.env,
        operation: rejected,
        includeOutputs: true,
        logger: args.logger,
      });
      return nativeReceiveOperationView(cleaned);
    }
    if (processorError.retryable && operation.attempts < MAX_PROCESS_ATTEMPTS) {
      const retrying: NativeReceiveOperation = {
        ...operation,
        state: "staged",
        updatedAt: Date.now(),
        errorCode: processorError.code,
        claimId: undefined,
        claimExpiresAt: undefined,
      };
      if (!(await storeClaimedOperation(args.ctx, retrying, claimId))) {
        return nativeReceiveOperationView(
          (await getNativeReceiveOperationState(args.ctx, operation.id)) ?? retrying
        );
      }
      const retryDelay = Math.min(30_000, 2 ** (operation.attempts - 1) * 1_000);
      await scheduleNativeWake(args.ctx, args.env, Date.now() + retryDelay);
      return nativeReceiveOperationView(retrying);
    }

    const failed = await markTerminal({
      ctx: args.ctx,
      operation,
      claimId,
      state: "failed",
      errorCode: processorError.code,
    });
    await abortReceiveLease(args.ctx, operation.leaseToken);
    const cleaned = await completeOperationCleanup({
      ctx: args.ctx,
      env: args.env,
      operation: failed,
      includeOutputs: true,
      logger: args.logger,
    });
    return nativeReceiveOperationView(cleaned);
  } finally {
    clearInterval(heartbeat);
    await releaseNativeCatalogReaderLease(args.ctx, operation.id);
  }

  args.logger.info("native-receive:processing-complete", {
    operationId: operation.id,
    elapsedMs: nativeResult.elapsedMs,
    hydratedBytes: nativeResult.hydratedBytes,
    downloadedBytes: nativeResult.downloadedBytes,
    cacheHitBytes: nativeResult.cacheHitBytes,
  });

  if (operation.stockReceive) {
    const expectedTrace = [
      "receive_pack_invoked",
      "pre_receive_started",
      "pre_receive_quarantine_nonempty",
      "logical_closure_started_ref_still_old",
      "incoming_oid_visible_in_quarantine",
      "logical_closure_completed",
      "pre_receive_succeeded",
      "disposable_ref_update_observed",
    ];
    if (
      nativeResult.inputRequestSha256 !== operation.stockReceive.inputRequestSha256 ||
      !nativeResult.receivePackResponse ||
      !nativeResult.packSha256 ||
      !nativeResult.idxSha256 ||
      !nativeResult.refsSha256 ||
      nativeResult.quarantinePathInsideOwnedWorkRoot !== true ||
      nativeResult.quarantineRemovedAfterReceive !== true ||
      !validateStockProcessorProof(operation, nativeResult) ||
      nativeResult.stockTrace?.length !== expectedTrace.length ||
      nativeResult.stockTrace.some(
        (entry, index) => entry.sequence !== index + 1 || entry.event !== expectedTrace[index]
      )
    ) {
      operation = await appendClaimedEvidenceEvents({
        ctx: args.ctx,
        operationId: operation.id,
        claimId,
        events: [
          {
            phase: "replacement-closure-rejected",
            detailCode: "stock-proof-invalid",
          },
        ],
      });
      const failed = await markTerminal({
        ctx: args.ctx,
        operation,
        claimId,
        state: "failed",
        errorCode: "stock_receive_proof_invalid",
      });
      await abortReceiveLease(args.ctx, operation.leaseToken);
      const cleaned = await completeOperationCleanup({
        ctx: args.ctx,
        env: args.env,
        operation: failed,
        includeOutputs: true,
        logger: args.logger,
      });
      return nativeReceiveOperationView(cleaned);
    }
    const tracePhases = new Map<string, string>([
      ["receive_pack_invoked", "receive-pack-start"],
      ["pre_receive_started", "pre-receive-start"],
      ["pre_receive_quarantine_nonempty", "quarantine-visible"],
      ["logical_closure_started_ref_still_old", "replacement-closure-start"],
      ["logical_closure_completed", "replacement-closure-complete"],
      ["pre_receive_succeeded", "pre-receive-complete"],
      ["disposable_ref_update_observed", "disposable-ref-updated"],
    ]);
    operation = await appendClaimedEvidenceEvents({
      ctx: args.ctx,
      operationId: operation.id,
      claimId,
      events: nativeResult.stockTrace
        .map((entry) => tracePhases.get(entry.event))
        .filter((phase): phase is string => phase !== undefined)
        .map((phase) => ({ phase })),
    });
  }

  const ready: NativeReceiveOperation = {
    ...operation,
    state: "ready",
    updatedAt: Date.now(),
    result: {
      statuses: [],
      changed: false,
      empty: false,
      packKey: operation.outputPackKey,
      packBytes: nativeResult.packBytes,
      receivePackResponse: nativeResult.receivePackResponse,
      stockTrace: nativeResult.stockTrace,
    },
    processorResult: nativeResult,
    claimId: undefined,
    claimExpiresAt: undefined,
  };
  if (!(await storeClaimedOperation(args.ctx, ready, claimId))) {
    const current = await getNativeReceiveOperationState(args.ctx, operation.id);
    return current ? nativeReceiveOperationView(current) : null;
  }
  await scheduleNativeWake(args.ctx, args.env, Date.now());
  if (pauseNextBeforeFinalizationForTesting) {
    pauseNextBeforeFinalizationForTesting = false;
    return nativeReceiveOperationView(ready);
  }
  return await runNativeReceiveOperationState(args);
}

export async function resumeNativeReceiveFromAlarm(args: {
  ctx: DurableObjectState;
  env: Env;
  logger: Logger;
}): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const operationIds = (await store.get("nativeReceiveOperationIndex")) ?? [];
  for (let index = operationIds.length - 1; index >= 0; index--) {
    const operationId = operationIds[index];
    if (!operationId) continue;
    const operation = await store.get(nativeReceiveOperationKey(operationId));
    if (!operation) continue;
    // The exact stock path is Worker-owned. Its state-only recovery runs before
    // this legacy alarm and must never cross from RepoDO into R2 or Container.
    if (operation.stockReceive) continue;
    if (isNativeReceiveTerminal(operation.state)) {
      if (!operation.cleanupPending) continue;
      await completeOperationCleanup({
        ctx: args.ctx,
        env: args.env,
        operation,
        includeOutputs: operation.state !== "committed",
        logger: args.logger,
      });
      return true;
    }
    await runNativeReceiveOperationState({ ...args, operationId });
    return true;
  }
  return false;
}
