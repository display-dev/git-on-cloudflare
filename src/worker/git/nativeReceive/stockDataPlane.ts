import type { CacheContext } from "@/worker/cache";
import type { Logger } from "@/worker/common/logger";
import type {
  NativeReceiveOperation,
  NativeReceiveCleanupDescriptor,
  NativeReceivePrepared,
  NativeReceiveProcessRequest,
  NativeReceiveProcessResult,
  NativeReceiveExecutionRejection,
} from "./types";
import type { Limiter } from "@/worker/git/operations/limits";
import type { StockReceivePlan, StockPlannerFailureMetrics } from "./stockPlanner";

import { z } from "zod";

import { asBufferSource, bytesToHex, createDigestStream } from "@/worker/common";
import { packIndexKey, packRefsKey } from "@/worker/keys";
import { planStockReceive, stockReceivePlanKeys, StockReceivePlannerError } from "./stockPlanner";
import { stockReceivePreparedProofFailure } from "./stockProof";

const REQUEST_MAGIC = new TextEncoder().encode("STKREQ1\n");
const RESPONSE_MAGIC = new TextEncoder().encode("STKOUT1\n");
const BUNDLE_HEADER_MAX_BYTES = 64 * 1024;
const HOST_RESULT_MAX_BYTES = 1024 * 1024;
const ARTIFACT_MAX_BYTES = 32 * 1024 * 1024;
const BUNDLE_REQUEST_MAX_BYTES = 48 * 1024 * 1024 + BUNDLE_HEADER_MAX_BYTES + 12;
const BUNDLE_RESPONSE_MAX_BYTES = 96 * 1024 * 1024 + HOST_RESULT_MAX_BYTES + 12;

const oidSchema = z.string().regex(/^[0-9a-f]{40}$/);
const sha256Schema = z.string().regex(/^[0-9a-f]{64}$/);
const countsSchema = z
  .object({
    commit: z.number().int().nonnegative(),
    tree: z.number().int().nonnegative(),
    blob: z.number().int().nonnegative(),
    tag: z.number().int().nonnegative(),
  })
  .strict();
const hostResultSchema = z
  .object({
    operationId: z.string().min(1).max(100),
    receivePackResponse: z.string().min(1).max(1_400_000),
    receiveResponseBytes: z
      .number()
      .int()
      .nonnegative()
      .max(1024 * 1024),
    inputRequestSha256: sha256Schema,
    packBytes: z.number().int().positive().max(ARTIFACT_MAX_BYTES),
    idxBytes: z.number().int().positive().max(ARTIFACT_MAX_BYTES),
    refsBytes: z.number().int().positive().max(ARTIFACT_MAX_BYTES),
    packSha1: oidSchema,
    packSha256: sha256Schema,
    idxSha256: sha256Schema,
    refsSha256: sha256Schema,
    objectCount: z.number().int().positive().max(100_256),
    inputPackObjectCount: z.number().int().positive().max(100_000),
    elapsedMs: z.number().int().nonnegative(),
    trace: z
      .array(
        z
          .object({
            sequence: z.number().int().positive(),
            event: z.string().min(1).max(100),
          })
          .strict()
      )
      .max(128),
    quarantinePathInsideOwnedWorkRoot: z.boolean(),
    quarantineRemovedAfterReceive: z.boolean(),
    quarantinePathNonEmpty: z.boolean(),
    freshWorkDirectory: z.boolean(),
    repositoryPackBytesBeforeHydration: z.number().int().nonnegative(),
    sharedObjectCacheDisabled: z.boolean(),
    skipConnectivityCheck: z.boolean(),
    planSha256: sha256Schema,
    closureProof: z
      .object({
        planSha256: sha256Schema,
        incomingOids: z.array(oidSchema).max(100_000),
        semanticExternalOids: z.array(oidSchema).max(100_000),
        visitedIncomingObjectCount: z.number().int().nonnegative().max(100_000),
        logicalEdgeCount: z.number().int().nonnegative().max(500_000),
        internalEdgeCount: z.number().int().nonnegative().max(500_000),
        externalEdgeCount: z.number().int().nonnegative().max(500_000),
        missingObjectCount: z.number().int().nonnegative().max(500_000),
        objectTypeCounts: countsSchema,
      })
      .strict(),
  })
  .strict();

type HostResult = z.infer<typeof hostResultSchema>;

type StockWorkerExecutor = (args: {
  operation: NativeReceiveOperation;
  cacheCtx: CacheContext;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
  logger: Logger;
}) => Promise<NativeReceiveProcessResult>;

type StreamingContainerPhase =
  | "bundle-read"
  | "bundle-request"
  | "container-rpc"
  | "bundle-write"
  | "response-header"
  | "output-upload"
  | "output-verification"
  | "proof-validation";

function streamingContainerPhaseError(phase: StreamingContainerPhase, error: unknown): Error {
  if (
    error instanceof Error &&
    /^(?:stock-plan|stock-physical-plan|stock-data-plane):[a-z0-9-]{1,80}$/.test(error.message)
  ) {
    return error;
  }
  return new Error(`stock-data-plane:${phase}-failed`, { cause: error });
}

function containerFailureCode(response: Response): string {
  const diagnostic = response.headers.get("X-Display-Stock-Container-Diagnostic");
  if (response.status < 500) return "stock-data-plane:container-rejected";
  if (diagnostic === "readiness-failed") {
    return "stock-data-plane:container-readiness-failed";
  }
  if (diagnostic === "forward-failed") return "stock-data-plane:container-forward-failed";
  return "stock-data-plane:container-transient";
}

let workerExecutorForTesting: StockWorkerExecutor | undefined;

type OutputMutationRole = "pack" | "index" | "references";
type OutputMutationFault = {
  operationId: string;
  role: OutputMutationRole;
  triggered: boolean;
  bytesMutated: boolean;
  staleCustomMetadataSha256Preserved: boolean;
};

let outputMutationFaultForTesting: OutputMutationFault | undefined;

export const __test = {
  setWorkerExecutor(executor: StockWorkerExecutor): void {
    workerExecutorForTesting = executor;
  },
  failOutputIntegrity(operationId: string, role: OutputMutationRole): void {
    outputMutationFaultForTesting = {
      operationId,
      role,
      triggered: false,
      bytesMutated: false,
      staleCustomMetadataSha256Preserved: false,
    };
  },
  outputMutationFault(operationId: string): OutputMutationFault | undefined {
    return outputMutationFaultForTesting?.operationId === operationId
      ? { ...outputMutationFaultForTesting }
      : undefined;
  },
  containerFailureCode,
  streamingContainerPhaseError,
  reset(): void {
    workerExecutorForTesting = undefined;
    outputMutationFaultForTesting = undefined;
  },
};

class VerifiedR2ObjectError extends Error {
  constructor(
    message: string,
    readonly bytesRead: number
  ) {
    super(message);
    this.name = "VerifiedR2ObjectError";
  }
}

export class StockReceiveDataPlaneError extends Error {
  constructor(
    readonly rejection: NativeReceiveExecutionRejection,
    message: string
  ) {
    super(message);
    this.name = "StockReceiveDataPlaneError";
  }
}

export function classifyStockReceiveDataPlaneError(
  error: unknown
): NativeReceiveExecutionRejection {
  if (error instanceof StockReceiveDataPlaneError) return error.rejection;
  if (error instanceof StockReceivePlannerError) {
    return {
      code: error.code,
      metrics: plannerFailureOperationMetrics(error.metrics),
    };
  }
  const diagnosticCode =
    error instanceof Error &&
    /^(?:stock-plan|stock-physical-plan|stock-data-plane):[a-z0-9-]{1,80}$/.test(error.message)
      ? error.message
      : "stock-data-plane:unclassified";
  return { code: "native-data-plane-failed", diagnosticCode };
}

function plannerFailureOperationMetrics(
  metrics: StockPlannerFailureMetrics
): NonNullable<NativeReceiveExecutionRejection["metrics"]> {
  return {
    elapsedMs: metrics.elapsedMs,
    scratchBytes: 0,
    hydratedBytes: 0,
    downloadedBytes: metrics.metadataBytes + metrics.inputBytesRead + metrics.rangeBytes,
    cacheHitBytes: 0,
    metadataBytes: metrics.metadataBytes,
    metadataRequests: metrics.metadataRequests,
    inputBytesRead: metrics.inputBytesRead,
    inputRequests: metrics.inputRequests,
    rangeBytes: metrics.rangeBytes,
    rangeRequests: metrics.rangeRequests,
    packsTouched: metrics.packsTouched,
    ranges: metrics.ranges,
    activePackReads: metrics.activePackReads,
    activePackTrailerBytes: metrics.activePackTrailerBytes,
    activePackTrailerRequests: metrics.activePackTrailerRequests,
    activePackRangeBytes: metrics.activePackRangeBytes,
    activePackRangeRequests: metrics.activePackRangeRequests,
    activePackWholeBytes: metrics.activePackWholeBytes,
    activePackWholeRequests: metrics.activePackWholeRequests,
    activePackUnattributedBytes: metrics.activePackUnattributedBytes,
    activePackUnattributedRequests: metrics.activePackUnattributedRequests,
    selectedPackBytes: metrics.selectedPackBytes,
    activePackCount: metrics.activePackCount,
    outputValidationBytes: 0,
    outputValidationRequests: 0,
    outputBytesWritten: 0,
    outputRequests: 0,
  };
}

async function sha256(bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

function uint32(value: number): Uint8Array {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, value, false);
  return bytes;
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  return (
    left.byteLength === right.byteLength && left.every((value, index) => value === right[index])
  );
}

async function readVerifiedR2Object(args: {
  env: Env;
  limiter: Limiter;
  key: string;
  bytes: number;
  sha256: string;
  expectedEtag?: string | undefined;
  countSubrequest(op: string, n?: number): void;
  role: string;
}): Promise<{ bytes: Uint8Array; etag: string }> {
  args.countSubrequest(`r2:get-stock-bundle-${args.role}`);
  const object = await args.limiter.run(`r2:get-stock-bundle-${args.role}`, () =>
    args.env.REPO_BUCKET.get(args.key)
  );
  if (
    !object ||
    object.size !== args.bytes ||
    (args.expectedEtag !== undefined && object.etag !== args.expectedEtag)
  ) {
    throw new VerifiedR2ObjectError(`stock-data-plane:${args.role}-authority-mismatch`, 0);
  }
  const bytes = new Uint8Array(await object.arrayBuffer());
  if (bytes.byteLength !== args.bytes || (await sha256(bytes)) !== args.sha256) {
    throw new VerifiedR2ObjectError(
      `stock-data-plane:${args.role}-digest-mismatch`,
      bytes.byteLength
    );
  }
  return { bytes, etag: object.etag };
}

async function writeRequestBundle(
  writable: WritableStream<Uint8Array>,
  header: Uint8Array,
  inputs: Uint8Array[]
): Promise<void> {
  const writer = writable.getWriter();
  try {
    await writer.write(REQUEST_MAGIC);
    await writer.write(uint32(header.byteLength));
    await writer.write(header);
    for (const input of inputs) await writer.write(input);
    await writer.close();
  } catch (error) {
    await writer.abort(error).catch(() => {});
    throw error;
  }
}

class BundleReader {
  private pending = new Uint8Array(0);

  constructor(private readonly reader: ReadableStreamDefaultReader<Uint8Array>) {}

  async exact(bytes: number): Promise<Uint8Array> {
    const output = new Uint8Array(bytes);
    let offset = 0;
    while (offset < bytes) {
      if (this.pending.byteLength === 0) {
        const next = await this.reader.read();
        if (next.done) throw new Error("stock-data-plane:bundle-truncated");
        this.pending = Uint8Array.from(next.value);
      }
      const consumed = Math.min(bytes - offset, this.pending.byteLength);
      output.set(this.pending.subarray(0, consumed), offset);
      this.pending = this.pending.subarray(consumed);
      offset += consumed;
    }
    return output;
  }

  async pipeExact(bytes: number, consume: (chunk: Uint8Array) => Promise<void>): Promise<void> {
    let remaining = bytes;
    while (remaining > 0) {
      if (this.pending.byteLength === 0) {
        const next = await this.reader.read();
        if (next.done) throw new Error("stock-data-plane:bundle-truncated");
        this.pending = Uint8Array.from(next.value);
      }
      const consumed = Math.min(remaining, this.pending.byteLength);
      await consume(this.pending.subarray(0, consumed));
      this.pending = this.pending.subarray(consumed);
      remaining -= consumed;
    }
  }

  async expectEof(): Promise<void> {
    if (this.pending.byteLength !== 0) throw new Error("stock-data-plane:bundle-trailing-bytes");
    const next = await this.reader.read();
    if (!next.done) throw new Error("stock-data-plane:bundle-trailing-bytes");
  }
}

async function receiveArtifact(args: {
  env: Env;
  limiter: Limiter;
  reader: BundleReader;
  operationId: string;
  key: string;
  bytes: number;
  sha256: string;
  role: string;
  countSubrequest(op: string, n?: number): void;
}): Promise<string> {
  const digest = createDigestStream("SHA-256");
  const digestWriter = digest.getWriter();
  const chunks: Uint8Array[] = [];
  try {
    await args.reader.pipeExact(args.bytes, async (chunk) => {
      await digestWriter.write(chunk);
      chunks.push(chunk.slice());
    });
    await digestWriter.close();
    const actualSha256 = await digest.digest.then((value) => bytesToHex(new Uint8Array(value)));
    if (actualSha256 !== args.sha256) {
      throw new Error(`stock-data-plane:${args.role}-stream-digest-mismatch`);
    }
    const artifact = new Uint8Array(args.bytes);
    let offset = 0;
    for (const chunk of chunks) {
      artifact.set(chunk, offset);
      offset += chunk.byteLength;
    }
    const mutation = outputMutationFaultForTesting;
    const shouldMutate =
      mutation?.operationId === args.operationId &&
      mutation.role === args.role &&
      mutation.triggered === false;
    if (shouldMutate) artifact[Math.floor(artifact.byteLength / 2)]! ^= 0x01;
    args.countSubrequest(`r2:put-stock-output-${args.role}`);
    let stored: R2Object | null;
    try {
      stored = await args.limiter.run(`r2:put-stock-output-${args.role}`, () =>
        args.env.REPO_BUCKET.put(args.key, artifact, {
          onlyIf: { etagDoesNotMatch: "*" },
          httpMetadata: { contentType: "application/octet-stream" },
          customMetadata: { sha256: args.sha256 },
        })
      );
    } catch {
      stored = null;
    }
    if (stored) {
      if (shouldMutate && mutation) {
        mutation.triggered = true;
        mutation.bytesMutated = true;
        mutation.staleCustomMetadataSha256Preserved = stored.customMetadata?.sha256 === args.sha256;
      }
      return stored.etag;
    }

    const existing = await readVerifiedR2Object({
      ...args,
      role: `existing-output-${args.role}`,
    });
    args.countSubrequest(`r2:head-existing-stock-output-${args.role}`);
    const head = await args.limiter.run(`r2:head-existing-stock-output-${args.role}`, () =>
      args.env.REPO_BUCKET.head(args.key)
    );
    if (
      !head ||
      head.etag !== existing.etag ||
      head.size !== args.bytes ||
      head.customMetadata?.sha256 !== args.sha256
    ) {
      throw new Error(`stock-data-plane:${args.role}-immutable-conflict`);
    }
    return existing.etag;
  } catch (error) {
    await digestWriter.abort(error).catch(() => {});
    throw error;
  }
}

async function verifyOutputArtifacts(args: {
  env: Env;
  operation: NativeReceiveOperation;
  result: NativeReceiveProcessResult;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
}): Promise<NativeReceiveProcessResult> {
  const artifacts = [
    {
      role: "pack",
      key: args.operation.outputPackKey,
      bytes: args.result.packBytes,
      sha256: args.result.packSha256,
    },
    {
      role: "index",
      key: args.operation.outputIdxKey,
      bytes: args.result.idxBytes,
      sha256: args.result.idxSha256,
    },
    {
      role: "references",
      key: args.operation.outputRefsKey,
      bytes: args.result.refsBytes,
      sha256: args.result.refsSha256,
    },
  ] as const;
  if (artifacts.some((artifact) => !artifact.sha256 || artifact.bytes > ARTIFACT_MAX_BYTES)) {
    throw new Error("stock-data-plane:output-declaration-invalid");
  }
  const etags: string[] = [];
  let validatedBytes = 0;
  let validationRequests = 0;
  for (const artifact of artifacts) {
    let verified: { bytes: Uint8Array; etag: string };
    try {
      verified = await readVerifiedR2Object({
        ...args,
        key: artifact.key,
        bytes: artifact.bytes,
        sha256: artifact.sha256!,
        role: `output-${artifact.role}`,
      });
      validatedBytes += verified.bytes.byteLength;
      validationRequests++;
    } catch (error) {
      const bytesRead = error instanceof VerifiedR2ObjectError ? error.bytesRead : 0;
      throw new StockReceiveDataPlaneError(
        {
          code: "output-integrity-invalid",
          processorResult: {
            ...args.result,
            outputValidationBytes: validatedBytes + bytesRead,
            outputValidationRequests: validationRequests + 1,
            outputIntegrityRejectedRole: artifact.role,
            outputIntegrityRejectedAt: "body",
          },
        },
        "stock-data-plane:output-integrity-rejected"
      );
    }
    args.countSubrequest(`r2:head-stock-output-${artifact.role}`);
    const head = await args.limiter.run(`r2:head-stock-output-${artifact.role}`, () =>
      args.env.REPO_BUCKET.head(artifact.key)
    );
    if (
      !head ||
      head.etag !== verified.etag ||
      head.size !== artifact.bytes ||
      head.customMetadata?.sha256 !== artifact.sha256
    ) {
      throw new StockReceiveDataPlaneError(
        {
          code: "output-integrity-invalid",
          processorResult: {
            ...args.result,
            outputValidationBytes: validatedBytes,
            outputValidationRequests: validationRequests,
            outputIntegrityRejectedRole: artifact.role,
            outputIntegrityRejectedAt: "head",
          },
        },
        "stock-data-plane:output-integrity-rejected"
      );
    }
    etags.push(head.etag);
  }
  return {
    ...args.result,
    outputValidationBytes: validatedBytes,
    outputValidationRequests: validationRequests,
    downloadedBytes: args.result.downloadedBytes + validatedBytes,
    outputPackEtag: etags[0],
    outputIdxEtag: etags[1],
    outputRefsEtag: etags[2],
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

function stockHostRequest(operation: NativeReceiveOperation, plan: StockReceivePlan) {
  const stock = operation.stockReceive!;
  return {
    operationId: operation.id,
    inputRequestKey: operation.inputPackKey,
    inputRequestBytes: operation.inputBytes,
    inputRequestSha256: stock.inputRequestSha256,
    packOffset: stock.packOffset,
    prerequisitePackKey: plan.prerequisitePackKey,
    prerequisitePackBytes: plan.prerequisitePackBytes,
    prerequisitePackSha256: plan.prerequisitePackSha256,
    closureManifestKey: plan.closureManifestKey,
    closureManifestBytes: plan.closureManifestBytes,
    closureManifestSha256: plan.closureManifestSha256,
    advertisedRefs: stock.advertisedRefs,
    commands: operation.commands,
    outputPackKey: operation.outputPackKey,
    outputIdxKey: operation.outputIdxKey,
    outputRefsKey: operation.outputRefsKey,
  };
}

function mergeResult(args: {
  operation: NativeReceiveOperation;
  plan: StockReceivePlan;
  host: HostResult;
  elapsedMs: number;
}): NativeReceiveProcessResult {
  return {
    operationId: args.host.operationId,
    packBytes: args.host.packBytes,
    idxBytes: args.host.idxBytes,
    refsBytes: args.host.refsBytes,
    objectCount: args.host.objectCount,
    inputPackObjectCount: args.host.inputPackObjectCount,
    packSha1: args.host.packSha1,
    elapsedMs: args.host.elapsedMs + args.elapsedMs,
    scratchBytes:
      args.operation.inputBytes +
      args.plan.prerequisitePackBytes +
      args.host.packBytes +
      args.host.idxBytes +
      args.host.refsBytes,
    hydratedBytes: args.plan.prerequisiteHydratedBytes,
    downloadedBytes:
      args.plan.inputBytesRead +
      args.plan.rangeBytes +
      args.plan.metadataBytes +
      args.operation.inputBytes +
      args.plan.prerequisitePackBytes +
      args.plan.closureManifestBytes,
    cacheHitBytes: 0,
    receivePackResponse: args.host.receivePackResponse,
    inputRequestSha256: args.host.inputRequestSha256,
    packSha256: args.host.packSha256,
    idxSha256: args.host.idxSha256,
    refsSha256: args.host.refsSha256,
    stockTrace: args.host.trace,
    metadataBytes: args.plan.metadataBytes + args.plan.closureManifestBytes,
    metadataRequests: args.plan.metadataRequests + 1,
    inputBytesRead:
      args.plan.inputBytesRead + args.operation.inputBytes + args.plan.prerequisitePackBytes,
    inputRequests: args.plan.inputRequests + 2,
    rangeBytes: args.plan.rangeBytes,
    rangeRequests: args.plan.rangeRequests,
    packsTouched: args.plan.packsTouched,
    quarantinePathInsideOwnedWorkRoot: args.host.quarantinePathInsideOwnedWorkRoot,
    quarantineRemovedAfterReceive: args.host.quarantineRemovedAfterReceive,
    quarantinePathNonEmpty: args.host.quarantinePathNonEmpty,
    freshWorkDirectory: args.host.freshWorkDirectory,
    repositoryPackBytesBeforeHydration: args.host.repositoryPackBytesBeforeHydration,
    sharedObjectCacheDisabled: args.host.sharedObjectCacheDisabled,
    skipConnectivityCheck: args.host.skipConnectivityCheck,
    planSha256: args.plan.planSha256,
    closureProof: args.host.closureProof,
    semanticExternalOids: args.plan.semanticExternalOids,
    thinDeltaBaseOids: args.plan.thinDeltaBaseOids,
    requiredRootOids: args.plan.requiredRootOids,
    prerequisiteObjectOids: args.plan.requiredRootOids,
    physicalNodes: args.plan.physicalNodes,
    physicalDependencies: args.plan.dependencies,
    topologicalEntryIds: args.plan.topologicalEntryIds,
    selectedPackChecksums: args.plan.selectedPackChecksums,
    activePackBindings: args.plan.activePackBindings,
    ranges: args.plan.ranges,
    activePackReads: args.plan.activePackReads,
    activePackTrailerBytes: args.plan.activePackTrailerBytes,
    activePackTrailerRequests: args.plan.activePackTrailerRequests,
    activePackRangeBytes: args.plan.activePackRangeBytes,
    activePackRangeRequests: args.plan.activePackRangeRequests,
    activePackWholeBytes: args.plan.activePackWholeBytes,
    activePackWholeRequests: args.plan.activePackWholeRequests,
    activePackUnattributedBytes: args.plan.activePackUnattributedBytes,
    activePackUnattributedRequests: args.plan.activePackUnattributedRequests,
    closureManifestKey: args.plan.closureManifestKey,
    closureManifestBytes: args.plan.closureManifestBytes,
    closureManifestSha256: args.plan.closureManifestSha256,
    closureManifestEtag: args.plan.closureManifestEtag,
    prerequisitePackKey: args.plan.prerequisitePackKey,
    prerequisitePackBytes: args.plan.prerequisitePackBytes,
    prerequisitePackSha256: args.plan.prerequisitePackSha256,
    prerequisitePackEtag: args.plan.prerequisitePackEtag,
    incomingObjectCount: args.plan.incomingObjectCount,
    visitedIncomingObjectCount: args.plan.visitedIncomingObjectCount,
    logicalEdgeCount: args.plan.logicalEdgeCount,
    internalEdgeCount: args.plan.internalEdgeCount,
    externalEdgeCount: args.plan.externalEdgeCount,
    missingObjectCount: args.plan.missingObjectCount,
    objectTypeCounts: args.plan.objectTypeCounts,
    selectedPackBytes: args.plan.selectedPackBytes,
    activePackCount: args.plan.activePackCount,
    outputBytesWritten: args.host.packBytes + args.host.idxBytes + args.host.refsBytes,
    outputRequests: 3,
  };
}

async function executeStreamingContainer(args: {
  env: Env;
  operation: NativeReceiveOperation;
  plan: StockReceivePlan;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
}): Promise<HostResult> {
  const stock = args.operation.stockReceive!;
  let inputObject: Awaited<ReturnType<typeof readVerifiedR2Object>>;
  let prerequisiteObject: Awaited<ReturnType<typeof readVerifiedR2Object>>;
  let manifestObject: Awaited<ReturnType<typeof readVerifiedR2Object>>;
  try {
    [inputObject, prerequisiteObject, manifestObject] = await Promise.all([
      readVerifiedR2Object({
        ...args,
        key: args.operation.inputPackKey,
        bytes: args.operation.inputBytes,
        sha256: stock.inputRequestSha256,
        expectedEtag: args.operation.inputEtag,
        role: "input",
      }),
      readVerifiedR2Object({
        ...args,
        key: args.plan.prerequisitePackKey,
        bytes: args.plan.prerequisitePackBytes,
        sha256: args.plan.prerequisitePackSha256,
        expectedEtag: args.plan.prerequisitePackEtag,
        role: "prerequisite",
      }),
      readVerifiedR2Object({
        ...args,
        key: args.plan.closureManifestKey,
        bytes: args.plan.closureManifestBytes,
        sha256: args.plan.closureManifestSha256,
        expectedEtag: args.plan.closureManifestEtag,
        role: "manifest",
      }),
    ]);
  } catch (error) {
    throw streamingContainerPhaseError("bundle-read", error);
  }
  const input = inputObject.bytes;
  const prerequisite = prerequisiteObject.bytes;
  const manifest = manifestObject.bytes;
  const header = new TextEncoder().encode(
    JSON.stringify(stockHostRequest(args.operation, args.plan))
  );
  const requestBytes =
    12 + header.byteLength + input.byteLength + prerequisite.byteLength + manifest.byteLength;
  if (header.byteLength > BUNDLE_HEADER_MAX_BYTES || requestBytes > BUNDLE_REQUEST_MAX_BYTES) {
    throw new Error("stock-data-plane:request-bundle-limit");
  }
  let writing: Promise<void>;
  let request: Request;
  try {
    const fixed = new FixedLengthStream(requestBytes);
    writing = writeRequestBundle(fixed.writable, header, [input, prerequisite, manifest]);
    request = new Request("https://stock-container.internal/stock-receive-bundle", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-display-stock-receive-bundle",
        "Content-Length": String(requestBytes),
      },
      body: fixed.readable,
    });
  } catch (error) {
    throw streamingContainerPhaseError("bundle-request", error);
  }
  const stub = args.env.STOCK_RECEIVE_CONTAINER_HOST.getByName(args.operation.repositoryId);
  args.countSubrequest("do:stock-container-process");
  let response: Response;
  try {
    response = await args.limiter.run<Response>(
      "do:stock-container-process",
      async () => await stub.processStockReceive(request)
    );
  } catch (error) {
    throw streamingContainerPhaseError("container-rpc", error);
  }
  try {
    await writing;
  } catch (error) {
    throw streamingContainerPhaseError("bundle-write", error);
  }
  const declaredResponse = Number(response.headers.get("Content-Length"));
  if (
    !response.ok ||
    !response.body ||
    !Number.isSafeInteger(declaredResponse) ||
    declaredResponse <= 12 ||
    declaredResponse > BUNDLE_RESPONSE_MAX_BYTES
  ) {
    const failureCode = containerFailureCode(response);
    await response.body?.cancel();
    throw new Error(failureCode);
  }
  const reader = new BundleReader(response.body.getReader());
  if (!equalBytes(await reader.exact(8), RESPONSE_MAGIC)) {
    throw new Error("stock-data-plane:response-magic-invalid");
  }
  const lengthBytes = await reader.exact(4);
  const resultLength = new DataView(
    lengthBytes.buffer,
    lengthBytes.byteOffset,
    lengthBytes.byteLength
  ).getUint32(0, false);
  if (resultLength <= 0 || resultLength > HOST_RESULT_MAX_BYTES) {
    throw new Error("stock-data-plane:response-header-limit");
  }
  let host: HostResult;
  try {
    host = hostResultSchema.parse(
      JSON.parse(new TextDecoder().decode(await reader.exact(resultLength)))
    );
  } catch (error) {
    throw streamingContainerPhaseError("response-header", error);
  }
  if (
    host.operationId !== args.operation.id ||
    host.inputRequestSha256 !== stock.inputRequestSha256 ||
    host.planSha256 !== args.plan.planSha256 ||
    declaredResponse !== 12 + resultLength + host.packBytes + host.idxBytes + host.refsBytes
  ) {
    throw new Error("stock-data-plane:response-binding-invalid");
  }
  try {
    await receiveArtifact({
      ...args,
      reader,
      operationId: args.operation.id,
      key: args.operation.outputPackKey,
      bytes: host.packBytes,
      sha256: host.packSha256,
      role: "pack",
    });
    await receiveArtifact({
      ...args,
      reader,
      operationId: args.operation.id,
      key: args.operation.outputIdxKey,
      bytes: host.idxBytes,
      sha256: host.idxSha256,
      role: "index",
    });
    await receiveArtifact({
      ...args,
      reader,
      operationId: args.operation.id,
      key: args.operation.outputRefsKey,
      bytes: host.refsBytes,
      sha256: host.refsSha256,
      role: "references",
    });
    await reader.expectEof();
  } catch (error) {
    throw streamingContainerPhaseError("output-upload", error);
  }
  return host;
}

export async function executeStockReceiveWorkerDataPlane(args: {
  env: Env;
  operation: NativeReceiveOperation;
  cacheCtx: CacheContext;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
  logger: Logger;
}): Promise<NativeReceivePrepared> {
  if (!args.operation.stockReceive) throw new Error("stock-data-plane:stock-input-absent");
  let result: NativeReceiveProcessResult;
  if (workerExecutorForTesting) {
    result = await workerExecutorForTesting(args);
  } else {
    const startedAt = Date.now();
    const stock = args.operation.stockReceive;
    const plan = await planStockReceive({
      env: args.env,
      repoId: args.operation.repositoryId,
      operationId: args.operation.id,
      inputRequestKey: args.operation.inputPackKey,
      inputRequestBytes: args.operation.inputBytes,
      inputRequestSha256: stock.inputRequestSha256,
      packOffset: stock.packOffset,
      packBytes: stock.packBytes,
      advertisedRefs: stock.advertisedRefs,
      commands: args.operation.commands,
      activePacks: processRequest(args.operation).activePacks,
      cacheCtx: args.cacheCtx,
      limiter: args.limiter,
      countSubrequest: (n) => args.countSubrequest("stock-plan", n),
      log: args.logger,
    });
    const host = await executeStreamingContainer({ ...args, plan });
    result = mergeResult({
      operation: args.operation,
      plan,
      host,
      elapsedMs: Date.now() - startedAt,
    });
  }
  try {
    result = await verifyOutputArtifacts({ ...args, result });
  } catch (error) {
    throw streamingContainerPhaseError("output-verification", error);
  }
  let proofFailure: string | undefined;
  try {
    proofFailure = await stockReceivePreparedProofFailure(args.operation, result);
  } catch (error) {
    throw streamingContainerPhaseError("proof-validation", error);
  }
  if (proofFailure) {
    throw new Error(`stock-data-plane:proof-${proofFailure}`);
  }
  args.logger.info("stock-data-plane:prepared", {
    operationId: args.operation.id,
    packBytes: result.packBytes,
    rangeBytes: result.rangeBytes,
  });
  return {
    operationId: args.operation.id,
    fingerprint: args.operation.fingerprint,
    processorResult: result,
  };
}

export async function cleanupStockReceiveWorkerDataPlane(args: {
  env: Env;
  operation: NativeReceiveOperation | NativeReceiveCleanupDescriptor;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
  logger: Logger;
  includeOutputs: boolean;
}): Promise<void> {
  const inputRequestSha256 =
    "inputRequestSha256" in args.operation
      ? args.operation.inputRequestSha256
      : args.operation.stockReceive?.inputRequestSha256;
  const planKeys = inputRequestSha256
    ? stockReceivePlanKeys(args.operation.inputPackKey, inputRequestSha256)
    : undefined;
  const keys = [
    args.operation.inputPackKey,
    planKeys?.temporaryPackKey,
    planKeys ? packIndexKey(planKeys.temporaryPackKey) : undefined,
    planKeys ? packRefsKey(planKeys.temporaryPackKey) : undefined,
    planKeys?.prerequisitePackKey,
    planKeys?.closureManifestKey,
    ...(args.includeOutputs
      ? [args.operation.outputPackKey, args.operation.outputIdxKey, args.operation.outputRefsKey]
      : []),
  ].filter((key, index, all): key is string => Boolean(key) && all.indexOf(key) === index);
  args.countSubrequest("r2:cleanup-stock-receive", keys.length);
  await args.limiter.run("r2:cleanup-stock-receive", () => args.env.REPO_BUCKET.delete(keys));
  args.logger.info("stock-data-plane:cleanup", {
    operationId: "id" in args.operation ? args.operation.id : args.operation.operationId,
    objectCount: keys.length,
    outputsDeleted: args.includeOutputs,
  });
}
