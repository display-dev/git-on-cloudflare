import type { CacheContext } from "@/worker/cache";
import type { Logger } from "@/worker/common/logger";
import type { RepoDurableObject } from "@/worker/do";
import type { PackCatalogRow } from "@/worker/do/repo/db/schema";
import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type {
  AdmitStockReceiveResult,
  CompleteStockReceiveCleanupResult,
  ConfirmStockReceivePublicationResult,
  EnqueueNativeReceiveResult,
  FinalizeStockReceiveResult,
  MatchNativeReceiveOperationResult,
  NativeReceiveAuthorityPublicationPlan,
  NativeReceiveCleanupDescriptor,
  NativeReceiveOperation,
  NativeReceiveOperationView,
  NativeReceivePrepared,
  RejectStockReceiveExecutionResult,
} from "@/worker/git/nativeReceive/types";
import type { Limiter } from "@/worker/git/operations/limits";
import type { ReceiveCommand } from "@/worker/git/operations/validation";

import {
  doPrefix,
  nativeReceiveInputPackKey,
  nativeReceiveInputRequestKey,
  nativeReceiveOutputPackKey,
  packIndexKey,
  packRefsKey,
} from "@/worker/keys";
import { isNativeReceiveTerminal } from "@/worker/git/nativeReceive/types";
import { fingerprintNativeReceive } from "@/worker/git/nativeReceive/fingerprint";
import { publishNativeReceiveAuthorityPlan } from "@/worker/git/nativeReceive/authorityPublication";
import {
  cleanupStockReceiveWorkerDataPlane,
  classifyStockReceiveDataPlaneError,
  executeStockReceiveWorkerDataPlane,
} from "@/worker/git/nativeReceive/stockDataPlane";
import { stagePackToR2, stageStockReceiveRequestToR2 } from "./r2Upload";
import { buildReceiveReportStatus } from "./support";
import {
  NativeReceiveIndeterminateError,
  ReceivePipelineHttpError,
  type ReceivePipelineResult,
} from "./pipelineTypes";

const NATIVE_RECEIVE_WAIT_MS = 14 * 60_000;
const STOCK_PROCESSOR_RESULT_MAX_BYTES = 256 * 1024;
let failBeforeClientAckOperationForTesting: string | undefined;

export const __test = {
  failBeforeClientAck(operationId: string): void {
    failBeforeClientAckOperationForTesting = operationId;
  },
  reset(): void {
    failBeforeClientAckOperationForTesting = undefined;
  },
};

function throwIfClientAckInterrupted(operationId: string): void {
  if (failBeforeClientAckOperationForTesting !== operationId) return;
  failBeforeClientAckOperationForTesting = undefined;
  throw new NativeReceiveIndeterminateError(
    "Native Git committed before the buffered client response could be released."
  );
}

function indeterminateMessage(hasStableOperationId: boolean): string {
  return hasStableOperationId
    ? "Native receive dispatch may be durable; query the authenticated operation outcome before retrying."
    : "Native receive dispatch may be durable; retry the identical update to resolve its outcome.";
}

function stockReceiveDiagnosticCode(error: unknown): string {
  if (!(error instanceof Error)) return "unclassified";
  return /^(?:stock-plan|stock-physical-plan|stock-data-plane):[a-z0-9-]{1,80}$/.test(error.message)
    ? error.message
    : "unclassified";
}

type RepoStub = DurableObjectStub<RepoDurableObject>;

type ExecuteNativeReceivePipelineArgs = {
  env: Env;
  repoId: string;
  routeStartedAt: number;
  request: Request;
  packStream: ReadableStream<Uint8Array>;
  bytesConsumed: number;
  rawPrefix?: Uint8Array | undefined;
  responseMode?: "plain" | "side-band-64k" | undefined;
  stockReceive?: boolean | undefined;
  stockRecovery?: { operationId: string; token: string } | undefined;
  advertisedRefs?: Array<{ name: string; oid: string }> | undefined;
  stub: RepoStub;
  leaseToken: string;
  operationId?: string | undefined;
  activeCatalog: PackCatalogRow[];
  catalogGeneration: number;
  commands: ReceiveCommand[];
  acceptedWrites: AcceptedWriteFact[];
  log: Logger;
  cacheCtx: CacheContext;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
  onProgress?: (message: string) => void;
};

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function resultFromOperation(
  operation: NativeReceiveOperationView,
  commands: ReceiveCommand[]
): ReceivePipelineResult {
  if (operation.state === "committed" && operation.result) {
    let receivePackResponse: Uint8Array | undefined;
    if (operation.result.receivePackResponse) {
      const binary = atob(operation.result.receivePackResponse);
      if (binary.length > 1024 * 1024) {
        throw new ReceivePipelineHttpError(
          502,
          "native-receive-response-too-large",
          "Native Git returned an oversized response."
        );
      }
      receivePackResponse = Uint8Array.from(binary, (character) => character.charCodeAt(0));
    }
    return {
      reportStatusBody: buildReceiveReportStatus({
        unpackOk: true,
        commands,
        statuses: operation.result.statuses,
      }),
      changed: operation.result.changed,
      empty: operation.result.empty,
      packKey: operation.result.packKey,
      packBytes: operation.result.packBytes,
      receivePackResponse,
    };
  }

  if (operation.state === "aborted") {
    return {
      reportStatusBody: buildReceiveReportStatus({
        unpackOk: true,
        commands,
        statuses:
          operation.result?.statuses ??
          commands.map((command) => ({
            ref: command.ref,
            ok: false,
            msg: operation.errorCode ?? "native receive aborted",
          })),
      }),
      changed: false,
      empty: false,
    };
  }

  throw new ReceivePipelineHttpError(
    503,
    operation.errorCode ?? "native-receive-incomplete",
    operation.state === "failed"
      ? "Native Git processing failed."
      : "Native Git processing is still running; retry the identical update."
  );
}

function stockCleanupDescriptor(operation: NativeReceiveOperation): NativeReceiveCleanupDescriptor {
  const inputRequestSha256 = operation.stockReceive?.inputRequestSha256;
  if (!inputRequestSha256) throw new Error("stock-receive:cleanup-input-missing");
  return {
    operationId: operation.id,
    fingerprint: operation.fingerprint,
    inputPackKey: operation.inputPackKey,
    inputRequestSha256,
    outputPackKey: operation.outputPackKey,
    outputIdxKey: operation.outputIdxKey,
    outputRefsKey: operation.outputRefsKey,
  };
}

async function cleanupStockOperation(args: {
  pipeline: ExecuteNativeReceivePipelineArgs;
  operation: NativeReceiveOperation | NativeReceiveCleanupDescriptor;
  includeOutputs: boolean;
  complete: boolean;
}): Promise<void> {
  await cleanupStockReceiveWorkerDataPlane({
    env: args.pipeline.env,
    operation: args.operation,
    limiter: args.pipeline.limiter,
    countSubrequest: args.pipeline.countSubrequest,
    logger: args.pipeline.log,
    includeOutputs: args.includeOutputs,
  });
  if (!args.complete) return;
  const operationId = "id" in args.operation ? args.operation.id : args.operation.operationId;
  args.pipeline.countSubrequest("do:complete-stock-receive-cleanup");
  const completed = await args.pipeline.limiter.run<CompleteStockReceiveCleanupResult>(
    "do:complete-stock-receive-cleanup",
    async () =>
      await args.pipeline.stub.completeStockReceiveCleanup(operationId, args.operation.fingerprint)
  );
  if (completed.status !== "complete") {
    throw new Error(`stock-receive:cleanup-state-${completed.code}`);
  }
}

async function cleanupCurrentStockRetry(args: {
  pipeline: ExecuteNativeReceivePipelineArgs;
  operation: NativeReceiveCleanupDescriptor;
  includeOutputs: boolean;
}): Promise<void> {
  await cleanupStockOperation({ ...args, complete: false });
  const recovery = args.pipeline.stockRecovery;
  if (!recovery) return;
  args.pipeline.countSubrequest("do:complete-stock-recovery");
  const completed = await args.pipeline.limiter.run("do:complete-stock-recovery", () =>
    args.pipeline.stub.completeStockReceiveRecovery(recovery.operationId, recovery.token)
  );
  if (!completed) throw new Error("stock-receive:recovery-lease-stale");
}

async function acknowledgeStockOperation(args: {
  pipeline: ExecuteNativeReceivePipelineArgs;
  operationId: string;
  operation: NativeReceiveOperationView;
}): Promise<ReceivePipelineResult> {
  if (args.operation.state === "committed" && args.operation.result?.receivePackResponse) {
    throwIfClientAckInterrupted(args.operationId);
    args.pipeline.countSubrequest("do:record-stock-receive-client-ack");
    const acknowledged = await args.pipeline.limiter.run("do:record-stock-receive-client-ack", () =>
      args.pipeline.stub.recordNativeReceiveClientAck(args.operationId)
    );
    if (!acknowledged) {
      throw new NativeReceiveIndeterminateError(
        "Stock receive committed, but the buffered acknowledgement was not durably authorized."
      );
    }
  }
  return resultFromOperation(args.operation, args.pipeline.commands);
}

async function finalizeStockReceiveWithBoundedWait(args: {
  pipeline: ExecuteNativeReceivePipelineArgs;
  executionToken: string;
  prepared?: NativeReceivePrepared | undefined;
}): Promise<Exclude<FinalizeStockReceiveResult, { status: "busy" }>> {
  for (let attempt = 0; attempt < 30; attempt++) {
    args.pipeline.countSubrequest("do:finalize-stock-receive");
    const finalized = await args.pipeline.limiter.run<FinalizeStockReceiveResult>(
      "do:finalize-stock-receive",
      async () => await args.pipeline.stub.finalizeStockReceive(args.executionToken, args.prepared)
    );
    if (finalized.status !== "busy") return finalized;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new NativeReceiveIndeterminateError(
    "Stock receive preparation remains durable while publication capacity is busy."
  );
}

async function executeConcreteStockReceiveSingleHop(args: {
  pipeline: ExecuteNativeReceivePipelineArgs;
  operation: NativeReceiveOperation;
}): Promise<ReceivePipelineResult> {
  const currentCleanup = stockCleanupDescriptor(args.operation);
  args.pipeline.countSubrequest("do:admit-stock-receive");
  let admission = await args.pipeline.limiter.run<AdmitStockReceiveResult>(
    "do:admit-stock-receive",
    async () => await args.pipeline.stub.admitStockReceive(args.operation)
  );
  if (admission.status === "cleanup_pending") {
    await cleanupStockOperation({
      ...args,
      operation: admission.cleanup,
      includeOutputs: admission.includeOutputs,
      complete: true,
    });
    args.pipeline.countSubrequest("do:readmit-stock-receive-after-cleanup");
    admission = await args.pipeline.limiter.run<AdmitStockReceiveResult>(
      "do:readmit-stock-receive-after-cleanup",
      async () => await args.pipeline.stub.admitStockReceive(args.operation)
    );
    if (admission.status === "cleanup_pending") {
      throw new NativeReceiveIndeterminateError(
        "Stock receive cleanup completed, but re-admission did not advance."
      );
    }
  }
  if (admission.status === "conflict" || admission.status === "rejected") {
    await cleanupCurrentStockRetry({
      ...args,
      operation: currentCleanup,
      includeOutputs: true,
    });
    throw new ReceivePipelineHttpError(409, admission.code, "Stock receive admission rejected.");
  }
  if (admission.status === "replayed") {
    await cleanupCurrentStockRetry({
      ...args,
      operation: currentCleanup,
      includeOutputs: false,
    });
    await cleanupStockOperation({
      ...args,
      operation: admission.cleanup,
      includeOutputs: false,
      complete: true,
    });
    return await acknowledgeStockOperation({
      ...args,
      operationId: args.operation.id,
      operation: admission.operation,
    });
  }

  let publicationToken: string;
  let publication: NativeReceiveAuthorityPublicationPlan;
  let authorityCleanup: NativeReceiveCleanupDescriptor;
  if (admission.status === "publication_pending") {
    publicationToken = admission.publicationToken;
    publication = admission.publication;
    authorityCleanup = admission.cleanup;
  } else {
    const executionToken = admission.executionToken;
    let finalized: Exclude<FinalizeStockReceiveResult, { status: "busy" }>;
    if (admission.status === "finalize_pending") {
      finalized = await finalizeStockReceiveWithBoundedWait({
        pipeline: args.pipeline,
        executionToken: admission.executionToken,
      });
    } else {
      let prepared;
      try {
        prepared = await executeStockReceiveWorkerDataPlane({
          env: args.pipeline.env,
          operation: admission.operation,
          cacheCtx: args.pipeline.cacheCtx,
          limiter: args.pipeline.limiter,
          countSubrequest: args.pipeline.countSubrequest,
          logger: args.pipeline.log,
        });
      } catch (error) {
        const rejection = classifyStockReceiveDataPlaneError(error);
        args.pipeline.log.warn("stock-data-plane:rejected", {
          operationId: args.operation.id,
          code: rejection.code,
          diagnosticCode: stockReceiveDiagnosticCode(error),
        });
        if (new TextEncoder().encode(JSON.stringify(rejection)).byteLength > 64 * 1024) {
          throw new Error("stock-receive:rejection-proof-limit");
        }
        args.pipeline.countSubrequest("do:reject-stock-receive-execution");
        const rejected = await args.pipeline.limiter.run<RejectStockReceiveExecutionResult>(
          "do:reject-stock-receive-execution",
          async () =>
            await args.pipeline.stub.rejectStockReceiveExecution(
              admission.executionToken,
              rejection
            )
        );
        if (rejected.status === "failed" || rejected.status === "replayed") {
          await cleanupCurrentStockRetry({
            ...args,
            operation: currentCleanup,
            includeOutputs: false,
          });
          await cleanupStockOperation({
            ...args,
            operation: stockCleanupDescriptor(admission.operation),
            includeOutputs: true,
            complete: true,
          });
        }
        throw new NativeReceiveIndeterminateError(
          `Stock receive data plane failed before authority CAS (${rejection.code}).`
        );
      }
      if (
        new TextEncoder().encode(JSON.stringify(prepared)).byteLength >
        STOCK_PROCESSOR_RESULT_MAX_BYTES
      ) {
        throw new Error("stock-receive:prepared-proof-limit");
      }
      finalized = await finalizeStockReceiveWithBoundedWait({
        pipeline: args.pipeline,
        executionToken: admission.executionToken,
        prepared,
      });
    }
    if (finalized.status === "ref_conflict") {
      await cleanupCurrentStockRetry({
        ...args,
        operation: currentCleanup,
        includeOutputs: false,
      });
      await cleanupStockOperation({
        ...args,
        operation: finalized.cleanup,
        includeOutputs: true,
        complete: true,
      });
      return {
        reportStatusBody: buildReceiveReportStatus({
          unpackOk: true,
          commands: args.pipeline.commands,
          statuses: args.pipeline.commands.map((command) => ({
            ref: command.ref,
            ok: false,
            msg: "stale info",
          })),
        }),
        changed: false,
        empty: false,
      };
    }
    if (finalized.status === "rejected") {
      args.pipeline.log.warn("stock-receive:finalize-rejected", {
        operationId: args.operation.id,
        code: finalized.code,
      });
      args.pipeline.countSubrequest("do:reject-stock-receive-finalize");
      await args.pipeline.limiter.run<RejectStockReceiveExecutionResult>(
        "do:reject-stock-receive-finalize",
        async () =>
          await args.pipeline.stub.rejectStockReceiveExecution(
            executionToken,
            // Attempt-specific claim fencing prevents a late Worker from
            // mutating a newer readmitted execution.
            "finalize-rejected"
          )
      );
      await cleanupCurrentStockRetry({
        ...args,
        operation: currentCleanup,
        includeOutputs: false,
      });
      await cleanupStockOperation({
        ...args,
        operation:
          finalized.cleanup ??
          (admission.status === "admitted"
            ? stockCleanupDescriptor(admission.operation)
            : currentCleanup),
        includeOutputs: true,
        complete: finalized.cleanup !== undefined,
      });
      throw new ReceivePipelineHttpError(409, finalized.code, "Stock receive finalize rejected.");
    }
    if (finalized.status === "replayed") {
      await cleanupCurrentStockRetry({
        ...args,
        operation: currentCleanup,
        includeOutputs: false,
      });
      await cleanupStockOperation({
        ...args,
        operation: finalized.cleanup,
        includeOutputs: false,
        complete: true,
      });
      return await acknowledgeStockOperation({
        ...args,
        operationId: args.operation.id,
        operation: finalized.operation,
      });
    }
    publicationToken = finalized.publicationToken;
    publication = finalized.publication;
    authorityCleanup = finalized.cleanup;
  }

  let proof;
  try {
    proof = await publishNativeReceiveAuthorityPlan({
      env: args.pipeline.env,
      limiter: args.pipeline.limiter,
      plan: publication,
      countSubrequest: args.pipeline.countSubrequest,
      logger: args.pipeline.log,
    });
  } catch (error) {
    throw new NativeReceiveIndeterminateError(
      `Stock receive ref CAS committed before immutable publication completed (${String(error)}).`
    );
  }
  args.pipeline.countSubrequest("do:confirm-stock-receive-publication");
  const confirmed = await args.pipeline.limiter.run<ConfirmStockReceivePublicationResult>(
    "do:confirm-stock-receive-publication",
    async () => await args.pipeline.stub.confirmStockReceivePublication(publicationToken, proof)
  );
  if (confirmed.status === "rejected") {
    throw new NativeReceiveIndeterminateError(
      `Stock receive publication confirmation was rejected (${confirmed.code}).`
    );
  }
  await cleanupCurrentStockRetry({
    ...args,
    operation: currentCleanup,
    includeOutputs: false,
  });
  await cleanupStockOperation({
    ...args,
    operation: authorityCleanup,
    includeOutputs: false,
    complete: true,
  });
  return await acknowledgeStockOperation({
    ...args,
    operationId: args.operation.id,
    operation: confirmed.operation,
  });
}

/**
 * Stages the raw receive body once, then hands ownership to the repository DO.
 * After enqueue succeeds, request cancellation must not abort or delete the
 * operation: the DO alarm owns retry, reconciliation, and exact cleanup.
 */
export async function executeNativeReceivePipeline(
  args: ExecuteNativeReceivePipelineArgs
): Promise<ReceivePipelineResult> {
  const routeStartedAt = args.routeStartedAt;
  // Display-managed clients supply a stable id so a disconnected caller can
  // query the durable result. Plain Git clients retain the lease-token
  // fallback and existing behavior.
  const operationId = args.operationId ?? args.leaseToken;
  const prefix = doPrefix(args.stub.id.toString());
  // Keep pre-enqueue input ownership tied to the receive lease. Alarm cleanup
  // can then remove an input even if the Worker dies before a durable native
  // operation record exists; the client operation id is only reconciliation
  // identity.
  const inputPackKey = args.stockReceive
    ? nativeReceiveInputRequestKey(prefix, args.leaseToken)
    : nativeReceiveInputPackKey(prefix, args.leaseToken);
  let enqueued = false;
  let operationFingerprint: string | undefined;

  try {
    args.onProgress?.("Staging received Git data\n");
    const staged = args.stockReceive
      ? await stageStockReceiveRequestToR2({
          env: args.env,
          request: args.request,
          rawPrefix:
            args.rawPrefix ??
            (() => {
              throw new Error("Stock receive command prefix was not retained.");
            })(),
          packStream: args.packStream,
          requestKey: inputPackKey,
          limiter: args.limiter,
          countSubrequest: args.countSubrequest,
        })
      : await stagePackToR2({
          env: args.env,
          request: args.request,
          packStream: args.packStream,
          packKey: inputPackKey,
          bytesConsumed: args.bytesConsumed,
          limiter: args.limiter,
          countSubrequest: args.countSubrequest,
          onProgress: args.onProgress,
        });
    const stagedBytes = "requestBytes" in staged ? staged.requestBytes : staged.packBytes;
    args.countSubrequest("r2:head-native-receive-input");
    const stagedHead = await args.limiter.run("r2:head-native-receive-input", () =>
      args.env.REPO_BUCKET.head(inputPackKey)
    );
    if (!stagedHead || stagedHead.size !== stagedBytes) {
      throw new Error("Staged native receive input could not be verified.");
    }
    const now = Date.now();
    operationFingerprint = await fingerprintNativeReceive({
      repositoryId: args.repoId,
      commands: args.commands,
      acceptedWrites: args.acceptedWrites,
      inputPackKey,
      inputBytes: stagedBytes,
      inputEtag: stagedHead.etag,
      stockReceive:
        "requestBytes" in staged
          ? {
              inputRequestSha256: staged.requestSha256,
              packOffset: staged.packOffset,
              advertisedRefs: args.advertisedRefs ?? [],
              sideBand64k: args.responseMode === "side-band-64k",
            }
          : undefined,
    });
    const outputPackKey = nativeReceiveOutputPackKey(prefix, operationId, operationFingerprint);
    const operation: NativeReceiveOperation = {
      id: operationId,
      fingerprint: operationFingerprint,
      leaseToken: args.leaseToken,
      repositoryId: args.repoId,
      state: "staged",
      inputPackKey,
      inputBytes: stagedBytes,
      inputEtag: stagedHead.etag,
      stockReceive:
        "requestBytes" in staged
          ? {
              inputRequestSha256: staged.requestSha256,
              packOffset: staged.packOffset,
              packBytes: staged.packBytes,
              advertisedRefs: args.advertisedRefs ?? [],
              sideBand64k: args.responseMode === "side-band-64k",
            }
          : undefined,
      outputPackKey,
      outputIdxKey: packIndexKey(outputPackKey),
      outputRefsKey: packRefsKey(outputPackKey),
      commands: args.commands,
      acceptedWrites: args.acceptedWrites,
      activeCatalog: args.activeCatalog,
      catalogGeneration: args.catalogGeneration,
      createdAt: now,
      updatedAt: now,
      attempts: 0,
      cleanupPending: false,
      events: args.stockReceive
        ? [{ sequence: 1, phase: "worker-route-receive-start", at: routeStartedAt }]
        : undefined,
    };

    if (operation.stockReceive) {
      return await executeConcreteStockReceiveSingleHop({ pipeline: args, operation });
    }

    args.countSubrequest("do:enqueue-native-receive");
    const queued = await args.limiter.run<EnqueueNativeReceiveResult>(
      "do:enqueue-native-receive",
      async () => await args.stub.enqueueNativeReceive(operation)
    );
    if (queued.status !== "queued" && queued.status !== "replayed") {
      throw new ReceivePipelineHttpError(409, queued.status, queued.message);
    }
    if (queued.status === "replayed") {
      args.countSubrequest("do:abort-replayed-stock-receive");
      const released = await args.limiter.run("do:abort-replayed-stock-receive", () =>
        args.stub.abortReceive(args.leaseToken)
      );
      if (!released) {
        throw new NativeReceiveIndeterminateError(
          "Exact receive replay was found, but its retry-owned lease could not be released."
        );
      }
      args.countSubrequest("r2:delete-replayed-stock-receive-input");
      await args.limiter.run("r2:delete-replayed-stock-receive-input", () =>
        args.env.REPO_BUCKET.delete(inputPackKey)
      );
      if (queued.operation.state === "committed" && queued.operation.result?.receivePackResponse) {
        throwIfClientAckInterrupted(operationId);
        args.countSubrequest("do:record-replayed-native-receive-client-ack");
        const ackReady = await args.limiter.run(
          "do:record-replayed-native-receive-client-ack",
          () => args.stub.recordNativeReceiveClientAck(operationId)
        );
        if (!ackReady) {
          throw new NativeReceiveIndeterminateError(
            "Exact receive replay was committed, but its acknowledgement was not authorized."
          );
        }
      }
      return resultFromOperation(queued.operation, args.commands);
    }
    enqueued = true;

    // The DO persists the operation before native work starts. A disconnected
    // client can retry safely while the alarm continues the same operation.
    const deadline = Date.now() + NATIVE_RECEIVE_WAIT_MS;
    let attempt = 0;
    while (Date.now() < deadline) {
      args.onProgress?.("Processing Git objects with native Git\n");
      args.countSubrequest("do:run-native-receive");
      const current = await args.limiter.run("do:run-native-receive", () =>
        args.stub.runNativeReceiveOperation(operationId)
      );
      if (!current) {
        throw new ReceivePipelineHttpError(
          503,
          "native-receive-missing",
          "Native receive operation could not be resolved."
        );
      }
      if (isNativeReceiveTerminal(current.state)) {
        if (current.state === "committed" && current.result?.receivePackResponse) {
          throwIfClientAckInterrupted(operationId);
          args.countSubrequest("do:record-native-receive-client-ack");
          const ackReady = await args.limiter.run("do:record-native-receive-client-ack", () =>
            args.stub.recordNativeReceiveClientAck(operationId)
          );
          if (!ackReady) {
            throw new NativeReceiveIndeterminateError(
              "Native Git committed, but the buffered client acknowledgement was not durably authorized."
            );
          }
        }
        return resultFromOperation(current, args.commands);
      }
      await delay(Math.min(30_000, 2 ** Math.min(attempt, 5) * 1_000));
      attempt++;
    }

    throw new NativeReceiveIndeterminateError(
      args.operationId
        ? "Native Git processing remains durable; query the authenticated operation outcome."
        : "Native Git processing remains durable but its terminal outcome is not yet known."
    );
  } catch (error) {
    if (!enqueued && operationFingerprint) {
      const fingerprint = operationFingerprint;
      args.countSubrequest("do:match-native-receive-after-enqueue-error");
      try {
        const matched = await args.limiter.run<MatchNativeReceiveOperationResult>(
          "do:match-native-receive-after-enqueue-error",
          () => args.stub.matchNativeReceiveOperation(operationId, fingerprint)
        );
        // Only an exact durable match owns this staged input. A conflict is a
        // conclusive rejection of the current receive: abort its lease, delete
        // its lease-owned input, and preserve the original 409 response.
        enqueued = matched.status === "match";
      } catch {
        // A failed reconciliation RPC is itself ambiguous. Preserve the staged
        // input and lease so an identical retry can resolve authoritative DO
        // state without destroying a possibly active operation.
        enqueued = true;
      }
    }
    if (!enqueued && args.stockRecovery) {
      args.countSubrequest("r2:delete-stock-recovery-input");
      await args.limiter
        .run("r2:delete-stock-recovery-input", () => args.env.REPO_BUCKET.delete(inputPackKey))
        .catch(() => {});
      args.countSubrequest("do:complete-stock-recovery-after-error");
      await args.limiter
        .run("do:complete-stock-recovery-after-error", () =>
          args.stub.completeStockReceiveRecovery(
            args.stockRecovery!.operationId,
            args.stockRecovery!.token
          )
        )
        .catch(() => false);
    } else if (!enqueued) {
      args.countSubrequest("do:abort-native-receive");
      const aborted = await args.limiter
        .run("do:abort-native-receive", () => args.stub.abortReceive(args.leaseToken))
        .catch(() => false);
      if (aborted) {
        args.countSubrequest("r2:delete-native-receive-input");
        await args.limiter
          .run("r2:delete-native-receive-input", () => args.env.REPO_BUCKET.delete(inputPackKey))
          .catch(() => {});
      } else {
        // Without a successful matching-token abort, ownership is ambiguous.
        // Preserve the immutable input for authoritative retry/reconciliation.
        enqueued = true;
      }
    }
    args.log.warn("native-receive:request-ended", {
      operationId,
      durable: enqueued,
      error: String(error),
    });
    if (enqueued && !(error instanceof NativeReceiveIndeterminateError)) {
      throw new NativeReceiveIndeterminateError(indeterminateMessage(Boolean(args.operationId)));
    }
    throw error;
  }
}
