import type { CacheContext } from "@/worker/cache";
import type { Logger } from "@/worker/common/logger";
import type { RepoDurableObject } from "@/worker/do";
import type { PackCatalogRow } from "@/worker/do/repo/db/schema";
import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type {
  EnqueueNativeReceiveResult,
  MatchNativeReceiveOperationResult,
  NativeReceiveOperation,
  NativeReceiveOperationView,
} from "@/worker/git/nativeReceive/types";
import type { Limiter } from "@/worker/git/operations/limits";
import type { ReceiveCommand } from "@/worker/git/operations/validation";

import {
  doPrefix,
  nativeReceiveInputPackKey,
  nativeReceiveOutputPackKey,
  packIndexKey,
  packRefsKey,
} from "@/worker/keys";
import { isNativeReceiveTerminal } from "@/worker/git/nativeReceive/types";
import { fingerprintNativeReceive } from "@/worker/git/nativeReceive/fingerprint";
import { stagePackToR2 } from "./r2Upload";
import { buildReceiveReportStatus } from "./support";
import {
  NativeReceiveIndeterminateError,
  ReceivePipelineHttpError,
  type ReceivePipelineResult,
} from "./pipelineTypes";

const NATIVE_RECEIVE_WAIT_MS = 30 * 60_000;

type RepoStub = DurableObjectStub<RepoDurableObject>;

type ExecuteNativeReceivePipelineArgs = {
  env: Env;
  repoId: string;
  request: Request;
  packStream: ReadableStream<Uint8Array>;
  bytesConsumed: number;
  stub: RepoStub;
  leaseToken: string;
  activeCatalog: PackCatalogRow[];
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

/**
 * Stages the raw receive body once, then hands ownership to the repository DO.
 * After enqueue succeeds, request cancellation must not abort or delete the
 * operation: the DO alarm owns retry, reconciliation, and exact cleanup.
 */
export async function executeNativeReceivePipeline(
  args: ExecuteNativeReceivePipelineArgs
): Promise<ReceivePipelineResult> {
  const operationId = args.leaseToken;
  const prefix = doPrefix(args.stub.id.toString());
  const inputPackKey = nativeReceiveInputPackKey(prefix, operationId);
  let enqueued = false;
  let operationFingerprint: string | undefined;

  try {
    args.onProgress?.("Staging received Git data\n");
    const staged = await stagePackToR2({
      env: args.env,
      request: args.request,
      packStream: args.packStream,
      packKey: inputPackKey,
      bytesConsumed: args.bytesConsumed,
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
      onProgress: args.onProgress,
    });
    args.countSubrequest("r2:head-native-receive-input");
    const stagedHead = await args.limiter.run("r2:head-native-receive-input", () =>
      args.env.REPO_BUCKET.head(inputPackKey)
    );
    if (!stagedHead || stagedHead.size !== staged.packBytes) {
      throw new Error("Staged native receive input could not be verified.");
    }
    const now = Date.now();
    operationFingerprint = await fingerprintNativeReceive({
      repositoryId: args.repoId,
      commands: args.commands,
      acceptedWrites: args.acceptedWrites,
      inputPackKey,
      inputBytes: staged.packBytes,
      inputEtag: stagedHead.etag,
    });
    const outputPackKey = nativeReceiveOutputPackKey(prefix, operationId, operationFingerprint);
    const operation: NativeReceiveOperation = {
      id: operationId,
      fingerprint: operationFingerprint,
      leaseToken: args.leaseToken,
      repositoryId: args.repoId,
      state: "staged",
      inputPackKey,
      inputBytes: staged.packBytes,
      inputEtag: stagedHead.etag,
      outputPackKey,
      outputIdxKey: packIndexKey(outputPackKey),
      outputRefsKey: packRefsKey(outputPackKey),
      commands: args.commands,
      acceptedWrites: args.acceptedWrites,
      activeCatalog: args.activeCatalog,
      createdAt: now,
      updatedAt: now,
      attempts: 0,
      cleanupPending: false,
    };

    args.countSubrequest("do:enqueue-native-receive");
    const queued = await args.limiter.run<EnqueueNativeReceiveResult>(
      "do:enqueue-native-receive",
      async () => await args.stub.enqueueNativeReceive(operation)
    );
    if (queued.status !== "queued" && queued.status !== "replayed") {
      throw new ReceivePipelineHttpError(409, queued.status, queued.message);
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
      if (isNativeReceiveTerminal(current.state))
        return resultFromOperation(current, args.commands);
      await delay(Math.min(30_000, 2 ** Math.min(attempt, 5) * 1_000));
      attempt++;
    }

    throw new NativeReceiveIndeterminateError(
      "Native Git processing remains durable but its terminal outcome is not yet known."
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
        // A matching or conflicting durable record makes local deletion
        // unsafe. The operation id is the receive lease token, so either state
        // must be reconciled by an identical retry rather than guessed here.
        enqueued = matched.status !== "not_found";
      } catch {
        // A failed reconciliation RPC is itself ambiguous. Preserve the staged
        // input and lease so an identical retry can resolve authoritative DO
        // state without destroying a possibly active operation.
        enqueued = true;
      }
    }
    if (!enqueued) {
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
      throw new NativeReceiveIndeterminateError(
        "Native receive dispatch may be durable; retry the identical update to resolve its outcome."
      );
    }
    throw error;
  }
}
