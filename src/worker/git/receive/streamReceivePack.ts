import type { CacheContext } from "@/worker/cache";
import type { Logger } from "@/worker/common/logger";
import type { RepoDurableObject } from "@/worker/do";
import type { PackCatalogRow } from "@/worker/do/repo/db/schema";
import type {
  BeginReceiveResult,
  BeginStockReceiveRecoveryResult,
} from "@/worker/do/repo/catalog/shared";
import type { ReceiveStatus } from "@/worker/git/operations/validation";
import type { ReceiveCommand } from "@/worker/git/operations/validation";
import {
  acceptedWriteFactsForCommands,
  type AcceptedWriteContext,
  type AcceptedWriteFact,
} from "@/worker/git/acceptedWrite";
export type { AcceptedWriteContext } from "@/worker/git/acceptedWrite";

import { clientAbortedResponse, createLogger, getRepoStub, isZeroOid } from "@/worker/common";
import { countSubrequest, type Limiter } from "@/worker/git/operations/limits";
import { isValidRefName, validateReceiveCommands } from "@/worker/git/operations/validation";
import { logOnce } from "@/worker/git/object-store/support";
import { executeReceivePipeline } from "./pipeline";
import { executeNativeReceivePipeline } from "./nativePipeline";
import { isValidNativeReceiveOperationId } from "@/worker/git/nativeReceive/types";
import { NativeReceiveIndeterminateError, ReceivePipelineHttpError } from "./pipelineTypes";
import { doPrefix, nativeReceiveInputRequestKey } from "@/worker/keys";
import { readPktSectionStream } from "./pktSectionStream";
import {
  parseReceiveRequest,
  type ParsedReceiveRequest,
  type ReceiveCommandList,
  type ReceiveNegotiatedCapabilities,
} from "./request";
import {
  buildReceiveResultResponse,
  ReceiveSidebandWriter,
  type ReceiveResponseMode,
} from "./response";
import {
  buildReceiveReportStatus,
  buildReceiveUnpackFailureReport,
  isReceiveAbort,
  throwIfReceiveAborted,
} from "./support";

const RECEIVE_SUBREQUEST_BUDGET = 5_000;
const NATIVE_RECEIVE_OPERATION_HEADER = "X-Display-Operation-ID";
const STOCK_RECEIVE_SPIKE_HEADER = "X-Display-Spike1b-Stock";
const STOCK_RECEIVE_REQUEST_MAX_BYTES = 16 * 1024 * 1024;
const STOCK_PREPARATION_ADMISSION_ATTEMPTS = 16;
const STOCK_PREPARATION_ADMISSION_INTERVAL_MS = 200;

type RepoStub = DurableObjectStub<RepoDurableObject>;
export type RepoStateChangeHandler = (change: {
  changed: boolean;
  empty: boolean;
  commands: ReceiveCommand[];
  acceptedWrites: AcceptedWriteFact[];
}) => Promise<void> | void;

function countReceiveSubrequest(cacheCtx: CacheContext, log: Logger, op: string, n = 1) {
  if (countSubrequest(cacheCtx, n)) return;
  logOnce(cacheCtx, `receive-soft-budget:${op}`, () => {
    log.warn("soft-budget-exhausted", { op });
  });
}

function logReceiveEnd(log: Logger, status: number, extra?: Record<string, unknown>) {
  log.info("receive:end", { status, ...extra });
}

function selectReceiveResponseMode(
  capabilities: ReceiveNegotiatedCapabilities
): ReceiveResponseMode {
  return capabilities.sideBand64k ? "side-band-64k" : "plain";
}

function useNativeReceive(env: Env, commands: ReceiveCommand[]): boolean {
  return (
    env.NATIVE_RECEIVE_CONTAINER === "1" && commands.some((command) => !isZeroOid(command.newOid))
  );
}

function declaredReceiveBytes(request: Request): number | undefined {
  const declaredLength = request.headers.get("Content-Length");
  if (!declaredLength || !/^\d+$/.test(declaredLength)) return undefined;
  const bytes = Number(declaredLength);
  return Number.isSafeInteger(bytes) && bytes > 0 ? bytes : undefined;
}

function scheduleRepoStateChange(
  ctx: ExecutionContext,
  onRepoStateChanged: RepoStateChangeHandler | undefined,
  change: {
    changed: boolean;
    empty: boolean;
    commands: ReceiveCommand[];
    acceptedWrites: AcceptedWriteFact[];
  }
): void {
  if (!onRepoStateChanged || !change.changed) return;
  ctx.waitUntil(Promise.resolve().then(() => onRepoStateChanged(change)));
}

function buildInvalidRefResponse(args: {
  mode: ReceiveResponseMode;
  commands: ReceiveCommandList;
}): Response {
  return buildReceiveResultResponse({
    mode: args.mode,
    reportStatusBody: buildReceiveUnpackFailureReport(args.commands, "invalid-ref", "invalid"),
    changed: false,
    empty: false,
  });
}

function buildPreflightConflictResponse(args: {
  mode: ReceiveResponseMode;
  commands: ReceiveCommandList;
  statuses: ReceiveStatus[];
}): Response {
  return buildReceiveResultResponse({
    mode: args.mode,
    reportStatusBody: buildReceiveReportStatus({
      unpackOk: true,
      commands: args.commands,
      statuses: args.statuses,
    }),
    changed: false,
    empty: false,
  });
}

function getErrorStatus(error: unknown): number {
  if (error instanceof ReceivePipelineHttpError) {
    return error.status;
  }

  const message = String(error);
  const lower = message.toLowerCase();
  if (lower.includes("unsupported pack version") || lower.includes("pack header")) {
    return 415;
  }
  if (
    lower.includes("malformed") ||
    lower.includes("missing") ||
    lower.includes("ended before") ||
    lower.includes("could not be resolved") ||
    lower.includes("delta")
  ) {
    return 400;
  }
  return 500;
}

async function beginReceiveWithBoundedPreparationWait(args: {
  stub: RepoStub;
  cacheCtx: CacheContext;
  log: Logger;
  limiter: Limiter;
  stockPreparation: boolean;
}): Promise<BeginReceiveResult> {
  const attempts = args.stockPreparation ? STOCK_PREPARATION_ADMISSION_ATTEMPTS : 1;
  let begin: BeginReceiveResult = { ok: false, retryAfter: 10 };
  for (let attempt = 0; attempt < attempts; attempt++) {
    countReceiveSubrequest(args.cacheCtx, args.log, "do:begin-receive");
    begin = await args.limiter.run<BeginReceiveResult>("do:begin-receive", async () =>
      args.stub.beginReceive({ stockPreparation: args.stockPreparation })
    );
    if (begin.ok || attempt + 1 === attempts) return begin;
    await new Promise((resolve) => setTimeout(resolve, STOCK_PREPARATION_ADMISSION_INTERVAL_MS));
  }
  return begin;
}

function createSidebandReceiveResponse(args: {
  env: Env;
  repoId: string;
  routeStartedAt: number;
  request: Request;
  ctx: ExecutionContext;
  stub: RepoStub;
  log: Logger;
  cacheCtx: CacheContext;
  limiter: Limiter;
  leaseToken: string;
  operationId?: string | undefined;
  activeCatalog: PackCatalogRow[];
  catalogGeneration: number;
  commands: ParsedReceiveRequest["commands"];
  acceptedWrites: AcceptedWriteFact[];
  capabilities: ReceiveNegotiatedCapabilities;
  packStream: ReadableStream<Uint8Array>;
  bytesConsumed: number;
  onRepoStateChanged?: RepoStateChangeHandler | undefined;
}): Response {
  const responseStream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const writer = new ReceiveSidebandWriter(controller);
      const onProgress = args.capabilities.quiet
        ? undefined
        : (message: string) => writer.progress(message);
      const heartbeat =
        onProgress && useNativeReceive(args.env, args.commands)
          ? setInterval(() => onProgress("Native Git processing is still active\n"), 15_000)
          : undefined;
      let closed = false;
      const close = () => {
        if (closed) return;
        closed = true;
        controller.close();
      };

      try {
        const executePipeline = useNativeReceive(args.env, args.commands)
          ? executeNativeReceivePipeline
          : executeReceivePipeline;
        const result = await executePipeline({
          env: args.env,
          repoId: args.repoId,
          routeStartedAt: args.routeStartedAt,
          request: args.request,
          ctx: args.ctx,
          packStream: args.packStream,
          bytesConsumed: args.bytesConsumed,
          stub: args.stub,
          leaseToken: args.leaseToken,
          operationId: args.operationId,
          activeCatalog: args.activeCatalog,
          catalogGeneration: args.catalogGeneration,
          commands: args.commands,
          acceptedWrites: args.acceptedWrites,
          log: args.log,
          cacheCtx: args.cacheCtx,
          limiter: args.limiter,
          countSubrequest: (op, n = 1) => countReceiveSubrequest(args.cacheCtx, args.log, op, n),
          onProgress,
        });

        scheduleRepoStateChange(args.ctx, args.onRepoStateChanged, {
          changed: result.changed,
          empty: result.empty,
          commands: args.commands,
          acceptedWrites: args.acceptedWrites,
        });
        writer.reportStatus(result.reportStatusBody);
        logReceiveEnd(args.log, 200, {
          changed: result.changed,
          empty: result.empty,
          packKey: result.packKey,
          packBytes: result.packBytes,
        });
      } catch (error) {
        if (isReceiveAbort(args.request, error)) {
          logReceiveEnd(args.log, 499, { reason: "client-aborted" });
          close();
          return;
        }

        if (error instanceof NativeReceiveIndeterminateError) {
          args.log.warn("native-receive:response-indeterminate", { error: error.message });
          logReceiveEnd(args.log, 200, { reason: "native-receive-indeterminate" });
          close();
          return;
        }

        args.log.error("receive:error", { error: String(error) });
        writer.reportStatus(
          buildReceiveUnpackFailureReport(
            args.commands,
            error instanceof ReceivePipelineHttpError ? error.message : String(error)
          )
        );
        logReceiveEnd(args.log, 200, { reason: "sideband-unpack-error" });
      } finally {
        if (heartbeat !== undefined) clearInterval(heartbeat);
        close();
      }
    },
  });

  return new Response(responseStream, {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-receive-pack-result",
      // Credentialed mutating path; never share in caches.
      "Cache-Control": "no-store",
    },
  });
}

export async function handleStreamingReceivePackPOST(
  env: Env,
  repoId: string,
  request: Request,
  ctx: ExecutionContext,
  options?: {
    cacheCtx?: CacheContext | undefined;
    limiter?: Limiter | undefined;
    onRepoStateChanged?: RepoStateChangeHandler | undefined;
    acceptedWriteContext?: AcceptedWriteContext | undefined;
  }
): Promise<Response> {
  const routeStartedAt = Date.now();
  const stub = getRepoStub(env, repoId);
  const log = createLogger(env.LOG_LEVEL, {
    service: "StreamingReceivePack",
    repoId,
  });
  log.info("receive:start", { mode: "streaming" });

  if (!request.body) {
    logReceiveEnd(log, 400, { reason: "missing-body" });
    return new Response("Missing receive-pack request body\n", { status: 400 });
  }
  if (request.signal.aborted) {
    logReceiveEnd(log, 499, { reason: "client-aborted" });
    return clientAbortedResponse();
  }

  const stockReceiveRequested = request.headers.get(STOCK_RECEIVE_SPIKE_HEADER) === "1";
  const requestBytes = declaredReceiveBytes(request);
  if (stockReceiveRequested) {
    const declaredLength = request.headers.get("Content-Length");
    if (!declaredLength || !/^\d+$/.test(declaredLength)) {
      logReceiveEnd(log, 411, { reason: "stock-receive-length-required" });
      return new Response("Stock receive requires an exact Content-Length.\n", { status: 411 });
    }
    const explicitBytes = Number(declaredLength);
    if (
      !Number.isSafeInteger(explicitBytes) ||
      explicitBytes <= 0 ||
      explicitBytes > STOCK_RECEIVE_REQUEST_MAX_BYTES
    ) {
      logReceiveEnd(log, 413, { reason: "stock-receive-declaration-too-large" });
      return new Response("Stock receive declaration exceeds its bound.\n", { status: 413 });
    }
  }
  const ordinarySelectiveReceiveEligible =
    !stockReceiveRequested &&
    requestBytes !== undefined &&
    requestBytes <= STOCK_RECEIVE_REQUEST_MAX_BYTES;
  const parallelStockPreparation =
    env.NATIVE_RECEIVE_CONTAINER === "1" &&
    (stockReceiveRequested || ordinarySelectiveReceiveEligible);

  const cacheCtx = options?.cacheCtx ?? {
    req: request,
    ctx,
    memo: { repoId, subreqBudget: RECEIVE_SUBREQUEST_BUDGET },
  };
  const limiter = options?.limiter ?? cacheCtx.memo?.limiter;
  if (!limiter) throw new Error("receive request limiter is required");

  const requestedOperationIdHeader = request.headers.get(NATIVE_RECEIVE_OPERATION_HEADER);
  const requestedOperationId = requestedOperationIdHeader?.trim();
  if (
    requestedOperationIdHeader !== null &&
    !isValidNativeReceiveOperationId(requestedOperationId ?? "")
  ) {
    logReceiveEnd(log, 400, { reason: "invalid-native-operation-id" });
    return new Response("Invalid native receive operation id.\n", {
      status: 400,
      headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" },
    });
  }

  let begin: BeginReceiveResult;
  if (stockReceiveRequested && requestedOperationId) {
    countReceiveSubrequest(cacheCtx, log, "do:begin-stock-receive-recovery");
    let recovery = await limiter.run<BeginStockReceiveRecoveryResult>(
      "do:begin-stock-receive-recovery",
      async () => await stub.beginStockReceiveRecovery(requestedOperationId)
    );
    if (recovery.status === "cleanup_required") {
      const cleanup = recovery;
      const staleKey = nativeReceiveInputRequestKey(doPrefix(stub.id.toString()), cleanup.token);
      countReceiveSubrequest(cacheCtx, log, "r2:cleanup-stock-recovery-input");
      await limiter.run("r2:cleanup-stock-recovery-input", () => env.REPO_BUCKET.delete(staleKey));
      countReceiveSubrequest(cacheCtx, log, "do:complete-stock-recovery-cleanup");
      const cleaned = await limiter.run("do:complete-stock-recovery-cleanup", () =>
        stub.completeStockReceiveRecovery(cleanup.operationId, cleanup.token)
      );
      if (!cleaned) throw new Error("stock-receive:recovery-cleanup-stale");
      countReceiveSubrequest(cacheCtx, log, "do:retry-begin-stock-receive-recovery");
      recovery = await limiter.run<BeginStockReceiveRecoveryResult>(
        "do:retry-begin-stock-receive-recovery",
        async () => await stub.beginStockReceiveRecovery(requestedOperationId)
      );
    }
    if (recovery.status === "busy" || recovery.status === "cleanup_required") {
      begin = {
        ok: false,
        retryAfter: recovery.status === "busy" ? recovery.retryAfter : 10,
      };
    } else if (recovery.status === "recovery") {
      begin = recovery.begin;
    } else {
      begin = await beginReceiveWithBoundedPreparationWait({
        stub,
        cacheCtx,
        log,
        limiter,
        stockPreparation: parallelStockPreparation,
      });
    }
  } else {
    begin = await beginReceiveWithBoundedPreparationWait({
      stub,
      cacheCtx,
      log,
      limiter,
      stockPreparation: parallelStockPreparation,
    });
  }
  if (!begin.ok) {
    log.warn("receive:block-busy", { retryAfter: begin.retryAfter, mode: "streaming" });
    logReceiveEnd(log, 503, { reason: "receive-lease-active" });
    return new Response("Repository is busy receiving; please retry shortly.\n", {
      status: 503,
      headers: {
        "Retry-After": String(begin.retryAfter),
        "Content-Type": "text/plain; charset=utf-8",
      },
    });
  }

  let pipelineStarted = false;
  try {
    const { lines, bytesConsumed, rawPrefix, packStream } = await readPktSectionStream(
      request.body
    );
    throwIfReceiveAborted(request, log, "read-command-section");

    const parsedRequest = parseReceiveRequest(lines);
    const acceptedWrites = options?.acceptedWriteContext
      ? acceptedWriteFactsForCommands({
          ...options.acceptedWriteContext,
          commands: parsedRequest.commands,
        })
      : [];
    const responseMode = selectReceiveResponseMode(parsedRequest.capabilities);
    const nativeReceive = useNativeReceive(env, parsedRequest.commands);
    const stockReceive =
      nativeReceive && (stockReceiveRequested || ordinarySelectiveReceiveEligible);

    if (begin.concurrentStockPreparation && !stockReceive) {
      countReceiveSubrequest(cacheCtx, log, "do:abort-nonstock-concurrent-receive");
      await limiter
        .run("do:abort-nonstock-concurrent-receive", () => stub.abortReceive(begin.lease.token))
        .catch(() => {});
      logReceiveEnd(log, 503, { reason: "stock-preparation-active" });
      return new Response("Repository is busy receiving; please retry shortly.\n", {
        status: 503,
        headers: {
          "Retry-After": "10",
          "Content-Type": "text/plain; charset=utf-8",
        },
      });
    }

    if (requestedOperationId && !nativeReceive) {
      countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
      await limiter
        .run("do:abort-receive", () => stub.abortReceive(begin.lease.token))
        .catch(() => {});
      logReceiveEnd(log, 409, { reason: "native-operation-id-unavailable" });
      return new Response(
        "A durable operation id requires a native receive with at least one non-delete update.\n",
        {
          status: 409,
          headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" },
        }
      );
    }

    const invalidCommand = parsedRequest.commands.find((command) => !isValidRefName(command.ref));
    if (invalidCommand) {
      countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
      await limiter
        .run("do:abort-receive", () => stub.abortReceive(begin.lease.token))
        .catch(() => {});
      log.warn("receive:invalid-ref", { ref: invalidCommand.ref });
      const response = buildInvalidRefResponse({
        mode: responseMode,
        commands: parsedRequest.commands,
      });
      logReceiveEnd(log, response.status, { reason: "invalid-ref", changed: false, empty: false });
      return response;
    }

    const preflightStatuses = validateReceiveCommands(begin.refs, parsedRequest.commands);
    const potentialCommittedReplay =
      stockReceive &&
      requestedOperationId !== undefined &&
      parsedRequest.commands.every(
        (command) =>
          begin.refs.find((ref) => ref.name === command.ref)?.oid.toLowerCase() ===
          command.newOid.toLowerCase()
      );
    if (!preflightStatuses.every((status) => status.ok) && !potentialCommittedReplay) {
      countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
      await limiter
        .run("do:abort-receive", () => stub.abortReceive(begin.lease.token))
        .catch(() => {});
      log.warn("receive:ref-conflict", {
        conflictCount: preflightStatuses.filter((status) => !status.ok).length,
        stage: "preflight",
      });
      const response = buildPreflightConflictResponse({
        mode: responseMode,
        commands: parsedRequest.commands,
        statuses: preflightStatuses,
      });
      logReceiveEnd(log, response.status, { reason: "preflight-ref-conflict", changed: false });
      return response;
    }

    if (!stockReceive && begin.stockPreparationReserved) {
      countReceiveSubrequest(cacheCtx, log, "do:promote-stock-preparation");
      const promoted = await limiter.run("do:promote-stock-preparation", () =>
        stub.promoteStockPreparation(begin.lease.token)
      );
      if (!promoted) {
        countReceiveSubrequest(cacheCtx, log, "do:abort-unpromoted-stock-preparation");
        await limiter
          .run("do:abort-unpromoted-stock-preparation", () => stub.abortReceive(begin.lease.token))
          .catch(() => {});
        logReceiveEnd(log, 503, { reason: "stock-preparation-active" });
        return new Response("Repository is busy receiving; please retry shortly.\n", {
          status: 503,
          headers: {
            "Retry-After": "10",
            "Content-Type": "text/plain; charset=utf-8",
          },
        });
      }
    }

    if (responseMode === "side-band-64k" && !stockReceive) {
      return createSidebandReceiveResponse({
        env,
        repoId,
        routeStartedAt,
        request,
        ctx,
        stub,
        log,
        cacheCtx,
        limiter,
        leaseToken: begin.lease.token,
        operationId: requestedOperationId,
        activeCatalog: begin.activeCatalog,
        catalogGeneration: begin.packsetVersion,
        commands: parsedRequest.commands,
        acceptedWrites,
        capabilities: parsedRequest.capabilities,
        packStream,
        bytesConsumed,
        onRepoStateChanged: options?.onRepoStateChanged,
      });
    }

    pipelineStarted = true;
    const executePipeline = nativeReceive ? executeNativeReceivePipeline : executeReceivePipeline;
    const result = await executePipeline({
      env,
      repoId,
      routeStartedAt,
      request,
      ctx,
      packStream,
      bytesConsumed,
      rawPrefix,
      stockReceive,
      stockRecovery: begin.stockRecovery,
      advertisedRefs: begin.refs,
      stub,
      leaseToken: begin.lease.token,
      operationId: requestedOperationId,
      activeCatalog: begin.activeCatalog,
      catalogGeneration: begin.packsetVersion,
      commands: parsedRequest.commands,
      acceptedWrites,
      log,
      cacheCtx,
      limiter,
      countSubrequest: (op, n = 1) => countReceiveSubrequest(cacheCtx, log, op, n),
    });

    scheduleRepoStateChange(ctx, options?.onRepoStateChanged, {
      changed: result.changed,
      empty: result.empty,
      commands: parsedRequest.commands,
      acceptedWrites,
    });

    const response = result.receivePackResponse
      ? new Response(Uint8Array.from(result.receivePackResponse).buffer, {
          status: 200,
          headers: {
            "Content-Type": "application/x-git-receive-pack-result",
            "Cache-Control": "no-store",
          },
        })
      : buildReceiveResultResponse({
          mode: "plain",
          reportStatusBody: result.reportStatusBody,
          changed: result.changed,
          empty: result.empty,
        });
    logReceiveEnd(log, response.status, {
      changed: result.changed,
      empty: result.empty,
      packKey: result.packKey,
      packBytes: result.packBytes,
    });
    return response;
  } catch (error) {
    if (!pipelineStarted) {
      countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
      await limiter
        .run("do:abort-receive", () => stub.abortReceive(begin.lease.token))
        .catch(() => {});
    }

    if (isReceiveAbort(request, error)) {
      log.info("receive:aborted", { error: String(error) });
      logReceiveEnd(log, 499, { reason: "client-aborted" });
      return clientAbortedResponse();
    }

    log.error("receive:error", { error: String(error) });

    if (error instanceof NativeReceiveIndeterminateError) {
      const response = new Response(`${error.message}\n`, {
        status: 503,
        headers: {
          "Content-Type": "text/plain; charset=utf-8",
          "Cache-Control": "no-store",
          "Retry-After": "5",
        },
      });
      logReceiveEnd(log, response.status, { reason: "native-receive-indeterminate" });
      return response;
    }

    if (error instanceof ReceivePipelineHttpError) {
      const response = new Response(`${error.message}\n`, {
        status: error.status,
        headers: { "Content-Type": "text/plain; charset=utf-8" },
      });
      logReceiveEnd(log, response.status, { reason: error.reason });
      return response;
    }

    const response = new Response(`${String(error)}\n`, {
      status: getErrorStatus(error),
      headers: { "Content-Type": "text/plain; charset=utf-8" },
    });
    logReceiveEnd(log, response.status, { reason: "error" });
    return response;
  }
}
