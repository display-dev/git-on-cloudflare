import type { CacheContext } from "@/worker/cache";
import type { ServeUploadPackPlan } from "../fetch/types";
import type { BeginRepositoryReadResult } from "@/worker/do/repo/repositoryLifecycle";

import { pktLine } from "@/worker/git/core";
import { responseCacheControl } from "@/worker/cache/policy";
import { createLogger, getRepoStub } from "@/worker/common";
import { getLimiter, countSubrequest } from "../limits";
import { parseFetchArgs } from "../args";
import { findCommonHaves } from "../closure";
import { buildAckOnlyResponse } from "../fetch/protocol";
import { repositoryNotReadyResponse } from "../fetch/responses";
import {
  buildServeUploadPackPlan,
  FetchPlanRetryError,
  loadUploadPackSnapshot,
} from "../fetch/plan";
import { resolvePackStreamResult } from "../fetch/execute";
import {
  closeSidebandWithFatal,
  SidebandProgressMux,
  emitProgress,
  pipePackWithSideband,
} from "../fetch/sideband";

export * from "../fetch/types";

let resolvePackForStream: typeof resolvePackStreamResult = resolvePackStreamResult;
let repositoryReadHeartbeatMs = 30_000;
let repositoryReadHeartbeatObserver: (() => void) | undefined;

export const __test = {
  setResolvePackStreamResult(value: typeof resolvePackStreamResult): void {
    resolvePackForStream = value;
  },
  setRepositoryReadHeartbeatMs(value: number): void {
    repositoryReadHeartbeatMs = value;
  },
  setRepositoryReadHeartbeatObserver(value: (() => void) | undefined): void {
    repositoryReadHeartbeatObserver = value;
  },
  reset(): void {
    resolvePackForStream = resolvePackStreamResult;
    repositoryReadHeartbeatMs = 30_000;
    repositoryReadHeartbeatObserver = undefined;
  },
};

function fetchPlanRetryResponse(error: FetchPlanRetryError): Response {
  return new Response("Repository fetch planning is not ready, please retry in a few moments.\n", {
    status: 503,
    headers: {
      "Retry-After": String(error.retryAfterSeconds),
      "Content-Type": "text/plain; charset=utf-8",
      "X-Git-Error": error.reason,
    },
  });
}

export async function handleFetchV2Streaming(
  env: Env,
  repoId: string,
  body: Uint8Array,
  signal?: AbortSignal,
  cacheCtx?: CacheContext
): Promise<Response> {
  const { wants, haves, done } = parseFetchArgs(body);
  const log = createLogger(env.LOG_LEVEL, { service: "StreamFetchV2", repoId });

  if (signal?.aborted) {
    return new Response("client aborted\n", { status: 499 });
  }

  if (wants.length === 0) {
    return buildAckOnlyResponse([], cacheCtx);
  }

  if (!done) {
    let ackOids: string[] = [];
    if (haves.length > 0) {
      ackOids = await findCommonHaves(env, repoId, haves, cacheCtx);
      log.debug("stream:fetch:negotiation", { haves: haves.length, acks: ackOids.length });
    }
    return buildAckOnlyResponse(ackOids, cacheCtx);
  }

  const limiter = getLimiter(cacheCtx);
  const repositoryStub = getRepoStub(env, repoId);
  countSubrequest(cacheCtx);
  const readLease = await limiter.run<BeginRepositoryReadResult>("do:begin-repository-read", () =>
    repositoryStub.beginRepositoryRead()
  );
  if (!readLease.ok) {
    return new Response("Repository read capacity is temporarily unavailable.\n", {
      status: 503,
      headers: { "Retry-After": "5", "Cache-Control": "no-store" },
    });
  }
  const operationAbort = new AbortController();
  if (signal) {
    signal.addEventListener("abort", () => operationAbort.abort(signal.reason), { once: true });
  }
  let heartbeatFailure: Error | undefined;
  let heartbeatTail = Promise.resolve();
  const heartbeat = setInterval(() => {
    heartbeatTail = heartbeatTail.then(async () => {
      if (heartbeatFailure) return;
      try {
        countSubrequest(cacheCtx);
        const renewed = await limiter.run("do:renew-repository-read", () =>
          repositoryStub.renewRepositoryRead(readLease.token)
        );
        if (!renewed) throw new Error("repository read lease could not be renewed");
        repositoryReadHeartbeatObserver?.();
      } catch (error) {
        heartbeatFailure = error instanceof Error ? error : new Error(String(error));
        operationAbort.abort(heartbeatFailure);
      }
    });
  }, repositoryReadHeartbeatMs);
  const assertReadLeaseHealthy = (): void => {
    if (heartbeatFailure) throw heartbeatFailure;
  };
  let readLeaseReleased = false;
  const releaseReadLease = async (): Promise<void> => {
    if (readLeaseReleased) return;
    readLeaseReleased = true;
    clearInterval(heartbeat);
    await heartbeatTail;
    countSubrequest(cacheCtx);
    await limiter.run("do:finish-repository-read", () =>
      repositoryStub.finishRepositoryRead(readLease.token)
    );
  };

  // Keep fetch readiness ahead of the response so clients still receive the
  // current 503 + Retry-After signal while the repository is not yet fetchable.
  const snapshotStart = Date.now();
  let snapshotLoad: Awaited<ReturnType<typeof loadUploadPackSnapshot>>;
  try {
    snapshotLoad = await loadUploadPackSnapshot(env, repoId, cacheCtx);
    assertReadLeaseHealthy();
  } catch (error) {
    await releaseReadLease();
    throw error;
  }
  if (snapshotLoad.type === "RepositoryNotReady") {
    log.warn("stream:fetch:repository-not-ready", { reason: snapshotLoad.reason });
    await releaseReadLease();
    return repositoryNotReadyResponse();
  }
  const snapshot = snapshotLoad.snapshot;

  if (env.QUALIFICATION_MODE === "1") {
    try {
      // A real reader already owns this snapshot and continues heartbeating.
      // The exact-target, operation-scoped latch never changes its lease.
      while (!operationAbort.signal.aborted) {
        countSubrequest(cacheCtx);
        const held = await limiter.run("do:gc-reader-latch", () =>
          repositoryStub.gcReaderLatch(
            readLease.token,
            snapshot.packs.map((pack) => pack.packKey),
            cacheCtx?.req.headers.get("X-Display-Operation-ID") ?? undefined
          )
        );
        if (!held) break;
        await new Promise((resolve) => setTimeout(resolve, 5_000));
        assertReadLeaseHealthy();
      }
      operationAbort.signal.throwIfAborted();
    } catch (error) {
      await releaseReadLease();
      throw error;
    }
  }

  log.info("stream:fetch:snapshot-ready", {
    wants: wants.length,
    haves: haves.length,
    packs: snapshot.packs.length,
    timeMs: Date.now() - snapshotStart,
  });

  const planStart = Date.now();
  log.info("stream:fetch:planning-start", {
    wants: wants.length,
    haves: haves.length,
  });
  let plan: ServeUploadPackPlan;
  try {
    plan = await buildServeUploadPackPlan(
      env,
      repoId,
      snapshot,
      wants,
      haves,
      operationAbort.signal,
      cacheCtx
    );
    assertReadLeaseHealthy();
  } catch (error) {
    if (error instanceof FetchPlanRetryError) {
      log.warn("stream:fetch:planning-retry", { reason: error.reason });
      await releaseReadLease();
      return fetchPlanRetryResponse(error);
    }
    await releaseReadLease();
    throw error;
  }
  log.info("stream:fetch:planning-complete", {
    needed: plan.neededOids.length,
    timeMs: Date.now() - planStart,
  });

  let resumeOnPull: (() => void) | undefined;
  const responseStream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const streamLog = createLogger(env.LOG_LEVEL, { service: "StreamFetchV2", repoId });
      const waitForCapacity = async (): Promise<void> => {
        while ((controller.desiredSize ?? 1) <= 0) {
          if (operationAbort.signal.aborted) {
            throw operationAbort.signal.reason ?? new Error("fetch response aborted");
          }
          await new Promise<void>((resolve) => {
            let settled = false;
            const resume = (): void => {
              if (settled) return;
              settled = true;
              operationAbort.signal.removeEventListener("abort", resume);
              if (resumeOnPull === resume) resumeOnPull = undefined;
              resolve();
            };
            resumeOnPull = resume;
            operationAbort.signal.addEventListener("abort", resume, { once: true });
            if (operationAbort.signal.aborted) resume();
          });
        }
      };
      try {
        controller.enqueue(pktLine("packfile\n"));
        // Once the response body has started, later failures must travel over
        // Git sideband because the HTTP status line is already committed.
        emitProgress(controller, "Preparing pack...\n");

        const progressMux = new SidebandProgressMux();
        const packResult = await resolvePackForStream(env, plan, {
          signal: plan.signal,
          limiter,
          countSubrequest: (n?: number) => countSubrequest(plan.cacheCtx, n),
          onProgress: (msg) => progressMux.push(msg),
        });
        assertReadLeaseHealthy();

        if (packResult.status !== "ok") {
          streamLog.warn("stream:fetch:assemble-unavailable", {
            needed: plan.neededOids.length,
            reason: packResult.failure.reason,
            retryable: packResult.failure.retryable,
            details: packResult.failure.details,
          });
          closeSidebandWithFatal(
            controller,
            `Unable to assemble pack: ${packResult.failure.reason}`
          );
          return;
        }

        await pipePackWithSideband(packResult.stream, controller, {
          signal: plan.signal,
          progressMux,
          log: streamLog,
          onChunk: async () => assertReadLeaseHealthy(),
          waitForCapacity,
        });

        controller.close();
      } catch (error) {
        streamLog.error("stream:response:error", { error: String(error) });
        try {
          // HTTP headers and possibly pack bytes are already committed.
          // Closing with a complete band-3 packet gives Git a conclusive
          // protocol error; controller.error() resets HTTP/2 and hides it.
          closeSidebandWithFatal(controller, String(error));
        } catch {
          controller.close();
        }
      } finally {
        try {
          await releaseReadLease();
        } catch (error) {
          streamLog.warn("stream:fetch:reader-release-failed", { error: String(error) });
        }
      }
    },
    pull() {
      resumeOnPull?.();
      resumeOnPull = undefined;
    },
    cancel(reason) {
      operationAbort.abort(reason);
      resumeOnPull?.();
      resumeOnPull = undefined;
    },
  });

  return new Response(responseStream, {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-upload-pack-result",
      "Cache-Control": responseCacheControl(cacheCtx),
    },
  });
}
