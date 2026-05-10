import type { CacheContext } from "@/worker/cache";
import type { Logger } from "@/worker/common/logger";
import type { RepoDurableObject } from "@/worker/do/repo/repoDO";

import { type PackRefBackfillQueueMessage, type RepoQueueMessageHandle } from "./types";

import { getRepoStubByDoId } from "@/worker/common";
import { loadIdxView } from "@/worker/git/object-store";
import { resolveDeltasAndWriteIdx, scanPack } from "@/worker/git/pack/indexer";
import { loadPackRefView } from "@/worker/git/pack/refIndex";
import { createQueueTaskContext, logSoftBudgetExhausted, retryQueueMessage } from "./context";

const REF_BACKFILL_SUBREQUEST_BUDGET = 7_500;
const REF_BACKFILL_RETRY_DELAY_SECONDS = 30;

function countBackfillSubrequest(cacheCtx: CacheContext, log: Logger, op: string, n = 1): void {
  logSoftBudgetExhausted({
    cacheCtx,
    log,
    flagPrefix: "ref-backfill-soft-budget",
    op,
    count: n,
  });
}

function isDeterministicPackFailure(error: unknown): boolean {
  const message = String(error);
  return (
    message.includes("invalid") ||
    message.includes("mismatch") ||
    message.includes("unsupported") ||
    message.includes("truncated") ||
    message.includes("cannot fit")
  );
}

export async function handlePackRefBackfillMessage(
  message: Omit<RepoQueueMessageHandle<PackRefBackfillQueueMessage>, "body">,
  body: PackRefBackfillQueueMessage,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const repoLabel = body.repoId || `do:${body.doId}`;
  const task = createQueueTaskContext({
    env,
    ctx,
    repoLabel,
    operation: "pack-refs",
    subrequestBudget: REF_BACKFILL_SUBREQUEST_BUDGET,
  });
  const log = task.logFor({
    service: "PackRefBackfillQueue",
    repoId: repoLabel,
    doId: body.doId,
  });
  const stub = getRepoStubByDoId(env, body.doId) as DurableObjectStub<RepoDurableObject>;
  const { cacheCtx, limiter } = task;

  try {
    log.info("ref-index:backfill-start", { packKey: body.packKey });

    countBackfillSubrequest(cacheCtx, log, "do:get-active-pack-catalog");
    const activeCatalog = await limiter.run("do:get-active-pack-catalog", async () => {
      return await stub.getActivePackCatalog();
    });
    cacheCtx.memo = cacheCtx.memo || {};
    cacheCtx.memo.packCatalog = activeCatalog;

    const target = activeCatalog.find((row) => row.packKey === body.packKey);
    if (!target) {
      log.info("ref-index:backfill-stale-pack", { packKey: body.packKey });
      message.ack();
      return;
    }
    const externalBaseCatalog = activeCatalog.filter((row) => row.packKey !== target.packKey);
    log.debug("ref-index:backfill-resolve-catalog", {
      packKey: target.packKey,
      activePacks: activeCatalog.length,
      externalBasePacks: externalBaseCatalog.length,
    });

    const idxView = await loadIdxView(env, target.packKey, cacheCtx, target.packBytes);
    if (!idxView) {
      log.warn("ref-index:backfill-invalid-pack", {
        packKey: target.packKey,
        reason: "missing-or-invalid-idx",
      });
      message.ack();
      return;
    }

    const existing = await loadPackRefView(env, target.packKey, idxView, cacheCtx);
    if (existing.type === "Ready") {
      log.info("ref-index:backfill-complete", {
        packKey: target.packKey,
        result: "already-present",
      });
      message.ack();
      return;
    }

    const scanResult = await scanPack({
      env,
      packKey: target.packKey,
      packSize: target.packBytes,
      limiter,
      countSubrequest: (n = 1) => countBackfillSubrequest(cacheCtx, log, "r2:scan-pack", n),
      log,
    });

    cacheCtx.memo.packCatalog = externalBaseCatalog;
    const resolveResult = await resolveDeltasAndWriteIdx({
      env,
      packKey: target.packKey,
      packSize: target.packBytes,
      limiter,
      countSubrequest: (n = 1) => countBackfillSubrequest(cacheCtx, log, "r2:resolve-pack", n),
      log,
      scanResult,
      activeCatalog: externalBaseCatalog,
      cacheCtx,
      repoId: repoLabel,
      writeIdx: false,
      existingIdxView: idxView,
    });

    log.info("ref-index:backfill-complete", {
      packKey: target.packKey,
      objectCount: resolveResult.objectCount,
      refIndexBytes: resolveResult.refIndexBytes,
    });
    message.ack();
  } catch (error) {
    if (isDeterministicPackFailure(error)) {
      log.warn("ref-index:backfill-invalid-pack", {
        packKey: body.packKey,
        error: String(error),
      });
      message.ack();
      return;
    }

    log.warn("ref-index:backfill-retry", {
      packKey: body.packKey,
      error: String(error),
    });
    retryQueueMessage(message, REF_BACKFILL_RETRY_DELAY_SECONDS);
  }
}
