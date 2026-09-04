import type { CacheContext } from "@/worker/cache";
import type { Logger } from "@/worker/common/logger";
import type { OrderedPackSnapshot } from "@/worker/git/operations/fetch/types";
import type { RepoDurableObject } from "@/worker/do/repo/repoDO";
import type { BeginRepositoryMaintenanceResult } from "@/worker/do/repo/repositoryLifecycle";

import {
  type CompactionDeleteQueueMessage,
  type CompactionQueueMessage,
  type RepoQueueMessageHandle,
  SUPERSEDED_PACK_DELETE_DELAY_SECONDS,
} from "./types";

import { getRepoStubByDoId } from "@/worker/common";
import { buildCompactionNeededOids } from "@/worker/git/compaction/plan";
import { type SubrequestLimiter } from "@/worker/git/operations/limits";
import { scanPack, resolveDeltasAndWriteIdx } from "@/worker/git/pack/indexer";
import { rewritePackResult } from "@/worker/git/pack/rewrite";
import { loadOrderedPackSnapshot } from "@/worker/git/pack/snapshot";
import {
  deleteStagedPack,
  stagePackToR2,
  type StagedPackUpload,
} from "@/worker/git/receive/r2Upload";
import { doPrefix, packIndexKey, packRefsKey, r2PackDirPrefix, r2PackKey } from "@/worker/keys";
import {
  catalogMetadataBundleEnabled,
  catalogMetadataBundleKey,
} from "@/worker/git/nativeReceive/catalogMetadataBundle";
import { createQueueTaskContext, logSoftBudgetExhausted, retryQueueMessage } from "./context";
import {
  publishRepositoryGeneration,
  readPublishedRepositoryGeneration,
  readPublishedRepositoryGenerationState,
} from "@/worker/git/generation/publish";

const COMPACTION_SUBREQUEST_BUDGET = 7_500;
const COMPACTION_RETRY_DELAY_SECONDS = 30;
const COMPACTION_CONFLICT_RETRY_DELAY_SECONDS = 10;
const COMPACTION_DELETE_RETRY_SPREAD_SECONDS = 15;
const SUPERSEDED_CLEANUP_PAGE_SIZE = 250;

export function compactionDeleteRetryDelaySeconds(
  readerRetryAfterSeconds: number | undefined,
  body: Pick<CompactionDeleteQueueMessage, "packKeys" | "supersededAtGeneration">
): number {
  let hash = body.supersededAtGeneration ?? 0;
  for (const packKey of body.packKeys) {
    for (let index = 0; index < packKey.length; index++) {
      hash = Math.imul(hash ^ packKey.charCodeAt(index), 16_777_619) >>> 0;
    }
  }
  const spread = (hash % COMPACTION_DELETE_RETRY_SPREAD_SECONDS) + 1;
  return Math.max(1, readerRetryAfterSeconds ?? COMPACTION_RETRY_DELAY_SECONDS) + spread;
}

function countCompactionSubrequest(cacheCtx: CacheContext, log: Logger, op: string, n = 1): void {
  logSoftBudgetExhausted({
    cacheCtx,
    log,
    flagPrefix: "compaction-soft-budget",
    op,
    count: n,
  });
}

async function publishPendingGeneration(args: {
  env: Env;
  doId: string;
  stub: DurableObjectStub<RepoDurableObject>;
  limiter: SubrequestLimiter;
  cacheCtx: CacheContext;
  log: Logger;
}): Promise<boolean> {
  countCompactionSubrequest(args.cacheCtx, args.log, "do:begin-generation-publication");
  const maintenance = await args.limiter.run<BeginRepositoryMaintenanceResult>(
    "do:begin-generation-publication",
    () => args.stub.beginRepositoryMaintenance("generation-publication")
  );
  if (!maintenance.ok) return false;
  try {
    for (let attempt = 0; attempt < 2; attempt++) {
      countCompactionSubrequest(args.cacheCtx, args.log, "do:get-pending-generation");
      const pending = await args.limiter.run("do:get-pending-generation", () =>
        args.stub.getPendingGenerationPublication()
      );
      let publication = pending;
      if (!publication) {
        const publishedGeneration = await readPublishedRepositoryGeneration({
          env: args.env,
          doId: args.doId,
          limiter: args.limiter,
          countSubrequest: (op, n = 1) => countCompactionSubrequest(args.cacheCtx, args.log, op, n),
        });
        if (publishedGeneration !== null) return true;
        countCompactionSubrequest(args.cacheCtx, args.log, "do:ensure-generation-publication");
        publication = await args.limiter.run("do:ensure-generation-publication", () =>
          args.stub.ensureGenerationPublicationPending()
        );
      }
      await publishRepositoryGeneration({
        env: args.env,
        doId: args.doId,
        generation: publication.generation,
        activePackKeys: publication.activePackKeys,
        limiter: args.limiter,
        countSubrequest: (op, n = 1) => countCompactionSubrequest(args.cacheCtx, args.log, op, n),
        log: args.log,
      });
      countCompactionSubrequest(args.cacheCtx, args.log, "do:complete-generation-publication");
      const completed = await args.limiter.run("do:complete-generation-publication", () =>
        args.stub.completeGenerationPublication(publication.generation)
      );
      if (completed) return true;
    }
    throw new Error("repository generation publication advanced during reconciliation");
  } finally {
    countCompactionSubrequest(args.cacheCtx, args.log, "do:finish-generation-publication");
    await args.limiter.run("do:finish-generation-publication", () =>
      args.stub.finishRepositoryMaintenance(maintenance.token)
    );
  }
}

async function scheduleSupersededPackCleanup(args: {
  env: Env;
  doId: string;
  repoId?: string;
  stub: DurableObjectStub<RepoDurableObject>;
  limiter: SubrequestLimiter;
  cacheCtx: CacheContext;
  log: Logger;
  supersededAtGeneration?: number;
}): Promise<void> {
  const packKeyPages: string[][] = [];
  let cursor: { seqHi: number; tier: number; packKey: string } | undefined;
  for (;;) {
    countCompactionSubrequest(args.cacheCtx, args.log, "do:list-superseded-packs");
    const superseded = await args.limiter.run("do:list-superseded-packs", () =>
      args.stub.listSupersededGcPacks(cursor, SUPERSEDED_CLEANUP_PAGE_SIZE)
    );
    if (superseded.length === 0) break;
    packKeyPages.push(superseded.map((row) => row.packKey));
    if (superseded.length < SUPERSEDED_CLEANUP_PAGE_SIZE) break;
    const last = superseded.at(-1)!;
    cursor = { seqHi: last.seqHi, tier: last.tier, packKey: last.packKey };
  }
  if (packKeyPages.length === 0) return;
  const publishedGeneration =
    args.supersededAtGeneration ??
    (await readPublishedRepositoryGeneration({
      env: args.env,
      doId: args.doId,
      limiter: args.limiter,
      countSubrequest: (op, n = 1) => countCompactionSubrequest(args.cacheCtx, args.log, op, n),
    }));
  if (publishedGeneration === null) {
    throw new Error("superseded cleanup is waiting for a published repository generation");
  }
  for (const packKeys of packKeyPages) {
    countCompactionSubrequest(args.cacheCtx, args.log, "queue:compaction-delete");
    await args.limiter.run("queue:compaction-delete", () =>
      args.env.REPO_TASKS_QUEUE.send(
        {
          kind: "compaction-delete",
          doId: args.doId,
          repoId: args.repoId,
          packKeys,
          supersededAtGeneration: publishedGeneration,
          removeCatalogRows: true,
        },
        { delaySeconds: SUPERSEDED_PACK_DELETE_DELAY_SECONDS }
      )
    );
  }
}

async function cleanupStagedCompaction(args: {
  stagedUpload: StagedPackUpload | undefined;
  log: Logger;
  reason: string;
}) {
  if (!args.stagedUpload) return;
  try {
    await deleteStagedPack(args.stagedUpload);
  } catch (error) {
    args.log.warn("compaction:cleanup-failed", {
      reason: args.reason,
      packKey: args.stagedUpload.packKey,
      error: String(error),
    });
  }
}

async function abortCompactionLease(args: {
  stub: DurableObjectStub<RepoDurableObject>;
  leaseToken: string | undefined;
  limiter: SubrequestLimiter;
  cacheCtx: CacheContext;
  log: Logger;
  reason: string;
}) {
  const leaseToken = args.leaseToken;
  if (!leaseToken) return;
  try {
    countCompactionSubrequest(args.cacheCtx, args.log, "do:abort-compaction");
    const cleared = await args.limiter.run("do:abort-compaction", async () => {
      return await args.stub.abortCompaction(leaseToken);
    });
    if (!cleared) {
      args.log.warn("compaction:abort-missed", {
        reason: args.reason,
        leaseToken: args.leaseToken,
      });
      return;
    }
    args.log.info("compaction:abort-complete", {
      reason: args.reason,
      leaseToken,
    });
  } catch (error) {
    args.log.warn("compaction:abort-failed", {
      reason: args.reason,
      leaseToken: args.leaseToken,
      error: String(error),
    });
  }
}

async function clearCompactionRequestAfterBlocked(args: {
  stub: DurableObjectStub<RepoDurableObject>;
  limiter: SubrequestLimiter;
  cacheCtx: CacheContext;
  log: Logger;
  reason: string;
}): Promise<void> {
  try {
    countCompactionSubrequest(args.cacheCtx, args.log, "do:clear-compaction-request");
    await args.limiter.run("do:clear-compaction-request", async () => {
      await args.stub.clearCompactionRequest();
    });
    args.log.warn("compaction:blocked-cleared", { reason: args.reason });
  } catch (error) {
    args.log.warn("compaction:blocked-clear-failed", {
      reason: args.reason,
      error: String(error),
    });
  }
}

export async function handleCompactionMessage(
  message: Omit<RepoQueueMessageHandle<CompactionQueueMessage>, "body">,
  body: CompactionQueueMessage,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const repoLabel = body.repoId || `do:${body.doId}`;
  const task = createQueueTaskContext({
    env,
    ctx,
    repoLabel,
    operation: "compaction",
    subrequestBudget: COMPACTION_SUBREQUEST_BUDGET,
  });
  const log = task.logFor({
    service: "CompactionQueue",
    repoId: repoLabel,
    doId: body.doId,
  });
  const stub = getRepoStubByDoId(env, body.doId) as DurableObjectStub<RepoDurableObject>;
  const { cacheCtx, limiter } = task;

  let stagedUpload: StagedPackUpload | undefined;
  let leaseToken: string | undefined;
  let finalCleanup:
    | { packCatalogVersion: number; supersededCount: number }
    | undefined;

  try {
    if (!(await publishPendingGeneration({ env, doId: body.doId, stub, limiter, cacheCtx, log }))) {
      message.ack();
      return;
    }
    countCompactionSubrequest(cacheCtx, log, "do:begin-compaction");
    const begin = await limiter.run("do:begin-compaction", async () => {
      return await stub.beginCompaction();
    });
    if (!begin.ok) {
      if (
        begin.status === "busy" &&
        (begin.reason === "receive-active" ||
          begin.reason === "compact-active" ||
          begin.reason === "recent-activity")
      ) {
        log.info("compaction:busy-retry", { reason: begin.reason });
        retryQueueMessage(message, begin.retryAfter);
        return;
      }

      log.info("compaction:skip", {
        status: begin.status,
        reason: "reason" in begin ? begin.reason : undefined,
      });
      // A recorded request whose catalog is already below threshold is a
      // genuine recovery opportunity. A message with no recorded request is
      // a stale duplicate after another pass completed; sweeping there would
      // multiply historical delete work for every duplicate delivery.
      if (begin.status === "no_work" && begin.reason === "below-threshold") {
        await scheduleSupersededPackCleanup({
          env,
          doId: body.doId,
          repoId: body.repoId,
          stub,
          limiter,
          cacheCtx,
          log,
        });
      }
      message.ack();
      return;
    }

    leaseToken = begin.lease.token;
    cacheCtx.memo = cacheCtx.memo || {};
    cacheCtx.memo.packCatalog = begin.activeCatalog;

    const snapshotLoad = await loadOrderedPackSnapshot(env, repoLabel, cacheCtx, log);
    if (snapshotLoad.type !== "Ready") {
      log.warn("compaction:snapshot-unavailable", { reason: snapshotLoad.reason });
      await abortCompactionLease({
        stub,
        leaseToken,
        limiter,
        cacheCtx,
        log,
        reason: snapshotLoad.reason,
      });
      retryQueueMessage(message, COMPACTION_RETRY_DELAY_SECONDS);
      return;
    }

    const snapshot = snapshotLoad.snapshot;
    const sourcePackMap = new Map(snapshot.packs.map((pack) => [pack.packKey, pack]));
    const sourcePacks = begin.sourcePacks
      .map((row) => sourcePackMap.get(row.packKey))
      .filter((pack): pack is (typeof snapshot.packs)[number] => pack !== undefined);
    if (sourcePacks.length !== begin.sourcePacks.length) {
      log.warn("compaction:source-pack-missing", {
        expected: begin.sourcePacks.length,
        actual: sourcePacks.length,
      });
      await abortCompactionLease({
        stub,
        leaseToken,
        limiter,
        cacheCtx,
        log,
        reason: "source-pack-missing",
      });
      retryQueueMessage(message, COMPACTION_CONFLICT_RETRY_DELAY_SECONDS);
      return;
    }

    const neededOids = buildCompactionNeededOids(sourcePacks);
    log.info("compaction:rewrite-start", {
      sourceTier: begin.targetTier - 1,
      targetTier: begin.targetTier,
      sourceCount: begin.sourcePacks.length,
      neededCount: neededOids.length,
    });

    // Build a compaction-specific snapshot: source packs first so
    // resolveOrderedEntryByOid picks authoritative source entries for needed
    // OIDs, then remaining active packs in their normal newest-first order
    // for delta base closure. Without this reorder, a duplicate identity
    // REF_DELTA in a newer non-source pack can shadow the source entry and
    // create a self-referential delta cycle in the topology sort.
    const sourceKeySet = new Set(begin.sourcePacks.map((row) => row.packKey));
    const fallbackPacks = snapshot.packs.filter((pack) => !sourceKeySet.has(pack.packKey));
    const compactionSnapshot: OrderedPackSnapshot = {
      packs: [...sourcePacks, ...fallbackPacks],
    };

    const rewriteResult = await rewritePackResult(env, compactionSnapshot, neededOids, {
      limiter,
      countSubrequest: (n) => countCompactionSubrequest(cacheCtx, log, "r2:rewrite-pack", n),
    });
    if (rewriteResult.status !== "ok") {
      log.warn("compaction:rewrite-unavailable", {
        reason: rewriteResult.failure.reason,
        retryable: rewriteResult.failure.retryable,
        details: rewriteResult.failure.details,
      });
      await abortCompactionLease({
        stub,
        leaseToken,
        limiter,
        cacheCtx,
        log,
        reason: rewriteResult.failure.reason,
      });

      if (rewriteResult.failure.retryable) {
        retryQueueMessage(message, COMPACTION_RETRY_DELAY_SECONDS);
        return;
      }

      leaseToken = undefined;
      await clearCompactionRequestAfterBlocked({
        stub,
        limiter,
        cacheCtx,
        log,
        reason: rewriteResult.failure.reason,
      });
      log.error("compaction:blocked", {
        reason: rewriteResult.failure.reason,
        sourceCount: begin.sourcePacks.length,
        sourceSeqLo: begin.sourcePacks[0]?.seqLo,
        sourceSeqHi: begin.sourcePacks[begin.sourcePacks.length - 1]?.seqHi,
      });
      // Earlier passes may already have committed superseded rows before a
      // later pass discovers an uncompactable source set. Reconcile once after
      // clearing the request; duplicate deliveries then become not-requested.
      await scheduleSupersededPackCleanup({
        env,
        doId: body.doId,
        repoId: body.repoId,
        stub,
        limiter,
        cacheCtx,
        log,
      });
      message.ack();
      return;
    }

    const packKey = r2PackKey(doPrefix(body.doId), `pack-cmp-${begin.lease.token}.pack`);
    stagedUpload = await stagePackToR2({
      env,
      request: new Request(`https://queue.internal/${encodeURIComponent(repoLabel)}/compact-pack`),
      packStream: rewriteResult.stream,
      packKey,
      bytesConsumed: 0,
      limiter,
      countSubrequest: (op, n = 1) => countCompactionSubrequest(cacheCtx, log, op, n),
    });

    const scanResult = await scanPack({
      env,
      packKey: stagedUpload.packKey,
      packSize: stagedUpload.packBytes,
      limiter,
      countSubrequest: (n = 1) => countCompactionSubrequest(cacheCtx, log, "r2:scan-pack", n),
      log,
    });
    const resolveResult = await resolveDeltasAndWriteIdx({
      env,
      packKey: stagedUpload.packKey,
      packSize: stagedUpload.packBytes,
      limiter,
      countSubrequest: (n = 1) => countCompactionSubrequest(cacheCtx, log, "r2:resolve-pack", n),
      log,
      scanResult,
      activeCatalog: begin.activeCatalog,
      cacheCtx,
      repoId: repoLabel,
    });

    countCompactionSubrequest(cacheCtx, log, "do:commit-compaction");
    const committedUpload = stagedUpload;
    const commit = await limiter.run("do:commit-compaction", async () => {
      return await stub.commitCompaction({
        token: begin.lease.token,
        sourcePacks: begin.sourcePacks,
        targetTier: begin.targetTier,
        packsetVersion: begin.packsetVersion,
        stagedPack: {
          packKey: committedUpload.packKey,
          packBytes: committedUpload.packBytes,
          idxBytes: resolveResult.idxBytes,
          objectCount: resolveResult.objectCount,
        },
      });
    });

    if (commit.status === "retry") {
      await cleanupStagedCompaction({
        stagedUpload,
        log,
        reason: commit.reason,
      });
      leaseToken = undefined;
      log.info("compaction:retry", { reason: commit.reason });
      retryQueueMessage(message, COMPACTION_CONFLICT_RETRY_DELAY_SECONDS);
      return;
    }

    leaseToken = undefined;
    stagedUpload = undefined;
    if (!commit.shouldRequeue) {
      finalCleanup = {
        packCatalogVersion: commit.packCatalogVersion,
        supersededCount: commit.supersededPackKeys.length,
      };
    }
    if (catalogMetadataBundleEnabled(env)) {
      const staleBundleKey = await catalogMetadataBundleKey(begin.activeCatalog);
      ctx.waitUntil(
        limiter
          .run("r2:delete-compacted-catalog-metadata", () => {
            countCompactionSubrequest(cacheCtx, log, "r2:delete-compacted-catalog-metadata");
            return env.REPO_BUCKET.delete(staleBundleKey);
          })
          .catch((error) => {
            log.warn("compaction:catalog-metadata-retire-failed", {
              key: staleBundleKey,
              error: String(error),
            });
          })
      );
    }
    if (commit.shouldRequeue) {
      ctx.waitUntil(
        env.REPO_TASKS_QUEUE.send({
          kind: "compaction",
          doId: body.doId,
          repoId: body.repoId,
        }).catch((error) => {
          log.warn("compaction:follow-up-enqueue-failed", { error: String(error) });
        })
      );
    }

    if (!(await publishPendingGeneration({ env, doId: body.doId, stub, limiter, cacheCtx, log }))) {
      if (finalCleanup) {
        log.warn("compaction:final-cleanup-deferred", {
          reason: "generation-publication-busy",
          ...finalCleanup,
        });
      }
      message.ack();
      return;
    }
    // One reconciliation after the final pass covers every superseded row.
    // Scheduling intermediate exact-key work as well would make the final
    // sweep duplicate those messages while readers still hold generations.
    if (!commit.shouldRequeue) {
      await scheduleSupersededPackCleanup({
        env,
        doId: body.doId,
        repoId: body.repoId,
        stub,
        limiter,
        cacheCtx,
        log,
        supersededAtGeneration: commit.packCatalogVersion,
      });
    }

    log.info("compaction:done", {
      targetPackKey: commit.targetPackKey,
      supersededCount: commit.supersededPackKeys.length,
      shouldRequeue: commit.shouldRequeue,
    });
    message.ack();
  } catch (error) {
    if (finalCleanup) {
      log.warn("compaction:final-cleanup-deferred", {
        reason: "post-commit-error",
        ...finalCleanup,
        error: String(error),
      });
    }
    log.error("compaction:error", { error: String(error) });
    await cleanupStagedCompaction({
      stagedUpload,
      log,
      reason: "error",
    });
    await abortCompactionLease({
      stub,
      leaseToken,
      limiter,
      cacheCtx,
      log,
      reason: "error",
    });
    retryQueueMessage(message, COMPACTION_RETRY_DELAY_SECONDS);
  }
}

export async function handleCompactionDeleteMessage(
  message: Omit<RepoQueueMessageHandle<CompactionDeleteQueueMessage>, "body">,
  body: CompactionDeleteQueueMessage,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const repoLabel = body.repoId || `do:${body.doId}`;
  const task = createQueueTaskContext({
    env,
    ctx,
    repoLabel,
    operation: "compaction-delete",
    subrequestBudget: 25,
  });
  const log = task.logFor({
    service: "CompactionDeleteQueue",
    repoId: repoLabel,
    doId: body.doId,
  });
  const { cacheCtx, limiter } = task;

  try {
    const readerStub = getRepoStubByDoId(env, body.doId) as DurableObjectStub<RepoDurableObject>;
    const ownedPackPrefix = r2PackDirPrefix(doPrefix(body.doId));
    if (body.packKeys.some((packKey) => !packKey.startsWith(ownedPackPrefix))) {
      log.warn("compaction:delete-invalid-pack-owner");
      message.ack();
      return;
    }
    const published = await readPublishedRepositoryGenerationState({
      env,
      doId: body.doId,
      limiter,
      countSubrequest: (op, n = 1) => countCompactionSubrequest(cacheCtx, log, op, n),
    });
    const generationTooOld =
      body.supersededAtGeneration !== undefined &&
      (published === null || published.generation < body.supersededAtGeneration);
    const stillPublished =
      published === null || body.packKeys.some((packKey) => published.activePackKeys.has(packKey));
    if (generationTooOld || stillPublished) {
      log.info("compaction:delete-generation-pending", {
        supersededAtGeneration: body.supersededAtGeneration,
        publishedGeneration: published?.generation,
        stillPublished,
      });
      retryQueueMessage(message, COMPACTION_RETRY_DELAY_SECONDS);
      return;
    }
    countCompactionSubrequest(cacheCtx, log, "do:check-native-reader-generation");
    const deletionGeneration = body.supersededAtGeneration ?? published.generation;
    const readerFence = await limiter.run("do:check-native-reader-generation", async () => {
      return await readerStub.canDeleteSupersededGeneration(deletionGeneration);
    });
    if (!readerFence.safe) {
      log.info("compaction:delete-reader-active", {
        supersededAtGeneration: deletionGeneration,
        retryAfterSeconds: readerFence.retryAfterSeconds,
      });
      retryQueueMessage(
        message,
        compactionDeleteRetryDelaySeconds(readerFence.retryAfterSeconds, body)
      );
      return;
    }
    let confirmedPackKeys = body.packKeys;
    let cleanupStub: DurableObjectStub<RepoDurableObject> | undefined;
    if (body.removeCatalogRows) {
      cleanupStub = getRepoStubByDoId(env, body.doId) as DurableObjectStub<RepoDurableObject>;
      countCompactionSubrequest(cacheCtx, log, "do:claim-superseded-packs");
      const claim = await limiter.run("do:claim-superseded-packs", async () => {
        return await cleanupStub!.claimSupersededGcPacks(body.packKeys);
      });
      if (claim.status === "retry") {
        log.warn("compaction:delete-claim-retry", { reason: claim.reason });
        retryQueueMessage(message, COMPACTION_RETRY_DELAY_SECONDS);
        return;
      }
      confirmedPackKeys = claim.packKeys;
    }

    if (confirmedPackKeys.length === 0) {
      log.info("compaction:delete-complete", { packCount: 0, artifactCount: 0 });
      message.ack();
      return;
    }

    const keysToDelete: string[] = [];
    for (const packKey of confirmedPackKeys) {
      // Each superseded pack has three derived immutable artifacts in R2:
      // the pack bytes, the idx, and the logical-reference sidecar.
      keysToDelete.push(packKey, packIndexKey(packKey), packRefsKey(packKey));
    }

    if (keysToDelete.length > 0) {
      countCompactionSubrequest(cacheCtx, log, "r2:delete-superseded-packs");
      await limiter.run("r2:delete-superseded-packs", async () => {
        await env.REPO_BUCKET.delete(keysToDelete);
      });
    }
    if (body.removeCatalogRows) {
      countCompactionSubrequest(cacheCtx, log, "do:remove-superseded-packs");
      const removal = await limiter.run("do:remove-superseded-packs", async () => {
        return await cleanupStub!.removeSupersededGcPacks(confirmedPackKeys);
      });
      if (removal.status === "retry") {
        log.warn("compaction:delete-metadata-retry", { reason: removal.reason });
        retryQueueMessage(message, COMPACTION_RETRY_DELAY_SECONDS);
        return;
      }
    }
    log.info("compaction:delete-complete", {
      packCount: confirmedPackKeys.length,
      artifactCount: keysToDelete.length,
    });
    message.ack();
  } catch (error) {
    log.warn("compaction:delete-failed", { error: String(error) });
    retryQueueMessage(message, COMPACTION_RETRY_DELAY_SECONDS);
  }
}
