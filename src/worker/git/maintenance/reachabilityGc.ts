import type { CacheContext } from "@/worker/cache";
import type { Logger } from "@/worker/common/logger";
import type {
  BeginReachabilityGcResult,
  CommitReachabilityGcResult,
  ReconcileReachabilityGcPendingResult,
  RecordReachabilityGcPendingResult,
} from "@/worker/do/repo/catalog/reachabilityGc";
import type { RepoDurableObject } from "@/worker/do/repo/repoDO";
import type { Limiter } from "@/worker/git/operations/limits";
import type { StagedPackUpload } from "@/worker/git/receive/r2Upload";
import type { IdxView } from "@/worker/git/object-store/types";

import { bytesToHex, getRepoStub } from "@/worker/common";
import { computeNeededFromPackRefs } from "@/worker/git/operations/fetch/refClosure";
import { loadPackRefSnapshot } from "@/worker/git/operations/fetch/plan";
import { scanPack, resolveDeltasAndWriteIdx } from "@/worker/git/pack/indexer";
import { rewritePackResult } from "@/worker/git/pack/rewrite";
import { loadOrderedPackSnapshot } from "@/worker/git/pack/snapshot";
import {
  deleteStagedPack,
  deleteStagedPackArtifacts,
  stagePackToR2,
} from "@/worker/git/receive/r2Upload";
import { doPrefix, getDoIdFromPath, r2PackKey } from "@/worker/keys";
import { SUPERSEDED_PACK_DELETE_DELAY_SECONDS } from "@/worker/tasks/types";
import {
  publishRepositoryGeneration,
  readPublishedRepositoryGeneration,
} from "@/worker/git/generation/publish";
import type { BeginRepositoryMaintenanceResult } from "@/worker/do/repo/repositoryLifecycle";

let failCommitResponsesForTesting = 0;
let failCommitTransportsForTesting = 0;
let afterLostCommitForTesting: (() => Promise<void>) | undefined;

const MAX_REACHABILITY_GC_SOURCE_PACKS = 250;
export const SUPERSEDED_PACK_CLEANUP_CHUNK_SIZE = 250;

export const __test = {
  failNextCommitResponse(afterCommit?: () => Promise<void>, count = 1): void {
    failCommitResponsesForTesting = count;
    afterLostCommitForTesting = afterCommit;
  },
  failNextCommitTransports(count = 1): void {
    failCommitTransportsForTesting = count;
  },
  reset(): void {
    failCommitResponsesForTesting = 0;
    failCommitTransportsForTesting = 0;
    afterLostCommitForTesting = undefined;
  },
};

export type ReachabilityGcResult =
  | {
      status: "completed";
      reachableObjects: number;
      sourcePacks: number;
      scheduledArtifacts: number;
      packCatalogVersion: number;
    }
  | {
      status: "retry";
      reason: string;
    }
  | {
      status: "blocked";
      reason: string;
    };

function blockedResult(log: Logger, reason: string): ReachabilityGcResult {
  log.error("reachability-gc:blocked", { reason });
  return { status: "blocked", reason };
}

function targetIdxMatchesClosure(idx: IdxView, reachableOids: string[]): boolean {
  if (idx.count !== reachableOids.length) return false;
  const remaining = new Set(reachableOids);
  if (remaining.size !== reachableOids.length) return false;
  for (let index = 0; index < idx.count; index++) {
    const oid = bytesToHex(idx.rawNames.subarray(index * 20, index * 20 + 20));
    if (!remaining.delete(oid)) return false;
  }
  return remaining.size === 0;
}

export function chunkSupersededPackKeys(packKeys: string[]): string[][] {
  const chunks: string[][] = [];
  for (let offset = 0; offset < packKeys.length; offset += SUPERSEDED_PACK_CLEANUP_CHUNK_SIZE) {
    chunks.push(packKeys.slice(offset, offset + SUPERSEDED_PACK_CLEANUP_CHUNK_SIZE));
  }
  return chunks;
}

function retryResult(log: Logger, reason: string): ReachabilityGcResult {
  log.warn("reachability-gc:retry", { reason });
  return { status: "retry", reason };
}

export async function publishPendingGeneration(args: {
  env: Env;
  repoId: string;
  stub: DurableObjectStub<RepoDurableObject>;
  limiter: Limiter;
  log: Logger;
  countSubrequest(op: string, n?: number): void;
}): Promise<boolean> {
  args.countSubrequest("do:begin-generation-publication");
  const maintenance = await args.limiter.run<BeginRepositoryMaintenanceResult>(
    "do:begin-generation-publication",
    () => args.stub.beginRepositoryMaintenance("generation-publication")
  );
  if (!maintenance.ok) return false;
  try {
    args.countSubrequest("do:get-pending-generation");
    let pending = await args.limiter.run("do:get-pending-generation", () =>
      args.stub.getPendingGenerationPublication()
    );
    if (!pending) {
      const publishedGeneration = await readPublishedRepositoryGeneration({
        env: args.env,
        doId: args.stub.id.toString(),
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
      });
      if (publishedGeneration !== null) return true;
      args.countSubrequest("do:ensure-generation-publication");
      pending = await args.limiter.run("do:ensure-generation-publication", () =>
        args.stub.ensureGenerationPublicationPending()
      );
    }
    await publishRepositoryGeneration({
      env: args.env,
      doId: args.stub.id.toString(),
      generation: pending.generation,
      activePackKeys: pending.activePackKeys,
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
      log: args.log,
    });
    args.countSubrequest("do:complete-generation-publication");
    return await args.limiter.run("do:complete-generation-publication", () =>
      args.stub.completeGenerationPublication(pending.generation)
    );
  } finally {
    args.countSubrequest("do:finish-generation-publication");
    await args.limiter.run("do:finish-generation-publication", () =>
      args.stub.finishRepositoryMaintenance(maintenance.token)
    );
  }
}

export async function scheduleSupersededPackCleanup(args: {
  env: Env;
  repoId: string;
  packKeys: string[];
  limiter: Limiter;
  log: Logger;
  countSubrequest(op: string, n?: number): void;
  supersededAtGeneration?: number;
}): Promise<boolean> {
  if (args.packKeys.length === 0) return true;
  const doId = getDoIdFromPath(args.packKeys[0]!);
  if (!doId) throw new Error("superseded pack has no Durable Object prefix");
  args.log.info("reachability-gc:cleanup-enqueue-start", {
    packCount: args.packKeys.length,
    delaySeconds: SUPERSEDED_PACK_DELETE_DELAY_SECONDS,
  });
  try {
    for (const packKeys of chunkSupersededPackKeys(args.packKeys)) {
      if (packKeys.some((packKey) => getDoIdFromPath(packKey) !== doId)) {
        throw new Error("superseded cleanup spans Durable Object prefixes");
      }
      args.countSubrequest("queue:reachability-gc-delete");
      await args.limiter.run("queue:reachability-gc-delete", () =>
        args.env.REPO_TASKS_QUEUE.send(
          {
            kind: "compaction-delete",
            doId,
            repoId: args.repoId,
            packKeys,
            removeCatalogRows: true,
            ...(typeof args.supersededAtGeneration === "number"
              ? { supersededAtGeneration: args.supersededAtGeneration }
              : {}),
          },
          { delaySeconds: SUPERSEDED_PACK_DELETE_DELAY_SECONDS }
        )
      );
    }
    args.log.info("reachability-gc:cleanup-enqueued", {
      packCount: args.packKeys.length,
      delaySeconds: SUPERSEDED_PACK_DELETE_DELAY_SECONDS,
    });
    return true;
  } catch (error) {
    args.log.warn("reachability-gc:cleanup-enqueue-failed", {
      packCount: args.packKeys.length,
      error: String(error),
    });
    return false;
  }
}

export async function reconcilePriorCleanup(args: {
  env: Env;
  repoId: string;
  stub: DurableObjectStub<RepoDurableObject>;
  limiter: Limiter;
  log: Logger;
  countSubrequest(op: string, n?: number): void;
}): Promise<ReachabilityGcResult | undefined> {
  args.countSubrequest("do:list-superseded-gc-packs");
  const rows = await args.limiter.run("do:list-superseded-gc-packs", () =>
    args.stub.listSupersededGcPacks()
  );
  if (rows.length === 0) return undefined;
  const scheduled = await scheduleSupersededPackCleanup({
    ...args,
    packKeys: rows.map((row) => row.packKey),
  });
  return retryResult(args.log, scheduled ? "cleanup-scheduled" : "cleanup-enqueue-failed");
}

export async function reconcilePendingGc(args: {
  env: Env;
  stub: DurableObjectStub<RepoDurableObject>;
  limiter: Limiter;
  log: Logger;
  countSubrequest(op: string, n?: number): void;
}): Promise<ReachabilityGcResult | undefined> {
  args.countSubrequest("do:reconcile-pending-reachability-gc");
  const pending = await args.limiter.run<ReconcileReachabilityGcPendingResult>(
    "do:reconcile-pending-reachability-gc",
    () => args.stub.reconcileReachabilityGcPending()
  );
  if (pending.status === "none" || pending.status === "committed") return undefined;
  if (pending.status === "wait") return retryResult(args.log, "pending-commit-outcome");

  try {
    await deleteStagedPackArtifacts({
      env: args.env,
      packKey: pending.packKey,
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
    });
  } catch (error) {
    args.log.warn("reachability-gc:pending-cleanup-failed", { error: String(error) });
    return retryResult(args.log, "pending-cleanup-failed");
  }
  args.countSubrequest("do:complete-pending-reachability-gc-cleanup");
  const completed = await args.limiter.run("do:complete-pending-reachability-gc-cleanup", () =>
    args.stub.completeReachabilityGcPendingCleanup({
      token: pending.token,
      packKey: pending.packKey,
    })
  );
  return completed ? undefined : retryResult(args.log, "pending-cleanup-outcome-changed");
}

async function commitWithReconciliation(args: {
  stub: DurableObjectStub<RepoDurableObject>;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
  commitArgs: Parameters<RepoDurableObject["commitReachabilityGc"]>[0];
  log: Logger;
  onRpcStart(): void;
}): Promise<CommitReachabilityGcResult> {
  const commitOnce = async (op: string): Promise<CommitReachabilityGcResult> => {
    args.countSubrequest(op);
    args.onRpcStart();
    if (failCommitTransportsForTesting > 0) {
      failCommitTransportsForTesting -= 1;
      throw new Error("injected reachability GC transport failure before DO");
    }
    const result = await args.limiter.run<CommitReachabilityGcResult>(op, () =>
      args.stub.commitReachabilityGc(args.commitArgs)
    );
    if (failCommitResponsesForTesting > 0) {
      failCommitResponsesForTesting -= 1;
      const afterCommit = afterLostCommitForTesting;
      afterLostCommitForTesting = undefined;
      await afterCommit?.();
      throw new Error("injected lost reachability GC commit response");
    }
    return result;
  };

  try {
    return await commitOnce("do:commit-reachability-gc");
  } catch (error) {
    args.log.warn("reachability-gc:commit-response-ambiguous", { error: String(error) });
    // The DO's atomic SQL catalog is the outcome record. Repeating the same
    // token and target reconciles an already-committed replacement without
    // risking deletion of the now-active staged artifacts.
    return await commitOnce("do:reconcile-reachability-gc");
  }
}

/**
 * Rewrite the complete active catalog to exactly the objects reachable from
 * the authoritative refs captured under the DO's compaction lease.
 *
 * The Worker owns R2 reads/writes; the DO atomically CAS-commits its metadata.
 * Superseded artifacts use the same delayed cleanup queue as compaction so
 * readers that already captured the old immutable catalog retain a drain
 * window before those objects are removed.
 */
export async function runReachabilityGc(args: {
  env: Env;
  repoId: string;
  cacheCtx: CacheContext;
  limiter: Limiter;
  log: Logger;
  countSubrequest(op: string, n?: number): void;
}): Promise<ReachabilityGcResult> {
  const stub = getRepoStub(args.env, args.repoId);
  args.cacheCtx.memo = args.cacheCtx.memo || {};
  args.cacheCtx.memo.limiter = args.limiter;
  if (!(await publishPendingGeneration({ ...args, stub }))) {
    return retryResult(args.log, "generation-publication-advanced");
  }
  const pending = await reconcilePendingGc({ ...args, stub });
  if (pending) return pending;
  const priorCleanup = await reconcilePriorCleanup({ ...args, stub });
  if (priorCleanup) return priorCleanup;

  args.countSubrequest("do:begin-reachability-gc");
  const begin = await args.limiter.run<BeginReachabilityGcResult>("do:begin-reachability-gc", () =>
    stub.beginReachabilityGc()
  );
  if (!begin.ok) return retryResult(args.log, begin.reason);

  let stagedUpload: StagedPackUpload | undefined;
  let pendingRecorded = false;
  let commitAttempted = false;
  try {
    if (begin.activeCatalog.length === 0) {
      args.countSubrequest("do:abort-empty-reachability-gc");
      await args.limiter.run("do:abort-empty-reachability-gc", () =>
        stub.abortCompaction(begin.lease.token)
      );
      if (begin.refs.length > 0) return retryResult(args.log, "active-catalog-empty");
      return {
        status: "completed",
        reachableObjects: 0,
        sourcePacks: 0,
        scheduledArtifacts: 0,
        packCatalogVersion: begin.packsetVersion,
      };
    }
    if (begin.activeCatalog.length > MAX_REACHABILITY_GC_SOURCE_PACKS) {
      return blockedResult(args.log, "source-pack-limit");
    }

    // Cold snapshot construction reads at most one idx and one logical-ref
    // sidecar per active pack. Those helpers decrement the cache budget
    // internally, so reserve their real platform calls in this task's strict
    // pre-call counter before starting either pass.
    args.countSubrequest("r2:load-gc-pack-metadata", begin.activeCatalog.length * 2);
    args.cacheCtx.memo.packCatalog = begin.activeCatalog;
    const snapshotLoad = await loadOrderedPackSnapshot(
      args.env,
      args.repoId,
      args.cacheCtx,
      args.log
    );
    if (snapshotLoad.type !== "Ready") return retryResult(args.log, snapshotLoad.reason);
    const refSnapshot = await loadPackRefSnapshot(
      args.env,
      args.repoId,
      snapshotLoad.snapshot,
      args.cacheCtx,
      { scheduleMissingBackfill: false }
    );
    if (refSnapshot.type !== "Ready") {
      for (const missing of refSnapshot.packs) {
        const doId = getDoIdFromPath(missing.packKey);
        if (!doId) return blockedResult(args.log, "missing-ref-index-do-id");
        args.countSubrequest("queue:reachability-gc-ref-backfill");
        await args.limiter.run("queue:reachability-gc-ref-backfill", () =>
          args.env.REPO_TASKS_QUEUE.send({
            kind: "pack-ref-backfill",
            doId,
            repoId: args.repoId,
            packKey: missing.packKey,
          })
        );
      }
      return retryResult(args.log, "missing-ref-index");
    }
    const closure = await computeNeededFromPackRefs({
      logLevel: args.env.LOG_LEVEL,
      repoId: args.repoId,
      packs: refSnapshot.packs,
      wants: begin.refs.map((ref) => ref.oid),
      haves: [],
    });
    if (closure.type !== "Ready") return retryResult(args.log, closure.reason);
    args.log.info("reachability-gc:closure", {
      reachableObjects: closure.neededOids.length,
      sourcePacks: snapshotLoad.snapshot.packs.length,
    });

    // An immutable source that already contains exactly the closure needs no
    // rewrite, upload or re-indexing. Keep it through the same catalog CAS and
    // generation publication as a new pack; never include it in deletion work.
    const retainedPack =
      closure.neededOids.length > 0
        ? snapshotLoad.snapshot.packs.find((pack) =>
            targetIdxMatchesClosure(pack.idx, closure.neededOids)
          )
        : undefined;
    if (retainedPack) {
      args.log.info("reachability-gc:reuse-exact-closure", {
        reachableObjects: closure.neededOids.length,
        packBytes: retainedPack.packBytes,
      });
      if (begin.activeCatalog.length === 1) {
        return {
          status: "completed",
          reachableObjects: closure.neededOids.length,
          sourcePacks: 1,
          scheduledArtifacts: 0,
          packCatalogVersion: begin.packsetVersion,
        };
      }
    }
    let stagedIdxBytes: number | undefined;
    if (closure.neededOids.length > 0 && !retainedPack) {
      const rewrite = await rewritePackResult(args.env, snapshotLoad.snapshot, closure.neededOids, {
        limiter: args.limiter,
        countSubrequest: (n) => args.countSubrequest("r2:rewrite-gc-pack", n),
      });
      if (rewrite.status !== "ok") return retryResult(args.log, rewrite.failure.reason);
      if (rewrite.addedDeltaBases > 0) {
        return blockedResult(args.log, "delta-base-outside-reachability-closure");
      }
      const doId = getDoIdFromPath(begin.activeCatalog[0]!.packKey);
      if (!doId) throw new Error("reachability GC source pack has no Durable Object prefix");
      const packKey = r2PackKey(doPrefix(doId), `pack-gc-${begin.lease.token}.pack`);
      stagedUpload = await stagePackToR2({
        env: args.env,
        request: new Request(
          `https://maintenance.internal/${encodeURIComponent(args.repoId)}/reachability-gc`
        ),
        packStream: rewrite.stream,
        packKey,
        bytesConsumed: 0,
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
      });
      const scanResult = await scanPack({
        env: args.env,
        packKey,
        packSize: stagedUpload.packBytes,
        limiter: args.limiter,
        countSubrequest: (n = 1) => args.countSubrequest("r2:scan-gc-pack", n),
        log: args.log,
      });
      const resolved = await resolveDeltasAndWriteIdx({
        env: args.env,
        packKey,
        packSize: stagedUpload.packBytes,
        limiter: args.limiter,
        countSubrequest: (n = 1) => args.countSubrequest("r2:index-gc-pack", n),
        log: args.log,
        scanResult,
        activeCatalog: begin.activeCatalog,
        cacheCtx: args.cacheCtx,
        repoId: args.repoId,
      });
      if (!targetIdxMatchesClosure(resolved.idxView, closure.neededOids)) {
        return blockedResult(args.log, "rewritten-oid-set-mismatch");
      }
      stagedIdxBytes = resolved.idxBytes;
    }

    const commitArgs: Parameters<RepoDurableObject["commitReachabilityGc"]>[0] = {
      token: begin.lease.token,
      refsVersion: begin.refsVersion,
      packsetVersion: begin.packsetVersion,
      sourcePacks: begin.activeCatalog,
      retainedPackKey: retainedPack?.packKey,
      stagedPack: stagedUpload
        ? {
            packKey: stagedUpload.packKey,
            packBytes: stagedUpload.packBytes,
            idxBytes: stagedIdxBytes!,
            objectCount: closure.neededOids.length,
          }
        : undefined,
    };
    if (stagedUpload) {
      args.countSubrequest("do:record-pending-reachability-gc");
      const recorded = await args.limiter.run<RecordReachabilityGcPendingResult>(
        "do:record-pending-reachability-gc",
        () =>
          stub.recordReachabilityGcPending({
            token: begin.lease.token,
            packKey: stagedUpload!.packKey,
          })
      );
      if (recorded.status !== "recorded") return retryResult(args.log, recorded.reason);
      pendingRecorded = true;
    }
    const commit = await commitWithReconciliation({
      stub,
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
      commitArgs,
      log: args.log,
      onRpcStart: () => {
        commitAttempted = true;
      },
    });
    if (commit.status !== "committed") {
      pendingRecorded = false;
      commitAttempted = false;
      return retryResult(args.log, commit.reason);
    }
    stagedUpload = undefined;
    if (!(await publishPendingGeneration({ ...args, stub }))) {
      return retryResult(args.log, "generation-publication-advanced");
    }
    const cleanupScheduled = await scheduleSupersededPackCleanup({
      ...args,
      packKeys: commit.supersededPackKeys,
      supersededAtGeneration: commit.packCatalogVersion,
    });
    if (!cleanupScheduled) return retryResult(args.log, "cleanup-enqueue-failed");
    args.log.info("reachability-gc:complete", {
      reachableObjects: closure.neededOids.length,
      sourcePacks: begin.activeCatalog.length,
      scheduledArtifacts: commit.supersededPackKeys.length * 3,
      packCatalogVersion: commit.packCatalogVersion,
    });
    return {
      status: "completed",
      reachableObjects: closure.neededOids.length,
      sourcePacks: begin.activeCatalog.length,
      scheduledArtifacts: commit.supersededPackKeys.length * 3,
      packCatalogVersion: commit.packCatalogVersion,
    };
  } finally {
    if (stagedUpload && !commitAttempted && !pendingRecorded) {
      args.log.info("reachability-gc:staged-cleanup-start", { packKey: stagedUpload.packKey });
      await deleteStagedPack(stagedUpload)
        .then(() => {
          args.log.info("reachability-gc:staged-cleanup-complete", {
            packKey: stagedUpload?.packKey,
          });
        })
        .catch((error) => {
          args.log.warn("reachability-gc:staged-cleanup-failed", { error: String(error) });
        });
    }
    if (!pendingRecorded) {
      args.countSubrequest("do:abort-reachability-gc");
      await args.limiter
        .run("do:abort-reachability-gc", () => stub.abortCompaction(begin.lease.token))
        .catch((error) => {
          args.log.warn("reachability-gc:lease-cleanup-failed", { error: String(error) });
        });
    }
  }
}
