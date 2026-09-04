import type { CacheContext } from "@/worker/cache";
import { bytesToHex } from "@/worker/common";
import type { Logger } from "@/worker/common/logger";
import type { RepoDurableObject } from "@/worker/do";
import type { IngestionReceipt } from "@/worker/do/repo/repoState";
import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type {
  FinalizeReceiveResult,
  ReconcileReceiveResult,
} from "@/worker/do/repo/catalog/receive";
import type { PackCatalogRow } from "@/worker/do/repo/db/schema";
import type { ReceiveCommand, ReceiveStatus } from "@/worker/git/operations/validation";

import type { Limiter } from "@/worker/git/operations/limits";
import {
  resolveDeltasAndWriteIdx,
  runPackConnectivityCheck,
  scanPack,
} from "@/worker/git/pack/indexer";
import { parseIdxView } from "@/worker/git/object-store";
import type { IdxView } from "@/worker/git/object-store/types";
import type { PackV2Artifacts } from "@/worker/git/pack/build";
import { doPrefix, packIndexKey, packRefsKey, r2PackKey } from "@/worker/keys";
import { deleteStagedPack, stagePackToR2, type StagedPackUpload } from "./r2Upload";
import { buildReceiveReportStatus, isReceiveAbort, throwIfReceiveAborted } from "./support";
import { ReceivePipelineHttpError, type ReceivePipelineResult } from "./pipelineTypes";

type RepoStub = DurableObjectStub<RepoDurableObject>;
type PrebuiltPackArtifacts = Pick<PackV2Artifacts, "idx" | "refs"> & {
  objectCount: number;
};

let afterFinalizeResponseForTesting: (() => void) | undefined;

export const __test = {
  failNextFinalizeResponse(): void {
    afterFinalizeResponseForTesting = () => {
      throw new Error("simulated lost finalize response");
    };
  },
  reset(): void {
    afterFinalizeResponseForTesting = undefined;
  },
};

type ReceiveCleanupAttempt = "inline" | "retry";

async function abortReceiveLease(args: {
  stub: RepoStub;
  leaseToken: string;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
  log: Logger;
  reason: string;
  attempt: ReceiveCleanupAttempt;
}): Promise<boolean> {
  try {
    args.countSubrequest("do:abort-receive");
    const cleared = await args.limiter.run("do:abort-receive", () =>
      args.stub.abortReceive(args.leaseToken)
    );
    if (!cleared) {
      args.log.warn("receive:abort-missed", {
        reason: args.reason,
        attempt: args.attempt,
        leaseToken: args.leaseToken,
      });
    }
    return cleared;
  } catch (error) {
    args.log.warn("receive:abort-failed", {
      reason: args.reason,
      attempt: args.attempt,
      leaseToken: args.leaseToken,
      error: String(error),
    });
    return false;
  }
}

async function cleanupStagedPack(args: {
  stagedUpload: StagedPackUpload | undefined;
  log: Logger;
  reason: string;
  attempt: ReceiveCleanupAttempt;
}): Promise<boolean> {
  if (!args.stagedUpload) return true;

  try {
    await deleteStagedPack(args.stagedUpload);
    return true;
  } catch (error) {
    args.log.warn("receive:staged-pack-cleanup-failed", {
      reason: args.reason,
      attempt: args.attempt,
      packKey: args.stagedUpload.packKey,
      error: String(error),
    });
    return false;
  }
}

async function cleanupFailedReceive(args: {
  ctx: ExecutionContext;
  stub: RepoStub;
  leaseToken: string;
  stagedUpload: StagedPackUpload | undefined;
  log: Logger;
  reason: string;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
}): Promise<void> {
  const leaseCleared = await abortReceiveLease({
    stub: args.stub,
    leaseToken: args.leaseToken,
    limiter: args.limiter,
    countSubrequest: args.countSubrequest,
    log: args.log,
    reason: args.reason,
    attempt: "inline",
  });
  // A false/ambiguous abort can mean finalization already committed. Never
  // delete staged objects unless the DO proved this lease was still active
  // and atomically aborted it.
  const stagedPackDeleted = leaseCleared
    ? await cleanupStagedPack({
        stagedUpload: args.stagedUpload,
        log: args.log,
        reason: args.reason,
        attempt: "inline",
      })
    : false;

  if (leaseCleared && stagedPackDeleted) return;

  args.log.warn("receive:cleanup-retry-scheduled", {
    reason: args.reason,
    leaseToken: args.leaseToken,
    packKey: args.stagedUpload?.packKey,
  });
  args.ctx.waitUntil(
    (async () => {
      const retryLeaseCleared =
        leaseCleared ||
        (await abortReceiveLease({
          stub: args.stub,
          leaseToken: args.leaseToken,
          limiter: args.limiter,
          countSubrequest: args.countSubrequest,
          log: args.log,
          reason: args.reason,
          attempt: "retry",
        }));
      let retryStagedPackDeleted = stagedPackDeleted;
      if (!retryStagedPackDeleted && retryLeaseCleared) {
        retryStagedPackDeleted = await cleanupStagedPack({
          stagedUpload: args.stagedUpload,
          log: args.log,
          reason: args.reason,
          attempt: "retry",
        });
      }

      if (!retryLeaseCleared || !retryStagedPackDeleted) {
        args.log.error("receive:cleanup-retry-incomplete", {
          reason: args.reason,
          leaseCleared: retryLeaseCleared,
          stagedPackDeleted: retryStagedPackDeleted,
          leaseToken: args.leaseToken,
          packKey: args.stagedUpload?.packKey,
        });
      }
    })()
  );
}

type ExecuteReceivePipelineArgs = {
  env: Env;
  repoId: string;
  request: Request;
  ctx: ExecutionContext;
  packStream: ReadableStream<Uint8Array>;
  bytesConsumed: number;
  stub: RepoStub;
  leaseToken: string;
  activeCatalog: PackCatalogRow[];
  commands: ReceiveCommand[];
  ingestionReceipt?: IngestionReceipt | undefined;
  acceptedWrites?: AcceptedWriteFact[] | undefined;
  /** Exact artifacts built from trusted in-process objects, never client input. */
  prebuiltPackArtifacts?: PrebuiltPackArtifacts | undefined;
  log: Logger;
  cacheCtx: CacheContext;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
  onProgress?: (message: string) => void;
};

function buildReceiveResult(args: {
  unpackOk: boolean;
  unpackMessage?: string;
  commands: ReceiveCommand[];
  statuses: ReceiveStatus[];
  changed: boolean;
  empty: boolean;
  packKey?: string;
  packBytes?: number;
}): ReceivePipelineResult {
  return {
    reportStatusBody: buildReceiveReportStatus({
      unpackOk: args.unpackOk,
      unpackMessage: args.unpackMessage,
      commands: args.commands,
      statuses: args.statuses,
    }),
    changed: args.changed,
    empty: args.empty,
    packKey: args.packKey,
    packBytes: args.packBytes,
  };
}

export async function executeReceivePipeline(
  args: ExecuteReceivePipelineArgs
): Promise<ReceivePipelineResult> {
  let stagedUpload: StagedPackUpload | undefined;
  let finalizationAttempted = false;

  try {
    const hasNonDelete = args.commands.some((command) => !/^0{40}$/i.test(command.newOid));
    let stagedPack:
      | {
          packKey: string;
          packBytes: number;
          idxBytes: number;
          objectCount: number;
        }
      | undefined;

    if (hasNonDelete) {
      const packKey = r2PackKey(
        doPrefix(args.stub.id.toString()),
        `pack-rx-${args.leaseToken}.pack`
      );
      stagedUpload = await stagePackToR2({
        env: args.env,
        request: args.request,
        packStream: args.packStream,
        packKey,
        bytesConsumed: args.bytesConsumed,
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
        onProgress: args.onProgress,
      });
      throwIfReceiveAborted(args.request, args.log, "stage-pack");

      let idxView: IdxView;
      let idxBytes: number;
      let objectCount: number;
      if (args.prebuiltPackArtifacts) {
        const artifacts = args.prebuiltPackArtifacts;
        const parsedIdxView = parseIdxView(
          stagedUpload.packKey,
          artifacts.idx,
          stagedUpload.packBytes
        );
        if (!parsedIdxView || parsedIdxView.count !== artifacts.objectCount) {
          throw new Error("receive: invalid prebuilt pack artifacts");
        }
        idxView = parsedIdxView;
        if (bytesToHex(idxView.packChecksum) !== stagedUpload.packSha1) {
          throw new Error("receive: prebuilt artifacts do not match staged pack");
        }
        args.countSubrequest("r2:put-pack-idx");
        args.countSubrequest("r2:put-pack-refs");
        const packKey = stagedUpload.packKey;
        const writes = await Promise.allSettled([
          args.limiter.run("r2:put-pack-idx", () =>
            args.env.REPO_BUCKET.put(packIndexKey(packKey), artifacts.idx)
          ),
          args.limiter.run("r2:put-pack-refs", () =>
            args.env.REPO_BUCKET.put(packRefsKey(packKey), artifacts.refs)
          ),
        ]);
        const failedWrite = writes.find(
          (write): write is PromiseRejectedResult => write.status === "rejected"
        );
        if (failedWrite) throw failedWrite.reason;
        throwIfReceiveAborted(args.request, args.log, "persist-prebuilt-pack-artifacts");
        idxBytes = artifacts.idx.byteLength;
        objectCount = artifacts.objectCount;
        args.log.info("receive:prebuilt-pack-artifacts-persisted", {
          objectCount,
          idxBytes,
          refsBytes: artifacts.refs.byteLength,
        });
      } else {
        const scanResult = await scanPack({
          env: args.env,
          packKey: stagedUpload.packKey,
          packSize: stagedUpload.packBytes,
          limiter: args.limiter,
          countSubrequest: (n = 1) => args.countSubrequest("r2:scan-pack", n),
          log: args.log,
          signal: args.request.signal,
          onProgress: args.onProgress,
        });
        throwIfReceiveAborted(args.request, args.log, "scan-pack");

        const resolveResult = await resolveDeltasAndWriteIdx({
          env: args.env,
          packKey: stagedUpload.packKey,
          packSize: stagedUpload.packBytes,
          limiter: args.limiter,
          countSubrequest: (n = 1) => args.countSubrequest("r2:resolve-pack", n),
          log: args.log,
          scanResult,
          activeCatalog: args.activeCatalog,
          cacheCtx: args.cacheCtx,
          repoId: args.repoId,
          signal: args.request.signal,
          onProgress: args.onProgress,
        });
        throwIfReceiveAborted(args.request, args.log, "resolve-pack");
        idxView = resolveResult.idxView;
        idxBytes = resolveResult.idxBytes;
        objectCount = resolveResult.objectCount;
      }

      const connectivityStatuses = args.commands.map((command) => ({
        ref: command.ref,
        ok: true,
      }));
      args.onProgress?.("Checking received object connectivity\n");
      await runPackConnectivityCheck({
        env: args.env,
        repoId: args.repoId,
        newPackKey: stagedUpload.packKey,
        newIdxView: idxView,
        newPackSize: stagedUpload.packBytes,
        activeCatalog: args.activeCatalog,
        commands: args.commands,
        statuses: connectivityStatuses,
        log: args.log,
        cacheCtx: args.cacheCtx,
      });
      throwIfReceiveAborted(args.request, args.log, "connectivity-check");

      if (!connectivityStatuses.every((status) => status.ok)) {
        await cleanupFailedReceive({
          ctx: args.ctx,
          stub: args.stub,
          leaseToken: args.leaseToken,
          stagedUpload,
          log: args.log,
          reason: "connectivity-rejected",
          limiter: args.limiter,
          countSubrequest: args.countSubrequest,
        });
        args.log.warn("receive:connectivity-rejected", {
          conflictCount: connectivityStatuses.filter((status) => !status.ok).length,
        });
        return buildReceiveResult({
          unpackOk: true,
          commands: args.commands,
          statuses: connectivityStatuses,
          changed: false,
          empty: false,
        });
      }

      stagedPack = {
        packKey: stagedUpload.packKey,
        packBytes: stagedUpload.packBytes,
        idxBytes,
        objectCount,
      };
    }

    args.countSubrequest("do:finalize-receive");
    throwIfReceiveAborted(args.request, args.log, "finalize-receive");
    args.onProgress?.("Updating refs\n");
    finalizationAttempted = true;
    let finalize: FinalizeReceiveResult;
    try {
      finalize = await args.limiter.run<FinalizeReceiveResult>("do:finalize-receive", async () => {
        const result = await args.stub.finalizeReceive({
          token: args.leaseToken,
          commands: args.commands,
          stagedPack,
          ingestionReceipt: args.ingestionReceipt,
          acceptedWrites: args.acceptedWrites,
        });
        const hook = afterFinalizeResponseForTesting;
        afterFinalizeResponseForTesting = undefined;
        hook?.();
        return result;
      });
    } catch (finalizeError) {
      args.countSubrequest("do:reconcile-receive");
      let reconciliation: ReconcileReceiveResult;
      try {
        reconciliation = await args.limiter.run<ReconcileReceiveResult>(
          "do:reconcile-receive",
          async () =>
            await args.stub.reconcileReceive({
              token: args.leaseToken,
              commands: args.commands,
              stagedPackKey: stagedUpload?.packKey,
              ingestionReceipt: args.ingestionReceipt,
            })
        );
      } catch (reconcileError) {
        args.log.error("receive:finalize-outcome-unknown", {
          leaseToken: args.leaseToken,
          packKey: stagedUpload?.packKey,
          finalizeError: String(finalizeError),
          reconcileError: String(reconcileError),
        });
        throw finalizeError;
      }

      if (reconciliation.status === "committed") {
        args.log.warn("receive:finalize-response-recovered", { leaseToken: args.leaseToken });
        finalize = reconciliation.result;
      } else if (reconciliation.status === "aborted") {
        const cleaned = await cleanupStagedPack({
          stagedUpload,
          log: args.log,
          reason: "finalize-not-committed",
          attempt: "inline",
        });
        if (!cleaned) {
          args.ctx.waitUntil(
            cleanupStagedPack({
              stagedUpload,
              log: args.log,
              reason: "finalize-not-committed",
              attempt: "retry",
            })
          );
        }
        throw finalizeError;
      } else {
        args.log.error("receive:finalize-outcome-unknown", {
          leaseToken: args.leaseToken,
          packKey: stagedUpload?.packKey,
          finalizeError: String(finalizeError),
        });
        throw finalizeError;
      }
    }

    if (finalize.status === "lease_mismatch") {
      // A missing lease without a retained outcome is not proof that the
      // staged pack was never committed. Preserve it for reconciliation.
      args.log.error("receive:lease-mismatch-pack-preserved", {
        leaseToken: args.leaseToken,
        packKey: stagedUpload?.packKey,
      });
      throw new ReceivePipelineHttpError(
        503,
        "lease-mismatch",
        "Repository receive lease expired before commit."
      );
    }

    if (finalize.status === "ref_conflict") {
      await cleanupStagedPack({
        stagedUpload,
        log: args.log,
        reason: "finalize-ref-conflict",
        attempt: "inline",
      });
      args.log.warn("receive:ref-conflict", {
        conflictCount: finalize.statuses.filter((status) => !status.ok).length,
        stage: "finalize",
      });
      return buildReceiveResult({
        unpackOk: true,
        commands: args.commands,
        statuses: finalize.statuses,
        changed: false,
        empty: false,
      });
    }

    if (finalize.shouldQueueCompaction) {
      args.log.info("receive:compaction-requested", { repoId: args.repoId });
      args.ctx.waitUntil(
        args.env.REPO_TASKS_QUEUE.send({
          kind: "compaction",
          doId: args.stub.id.toString(),
          repoId: args.repoId,
        }).catch((error) => {
          args.log.warn("receive:compaction-enqueue-failed", {
            repoId: args.repoId,
            error: String(error),
          });
        })
      );
    }

    return buildReceiveResult({
      unpackOk: true,
      commands: args.commands,
      statuses: finalize.statuses,
      changed: finalize.changed,
      empty: finalize.empty,
      packKey: stagedPack?.packKey,
      packBytes: stagedPack?.packBytes,
    });
  } catch (error) {
    const aborted = isReceiveAbort(args.request, error);
    if (!finalizationAttempted) {
      await cleanupFailedReceive({
        ctx: args.ctx,
        stub: args.stub,
        leaseToken: args.leaseToken,
        stagedUpload,
        log: args.log,
        reason: aborted ? "receive-aborted" : "receive-error",
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
      });
    }
    throw error;
  }
}
