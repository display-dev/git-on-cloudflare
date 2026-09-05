import { getRepoStub, zeroOid } from "@/worker/common";
import type { CacheContext } from "@/worker/cache";
import { markRequestPrivate } from "@/worker/cache/policy";
import type {
  AcceptedWriteProjectionResult,
  ReconciledHeadProjectionResult,
  SnapshotReconcilePlan,
} from "@/worker/do/repo/acceptedWrites";
import type { Logger } from "@/worker/common/logger";
import type { BeginRepositoryReadResult } from "@/worker/do/repo/repositoryLifecycle";
import { countSubrequest } from "@/worker/git/operations/limits";
import {
  inspectSnapshotCommit,
  SnapshotLimitError,
  snapshotRepositoryPrefix,
  type SnapshotMaterializationTarget,
} from "@/worker/git/snapshot/materialize";

import { createQueueTaskContext, retryQueueMessage } from "./context";
import type { RepoQueueMessageHandle, SnapshotMaterializeQueueMessage } from "./types";

const SNAPSHOT_SUBREQUEST_BUDGET = 900;
const SNAPSHOT_RETRY_DELAY_SECONDS = 30;
export const SNAPSHOT_MAX_DELIVERY_ATTEMPTS = 5;

type TerminalSnapshotObserver = (
  body: SnapshotMaterializeQueueMessage,
  error: SnapshotLimitError
) => void;
type BeforeSnapshotProjectionObserver = (body: SnapshotMaterializeQueueMessage) => Promise<void>;

let terminalSnapshotObserver: TerminalSnapshotObserver | undefined;
let beforeSnapshotProjectionObserver: BeforeSnapshotProjectionObserver | undefined;

function count(cacheCtx: CacheContext, log: Logger, op: string): void {
  if (!countSubrequest(cacheCtx)) log.warn("snapshot:queue-soft-budget-exhausted", { op });
}

function isTerminalSnapshotError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return (
    message === "Snapshot exceeds benchmark limits" ||
    message === "Snapshot contains an invalid path" ||
    message === "Snapshot contains an unsupported Git entry"
  );
}

function retryOrAck(
  message: Omit<RepoQueueMessageHandle<SnapshotMaterializeQueueMessage>, "body">,
  log: Logger,
  body: SnapshotMaterializeQueueMessage,
  reason: string,
  error?: unknown
): void {
  if (message.attempts >= SNAPSHOT_MAX_DELIVERY_ATTEMPTS) {
    log.error("snapshot:queue-attempts-exhausted", {
      repositoryId: body.repositoryId,
      ref: body.ref,
      commitSha: body.afterSha,
      attempts: message.attempts,
      reason,
      error: error === undefined ? undefined : String(error),
    });
    message.ack();
    return;
  }
  log.warn("snapshot:queue-retry", {
    repositoryId: body.repositoryId,
    ref: body.ref,
    commitSha: body.afterSha,
    attempts: message.attempts,
    reason,
    error: error === undefined ? undefined : String(error),
  });
  retryQueueMessage(message, SNAPSHOT_RETRY_DELAY_SECONDS);
}

export async function handleSnapshotMaterializeMessage(
  message: Omit<RepoQueueMessageHandle<SnapshotMaterializeQueueMessage>, "body">,
  body: SnapshotMaterializeQueueMessage,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const task = createQueueTaskContext({
    env,
    ctx,
    repoLabel: body.repositoryId,
    operation: "snapshot-materialize",
    subrequestBudget: SNAPSHOT_SUBREQUEST_BUDGET,
  });
  markRequestPrivate(task.cacheCtx);
  task.cacheCtx.memo!.repoId = body.doName;
  const log = task.logFor({ service: "SnapshotMaterializeQueue" });
  let configuredPrefix: string | null;
  try {
    configuredPrefix = snapshotRepositoryPrefix(env, body.repositoryId);
  } catch (error) {
    log.error("snapshot:queue-invalid-configuration", {
      repositoryId: body.repositoryId,
      error: String(error),
    });
    message.ack();
    return;
  }
  if (!configuredPrefix) {
    log.info("snapshot:queue-disabled", { repositoryId: body.repositoryId });
    message.ack();
    return;
  }

  const stub = getRepoStub(env, body.doName);
  try {
    count(task.cacheCtx, log, "do:get-snapshot-reconcile-plan");
    const plan = await task.limiter.run<SnapshotReconcilePlan>(
      "do:get-snapshot-reconcile-plan",
      () => stub.getSnapshotReconcilePlan(body.ref)
    );
    if (plan.status === "up_to_date" || plan.status === "unborn") {
      log.info("snapshot:queue-already-converged", {
        repositoryId: body.repositoryId,
        ref: body.ref,
        status: plan.status,
      });
      message.ack();
      return;
    }

    const target: SnapshotMaterializationTarget =
      plan.status === "deliver"
        ? plan.entry.fact
        : {
            repositoryId: body.repositoryId,
            afterSha: plan.afterSha,
            sourceSurface: "reconcile",
          };
    let materializedTreeSha = zeroOid();
    let readerToken: string | undefined;
    try {
      if (target.afterSha !== zeroOid()) {
        count(task.cacheCtx, log, "do:begin-repository-read");
        const reader = await task.limiter.run<BeginRepositoryReadResult>(
          "do:begin-repository-read",
          () => stub.beginRepositoryRead("snapshot-projection")
        );
        if (!reader.ok) {
          if (reader.reason === "repository-deleting") {
            log.info("snapshot:queue-repository-deleting", {
              repositoryId: body.repositoryId,
              ref: body.ref,
            });
            message.ack();
            return;
          }
          retryOrAck(message, log, body, "reader-capacity");
          return;
        }
        readerToken = reader.token;
        const inspected = await inspectSnapshotCommit({
          env,
          repoId: body.doName,
          commitSha: target.afterSha,
          cacheCtx: task.cacheCtx,
          collectFiles: false,
        });
        materializedTreeSha = inspected.treeSha;
      }
      await beforeSnapshotProjectionObserver?.(body);

      if (plan.status === "deliver") {
        count(task.cacheCtx, log, "do:project-accepted-write");
        const projection = await task.limiter.run<AcceptedWriteProjectionResult>(
          "do:project-accepted-write",
          () =>
            stub.projectAcceptedWrite({
              entryId: plan.entry.id,
              commitSha: plan.entry.fact.afterSha,
              treeSha: materializedTreeSha,
              materializedAt: Date.now(),
              readerToken,
            })
        );
        if ("status" in projection) {
          if (projection.status === "projection-lease-expired") {
            retryOrAck(message, log, body, projection.status);
            return;
          }
          if (projection.status === "invalid-snapshot") {
            log.error("snapshot:queue-invalid-identity", {
              repositoryId: body.repositoryId,
              ref: body.ref,
              commitSha: target.afterSha,
            });
            message.ack();
            return;
          }
          log.info("snapshot:queue-repository-deleting", {
            repositoryId: body.repositoryId,
            ref: body.ref,
          });
          message.ack();
          return;
        }
      } else {
        count(task.cacheCtx, log, "do:project-reconciled-head");
        const projection = await task.limiter.run<ReconciledHeadProjectionResult>(
          "do:project-reconciled-head",
          () =>
            stub.projectReconciledHead({
              ref: plan.ref,
              commitSha: plan.afterSha,
              treeSha: materializedTreeSha,
              sequence: plan.sequence,
              materializedAt: Date.now(),
              readerToken,
            })
        );
        if (projection.status === "stale") {
          retryOrAck(message, log, body, "authoritative-head-changed");
          return;
        }
        if (projection.status === "projection-lease-expired") {
          retryOrAck(message, log, body, projection.status);
          return;
        }
        if (projection.status === "repository-deleting") {
          log.info("snapshot:queue-repository-deleting", {
            repositoryId: body.repositoryId,
            ref: body.ref,
          });
          message.ack();
          return;
        }
        if (projection.status === "invalid-snapshot") {
          log.error("snapshot:queue-invalid-identity", {
            repositoryId: body.repositoryId,
            ref: body.ref,
            commitSha: target.afterSha,
          });
          message.ack();
          return;
        }
      }
      log.info("snapshot:queue-complete", {
        repositoryId: body.repositoryId,
        ref: body.ref,
        commitSha: target.afterSha,
        reconciliation: plan.status,
      });
      message.ack();
    } finally {
      if (readerToken) {
        const token = readerToken;
        count(task.cacheCtx, log, "do:finish-repository-read");
        await task.limiter
          .run("do:finish-repository-read", () => stub.finishRepositoryRead(token))
          .catch((error) =>
            log.warn("snapshot:queue-lease-release-failed", { error: String(error) })
          );
      }
    }
  } catch (error) {
    if (isTerminalSnapshotError(error)) {
      if (error instanceof SnapshotLimitError) terminalSnapshotObserver?.(body, error);
      log.error("snapshot:queue-terminal-rejection", {
        repositoryId: body.repositoryId,
        ref: body.ref,
        commitSha: body.afterSha,
        error: String(error),
      });
      message.ack();
      return;
    }
    retryOrAck(message, log, body, "transient-delivery-failure", error);
  }
}

export const __test = {
  SNAPSHOT_MAX_DELIVERY_ATTEMPTS,
  isTerminalSnapshotError,
  retryOrAck,
  setTerminalSnapshotObserver(observer: TerminalSnapshotObserver | undefined): void {
    terminalSnapshotObserver = observer;
  },
  setBeforeSnapshotProjectionObserver(
    observer: BeforeSnapshotProjectionObserver | undefined
  ): void {
    beforeSnapshotProjectionObserver = observer;
  },
};
