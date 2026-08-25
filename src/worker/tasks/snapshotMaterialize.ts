import { getRepoStub, zeroOid } from "@/worker/common";
import type { CacheContext } from "@/worker/cache";
import type {
  AcceptedWriteProjectionResult,
  ReconciledHeadProjectionResult,
  SnapshotReconcilePlan,
} from "@/worker/do/repo/acceptedWrites";
import type { Logger } from "@/worker/common/logger";
import { countSubrequest } from "@/worker/git/operations/limits";
import {
  materializeAcceptedWrite,
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

let terminalSnapshotObserver: TerminalSnapshotObserver | undefined;

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
    if (target.afterSha !== zeroOid()) {
      const manifest = await materializeAcceptedWrite({
        env,
        repoId: body.doName,
        fact: target,
        request: task.cacheCtx.req,
        ctx,
        limiter: task.limiter,
        log,
      });
      if (!manifest) {
        // A configured prefix can return null only after the repository
        // deletion fence rejects a new materialization lease. Deletion is a
        // terminal outcome for this immutable queue fact.
        log.info("snapshot:queue-repository-deleting", {
          repositoryId: body.repositoryId,
          ref: body.ref,
        });
        message.ack();
        return;
      }
    }

    if (plan.status === "deliver") {
      count(task.cacheCtx, log, "do:project-accepted-write");
      const projection = await task.limiter.run<AcceptedWriteProjectionResult>(
        "do:project-accepted-write",
        () =>
          stub.projectAcceptedWrite({
            entryId: plan.entry.id,
            commitSha: plan.entry.fact.afterSha,
            materializedAt: Date.now(),
          })
      );
      if ("status" in projection) {
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
            sequence: plan.sequence,
            materializedAt: Date.now(),
          })
      );
      if (projection.status === "stale") {
        retryOrAck(message, log, body, "authoritative-head-changed");
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
    }
    log.info("snapshot:queue-complete", {
      repositoryId: body.repositoryId,
      ref: body.ref,
      commitSha: target.afterSha,
      reconciliation: plan.status,
    });
    message.ack();
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
};
