import type { ReachabilityGcQueueMessage, RepoQueueMessageHandle } from "./types";

import { runReachabilityGc } from "@/worker/git/maintenance/reachabilityGc";
import { createQueueTaskContext, logSoftBudgetExhausted, retryQueueMessage } from "./context";

export const REACHABILITY_GC_SUBREQUEST_BUDGET = 900;
export const REACHABILITY_GC_WORK_SUBREQUEST_BUDGET = 890;
const REACHABILITY_GC_RETRY_DELAY_SECONDS = 30;

export class ReachabilityGcBudgetExceededError extends Error {}

export class ReachabilityGcSubrequestBudget {
  private count = 0;

  public consume(next: number, reservedCleanup = false): void {
    const normalized = Math.max(1, next);
    const limit = reservedCleanup
      ? REACHABILITY_GC_SUBREQUEST_BUDGET
      : REACHABILITY_GC_WORK_SUBREQUEST_BUDGET;
    if (this.count + normalized > limit) {
      throw new ReachabilityGcBudgetExceededError("reachability GC subrequest limit exceeded");
    }
    this.count += normalized;
  }

  public get used(): number {
    return this.count;
  }
}

export function isReachabilityGcReservedSubrequest(op: string): boolean {
  return (
    op === "queue:reachability-gc-delete" ||
    op === "do:reconcile-reachability-gc" ||
    op === "do:abort-empty-reachability-gc" ||
    op === "do:abort-reachability-gc" ||
    op === "r2:abort-pack-multipart" ||
    op === "r2:delete-staged-pack" ||
    op === "r2:delete-pack-idx" ||
    op === "r2:delete-pack-refs"
  );
}

/** Execute one operator-requested reachability rewrite through the real queue boundary. */
export async function handleReachabilityGcMessage(
  message: Omit<RepoQueueMessageHandle<ReachabilityGcQueueMessage>, "body">,
  body: ReachabilityGcQueueMessage,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const task = createQueueTaskContext({
    env,
    ctx,
    repoLabel: body.repoId,
    operation: "reachability-gc",
    subrequestBudget: REACHABILITY_GC_SUBREQUEST_BUDGET,
  });
  const subrequestBudget = new ReachabilityGcSubrequestBudget();
  const log = task.logFor({
    service: "ReachabilityGcQueue",
    repoId: body.repoId,
    doId: body.doId,
  });
  try {
    const result = await runReachabilityGc({
      env,
      repoId: body.repoId,
      cacheCtx: task.cacheCtx,
      limiter: task.limiter,
      log,
      countSubrequest: (op, count = 1) => {
        const reservedCleanup = isReachabilityGcReservedSubrequest(op);
        subrequestBudget.consume(count, reservedCleanup);
        if (op !== "r2:load-gc-pack-metadata") {
          logSoftBudgetExhausted({
            cacheCtx: task.cacheCtx,
            log,
            flagPrefix: "reachability-gc-soft-budget",
            op,
            count,
          });
        }
      },
    });
    if (result.status === "retry") {
      log.warn("reachability-gc:queue-retry", { reason: result.reason });
      retryQueueMessage(message, REACHABILITY_GC_RETRY_DELAY_SECONDS);
      return;
    }
    if (result.status === "blocked") {
      log.error("reachability-gc:queue-blocked", { reason: result.reason });
      message.ack();
      return;
    }
    log.info("reachability-gc:queue-complete", result);
    message.ack();
  } catch (error) {
    if (error instanceof ReachabilityGcBudgetExceededError) {
      log.warn("reachability-gc:queue-budget-retry", {
        countedSubrequests: subrequestBudget.used,
        budget: REACHABILITY_GC_SUBREQUEST_BUDGET,
      });
      retryQueueMessage(message, REACHABILITY_GC_RETRY_DELAY_SECONDS);
      return;
    }
    log.error("reachability-gc:queue-error", { error: String(error) });
    retryQueueMessage(message, REACHABILITY_GC_RETRY_DELAY_SECONDS);
  }
}
