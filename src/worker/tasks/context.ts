import type { CacheContext } from "@/worker/cache";
import type { Logger, LoggerContext } from "@/worker/common/logger";
import type { Db } from "@/worker/db/d1/client";

import { createLogger } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import {
  MAX_SIMULTANEOUS_CONNECTIONS,
  SubrequestLimiter,
  countSubrequest,
} from "@/worker/git/operations/limits";

export type QueueLogContext = Omit<LoggerContext, "requestId">;

export type QueueTaskContext = {
  db: Db;
  cacheCtx: CacheContext;
  limiter: SubrequestLimiter;
  logFor: (context: QueueLogContext) => Logger;
};

export function createQueueTaskContext(args: {
  env: Env;
  ctx: ExecutionContext;
  repoLabel: string;
  operation: string;
  subrequestBudget: number;
}): QueueTaskContext {
  const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
  return {
    db: createDb(args.env.DB),
    cacheCtx: {
      req: new Request(
        `https://queue.internal/${encodeURIComponent(args.repoLabel)}/${args.operation}`
      ),
      ctx: args.ctx,
      memo: {
        repoId: args.repoLabel,
        limiter,
        subreqBudget: args.subrequestBudget,
      },
    },
    limiter,
    logFor: (context) => createLogger(args.env.LOG_LEVEL, context),
  };
}

export function retryQueueMessage(
  message: { retry: (options?: { delaySeconds?: number }) => void },
  seconds: number
): void {
  message.retry({ delaySeconds: seconds });
}

export function logSoftBudgetExhausted(args: {
  cacheCtx: CacheContext;
  log: Logger;
  flagPrefix: string;
  op: string;
  count?: number;
}): void {
  if (countSubrequest(args.cacheCtx, args.count)) return;
  args.cacheCtx.memo = args.cacheCtx.memo || {};
  args.cacheCtx.memo.flags = args.cacheCtx.memo.flags || new Set();
  const flag = `${args.flagPrefix}:${args.op}`;
  if (args.cacheCtx.memo.flags.has(flag)) return;
  args.cacheCtx.memo.flags.add(flag);
  args.log.warn("soft-budget-exhausted", { op: args.op });
}
