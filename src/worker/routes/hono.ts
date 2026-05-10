import { createMiddleware } from "hono/factory";
import type { Context, Hono } from "hono";
import type { Viewer } from "@/client/server/viewer";
import type { CacheContext } from "@/worker/cache/cache";
import type { ActiveSession } from "@/worker/auth/session";
import type { Db } from "@/worker/db/d1/client";
import type { Limiter } from "@/worker/git/operations/limits";
import type { Logger, LoggerContext } from "@/worker/common/logger";

import { createLogger } from "@/worker/common/logger";
import { createDb } from "@/worker/db/d1/client";
import { getLimiter } from "@/worker/git/operations/limits";

export type RequestLogContext = Omit<LoggerContext, "requestId">;
export type RequestLogFactory = (context: RequestLogContext) => Logger;

export type AppBindings = {
  Bindings: Env;
  Variables: {
    db: Db;
    cacheCtx: CacheContext;
    limiter: Limiter;
    requestId: string;
    logFor: RequestLogFactory;
    activeSessionPromise?: Promise<ActiveSession | null>;
    viewerPromise?: Promise<Viewer | null>;
  };
};

export type AppRouter = Hono<AppBindings>;
// Route handlers should accept AppContext directly. Adapter wrappers hide Hono's
// request state, middleware variables, response helpers, and executionCtx.
export type AppContext<Path extends string = string> = Context<AppBindings, Path>;

function requestIdFrom(request: Request): string {
  return request.headers.get("Cf-Ray")?.trim() || crypto.randomUUID();
}

export const requestServicesMiddleware = createMiddleware<AppBindings>(async (c, next) => {
  const requestId = requestIdFrom(c.req.raw);
  const cacheCtx: CacheContext = {
    req: c.req.raw,
    ctx: c.executionCtx,
  };
  const limiter = getLimiter(cacheCtx);

  c.set("requestId", requestId);
  c.set("db", createDb(c.env.DB));
  c.set("cacheCtx", cacheCtx);
  c.set("limiter", limiter);
  c.set("logFor", (context) =>
    createLogger(c.env.LOG_LEVEL, {
      ...context,
      requestId,
    })
  );

  await next();
});
