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
import { emitD1Bookmark, readInboundD1Bookmark } from "./d1Bookmark";

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

type SessionConstraintDecision =
  | { kind: "primary" }
  | { kind: "read"; anchor: D1SessionBookmark | D1SessionConstraint };

// Centralized read/write classification. The selector runs once before
// `next()` and the chosen anchor cannot be changed mid-route: every route
// is classified purely by `(method, path)`.
function selectSessionConstraint(
  method: string,
  path: string,
  inboundBookmark: string | null
): SessionConstraintDecision {
  // Hono with `strict: false` already strips a trailing slash before our
  // middleware sees `c.req.path`, but we normalize defensively so the
  // classification keeps working if that option is ever flipped.
  const normalizedPath = path.length > 1 && path.endsWith("/") ? path.slice(0, -1) : path;

  // Write-shaped GET: OIDC callback writes user/membership rows and then
  // reads them back in the same request.
  if (method === "GET" && normalizedPath === "/auth/callback") return { kind: "primary" };

  // Git protocol routes lie about read/write through the HTTP verb:
  // upload-pack POST is the fetch path (read-only); receive-pack POST is
  // the push path. The discovery GETs on `info/refs` fall through to the
  // default-method rule below as read-like.
  if (method === "POST") {
    if (normalizedPath.endsWith("/git-upload-pack")) {
      return { kind: "read", anchor: inboundBookmark ?? "first-unconstrained" };
    }
    if (normalizedPath.endsWith("/git-receive-pack")) return { kind: "primary" };
  }

  if (method === "GET" || method === "HEAD" || method === "OPTIONS") {
    return { kind: "read", anchor: inboundBookmark ?? "first-unconstrained" };
  }
  return { kind: "primary" };
}

export function workerExecutionContext(c: AppContext): ExecutionContext {
  // Hono 4.12.x still types `executionCtx.exports` as optional while Wrangler's
  // generated Workers runtime types require it. At runtime Hono is carrying the
  // real Cloudflare Workers context; keep the cast at this framework boundary.
  // See https://github.com/honojs/hono/issues/4493.
  return c.executionCtx as ExecutionContext;
}

function requestIdFrom(request: Request): string {
  return request.headers.get("Cf-Ray")?.trim() || crypto.randomUUID();
}

export const requestServicesMiddleware = createMiddleware<AppBindings>(async (c, next) => {
  const requestId = requestIdFrom(c.req.raw);
  const cacheCtx: CacheContext = {
    req: c.req.raw,
    ctx: workerExecutionContext(c),
  };
  const limiter = getLimiter(cacheCtx);

  // Open exactly one D1 session per request. The anchor is fixed before
  // any route runs and `c.var.db` is not allowed to be re-anchored from
  // inside a route. `emitD1Bookmark` runs in `finally` so the bookmark is
  // shipped even when a handler throws and `onError` rewrites `c.res`.
  const inboundBookmark = readInboundD1Bookmark(c);
  const decision = selectSessionConstraint(c.req.method, c.req.path, inboundBookmark);
  const anchor: D1SessionBookmark | D1SessionConstraint =
    decision.kind === "primary" ? "first-primary" : decision.anchor;
  const session = c.env.DB.withSession(anchor);

  c.set("requestId", requestId);
  c.set("db", createDb(session));
  c.set("cacheCtx", cacheCtx);
  c.set("limiter", limiter);
  c.set("logFor", (context) =>
    createLogger(c.env.LOG_LEVEL, {
      ...context,
      requestId,
    })
  );

  try {
    await next();
  } finally {
    emitD1Bookmark(c, session, inboundBookmark);
  }
});
