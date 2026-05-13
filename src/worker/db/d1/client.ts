import { drizzle, type DrizzleD1Database } from "drizzle-orm/d1";

import * as schema from "./schema";

// Global D1 client. Request-scoped D1 access in Hono routes runs through
// a per-request `D1DatabaseSession` opened by `requestServicesMiddleware`
// in `src/worker/routes/hono.ts`; non-request paths (queue tasks in
// `tasks/context.ts`, plus the defensive fallbacks in `repositories/route.ts`,
// `auth/gitAuth.ts`, and `auth/pat.ts`) keep plain `D1Database` semantics,
// which Cloudflare reads as `first-primary` by default. The overload lets
// both shapes share one helper; drizzle's runtime accepts either, so the
// cast collapses the overload boundary inside this module only.
export type D1Executor = D1Database | D1DatabaseSession;
export type Db<TClient extends D1Executor = D1Executor> = DrizzleD1Database<typeof schema> & {
  $client: TClient;
};

export function createDb(executor: D1Database): Db<D1Database>;
export function createDb(executor: D1DatabaseSession): Db<D1DatabaseSession>;
export function createDb(executor: D1Executor): Db {
  return drizzle(executor as D1Database, { schema });
}

// Type alias used by DAL function signatures so callers can pass the
// session-backed or plain client without changing the helper.
export type DbExecutor = Db;
