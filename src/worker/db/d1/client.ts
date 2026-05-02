import { drizzle, type DrizzleD1Database } from "drizzle-orm/d1";

import * as schema from "./schema";

// Global D1 client. Today every call site passes `env.DB` (a `D1Database`);
// the overload mirrors anvil so future code paths can adopt session
// bookmarks without churning the helper signature. drizzle's runtime
// accepts either shape; the cast collapses the overload boundary inside
// this module only.
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
