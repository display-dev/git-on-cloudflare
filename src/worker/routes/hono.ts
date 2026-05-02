import type { Context, Hono } from "hono";

export type AppBindings = {
  Bindings: Env;
};

export type AppRouter = Hono<AppBindings>;
// Route handlers should accept AppContext directly. Adapter wrappers hide Hono's
// request state, middleware variables, response helpers, and executionCtx.
export type AppContext<Path extends string = string> = Context<AppBindings, Path>;
