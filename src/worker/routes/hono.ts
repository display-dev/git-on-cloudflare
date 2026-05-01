import type { Context, Handler, Hono } from "hono";

export type AppBindings = {
  Bindings: Env;
};

export type AppRouter = Hono<AppBindings>;
export type AppContext = Context<AppBindings>;

export type EmptyParams = Record<string, never>;
export type OwnerParams = {
  owner: string;
};
export type RepoParams = OwnerParams & {
  repo: string;
};
export type RepoOidParams = RepoParams & {
  oid: string;
};
export type RepoCommitParams = RepoParams & {
  commit: string;
};
export type RepoPackParams = RepoParams & {
  packKey: string;
};

export type RouteArgs<Params extends object = EmptyParams> = {
  request: Request;
  env: Env;
  ctx: ExecutionContext;
  params: Params;
};

export type RouteHandler<Params extends object = EmptyParams> = (
  args: RouteArgs<Params>
) => Response | Promise<Response>;

function baseRouteArgs(c: AppContext) {
  // Hono keeps the original Worker Request on c.req.raw. Pass that through so
  // existing body streaming, signal handling, and header semantics stay intact.
  return {
    request: c.req.raw,
    env: c.env,
    ctx: c.executionCtx,
  };
}

function routeParam(c: AppContext, name: string): string {
  // Hono types path parameters as optional. A missing value here means the route
  // pattern and adapter disagree, so use the same empty-string failure shape
  // that downstream validation already handles for malformed path parts.
  return c.req.param(name) ?? "";
}

export function noParamsRoute(handler: RouteHandler): Handler<AppBindings> {
  return (c) => handler({ ...baseRouteArgs(c), params: {} });
}

export function ownerRoute(handler: RouteHandler<OwnerParams>): Handler<AppBindings> {
  return (c) =>
    handler({
      ...baseRouteArgs(c),
      params: {
        owner: routeParam(c, "owner"),
      },
    });
}

export function repoRoute(handler: RouteHandler<RepoParams>): Handler<AppBindings> {
  return (c) =>
    handler({
      ...baseRouteArgs(c),
      params: {
        owner: routeParam(c, "owner"),
        repo: routeParam(c, "repo"),
      },
    });
}

export function repoOidRoute(handler: RouteHandler<RepoOidParams>): Handler<AppBindings> {
  return (c) =>
    handler({
      ...baseRouteArgs(c),
      params: {
        owner: routeParam(c, "owner"),
        repo: routeParam(c, "repo"),
        oid: routeParam(c, "oid"),
      },
    });
}

export function repoCommitRoute(handler: RouteHandler<RepoCommitParams>): Handler<AppBindings> {
  return (c) =>
    handler({
      ...baseRouteArgs(c),
      params: {
        owner: routeParam(c, "owner"),
        repo: routeParam(c, "repo"),
        commit: routeParam(c, "commit"),
      },
    });
}

export function repoPackRoute(handler: RouteHandler<RepoPackParams>): Handler<AppBindings> {
  return (c) =>
    handler({
      ...baseRouteArgs(c),
      params: {
        owner: routeParam(c, "owner"),
        repo: routeParam(c, "repo"),
        packKey: routeParam(c, "packKey"),
      },
    });
}
