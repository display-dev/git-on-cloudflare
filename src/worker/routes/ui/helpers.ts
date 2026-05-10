import type { CacheContext } from "@/worker/cache";
import type { DebugPackState, DebugStateSnapshot } from "@/worker/do/repo/debug";
import type { HeadInfo, Ref } from "@/worker/git";
import type { PackRefIndexStatus } from "@/shared/git/types";
import type { Viewer } from "@/client/server/viewer";
import { getHeadAndRefs } from "@/worker/git";
import { shortRefName } from "@/shared/git/ref-display";
import { formatSize, HttpError, isValidOwnerRepo } from "@/shared/web";
import { handleError } from "@/client/server/error";
import { buildCacheKeyFrom, cacheOrLoadJSONForRequest } from "@/worker/cache";
import { isRequestPrivate, markRequestPrivate } from "@/worker/cache/policy";
import { loadSessionMembership } from "@/worker/auth/sessionMembership";
import { loadViewer } from "@/worker/auth/session";
import { getRepoActivity, type RepoActivity } from "@/worker/common";
import { createLogger } from "@/worker/common/logger";
import { countSubrequest, getLimiter, type Limiter } from "@/worker/git/operations/limits";
import { packRefsKey } from "@/worker/keys";
import { resolveRepositoryRoute, type RepositoryRoute } from "@/worker/repositories/route";
import type { AppContext } from "@/worker/routes/hono";
import { renderUiDocumentResponse } from "@/worker/routes/uiResponse";

export type AdminPackState = DebugPackState & {
  refIndexStatus?: PackRefIndexStatus;
  refIndexSize?: number;
};
export type DebugState = Omit<
  DebugStateSnapshot,
  "packStats" | "activePacks" | "supersededPacks"
> & {
  packStats?: AdminPackState[];
  activePacks: AdminPackState[];
  supersededPacks: AdminPackState[];
};
export type CompactionData = DebugState["compaction"];

type PackRefIndexMetadata = {
  status: PackRefIndexStatus;
  size?: number;
};

export async function badRequest(
  env: Env,
  title: string,
  message: string,
  extra?: { owner?: string; repo?: string; refEnc?: string; path?: string }
): Promise<Response> {
  return handleError(env, new HttpError(400, message, { expose: true }), title, extra);
}

export function formatFromNowShort(deltaMs: number): string {
  const s = Math.round(deltaMs / 1000);
  if (s <= 0) return "soon";
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  const d = Math.floor(h / 24);
  if (d > 0) return `in ${d}d ${h % 24}h`;
  if (h > 0) return `in ${h}h ${m % 60}m`;
  if (m > 0) return `in ${m}m`;
  return `in ${s}s`;
}

// Re-export the cache-policy predicates so existing UI handlers don't need
// to know they live in the cache layer. The lower Git/protocol modules
// import directly from `@/worker/cache/policy`.
export { isRequestPrivate, markRequestPrivate };

export async function loadHeadAndRefsCached(
  env: Env,
  cacheCtx: CacheContext,
  repoId: string
): Promise<{ head: HeadInfo | undefined; refs: Ref[] } | null> {
  const loader = async (): Promise<{ head: HeadInfo | undefined; refs: Ref[] } | null> => {
    try {
      const res = await getHeadAndRefs(env, repoId, cacheCtx);
      return { head: res.head, refs: res.refs };
    } catch {
      return null;
    }
  };
  const cacheKeyRefs = buildCacheKeyFrom(cacheCtx.req, "/_cache/refs", { repo: repoId });
  return cacheOrLoadJSONForRequest<{ head: HeadInfo | undefined; refs: Ref[] }>(
    cacheCtx,
    cacheKeyRefs,
    loader,
    60
  );
}

// Shared 404 response for repo-serving handlers. Centralizes the SSR shell +
// viewer load so handlers don't reach into `index.ts` internals. Callers
// that have already resolved a viewer can pass it through to avoid a
// second D1 round trip.
export async function notFound(
  c: AppContext,
  title?: string,
  viewer?: Viewer | null
): Promise<Response> {
  const resolvedViewer = viewer === undefined ? await loadViewer(c) : viewer;
  return renderUiDocumentResponse(c.env, "404", title ? { title } : {}, {
    status: 404,
    failureBody: "Not found\n",
    failureStatus: 404,
    viewer: resolvedViewer,
  });
}

// Same as `notFound` but returns a JSON 404 — used by data API endpoints
// (`/api/refs`) where SSR shell would be wasted bytes.
export function notFoundJson(): Response {
  return new Response(JSON.stringify({ error: "Not found" }), {
    status: 404,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

// Bundle for the resolve+session+visibility decision shared by every UI
// repo-serving handler. Returns either:
//   - { kind: "ok", route, cacheCtx, viewer, showActivityBanner }: caller
//     proceeds to render. The `cacheCtx` is already marked private when
//     private repository data is rendered.
//   - { kind: "response", response }: caller returns the response as-is.
//     Centralizes the non-disclosure rule (private + non-member -> 404,
//     identical to private + anonymous).
export type UiRepoAccess =
  | {
      kind: "ok";
      route: RepositoryRoute;
      cacheCtx: CacheContext;
      viewer: Viewer | null;
      showActivityBanner: boolean;
    }
  | { kind: "response"; response: Response };

export type AdminRepoAccess =
  | { kind: "ok"; route: RepositoryRoute; cacheCtx: CacheContext; viewer: Viewer; limiter: Limiter }
  | { kind: "response"; response: Response };

export type UiRepoAccessOptions = {
  // For data-API endpoints that return JSON (not HTML) on failure.
  responseShape?: "html" | "json";
};

export async function resolveUiRepoAccess(
  c: AppContext,
  owner: string,
  repo: string,
  options: UiRepoAccessOptions = {}
): Promise<UiRepoAccess> {
  const cacheCtx = c.var.cacheCtx;
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return {
      kind: "response",
      response:
        options.responseShape === "json" ? notFoundJson() : await notFound(c, "Invalid owner/repo"),
    };
  }
  const viewer = await loadViewer(c);
  const route = await resolveRepositoryRoute(c.env, owner, repo, {
    mode: viewer ? "allow-d1-fallback" : "route-cache-only",
    db: c.var.db,
    log: c.var.logFor({ service: "RepoRoute" }),
  });
  if (!route) {
    return {
      kind: "response",
      response: options.responseShape === "json" ? notFoundJson() : await notFound(c),
    };
  }
  if (route.visibility === "public") {
    if (!viewer) {
      return { kind: "ok", route, cacheCtx, viewer, showActivityBanner: false };
    }
    const membership = await loadSessionMembership(c, route.namespaceId);
    const showActivityBanner = membership.kind === "member";
    return {
      kind: "ok",
      route,
      cacheCtx,
      viewer: membership.kind === "anonymous" ? viewer : membership.viewer,
      showActivityBanner,
    };
  }
  if (!viewer) {
    const log = c.var.logFor({ service: "UiAcl", repoId: route.doName });
    log.debug("ui-acl:private-non-member-404", { kind: "anonymous" });
    return {
      kind: "response",
      response: options.responseShape === "json" ? notFoundJson() : await notFound(c),
    };
  }
  // Private: gate on session membership. PAT credentials are never honored
  // for UI/data routes (PATs are git-only).
  const membership = await loadSessionMembership(c, route.namespaceId);
  const log = c.var.logFor({ service: "UiAcl", repoId: route.doName });
  if (membership.kind !== "member") {
    log.debug("ui-acl:private-non-member-404", { kind: membership.kind });
    // `loadSessionMembership` returns the viewer for signed-in-non-member
    // results; reuse it so the 404 page renders the correct shell without a
    // second D1 round-trip.
    const passthroughViewer = membership.kind === "signed-in-non-member" ? membership.viewer : null;
    return {
      kind: "response",
      response:
        options.responseShape === "json"
          ? notFoundJson()
          : await notFound(c, undefined, passthroughViewer),
    };
  }
  markRequestPrivate(cacheCtx);
  return { kind: "ok", route, cacheCtx, viewer: membership.viewer, showActivityBanner: true };
}

export async function loadUiRepoActivity(
  env: Env,
  access: Extract<UiRepoAccess, { kind: "ok" }>
): Promise<RepoActivity | null> {
  if (!access.showActivityBanner) return null;
  return await getRepoActivity(env, access.route.doName, access.cacheCtx);
}

function adminForbidden(): Response {
  return new Response("Forbidden\n", {
    status: 403,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

// Browser admin access is session-only. Public repositories may disclose
// their existence, but private repositories return 404 to anonymous users
// and signed-in non-members so the admin surface does not become a
// membership oracle.
export async function resolveAdminPageRepoAccess(
  c: AppContext,
  owner: string,
  repo: string
): Promise<AdminRepoAccess> {
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return {
      kind: "response",
      response: await badRequest(c.env, "Invalid owner/repo", "Owner or repo invalid", {
        owner,
        repo,
      }),
    };
  }

  const viewerForResolution = await loadViewer(c);
  const route = await resolveRepositoryRoute(c.env, owner, repo, {
    mode: viewerForResolution ? "allow-d1-fallback" : "route-cache-only",
    db: c.var.db,
    log: c.var.logFor({ service: "RepoRoute" }),
  });
  if (!route) return { kind: "response", response: await notFound(c) };

  const membership = await loadSessionMembership(c, route.namespaceId);
  if (membership.kind === "anonymous") {
    if (route.visibility === "private") {
      return { kind: "response", response: await notFound(c) };
    }
    return {
      kind: "response",
      response: c.redirect(`/auth?next=${encodeURIComponent(`/${owner}/${repo}/admin`)}`, 302),
    };
  }
  if (membership.kind === "signed-in-non-member") {
    if (route.visibility === "private") {
      return { kind: "response", response: await notFound(c) };
    }
    return { kind: "response", response: adminForbidden() };
  }

  const cacheCtx = c.var.cacheCtx;
  markRequestPrivate(cacheCtx);
  return { kind: "ok", route, cacheCtx, viewer: membership.viewer, limiter: c.var.limiter };
}

function adminJsonError(message: string, status: number): Response {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

// JSON admin endpoints keep the same disclosure model as the admin page,
// but public anonymous callers receive 401 instead of the sign-in redirect
// and public signed-in non-members receive 403.
export async function resolveAdminApiRepoAccess(c: AppContext): Promise<AdminRepoAccess> {
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  if (!owner || !repo || !isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return { kind: "response", response: adminJsonError("Not found", 404) };
  }

  const viewerForResolution = await loadViewer(c);
  const route = await resolveRepositoryRoute(c.env, owner, repo, {
    mode: viewerForResolution ? "allow-d1-fallback" : "route-cache-only",
    db: c.var.db,
    log: c.var.logFor({ service: "RepoRoute" }),
  });
  if (!route) return { kind: "response", response: adminJsonError("Not found", 404) };

  const membership = await loadSessionMembership(c, route.namespaceId);
  if (membership.kind === "anonymous") {
    if (route.visibility === "private") {
      return { kind: "response", response: adminJsonError("Not found", 404) };
    }
    return { kind: "response", response: adminJsonError("Unauthorized", 401) };
  }
  if (membership.kind === "signed-in-non-member") {
    if (route.visibility === "private") {
      return { kind: "response", response: adminJsonError("Not found", 404) };
    }
    return { kind: "response", response: adminJsonError("Forbidden", 403) };
  }

  const cacheCtx = c.var.cacheCtx;
  markRequestPrivate(cacheCtx);
  return { kind: "ok", route, cacheCtx, viewer: membership.viewer, limiter: c.var.limiter };
}

export function getDefaultBranchFromHead(head: HeadInfo | undefined): string {
  return head?.target ? shortRefName(head.target) : "main";
}

function collectPackKeys(state: Partial<DebugStateSnapshot>): string[] {
  const keys = new Set<string>();
  for (const pack of state.packStats ?? []) keys.add(pack.key);
  for (const pack of state.activePacks ?? []) keys.add(pack.key);
  for (const pack of state.supersededPacks ?? []) keys.add(pack.key);
  return [...keys];
}

function applyPackRefMetadata(
  packs: DebugPackState[] | undefined,
  metadataByPackKey: Map<string, PackRefIndexMetadata>
): AdminPackState[] | undefined {
  if (!packs) return undefined;

  return packs.map((pack) => {
    const metadata = metadataByPackKey.get(pack.key);
    if (!metadata) {
      return { ...pack, refIndexStatus: "unknown" };
    }

    return {
      ...pack,
      refIndexStatus: metadata.status,
      refIndexSize: metadata.size,
    };
  });
}

async function loadPackRefIndexMetadata(args: {
  env: Env;
  repoId: string;
  state: Partial<DebugStateSnapshot>;
  cacheCtx: CacheContext;
}): Promise<Map<string, PackRefIndexMetadata>> {
  const packKeys = collectPackKeys(args.state);
  const metadataByPackKey = new Map<string, PackRefIndexMetadata>();
  if (packKeys.length === 0) return metadataByPackKey;

  const limiter = getLimiter(args.cacheCtx);
  const log = createLogger(args.env.LOG_LEVEL, { service: "AdminPage", repoId: args.repoId });
  const entries = await Promise.all(
    packKeys.map(async (packKey): Promise<[string, PackRefIndexMetadata]> => {
      const refsKey = packRefsKey(packKey);
      try {
        const refsObject = await limiter.run("r2:head-pack-refs", async () => {
          if (!countSubrequest(args.cacheCtx)) {
            log.warn("admin:pack-ref-head-budget-exhausted", { packKey, refsKey });
          }
          return await args.env.REPO_BUCKET.head(refsKey);
        });

        if (!refsObject) {
          log.debug("admin:pack-ref-head-missing", { packKey, refsKey });
          return [packKey, { status: "missing" }];
        }

        log.debug("admin:pack-ref-head-present", {
          packKey,
          refsKey,
          bytes: refsObject.size,
        });
        return [packKey, { status: "present", size: refsObject.size }];
      } catch (error) {
        log.warn("admin:pack-ref-head-failed", {
          packKey,
          refsKey,
          error: String(error),
        });
        return [packKey, { status: "unknown" }];
      }
    })
  );

  let present = 0;
  let missing = 0;
  let unknown = 0;
  for (const [packKey, metadata] of entries) {
    metadataByPackKey.set(packKey, metadata);
    if (metadata.status === "present") present++;
    else if (metadata.status === "missing") missing++;
    else unknown++;
  }
  log.debug("admin:pack-ref-head-summary", {
    packs: packKeys.length,
    present,
    missing,
    unknown,
  });
  return metadataByPackKey;
}

export async function loadAdminPackRefIndexState(args: {
  env: Env;
  repoId: string;
  state: Partial<DebugStateSnapshot>;
  cacheCtx: CacheContext;
}): Promise<Partial<DebugState>> {
  const metadataByPackKey = await loadPackRefIndexMetadata(args);
  return {
    ...args.state,
    packStats: applyPackRefMetadata(args.state.packStats, metadataByPackKey),
    activePacks: applyPackRefMetadata(args.state.activePacks, metadataByPackKey),
    supersededPacks: applyPackRefMetadata(args.state.supersededPacks, metadataByPackKey),
  };
}

export function computeStorageMetrics(state: Partial<DebugState> | undefined): {
  storageSize: string;
  packCount: number;
  packList: string[];
  supersededPackCount: number;
} {
  let totalStorageBytes = 0;
  const packStats = state?.packStats ?? [];
  for (const pack of packStats) {
    if (typeof pack.packSize === "number") totalStorageBytes += pack.packSize;
    if (typeof pack.indexSize === "number") totalStorageBytes += pack.indexSize;
    if (typeof pack.refIndexSize === "number") totalStorageBytes += pack.refIndexSize;
  }
  const storageSize = formatSize(totalStorageBytes);
  const activePacks = state?.activePacks ?? [];
  const packList = activePacks.map((pack) => pack.key);
  const packCount = packList.length;
  const supersededPackCount = state?.supersededPacks?.length ?? 0;
  return { storageSize, packCount, packList, supersededPackCount };
}

export function computeCompactionStatus(compactionData: CompactionData | undefined): {
  compactionStatus: string;
  compactionStartedAt: string | null;
} {
  let compactionStatus = "Idle";
  let compactionStartedAt: string | null = null;
  if (compactionData?.running) {
    compactionStatus = "Running";
    if (compactionData.startedAt) {
      try {
        compactionStartedAt = new Date(compactionData.startedAt).toLocaleString();
      } catch {}
    }
  } else if (compactionData?.queued) {
    compactionStatus = "Queued";
  }
  return { compactionStatus, compactionStartedAt };
}
