import type { HeadInfo, Ref } from "@/worker/git";
import type { CacheContext } from "@/worker/cache/cache";

import {
  capabilityAdvertisement,
  parseV2Command,
  pktLine,
  flushPkt,
  concatChunks,
  getHeadAndRefs,
} from "@/worker/git";
import { loadPeeledTagTargets } from "@/worker/git/object-store";
import { handleFetchV2Streaming } from "@/worker/git/operations/uploadStream";
import { handleStreamingReceivePackPOST } from "@/worker/git/receive/streamReceivePack";
import { asBodyInit, gunzip } from "@/worker/common";
import { buildCacheKeyFrom, cacheOrLoadJSONForRequest } from "@/worker/cache";
import { markRequestPrivate, responseCacheControl } from "@/worker/cache/policy";
import { isValidOwnerRepo } from "@/shared/web";
import { resolveRepositoryRoute, type RepositoryRoute } from "@/worker/repositories/route";
import {
  authenticateGitRequest,
  getBasicCredentials,
  scheduleTouchPatLastUsedAt,
  type GitAuthResult,
} from "@/worker/auth/gitAuth";
import type { Db } from "@/worker/db/d1/client";
import type { Logger } from "@/worker/common/logger";
import { touchRepositoryUpdatedAt } from "@/worker/db/d1/dal/repositories";
import { workerExecutionContext, type AppContext, type AppRouter } from "./hono";

type GitService = "git-upload-pack" | "git-receive-pack";
type PatTouchOp = "read" | "write";

// Realm string emitted on Basic auth challenges so git CLI prompts the user
// with a recognisable label.
const GIT_BASIC_REALM = 'Basic realm="git", charset="UTF-8"';

function basicChallenge(): Response {
  return new Response("Authentication required\n", {
    status: 401,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "WWW-Authenticate": GIT_BASIC_REALM,
      "Cache-Control": "no-store",
    },
  });
}

function forbidden(message = "Forbidden\n"): Response {
  return new Response(message, {
    status: 403,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

function gitNotFound(): Response {
  return new Response("Not found\n", {
    status: 404,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

async function decodeUploadPackBody(request: Request): Promise<Uint8Array | Response> {
  const rawBody = new Uint8Array(await request.arrayBuffer());
  const contentEncoding = (request.headers.get("Content-Encoding") || "").trim().toLowerCase();

  if (!contentEncoding || contentEncoding === "identity") {
    return rawBody;
  }

  if (contentEncoding !== "gzip") {
    return new Response(`Unsupported Content-Encoding: ${contentEncoding}\n`, {
      status: 415,
      headers: { "Content-Type": "text/plain; charset=utf-8" },
    });
  }

  try {
    return await gunzip(rawBody);
  } catch {
    return new Response("Invalid gzip request body\n", {
      status: 400,
      headers: { "Content-Type": "text/plain; charset=utf-8" },
    });
  }
}

/**
 * Handles Git upload-pack (fetch) POST requests.
 * Supports both protocol v2 and legacy protocol based on Git-Protocol header.
 */
async function handleUploadPackPOST(
  env: Env,
  route: RepositoryRoute,
  request: Request,
  cacheCtx: CacheContext
) {
  const decodedBody = await decodeUploadPackBody(request);
  if (decodedBody instanceof Response) return decodedBody;
  const body = decodedBody;
  const gitProto = request.headers.get("Git-Protocol") || "";
  const { command } = parseV2Command(body);
  // Accept either explicit v2 header or a v2-formatted body (contains command=...)
  if (!/version=2/.test(gitProto) && !command) {
    return new Response("Expected Git protocol v2 (set Git-Protocol: version=2)\n", {
      status: 400,
    });
  }

  if (command === "ls-refs") {
    const loader = async (): Promise<{ head: HeadInfo | undefined; refs: Ref[] } | null> => {
      try {
        const result = await getHeadAndRefs(env, route.doName, cacheCtx);
        return { head: result.head, refs: result.refs };
      } catch {
        return null;
      }
    };
    const cacheKeyRefs = buildCacheKeyFrom(request, "/_cache/refs", { repo: route.doName });
    const refsData = await cacheOrLoadJSONForRequest<{ head: HeadInfo | undefined; refs: Ref[] }>(
      cacheCtx,
      cacheKeyRefs,
      loader,
      60
    );
    const { head, refs } = refsData || { refs: [] };

    // Parse ls-refs arguments (reuse already-read body to avoid double-read of the stream)
    const { args } = parseV2Command(body);
    const refPrefixes: string[] = [];
    let wantPeel = false;
    for (const a of args) {
      if (a === "peel") wantPeel = true;
      else if (a.startsWith("ref-prefix ")) refPrefixes.push(a.slice("ref-prefix ".length));
    }

    let filteredRefs = refs;
    if (refPrefixes.length > 0) {
      filteredRefs = refs.filter((r) => refPrefixes.some((p) => r.name.startsWith(p)));
    }

    let peeledByRef = new Map<string, string>();
    if (wantPeel) {
      try {
        const tagRefs = filteredRefs.filter((r) => r.name.startsWith("refs/tags/"));
        if (tagRefs.length > 0) {
          peeledByRef = await loadPeeledTagTargets(env, route.doName, tagRefs, cacheCtx);
        }
      } catch {}
    }

    const chunks: Uint8Array[] = [];
    if (head && head.target) {
      const t =
        filteredRefs.find((r) => r.name === head.target) ||
        refs.find((r) => r.name === head.target);
      const headOid = head.oid ?? t?.oid;
      const headLineAttrs: string[] = [];
      headLineAttrs.push(`symref-target:${head.target}`);
      if (headOid) {
        const base = [`${headOid} HEAD`, ...headLineAttrs].join(" ");
        chunks.push(pktLine(base + "\n"));
      } else {
        const base = ["unborn HEAD", ...headLineAttrs].join(" ");
        chunks.push(pktLine(base + "\n"));
      }
    }

    for (const r of filteredRefs) {
      const attrs: string[] = [];
      if (wantPeel) {
        const peeled = peeledByRef.get(r.name);
        if (peeled) attrs.push(`peeled:${peeled}`);
      }
      const line =
        attrs.length > 0 ? `${r.oid} ${r.name} ${attrs.join(" ")}` : `${r.oid} ${r.name}`;
      chunks.push(pktLine(line + "\n"));
    }
    chunks.push(flushPkt());
    return new Response(asBodyInit(concatChunks(chunks)), {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-upload-pack-result",
        "Cache-Control": responseCacheControl(cacheCtx),
      },
    });
  }

  if (command === "fetch") {
    return handleFetchV2Streaming(env, route.doName, body, request.signal, cacheCtx);
  }

  return new Response("Unsupported command or malformed request\n", { status: 400 });
}

async function handleReceivePackPOST(
  env: Env,
  route: RepositoryRoute,
  request: Request,
  ctx: ExecutionContext,
  db: Db,
  log: Logger
) {
  return await handleStreamingReceivePackPOST(env, route.doName, request, ctx, {
    onRepoStateChanged: async ({ changed }) => {
      if (!changed) return;
      try {
        await touchRepositoryUpdatedAt(db, route.repositoryId, Date.now());
        log.debug("receive:repo-updated-at-touched", { repositoryId: route.repositoryId });
      } catch (error) {
        log.warn("receive:repo-updated-at-failed", {
          repositoryId: route.repositoryId,
          error: String(error),
        });
      }
    },
  });
}

// Validate URL slug shape before any DB/DO/R2 work. Mirrors `repoKey` validity.
function validateRouteSlugs(owner: string, repo: string): boolean {
  return isValidOwnerRepo(owner) && isValidOwnerRepo(repo);
}

function normalizeGitRouteRepoSlug(repo: string): string {
  // Git clients append Smart HTTP endpoints to the clone URL. Accept
  // clone-style `/repo.git/...` URLs while resolving storage against the
  // canonical repository slug.
  return repo.endsWith(".git") ? repo.slice(0, -".git".length) : repo;
}

function gitRequestAllowsD1Fallback(request: Request): boolean {
  const credentials = getBasicCredentials(request);
  return credentials !== null && credentials.password.length > 0;
}

async function resolveGitRoute(
  c: AppContext,
  owner: string,
  repo: string
): Promise<RepositoryRoute | null> {
  return await resolveRepositoryRoute(c.env, owner, repo, {
    mode: gitRequestAllowsD1Fallback(c.req.raw) ? "allow-d1-fallback" : "route-cache-only",
    db: c.var.db,
    log: c.var.logFor({ service: "RepoRoute" }),
  });
}

type ResolveGitRouteResult =
  | { kind: "ok"; route: RepositoryRoute }
  | { kind: "response"; response: Response };

async function resolveGitRouteForRequest(
  c: AppContext,
  owner: string,
  repo: string,
  service: GitService,
  isDiscovery: boolean
): Promise<ResolveGitRouteResult> {
  const route = await resolveGitRoute(c, owner, repo);
  if (route) return { kind: "ok", route };

  // Git sends the first receive-pack discovery request before it has
  // credentials. If the route cache has no candidate yet (or the repo is
  // private and intentionally absent from ROUTES), challenge so the client
  // can retry with Basic/PAT. A request that already carried a Basic
  // password has used D1 fallback and remains a real 404 when unresolved.
  if (service === "git-receive-pack" && !gitRequestAllowsD1Fallback(c.req.raw)) {
    return {
      kind: "response",
      response: challengeUnresolvedPushRoute(c, owner, repo, isDiscovery),
    };
  }
  return { kind: "response", response: gitNotFound() };
}

function challengeUnresolvedPushRoute(
  c: AppContext,
  owner: string,
  repo: string,
  isDiscovery: boolean
): Response {
  const log = c.var.logFor({ service: "GitAcl" });
  log.info("git-acl:push-route-miss-401-challenge", {
    owner,
    repo,
    discovery: isDiscovery,
  });
  return basicChallenge();
}

function gateD1FallbackGitAuth(
  c: AppContext,
  route: RepositoryRoute,
  auth: GitAuthResult
): Response | null {
  if (route.source !== "d1") return null;
  if (auth.kind === "pat") return null;
  const log = c.var.logFor({ service: "GitAcl", repoId: route.doName });
  if (auth.kind === "pat-rejected" && auth.reason === "grant-missing") {
    log.info("git-acl:d1-fallback-grant-missing", { reason: auth.reason });
    return forbidden();
  }
  log.info("git-acl:d1-fallback-unauthorized", { reason: auth.kind });
  return basicChallenge();
}

// Decide whether a Git read (info-refs upload-pack, git-upload-pack) is
// allowed for the resolved route given the authenticated principal. Returns
// `null` when allowed, otherwise the response to send.
function gateGitRead(c: AppContext, route: RepositoryRoute, auth: GitAuthResult): Response | null {
  if (route.visibility === "public") return null;
  const log = c.var.logFor({ service: "GitAcl", repoId: route.doName });
  switch (auth.kind) {
    case "anonymous":
      // Discovery hop on private upload-pack: 404 to avoid leaking existence.
      log.info("git-acl:private-404", { reason: "anonymous-read" });
      return gitNotFound();
    case "missing-credentials":
      log.info("git-acl:private-401-challenge", { reason: "missing-credentials" });
      return basicChallenge();
    case "pat-rejected":
      // Any PAT failure on a read is reported as 401 so the client can retry
      // with fresh creds, EXCEPT grant-missing which is 403 (the user has
      // proven their identity but lacks access).
      if (auth.reason === "grant-missing") {
        log.info("git-acl:pat-rejected", { reason: auth.reason });
        return forbidden();
      }
      log.info("git-acl:pat-rejected", { reason: auth.reason });
      return basicChallenge();
    case "pat":
      return null;
  }
}

// Push (receive-pack) requires a PAT with `level === "push"` regardless of
// repo visibility. Public repos do NOT fall through to anonymous push: the
// resolved D1 row is the only authority, and it has no per-repo "anyone can
// push" toggle. Treat any non-PAT-push principal as an auth challenge or
// 403 by reason.
async function gateGitPush(
  c: AppContext,
  route: RepositoryRoute,
  auth: GitAuthResult,
  isDiscovery: boolean
): Promise<Response | null> {
  const log = c.var.logFor({ service: "GitAcl", repoId: route.doName });
  if (auth.kind === "pat") {
    if (auth.verified.level !== "push") {
      log.info("git-acl:push-pull-only", { patId: auth.verified.patId });
      return forbidden();
    }
    return null;
  }
  if (auth.kind === "pat-rejected") {
    if (auth.reason === "grant-missing") {
      log.info("git-acl:pat-rejected", { reason: auth.reason });
      return forbidden();
    }
    log.info("git-acl:pat-rejected", { reason: auth.reason });
    return basicChallenge();
  }
  // anonymous | missing-credentials -> 401 challenge so the git client
  // re-issues with Basic credentials.
  log.info("git-acl:push-401-challenge", {
    reason: auth.kind === "anonymous" ? "anonymous" : "missing-credentials",
    discovery: isDiscovery,
    visibility: route.visibility,
  });
  return basicChallenge();
}

type GitAuthorizationResult =
  | { kind: "ok"; cacheCtx: CacheContext }
  | { kind: "response"; response: Response };

async function authorizeGitRouteForRequest(
  c: AppContext,
  route: RepositoryRoute,
  service: GitService,
  isDiscovery: boolean,
  patTouchOp: PatTouchOp
): Promise<GitAuthorizationResult> {
  const cacheCtx = c.var.cacheCtx;
  if (route.visibility === "private" || service === "git-receive-pack") {
    markRequestPrivate(cacheCtx);
  }

  const auth = await authenticateGitRequest(c.env, c.req.raw, route, { db: c.var.db });
  const fallbackBlocked = gateD1FallbackGitAuth(c, route, auth);
  if (fallbackBlocked) return { kind: "response", response: fallbackBlocked };

  const blocked =
    service === "git-receive-pack"
      ? await gateGitPush(c, route, auth, isDiscovery)
      : gateGitRead(c, route, auth);
  if (blocked) return { kind: "response", response: blocked };

  if (auth.kind === "pat") {
    // PAT `last_used_at` is a visibility signal for token management only;
    // it is throttled for reads and always attempted for writes.
    scheduleTouchPatLastUsedAt(
      c.env,
      workerExecutionContext(c),
      auth.verified,
      patTouchOp,
      Date.now(),
      {
        db: c.var.db,
        log: c.var.logFor({ service: "GitAuth" }),
      }
    );
  }

  return { kind: "ok", cacheCtx };
}

/**
 * Registers Git Smart HTTP v2 routes on the router.
 */
export function registerGitRoutes(router: AppRouter) {
  router.get(`/:owner/:repo/info/refs`, async (c) => {
    const owner = c.req.param("owner");
    const repo = normalizeGitRouteRepoSlug(c.req.param("repo"));
    if (!validateRouteSlugs(owner, repo)) return gitNotFound();
    const url = new URL(c.req.url);
    const service = url.searchParams.get("service");
    if (service !== "git-upload-pack" && service !== "git-receive-pack") {
      return new Response("Missing or unsupported service\n", { status: 400 });
    }
    const resolved = await resolveGitRouteForRequest(c, owner, repo, service, true);
    if (resolved.kind === "response") return resolved.response;
    const route = resolved.route;
    const authorized = await authorizeGitRouteForRequest(c, route, service, true, "read");
    if (authorized.kind === "response") return authorized.response;
    const { cacheCtx } = authorized;
    return await capabilityAdvertisement(c.env, service, route.doName, cacheCtx);
  });

  router.post(`/:owner/:repo/git-upload-pack`, async (c) => {
    const owner = c.req.param("owner");
    const repo = normalizeGitRouteRepoSlug(c.req.param("repo"));
    if (!validateRouteSlugs(owner, repo)) return gitNotFound();
    const resolved = await resolveGitRouteForRequest(c, owner, repo, "git-upload-pack", false);
    if (resolved.kind === "response") return resolved.response;
    const route = resolved.route;
    const authorized = await authorizeGitRouteForRequest(
      c,
      route,
      "git-upload-pack",
      false,
      "read"
    );
    if (authorized.kind === "response") return authorized.response;
    return handleUploadPackPOST(c.env, route, c.req.raw, authorized.cacheCtx);
  });

  router.post(`/:owner/:repo/git-receive-pack`, async (c) => {
    const owner = c.req.param("owner");
    const repo = normalizeGitRouteRepoSlug(c.req.param("repo"));
    if (!validateRouteSlugs(owner, repo)) return gitNotFound();
    const resolved = await resolveGitRouteForRequest(c, owner, repo, "git-receive-pack", false);
    if (resolved.kind === "response") return resolved.response;
    const route = resolved.route;
    const authorized = await authorizeGitRouteForRequest(
      c,
      route,
      "git-receive-pack",
      false,
      "write"
    );
    if (authorized.kind === "response") return authorized.response;
    return await handleReceivePackPOST(
      c.env,
      route,
      c.req.raw,
      workerExecutionContext(c),
      c.var.db,
      c.var.logFor({ service: "ReceiveAcl", repoId: route.doName })
    );
  });
}
