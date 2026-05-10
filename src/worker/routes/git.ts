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
import { asBodyInit, createLogger, gunzip } from "@/worker/common";
import { buildCacheKeyFrom, cacheOrLoadJSON } from "@/worker/cache";
import { isValidOwnerRepo } from "@/shared/web";
import { addRepoToOwner, removeRepoFromOwner } from "@/worker/registry";
import { resolveRepositoryRoute, type RepositoryRoute } from "@/worker/repositories/route";
import { verifyAuth } from "@/worker/auth";
import { authenticateGitRequest, scheduleTouchPatLastUsedAt } from "@/worker/auth/gitAuth";
import { createDb } from "@/worker/db/d1/client";
import { touchRepositoryUpdatedAt } from "@/worker/db/d1/dal/repositories";
import { isRequestPrivate, markRequestPrivate, requestCacheContext } from "./ui/helpers";
import type { AppContext, AppRouter } from "./hono";

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

async function applyLegacyOwnerRegistrySideEffects(
  env: Env,
  route: RepositoryRoute,
  changed: boolean,
  empty: boolean
): Promise<void> {
  if (!changed) return;
  try {
    if (empty) await removeRepoFromOwner(env, route.routeNamespaceSlug, route.routeRepoSlug);
    else await addRepoToOwner(env, route.routeNamespaceSlug, route.routeRepoSlug);
  } catch {}
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
  ctx: ExecutionContext,
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
    let refsData: { head: HeadInfo | undefined; refs: Ref[] } | null;
    if (isRequestPrivate(cacheCtx)) {
      refsData = await loader();
    } else {
      const cacheKeyRefs = buildCacheKeyFrom(request, "/_cache/refs", { repo: route.doName });
      refsData = await cacheOrLoadJSON<{ head: HeadInfo | undefined; refs: Ref[] }>(
        cacheKeyRefs,
        loader,
        60,
        ctx
      );
    }
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
        "Cache-Control": isRequestPrivate(cacheCtx) ? "no-store" : "no-cache",
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
  ctx: ExecutionContext
) {
  const log = createLogger(env.LOG_LEVEL, { service: "ReceiveAcl", repoId: route.doName });
  return await handleStreamingReceivePackPOST(env, route.doName, request, ctx, {
    onRepoStateChanged: async ({ changed, empty }) => {
      if (!changed) return;
      try {
        await touchRepositoryUpdatedAt(createDb(env.DB), route.repositoryId, Date.now());
        log.debug("receive:repo-updated-at-touched", { repositoryId: route.repositoryId });
      } catch (error) {
        log.warn("receive:repo-updated-at-failed", {
          repositoryId: route.repositoryId,
          error: String(error),
        });
      }
      await applyLegacyOwnerRegistrySideEffects(env, route, changed, empty);
    },
  });
}

// Validate URL slug shape before any DB/DO/R2 work. Mirrors `repoKey` validity.
function validateRouteSlugs(owner: string, repo: string): boolean {
  return isValidOwnerRepo(owner) && isValidOwnerRepo(repo);
}

// Decide whether a Git read (info-refs upload-pack, git-upload-pack) is
// allowed for the resolved route given the authenticated principal. Returns
// `null` when allowed, otherwise the response to send.
function gateGitRead(
  c: AppContext,
  route: RepositoryRoute,
  auth: Awaited<ReturnType<typeof authenticateGitRequest>>
): Response | null {
  if (route.visibility === "public") return null;
  const log = createLogger(c.env.LOG_LEVEL, { service: "GitAcl", repoId: route.doName });
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

// Private repos always require a valid PAT with push. Public repos fall
// back to `verifyAuth` so push stays open when `AUTH_ADMIN_TOKEN` is unset.
async function gateGitPush(
  c: AppContext,
  route: RepositoryRoute,
  auth: Awaited<ReturnType<typeof authenticateGitRequest>>,
  request: Request,
  isDiscovery: boolean
): Promise<Response | null> {
  const log = createLogger(c.env.LOG_LEVEL, { service: "GitAcl", repoId: route.doName });
  if (auth.kind === "pat") {
    if (auth.verified.level !== "push") {
      log.info("git-acl:push-pull-only", { patId: auth.verified.patId });
      return forbidden();
    }
    return null;
  }
  if (auth.kind === "pat-rejected" && auth.reason !== "malformed") {
    if (auth.reason === "grant-missing") {
      log.info("git-acl:pat-rejected", { reason: auth.reason });
      return forbidden();
    }
    log.info("git-acl:pat-rejected", { reason: auth.reason });
    return basicChallenge();
  }
  if (route.visibility === "private") {
    log.info("git-acl:push-401-challenge", { reason: "private-no-pat", discovery: isDiscovery });
    return basicChallenge();
  }
  if (await verifyAuth(c.env, route.routeNamespaceSlug, request, false)) {
    return null;
  }
  log.info("git-acl:push-401-challenge", { reason: "legacy-rejected", discovery: isDiscovery });
  return basicChallenge();
}

/**
 * Registers Git Smart HTTP v2 routes on the router.
 */
export function registerGitRoutes(router: AppRouter) {
  router.get(`/:owner/:repo/info/refs`, async (c) => {
    const owner = c.req.param("owner");
    const repo = c.req.param("repo");
    if (!validateRouteSlugs(owner, repo)) return gitNotFound();
    const url = new URL(c.req.url);
    const service = url.searchParams.get("service");
    if (service !== "git-upload-pack" && service !== "git-receive-pack") {
      return new Response("Missing or unsupported service\n", { status: 400 });
    }
    const route = await resolveRepositoryRoute(c.env, owner, repo);
    if (!route) return gitNotFound();
    const cacheCtx = requestCacheContext(c);
    if (route.visibility === "private") markRequestPrivate(cacheCtx);
    const auth = await authenticateGitRequest(c.env, c.req.raw, route);
    const blocked =
      service === "git-receive-pack"
        ? await gateGitPush(c, route, auth, c.req.raw, true)
        : gateGitRead(c, route, auth);
    if (blocked) return blocked;
    if (auth.kind === "pat") {
      // Discovery hop counts as a read; throttle policy applies.
      scheduleTouchPatLastUsedAt(c.env, c.executionCtx, auth.verified, "read");
    }
    return await capabilityAdvertisement(c.env, service, route.doName, cacheCtx);
  });

  router.post(`/:owner/:repo/git-upload-pack`, async (c) => {
    const owner = c.req.param("owner");
    const repo = c.req.param("repo");
    if (!validateRouteSlugs(owner, repo)) return gitNotFound();
    const route = await resolveRepositoryRoute(c.env, owner, repo);
    if (!route) return gitNotFound();
    const cacheCtx = requestCacheContext(c);
    if (route.visibility === "private") markRequestPrivate(cacheCtx);
    const auth = await authenticateGitRequest(c.env, c.req.raw, route);
    const blocked = gateGitRead(c, route, auth);
    if (blocked) return blocked;
    if (auth.kind === "pat") {
      scheduleTouchPatLastUsedAt(c.env, c.executionCtx, auth.verified, "read");
    }
    return handleUploadPackPOST(c.env, route, c.req.raw, c.executionCtx, cacheCtx);
  });

  router.post(`/:owner/:repo/git-receive-pack`, async (c) => {
    const owner = c.req.param("owner");
    const repo = c.req.param("repo");
    if (!validateRouteSlugs(owner, repo)) return gitNotFound();
    const route = await resolveRepositoryRoute(c.env, owner, repo);
    if (!route) return gitNotFound();
    const cacheCtx = requestCacheContext(c);
    markRequestPrivate(cacheCtx);
    const auth = await authenticateGitRequest(c.env, c.req.raw, route);
    const blocked = await gateGitPush(c, route, auth, c.req.raw, false);
    if (blocked) return blocked;
    if (auth.kind === "pat") {
      scheduleTouchPatLastUsedAt(c.env, c.executionCtx, auth.verified, "write");
    }
    return await handleReceivePackPOST(c.env, route, c.req.raw, c.executionCtx);
  });
}
