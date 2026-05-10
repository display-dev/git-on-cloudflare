import type { HeadInfo, Ref } from "@/worker/git";
import { getRepoActivity, getRepoStub } from "@/worker/common";
import { isValidOwnerRepo } from "@/shared/web";
import { loadSessionMembership } from "@/worker/auth/sessionMembership";
import { resolveRepositoryRoute } from "@/worker/repositories/route";
import { getLimiter } from "@/worker/git/operations/limits";
import {
  badRequest,
  computeStorageMetrics,
  computeCompactionStatus,
  getDefaultBranchFromHead,
  loadAdminPackRefIndexState,
  loadHeadAndRefsCached,
  markRequestPrivate,
  notFound,
  requestCacheContext,
  type DebugState,
} from "./helpers";
import type { AppContext } from "../hono";
import { renderUiDocumentResponse } from "../uiResponse";

function adminForbidden(): Response {
  return new Response("Forbidden\n", {
    status: 403,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

export async function handleAdminPage(c: AppContext<"/:owner/:repo/admin">) {
  const env = c.env;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");

  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
  }

  const route = await resolveRepositoryRoute(env, owner, repo);
  if (!route) return await notFound(c);

  // Browser admin requires a tessera session + namespace membership. PATs
  // and AuthDO Basic credentials are deliberately never accepted here.
  const membership = await loadSessionMembership(c, route.namespaceId);
  if (membership.kind === "anonymous") {
    return c.redirect(`/auth?next=${encodeURIComponent(`/${owner}/${repo}/admin`)}`, 302);
  }
  if (membership.kind === "signed-in-non-member") {
    if (route.visibility === "private") return await notFound(c);
    return adminForbidden();
  }
  const viewer = membership.viewer;

  const cacheCtx = requestCacheContext(c);
  // Admin pages are member-only and may render private metadata; always
  // bypass shared cache for both reads and writes.
  markRequestPrivate(cacheCtx);
  const repoId = route.doName;
  const stub = getRepoStub(env, repoId);
  const limiter = getLimiter(cacheCtx);

  const [rawState, refsData, progress] = await Promise.all([
    limiter
      .run("do:admin-page-debug-state", () => stub.debugState())
      .catch(() => ({}) as Partial<DebugState>),
    loadHeadAndRefsCached(env, cacheCtx, repoId),
    getRepoActivity(env, repoId, cacheCtx),
  ]);
  const state = await loadAdminPackRefIndexState({
    env,
    repoId,
    state: rawState,
    cacheCtx,
  });
  const head: HeadInfo | undefined = refsData?.head || undefined;
  const refs: Ref[] = refsData?.refs || [];

  const { storageSize, packCount, packList, supersededPackCount } = computeStorageMetrics(state);
  const { compactionStatus, compactionStartedAt } = computeCompactionStatus(state.compaction);

  const defaultBranch = getDefaultBranchFromHead(head);
  const refEnc = encodeURIComponent(defaultBranch);

  return renderUiDocumentResponse(
    env,
    "admin",
    {
      title: `Admin · ${owner}/${repo}`,
      owner,
      repo,
      refEnc,
      head,
      refs,
      storageSize,
      packCount,
      packList,
      state,
      defaultBranch,
      compactionStatus,
      compactionStartedAt,
      compactionData: state.compaction,
      supersededPackCount,
      progress,
    },
    {
      cacheControl: "no-store",
      failureBody: "Failed to render view",
      viewer,
    }
  );
}
