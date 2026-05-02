import type { HeadInfo, Ref } from "@/worker/git";
import { isValidOwnerRepo } from "@/shared/web";
import { getRepoActivity, getRepoStub, unauthorizedAdminBasic } from "@/worker/common";
import { verifyAuth } from "@/worker/auth";
import { repoKey } from "@/worker/keys";
import {
  badRequest,
  computeStorageMetrics,
  computeCompactionStatus,
  getDefaultBranchFromHead,
  loadAdminPackRefIndexState,
  loadHeadAndRefsCached,
  type DebugState,
} from "./helpers";
import type { AppContext } from "../hono";
import { renderUiDocumentResponse } from "../uiResponse";

export async function handleAdminPage(c: AppContext<"/:owner/:repo/admin">) {
  const request = c.req.raw;
  const env = c.env;
  const ctx = c.executionCtx;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");

  // Validate parameters
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
  }

  // Check authentication - admin access required
  if (!(await verifyAuth(env, owner, request, true))) {
    return unauthorizedAdminBasic();
  }

  const repoId = repoKey(owner, repo);
  const stub = getRepoStub(env, repoId);
  const cacheCtx = { req: request, ctx };

  // Gather admin data in parallel for performance
  const [rawState, refsData, progress] = await Promise.all([
    stub.debugState().catch(() => ({}) as Partial<DebugState>),
    loadHeadAndRefsCached(env, request, ctx, repoId),
    getRepoActivity(env, repoId),
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
      failureBody: "Failed to render view",
    }
  );
}
