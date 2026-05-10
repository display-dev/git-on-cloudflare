import type { HeadInfo, Ref } from "@/worker/git";
import { getRepoActivity, getRepoStub } from "@/worker/common";
import {
  computeStorageMetrics,
  computeCompactionStatus,
  getDefaultBranchFromHead,
  loadAdminPackRefIndexState,
  loadHeadAndRefsCached,
  resolveAdminPageRepoAccess,
  type DebugState,
} from "./helpers";
import type { AppContext } from "../hono";
import { renderUiDocumentResponse } from "../uiResponse";

export async function handleAdminPage(c: AppContext<"/:owner/:repo/admin">) {
  const env = c.env;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");

  const access = await resolveAdminPageRepoAccess(c, owner, repo);
  if (access.kind === "response") return access.response;
  const { route, cacheCtx, viewer, limiter } = access;
  const repoId = route.doName;
  const stub = getRepoStub(env, repoId);

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
