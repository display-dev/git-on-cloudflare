import type { HeadInfo, Ref } from "@/worker/git";
import type { CacheContext } from "@/worker/cache";
import { readPath } from "@/worker/git";
import { classifyRef, formatRefOption, shortRefName } from "@/shared/git/ref-display";
import { isValidOwnerRepo, bytesToText } from "@/shared/web";
import { listReposForOwner } from "@/worker/registry";
import { buildCacheKeyFrom, cacheOrLoadJSON } from "@/worker/cache";
import { getRepoActivity } from "@/worker/common";
import { repoKey } from "@/worker/keys";
import { loadViewer } from "@/worker/auth/session";
import { badRequest, loadHeadAndRefsCached } from "./helpers";
import type { AppContext } from "../hono";
import { renderUiDocumentResponse } from "../uiResponse";

export async function handleOwnerOverview(c: AppContext<"/:owner">) {
  const env = c.env;
  const owner = c.req.param("owner");
  if (!isValidOwnerRepo(owner)) {
    return badRequest(env, "Invalid owner", "Owner contains invalid characters or length");
  }
  const repos = await listReposForOwner(env, owner);
  const viewer = await loadViewer(c);
  return renderUiDocumentResponse(
    env,
    "owner",
    {
      title: `${owner} · Repositories`,
      owner,
      repos,
    },
    {
      cacheControl: "public, max-age=60",
      failureBody: "Failed to render view",
      viewer,
    }
  );
}

export async function handleRepoOverview(c: AppContext<"/:owner/:repo">) {
  const request = c.req.raw;
  const env = c.env;
  const ctx = c.executionCtx;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
  }
  const repoId = repoKey(owner, repo);

  const refsData = await loadHeadAndRefsCached(env, request, ctx, repoId);
  const head: HeadInfo | undefined = refsData?.head;
  const refs: Ref[] = refsData?.refs || [];

  const defaultRef = head?.target || (refs[0]?.name ?? "refs/heads/main");
  const refShort = shortRefName(defaultRef);
  const refEnc = encodeURIComponent(refShort);
  const branchesData = refs
    .filter((ref) => classifyRef(ref.name) === "branch")
    .map(formatRefOption);
  const tagsData = refs.filter((ref) => classifyRef(ref.name) === "tag").map(formatRefOption);

  // Try to load README at repo root on default branch with caching (5 minutes)
  const cacheKeyReadme = buildCacheKeyFrom(request, "/_cache/readme", {
    repo: repoId,
    ref: refShort,
  });
  const readmeData = await cacheOrLoadJSON<{ md: string }>(
    cacheKeyReadme,
    async () => {
      try {
        // Load all candidates in parallel for better performance
        const candidates = ["README.md", "README.MD", "Readme.md", "README", "readme.md"];
        const cacheCtx: CacheContext = { req: request, ctx };
        const results = await Promise.all(
          candidates.map(async (name) => {
            try {
              const res = await readPath(env, repoId, refShort, name, cacheCtx);
              if (res.type === "blob") {
                return { name, content: res.content };
              }
            } catch {}
            return null;
          })
        );
        const found = results.find((r) => r !== null) as {
          name: string;
          content: Uint8Array;
        } | null;
        if (!found) return null;
        const text = bytesToText(found.content);
        return { md: text };
      } catch {
        return null;
      }
    },
    300,
    ctx
  );
  const readmeMd = readmeData?.md || "";
  const progress = await getRepoActivity(env, repoId);
  const viewer = await loadViewer(c);

  return renderUiDocumentResponse(
    env,
    "overview",
    {
      title: `${owner}/${repo}`,
      owner,
      repo,
      refShort,
      refEnc,
      branches: branchesData,
      tags: tagsData,
      readmeMd,
      progress,
    },
    {
      failureBody: "Failed to render view",
      viewer,
    }
  );
}
