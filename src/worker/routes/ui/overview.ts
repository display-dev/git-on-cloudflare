import type { HeadInfo, Ref } from "@/worker/git";
import { readPath } from "@/worker/git";
import { classifyRef, formatRefOption, shortRefName } from "@/shared/git/ref-display";
import { isValidOwnerRepo, bytesToText } from "@/shared/web";
import { buildCacheKeyFrom, cacheOrLoadJSONForRequest } from "@/worker/cache";
import { findNamespaceBySlug } from "@/worker/db/d1/dal/namespaces";
import { listRepositoriesForNamespace } from "@/worker/db/d1/dal/repositories";
import { loadViewer } from "@/worker/auth/session";
import {
  badRequest,
  loadHeadAndRefsCached,
  loadUiRepoActivity,
  notFound,
  resolveUiRepoAccess,
} from "./helpers";
import type { AppContext } from "../hono";
import { renderUiDocumentResponse } from "../uiResponse";

export async function handleOwnerOverview(c: AppContext<"/:owner">) {
  const env = c.env;
  const owner = c.req.param("owner");
  if (!isValidOwnerRepo(owner)) {
    return badRequest(env, "Invalid owner", "Owner contains invalid characters or length");
  }
  const db = c.var.db;
  const namespace = await findNamespaceBySlug(db, owner);
  if (!namespace) {
    // Namespace rows are the owner-listing authority.
    return await notFound(c);
  }
  const viewer = await loadViewer(c);
  const repos = await listRepositoriesForNamespace(db, namespace.id, viewer?.userId ?? null);
  const includesPrivate = repos.some((row) => row.visibility === "private");
  return renderUiDocumentResponse(
    env,
    "owner",
    {
      title: `${owner} · Repositories`,
      owner,
      repos: repos.map((row) => ({
        slug: row.slug,
        visibility: row.visibility,
      })),
    },
    {
      // Public-only listings cache briefly per-colo. As soon as a private
      // row enters the response, we must not cache (membership-derived).
      cacheControl: includesPrivate ? "no-store" : "public, max-age=60",
      failureBody: "Failed to render view",
      viewer,
    }
  );
}

export async function handleRepoOverview(c: AppContext<"/:owner/:repo">) {
  const env = c.env;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  const access = await resolveUiRepoAccess(c, owner, repo);
  if (access.kind === "response") return access.response;
  const { route, cacheCtx, viewer } = access;
  const repoId = route.doName;

  const refsData = await loadHeadAndRefsCached(env, cacheCtx, repoId);
  const head: HeadInfo | undefined = refsData?.head;
  const refs: Ref[] = refsData?.refs || [];

  const defaultRef = head?.target || (refs[0]?.name ?? "refs/heads/main");
  const refShort = shortRefName(defaultRef);
  const refEnc = encodeURIComponent(refShort);
  const branchesData = refs
    .filter((ref) => classifyRef(ref.name) === "branch")
    .map(formatRefOption);
  const tagsData = refs.filter((ref) => classifyRef(ref.name) === "tag").map(formatRefOption);

  const readReadme = async (): Promise<{ md: string } | null> => {
    try {
      const candidates = ["README.md", "README.MD", "Readme.md", "README", "readme.md"];
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
  };

  const cacheKeyReadme = buildCacheKeyFrom(c.req.raw, "/_cache/readme", {
    repo: repoId,
    ref: refShort,
  });
  const readmeData = await cacheOrLoadJSONForRequest<{ md: string }>(
    cacheCtx,
    cacheKeyReadme,
    readReadme,
    300
  );
  const readmeMd = readmeData?.md || "";
  const progress = await loadUiRepoActivity(env, access);

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
      cacheControl: route.visibility === "private" ? "no-store" : undefined,
      failureBody: "Failed to render view",
      viewer,
    }
  );
}
