import type { CommitDiffResult, CommitFilePatchResult } from "@/shared/git/types";
import {
  listCommitsFirstParentRange,
  listMergeSideFirstParent,
  readCommitInfo,
  listCommitChangedFiles,
  readCommitFilePatch,
} from "@/worker/git";
import { isValidPath, formatWhen, OID_RE } from "@/shared/web";
import { handleError } from "@/client/server/error";
import { buildCacheKeyFrom, cacheOrLoadJSONForRequestWithTTL } from "@/worker/cache";
import { badRequest, isRequestPrivate, loadUiRepoActivity, resolveUiRepoAccess } from "./helpers";
import { isValidRef } from "@/shared/web";
import type { AppContext } from "../hono";
import { renderUiDocumentResponse } from "../uiResponse";

export async function handleCommits(c: AppContext<"/:owner/:repo/commits">) {
  const env = c.env;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  const access = await resolveUiRepoAccess(c, owner, repo);
  if (access.kind === "response") return access.response;
  const { route, cacheCtx } = access;
  const repoId = route.doName;
  const isPrivate = isRequestPrivate(cacheCtx);
  const u = new URL(c.req.url);
  const ref = u.searchParams.get("ref") || "main";
  if (!isValidRef(ref)) {
    return badRequest(env, "Invalid ref", "Ref format not allowed", {
      owner,
      repo,
      refEnc: encodeURIComponent(ref),
    });
  }
  const pageRaw = u.searchParams.get("page") || "";
  const perRaw = Number(u.searchParams.get("per_page") || "25");
  const perPage = Number.isFinite(perRaw) ? Math.max(5, Math.min(100, Math.floor(perRaw))) : 25;
  try {
    let page = Number(pageRaw);
    if (!Number.isFinite(page) || page < 0) page = 0;
    const offset = page * perPage;

    const loader = async () => {
      const commits = await listCommitsFirstParentRange(
        env,
        repoId,
        ref,
        offset,
        perPage,
        cacheCtx
      );
      return commits.map((c) => ({
        oid: c.oid,
        shortOid: c.oid.slice(0, 7),
        firstLine: (c.message || "").split(/\r?\n/, 1)[0],
        authorName: c.author?.name || "",
        when: c.author ? formatWhen(c.author.when, c.author.tz) : "",
        isMerge: Array.isArray(c.parents) && c.parents.length > 1,
      }));
    };
    const cacheKey = buildCacheKeyFrom(c.req.raw, "/_cache/commits", {
      repo: repoId,
      ref,
      per_page: String(perPage),
      page: String(page),
      offset: String(offset),
    });
    const commitsView = await cacheOrLoadJSONForRequestWithTTL<
      Array<{
        oid: string;
        shortOid: string;
        firstLine: string;
        authorName: string;
        when: string;
      }>
    >(cacheCtx, cacheKey, loader, () => {
      const isOid = OID_RE.test(ref);
      const isTag = ref.startsWith("refs/tags/");
      // Branch commits: 300s; Tags/OIDs (immutable): 3600s
      return isOid || isTag ? 3600 : 300;
    });
    const list = commitsView || [];
    const last = list[list.length - 1]?.oid || "";
    const refEnc = encodeURIComponent(ref);
    const pager = {
      perPageLinks: [10, 25, 50].map((n) => ({
        text: String(n),
        href: `/${owner}/${repo}/commits?ref=${refEnc}&page=${page}&per_page=${n}`,
      })),
      newerHref:
        page > 0
          ? `/${owner}/${repo}/commits?ref=${refEnc}&page=${page - 1}&per_page=${perPage}`
          : undefined,
      olderHref:
        last && list.length === perPage
          ? `/${owner}/${repo}/commits?ref=${refEnc}&page=${page + 1}&per_page=${perPage}`
          : undefined,
    };
    const progress = await loadUiRepoActivity(env, access);
    return renderUiDocumentResponse(
      env,
      "commits",
      {
        title: `Commits on ${ref} · ${owner}/${repo}`,
        owner,
        repo,
        ref,
        refEnc,
        commits: list,
        pager,
        progress,
      },
      {
        cacheControl: isPrivate ? "no-store" : undefined,
        failureBody: "Failed to render view",
        viewer: access.viewer,
      }
    );
  } catch (e) {
    return handleError(env, e, `Error · ${owner}/${repo}`, {
      owner,
      repo,
      refEnc: encodeURIComponent(ref),
    });
  }
}

export async function handleCommitFragments(c: AppContext<"/:owner/:repo/commits/fragments/:oid">) {
  const env = c.env;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  const oid = c.req.param("oid");
  const access = await resolveUiRepoAccess(c, owner, repo, { responseShape: "json" });
  if (access.kind === "response") return access.response;
  const { route, cacheCtx } = access;
  if (!OID_RE.test(oid)) {
    return badRequest(env, "Invalid OID", "OID must be 40 hex", {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
    });
  }
  const u = new URL(c.req.url);
  const limitRaw = Number(u.searchParams.get("limit") || "20");
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(100, Math.floor(limitRaw))) : 20;
  const repoId = route.doName;
  try {
    const side = await listMergeSideFirstParent(
      env,
      repoId,
      oid,
      limit,
      {
        scanLimit: Math.min(400, limit * 5),
        timeBudgetMs: 5000,
        mainlineProbe: 50,
      },
      cacheCtx
    );
    const commits = (side || []).map((c) => ({
      oid: c.oid,
      shortOid: c.oid.slice(0, 7),
      firstLine: (c.message || "").split(/\r?\n/, 1)[0],
      authorName: c.author?.name || "",
      when: c.author ? formatWhen(c.author.when, c.author.tz) : "",
    }));
    return new Response(JSON.stringify({ owner, repo, commits, compact: true, mergeOf: oid }), {
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "react-fragment-json",
      },
    });
  } catch (e) {
    return handleError(env, e, `Error · ${owner}/${repo}`, {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
    });
  }
}

export async function handleCommitDiff(c: AppContext<"/:owner/:repo/commit/:oid/diff">) {
  const env = c.env;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  const oid = c.req.param("oid");
  const access = await resolveUiRepoAccess(c, owner, repo, { responseShape: "json" });
  if (access.kind === "response") return access.response;
  const { route, cacheCtx } = access;
  if (!OID_RE.test(oid)) {
    return badRequest(env, "Invalid commit OID", "Commit id must be 40-hex", {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
    });
  }
  const url = new URL(c.req.url);
  const path = url.searchParams.get("path") || "";
  if (!path || !isValidPath(path)) {
    return badRequest(env, "Invalid path", "Path contains invalid characters or is too long", {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
      path,
    });
  }
  const repoId = route.doName;
  try {
    const loader = async () => await readCommitFilePatch(env, repoId, oid, path, cacheCtx);
    const patchCacheKey = buildCacheKeyFrom(c.req.raw, "/_cache/commit-patch", {
      repo: repoId,
      oid,
      path,
      v: "1",
    });
    const patch = await cacheOrLoadJSONForRequestWithTTL<CommitFilePatchResult>(
      cacheCtx,
      patchCacheKey,
      loader,
      () => 86400
    );
    return new Response(JSON.stringify(patch), {
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "react-fragment-json",
      },
    });
  } catch (e) {
    return handleError(env, e, `Error · ${owner}/${repo}`, {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
      path,
    });
  }
}

export async function handleCommit(c: AppContext<"/:owner/:repo/commit/:oid">) {
  const env = c.env;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  const oid = c.req.param("oid");
  const access = await resolveUiRepoAccess(c, owner, repo);
  if (access.kind === "response") return access.response;
  const { route, cacheCtx } = access;
  const isPrivate = isRequestPrivate(cacheCtx);
  if (!OID_RE.test(oid)) {
    return badRequest(env, "Invalid commit OID", "Commit id must be 40-hex", {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
    });
  }
  const repoId = route.doName;
  try {
    const commit = await readCommitInfo(env, repoId, oid, cacheCtx);
    const diffLoader = async () =>
      await listCommitChangedFiles(env, repoId, oid, cacheCtx, {
        timeBudgetMs: 5000,
      });
    const diffCacheKey = buildCacheKeyFrom(c.req.raw, "/_cache/commit-diff", {
      repo: repoId,
      oid,
      v: "1",
    });
    const diff = await cacheOrLoadJSONForRequestWithTTL<CommitDiffResult>(
      cacheCtx,
      diffCacheKey,
      diffLoader,
      () => 86400
    );
    const when = commit.author ? formatWhen(commit.author.when, commit.author.tz) : "";
    const parents = (commit.parents || []).map((p) => ({ oid: p, short: p.slice(0, 7) }));
    const progress = await loadUiRepoActivity(env, access);
    return renderUiDocumentResponse(
      env,
      "commit",
      {
        title: `${commit.oid.slice(0, 7)} · ${owner}/${repo}`,
        owner,
        repo,
        commitOid: commit.oid,
        refEnc: encodeURIComponent(commit.oid),
        progress,
        commitShort: commit.oid.slice(0, 7),
        authorName: commit.author?.name || "",
        authorEmail: commit.author?.email || "",
        when,
        parents,
        treeShort: (commit.tree || "").slice(0, 7),
        message: commit.message || "",
        diffBaseRefEnc: diff?.baseCommitOid ? encodeURIComponent(diff.baseCommitOid) : "",
        diffCompareMode: diff?.compareMode || "root",
        diffEntries: diff?.entries || [],
        diffSummary: {
          added: diff?.added || 0,
          modified: diff?.modified || 0,
          deleted: diff?.deleted || 0,
          total: diff?.total || 0,
        },
        diffTruncated: diff?.truncated || false,
        diffTruncateReason: diff?.truncateReason || "",
      },
      {
        cacheControl: isPrivate ? "no-store" : undefined,
        failureBody: "Failed to render view",
        viewer: access.viewer,
      }
    );
  } catch (e) {
    return handleError(env, e, `Error · ${owner}/${repo}`, {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
      path: "",
    });
  }
}
