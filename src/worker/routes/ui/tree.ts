import type { TreeEntry } from "@/worker/git";
import type { CacheContext } from "@/worker/cache";
import { readPath } from "@/worker/git";
import {
  isValidOwnerRepo,
  isValidRef,
  isValidPath,
  bytesToText,
  getFileIconName,
  getHighlightLangsForBlobSmart,
  type FileIconName,
} from "@/shared/web";
import { renderUiView } from "@/client/server/render";
import { handleError } from "@/client/server/error";
import { buildCacheKeyFrom, cacheOrLoadJSONWithTTL } from "@/worker/cache";
import { getRepoActivity } from "@/worker/common";
import { repoKey } from "@/worker/keys";
import { badRequest } from "./helpers";
import type { AppContext } from "../hono";
import { renderUiDocumentResponse } from "../uiResponse";
import type { ReadPathResult } from "@/worker/git";

export async function handleTree(c: AppContext<"/:owner/:repo/tree">) {
  const request = c.req.raw;
  const env = c.env;
  const ctx = c.executionCtx;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
  }
  const repoId = repoKey(owner, repo);
  const u = new URL(request.url);
  const ref = u.searchParams.get("ref") || "main";
  const path = u.searchParams.get("path") || "";
  if (!isValidRef(ref)) {
    return badRequest(env, "Invalid ref", "Ref format not allowed", {
      owner,
      repo,
      refEnc: encodeURIComponent(ref),
      path,
    });
  }
  if (path && !isValidPath(path)) {
    return badRequest(env, "Invalid path", "Path contains invalid characters or is too long", {
      owner,
      repo,
      refEnc: encodeURIComponent(ref),
      path,
    });
  }

  // Build cache key for tree content
  const cacheKeyTree = buildCacheKeyFrom(request, "/_cache/tree", {
    repo: repoId,
    ref,
    path,
  });

  const result = await cacheOrLoadJSONWithTTL<ReadPathResult | null>(
    cacheKeyTree,
    async () => {
      try {
        const cacheCtx: CacheContext = { req: request, ctx };
        return await readPath(env, repoId, ref, path, cacheCtx);
      } catch {
        return null;
      }
    },
    (value) => (value && value.type === "tree" ? 60 : 300),
    ctx
  );

  // Handle missing tree/blob result gracefully (e.g., non-existent repo or path)
  if (!result) {
    try {
      const errHtml = await renderUiView(env, "error", {
        title: `${owner}/${repo} · Tree`,
        message: "Not found",
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
      if (errHtml) {
        return new Response(errHtml, {
          status: 404,
          headers: { "Content-Type": "text/html; charset=utf-8" },
        });
      }
    } catch {}
    return new Response("Not found\n", { status: 404 });
  }

  try {
    if (result.type === "tree") {
      // Format tree entries as structured data
      let entries: Array<{
        name: string;
        href: string;
        isDir: boolean;
        iconName: FileIconName;
        shortOid: string;
        size: string;
      }> = [];
      if (result.type === "tree" && result.entries) {
        // Helper to determine if entry is a directory based on git mode
        const isDirectory = (mode: string) => mode.startsWith("40000");

        const sorted = result.entries.sort((a: TreeEntry, b: TreeEntry) => {
          const aIsDir = isDirectory(a.mode);
          const bIsDir = isDirectory(b.mode);
          if (aIsDir !== bIsDir) return aIsDir ? -1 : 1;
          return a.name.localeCompare(b.name);
        });
        entries = sorted.map((e: TreeEntry) => {
          const isDir = isDirectory(e.mode);
          return {
            name: e.name,
            href: isDir
              ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(
                  (path ? path + "/" : "") + e.name
                )}`
              : `/${owner}/${repo}/blob?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(
                  (path ? path + "/" : "") + e.name
                )}`,
            isDir,
            iconName: isDir ? "folder" : getFileIconName(e.name),
            shortOid: e.oid ? e.oid.slice(0, 7) : "",
            size: "", // Size not available in tree entries, would need separate lookup
          };
        });
      }
      // Generate breadcrumbs and parent link
      const parts = (path || "").split("/").filter(Boolean);
      // Truncate ref if too long (e.g., commit hashes)
      const refDisplay = ref.length > 20 ? ref.slice(0, 7) + "..." : ref;
      const breadcrumbs = [
        {
          name: refDisplay,
          href: parts.length > 0 ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}` : null,
        },
        ...parts.map((part, i) => {
          const subPath = parts.slice(0, i + 1).join("/");
          const isLast = i === parts.length - 1;
          return {
            name: part,
            href: isLast
              ? null
              : `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(subPath)}`,
          };
        }),
      ];
      const parentHref =
        parts.length > 0
          ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(parts.slice(0, -1).join("/"))}`
          : null;
      const progress = await getRepoActivity(env, repoId);
      return renderUiDocumentResponse(
        env,
        "tree",
        {
          title: `${path || "root"} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          progress,
          breadcrumbs,
          parentHref,
          entries,
        },
        {
          failureBody: "Failed to render view",
        }
      );
    } else {
      const raw = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}`;
      const text = bytesToText(result.content);
      const lineCount = text === "" ? 0 : text.split(/\r?\n/).length;
      const title = path || result.oid;
      // Infer language and load only what we need (use smart inference with content)
      const langs = getHighlightLangsForBlobSmart(title, text);
      const codeLang = langs[0] || null;
      const progress = await getRepoActivity(env, repoId);
      return renderUiDocumentResponse(
        env,
        "blob",
        {
          title: `${title} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          progress,
          fileName: title,
          viewRawHref: `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&view=1&name=${encodeURIComponent(title)}`,
          rawHref: raw,
          codeText: text,
          codeLang,
          lineCount,
        },
        {
          failureBody: "Failed to render view",
        }
      );
    }
  } catch (e) {
    return handleError(env, e, `${owner}/${repo} · Tree`, {
      owner,
      repo,
      refEnc: encodeURIComponent(ref),
      path,
    });
  }
}
