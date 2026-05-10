import { readPath } from "@/worker/git";
import {
  isValidRef,
  isValidPath,
  formatSize,
  detectBinary,
  bytesToText,
  getHighlightLangsForBlobSmart,
} from "@/shared/web";
import { handleError } from "@/client/server/error";
import { badRequest, isRequestPrivate, resolveUiRepoAccess } from "./helpers";
import type { AppContext } from "../hono";
import { renderUiDocumentResponse } from "../uiResponse";

export async function handleBlob(c: AppContext<"/:owner/:repo/blob">) {
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
  try {
    const result = await readPath(env, repoId, ref, path, cacheCtx);
    if (result.type !== "blob") return new Response("Not a blob\n", { status: 400 });
    const fileName = path || result.oid;
    const viewer = access.viewer;

    // Generate breadcrumbs and parent link (same pattern as tree.ts)
    const parts = (path || "").split("/").filter(Boolean);
    const refDisplay = ref.length > 20 ? ref.slice(0, 7) + "..." : ref;
    const breadcrumbs = [
      {
        name: refDisplay,
        href: parts.length > 0 ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}` : null,
      },
      ...parts.map((part: string, i: number) => {
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
      parts.length > 1
        ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(parts.slice(0, -1).join("/"))}`
        : parts.length === 1
          ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}`
          : null;

    // Too large to render inline
    if (result.tooLarge) {
      const sizeStr = formatSize(result.size || 0);
      const viewRawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&view=1&name=${encodeURIComponent(fileName)}`;
      const rawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&download=1&name=${encodeURIComponent(fileName)}`;
      return renderUiDocumentResponse(
        env,
        "blob",
        {
          title: `${fileName} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          fileName,
          tooLarge: true,
          sizeStr,
          viewRawHref,
          rawHref,
          breadcrumbs,
          parentHref,
        },
        {
          cacheControl: isPrivate ? "no-store" : undefined,
          failureBody: "Failed to render view",
          viewer,
        }
      );
    }

    // Binary vs text
    const isBinary = detectBinary(result.content);
    const size = result.content.byteLength;
    const viewRawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&view=1&name=${encodeURIComponent(fileName)}`;
    const rawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&download=1&name=${encodeURIComponent(fileName)}`;
    const templateData: Record<string, unknown> = {
      title: `${fileName} · ${owner}/${repo}`,
      owner,
      repo,
      refEnc: encodeURIComponent(ref),
      fileName,
      viewRawHref,
      breadcrumbs,
      parentHref,
      rawHref,
    };

    if (isBinary) {
      const ext = (fileName.split(".").pop() || "").toLowerCase();
      const isImage = ["png", "jpg", "jpeg", "gif", "webp", "bmp", "ico", "svg"].includes(ext);
      const isPdf = ext === "pdf";
      if ((isImage || isPdf) && path) {
        const name = encodeURIComponent(fileName);
        const mediaSrc = `/${owner}/${repo}/rawpath?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(path)}&name=${name}`;
        templateData.isImage = isImage;
        templateData.isPdf = isPdf;
        templateData.mediaSrc = mediaSrc;
        templateData.sizeStr = formatSize(size);
      } else {
        templateData.isBinary = true;
        templateData.sizeStr = formatSize(size);
      }
    } else {
      const text = bytesToText(result.content);
      const lineCount = text === "" ? 0 : text.split(/\r?\n/).length;
      const isMd =
        fileName.toLowerCase().endsWith(".md") || fileName.toLowerCase().endsWith(".markdown");
      if (isMd) {
        const baseDir = (path || "").split("/").filter(Boolean).slice(0, -1).join("/");
        templateData.isMarkdown = true;
        templateData.markdownRaw = text;
        templateData.lineCount = lineCount;
        templateData.mdOwner = owner;
        templateData.mdRepo = repo;
        templateData.mdRef = ref;
        templateData.mdBase = baseDir;
      } else {
        const langs = getHighlightLangsForBlobSmart(fileName, text);
        const codeLang = langs[0] || null;
        templateData.codeText = text;
        templateData.codeLang = codeLang;
        templateData.lineCount = lineCount;
        if (!codeLang) {
          templateData.sizeStr = formatSize(size);
        }
      }
    }

    return renderUiDocumentResponse(env, "blob", templateData, {
      cacheControl: isPrivate ? "no-store" : undefined,
      failureBody: "Failed to render view",
      viewer,
    });
  } catch (e) {
    return handleError(env, e, `Error · ${owner}/${repo}`, {
      owner,
      repo,
      refEnc: encodeURIComponent(ref),
      path,
    });
  }
}
