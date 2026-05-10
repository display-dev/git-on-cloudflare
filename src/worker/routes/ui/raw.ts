import { readBlobStream, readPath } from "@/worker/git";
import { isValidRef, isValidPath, OID_RE, getContentTypeFromName } from "@/shared/web";
import { isRequestPrivate, resolveUiRepoAccess } from "./helpers";
import type { AppContext } from "../hono";

export async function handleRaw(c: AppContext<"/:owner/:repo/raw">) {
  const env = c.env;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  const access = await resolveUiRepoAccess(c, owner, repo);
  if (access.kind === "response") return access.response;
  const { route, cacheCtx } = access;
  const isPrivate = isRequestPrivate(cacheCtx);
  const url = new URL(c.req.url);
  const oid = url.searchParams.get("oid") || "";
  if (!OID_RE.test(oid)) return new Response("Bad Request\n", { status: 400 });
  const fileName = url.searchParams.get("name") || oid;
  const download = url.searchParams.get("download") === "1";

  if (!oid) return new Response("Missing oid\n", { status: 400 });

  // This route still avoids whole-pack buffering, but a packed blob may be
  // materialized before the response body is streamed to the client.
  const streamResponse = await readBlobStream(env, route.doName, oid, cacheCtx);
  if (!streamResponse) return new Response("Not found\n", { status: 404 });

  // Use text/plain for all files (like GitHub's raw view) to prevent
  // browsers from executing HTML/JS and ensure consistent display.
  const headers = new Headers(streamResponse.headers);
  headers.set("Content-Type", "text/plain; charset=utf-8");
  if (isPrivate) headers.set("Cache-Control", "no-store");

  if (download) {
    headers.set("Content-Disposition", `attachment; filename="${fileName}"`);
  } else {
    headers.set("Content-Disposition", `inline; filename="${fileName}"`);
  }

  return new Response(streamResponse.body, {
    status: streamResponse.status,
    headers,
  });
}

export async function handleRawPath(c: AppContext<"/:owner/:repo/rawpath">) {
  const env = c.env;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  const access = await resolveUiRepoAccess(c, owner, repo);
  if (access.kind === "response") return access.response;
  const { route, cacheCtx } = access;
  const isPrivate = isRequestPrivate(cacheCtx);
  const url = new URL(c.req.url);
  const ref = url.searchParams.get("ref") || "main";
  const path = url.searchParams.get("path") || "";
  const name = url.searchParams.get("name") || path.split("/").pop() || "file";
  const download = url.searchParams.get("download") === "1";
  if (!isValidRef(ref) || !isValidPath(path)) {
    return new Response("Bad Request\n", { status: 400 });
  }

  // Basic hotlink protection: require same-origin Referer
  try {
    const referer = c.req.raw.headers.get("referer") || "";
    const allowed = (() => {
      try {
        const r = new URL(referer);
        return r.host === url.host;
      } catch {
        return false;
      }
    })();
    if (!allowed) {
      return new Response("Hotlinking not allowed\n", { status: 403 });
    }
  } catch {}

  try {
    const repoId = route.doName;
    const result = await readPath(env, repoId, ref, path, cacheCtx);
    if (result.type !== "blob") return new Response("Not a blob\n", { status: 400 });
    const streamResponse = await readBlobStream(env, repoId, result.oid, cacheCtx);
    if (!streamResponse) return new Response("Not found\n", { status: 404 });

    const headers = new Headers(streamResponse.headers);
    headers.set("Content-Type", getContentTypeFromName(name));
    if (isPrivate) headers.set("Cache-Control", "no-store");
    if (download) headers.set("Content-Disposition", `attachment; filename="${name}"`);
    else headers.set("Content-Disposition", `inline; filename="${name}"`);
    return new Response(streamResponse.body, { status: streamResponse.status, headers });
  } catch {
    return new Response("Not found\n", { status: 404 });
  }
}
