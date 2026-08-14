import { resolveRepositoryRoute } from "@/worker/repositories/route";
import { snapshotObjectKey } from "@/worker/git/snapshot/materialize";
import { countSubrequest } from "@/worker/git/operations/limits";
import { isValidOwnerRepo } from "@/shared/web";
import { authorizeInternalRequest } from "./internalAuth";
import type { AppContext, AppRouter } from "./hono";

async function handleSnapshot(c: AppContext, path?: string | null): Promise<Response> {
  const denied = await authorizeInternalRequest(c);
  if (denied) return denied;
  const owner = c.req.param("owner") ?? "";
  const repo = c.req.param("repo") ?? "";
  const commitSha = (c.req.param("commit") ?? "").toLowerCase();
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return new Response("Not found\n", { status: 404 });
  }
  const log = c.var.logFor({ service: "SnapshotRead" });
  const route = await resolveRepositoryRoute(c.env, owner, repo, {
    mode: "allow-d1-fallback",
    db: c.var.db,
    log,
  });
  if (!route) return new Response("Not found\n", { status: 404 });
  let key: string | null;
  try {
    if (path === null) return Response.json({ error: "Missing path" }, { status: 400 });
    key = snapshotObjectKey({ env: c.env, repositoryId: route.repositoryId, commitSha, path });
  } catch {
    return Response.json({ error: "Invalid path" }, { status: 400 });
  }
  if (!key) return new Response("Not found\n", { status: 404 });
  if (!countSubrequest(c.var.cacheCtx)) {
    log.warn("snapshot-read:soft-budget-exhausted", { repositoryId: route.repositoryId });
  }
  let object: R2ObjectBody | null;
  try {
    object = await c.var.limiter.run("r2:get-snapshot", () => c.env.REPO_BUCKET.get(key));
  } catch (error) {
    log.error("snapshot-read:failed", {
      repositoryId: route.repositoryId,
      commitSha,
      kind: path === undefined ? "manifest" : "file",
      error: String(error),
    });
    throw error;
  }
  if (!object) {
    log.info("snapshot-read:miss", {
      repositoryId: route.repositoryId,
      commitSha,
      kind: path === undefined ? "manifest" : "file",
    });
    return new Response("Not found\n", { status: 404 });
  }
  log.info("snapshot-read:served", {
    repositoryId: route.repositoryId,
    commitSha,
    kind: path === undefined ? "manifest" : "file",
    bytes: object.size,
  });
  return new Response(object.body, {
    headers: {
      "Content-Type": object.httpMetadata?.contentType ?? "application/octet-stream",
      "Cache-Control": "private, no-store",
      ...(object.customMetadata?.sha256 ? { ETag: `"${object.customMetadata.sha256}"` } : {}),
    },
  });
}

export function registerSnapshotRoutes(router: AppRouter): void {
  router.get("/_internal/snapshots/:owner/:repo/:commit/manifest", (c) => handleSnapshot(c));
  router.get("/_internal/snapshots/:owner/:repo/:commit/file", (c) =>
    handleSnapshot(c, c.req.query("path") ?? null)
  );
}
