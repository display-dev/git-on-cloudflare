import { resolveRepositoryRoute } from "@/worker/repositories/route";
import { snapshotBundleObjectKey, snapshotObjectKey } from "@/worker/git/snapshot/materialize";
import { countSubrequest } from "@/worker/git/operations/limits";
import { isValidOwnerRepo } from "@/shared/web";
import { authorizeInternalRequest } from "./internalAuth";
import type { AppContext, AppRouter } from "./hono";

function bundledFile(
  value: unknown,
  path: string
): { path: string; bytes: number; sha256: string; offset: number } | null {
  if (
    value === null ||
    typeof value !== "object" ||
    !("version" in value) ||
    value.version !== 1 ||
    !("files" in value) ||
    !Array.isArray(value.files) ||
    !("bundle" in value) ||
    value.bundle === null ||
    typeof value.bundle !== "object" ||
    !("bytes" in value.bundle) ||
    typeof value.bundle.bytes !== "number" ||
    !Number.isSafeInteger(value.bundle.bytes)
  ) {
    return null;
  }
  const bundleBytes = value.bundle.bytes;
  const file = value.files.find(
    (candidate) =>
      candidate !== null &&
      typeof candidate === "object" &&
      "path" in candidate &&
      candidate.path === path &&
      "bytes" in candidate &&
      typeof candidate.bytes === "number" &&
      Number.isSafeInteger(candidate.bytes) &&
      candidate.bytes >= 0 &&
      "offset" in candidate &&
      typeof candidate.offset === "number" &&
      Number.isSafeInteger(candidate.offset) &&
      candidate.offset >= 0 &&
      candidate.offset + candidate.bytes <= bundleBytes &&
      "sha256" in candidate &&
      typeof candidate.sha256 === "string" &&
      /^[0-9a-f]{64}$/.test(candidate.sha256)
  );
  if (!file) return null;
  return {
    path: file.path,
    bytes: file.bytes,
    sha256: file.sha256,
    offset: file.offset,
  };
}

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
    if (path !== undefined) {
      // The manifest selects the indexed bundle layout or the legacy per-file
      // fallback before the file payload is read.
      const manifestKey = snapshotObjectKey({
        env: c.env,
        repositoryId: route.repositoryId,
        commitSha,
      });
      const manifestObject = manifestKey
        ? await c.var.limiter.run("r2:get-snapshot-manifest", () =>
            c.env.REPO_BUCKET.get(manifestKey)
          )
        : null;
      const selected = manifestObject ? bundledFile(await manifestObject.json(), path) : null;
      if (selected) {
        // R2 rejects zero-length ranges, so an indexed empty file has no
        // corresponding range request or bundle-object dependency.
        if (selected.bytes === 0) {
          log.info("snapshot-read:served", {
            repositoryId: route.repositoryId,
            commitSha,
            kind: "bundle-range",
            bytes: 0,
          });
          return new Response(new Uint8Array(), {
            headers: {
              "Content-Type": "application/octet-stream",
              "Content-Length": "0",
              "Cache-Control": "private, no-store",
              ETag: `"${selected.sha256}"`,
            },
          });
        }
        const bundleKey = snapshotBundleObjectKey({
          env: c.env,
          repositoryId: route.repositoryId,
          commitSha,
        });
        if (!countSubrequest(c.var.cacheCtx)) {
          log.warn("snapshot-read:soft-budget-exhausted", { repositoryId: route.repositoryId });
        }
        object = bundleKey
          ? await c.var.limiter.run("r2:get-snapshot-bundle-range", () =>
              c.env.REPO_BUCKET.get(bundleKey, {
                range: { offset: selected.offset, length: selected.bytes },
              })
            )
          : null;
        if (!object) {
          log.info("snapshot-read:miss", {
            repositoryId: route.repositoryId,
            commitSha,
            kind: "bundle-range",
          });
          return new Response("Not found\n", { status: 404 });
        }
        log.info("snapshot-read:served", {
          repositoryId: route.repositoryId,
          commitSha,
          kind: "bundle-range",
          bytes: selected.bytes,
        });
        return new Response(object.body, {
          headers: {
            "Content-Type": "application/octet-stream",
            "Content-Length": String(selected.bytes),
            "Cache-Control": "private, no-store",
            ETag: `"${selected.sha256}"`,
          },
        });
      }
    }
    if (path !== undefined && !countSubrequest(c.var.cacheCtx)) {
      log.warn("snapshot-read:soft-budget-exhausted", { repositoryId: route.repositoryId });
    }
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
