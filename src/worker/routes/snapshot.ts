import { resolveRepositoryRoute } from "@/worker/repositories/route";
import { asBodyInit, asBufferSource, bytesToHex, getRepoStub } from "@/worker/common";
import type { CacheContext } from "@/worker/cache";
import { markRequestPrivate } from "@/worker/cache/policy";
import type { Logger } from "@/worker/common/logger";
import type { ReleaseSnapshotPinResult, SnapshotResolution } from "@/worker/do/repo/acceptedWrites";
import type { BeginRepositoryReadResult } from "@/worker/do/repo/repositoryLifecycle";
import type { SnapshotPin } from "@/worker/do/repo/repoState";
import {
  snapshotBundleObjectKey,
  snapshotObjectKey,
  SNAPSHOT_MAX_FILES,
  SNAPSHOT_MAX_PATH_BYTES,
  SNAPSHOT_MAX_PATH_SEGMENTS,
  SNAPSHOT_MAX_TOTAL_BYTES,
  validateSnapshotPath,
} from "@/worker/git/snapshot/materialize";
import { countSubrequest } from "@/worker/git/operations/limits";
import { readCommit } from "@/worker/git/operations/read/commits";
import { readBlob } from "@/worker/git/operations/read/objects";
import {
  isSymlinkMode,
  isTreeMode,
  joinTreePath,
  readTree,
} from "@/worker/git/operations/read/tree";
import { isValidOwnerRepo } from "@/shared/web";
import { authorizeInternalRequest } from "./internalAuth";
import type { AppContext, AppRouter } from "./hono";

let snapshotReadLeaseObserver: ((phase: "begin" | "finish") => void) | undefined;

class SnapshotPathNotFoundError extends Error {}
class SnapshotUnserveableError extends Error {}

export type PinnedSnapshotManifest = {
  version: 2;
  repositoryId: string;
  commitSha: string;
  treeSha: string;
  files: Array<{ path: string; bytes: number; sha256: string }>;
};

export const __test = {
  setSnapshotReadLeaseObserver(observer: ((phase: "begin" | "finish") => void) | undefined): void {
    snapshotReadLeaseObserver = observer;
  },
};

async function sha256(bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

function snapshotCacheContext(c: AppContext, repoId: string): CacheContext {
  markRequestPrivate(c.var.cacheCtx);
  c.var.cacheCtx.memo!.repoId = repoId;
  return c.var.cacheCtx;
}

type SnapshotReadLeaseResult<T> =
  | { ok: true; value: T }
  | { ok: false; reason: "repository-deleting" | "reader-capacity" };

function snapshotUnavailable(
  reason: "repository-deleting" | "reader-capacity" | "read-failed"
): Response {
  return new Response(`Snapshot temporarily unavailable: ${reason}\n`, {
    status: 503,
    headers: { "Cache-Control": "no-store", "Retry-After": "1" },
  });
}

function snapshotLeaseUnavailable(reason: "repository-deleting" | "reader-capacity"): Response {
  if (reason === "repository-deleting") {
    return new Response("Repository is being deleted\n", {
      status: 410,
      headers: { "Cache-Control": "no-store" },
    });
  }
  return snapshotUnavailable(reason);
}

function snapshotUnserveable(error: SnapshotUnserveableError): Response {
  return Response.json(
    { error: "Snapshot cannot be served", reason: error.message },
    { status: 422, headers: { "Cache-Control": "no-store" } }
  );
}

async function withSnapshotReadLease<T>(
  c: AppContext,
  repoId: string,
  log: Logger,
  read: () => Promise<T>
): Promise<SnapshotReadLeaseResult<T>> {
  const stub = getRepoStub(c.env, repoId);
  if (!countSubrequest(c.var.cacheCtx)) log.warn("snapshot-read:soft-budget-exhausted");
  const lease = await c.var.limiter.run<BeginRepositoryReadResult>(
    "do:begin-repository-read",
    async () => await stub.beginRepositoryRead("snapshot-read")
  );
  if (!lease.ok) return lease;
  snapshotReadLeaseObserver?.("begin");
  try {
    return { ok: true, value: await read() };
  } finally {
    if (!countSubrequest(c.var.cacheCtx)) log.warn("snapshot-read:soft-budget-exhausted");
    await c.var.limiter
      .run("do:finish-repository-read", () => stub.finishRepositoryRead(lease.token))
      .catch((error) => log.warn("snapshot-read:lease-release-failed", { error: String(error) }));
    snapshotReadLeaseObserver?.("finish");
  }
}

async function resolvePinnedBlob(args: {
  c: AppContext;
  repoId: string;
  pin: SnapshotPin;
  path: string;
  cacheCtx: CacheContext;
}): Promise<Uint8Array> {
  const commit = await readCommit(args.c.env, args.repoId, args.pin.commitSha, args.cacheCtx);
  if (commit.tree !== args.pin.treeSha) {
    throw new SnapshotUnserveableError("Snapshot pin tree does not match commit");
  }
  let treeSha = commit.tree;
  const segments = args.path.split("/");
  for (const [index, segment] of segments.entries()) {
    const entries = await readTree(args.c.env, args.repoId, treeSha, args.cacheCtx);
    const entry = entries.find((candidate) => candidate.name === segment);
    if (!entry) throw new SnapshotPathNotFoundError("Snapshot path is not a blob");
    if (index < segments.length - 1) {
      if (!isTreeMode(entry.mode)) {
        throw new SnapshotPathNotFoundError("Snapshot path is not a blob");
      }
      treeSha = entry.oid;
      continue;
    }
    if (isTreeMode(entry.mode)) {
      throw new SnapshotPathNotFoundError("Snapshot path is not a blob");
    }
    if (isSymlinkMode(entry.mode) || !entry.mode.startsWith("100")) {
      throw new SnapshotUnserveableError("Snapshot contains an unsupported Git entry");
    }
    const blob = await readBlob(args.c.env, args.repoId, entry.oid, args.cacheCtx);
    if (blob.type !== "blob" || !blob.content) throw new Error("Snapshot blob is unavailable");
    return blob.content;
  }
  throw new SnapshotPathNotFoundError("Snapshot path is not a blob");
}

async function manifestFromPin(
  c: AppContext,
  repoId: string,
  repositoryId: string,
  pin: SnapshotPin,
  cacheCtx: CacheContext
): Promise<PinnedSnapshotManifest> {
  const commit = await readCommit(c.env, repoId, pin.commitSha, cacheCtx);
  if (commit.tree !== pin.treeSha) {
    throw new SnapshotUnserveableError("Snapshot pin tree does not match commit");
  }
  const files: Array<{ path: string; bytes: number; sha256: string }> = [];
  let totalBytes = 0;
  const visit = async (treeSha: string, basePath: string): Promise<void> => {
    const entries = await readTree(c.env, repoId, treeSha, cacheCtx);
    for (const entry of entries) {
      const path = joinTreePath(basePath, entry.name);
      const pathBytes = new TextEncoder().encode(path).byteLength;
      const pathSegments = path.split("/").length;
      if (pathBytes > SNAPSHOT_MAX_PATH_BYTES || pathSegments > SNAPSHOT_MAX_PATH_SEGMENTS) {
        throw new SnapshotUnserveableError("Snapshot path limit exceeded");
      }
      try {
        validateSnapshotPath(path);
      } catch {
        throw new SnapshotUnserveableError("Snapshot contains an invalid path");
      }
      if (isTreeMode(entry.mode)) {
        await visit(entry.oid, path);
        continue;
      }
      if (isSymlinkMode(entry.mode) || !entry.mode.startsWith("100")) {
        throw new SnapshotUnserveableError("Snapshot contains an unsupported Git entry");
      }
      if (files.length >= SNAPSHOT_MAX_FILES) {
        throw new SnapshotUnserveableError("Snapshot file limit exceeded");
      }
      const blob = await readBlob(c.env, repoId, entry.oid, cacheCtx);
      if (blob.type !== "blob" || !blob.content) throw new Error("Snapshot blob is unavailable");
      totalBytes += blob.content.byteLength;
      if (totalBytes > SNAPSHOT_MAX_TOTAL_BYTES) {
        throw new SnapshotUnserveableError("Snapshot byte limit exceeded");
      }
      files.push({ path, bytes: blob.content.byteLength, sha256: await sha256(blob.content) });
    }
  };
  await visit(commit.tree, "");
  return {
    version: 2 as const,
    repositoryId,
    commitSha: pin.commitSha,
    treeSha: pin.treeSha,
    files,
  };
}

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
  if (!/^[0-9a-f]{40}$/.test(commitSha)) {
    return Response.json({ error: "Invalid commit" }, { status: 400 });
  }
  const requestStartedAt = performance.now();
  if (path !== undefined) {
    try {
      if (path === null) return Response.json({ error: "Missing path" }, { status: 400 });
      try {
        validateSnapshotPath(path);
      } catch {
        return Response.json({ error: "Invalid path" }, { status: 400 });
      }
      const cacheCtx = snapshotCacheContext(c, route.doName);
      if (!countSubrequest(cacheCtx)) {
        log.warn("snapshot-read:soft-budget-exhausted", { repositoryId: route.repositoryId });
      }
      const leased = await withSnapshotReadLease(c, route.doName, log, async () => {
        const pinStartedAt = performance.now();
        const resolution = await c.var.limiter.run<SnapshotResolution>(
          "do:get-snapshot-resolution",
          async () => await getRepoStub(c.env, route.doName).getSnapshotResolution(commitSha)
        );
        const pinRpcMs = performance.now() - pinStartedAt;
        if (resolution.status === "released") {
          return new Response("Not found\n", { status: 404 });
        }
        if (resolution.status !== "pinned") return null;
        const packReadStartedAt = performance.now();
        const bytes = await resolvePinnedBlob({
          c,
          repoId: route.doName,
          pin: resolution.pin,
          path,
          cacheCtx,
        });
        const packReadMs = performance.now() - packReadStartedAt;
        const digest = await sha256(bytes);
        log.info("snapshot-read:served", {
          repositoryId: route.repositoryId,
          commitSha,
          kind: "git-pack",
          bytes: bytes.byteLength,
          pinRpcMs,
          packReadMs,
          firstServeMs: performance.now() - requestStartedAt,
        });
        return new Response(asBodyInit(bytes), {
          headers: {
            "Content-Type": "application/octet-stream",
            "Content-Length": String(bytes.byteLength),
            "Cache-Control": "private, no-store",
            ETag: `"${digest}"`,
          },
        });
      });
      if (!leased.ok) return snapshotLeaseUnavailable(leased.reason);
      if (leased.value) return leased.value;
    } catch (error) {
      if (error instanceof SnapshotPathNotFoundError) {
        log.info("snapshot-read:path-miss", {
          repositoryId: route.repositoryId,
          commitSha,
          path,
        });
        return new Response("Not found\n", { status: 404 });
      }
      log.warn("snapshot-read:pack-resolution-failed", {
        repositoryId: route.repositoryId,
        commitSha,
        error: String(error),
      });
      if (error instanceof SnapshotUnserveableError) return snapshotUnserveable(error);
      return snapshotUnavailable("read-failed");
    }
  } else {
    const cacheCtx = snapshotCacheContext(c, route.doName);
    if (!countSubrequest(cacheCtx)) {
      log.warn("snapshot-read:soft-budget-exhausted", { repositoryId: route.repositoryId });
    }
    try {
      const leased = await withSnapshotReadLease(c, route.doName, log, async () => {
        const resolution = await c.var.limiter.run<SnapshotResolution>(
          "do:get-snapshot-resolution",
          async () => await getRepoStub(c.env, route.doName).getSnapshotResolution(commitSha)
        );
        if (resolution.status === "released") return new Response("Not found\n", { status: 404 });
        if (resolution.status !== "pinned") return null;
        const manifest = await manifestFromPin(
          c,
          route.doName,
          route.repositoryId,
          resolution.pin,
          cacheCtx
        );
        log.info("snapshot-read:served", {
          repositoryId: route.repositoryId,
          commitSha,
          kind: "git-pack-manifest",
          fileCount: manifest.files.length,
          bytes: manifest.files.reduce((sum, file) => sum + file.bytes, 0),
          firstServeMs: performance.now() - requestStartedAt,
        });
        return Response.json(manifest, { headers: { "Cache-Control": "private, no-store" } });
      });
      if (!leased.ok) return snapshotLeaseUnavailable(leased.reason);
      if (leased.value) return leased.value;
    } catch (error) {
      log.warn("snapshot-read:pack-resolution-failed", {
        repositoryId: route.repositoryId,
        commitSha,
        error: String(error),
      });
      if (error instanceof SnapshotUnserveableError) return snapshotUnserveable(error);
      return snapshotUnavailable("read-failed");
    }
  }
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

async function handleSnapshotRelease(c: AppContext): Promise<Response> {
  const authResult = await authorizeInternalRequest(c);
  if (authResult) return authResult;
  const owner = c.req.param("owner") ?? "";
  const repo = c.req.param("repo") ?? "";
  const commitSha = (c.req.param("commit") ?? "").toLowerCase();
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo) || !/^[0-9a-f]{40}$/.test(commitSha)) {
    return new Response("Not found\n", { status: 404 });
  }
  const log = c.var.logFor({ service: "SnapshotRelease" });
  const route = await resolveRepositoryRoute(c.env, owner, repo, {
    mode: "allow-d1-fallback",
    db: c.var.db,
    log,
  });
  if (!route) return new Response("Not found\n", { status: 404 });
  let manifestKey: string | null = null;
  let bundleKey: string | null = null;
  try {
    manifestKey = snapshotObjectKey({
      env: c.env,
      repositoryId: route.repositoryId,
      commitSha,
    });
    bundleKey = snapshotBundleObjectKey({
      env: c.env,
      repositoryId: route.repositoryId,
      commitSha,
    });
  } catch (error) {
    log.error("snapshot-release:invalid-configuration", {
      repositoryId: route.repositoryId,
      commitSha,
      error: String(error),
    });
  }
  const legacyKeys = [manifestKey, bundleKey].filter((key): key is string => key !== null);
  let legacySnapshotExists = false;
  if (manifestKey) {
    if (!countSubrequest(c.var.cacheCtx)) {
      log.warn("snapshot-release:soft-budget-exhausted", { repositoryId: route.repositoryId });
    }
    const manifest = await c.var.limiter.run("r2:get-released-snapshot-manifest", () =>
      c.env.REPO_BUCKET.get(manifestKey)
    );
    legacySnapshotExists = Boolean(manifest);
    if (manifest && manifest.size <= 1024 * 1024) {
      try {
        const value = (await manifest.json()) as { files?: Array<{ path?: unknown }> };
        for (const file of value.files ?? []) {
          if (typeof file.path !== "string") continue;
          try {
            const key = snapshotObjectKey({
              env: c.env,
              repositoryId: route.repositoryId,
              commitSha,
              path: file.path,
            });
            if (key) legacyKeys.push(key);
          } catch {
            // A malformed legacy manifest cannot widen the deletion prefix.
          }
        }
      } catch (error) {
        log.warn("snapshot-release:legacy-manifest-unreadable", {
          repositoryId: route.repositoryId,
          commitSha,
          error: String(error),
        });
      }
    } else {
      await manifest?.body.cancel();
    }
  }
  if (!countSubrequest(c.var.cacheCtx)) {
    log.warn("snapshot-release:soft-budget-exhausted", { repositoryId: route.repositoryId });
  }
  const released = await c.var.limiter.run<ReleaseSnapshotPinResult>(
    "do:release-snapshot-pin",
    () =>
      getRepoStub(c.env, route.doName).releaseSnapshotPin(commitSha, {
        legacySnapshotExists,
        qualificationOwned: c.env.QUALIFICATION_MODE === "1",
      })
  );
  if (!released.released) {
    log.info("snapshot-release:refused", {
      repositoryId: route.repositoryId,
      commitSha,
      legacySnapshotExists,
      reason: released.reason,
    });
    return Response.json(
      {
        error:
          released.reason === "repository-deleting"
            ? "Repository is being deleted"
            : "Snapshot is still referenced",
        reason: released.reason,
      },
      {
        status: released.reason === "repository-deleting" ? 410 : 409,
        headers: { "Cache-Control": "no-store" },
      }
    );
  }
  if (legacyKeys.length > 0) {
    if (!countSubrequest(c.var.cacheCtx)) {
      log.warn("snapshot-release:soft-budget-exhausted", { repositoryId: route.repositoryId });
    }
    await c.var.limiter.run("r2:delete-released-snapshot", () =>
      c.env.REPO_BUCKET.delete(Array.from(new Set(legacyKeys)))
    );
  }
  log.info("snapshot-release:completed", {
    repositoryId: route.repositoryId,
    commitSha,
    legacySnapshotExists,
    deletedKeyCount: new Set(legacyKeys).size,
  });
  return new Response(null, { status: 204, headers: { "Cache-Control": "no-store" } });
}

export function registerSnapshotRoutes(router: AppRouter): void {
  router.get("/_internal/snapshots/:owner/:repo/:commit/manifest", (c) => handleSnapshot(c));
  router.get("/_internal/snapshots/:owner/:repo/:commit/file", (c) =>
    handleSnapshot(c, c.req.query("path") ?? null)
  );
  router.delete("/_internal/snapshots/:owner/:repo/:commit", handleSnapshotRelease);
}
