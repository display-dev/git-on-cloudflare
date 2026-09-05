import type { CacheContext } from "@/worker/cache";
import { asBufferSource, bytesToHex, getRepoStub } from "@/worker/common";
import type { Logger } from "@/worker/common/logger";
import type { RepoDurableObject } from "@/worker/do";
import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type { Limiter } from "@/worker/git/operations/limits";
import type { BeginSnapshotMaterializationResult } from "@/worker/do/repo/repositoryLifecycle";
import { countSubrequest } from "@/worker/git/operations/limits";
import { readCommit } from "@/worker/git/operations/read/commits";
import { readBlob } from "@/worker/git/operations/read/objects";
import {
  isSymlinkMode,
  isTreeMode,
  joinTreePath,
  readTree,
} from "@/worker/git/operations/read/tree";

export const SNAPSHOT_MAX_FILES = 100;
export const SNAPSHOT_MAX_TOTAL_BYTES = 10 * 1024 * 1024;
export const SNAPSHOT_MAX_PATH_BYTES = 4096;
export const SNAPSHOT_MAX_PATH_SEGMENTS = 128;

export type SnapshotLimitReason =
  | "snapshot-file-count-limit"
  | "snapshot-total-bytes-limit"
  | "snapshot-path-bytes-limit"
  | "snapshot-path-segments-limit";

export type SnapshotLimitObservation = {
  fileCount: number;
  totalBytes: number;
  maxPathBytes: number;
  maxSegments: number;
};

export class SnapshotLimitError extends Error {
  public readonly reason: SnapshotLimitReason;
  public readonly observed: SnapshotLimitObservation;
  public readonly limits = {
    maxFiles: SNAPSHOT_MAX_FILES,
    maxBytes: SNAPSHOT_MAX_TOTAL_BYTES,
    maxPathBytes: SNAPSHOT_MAX_PATH_BYTES,
    maxSegments: SNAPSHOT_MAX_PATH_SEGMENTS,
  };

  public constructor(reason: SnapshotLimitReason, observed: SnapshotLimitObservation) {
    super("Snapshot exceeds benchmark limits");
    this.name = "SnapshotLimitError";
    this.reason = reason;
    this.observed = observed;
  }
}

export type SnapshotManifest = {
  version: 1;
  repositoryId: string;
  commitSha: string;
  treeSha: string;
  files: Array<{ path: string; bytes: number; sha256: string; offset?: number }>;
  bundle?: { bytes: number; sha256: string };
};

export type SnapshotMaterializationTarget =
  | Pick<AcceptedWriteFact, "repositoryId" | "afterSha" | "sourceSurface">
  | {
      repositoryId: string;
      afterSha: string;
      sourceSurface: "reconcile";
    };

function configuredPrefix(env: Env): string | null {
  const prefix = env.SNAPSHOT_BENCHMARK_PREFIX?.trim();
  if (!prefix) return null;
  if (!/^[a-z0-9][a-z0-9/_-]{0,127}$/i.test(prefix) || prefix.includes("..")) {
    throw new Error("Invalid snapshot benchmark prefix");
  }
  const normalized = prefix.replace(/\/$/, "");
  if (!normalized.split("/").at(-1)?.endsWith("snapshots")) {
    throw new Error("Snapshot prefix must use the reserved snapshots namespace");
  }
  return normalized;
}

function snapshotRoot(prefix: string, repositoryId: string, commitSha: string): string {
  return `${prefix}/${encodeURIComponent(repositoryId)}/${commitSha}`;
}

export function snapshotRepositoryPrefix(env: Env, repositoryId: string): string | null {
  const prefix = configuredPrefix(env);
  if (!prefix) return null;
  return `${prefix}/${encodeURIComponent(repositoryId)}/`;
}

export function isRepositorySnapshotPrefix(prefix: string, repositoryId: string): boolean {
  const repositorySuffix = `/${encodeURIComponent(repositoryId)}/`;
  if (!prefix.endsWith(repositorySuffix)) return false;
  const base = prefix.slice(0, -repositorySuffix.length);
  return (
    /^[a-z0-9][a-z0-9/_-]{0,127}$/i.test(base) &&
    !base.includes("..") &&
    Boolean(base.split("/").at(-1)?.endsWith("snapshots"))
  );
}

export function validateSnapshotPath(path: string): void {
  const segments = path.split("/");
  if (
    !path ||
    new TextEncoder().encode(path).byteLength > SNAPSHOT_MAX_PATH_BYTES ||
    segments.length > SNAPSHOT_MAX_PATH_SEGMENTS ||
    segments.some((segment) => !segment || segment === "." || segment === "..")
  ) {
    throw new Error("Snapshot contains an invalid path");
  }
}

async function digest(bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

function count(cacheCtx: CacheContext, log: Logger, op: string): void {
  if (!countSubrequest(cacheCtx)) log.warn("snapshot:soft-budget-exhausted", { op });
}

type SnapshotFile = { path: string; bytes: Uint8Array; sha256: string };
type RepoStub = DurableObjectStub<RepoDurableObject>;

type PreparedSnapshotBundle = {
  files: SnapshotManifest["files"];
  bundleBytes: Uint8Array;
  bundleSha256: string;
};

type SnapshotLease = {
  env: Env;
  fact: SnapshotMaterializationTarget;
  limiter: Limiter;
  log: Logger;
  cacheCtx: CacheContext;
  stub: RepoStub;
  token: string;
  root: string;
  finished: boolean;
};

async function prepareBundle(files: SnapshotFile[]): Promise<PreparedSnapshotBundle> {
  let offset = 0;
  const manifestFiles = files.map((file) => {
    const entry = {
      path: file.path,
      bytes: file.bytes.byteLength,
      sha256: file.sha256,
      offset,
    };
    offset += file.bytes.byteLength;
    return entry;
  });
  const bundleBytes = new Uint8Array(offset);
  for (const [index, file] of files.entries()) {
    bundleBytes.set(file.bytes, manifestFiles[index]!.offset);
  }
  return {
    files: manifestFiles,
    bundleBytes,
    bundleSha256: await digest(bundleBytes),
  };
}

async function beginSnapshotLease(args: {
  env: Env;
  repoId: string;
  fact: SnapshotMaterializationTarget;
  request: Request;
  ctx: ExecutionContext;
  limiter: Limiter;
  log: Logger;
}): Promise<SnapshotLease | null> {
  const prefix = configuredPrefix(args.env);
  if (!prefix) return null;
  const repositoryPrefix = snapshotRepositoryPrefix(args.env, args.fact.repositoryId);
  if (!repositoryPrefix) return null;
  const cacheCtx: CacheContext = {
    req: args.request,
    ctx: args.ctx,
    memo: { limiter: args.limiter, flags: new Set(["no-cache-read", "no-cache-write"]) },
  };
  const stub = getRepoStub(args.env, args.repoId);
  count(cacheCtx, args.log, "do:begin-snapshot-materialization");
  const lease = await args.limiter.run<BeginSnapshotMaterializationResult>(
    "do:begin-snapshot-materialization",
    () => stub.beginSnapshotMaterialization(repositoryPrefix)
  );
  if (!lease.ok) {
    args.log.info("snapshot:materialization-skipped", {
      repositoryId: args.fact.repositoryId,
      reason: lease.reason,
    });
    if (lease.reason === "maintenance-active") {
      throw new Error("Snapshot materialization is blocked by repository maintenance");
    }
    return null;
  }
  return {
    env: args.env,
    fact: args.fact,
    limiter: args.limiter,
    log: args.log,
    cacheCtx,
    stub,
    token: lease.token,
    root: snapshotRoot(prefix, args.fact.repositoryId, args.fact.afterSha),
    finished: false,
  };
}

async function renewSnapshotLease(lease: SnapshotLease): Promise<void> {
  count(lease.cacheCtx, lease.log, "do:renew-snapshot-materialization");
  const renewed = await lease.limiter.run("do:renew-snapshot-materialization", () =>
    lease.stub.renewSnapshotMaterialization(lease.token)
  );
  if (!renewed) throw new Error("Snapshot materialization lease is no longer active");
}

async function finishSnapshotLease(lease: SnapshotLease): Promise<void> {
  if (lease.finished) return;
  count(lease.cacheCtx, lease.log, "do:finish-snapshot-materialization");
  const finished = await lease.limiter.run("do:finish-snapshot-materialization", () =>
    lease.stub.finishSnapshotMaterialization(lease.token)
  );
  lease.finished = true;
  if (!finished) {
    lease.log.warn("snapshot:materialization-lease-missing", {
      repositoryId: lease.fact.repositoryId,
    });
  }
}

async function putSnapshotBundle(
  lease: SnapshotLease,
  prepared: PreparedSnapshotBundle
): Promise<void> {
  if (prepared.bundleBytes.byteLength === 0) return;
  await renewSnapshotLease(lease);
  count(lease.cacheCtx, lease.log, "r2:put-snapshot-bundle");
  try {
    await lease.limiter.run("r2:put-snapshot-bundle", () =>
      lease.env.REPO_BUCKET.put(`${lease.root}/bundle.bin`, prepared.bundleBytes, {
        httpMetadata: { contentType: "application/octet-stream" },
        customMetadata: { sha256: prepared.bundleSha256 },
      })
    );
  } catch (error) {
    lease.log.warn("snapshot:bundle-write-failed", {
      repositoryId: lease.fact.repositoryId,
      commitSha: lease.fact.afterSha,
      error: String(error),
    });
    throw error;
  }
}

async function putSnapshotManifest(
  lease: SnapshotLease,
  manifest: SnapshotManifest
): Promise<void> {
  await renewSnapshotLease(lease);
  count(lease.cacheCtx, lease.log, "r2:put-snapshot-manifest");
  await lease.limiter.run("r2:put-snapshot-manifest", () =>
    lease.env.REPO_BUCKET.put(`${lease.root}/manifest.json`, JSON.stringify(manifest), {
      httpMetadata: { contentType: "application/json" },
    })
  );
}

export type SnapshotInspection = {
  treeSha: string;
  files: Array<{ path: string; bytes: Uint8Array; sha256: string }>;
  totalBytes: number;
};

/** Validates and reads the bounded snapshot tree without writing derived bytes. */
export async function inspectSnapshotCommit(args: {
  env: Env;
  repoId: string;
  commitSha: string;
  cacheCtx: CacheContext;
  collectFiles?: boolean;
}): Promise<SnapshotInspection> {
  const commit = await readCommit(args.env, args.repoId, args.commitSha, args.cacheCtx);
  const files: SnapshotFile[] = [];
  let fileCount = 0;
  let totalBytes = 0;
  let maxPathBytes = 0;
  let maxSegments = 0;

  const visitTree = async (treeOid: string, basePath: string): Promise<void> => {
    const entries = await readTree(args.env, args.repoId, treeOid, args.cacheCtx);
    for (const entry of entries) {
      const path = joinTreePath(basePath, entry.name);
      const pathBytes = new TextEncoder().encode(path).byteLength;
      const pathSegments = path.split("/").length;
      maxPathBytes = Math.max(maxPathBytes, pathBytes);
      maxSegments = Math.max(maxSegments, pathSegments);
      const observed = (): SnapshotLimitObservation => ({
        fileCount: fileCount + 1,
        totalBytes,
        maxPathBytes,
        maxSegments,
      });
      if (pathBytes > SNAPSHOT_MAX_PATH_BYTES) {
        throw new SnapshotLimitError("snapshot-path-bytes-limit", observed());
      }
      if (pathSegments > SNAPSHOT_MAX_PATH_SEGMENTS) {
        throw new SnapshotLimitError("snapshot-path-segments-limit", observed());
      }
      validateSnapshotPath(path);
      if (isTreeMode(entry.mode)) {
        await visitTree(entry.oid, path);
        continue;
      }
      if (isSymlinkMode(entry.mode) || !entry.mode.startsWith("100")) {
        throw new Error("Snapshot contains an unsupported Git entry");
      }
      const blob = await readBlob(args.env, args.repoId, entry.oid, args.cacheCtx);
      if (blob.type !== "blob" || !blob.content) throw new Error("Snapshot blob is unavailable");
      totalBytes += blob.content.byteLength;
      if (fileCount >= SNAPSHOT_MAX_FILES) {
        throw new SnapshotLimitError("snapshot-file-count-limit", observed());
      }
      if (totalBytes > SNAPSHOT_MAX_TOTAL_BYTES) {
        throw new SnapshotLimitError("snapshot-total-bytes-limit", observed());
      }
      fileCount += 1;
      if (args.collectFiles !== false) {
        files.push({ path, bytes: blob.content, sha256: await digest(blob.content) });
      }
    }
  };

  await visitTree(commit.tree, "");
  return { treeSha: commit.tree, files, totalBytes };
}

export async function materializeAcceptedWrite(args: {
  env: Env;
  repoId: string;
  fact: SnapshotMaterializationTarget;
  request: Request;
  ctx: ExecutionContext;
  limiter: Limiter;
  log: Logger;
}): Promise<SnapshotManifest | null> {
  const lease = await beginSnapshotLease(args);
  if (!lease) return null;

  try {
    const inspected = await inspectSnapshotCommit({
      env: args.env,
      repoId: args.repoId,
      commitSha: args.fact.afterSha,
      cacheCtx: lease.cacheCtx,
    });
    const prepared = await prepareBundle(inspected.files);
    await putSnapshotBundle(lease, prepared);
    const manifest: SnapshotManifest = {
      version: 1,
      repositoryId: args.fact.repositoryId,
      commitSha: args.fact.afterSha,
      treeSha: inspected.treeSha,
      files: prepared.files,
      bundle: { bytes: prepared.bundleBytes.byteLength, sha256: prepared.bundleSha256 },
    };
    await putSnapshotManifest(lease, manifest);
    args.log.info("snapshot:materialized", {
      repositoryId: args.fact.repositoryId,
      commitSha: args.fact.afterSha,
      fileCount: manifest.files.length,
      totalBytes: inspected.totalBytes,
      sourceSurface: args.fact.sourceSurface,
    });
    return manifest;
  } finally {
    await finishSnapshotLease(lease);
  }
}

export function snapshotObjectKey(args: {
  env: Env;
  repositoryId: string;
  commitSha: string;
  path?: string;
}): string | null {
  const prefix = configuredPrefix(args.env);
  if (!prefix || !/^[0-9a-f]{40}$/.test(args.commitSha)) return null;
  const root = snapshotRoot(prefix, args.repositoryId, args.commitSha);
  if (args.path === undefined) return `${root}/manifest.json`;
  validateSnapshotPath(args.path);
  return `${root}/files/${args.path}`;
}

export function snapshotBundleObjectKey(args: {
  env: Env;
  repositoryId: string;
  commitSha: string;
}): string | null {
  const prefix = configuredPrefix(args.env);
  if (!prefix || !/^[0-9a-f]{40}$/.test(args.commitSha)) return null;
  return `${snapshotRoot(prefix, args.repositoryId, args.commitSha)}/bundle.bin`;
}
