import type { CacheContext } from "@/worker/cache";
import { asBufferSource, bytesToHex } from "@/worker/common";
import type { Logger } from "@/worker/common/logger";
import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type { Limiter } from "@/worker/git/operations/limits";
import { countSubrequest } from "@/worker/git/operations/limits";
import { readCommit } from "@/worker/git/operations/read/commits";
import { readBlob } from "@/worker/git/operations/read/objects";
import {
  isSymlinkMode,
  isTreeMode,
  joinTreePath,
  readTree,
} from "@/worker/git/operations/read/tree";

const MAX_FILES = 100;
const MAX_TOTAL_BYTES = 10 * 1024 * 1024;
const MAX_PATH_BYTES = 4096;
const MAX_PATH_SEGMENTS = 128;

export type SnapshotManifest = {
  version: 1;
  repositoryId: string;
  commitSha: string;
  treeSha: string;
  files: Array<{ path: string; bytes: number; sha256: string }>;
};

function configuredPrefix(env: Env): string | null {
  const prefix = env.SNAPSHOT_BENCHMARK_PREFIX?.trim();
  if (!prefix) return null;
  if (!/^[a-z0-9][a-z0-9/_-]{0,127}$/i.test(prefix) || prefix.includes("..")) {
    throw new Error("Invalid snapshot benchmark prefix");
  }
  return prefix.replace(/\/$/, "");
}

function snapshotRoot(prefix: string, repositoryId: string, commitSha: string): string {
  return `${prefix}/${encodeURIComponent(repositoryId)}/${commitSha}`;
}

function validateMaterializedPath(path: string): void {
  const segments = path.split("/");
  if (
    !path ||
    new TextEncoder().encode(path).byteLength > MAX_PATH_BYTES ||
    segments.length > MAX_PATH_SEGMENTS ||
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

export async function materializeAcceptedWrite(args: {
  env: Env;
  repoId: string;
  fact: AcceptedWriteFact;
  request: Request;
  ctx: ExecutionContext;
  limiter: Limiter;
  log: Logger;
}): Promise<SnapshotManifest | null> {
  const prefix = configuredPrefix(args.env);
  if (!prefix) return null;
  const cacheCtx: CacheContext = {
    req: args.request,
    ctx: args.ctx,
    memo: { limiter: args.limiter, flags: new Set(["no-cache-read", "no-cache-write"]) },
  };
  const commit = await readCommit(args.env, args.repoId, args.fact.afterSha, cacheCtx);
  const files: Array<{ path: string; bytes: Uint8Array; sha256: string }> = [];
  let totalBytes = 0;

  const visitTree = async (treeOid: string, basePath: string): Promise<void> => {
    const entries = await readTree(args.env, args.repoId, treeOid, cacheCtx);
    for (const entry of entries) {
      const path = joinTreePath(basePath, entry.name);
      validateMaterializedPath(path);
      if (isTreeMode(entry.mode)) {
        await visitTree(entry.oid, path);
        continue;
      }
      if (isSymlinkMode(entry.mode) || !entry.mode.startsWith("100")) {
        throw new Error("Snapshot contains an unsupported Git entry");
      }
      const blob = await readBlob(args.env, args.repoId, entry.oid, cacheCtx);
      if (blob.type !== "blob" || !blob.content) throw new Error("Snapshot blob is unavailable");
      totalBytes += blob.content.byteLength;
      if (files.length >= MAX_FILES || totalBytes > MAX_TOTAL_BYTES) {
        throw new Error("Snapshot exceeds benchmark limits");
      }
      files.push({ path, bytes: blob.content, sha256: await digest(blob.content) });
    }
  };

  await visitTree(commit.tree, "");
  const root = snapshotRoot(prefix, args.fact.repositoryId, args.fact.afterSha);
  await Promise.all(
    files.map(async (file) => {
      count(cacheCtx, args.log, "r2:put-snapshot-file");
      await args.limiter.run("r2:put-snapshot-file", () =>
        args.env.REPO_BUCKET.put(`${root}/files/${file.path}`, file.bytes, {
          httpMetadata: { contentType: "application/octet-stream" },
          customMetadata: { sha256: file.sha256 },
        })
      );
    })
  );
  const manifest: SnapshotManifest = {
    version: 1,
    repositoryId: args.fact.repositoryId,
    commitSha: args.fact.afterSha,
    treeSha: commit.tree,
    files: files.map(({ path, bytes, sha256 }) => ({
      path,
      bytes: bytes.byteLength,
      sha256,
    })),
  };
  count(cacheCtx, args.log, "r2:put-snapshot-manifest");
  await args.limiter.run("r2:put-snapshot-manifest", () =>
    args.env.REPO_BUCKET.put(`${root}/manifest.json`, JSON.stringify(manifest), {
      httpMetadata: { contentType: "application/json" },
    })
  );
  args.log.info("snapshot:materialized", {
    repositoryId: args.fact.repositoryId,
    commitSha: args.fact.afterSha,
    fileCount: manifest.files.length,
    totalBytes,
    sourceSurface: args.fact.sourceSurface,
  });
  return manifest;
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
  validateMaterializedPath(args.path);
  return `${root}/files/${args.path}`;
}
