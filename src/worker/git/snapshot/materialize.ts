import type { CacheContext } from "@/worker/cache";
import { asBufferSource, bytesToHex, getRepoStub } from "@/worker/common";
import type { Logger } from "@/worker/common/logger";
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
  fact: SnapshotMaterializationTarget;
  request: Request;
  ctx: ExecutionContext;
  limiter: Limiter;
  log: Logger;
}): Promise<SnapshotManifest | null> {
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
    return null;
  }

  try {
    const renewLease = async (): Promise<void> => {
      count(cacheCtx, args.log, "do:renew-snapshot-materialization");
      const renewed = await args.limiter.run("do:renew-snapshot-materialization", () =>
        stub.renewSnapshotMaterialization(lease.token)
      );
      if (!renewed) throw new Error("Snapshot materialization lease is no longer active");
    };
    const commit = await readCommit(args.env, args.repoId, args.fact.afterSha, cacheCtx);
    const files: Array<{ path: string; bytes: Uint8Array; sha256: string }> = [];
    let totalBytes = 0;

    const visitTree = async (treeOid: string, basePath: string): Promise<void> => {
      const entries = await readTree(args.env, args.repoId, treeOid, cacheCtx);
      for (const entry of entries) {
        const path = joinTreePath(basePath, entry.name);
        validateSnapshotPath(path);
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
    // Keep file writes sequential so a failed renewal cannot release the lease
    // while an earlier R2 put remains in flight. Manifest-last ordering remains
    // the readiness boundary.
    for (const file of files) {
      await renewLease();
      count(cacheCtx, args.log, "r2:put-snapshot-file");
      await args.limiter.run("r2:put-snapshot-file", () =>
        args.env.REPO_BUCKET.put(`${root}/files/${file.path}`, file.bytes, {
          httpMetadata: { contentType: "application/octet-stream" },
          customMetadata: { sha256: file.sha256 },
        })
      );
    }
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
    await renewLease();
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
  } finally {
    count(cacheCtx, args.log, "do:finish-snapshot-materialization");
    const finished = await args.limiter.run("do:finish-snapshot-materialization", () =>
      stub.finishSnapshotMaterialization(lease.token)
    );
    if (!finished) {
      args.log.warn("snapshot:materialization-lease-missing", {
        repositoryId: args.fact.repositoryId,
      });
    }
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
