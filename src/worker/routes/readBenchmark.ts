import type { CacheContext } from "@/worker/cache";
import type { SnapshotResolution } from "@/worker/do/repo/acceptedWrites";
import type { BeginRepositoryReadResult } from "@/worker/do/repo/repositoryLifecycle";
import { asBufferSource, bytesToHex, getRepoStub } from "@/worker/common";
import { loadActivePackCatalog } from "@/worker/git/object-store/catalog";
import { findObject } from "@/worker/git/object-store/lookup";
import {
  decodePackObjectSize,
  readPackHeaderExFromBuf,
  readPackRange,
} from "@/worker/git/pack/packMeta";
import { countSubrequest, DEFAULT_SUBREQUEST_BUDGET } from "@/worker/git/operations/limits";
import { listCommitChangedFiles } from "@/worker/git/operations/read/diff";
import { listCommitsFirstParentRange, readCommit } from "@/worker/git/operations/read/commits";
import { evictObjectFromRequestMemo, readBlob } from "@/worker/git/operations/read/objects";
import {
  isSymlinkMode,
  isTreeMode,
  joinTreePath,
  readTree,
} from "@/worker/git/operations/read/tree";
import {
  snapshotBundleObjectKey,
  snapshotObjectKey,
  type SnapshotManifest,
  validateSnapshotPath,
} from "@/worker/git/snapshot/materialize";
import { resolveRepositoryRoute, type RepositoryRoute } from "@/worker/repositories/route";
import { isValidOwnerRepo } from "@/shared/web";
import { authorizeInternalRequest } from "./internalAuth";
import type { AppContext, AppRouter } from "./hono";

const MAX_SEARCH_NEEDLE_BYTES = 64;
const MAX_LOG_COMMITS = 100;
const MAX_BENCHMARK_FILES = 100;
const MAX_BENCHMARK_TREES = 200;
const MAX_SCALE_BENCHMARK_FILES = 1_000;
const MAX_SCALE_BENCHMARK_TREES = 2_000;
const MAX_BENCHMARK_PACKS = 128;
const MAX_BENCHMARK_FILE_BYTES = 8 * 1024 * 1024;
const MAX_BENCHMARK_TOTAL_BYTES = 512 * 1024 * 1024;
const MAX_SCALE_BENCHMARK_TOTAL_BYTES = 5_000_000_000;
const BENCHMARK_SUBREQUEST_HEADROOM = 300;

type WalkedFile = { path: string; oid: string };

class BenchmarkLimitError extends Error {}

function isSnapshotBenchmarkOperation(operation: string): boolean {
  return operation.startsWith("snapshot-") || operation.startsWith("scale-snapshot-");
}

function isScaleSnapshotBenchmarkOperation(operation: string): boolean {
  return operation === "scale-snapshot-tree" || operation === "scale-snapshot-blob";
}

function addBenchmarkBytes(total: number, next: number): number {
  const result = total + next;
  if (!Number.isSafeInteger(result) || result > MAX_BENCHMARK_TOTAL_BYTES) {
    throw new BenchmarkLimitError("Benchmark aggregate byte limit exceeded");
  }
  return result;
}

function assertBenchmarkFileCapacity(fileCount: number, maxFiles: number): void {
  if (fileCount >= maxFiles) {
    throw new BenchmarkLimitError("Benchmark file limit exceeded");
  }
}

export const __test = {
  addBenchmarkBytes,
  assertBenchmarkFileCapacity,
  isScaleSnapshotBenchmarkOperation,
  maxScaleBenchmarkFiles: MAX_SCALE_BENCHMARK_FILES,
};

async function sha256(bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

async function sha256Text(value: string): Promise<string> {
  return await sha256(new TextEncoder().encode(value));
}

function countNeedle(content: Uint8Array, needle: Uint8Array): number {
  if (needle.byteLength === 0 || content.byteLength < needle.byteLength) return 0;
  let matches = 0;
  for (let offset = 0; offset <= content.byteLength - needle.byteLength; offset++) {
    let matched = true;
    for (let index = 0; index < needle.byteLength; index++) {
      if (content[offset + index] !== needle[index]) {
        matched = false;
        break;
      }
    }
    if (matched) matches++;
  }
  return matches;
}

function benchmarkCacheContext(c: AppContext, route: RepositoryRoute): CacheContext {
  c.var.cacheCtx.memo = {
    repoId: route.doName,
    limiter: c.var.limiter,
    subreqBudget: DEFAULT_SUBREQUEST_BUDGET,
    flags: new Set(["no-cache-read", "no-cache-write"]),
  };
  return c.var.cacheCtx;
}

function requireReadBudget(cacheCtx: CacheContext): void {
  const remaining = cacheCtx.memo?.subreqBudget ?? DEFAULT_SUBREQUEST_BUDGET;
  if (remaining <= BENCHMARK_SUBREQUEST_HEADROOM) {
    throw new BenchmarkLimitError("Benchmark subrequest headroom exhausted");
  }
}

async function walkCommitFiles(
  c: AppContext,
  route: RepositoryRoute,
  commitSha: string,
  cacheCtx: CacheContext,
  limits: { maxFiles: number; maxTrees: number } = {
    maxFiles: MAX_BENCHMARK_FILES,
    maxTrees: MAX_BENCHMARK_TREES,
  }
): Promise<{ treeSha: string; treeCount: number; files: WalkedFile[] }> {
  requireReadBudget(cacheCtx);
  const commit = await readCommit(c.env, route.doName, commitSha, cacheCtx);
  const files: WalkedFile[] = [];
  let treeCount = 0;
  const visit = async (treeSha: string, basePath: string): Promise<void> => {
    treeCount++;
    if (treeCount > limits.maxTrees) {
      throw new BenchmarkLimitError("Benchmark tree limit exceeded");
    }
    requireReadBudget(cacheCtx);
    const entries = await readTree(c.env, route.doName, treeSha, cacheCtx);
    for (const entry of entries) {
      const path = joinTreePath(basePath, entry.name);
      validateSnapshotPath(path);
      if (isTreeMode(entry.mode)) {
        await visit(entry.oid, path);
      } else if (isSymlinkMode(entry.mode) || !entry.mode.startsWith("100")) {
        throw new Error("Benchmark repository contains an unsupported Git entry");
      } else {
        assertBenchmarkFileCapacity(files.length, limits.maxFiles);
        files.push({ path, oid: entry.oid });
      }
    }
  };
  await visit(commit.tree, "");
  return { treeSha: commit.tree, treeCount, files };
}

async function resolveBenchmarkBlob(
  c: AppContext,
  route: RepositoryRoute,
  commitSha: string,
  path: string,
  cacheCtx: CacheContext
): Promise<WalkedFile> {
  try {
    validateSnapshotPath(path);
  } catch {
    throw new Error("Benchmark path is not a blob");
  }
  const parts = path.split("/");
  requireReadBudget(cacheCtx);
  let treeSha = (await readCommit(c.env, route.doName, commitSha, cacheCtx)).tree;
  for (let index = 0; index < parts.length; index++) {
    if (index >= MAX_BENCHMARK_TREES) {
      throw new BenchmarkLimitError("Benchmark tree limit exceeded");
    }
    requireReadBudget(cacheCtx);
    const entries = await readTree(c.env, route.doName, treeSha, cacheCtx);
    const entry = entries.find((candidate) => candidate.name === parts[index]);
    if (!entry) throw new Error("Benchmark path is not a blob");
    if (index === parts.length - 1) {
      if (isTreeMode(entry.mode) || isSymlinkMode(entry.mode) || !entry.mode.startsWith("100")) {
        throw new Error("Benchmark path is not a blob");
      }
      return { path, oid: entry.oid };
    }
    if (!isTreeMode(entry.mode)) throw new Error("Benchmark path is not a blob");
    treeSha = entry.oid;
  }
  throw new Error("Benchmark path is not a blob");
}

async function assertBenchmarkBlobSize(
  c: AppContext,
  route: RepositoryRoute,
  oid: string,
  cacheCtx: CacheContext
): Promise<number> {
  requireReadBudget(cacheCtx);
  const location = await findObject(c.env, route.doName, oid, cacheCtx);
  if (!location) throw new Error("Benchmark blob is unavailable");
  const headerBytes = await readPackRange(
    c.env,
    location.source.packKey,
    location.offset,
    Math.min(64, location.nextOffset - location.offset),
    {
      limiter: c.var.limiter,
      countSubrequest: () => countSubrequest(cacheCtx),
      exactLength: true,
    }
  );
  const header = headerBytes ? readPackHeaderExFromBuf(headerBytes, 0) : null;
  const declaredSize = header ? decodePackObjectSize(header.sizeVarBytes) : undefined;
  // Delta objects require materialization to discover their final size. The
  // synthetic corpus uses independently generated full blobs, so reject a
  // delta rather than risking an unbounded inflate inside this probe.
  if (
    !header ||
    header.type !== 3 ||
    declaredSize === undefined ||
    declaredSize > MAX_BENCHMARK_FILE_BYTES
  ) {
    throw new BenchmarkLimitError("Benchmark blob exceeds the bounded corpus shape");
  }
  return declaredSize;
}

function countBenchmarkSubrequest(c: AppContext, operation: string): void {
  if (!countSubrequest(c.var.cacheCtx)) {
    c.var.logFor({ service: "ReadBenchmark" }).warn("read-benchmark:soft-budget-exhausted", {
      operation,
    });
    throw new BenchmarkLimitError("Benchmark subrequest budget exhausted");
  }
}

async function getSnapshotObject(
  c: AppContext,
  key: string,
  operation: string,
  options?: R2GetOptions
) {
  countBenchmarkSubrequest(c, operation);
  return await c.var.limiter.run(`r2:${operation}`, () => c.env.REPO_BUCKET.get(key, options));
}

function bundledSnapshotFile(
  manifest: SnapshotManifest,
  file: SnapshotManifest["files"][number]
): { offset: number; bytes: number } | null {
  if (!manifest.bundle || file.offset === undefined) return null;
  if (
    !Number.isSafeInteger(manifest.bundle.bytes) ||
    manifest.bundle.bytes < 0 ||
    !/^[0-9a-f]{64}$/.test(manifest.bundle.sha256) ||
    !Number.isSafeInteger(file.offset) ||
    file.offset < 0 ||
    file.offset + file.bytes > manifest.bundle.bytes
  ) {
    throw new Error("Invalid snapshot bundle index");
  }
  return { offset: file.offset, bytes: file.bytes };
}

function isSnapshotManifestFile(value: unknown): value is SnapshotManifest["files"][number] {
  return (
    value !== null &&
    typeof value === "object" &&
    "path" in value &&
    typeof value.path === "string" &&
    value.path.length > 0 &&
    !value.path.startsWith("/") &&
    !value.path.split("/").some((segment) => !segment || segment === "." || segment === "..") &&
    "bytes" in value &&
    typeof value.bytes === "number" &&
    Number.isSafeInteger(value.bytes) &&
    value.bytes >= 0 &&
    "sha256" in value &&
    typeof value.sha256 === "string" &&
    /^[0-9a-f]{64}$/.test(value.sha256)
  );
}

function isSnapshotManifest(
  value: unknown,
  limits: { maxFiles: number; maxTotalBytes: number } = {
    maxFiles: MAX_BENCHMARK_FILES,
    maxTotalBytes: MAX_BENCHMARK_TOTAL_BYTES,
  }
): value is SnapshotManifest {
  if (
    value === null ||
    typeof value !== "object" ||
    !("version" in value) ||
    value.version !== 1 ||
    !("repositoryId" in value) ||
    typeof value.repositoryId !== "string" ||
    value.repositoryId.length === 0 ||
    !("commitSha" in value) ||
    typeof value.commitSha !== "string" ||
    !/^[0-9a-f]{40}$/.test(value.commitSha) ||
    !("treeSha" in value) ||
    typeof value.treeSha !== "string" ||
    !/^[0-9a-f]{40}$/.test(value.treeSha) ||
    !("files" in value) ||
    !Array.isArray(value.files) ||
    value.files.length > limits.maxFiles ||
    !value.files.every(isSnapshotManifestFile)
  ) {
    return false;
  }
  const paths = new Set<string>();
  let totalBytes = 0;
  for (const file of value.files) {
    try {
      validateSnapshotPath(file.path);
    } catch {
      return false;
    }
    if (paths.has(file.path)) return false;
    if (file.bytes > MAX_BENCHMARK_FILE_BYTES) return false;
    paths.add(file.path);
    totalBytes += file.bytes;
    if (!Number.isSafeInteger(totalBytes) || totalBytes > limits.maxTotalBytes) return false;
  }
  return true;
}

function assertSnapshotManifest(
  value: unknown,
  limits?: { maxFiles: number; maxTotalBytes: number }
): SnapshotManifest {
  if (!isSnapshotManifest(value, limits)) throw new Error("Invalid snapshot manifest");
  return value;
}

async function loadSnapshotManifest(
  c: AppContext,
  route: RepositoryRoute,
  commitSha: string,
  limits?: { maxFiles: number; maxTotalBytes: number }
): Promise<SnapshotManifest> {
  const key = snapshotObjectKey({
    env: c.env,
    repositoryId: route.repositoryId,
    commitSha,
  });
  if (!key) throw new Error("Snapshot benchmark is disabled");
  const object = await getSnapshotObject(c, key, "get-benchmark-manifest");
  if (!object) throw new Error("Snapshot manifest not found");
  return assertSnapshotManifest(await object.json(), limits);
}

async function runDirectOperation(
  c: AppContext,
  route: RepositoryRoute,
  commitSha: string,
  operation: string,
  cacheCtx: CacheContext
): Promise<Record<string, unknown>> {
  if (operation === "tree") {
    const walked = await walkCommitFiles(c, route, commitSha, cacheCtx);
    return {
      treeSha: walked.treeSha,
      treeCount: walked.treeCount,
      fileCount: walked.files.length,
      digest: await sha256Text(walked.files.map((file) => `${file.path}\0${file.oid}`).join("\n")),
    };
  }
  if (operation === "scale-tree") {
    const walked = await walkCommitFiles(c, route, commitSha, cacheCtx, {
      maxFiles: MAX_SCALE_BENCHMARK_FILES,
      maxTrees: MAX_SCALE_BENCHMARK_TREES,
    });
    return {
      treeSha: walked.treeSha,
      treeCount: walked.treeCount,
      fileCount: walked.files.length,
      digest: await sha256Text(walked.files.map((file) => `${file.path}\0${file.oid}`).join("\n")),
    };
  }
  if (operation === "blob") {
    const path = c.req.query("path") ?? "";
    const result = await resolveBenchmarkBlob(c, route, commitSha, path, cacheCtx);
    const declaredSize = await assertBenchmarkBlobSize(c, route, result.oid, cacheCtx);
    const blob = await readBlob(c.env, route.doName, result.oid, cacheCtx);
    if (blob.type !== "blob" || !blob.content) throw new Error("Benchmark blob is unavailable");
    try {
      if (blob.content.byteLength !== declaredSize) {
        throw new Error("Benchmark blob size mismatch");
      }
      return {
        path,
        oid: result.oid,
        bytes: blob.content.byteLength,
        sha256: await sha256(blob.content),
      };
    } finally {
      evictObjectFromRequestMemo(cacheCtx, result.oid);
    }
  }
  if (operation === "log") {
    requireReadBudget(cacheCtx);
    const requested = Number(c.req.query("limit") ?? "50");
    const limit = Number.isSafeInteger(requested)
      ? Math.min(MAX_LOG_COMMITS, Math.max(1, requested))
      : 50;
    const commits = await listCommitsFirstParentRange(
      c.env,
      route.doName,
      commitSha,
      0,
      limit,
      cacheCtx
    );
    return {
      commitCount: commits.length,
      head: commits[0]?.oid ?? null,
      tail: commits.at(-1)?.oid ?? null,
      digest: await sha256Text(commits.map((commit) => commit.oid).join("\n")),
    };
  }
  if (operation === "compare") {
    requireReadBudget(cacheCtx);
    const compared = await listCommitChangedFiles(c.env, route.doName, commitSha, cacheCtx, {
      maxFiles: MAX_BENCHMARK_FILES,
      maxTreePairs: MAX_BENCHMARK_TREES,
      maxTreeReads: MAX_BENCHMARK_TREES,
      timeBudgetMs: 15_000,
      minSubrequestBudget: BENCHMARK_SUBREQUEST_HEADROOM,
    });
    return {
      baseCommitOid: compared.baseCommitOid ?? null,
      added: compared.added,
      modified: compared.modified,
      deleted: compared.deleted,
      total: compared.total,
      truncated: compared.truncated,
      truncateReason: compared.truncateReason ?? null,
      digest: await sha256Text(
        compared.entries
          .map(
            (entry) =>
              `${entry.changeType}\0${entry.path}\0${entry.oldOid ?? ""}\0${entry.newOid ?? ""}`
          )
          .join("\n")
      ),
    };
  }
  if (operation === "search") {
    const needleText = c.req.query("needle") ?? "display-search-needle";
    const needle = new TextEncoder().encode(needleText);
    if (needle.byteLength === 0 || needle.byteLength > MAX_SEARCH_NEEDLE_BYTES) {
      throw new Error("Invalid search needle");
    }
    const walked = await walkCommitFiles(c, route, commitSha, cacheCtx);
    const indexed: string[] = [];
    let totalBytes = 0;
    let matches = 0;
    for (const file of walked.files) {
      requireReadBudget(cacheCtx);
      const declaredSize = await assertBenchmarkBlobSize(c, route, file.oid, cacheCtx);
      totalBytes = addBenchmarkBytes(totalBytes, declaredSize);
      const blob = await readBlob(c.env, route.doName, file.oid, cacheCtx);
      if (blob.type !== "blob" || !blob.content) throw new Error("Benchmark blob is unavailable");
      try {
        if (blob.content.byteLength !== declaredSize) {
          throw new Error("Benchmark blob size mismatch");
        }
        matches += countNeedle(blob.content, needle);
        indexed.push(`${file.path}\0${await sha256(blob.content)}`);
      } finally {
        evictObjectFromRequestMemo(cacheCtx, file.oid);
      }
    }
    return {
      treeCount: walked.treeCount,
      fileCount: walked.files.length,
      totalBytes,
      matches,
      digest: await sha256Text(indexed.join("\n")),
    };
  }
  throw new Error("Unknown benchmark operation");
}

async function runSnapshotOperation(
  c: AppContext,
  route: RepositoryRoute,
  commitSha: string,
  operation: string,
  cacheCtx: CacheContext
): Promise<Record<string, unknown>> {
  const log = c.var.logFor({ service: "ReadBenchmark" });
  const scaleOperation = isScaleSnapshotBenchmarkOperation(operation);
  if (!scaleOperation) {
    const stub = getRepoStub(c.env, route.doName);
    countBenchmarkSubrequest(c, "snapshot-read-lease");
    const lease = await c.var.limiter.run<BeginRepositoryReadResult>(
      "do:begin-repository-read",
      async () => await stub.beginRepositoryRead("snapshot-read")
    );
    if (!lease.ok) throw new Error(`Snapshot read unavailable: ${lease.reason}`);
    try {
      countBenchmarkSubrequest(c, "snapshot-pin");
      const resolution = await c.var.limiter.run<SnapshotResolution>(
        "do:get-snapshot-resolution",
        async () => await stub.getSnapshotResolution(commitSha)
      );
      if (resolution.status === "released") throw new Error("Snapshot has been released");
      if (resolution.status === "pinned") {
        const commit = await readCommit(c.env, route.doName, commitSha, cacheCtx);
        if (commit.tree !== resolution.pin.treeSha) {
          throw new Error("Snapshot pin tree does not match commit");
        }
        const directOperation = operation.slice("snapshot-".length);
        return {
          source: "git-pack",
          ...(await runDirectOperation(c, route, commitSha, directOperation, cacheCtx)),
        };
      }
    } finally {
      countBenchmarkSubrequest(c, "snapshot-read-release");
      await c.var.limiter
        .run("do:finish-repository-read", () => stub.finishRepositoryRead(lease.token))
        .catch((error) =>
          log.warn("read-benchmark:lease-release-failed", { error: String(error) })
        );
    }
  }
  const manifest = await loadSnapshotManifest(
    c,
    route,
    commitSha,
    scaleOperation
      ? {
          maxFiles: MAX_SCALE_BENCHMARK_FILES,
          maxTotalBytes: MAX_SCALE_BENCHMARK_TOTAL_BYTES,
        }
      : undefined
  );
  if (manifest.repositoryId !== route.repositoryId || manifest.commitSha !== commitSha) {
    throw new Error("Snapshot manifest identity mismatch");
  }
  if (operation === "snapshot-tree" || operation === "scale-snapshot-tree") {
    return {
      treeSha: manifest.treeSha,
      fileCount: manifest.files.length,
      totalBytes: manifest.files.reduce((sum, file) => sum + file.bytes, 0),
      digest: await sha256Text(
        manifest.files.map((file) => `${file.path}\0${file.bytes}\0${file.sha256}`).join("\n")
      ),
    };
  }
  const requestedPath = c.req.query("path");
  const selected =
    operation === "snapshot-blob" || operation === "scale-snapshot-blob"
      ? manifest.files.filter((file) => file.path === requestedPath)
      : manifest.files;
  if (
    (operation === "snapshot-blob" || operation === "scale-snapshot-blob") &&
    selected.length !== 1
  ) {
    throw new Error("Benchmark snapshot path is not a blob");
  }
  if (
    operation !== "snapshot-blob" &&
    operation !== "scale-snapshot-blob" &&
    operation !== "snapshot-search"
  ) {
    throw new Error("Unknown benchmark operation");
  }
  const indexed: string[] = [];
  let totalBytes = 0;
  let matches = 0;
  const needleText = c.req.query("needle") ?? "display-search-needle";
  const needle = new TextEncoder().encode(needleText);
  if (
    operation === "snapshot-search" &&
    (needle.byteLength === 0 || needle.byteLength > MAX_SEARCH_NEEDLE_BYTES)
  ) {
    throw new Error("Invalid search needle");
  }
  for (const file of selected) {
    const bundled = bundledSnapshotFile(manifest, file);
    let bytes: Uint8Array;
    // R2 rejects zero-length ranges; the empty payload is fully described and
    // still verified by the per-file digest below.
    if (bundled && bundled.bytes === 0) {
      bytes = new Uint8Array();
    } else {
      const key = bundled
        ? snapshotBundleObjectKey({
            env: c.env,
            repositoryId: route.repositoryId,
            commitSha,
          })
        : snapshotObjectKey({
            env: c.env,
            repositoryId: route.repositoryId,
            commitSha,
            path: file.path,
          });
      if (!key) throw new Error("Snapshot benchmark is disabled");
      const object = await getSnapshotObject(
        c,
        key,
        "get-benchmark-file",
        bundled ? { range: { offset: bundled.offset, length: bundled.bytes } } : undefined
      );
      if (!object) throw new Error("Snapshot file not found");
      if (!bundled && object.size > MAX_BENCHMARK_FILE_BYTES) {
        throw new BenchmarkLimitError("Benchmark snapshot file exceeds the bounded corpus shape");
      }
      bytes = new Uint8Array(await object.arrayBuffer());
    }
    if (bytes.byteLength > MAX_BENCHMARK_FILE_BYTES) {
      throw new BenchmarkLimitError("Benchmark snapshot file exceeds the bounded corpus shape");
    }
    totalBytes = addBenchmarkBytes(totalBytes, bytes.byteLength);
    const digest = await sha256(bytes);
    if (bytes.byteLength !== file.bytes || digest !== file.sha256) {
      throw new Error("Snapshot file integrity mismatch");
    }
    if (operation === "snapshot-search") matches += countNeedle(bytes, needle);
    indexed.push(`${file.path}\0${digest}`);
  }
  if (operation === "snapshot-blob" || operation === "scale-snapshot-blob") {
    const file = selected[0]!;
    return { path: file.path, bytes: totalBytes, sha256: file.sha256 };
  }
  return {
    fileCount: selected.length,
    totalBytes,
    matches,
    digest: await sha256Text(indexed.join("\n")),
  };
}

async function resolveBenchmarkRoute(c: AppContext): Promise<RepositoryRoute | Response> {
  if (!c.env.SNAPSHOT_BENCHMARK_PREFIX?.trim()) {
    return new Response("Not found\n", { status: 404 });
  }
  const denied = await authorizeInternalRequest(c);
  if (denied) return denied;
  const owner = c.req.param("owner") ?? "";
  const repo = c.req.param("repo") ?? "";
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return new Response("Not found\n", { status: 404 });
  }
  const route = await resolveRepositoryRoute(c.env, owner, repo, {
    mode: "allow-d1-fallback",
    db: c.var.db,
    log: c.var.logFor({ service: "ReadBenchmark" }),
  });
  return route ?? new Response("Not found\n", { status: 404 });
}

async function handleReadBenchmark(c: AppContext): Promise<Response> {
  const resolved = await resolveBenchmarkRoute(c);
  if (resolved instanceof Response) return resolved;
  const commitSha = (c.req.param("commit") ?? "").toLowerCase();
  const operation = c.req.param("operation") ?? "";
  if (!/^[0-9a-f]{40}$/.test(commitSha)) {
    return Response.json({ error: "Invalid commit" }, { status: 400 });
  }
  const log = c.var.logFor({ service: "ReadBenchmark" });
  const cacheCtx = benchmarkCacheContext(c, resolved);
  if (c.req.query("cold") === "1" && !isScaleSnapshotBenchmarkOperation(operation)) {
    cacheCtx.memo?.flags?.add("no-isolate-idx-cache");
  }
  const startedAt = performance.now();
  log.info("read-benchmark:start", { repositoryId: resolved.repositoryId, commitSha, operation });
  try {
    if (operation === "state") {
      countBenchmarkSubrequest(c, operation);
      const state = await c.var.limiter.run("do:read-benchmark-state", () =>
        getRepoStub(c.env, resolved.doName).debugState()
      );
      const operationMs = performance.now() - startedAt;
      log.info("read-benchmark:complete", {
        repositoryId: resolved.repositoryId,
        commitSha,
        operation,
        operationMs,
      });
      return Response.json(
        {
          operation,
          operationMs,
          head: state.refs.find((ref) => ref.name === "refs/heads/main")?.oid ?? null,
          activePackCount: state.activePacks.length,
          packCatalogVersion: state.packCatalogVersion,
          compactionQueued: state.compaction.queued,
          compactionRunning: state.compaction.running,
        },
        { headers: { "Cache-Control": "no-store" } }
      );
    }
    if (!isScaleSnapshotBenchmarkOperation(operation)) {
      const catalog = await loadActivePackCatalog(c.env, resolved.doName, cacheCtx);
      if (catalog.length > MAX_BENCHMARK_PACKS) {
        throw new BenchmarkLimitError("Benchmark pack limit exceeded");
      }
    }
    const result = isSnapshotBenchmarkOperation(operation)
      ? await runSnapshotOperation(c, resolved, commitSha, operation, cacheCtx)
      : await runDirectOperation(c, resolved, commitSha, operation, cacheCtx);
    const operationMs = performance.now() - startedAt;
    log.info("read-benchmark:complete", {
      repositoryId: resolved.repositoryId,
      commitSha,
      operation,
      operationMs,
    });
    return Response.json(
      { operation, operationMs, ...result },
      { headers: { "Cache-Control": "no-store" } }
    );
  } catch (error) {
    log.error("read-benchmark:failed", {
      repositoryId: resolved.repositoryId,
      commitSha,
      operation,
      error: String(error),
    });
    return Response.json(
      { error: error instanceof Error ? error.message : "Benchmark failed" },
      { status: 422, headers: { "Cache-Control": "no-store" } }
    );
  }
}

export function registerReadBenchmarkRoutes(router: AppRouter): void {
  router.get("/_internal/read-benchmark/:owner/:repo/:commit/:operation", handleReadBenchmark);
}
