import type { CacheContext } from "@/worker/cache";
import type {
  CommitDiffChangeType,
  CommitDiffEntry,
  CommitDiffResult,
  CommitFilePatchResult,
  TreeEntry,
} from "./types";
import { readTree, isTreeMode, joinTreePath } from "./tree";
import { readCommitInfo } from "./commits";
import { readBlob } from "./objects";
import { bytesToText, detectBinary } from "@/shared/web";
import { buildCacheKeyFrom, cacheOrLoadJSONForRequestWithTTL } from "@/worker/cache";

export async function listCommitChangedFiles(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext,
  opts?: {
    maxFiles?: number;
    maxTreePairs?: number;
    timeBudgetMs?: number;
  }
): Promise<CommitDiffResult> {
  const maxFiles = Math.max(1, Math.floor(opts?.maxFiles ?? 300));
  const maxTreePairs = Math.max(1, Math.floor(opts?.maxTreePairs ?? 2000));
  const timeBudgetMs = Math.max(1, Math.floor(opts?.timeBudgetMs ?? 2000));
  const startedAt = Date.now();
  const treeMemo = new Map<string, TreeEntry[]>();
  const entries: CommitDiffEntry[] = [];

  let added = 0;
  let modified = 0;
  let deleted = 0;
  let treePairs = 0;
  let truncated = false;
  let truncateReason: CommitDiffResult["truncateReason"];

  const setTruncated = (reason: NonNullable<CommitDiffResult["truncateReason"]>) => {
    if (!truncated) {
      truncated = true;
      truncateReason = reason;
    }
  };

  const shouldStop = () => {
    if (truncated) return true;
    if (entries.length >= maxFiles) {
      setTruncated("max_files");
      return true;
    }
    if (treePairs >= maxTreePairs) {
      setTruncated("max_tree_pairs");
      return true;
    }
    if (Date.now() - startedAt >= timeBudgetMs) {
      setTruncated("time_budget");
      return true;
    }
    if ((cacheCtx?.memo?.subreqBudget ?? 0) < 0) {
      setTruncated("soft_budget");
      return true;
    }
    return false;
  };

  const readTreeEntriesMemoized = async (treeOid?: string): Promise<TreeEntry[] | null> => {
    if (!treeOid) return [];
    const cached = treeMemo.get(treeOid);
    if (cached) return cached;
    try {
      const treeEntries = await readTree(env, repoId, treeOid, cacheCtx);
      treeMemo.set(treeOid, treeEntries);
      return treeEntries;
    } catch (error) {
      if (Date.now() - startedAt >= timeBudgetMs) {
        setTruncated("time_budget");
        return null;
      }
      if ((cacheCtx?.memo?.subreqBudget ?? 0) < 0) {
        setTruncated("soft_budget");
        return null;
      }
      throw error;
    }
  };

  const addEntry = (
    changeType: CommitDiffChangeType,
    path: string,
    oldEntry?: TreeEntry,
    newEntry?: TreeEntry
  ) => {
    if (shouldStop()) return;
    entries.push({
      path,
      changeType,
      oldOid: oldEntry?.oid,
      newOid: newEntry?.oid,
      oldMode: oldEntry?.mode,
      newMode: newEntry?.mode,
    });
    if (changeType === "A") added++;
    else if (changeType === "M") modified++;
    else deleted++;
  };

  const walkAddedOrDeletedSubtree = async (
    entry: TreeEntry,
    basePath: string,
    changeType: Exclude<CommitDiffChangeType, "M">
  ): Promise<void> => {
    if (shouldStop()) return;
    const path = joinTreePath(basePath, entry.name);
    if (!isTreeMode(entry.mode)) {
      addEntry(
        changeType,
        path,
        changeType === "D" ? entry : undefined,
        changeType === "A" ? entry : undefined
      );
      return;
    }
    const childEntries = await readTreeEntriesMemoized(entry.oid);
    if (!childEntries || shouldStop()) return;
    const sortedChildren = [...childEntries].sort((a, b) =>
      a.name === b.name ? a.oid.localeCompare(b.oid) : a.name.localeCompare(b.name)
    );
    for (const child of sortedChildren) {
      await walkAddedOrDeletedSubtree(child, path, changeType);
      if (shouldStop()) return;
    }
  };

  const diffTreePair = async (
    oldTreeOid?: string,
    newTreeOid?: string,
    basePath = ""
  ): Promise<void> => {
    if (shouldStop()) return;
    if (oldTreeOid && newTreeOid && oldTreeOid === newTreeOid) return;

    if (!oldTreeOid && !newTreeOid) return;

    if (!oldTreeOid) {
      const newEntries = await readTreeEntriesMemoized(newTreeOid);
      if (!newEntries || shouldStop()) return;
      const sortedNewEntries = [...newEntries].sort((a, b) =>
        a.name === b.name ? a.oid.localeCompare(b.oid) : a.name.localeCompare(b.name)
      );
      for (const entry of sortedNewEntries) {
        await walkAddedOrDeletedSubtree(entry, basePath, "A");
        if (shouldStop()) return;
      }
      return;
    }

    if (!newTreeOid) {
      const oldEntries = await readTreeEntriesMemoized(oldTreeOid);
      if (!oldEntries || shouldStop()) return;
      const sortedOldEntries = [...oldEntries].sort((a, b) =>
        a.name === b.name ? a.oid.localeCompare(b.oid) : a.name.localeCompare(b.name)
      );
      for (const entry of sortedOldEntries) {
        await walkAddedOrDeletedSubtree(entry, basePath, "D");
        if (shouldStop()) return;
      }
      return;
    }

    treePairs++;
    if (shouldStop()) return;

    const [oldEntries, newEntries] = await Promise.all([
      readTreeEntriesMemoized(oldTreeOid),
      readTreeEntriesMemoized(newTreeOid),
    ]);
    if (!oldEntries || !newEntries || shouldStop()) return;

    const oldByName = new Map(oldEntries.map((entry) => [entry.name, entry]));
    const newByName = new Map(newEntries.map((entry) => [entry.name, entry]));
    const names = [...new Set([...oldByName.keys(), ...newByName.keys()])].sort((a, b) =>
      a.localeCompare(b)
    );

    for (const name of names) {
      if (shouldStop()) return;
      const oldEntry = oldByName.get(name);
      const newEntry = newByName.get(name);
      if (!oldEntry && newEntry) {
        await walkAddedOrDeletedSubtree(newEntry, basePath, "A");
        continue;
      }
      if (oldEntry && !newEntry) {
        await walkAddedOrDeletedSubtree(oldEntry, basePath, "D");
        continue;
      }
      if (!oldEntry || !newEntry) continue;

      const path = joinTreePath(basePath, name);
      const oldIsTree = isTreeMode(oldEntry.mode);
      const newIsTree = isTreeMode(newEntry.mode);

      if (oldIsTree && newIsTree) {
        await diffTreePair(oldEntry.oid, newEntry.oid, path);
        continue;
      }

      if (!oldIsTree && !newIsTree) {
        if (oldEntry.oid !== newEntry.oid || oldEntry.mode !== newEntry.mode) {
          addEntry("M", path, oldEntry, newEntry);
        }
        continue;
      }

      if (!oldIsTree && newIsTree) {
        addEntry("D", path, oldEntry, undefined);
        const childEntries = await readTreeEntriesMemoized(newEntry.oid);
        if (!childEntries || shouldStop()) return;
        const sortedChildren = [...childEntries].sort((a, b) =>
          a.name === b.name ? a.oid.localeCompare(b.oid) : a.name.localeCompare(b.name)
        );
        for (const child of sortedChildren) {
          await walkAddedOrDeletedSubtree(child, path, "A");
          if (shouldStop()) return;
        }
        continue;
      }

      const childEntries = await readTreeEntriesMemoized(oldEntry.oid);
      if (!childEntries || shouldStop()) return;
      const sortedChildren = [...childEntries].sort((a, b) =>
        a.name === b.name ? a.oid.localeCompare(b.oid) : a.name.localeCompare(b.name)
      );
      for (const child of sortedChildren) {
        await walkAddedOrDeletedSubtree(child, path, "D");
        if (shouldStop()) return;
      }
      addEntry("A", path, undefined, newEntry);
    }
  };

  const commit = await readCommitInfo(env, repoId, oid, cacheCtx);
  const baseCommitOid = commit.parents[0];
  const baseCommit = baseCommitOid
    ? await readCommitInfo(env, repoId, baseCommitOid, cacheCtx)
    : undefined;

  await diffTreePair(baseCommit?.tree, commit.tree);

  entries.sort((a, b) =>
    a.path === b.path ? a.changeType.localeCompare(b.changeType) : a.path.localeCompare(b.path)
  );

  return {
    baseCommitOid,
    compareMode: baseCommitOid ? "first-parent" : "root",
    entries,
    added,
    modified,
    deleted,
    total: entries.length,
    truncated,
    truncateReason,
  };
}

function toPatchLines(text: string): string[] {
  if (!text) return [];
  const normalized = text.replace(/\r\n/g, "\n");
  if (!normalized) return [];
  const lines = normalized.split("\n");
  if (normalized.endsWith("\n")) lines.pop();
  return lines;
}

type PatchOp = {
  kind: " " | "+" | "-";
  line: string;
};

function diffLines(oldLines: string[], newLines: string[]): PatchOp[] {
  const oldCount = oldLines.length;
  const newCount = newLines.length;
  if (!oldCount && !newCount) return [];
  if (!oldCount) return newLines.map((line) => ({ kind: "+", line }));
  if (!newCount) return oldLines.map((line) => ({ kind: "-", line }));

  const directions = new Uint8Array(oldCount * newCount);
  let previous = new Uint16Array(newCount + 1);
  let current = new Uint16Array(newCount + 1);

  for (let oldIndex = 1; oldIndex <= oldCount; oldIndex++) {
    current.fill(0);
    for (let newIndex = 1; newIndex <= newCount; newIndex++) {
      const directionIndex = (oldIndex - 1) * newCount + (newIndex - 1);
      if (oldLines[oldIndex - 1] === newLines[newIndex - 1]) {
        current[newIndex] = previous[newIndex - 1] + 1;
        directions[directionIndex] = 1;
      } else if (previous[newIndex] >= current[newIndex - 1]) {
        current[newIndex] = previous[newIndex];
        directions[directionIndex] = 2;
      } else {
        current[newIndex] = current[newIndex - 1];
        directions[directionIndex] = 3;
      }
    }
    const swap = previous;
    previous = current;
    current = swap;
  }

  const ops: PatchOp[] = [];
  let oldIndex = oldCount;
  let newIndex = newCount;
  while (oldIndex > 0 || newIndex > 0) {
    if (oldIndex > 0 && newIndex > 0) {
      const direction = directions[(oldIndex - 1) * newCount + (newIndex - 1)];
      if (direction === 1) {
        ops.push({ kind: " ", line: oldLines[oldIndex - 1] });
        oldIndex--;
        newIndex--;
        continue;
      }
      if (direction === 3) {
        ops.push({ kind: "+", line: newLines[newIndex - 1] });
        newIndex--;
        continue;
      }
    }
    if (oldIndex > 0) {
      ops.push({ kind: "-", line: oldLines[oldIndex - 1] });
      oldIndex--;
    } else {
      ops.push({ kind: "+", line: newLines[newIndex - 1] });
      newIndex--;
    }
  }

  ops.reverse();
  return ops;
}

function formatUnifiedRange(start: number, count: number): string {
  if (count === 0) return `${Math.max(0, start - 1)},0`;
  if (count === 1) return `${start}`;
  return `${start},${count}`;
}

function buildUnifiedPatch(
  path: string,
  changeType: CommitDiffChangeType,
  oldText: string,
  newText: string
): string {
  const oldLines = toPatchLines(oldText);
  const newLines = toPatchLines(newText);
  const ops = diffLines(oldLines, newLines);
  const changeIndexes = ops
    .map((op, index) => ({ op, index }))
    .filter(({ op }) => op.kind !== " ")
    .map(({ index }) => index);
  const oldLabel = changeType === "A" ? "/dev/null" : `a/${path}`;
  const newLabel = changeType === "D" ? "/dev/null" : `b/${path}`;
  if (!changeIndexes.length) {
    return `--- ${oldLabel}\n+++ ${newLabel}\n`;
  }

  const contextLines = 3;
  const ranges: Array<{ start: number; end: number }> = [];
  for (const index of changeIndexes) {
    const start = Math.max(0, index - contextLines);
    const end = Math.min(ops.length, index + contextLines + 1);
    const lastRange = ranges[ranges.length - 1];
    if (lastRange && start <= lastRange.end) {
      lastRange.end = Math.max(lastRange.end, end);
      continue;
    }
    ranges.push({ start, end });
  }

  const oldLineStarts = new Uint32Array(ops.length + 1);
  const newLineStarts = new Uint32Array(ops.length + 1);
  oldLineStarts[0] = 1;
  newLineStarts[0] = 1;
  for (let index = 0; index < ops.length; index++) {
    oldLineStarts[index + 1] = oldLineStarts[index] + (ops[index].kind === "+" ? 0 : 1);
    newLineStarts[index + 1] = newLineStarts[index] + (ops[index].kind === "-" ? 0 : 1);
  }

  const out: string[] = [`--- ${oldLabel}`, `+++ ${newLabel}`];
  for (const range of ranges) {
    const hunkOps = ops.slice(range.start, range.end);
    const oldStart = oldLineStarts[range.start];
    const newStart = newLineStarts[range.start];
    const oldCount = hunkOps.reduce((count, op) => count + (op.kind === "+" ? 0 : 1), 0);
    const newCount = hunkOps.reduce((count, op) => count + (op.kind === "-" ? 0 : 1), 0);
    out.push(
      `@@ -${formatUnifiedRange(oldStart, oldCount)} +${formatUnifiedRange(newStart, newCount)} @@`
    );
    for (const op of hunkOps) {
      out.push(`${op.kind}${op.line}`);
    }
  }

  return `${out.join("\n")}\n`;
}

async function loadCommitDiffResultCached(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<CommitDiffResult> {
  if (!cacheCtx) {
    return await listCommitChangedFiles(env, repoId, oid);
  }
  const diffCacheKey = buildCacheKeyFrom(cacheCtx.req, "/_cache/commit-diff", {
    repo: repoId,
    oid,
    v: "1",
  });
  const diff = await cacheOrLoadJSONForRequestWithTTL<CommitDiffResult>(
    cacheCtx,
    diffCacheKey,
    async () => await listCommitChangedFiles(env, repoId, oid, cacheCtx),
    () => 86400
  );
  if (!diff) {
    throw new Error("Commit diff not found");
  }
  return diff;
}

export async function readCommitFilePatch(
  env: Env,
  repoId: string,
  oid: string,
  path: string,
  cacheCtx?: CacheContext,
  opts?: {
    maxBlobBytes?: number;
    maxPatchBytes?: number;
    maxLines?: number;
  }
): Promise<CommitFilePatchResult> {
  const maxBlobBytes = Math.max(1, Math.floor(opts?.maxBlobBytes ?? 128 * 1024));
  const maxPatchBytes = Math.max(1, Math.floor(opts?.maxPatchBytes ?? 256 * 1024));
  const maxLines = Math.max(1, Math.floor(opts?.maxLines ?? 4000));
  const diff = await loadCommitDiffResultCached(env, repoId, oid, cacheCtx);
  const entry = diff.entries.find((candidate) => candidate.path === path);
  if (!entry) {
    return {
      path,
      changeType: "M",
      skipped: true,
      skipReason: "not_found",
    };
  }

  const result: CommitFilePatchResult = {
    path: entry.path,
    changeType: entry.changeType,
    oldOid: entry.oldOid,
    newOid: entry.newOid,
  };

  const [oldBlob, newBlob] = await Promise.all([
    entry.oldOid
      ? readBlob(env, repoId, entry.oldOid, cacheCtx)
      : Promise.resolve({ content: null, type: null }),
    entry.newOid
      ? readBlob(env, repoId, entry.newOid, cacheCtx)
      : Promise.resolve({ content: null, type: null }),
  ]);

  if (
    (entry.oldOid && (oldBlob.type !== "blob" || !oldBlob.content)) ||
    (entry.newOid && (newBlob.type !== "blob" || !newBlob.content))
  ) {
    return {
      ...result,
      skipped: true,
      skipReason: "not_found",
    };
  }

  const oldContent = oldBlob.content ?? new Uint8Array(0);
  const newContent = newBlob.content ?? new Uint8Array(0);

  if (oldContent.byteLength > maxBlobBytes) result.oldTooLarge = true;
  if (newContent.byteLength > maxBlobBytes) result.newTooLarge = true;
  if (result.oldTooLarge || result.newTooLarge) {
    return {
      ...result,
      skipped: true,
      skipReason: "too_large",
    };
  }

  if (detectBinary(oldContent) || detectBinary(newContent)) {
    return {
      ...result,
      binary: true,
      skipped: true,
      skipReason: "binary",
    };
  }

  const oldText = bytesToText(oldContent);
  const newText = bytesToText(newContent);
  const oldLines = toPatchLines(oldText);
  const newLines = toPatchLines(newText);
  if (oldLines.length > maxLines || newLines.length > maxLines) {
    return {
      ...result,
      skipped: true,
      skipReason: "too_many_lines",
    };
  }

  const patch = buildUnifiedPatch(path, entry.changeType, oldText, newText);
  if (new TextEncoder().encode(patch).byteLength > maxPatchBytes) {
    return {
      ...result,
      skipped: true,
      skipReason: "too_large",
    };
  }

  return {
    ...result,
    patch,
  };
}
