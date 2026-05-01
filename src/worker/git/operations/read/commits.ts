import type { CacheContext } from "@/worker/cache";
import type { CommitInfo, MergeSideOptions } from "./types";
import { parseCommitText } from "@/worker/git/core/commitParse";
import { readLooseObjectRaw } from "./objects";
import { resolveRef } from "./refs";
import { MAX_SIMULTANEOUS_CONNECTIONS } from "../limits";
import { createLogger, BinaryHeap } from "@/worker/common";

export async function readCommit(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<{ tree: string; parents: string[]; message: string }> {
  const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
  if (!obj || obj.type !== "commit") throw new Error("Not a commit");
  const text = new TextDecoder().decode(obj.payload);
  const parsed = parseCommitText(text);
  return { tree: parsed.tree, parents: parsed.parents, message: parsed.message };
}

export async function readCommitInfo(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<CommitInfo> {
  const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
  if (!obj || obj.type !== "commit") throw new Error("Not a commit");
  const text = new TextDecoder().decode(obj.payload);
  const parsed = parseCommitText(text);
  const { tree, parents, author, committer, message } = parsed;
  return { oid, tree, parents, author, committer, message };
}

export async function listCommitsFirstParentRange(
  env: Env,
  repoId: string,
  start: string,
  offset: number,
  limit: number,
  cacheCtx?: CacheContext
): Promise<CommitInfo[]> {
  let oid = await resolveRef(env, repoId, start);
  if (!oid && /^[0-9a-f]{40}$/i.test(start)) oid = start.toLowerCase();
  // Peel annotated tag
  if (oid) {
    const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
    if (obj && obj.type === "tag") {
      const text = new TextDecoder().decode(obj.payload);
      const m = text.match(/^object ([0-9a-f]{40})/m);
      if (m) oid = m[1];
    }
  }
  if (!oid) throw new Error("Ref not found");
  const seen = new Set<string>();

  const targetOids: string[] = [];
  let index = 0;
  while (oid && !seen.has(oid) && targetOids.length < limit) {
    seen.add(oid);
    if (index >= offset) {
      targetOids.push(oid);
    }
    const c = await readCommit(env, repoId, oid, cacheCtx);
    index++;
    oid = c.parents[0];
  }
  if (targetOids.length === 0) return [];

  const out: CommitInfo[] = [];
  const CONCURRENCY = Math.max(1, Math.min(MAX_SIMULTANEOUS_CONNECTIONS, 6));
  for (let i = 0; i < targetOids.length; i += CONCURRENCY) {
    const batch = targetOids.slice(i, i + CONCURRENCY);
    const infos = await Promise.all(batch.map((q) => readCommitInfo(env, repoId, q, cacheCtx)));
    out.push(...infos);
  }
  return out;
}

export async function listMergeSideFirstParent(
  env: Env,
  repoId: string,
  mergeOid: string,
  limit = 20,
  options: MergeSideOptions = {},
  cacheCtx?: CacheContext
): Promise<CommitInfo[]> {
  const logger = createLogger(env.LOG_LEVEL, { service: "listMergeSideFirstParent", repoId });
  const scanLimit = Math.min(400, Math.max(limit * 3, options.scanLimit ?? 120));
  const timeBudgetMs = Math.max(50, Math.min(10000, options.timeBudgetMs ?? 150));
  const mainlineProbe = Math.min(1000, Math.max(50, options.mainlineProbe ?? 100));
  const started = Date.now();

  const merge = await readCommitInfo(env, repoId, mergeOid, cacheCtx);
  const parents = merge.parents || [];
  if (parents.length < 2) return [];

  const mainlineSet = new Set<string>();
  try {
    let cur: string | undefined = parents[0];
    let seen = 0;
    const visited = new Set<string>();
    const probeStarted = Date.now();
    const probeTimeBudget = Math.min(1500, timeBudgetMs / 3);

    while (
      cur &&
      seen < mainlineProbe &&
      !visited.has(cur) &&
      Date.now() - probeStarted < probeTimeBudget
    ) {
      visited.add(cur);
      mainlineSet.add(cur);
      const info = await readCommitInfo(env, repoId, cur, cacheCtx);
      cur = info.parents?.[0];
      seen++;
    }

    logger.info("Mainline probe completed", {
      commits: seen,
      timeMs: Date.now() - probeStarted,
      mergeOid,
    });
  } catch {}

  const newerFirst = (a: CommitInfo, b: CommitInfo) => {
    const aw = a.author?.when ?? 0;
    const bw = b.author?.when ?? 0;
    if (aw !== bw) return bw - aw;
    return b.oid.localeCompare(a.oid);
  };

  const visited = new Set<string>();
  const frontier: CommitInfo[] = [];
  for (let i = 1; i < parents.length; i++) {
    const p = parents[i];
    try {
      const info = await readCommitInfo(env, repoId, p, cacheCtx);
      frontier.push(info);
    } catch {}
  }
  const heap = new BinaryHeap<CommitInfo>(newerFirst, frontier);

  const out: CommitInfo[] = [];
  let scanned = 0;

  while (
    out.length < limit &&
    !heap.isEmpty() &&
    scanned < scanLimit &&
    Date.now() - started < timeBudgetMs
  ) {
    const current = heap.pop()!;
    scanned++;
    if (visited.has(current.oid)) continue;
    visited.add(current.oid);

    if (mainlineSet.has(current.oid)) continue;

    out.push(current);
    if (out.length >= limit) break;

    const next = current.parents?.[0];
    if (next && !visited.has(next)) {
      try {
        const ni = await readCommitInfo(env, repoId, next, cacheCtx);
        heap.push(ni);
      } catch {}
    }
  }

  return out;
}
