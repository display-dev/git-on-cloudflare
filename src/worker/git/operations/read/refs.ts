import type { CacheContext } from "@/worker/cache";
import type { HeadInfo, Ref } from "../types";
import { getRepoStub, createLogger } from "@/worker/common";
import { getLimiter } from "@/worker/git/operations/limits";

// `cacheCtx` is optional so legacy callers without a request context still
// work, but route handlers should always thread it so the DO RPC shares the
// per-request subrequest limiter.
export async function getHeadAndRefs(
  env: Env,
  repoId: string,
  cacheCtx?: CacheContext
): Promise<{ head: HeadInfo | undefined; refs: Ref[] }> {
  const stub = getRepoStub(env, repoId);
  const logger = createLogger(env.LOG_LEVEL, { service: "getHeadAndRefs", repoId });
  try {
    const limiter = getLimiter(cacheCtx);
    return await limiter.run("do:get-head-and-refs", () => stub.getHeadAndRefs());
  } catch (e) {
    logger.debug("getHeadAndRefs:error", { error: String(e) });
    return { head: undefined, refs: [] };
  }
}

export async function resolveRef(
  env: Env,
  repoId: string,
  refOrOid: string,
  cacheCtx?: CacheContext
): Promise<string | undefined> {
  if (/^[0-9a-f]{40}$/i.test(refOrOid)) return refOrOid.toLowerCase();
  const { head, refs } = await getHeadAndRefs(env, repoId, cacheCtx);
  if (refOrOid === "HEAD" && head?.target) {
    const r = refs.find((x) => x.name === head.target);
    return r?.oid;
  }
  if (refOrOid.startsWith("refs/")) {
    const r = refs.find((x) => x.name === refOrOid);
    return r?.oid;
  }
  // Try branches first, then tags
  const candidates = [`refs/heads/${refOrOid}`, `refs/tags/${refOrOid}`];
  for (const name of candidates) {
    const r = refs.find((x) => x.name === name);
    if (r) return r.oid;
  }
  return undefined;
}
