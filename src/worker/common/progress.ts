import type { CacheContext } from "@/worker/cache";
import { getLimiter } from "@/worker/git/operations/limits";

import { getRepoStub } from "./stub";

export interface RepoActivity {
  state: "receiving" | "compacting";
  startedAt?: number;
  expiresAt?: number;
}

/**
 * Fetch repository activity for banner rendering.
 * Idle repos return null so callers can keep the existing "render nothing"
 * behavior without interpreting state as correctness data.
 *
 * Pass `cacheCtx` so the DO RPC shares the per-request subrequest limiter.
 */
export async function getRepoActivity(
  env: Env,
  repoId: string,
  cacheCtx?: CacheContext
): Promise<RepoActivity | null> {
  try {
    const stub = getRepoStub(env, repoId);
    const limiter = getLimiter(cacheCtx);
    return await limiter.run("do:get-repo-activity", () => stub.getRepoActivity());
  } catch {
    return null;
  }
}
