import type { ServeUploadPackPlan } from "./types";
import type { RewriteOptions } from "@/worker/git/pack/rewrite/shared";

import { rewritePack, rewritePackResult, type PackRewriteResult } from "@/worker/git/pack/rewrite";

export async function resolvePackStreamResult(
  env: Env,
  plan: ServeUploadPackPlan,
  options?: RewriteOptions
): Promise<PackRewriteResult> {
  return await rewritePackResult(env, plan.snapshot, plan.neededOids, options);
}

export async function resolvePackStream(
  env: Env,
  plan: ServeUploadPackPlan,
  options?: RewriteOptions
): Promise<ReadableStream<Uint8Array> | undefined> {
  return await rewritePack(env, plan.snapshot, plan.neededOids, options);
}
