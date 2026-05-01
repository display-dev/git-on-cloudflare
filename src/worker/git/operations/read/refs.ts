import type { HeadInfo, Ref } from "../types";
import { getRepoStub, createLogger } from "@/worker/common";

export async function getHeadAndRefs(
  env: Env,
  repoId: string
): Promise<{ head: HeadInfo | undefined; refs: Ref[] }> {
  const stub = getRepoStub(env, repoId);
  const logger = createLogger(env.LOG_LEVEL, { service: "getHeadAndRefs", repoId });
  try {
    return await stub.getHeadAndRefs();
  } catch (e) {
    logger.debug("getHeadAndRefs:error", { error: String(e) });
    return { head: undefined, refs: [] };
  }
}

export async function resolveRef(
  env: Env,
  repoId: string,
  refOrOid: string
): Promise<string | undefined> {
  if (/^[0-9a-f]{40}$/i.test(refOrOid)) return refOrOid.toLowerCase();
  const { head, refs } = await getHeadAndRefs(env, repoId);
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
