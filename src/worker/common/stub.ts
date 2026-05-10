import type { RepoDurableObject } from "@/worker/do";
/**
 * Get the Durable Object stub for a repository.
 * @param env - Cloudflare environment bindings
 * @param repoId - RepoDO storage identity (`doName` in D1)
 * @returns DurableObjectStub for the specified repository
 */
export function getRepoStub(env: Env, repoId: string): DurableObjectStub<RepoDurableObject> {
  const id = env.REPO_DO.idFromName(repoId);
  return env.REPO_DO.get(id);
}

export function getRepoStubByDoId(env: Env, doId: string): DurableObjectStub<RepoDurableObject> {
  const id = env.REPO_DO.idFromString(doId);
  return env.REPO_DO.get(id);
}
