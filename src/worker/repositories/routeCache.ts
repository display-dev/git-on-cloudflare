import { createLogger } from "@/worker/common";
import { z } from "zod";

// Workers KV `ROUTES` candidate cache. Backfill and repo-mutation flows
// write here; the resolver reads here as a hint and re-confirms against
// D1. The payload deliberately omits `visibility` because KV is
// eventually consistent and must not be an authorization source.

export const RouteCacheRecordSchema = z.object({
  repositoryId: z.string(),
  namespaceId: z.string(),
  doName: z.string(),
  updatedAt: z.number(),
});

export type RouteCacheRecord = z.infer<typeof RouteCacheRecordSchema>;

export function routeCacheKey(namespaceSlug: string, repoSlug: string): string {
  return `repo-route:v1:${namespaceSlug}/${repoSlug}`;
}

// Writes/deletes log only on success. Failures throw to the caller so
// surrounding handlers can attach their own context (legacyBackfill,
// repo create/rename/delete) - duplicating a generic error log here
// would just bury the operational signal.
export async function putRouteCacheRecord(
  env: Env,
  namespaceSlug: string,
  repoSlug: string,
  record: RouteCacheRecord
): Promise<void> {
  await env.ROUTES.put(routeCacheKey(namespaceSlug, repoSlug), JSON.stringify(record));
  const log = createLogger(env.LOG_LEVEL, { service: "RouteCache" });
  log.debug("routes:put-ok", {
    namespaceSlug,
    repoSlug,
    repositoryId: record.repositoryId,
  });
}

export async function getRouteCacheRecord(
  env: Env,
  namespaceSlug: string,
  repoSlug: string
): Promise<RouteCacheRecord | null> {
  const raw = await env.ROUTES.get(routeCacheKey(namespaceSlug, repoSlug));
  if (!raw) return null;
  try {
    const parsed = RouteCacheRecordSchema.safeParse(JSON.parse(raw));
    if (parsed.success) return parsed.data;
  } catch {}
  return null;
}

export async function deleteRouteCacheRecord(
  env: Env,
  namespaceSlug: string,
  repoSlug: string
): Promise<void> {
  await env.ROUTES.delete(routeCacheKey(namespaceSlug, repoSlug));
  const log = createLogger(env.LOG_LEVEL, { service: "RouteCache" });
  log.debug("routes:delete-ok", { namespaceSlug, repoSlug });
}
