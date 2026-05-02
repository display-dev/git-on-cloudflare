import { createLogger } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import {
  findNamespaceById,
  findNamespaceBySlug,
  findRepositoryById,
  findRepositoryByNamespaceAndSlug,
  getRouteCacheRecord,
} from "@/worker/db/d1/dal";

// Resolver for `(:owner, :repo)` URL paths backed by the `ROUTES` KV cache
// and the global `repositories` D1 table.
//
// Invariant: a route resolves only when D1 confirms the requested namespace
// slug, repo slug, namespace id, repository id, and DO name all describe
// the same repository. KV is a candidate cache; a stale entry from before
// a rename or visibility flip must never authorize itself.
//
// Logging strategy: every branch logs at debug because this fires on every
// :owner/:repo request. Production (LOG_LEVEL=info) stays quiet; bumping to
// debug for an investigation immediately surfaces stale-cache patterns.

export type RepositoryRoute = {
  routeNamespaceSlug: string;
  routeRepoSlug: string;
  namespaceId: string;
  repositoryId: string;
  doName: string;
  visibility: "public" | "private";
  source: "kv" | "d1";
};

export async function resolveRepositoryRoute(
  env: Env,
  namespaceSlug: string,
  repoSlug: string
): Promise<RepositoryRoute | null> {
  const log = createLogger(env.LOG_LEVEL, { service: "RepoRoute" });
  const db = createDb(env.DB);
  const cached = await getRouteCacheRecord(env, namespaceSlug, repoSlug);
  if (cached) {
    const repository = await findRepositoryById(db, cached.repositoryId);
    // Every cached field must still match the canonical D1 row, AND the
    // canonical row must still be addressable at the requested URL.
    // Otherwise we fall through to the slug-based D1 lookup and let the
    // caller decide whether to refresh KV.
    if (
      repository &&
      repository.namespaceId === cached.namespaceId &&
      repository.doName === cached.doName &&
      repository.slug === repoSlug
    ) {
      const namespace = await findNamespaceById(db, repository.namespaceId);
      if (namespace && namespace.slug === namespaceSlug) {
        log.debug("route:resolve-kv-hit", {
          namespaceSlug,
          repoSlug,
          repositoryId: repository.id,
        });
        return {
          routeNamespaceSlug: namespaceSlug,
          routeRepoSlug: repoSlug,
          namespaceId: repository.namespaceId,
          repositoryId: repository.id,
          doName: repository.doName,
          visibility: repository.visibility as RepositoryRoute["visibility"],
          source: "kv",
        };
      }
      log.debug("route:resolve-kv-stale-namespace-mismatch", {
        namespaceSlug,
        repoSlug,
        repositoryId: repository.id,
        cachedNamespaceId: cached.namespaceId,
      });
    } else {
      log.debug("route:resolve-kv-stale-repo-mismatch", {
        namespaceSlug,
        repoSlug,
        cachedRepositoryId: cached.repositoryId,
        cachedDoName: cached.doName,
      });
    }
  }
  const namespace = await findNamespaceBySlug(db, namespaceSlug);
  if (!namespace) {
    log.debug("route:resolve-namespace-not-found", { namespaceSlug, repoSlug });
    return null;
  }
  const repository = await findRepositoryByNamespaceAndSlug(db, namespace.id, repoSlug);
  if (!repository) {
    log.debug("route:resolve-repo-not-found", {
      namespaceSlug,
      repoSlug,
      namespaceId: namespace.id,
    });
    return null;
  }
  log.debug("route:resolve-d1-hit", {
    namespaceSlug,
    repoSlug,
    repositoryId: repository.id,
  });
  return {
    routeNamespaceSlug: namespaceSlug,
    routeRepoSlug: repoSlug,
    namespaceId: namespace.id,
    repositoryId: repository.id,
    doName: repository.doName,
    visibility: repository.visibility as RepositoryRoute["visibility"],
    source: "d1",
  };
}
