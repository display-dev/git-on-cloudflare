import { createLogger, type Logger } from "@/worker/common";
import { createDb, type Db } from "@/worker/db/d1/client";
import type { RepositoryVisibility } from "@/worker/db/d1/schema";
import {
  findNamespaceById,
  findNamespaceBySlug,
  findRepositoryById,
  findRepositoryByNamespaceAndSlug,
} from "@/worker/db/d1/dal";
import { getRouteCacheRecord } from "./routeCache";

// A route resolves only when D1 confirms the namespace slug, repo slug,
// namespace id, repository id, and `doName` all describe the same row. KV
// is a candidate cache; a stale entry from before a rename or visibility
// flip must never authorize itself.

export type RepositoryRoute = {
  routeNamespaceSlug: string;
  routeRepoSlug: string;
  namespaceId: string;
  repositoryId: string;
  doName: string;
  visibility: RepositoryVisibility;
  source: "kv" | "d1";
};

export type RepositoryRouteResolutionMode = "route-cache-only" | "allow-d1-fallback";

export type RepositoryRouteResolutionOptions = {
  // Anonymous repository serving uses only the ROUTES candidate cache so a
  // scanner cannot force namespace/repo D1 lookups by walking arbitrary URLs.
  // Authenticated browser and PAT paths can fall back to D1 because they
  // already carry a principal that can be checked before data is served.
  mode: RepositoryRouteResolutionMode;
  db?: Db;
  log?: Logger;
};

export async function resolveRepositoryRoute(
  env: Env,
  namespaceSlug: string,
  repoSlug: string,
  options: RepositoryRouteResolutionOptions = { mode: "allow-d1-fallback" }
): Promise<RepositoryRoute | null> {
  const log = options.log ?? createLogger(env.LOG_LEVEL, { service: "RepoRoute" });
  const db = options.db ?? createDb(env.DB);
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
          visibility: repository.visibility,
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
  if (options.mode === "route-cache-only") {
    log.debug("route:resolve-cache-only-miss", { namespaceSlug, repoSlug });
    return null;
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
    visibility: repository.visibility,
    source: "d1",
  };
}
