import type { TokensIslandSummary } from "@/client/islands/tokens";
import type { AppContext } from "./hono";
import type { RouteCacheSyncMessage } from "@/worker/tasks/queue";

import { listPatGrantsByIds, type PersonalAccessTokenRow } from "@/worker/db/d1/dal";
import type { Db } from "@/worker/db/d1/client";
import type { Logger } from "@/worker/common";

// Best-effort enqueue of a `route-cache-sync` task after a D1 mutation that
// changes ROUTES KV state (repo create, visibility flip, future rename).
// D1 is canonical and the resolver D1 fallback covers the gap until the
// queue drains, so a send failure is logged but does not fail the request.
export function enqueueRouteCacheSync(
  c: AppContext,
  log: Logger,
  payload: { repositoryId: string; namespaceSlug: string; repoSlug: string }
): void {
  const message: RouteCacheSyncMessage = {
    kind: "route-cache-sync",
    repositoryId: payload.repositoryId,
    namespaceSlug: payload.namespaceSlug,
    repoSlug: payload.repoSlug,
    enqueuedAt: Date.now(),
  };
  c.executionCtx.waitUntil(
    c.env.REPO_TASKS_QUEUE.send(message).catch((error) => {
      log.warn("route-cache-sync:enqueue-failed", {
        repositoryId: payload.repositoryId,
        namespaceSlug: payload.namespaceSlug,
        repoSlug: payload.repoSlug,
        error: String(error),
      });
    })
  );
}

// Build the wire-shape summary that the management UI consumes. Grants are
// fetched in two batched queries (one per grant table) and grouped by PAT
// id, so an arbitrary number of tokens still costs the same fixed number
// of round trips.
export async function summarizeTokens(
  db: Db,
  tokens: PersonalAccessTokenRow[]
): Promise<TokensIslandSummary[]> {
  if (tokens.length === 0) return [];
  const ids = tokens.map((row) => row.id);
  const grants = await listPatGrantsByIds(db, ids);
  const namespaceByPatId = new Map<string, TokensIslandSummary["namespaceGrants"]>();
  for (const grant of grants.namespaceGrants) {
    const list = namespaceByPatId.get(grant.patId) ?? [];
    list.push({
      namespaceSlug: grant.namespaceSlug,
      level: grant.level,
    });
    namespaceByPatId.set(grant.patId, list);
  }
  const repoByPatId = new Map<string, TokensIslandSummary["repoGrants"]>();
  for (const grant of grants.repoGrants) {
    const list = repoByPatId.get(grant.patId) ?? [];
    list.push({
      namespaceSlug: grant.namespaceSlug,
      repoSlug: grant.repoSlug,
      level: grant.level,
    });
    repoByPatId.set(grant.patId, list);
  }
  return tokens.map((token) => ({
    id: token.id,
    name: token.name,
    prefix: token.prefix,
    createdAt: token.createdAt,
    expiresAt: token.expiresAt ?? undefined,
    revokedAt: token.revokedAt ?? undefined,
    lastUsedAt: token.lastUsedAt ?? undefined,
    namespaceGrants: namespaceByPatId.get(token.id) ?? [],
    repoGrants: repoByPatId.get(token.id) ?? [],
  }));
}

export function safeRedirect(c: AppContext, url: string, status: 302 | 303 = 302): Response {
  // Hono's c.redirect uses 302 by default. Sign-out uses 303 to force a GET on
  // the target after a same-origin POST.
  return c.redirect(url, status);
}

export function errorRedirect(c: AppContext, code: string): Response {
  return safeRedirect(c, `/auth?error=${encodeURIComponent(code)}`);
}
