import { beforeAll, describe, expect, it } from "vitest";
import { env } from "cloudflare:workers";

import { createDb } from "@/worker/db/d1/client";
import { deleteRepositoryById, updateRepositoryVisibility } from "@/worker/db/d1/dal";
import { putRouteCacheRecord, routeCacheKey } from "@/worker/repositories/routeCache";

import { ensureD1Migrations } from "./util/d1Setup";
import { seedRepo, type SeededRepo } from "./util/repoSeed";
import { runQueueMessage } from "./util/queue";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

function uniqueNs(): string {
  return `rcs-${Math.random().toString(36).slice(2, 8)}`;
}

function syncMessage(
  s: SeededRepo,
  override?: Partial<{ namespaceSlug: string; repoSlug: string }>
) {
  return {
    kind: "route-cache-sync" as const,
    repositoryId: s.repositoryId,
    namespaceSlug: override?.namespaceSlug ?? s.namespaceSlug,
    repoSlug: override?.repoSlug ?? s.repoSlug,
    enqueuedAt: Date.now(),
  };
}

describe("route-cache-sync consumer", () => {
  it("D1 public + KV absent -> writes canonical record", async () => {
    const s = await seedRepo(env, {
      namespaceSlug: uniqueNs(),
      repoSlug: "site",
      visibility: "public",
      skipRouteCache: true,
    });
    expect(await env.ROUTES.get(s.routeCacheKey)).toBeNull();
    const result = await runQueueMessage(syncMessage(s));
    expect(result.acked).toBe(true);
    const raw = await env.ROUTES.get(s.routeCacheKey);
    expect(raw).not.toBeNull();
    const record = JSON.parse(raw!) as {
      repositoryId: string;
      namespaceId: string;
      doName: string;
    };
    expect(record.repositoryId).toBe(s.repositoryId);
    expect(record.namespaceId).toBe(s.namespaceId);
    expect(record.doName).toBe(s.doName);
  });

  it("D1 private + KV present -> deletes canonical key", async () => {
    const s = await seedRepo(env, {
      namespaceSlug: uniqueNs(),
      repoSlug: "site",
      visibility: "private",
    });
    await putRouteCacheRecord(env, s.namespaceSlug, s.repoSlug, {
      repositoryId: s.repositoryId,
      namespaceId: s.namespaceId,
      doName: s.doName,
      updatedAt: Date.now(),
    });
    expect(await env.ROUTES.get(s.routeCacheKey)).not.toBeNull();
    const result = await runQueueMessage(syncMessage(s));
    expect(result.acked).toBe(true);
    expect(await env.ROUTES.get(s.routeCacheKey)).toBeNull();
  });

  it("D1 row missing -> deletes captured key", async () => {
    const s = await seedRepo(env, {
      namespaceSlug: uniqueNs(),
      repoSlug: "site",
      visibility: "public",
    });
    const db = createDb(env.DB);
    await deleteRepositoryById(db, s.repositoryId);
    const result = await runQueueMessage(syncMessage(s));
    expect(result.acked).toBe(true);
    expect(await env.ROUTES.get(s.routeCacheKey)).toBeNull();
  });

  it("private -> public flip: re-running sync repopulates ROUTES", async () => {
    const s = await seedRepo(env, {
      namespaceSlug: uniqueNs(),
      repoSlug: "site",
      visibility: "public",
      skipRouteCache: true,
    });
    await runQueueMessage(syncMessage(s));
    expect(await env.ROUTES.get(s.routeCacheKey)).not.toBeNull();
    const db = createDb(env.DB);
    await updateRepositoryVisibility(db, s.repositoryId, "private", Date.now());
    await runQueueMessage(syncMessage(s));
    expect(await env.ROUTES.get(s.routeCacheKey)).toBeNull();
    await updateRepositoryVisibility(db, s.repositoryId, "public", Date.now());
    await runQueueMessage(syncMessage(s));
    expect(await env.ROUTES.get(s.routeCacheKey)).not.toBeNull();
  });

  it("stale captured key + canonical mismatch: deletes captured, puts canonical", async () => {
    const s = await seedRepo(env, {
      namespaceSlug: uniqueNs(),
      repoSlug: "site",
      visibility: "public",
      skipRouteCache: true,
    });
    const staleNamespaceSlug = `${s.namespaceSlug}-stale`;
    await putRouteCacheRecord(env, staleNamespaceSlug, s.repoSlug, {
      repositoryId: s.repositoryId,
      namespaceId: s.namespaceId,
      doName: s.doName,
      updatedAt: Date.now(),
    });
    const result = await runQueueMessage(syncMessage(s, { namespaceSlug: staleNamespaceSlug }));
    expect(result.acked).toBe(true);
    expect(await env.ROUTES.get(routeCacheKey(staleNamespaceSlug, s.repoSlug))).toBeNull();
    expect(await env.ROUTES.get(s.routeCacheKey)).not.toBeNull();
  });

  it("replay: running the same message twice is idempotent", async () => {
    const s = await seedRepo(env, {
      namespaceSlug: uniqueNs(),
      repoSlug: "site",
      visibility: "public",
      skipRouteCache: true,
    });
    const message = syncMessage(s);
    const first = await runQueueMessage(message);
    const second = await runQueueMessage(message);
    expect(first.acked).toBe(true);
    expect(second.acked).toBe(true);
    expect(await env.ROUTES.get(s.routeCacheKey)).not.toBeNull();
  });
});
