import { applyD1Migrations } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { beforeAll, beforeEach, describe, expect, it } from "vitest";

import { createDb } from "@/worker/db/d1/client";
import {
  insertMembershipIfMissing,
  insertRepositoryIfNew,
  insertUserIfNew,
} from "@/worker/db/d1/dal";
import { putRouteCacheRecord, routeCacheKey } from "@/worker/repositories/routeCache";
import { claimNamespace } from "@/worker/db/d1/dal/namespaces";
import { repositories } from "@/worker/db/d1/schema";
import { resolveRepositoryRoute } from "@/worker/repositories/route";
import { eq } from "drizzle-orm";

import { readAppD1Migrations } from "./util/d1Migrations";

beforeAll(async () => {
  await applyD1Migrations(env.DB, readAppD1Migrations());
});

const USER_ID = "user-rr";
const NAMESPACE_ID = "ns-rr";
const NAMESPACE_SLUG = "rr-rachel";
const REPO_ID = "repo-rr";
const REPO_SLUG = "site";
const DO_NAME = "repo:uuid-rr";

beforeEach(async () => {
  const db = createDb(env.DB);
  const now = Date.now();
  await insertUserIfNew(db, { id: USER_ID, tesseraSub: "sub-rr", createdAt: now });
  await claimNamespace(db, {
    id: NAMESPACE_ID,
    slug: NAMESPACE_SLUG,
    createdBy: USER_ID,
    createdAt: now,
  });
  await insertMembershipIfMissing(db, {
    namespaceId: NAMESPACE_ID,
    userId: USER_ID,
    createdAt: now,
  });
  await insertRepositoryIfNew(db, {
    id: REPO_ID,
    namespaceId: NAMESPACE_ID,
    createdBy: USER_ID,
    slug: REPO_SLUG,
    doName: DO_NAME,
    visibility: "public",
    createdAt: now,
    updatedAt: now,
  });
  // Reset known cache keys.
  for (const slug of [REPO_SLUG, "renamed", "ghost"]) {
    await env.ROUTES.delete(routeCacheKey(NAMESPACE_SLUG, slug));
  }
});

describe("resolveRepositoryRoute", () => {
  it("returns the route via KV when cached metadata matches D1", async () => {
    await putRouteCacheRecord(env, NAMESPACE_SLUG, REPO_SLUG, {
      repositoryId: REPO_ID,
      namespaceId: NAMESPACE_ID,
      doName: DO_NAME,
      updatedAt: Date.now(),
    });
    const route = await resolveRepositoryRoute(env, NAMESPACE_SLUG, REPO_SLUG);
    expect(route).toMatchObject({
      routeNamespaceSlug: NAMESPACE_SLUG,
      routeRepoSlug: REPO_SLUG,
      namespaceId: NAMESPACE_ID,
      repositoryId: REPO_ID,
      doName: DO_NAME,
      visibility: "public",
      source: "kv",
    });
  });

  it("falls through KV when the cached repositoryId is stale", async () => {
    await putRouteCacheRecord(env, NAMESPACE_SLUG, REPO_SLUG, {
      repositoryId: "ghost-id",
      namespaceId: NAMESPACE_ID,
      doName: DO_NAME,
      updatedAt: Date.now(),
    });
    const route = await resolveRepositoryRoute(env, NAMESPACE_SLUG, REPO_SLUG);
    expect(route?.source).toBe("d1");
    expect(route?.repositoryId).toBe(REPO_ID);
  });

  it("falls through KV when the cached doName is stale", async () => {
    await putRouteCacheRecord(env, NAMESPACE_SLUG, REPO_SLUG, {
      repositoryId: REPO_ID,
      namespaceId: NAMESPACE_ID,
      doName: "stale/doName",
      updatedAt: Date.now(),
    });
    const route = await resolveRepositoryRoute(env, NAMESPACE_SLUG, REPO_SLUG);
    expect(route?.source).toBe("d1");
    expect(route?.doName).toBe(DO_NAME);
  });

  it("falls through KV when the repository's current slug differs from the URL", async () => {
    // Old URL key still in KV; the repo has been renamed in D1.
    const db = createDb(env.DB);
    await db
      .update(repositories)
      .set({ slug: "renamed", updatedAt: Date.now() })
      .where(eq(repositories.id, REPO_ID));
    try {
      await putRouteCacheRecord(env, NAMESPACE_SLUG, REPO_SLUG, {
        repositoryId: REPO_ID,
        namespaceId: NAMESPACE_ID,
        doName: DO_NAME,
        updatedAt: Date.now(),
      });
      // Old URL no longer resolves; the resolver must NOT return the renamed repo.
      expect(await resolveRepositoryRoute(env, NAMESPACE_SLUG, REPO_SLUG)).toBeNull();
      // New URL resolves via D1.
      const renamed = await resolveRepositoryRoute(env, NAMESPACE_SLUG, "renamed");
      expect(renamed?.source).toBe("d1");
      expect(renamed?.repositoryId).toBe(REPO_ID);
    } finally {
      // Restore for sibling tests; D1 in this pool is shared across tests.
      await db
        .update(repositories)
        .set({ slug: REPO_SLUG, updatedAt: Date.now() })
        .where(eq(repositories.id, REPO_ID));
    }
  });

  it("falls through KV when the cached namespaceId no longer matches the URL slug", async () => {
    // Stash a bad cached record claiming a different namespaceId.
    await putRouteCacheRecord(env, NAMESPACE_SLUG, REPO_SLUG, {
      repositoryId: REPO_ID,
      namespaceId: "ns-other",
      doName: DO_NAME,
      updatedAt: Date.now(),
    });
    const route = await resolveRepositoryRoute(env, NAMESPACE_SLUG, REPO_SLUG);
    // The slug-based fallback still finds the canonical repo via D1.
    expect(route?.source).toBe("d1");
    expect(route?.namespaceId).toBe(NAMESPACE_ID);
  });

  it("returns null on KV miss + D1 miss", async () => {
    expect(await resolveRepositoryRoute(env, NAMESPACE_SLUG, "ghost")).toBeNull();
  });

  it("returns the route via D1 on KV miss", async () => {
    const route = await resolveRepositoryRoute(env, NAMESPACE_SLUG, REPO_SLUG);
    expect(route?.source).toBe("d1");
    expect(route?.repositoryId).toBe(REPO_ID);
  });

  it("returns null in cache-only mode when KV misses even if D1 has the row", async () => {
    const route = await resolveRepositoryRoute(env, NAMESPACE_SLUG, REPO_SLUG, {
      mode: "route-cache-only",
    });
    expect(route).toBeNull();
  });

  it("returns null in cache-only mode when the cached record is stale", async () => {
    await putRouteCacheRecord(env, NAMESPACE_SLUG, REPO_SLUG, {
      repositoryId: "ghost-id",
      namespaceId: NAMESPACE_ID,
      doName: DO_NAME,
      updatedAt: Date.now(),
    });
    const route = await resolveRepositoryRoute(env, NAMESPACE_SLUG, REPO_SLUG, {
      mode: "route-cache-only",
    });
    expect(route).toBeNull();
  });

  it("returns null when the namespace slug does not exist", async () => {
    expect(await resolveRepositoryRoute(env, "nope", REPO_SLUG)).toBeNull();
  });
});
