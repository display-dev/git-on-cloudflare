import { applyD1Migrations } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { beforeAll, beforeEach, describe, expect, it } from "vitest";

import { createDb } from "@/worker/db/d1/client";
import {
  findRepositoryByDoName,
  insertMembershipIfMissing,
  insertUserIfNew,
} from "@/worker/db/d1/dal";
import { claimNamespace } from "@/worker/db/d1/dal/namespaces";

import { readAppD1Migrations } from "./util/d1Migrations";
import { runQueueMessage } from "./util/queue";

beforeAll(async () => {
  await applyD1Migrations(env.DB, readAppD1Migrations());
});

const USER_ID = "user-bf";
const NAMESPACE_ID = "ns-bf";
const NAMESPACE_SLUG = "bfowner";
const PREFIX = `owner:${NAMESPACE_SLUG}:`;

beforeEach(async () => {
  const db = createDb(env.DB);
  const now = Date.now();
  await insertUserIfNew(db, { id: USER_ID, tesseraSub: "sub-bf", createdAt: now });
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
  // Best-effort wipe; KV in miniflare is per-test by default but we keep it
  // explicit so failed tests do not leak across runs.
  for (const repo of ["repo-a", "repo-b", "repo-c"]) {
    await env.OWNER_REGISTRY.delete(`${PREFIX}${repo}`);
    await env.ROUTES.delete(`repo-route:v1:${NAMESPACE_SLUG}/${repo}`);
  }
});

async function seedLegacyKeys(repos: string[]) {
  for (const repo of repos) {
    await env.OWNER_REGISTRY.put(`${PREFIX}${repo}`, "1");
  }
}

async function getRouteRecord(repo: string) {
  const raw = await env.ROUTES.get(`repo-route:v1:${NAMESPACE_SLUG}/${repo}`);
  return raw ? (JSON.parse(raw) as Record<string, unknown>) : null;
}

describe("legacy backfill queue handler", () => {
  it("acks an empty owner-registry page", async () => {
    const result = await runQueueMessage({
      kind: "legacy-backfill",
      userId: USER_ID,
      namespaceSlug: NAMESPACE_SLUG,
    });
    expect(result.acked).toBe(true);
    expect(result.retried).toBe(false);
  });

  it("upserts D1 + ROUTES for each legacy repo", async () => {
    await seedLegacyKeys(["repo-a", "repo-b"]);
    const result = await runQueueMessage({
      kind: "legacy-backfill",
      userId: USER_ID,
      namespaceSlug: NAMESPACE_SLUG,
    });
    expect(result.acked).toBe(true);
    expect(result.retried).toBe(false);
    const db = createDb(env.DB);
    const repoA = await findRepositoryByDoName(db, `${NAMESPACE_SLUG}/repo-a`);
    const repoB = await findRepositoryByDoName(db, `${NAMESPACE_SLUG}/repo-b`);
    expect(repoA?.namespaceId).toBe(NAMESPACE_ID);
    expect(repoB?.visibility).toBe("public");
    const routeA = await getRouteRecord("repo-a");
    expect(routeA?.repositoryId).toBe(repoA!.id);
    expect(routeA?.namespaceId).toBe(NAMESPACE_ID);
    expect(routeA?.doName).toBe(`${NAMESPACE_SLUG}/repo-a`);
    expect(typeof routeA?.updatedAt).toBe("number");
    expect("visibility" in (routeA ?? {})).toBe(false);
  });

  it("is idempotent under replay", async () => {
    await seedLegacyKeys(["repo-c"]);
    const first = await runQueueMessage({
      kind: "legacy-backfill",
      userId: USER_ID,
      namespaceSlug: NAMESPACE_SLUG,
    });
    expect(first.acked).toBe(true);
    const db = createDb(env.DB);
    const before = await findRepositoryByDoName(db, `${NAMESPACE_SLUG}/repo-c`);
    expect(before).toBeDefined();
    const second = await runQueueMessage({
      kind: "legacy-backfill",
      userId: USER_ID,
      namespaceSlug: NAMESPACE_SLUG,
    });
    expect(second.acked).toBe(true);
    expect(second.retried).toBe(false);
    const after = await findRepositoryByDoName(db, `${NAMESPACE_SLUG}/repo-c`);
    expect(after?.id).toBe(before?.id);
  });

  it("retries (does not ack) when ROUTES.put fails after D1 upsert", async () => {
    await seedLegacyKeys(["repo-a"]);
    const overrideEnv: Env = {
      ...env,
      ROUTES: {
        ...env.ROUTES,
        async put() {
          throw new Error("kv-down");
        },
      },
    } as Env;
    const result = await runQueueMessage(
      { kind: "legacy-backfill", userId: USER_ID, namespaceSlug: NAMESPACE_SLUG },
      overrideEnv
    );
    expect(result.retried).toBe(true);
    expect(result.acked).toBe(false);
    // D1 upsert persists, so the next replay is fast (idempotent).
    const db = createDb(env.DB);
    expect(await findRepositoryByDoName(db, `${NAMESPACE_SLUG}/repo-a`)).toBeDefined();
  });

  it("skips invalid/reserved legacy suffixes without seeding D1 or ROUTES", async () => {
    // Mix valid + invalid + reserved suffixes in the same namespace.
    const validRepo = "valid-one";
    await env.OWNER_REGISTRY.put(`${PREFIX}${validRepo}`, "1");
    await env.OWNER_REGISTRY.put(`${PREFIX}MixedCase`, "1"); // legacy permissive policy allowed it
    await env.OWNER_REGISTRY.put(`${PREFIX}has.dot`, "1");
    await env.OWNER_REGISTRY.put(`${PREFIX}auth`, "1"); // reserved
    try {
      const result = await runQueueMessage({
        kind: "legacy-backfill",
        userId: USER_ID,
        namespaceSlug: NAMESPACE_SLUG,
      });
      expect(result.acked).toBe(true);
      expect(result.retried).toBe(false);
      const db = createDb(env.DB);
      // Only the valid suffix becomes authoritative.
      expect(await findRepositoryByDoName(db, `${NAMESPACE_SLUG}/${validRepo}`)).toBeDefined();
      expect(await findRepositoryByDoName(db, `${NAMESPACE_SLUG}/MixedCase`)).toBeUndefined();
      expect(await findRepositoryByDoName(db, `${NAMESPACE_SLUG}/has.dot`)).toBeUndefined();
      expect(await findRepositoryByDoName(db, `${NAMESPACE_SLUG}/auth`)).toBeUndefined();
      // Route cache reflects the same restriction.
      expect(await getRouteRecord(validRepo)).not.toBeNull();
      expect(await getRouteRecord("MixedCase")).toBeNull();
      expect(await getRouteRecord("has.dot")).toBeNull();
      expect(await getRouteRecord("auth")).toBeNull();
    } finally {
      await env.OWNER_REGISTRY.delete(`${PREFIX}${validRepo}`);
      await env.OWNER_REGISTRY.delete(`${PREFIX}MixedCase`);
      await env.OWNER_REGISTRY.delete(`${PREFIX}has.dot`);
      await env.OWNER_REGISTRY.delete(`${PREFIX}auth`);
      await env.ROUTES.delete(`repo-route:v1:${NAMESPACE_SLUG}/${validRepo}`);
    }
  });

  it("retries when continuation enqueue fails", async () => {
    await seedLegacyKeys(["repo-a", "repo-b", "repo-c"]);
    // Force the KV list to claim incomplete with a cursor by stubbing list.
    const overrideEnv: Env = {
      ...env,
      OWNER_REGISTRY: {
        ...env.OWNER_REGISTRY,
        async list(_options: KVNamespaceListOptions) {
          return {
            keys: [{ name: `${PREFIX}repo-a` }],
            list_complete: false,
            cursor: "next",
          } as KVNamespaceListResult<unknown, string>;
        },
      },
      REPO_MAINT_QUEUE: {
        ...env.REPO_MAINT_QUEUE,
        async send() {
          throw new Error("queue-down");
        },
      },
    } as Env;
    const result = await runQueueMessage(
      { kind: "legacy-backfill", userId: USER_ID, namespaceSlug: NAMESPACE_SLUG },
      overrideEnv
    );
    expect(result.retried).toBe(true);
    expect(result.acked).toBe(false);
    // The page that succeeded is durable.
    const db = createDb(env.DB);
    expect(await findRepositoryByDoName(db, `${NAMESPACE_SLUG}/repo-a`)).toBeDefined();
  });
});
