import { applyD1Migrations } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { beforeAll, describe, expect, it } from "vitest";

import { createDb } from "@/worker/db/d1/client";
import {
  claimNamespace,
  findNamespaceBySlug,
  findPatByPrefix,
  findRepositoryByDoName,
  findUserByTesseraSub,
  insertMembershipIfMissing,
  insertPatWithGrants,
  insertRepositoryIfNew,
  insertUserIfNew,
  listNamespacesForUser,
  listPatsForUser,
  listRepositoriesForNamespace,
  listRepositoriesForUser,
  revokePatById,
} from "@/worker/db/d1/dal";

import { readAppD1Migrations } from "./util/d1Migrations";

beforeAll(async () => {
  await applyD1Migrations(env.DB, readAppD1Migrations());
});

describe("D1 schema + DAL round-trips", () => {
  it("inserts a user and reads it back by tessera_sub", async () => {
    const db = createDb(env.DB);
    const now = Date.now();
    const inserted = await insertUserIfNew(db, {
      id: "user-1",
      tesseraSub: "sub-1",
      createdAt: now,
    });
    expect(inserted?.id).toBe("user-1");
    const found = await findUserByTesseraSub(db, "sub-1");
    expect(found?.id).toBe("user-1");
    // Insert with same `tessera_sub` is a no-op (returns undefined).
    const dup = await insertUserIfNew(db, {
      id: "user-1-dup",
      tesseraSub: "sub-1",
      createdAt: now + 1,
    });
    expect(dup).toBeUndefined();
  });

  it("claims a namespace then refuses a duplicate slug", async () => {
    const db = createDb(env.DB);
    const now = Date.now();
    await insertUserIfNew(db, { id: "user-ns", tesseraSub: "sub-ns", createdAt: now });
    const claimed = await claimNamespace(db, {
      id: "ns-1",
      slug: "alice",
      createdBy: "user-ns",
      createdAt: now,
    });
    expect(claimed?.id).toBe("ns-1");
    const taken = await claimNamespace(db, {
      id: "ns-1-dup",
      slug: "alice",
      createdBy: "user-ns",
      createdAt: now,
    });
    expect(taken).toBeUndefined();
    expect((await findNamespaceBySlug(db, "alice"))?.id).toBe("ns-1");
  });

  it("inserts membership, lists namespaces for user, then repository listing", async () => {
    const db = createDb(env.DB);
    const now = Date.now();
    await insertUserIfNew(db, { id: "user-r", tesseraSub: "sub-r", createdAt: now });
    await claimNamespace(db, {
      id: "ns-r",
      slug: "rachel",
      createdBy: "user-r",
      createdAt: now,
    });
    await insertMembershipIfMissing(db, {
      namespaceId: "ns-r",
      userId: "user-r",
      createdAt: now,
    });
    expect((await listNamespacesForUser(db, "user-r")).map((n) => n.slug)).toEqual(["rachel"]);
    const created = await insertRepositoryIfNew(db, {
      id: "repo-1",
      namespaceId: "ns-r",
      createdBy: "user-r",
      slug: "site",
      doName: "rachel/site",
      visibility: "public",
      createdAt: now,
      updatedAt: now,
    });
    expect(created?.id).toBe("repo-1");
    // Replay: ON CONFLICT DO NOTHING returns undefined.
    const replay = await insertRepositoryIfNew(db, {
      id: "repo-1-dup",
      namespaceId: "ns-r",
      createdBy: "user-r",
      slug: "site",
      doName: "rachel/site",
      visibility: "public",
      createdAt: now + 1,
      updatedAt: now + 1,
    });
    expect(replay).toBeUndefined();
    expect((await findRepositoryByDoName(db, "rachel/site"))?.id).toBe("repo-1");
    const createdLater = await insertRepositoryIfNew(db, {
      id: "repo-2",
      namespaceId: "ns-r",
      createdBy: "user-r",
      slug: "api",
      doName: "rachel/api",
      visibility: "public",
      createdAt: now + 2,
      updatedAt: now + 2,
    });
    expect(createdLater?.id).toBe("repo-2");
    const list = await listRepositoriesForUser(db, "user-r");
    expect(list.map((entry) => entry.repository.slug)).toEqual(["api", "site"]);
    expect(list[0]?.namespace.slug).toBe("rachel");
    const namespaceList = await listRepositoriesForNamespace(db, "ns-r", null);
    expect(namespaceList.map((entry) => entry.slug)).toEqual(["api", "site"]);
  });

  it("inserts a PAT with grants and revokes it via the result-union DAL", async () => {
    const db = createDb(env.DB);
    const now = Date.now();
    await insertUserIfNew(db, { id: "user-p", tesseraSub: "sub-p", createdAt: now });
    await claimNamespace(db, {
      id: "ns-p",
      slug: "patowner",
      createdBy: "user-p",
      createdAt: now,
    });
    await insertMembershipIfMissing(db, {
      namespaceId: "ns-p",
      userId: "user-p",
      createdAt: now,
    });
    await insertPatWithGrants(db, {
      pat: {
        id: "pat-1",
        userId: "user-p",
        name: "ci",
        prefix: "goc_aaaaaaaa",
        hash: "hash-aaaaaaaa",
        createdAt: now,
        expiresAt: null,
        revokedAt: null,
        lastUsedAt: null,
      },
      namespaceGrants: [{ patId: "pat-1", namespaceId: "ns-p", level: "pull" }],
      repoGrants: [],
    });
    expect((await findPatByPrefix(db, "goc_aaaaaaaa"))?.id).toBe("pat-1");
    expect((await listPatsForUser(db, "user-p")).map((row) => row.id)).toEqual(["pat-1"]);
    // Cross-user revoke fails closed.
    expect(await revokePatById(db, "pat-1", "user-r", now + 1)).toEqual({
      ok: false,
      reason: "not-owner",
    });
    expect(await revokePatById(db, "pat-1", "user-p", now + 2)).toEqual({ ok: true });
    // Re-revoke is reported as already-revoked.
    expect(await revokePatById(db, "pat-1", "user-p", now + 3)).toEqual({
      ok: false,
      reason: "already-revoked",
    });
  });
});
