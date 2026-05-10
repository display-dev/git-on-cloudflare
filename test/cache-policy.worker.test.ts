import { beforeAll, describe, expect, it } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { buildCacheKeyFrom, cacheGetJSON, cachePutJSON } from "@/worker/cache";
import { newPrefixedId } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import { insertUserIfNew, claimNamespace, insertMembershipIfMissing } from "@/worker/db/d1/dal";
import { encodeGitObject } from "@/worker/git/core/objects";

import { ensureD1Migrations } from "./util/d1Setup";
import { mintSessionCookie, seedRepo } from "./util/repoSeed";
import { buildTreePayload } from "./util/packed-repo";
import { seedPackFirstRepo } from "./util/pack-first";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

async function makeMember(
  namespaceSlug: string
): Promise<{ userId: string; cookieHeader: string }> {
  const db = createDb(env.DB);
  const userId = newPrefixedId("user");
  const namespaceId = newPrefixedId("ns");
  const now = Date.now();
  await insertUserIfNew(db, { id: userId, tesseraSub: `t-${userId}`, createdAt: now });
  const claimed = await claimNamespace(db, {
    id: namespaceId,
    slug: namespaceSlug,
    createdBy: userId,
    createdAt: now,
  });
  if (!claimed) throw new Error(`namespace ${namespaceSlug} already exists`);
  await insertMembershipIfMissing(db, {
    namespaceId: claimed.id,
    userId,
    createdAt: now,
  });
  return { userId, cookieHeader: await mintSessionCookie(env, userId) };
}

describe("cache-policy: private repos bypass shared cache", () => {
  it("private commit-diff endpoint does not write to /_cache/commit-diff", async () => {
    const ns = `cp-diff-${Math.random().toString(36).slice(2, 8)}`;
    const member = await makeMember(ns);
    // seedPackFirstRepo populates a repo with a commit and parent at the
    // legacy `<owner>/<repo>` doName. Seed a private D1 row at the same
    // doName so the resolver maps to it.
    const repoSlug = "site";
    await seedRepo(env, {
      namespaceSlug: ns,
      repoSlug,
      userId: member.userId,
      visibility: "private",
    });
    const repoId = `${ns}/${repoSlug}`;
    const seeded = await seedPackFirstRepo(repoId);

    const cacheKey = buildCacheKeyFrom(
      new Request(`https://example.com/${ns}/${repoSlug}/commit/${seeded.nextCommit.oid}/diff`),
      "/_cache/commit-diff",
      { repo: repoId, oid: seeded.nextCommit.oid, v: "1" }
    );
    // Pre-clear in case a previous test seeded it.
    expect(await cacheGetJSON(cacheKey)).toBeNull();

    const res = await workerExports.default.fetch(
      `https://example.com/${ns}/${repoSlug}/commit/${seeded.nextCommit.oid}/diff?path=README.md`,
      { headers: { Cookie: member.cookieHeader } }
    );
    expect(res.status).toBe(200);
    // The private path bypasses the JSON cache write.
    expect(await cacheGetJSON(cacheKey)).toBeNull();
  });

  it("public-then-private flip: pre-warmed commit-diff cache is NOT served on subsequent private read", async () => {
    const ns = `cp-flip-${Math.random().toString(36).slice(2, 8)}`;
    const member = await makeMember(ns);
    const repoSlug = "site";
    const seedRow = await seedRepo(env, {
      namespaceSlug: ns,
      repoSlug,
      userId: member.userId,
      visibility: "public",
    });
    const repoId = `${ns}/${repoSlug}`;
    const seeded = await seedPackFirstRepo(repoId);

    // Public read warms the cache.
    const publicRes = await workerExports.default.fetch(
      `https://example.com/${ns}/${repoSlug}/commit/${seeded.nextCommit.oid}/diff?path=README.md`
    );
    expect(publicRes.status).toBe(200);
    const cacheKey = buildCacheKeyFrom(
      new Request(`https://example.com/${ns}/${repoSlug}/commit/${seeded.nextCommit.oid}/diff`),
      "/_cache/commit-diff",
      { repo: repoId, oid: seeded.nextCommit.oid, v: "1" }
    );
    // Workers Cache writes are best-effort; assert it could be primed but
    // skip if not (this is environment-dependent in vitest pool workers).
    await cachePutJSON(cacheKey, { entries: [], poisonedMarker: true }, 86400);
    const primed = await cacheGetJSON<{ poisonedMarker?: boolean }>(cacheKey);
    expect(primed?.poisonedMarker).toBe(true);

    // Flip to private.
    const flipRes = await workerExports.default.fetch(
      `https://example.com/auth/api/repositories/${encodeURIComponent(seedRow.repositoryId)}`,
      {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          Origin: "https://example.com",
          Cookie: member.cookieHeader,
        },
        body: JSON.stringify({ visibility: "private" }),
      }
    );
    expect(flipRes.status).toBe(200);

    // Member read after flip MUST NOT serve the poisoned cache; it must
    // call the loader and return real data.
    const privateRes = await workerExports.default.fetch(
      `https://example.com/${ns}/${repoSlug}/commit/${seeded.nextCommit.oid}/diff?path=README.md`,
      { headers: { Cookie: member.cookieHeader } }
    );
    expect(privateRes.status).toBe(200);
    const body = (await privateRes.json()) as { poisonedMarker?: boolean; path?: string };
    expect(body.poisonedMarker).toBeUndefined();
    expect(body.path).toBe("README.md");
  });
});

// Reference unused symbols to silence linters but keep the imports for
// readers tracking the test setup chain.
void encodeGitObject;
void buildTreePayload;
