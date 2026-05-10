import { beforeAll, describe, expect, it } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { newPrefixedId } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import { claimNamespace, insertMembershipIfMissing, insertUserIfNew } from "@/worker/db/d1/dal";
import { asTypedStorage, type RepoStateSchema } from "@/worker/do/repo/repoState";

import { ensureD1Migrations } from "./util/d1Setup";
import { mintSessionCookie, setupRepoForTests } from "./util/repoSeed";
import { seedPackFirstRepo } from "./util/pack-first";
import { runDOWithRetry } from "./util/test-helpers";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

async function makeOutsider(): Promise<string> {
  const db = createDb(env.DB);
  const userId = newPrefixedId("user");
  const namespaceId = newPrefixedId("ns");
  const slug = `out-${Math.random().toString(36).slice(2, 8)}`;
  const now = Date.now();
  await insertUserIfNew(db, { id: userId, tesseraSub: `t-${userId}`, createdAt: now });
  const claimed = await claimNamespace(db, {
    id: namespaceId,
    slug,
    createdBy: userId,
    createdAt: now,
  });
  if (!claimed) throw new Error("namespace already exists");
  await insertMembershipIfMissing(db, { namespaceId: claimed.id, userId, createdAt: now });
  return await mintSessionCookie(env, userId);
}

async function setupPublicRepoWithReceiveActivity(owner: string, repo: string) {
  const seededRepo = await setupRepoForTests(env, owner, repo, { visibility: "public" });
  const seededPack = await seedPackFirstRepo(`${owner}/${repo}`);
  await runDOWithRetry(seededPack.getStub, async (_instance, state) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const now = Date.now();
    await store.put("receiveLease", {
      token: `test-receive-lease-${owner}-${repo}`,
      createdAt: now,
      expiresAt: now + 60_000,
    });
  });
  return seededRepo;
}

describe("repo page route-cache disclosure", () => {
  it("anonymous + public repo + missing route cache -> 404", async () => {
    const owner = `dis-route-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupRepoForTests(env, owner, repo, {
      visibility: "public",
      skipRouteCache: true,
    });
    const res = await workerExports.default.fetch(`https://example.com/${owner}/${repo}`, {
      redirect: "manual",
    });
    expect(res.status).toBe(404);
  });

  it("anonymous + public repo + active receive lease -> hides activity banner", async () => {
    const owner = `dis-act-anon-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupPublicRepoWithReceiveActivity(owner, repo);
    const res = await workerExports.default.fetch(`https://example.com/${owner}/${repo}`, {
      redirect: "manual",
    });
    expect(res.status).toBe(200);
    const html = await res.text();
    expect(html).not.toContain("Receiving push...");
  });

  it("signed-in non-member + public repo + active receive lease -> hides activity banner", async () => {
    const owner = `dis-act-out-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupPublicRepoWithReceiveActivity(owner, repo);
    const outsider = await makeOutsider();
    const res = await workerExports.default.fetch(`https://example.com/${owner}/${repo}`, {
      headers: { Cookie: outsider },
      redirect: "manual",
    });
    expect(res.status).toBe(200);
    const html = await res.text();
    expect(html).not.toContain("Receiving push...");
  });

  it("namespace member + public repo + active receive lease -> shows activity banner", async () => {
    const owner = `dis-act-mem-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    const seeded = await setupPublicRepoWithReceiveActivity(owner, repo);
    const res = await workerExports.default.fetch(`https://example.com/${owner}/${repo}`, {
      headers: { Cookie: seeded.cookieHeader },
      redirect: "manual",
    });
    expect(res.status).toBe(200);
    const html = await res.text();
    expect(html).toContain("Receiving push...");
  });
});

describe("admin UI page disclosure", () => {
  it("anonymous + private repo -> 404 (does not redirect to /auth)", async () => {
    const owner = `dis-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupRepoForTests(env, owner, repo, { visibility: "private" });
    const res = await workerExports.default.fetch(`https://example.com/${owner}/${repo}/admin`, {
      redirect: "manual",
    });
    expect(res.status).toBe(404);
  });

  it("anonymous + public repo -> 302 to /auth?next=...", async () => {
    const owner = `dis-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupRepoForTests(env, owner, repo, { visibility: "public" });
    const res = await workerExports.default.fetch(`https://example.com/${owner}/${repo}/admin`, {
      redirect: "manual",
    });
    expect(res.status).toBe(302);
    expect(res.headers.get("Location") ?? "").toContain("/auth?next=");
  });

  it("signed-in non-member + private repo -> 404", async () => {
    const owner = `dis-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupRepoForTests(env, owner, repo, { visibility: "private" });
    const outsider = await makeOutsider();
    const res = await workerExports.default.fetch(`https://example.com/${owner}/${repo}/admin`, {
      headers: { Cookie: outsider },
      redirect: "manual",
    });
    expect(res.status).toBe(404);
  });

  it("signed-in non-member + public repo -> 403", async () => {
    const owner = `dis-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupRepoForTests(env, owner, repo, { visibility: "public" });
    const outsider = await makeOutsider();
    const res = await workerExports.default.fetch(`https://example.com/${owner}/${repo}/admin`, {
      headers: { Cookie: outsider },
      redirect: "manual",
    });
    expect(res.status).toBe(403);
  });

  it("member + private repo -> 200", async () => {
    const owner = `dis-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    const seeded = await setupRepoForTests(env, owner, repo, { visibility: "private" });
    const res = await workerExports.default.fetch(`https://example.com/${owner}/${repo}/admin`, {
      headers: { Cookie: seeded.cookieHeader },
      redirect: "manual",
    });
    expect(res.status).toBe(200);
  });

  it("member + private repo + missing route cache -> 200", async () => {
    const owner = `dis-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    const seeded = await setupRepoForTests(env, owner, repo, {
      visibility: "private",
      skipRouteCache: true,
    });
    const res = await workerExports.default.fetch(`https://example.com/${owner}/${repo}/admin`, {
      headers: { Cookie: seeded.cookieHeader },
      redirect: "manual",
    });
    expect(res.status).toBe(200);
  });
});

describe("admin JSON endpoints disclosure (GET admin/refs as a representative)", () => {
  it("anonymous + private -> 404", async () => {
    const owner = `disj-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupRepoForTests(env, owner, repo, { visibility: "private" });
    const res = await workerExports.default.fetch(
      `https://example.com/${owner}/${repo}/admin/refs`
    );
    expect(res.status).toBe(404);
  });

  it("anonymous + public -> 401", async () => {
    const owner = `disj-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupRepoForTests(env, owner, repo, { visibility: "public" });
    const res = await workerExports.default.fetch(
      `https://example.com/${owner}/${repo}/admin/refs`
    );
    expect(res.status).toBe(401);
  });

  it("signed-in non-member + private -> 404", async () => {
    const owner = `disj-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupRepoForTests(env, owner, repo, { visibility: "private" });
    const outsider = await makeOutsider();
    const res = await workerExports.default.fetch(
      `https://example.com/${owner}/${repo}/admin/refs`,
      { headers: { Cookie: outsider } }
    );
    expect(res.status).toBe(404);
  });

  it("signed-in non-member + public -> 403", async () => {
    const owner = `disj-${Math.random().toString(36).slice(2, 8)}`;
    const repo = "site";
    await setupRepoForTests(env, owner, repo, { visibility: "public" });
    const outsider = await makeOutsider();
    const res = await workerExports.default.fetch(
      `https://example.com/${owner}/${repo}/admin/refs`,
      { headers: { Cookie: outsider } }
    );
    expect(res.status).toBe(403);
  });
});
