import { beforeAll, beforeEach, describe, expect, it } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { resolveRepositoryRoute } from "@/worker/repositories/route";

import { ensureD1Migrations } from "./util/d1Setup";
import { mintSessionCookie, seedRepo } from "./util/repoSeed";
import { runQueueMessage } from "./util/queue";
import { newPrefixedId } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import { insertUserIfNew, claimNamespace, insertMembershipIfMissing } from "@/worker/db/d1/dal";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

type CreateOk = {
  ok: true;
  id: string;
  namespaceSlug: string;
  slug: string;
  visibility: "public" | "private";
  updatedAt: number;
};
type CreateFail =
  | { ok: false; reason: "invalid-slug" }
  | { ok: false; reason: "invalid-visibility" }
  | { ok: false; reason: "namespace-not-found" }
  | { ok: false; reason: "not-member" }
  | { ok: false; reason: "slug-taken" };

async function createMember(
  namespaceSlug: string
): Promise<{ userId: string; cookieHeader: string; namespaceId: string }> {
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
  if (!claimed) {
    throw new Error(`createMember: namespace ${namespaceSlug} already exists`);
  }
  await insertMembershipIfMissing(db, {
    namespaceId: claimed.id,
    userId,
    createdAt: now,
  });
  const cookieHeader = await mintSessionCookie(env, userId);
  return { userId, namespaceId: claimed.id, cookieHeader };
}

async function postCreate(
  cookieHeader: string | null,
  body: Record<string, unknown>
): Promise<{ status: number; payload: CreateOk | CreateFail | { error: string } }> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Origin: "https://example.com",
  };
  if (cookieHeader) headers.Cookie = cookieHeader;
  const res = await workerExports.default.fetch("https://example.com/auth/api/repositories", {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  return { status: res.status, payload: (await res.json()) as CreateOk | CreateFail };
}

describe("POST /auth/api/repositories", () => {
  let nsCounter = 0;
  function uniqueNs(prefix: string): string {
    nsCounter += 1;
    return `${prefix}-${nsCounter}-${Math.random().toString(36).slice(2, 8)}`;
  }

  it("creates a repo for a member and the route resolves immediately via D1", async () => {
    const ns = uniqueNs("rc-ok");
    const member = await createMember(ns);
    const slug = "site";
    const { status, payload } = await postCreate(member.cookieHeader, {
      namespaceSlug: ns,
      slug,
      visibility: "private",
    });
    expect(status).toBe(200);
    expect(payload).toMatchObject({
      ok: true,
      namespaceSlug: ns,
      slug,
      visibility: "private",
    });
    const route = await resolveRepositoryRoute(env, ns, slug);
    expect(route).not.toBeNull();
    expect(route?.namespaceId).toBe(member.namespaceId);
    expect(route?.visibility).toBe("private");
    // doName for fresh repos uses the `repo:<id-suffix>` form.
    expect(route?.doName.startsWith("repo:")).toBe(true);
  });

  it("rejects anonymous callers with 401", async () => {
    const { status, payload } = await postCreate(null, {
      namespaceSlug: "anonns",
      slug: "x",
      visibility: "public",
    });
    expect(status).toBe(401);
    expect((payload as { error?: string }).error).toBe("Unauthorized");
  });

  it("rejects non-members with 403 not-member", async () => {
    const ns = uniqueNs("rc-nonmember");
    await createMember(ns); // ns exists, but `intruder` is a separate user not a member.
    const intruderNs = uniqueNs("rc-intruder");
    const intruder = await createMember(intruderNs);
    const { status, payload } = await postCreate(intruder.cookieHeader, {
      namespaceSlug: ns,
      slug: "anything",
      visibility: "public",
    });
    expect(status).toBe(403);
    expect(payload).toEqual({ ok: false, reason: "not-member" });
  });

  it("rejects unknown namespace with 404 namespace-not-found", async () => {
    const member = await createMember(uniqueNs("rc-known"));
    const { status, payload } = await postCreate(member.cookieHeader, {
      namespaceSlug: "ghost-namespace-xyz",
      slug: "site",
      visibility: "public",
    });
    expect(status).toBe(404);
    expect(payload).toEqual({ ok: false, reason: "namespace-not-found" });
  });

  it("rejects duplicate slug with 409 slug-taken", async () => {
    const ns = uniqueNs("rc-dup");
    const member = await createMember(ns);
    await seedRepo(env, { namespaceSlug: ns, repoSlug: "site", userId: member.userId });
    const { status, payload } = await postCreate(member.cookieHeader, {
      namespaceSlug: ns,
      slug: "site",
      visibility: "public",
    });
    expect(status).toBe(409);
    expect(payload).toEqual({ ok: false, reason: "slug-taken" });
  });

  it("rejects invalid slug with 400 invalid-slug", async () => {
    const ns = uniqueNs("rc-bad");
    const member = await createMember(ns);
    const { status, payload } = await postCreate(member.cookieHeader, {
      namespaceSlug: ns,
      slug: "Bad Slug!",
      visibility: "private",
    });
    expect(status).toBe(400);
    expect(payload).toEqual({ ok: false, reason: "invalid-slug" });
  });

  it("rejects missing visibility with 400 invalid-visibility", async () => {
    const ns = uniqueNs("rc-vis");
    const member = await createMember(ns);
    const { status, payload } = await postCreate(member.cookieHeader, {
      namespaceSlug: ns,
      slug: "site",
    });
    expect(status).toBe(400);
    expect(payload).toEqual({ ok: false, reason: "invalid-visibility" });
  });

  it("rejects same-origin violations (no Origin header)", async () => {
    const ns = uniqueNs("rc-csrf");
    const member = await createMember(ns);
    const res = await workerExports.default.fetch("https://example.com/auth/api/repositories", {
      method: "POST",
      headers: { "Content-Type": "application/json", Cookie: member.cookieHeader },
      body: JSON.stringify({ namespaceSlug: ns, slug: "x", visibility: "private" }),
    });
    expect(res.status).toBe(403);
  });
});

describe("PATCH /auth/api/repositories/:repositoryId", () => {
  it("public->private flip enqueues route-cache-sync that removes the route KV record", async () => {
    const ns = `rcv-${Math.random().toString(36).slice(2, 8)}`;
    const member = await createMember(ns);
    const seed = await seedRepo(env, {
      namespaceSlug: ns,
      repoSlug: "site",
      userId: member.userId,
      visibility: "public",
    });
    // Confirm KV record exists before flipping.
    expect(await env.ROUTES.get(seed.routeCacheKey)).not.toBeNull();
    const res = await workerExports.default.fetch(
      `https://example.com/auth/api/repositories/${encodeURIComponent(seed.repositoryId)}`,
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
    expect(res.status).toBe(200);
    const payload = (await res.json()) as {
      ok: true;
      visibility: "public" | "private";
      previous: "public" | "private";
    };
    expect(payload.visibility).toBe("private");
    expect(payload.previous).toBe("public");
    // The request only enqueues; drive the consumer manually to converge.
    // The consumer reads D1 (now private), so it deletes the canonical key.
    const result = await runQueueMessage({
      kind: "route-cache-sync",
      repositoryId: seed.repositoryId,
      namespaceSlug: ns,
      repoSlug: "site",
      enqueuedAt: Date.now(),
    });
    expect(result.acked).toBe(true);
    expect(await env.ROUTES.get(seed.routeCacheKey)).toBeNull();
  });

  it("rejects PATCH from non-members with 403 not-member", async () => {
    const ns = `rcv-${Math.random().toString(36).slice(2, 8)}`;
    const member = await createMember(ns);
    const seed = await seedRepo(env, {
      namespaceSlug: ns,
      repoSlug: "site",
      userId: member.userId,
    });
    const intruderNs = `rcv-int-${Math.random().toString(36).slice(2, 8)}`;
    const intruder = await createMember(intruderNs);
    const res = await workerExports.default.fetch(
      `https://example.com/auth/api/repositories/${encodeURIComponent(seed.repositoryId)}`,
      {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          Origin: "https://example.com",
          Cookie: intruder.cookieHeader,
        },
        body: JSON.stringify({ visibility: "private" }),
      }
    );
    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({ ok: false, reason: "not-member" });
  });

  it("returns 404 not-found for unknown repository id", async () => {
    const member = await createMember(`rcv-x-${Math.random().toString(36).slice(2, 8)}`);
    const res = await workerExports.default.fetch(
      `https://example.com/auth/api/repositories/repo_ghost`,
      {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          Origin: "https://example.com",
          Cookie: member.cookieHeader,
        },
        body: JSON.stringify({ visibility: "public" }),
      }
    );
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ ok: false, reason: "not-found" });
  });
});

describe("public->private visibility flip changes anonymous read response", () => {
  beforeEach(async () => {
    await ensureD1Migrations(env);
  });

  it("anonymous overview is 200 while public, 404 after flip; member overview stays 200", async () => {
    const ns = `flip-${Math.random().toString(36).slice(2, 8)}`;
    const member = await createMember(ns);
    const seed = await seedRepo(env, {
      namespaceSlug: ns,
      repoSlug: "site",
      userId: member.userId,
      visibility: "public",
    });

    const anonPublic = await workerExports.default.fetch(`https://example.com/${ns}/site`);
    expect(anonPublic.status).toBe(200);

    const flipRes = await workerExports.default.fetch(
      `https://example.com/auth/api/repositories/${encodeURIComponent(seed.repositoryId)}`,
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

    const anonPrivate = await workerExports.default.fetch(`https://example.com/${ns}/site`);
    expect(anonPrivate.status).toBe(404);

    const memberPrivate = await workerExports.default.fetch(`https://example.com/${ns}/site`, {
      headers: { Cookie: member.cookieHeader },
    });
    expect(memberPrivate.status).toBe(200);
  });
});
