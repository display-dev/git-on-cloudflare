import { applyD1Migrations } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { createDb } from "@/worker/db/d1/client";
import {
  findNamespaceBySlug,
  findPatByPrefix,
  insertRepositoryIfNew,
  listPatsForUser,
} from "@/worker/db/d1/dal";
import { __test as oidcTest } from "@/worker/auth/oidc";

import { fakeProvider } from "./util/oidcFake";
import { readAppD1Migrations } from "./util/d1Migrations";
import {
  extractSessionToken,
  oidcTransactionCookieHeader,
  sessionCookieHeader,
} from "./util/authCookies";

beforeAll(async () => {
  await applyD1Migrations(env.DB, readAppD1Migrations());
});

beforeEach(() => {
  oidcTest.setProviderForTesting(
    {
      issuer: env.TESSERA_OIDC_ISSUER,
      clientId: env.TESSERA_OIDC_CLIENT_ID,
      clientSecret: env.TESSERA_OIDC_CLIENT_SECRET,
    },
    fakeProvider({
      authorizationEndpoint: "https://auth.example.com/authorize",
      tokenEndpoint: "https://auth.example.com/token",
      jwksUri: "https://auth.example.com/.well-known/jwks.json",
    })
  );
});

afterEach(() => {
  oidcTest.clearProviderCache();
  oidcTest.setAuthorizationCodeGrantImpl(null);
});

async function signInAndGetCookie(sub: string, preferredUsername: string): Promise<string> {
  const state = `state-${sub}`;
  const cookie = await oidcTransactionCookieHeader(env.TESSERA_OIDC_CLIENT_SECRET, {
    state,
    nonce: "n",
    codeVerifier: "v",
    redirectUri: "https://example.com/auth/callback",
    createdAt: Date.now(),
  });
  oidcTest.setAuthorizationCodeGrantImpl(async () => {
    return {
      access_token: "fake",
      token_type: "Bearer",
      claims: () => ({ sub, preferred_username: preferredUsername }),
    } as unknown as Awaited<ReturnType<typeof import("openid-client").authorizationCodeGrant>>;
  });
  const url = new URL("https://example.com/auth/callback");
  url.searchParams.set("code", "x");
  url.searchParams.set("state", state);
  const res = await workerExports.default.fetch(url.toString(), {
    redirect: "manual",
    headers: { Cookie: cookie },
  });
  expect(res.status).toBe(302);
  const token = extractSessionToken(res.headers.get("set-cookie"));
  expect(token).toBeTruthy();
  return token!;
}

describe("PAT management endpoints", () => {
  it("creates, lists, and revokes a token; cross-user revoke fails", async () => {
    const aliceCookie = await signInAndGetCookie("sub-alice", "pat-alice");
    const bobCookie = await signInAndGetCookie("sub-bob", "pat-bob");

    // Create token for Alice scoped to her namespace.
    const create = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://example.com",
        Cookie: sessionCookieHeader(aliceCookie),
      },
      body: JSON.stringify({
        scope: "namespace",
        name: "ci",
        namespaceSlug: "pat-alice",
        level: "push",
      }),
    });
    expect(create.status).toBe(200);
    const created = (await create.json()) as { id: string; plaintext: string; prefix: string };
    expect(created.plaintext.startsWith(`${created.prefix}_`)).toBe(true);

    // Plaintext is not retrievable on subsequent list.
    const list = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      headers: { Cookie: sessionCookieHeader(aliceCookie) },
    });
    expect(list.status).toBe(200);
    const listed = (await list.json()) as {
      tokens: Array<{
        id: string;
        prefix: string;
        namespaceGrants: Array<{ namespaceSlug: string; level: "pull" | "push" }>;
        repoGrants: unknown[];
      }>;
    };
    expect(listed.tokens.length).toBe(1);
    expect(listed.tokens[0]?.id).toBe(created.id);
    // Permissions and namespace are surfaced for audit.
    expect(listed.tokens[0]?.namespaceGrants).toEqual([
      { namespaceSlug: "pat-alice", level: "push" },
    ]);
    expect(listed.tokens[0]?.repoGrants).toEqual([]);
    // No `plaintext` field is leaked.
    for (const token of listed.tokens) {
      expect(Object.keys(token)).not.toContain("plaintext");
    }
    // DB has the matching prefix and a non-empty hash.
    const db = createDb(env.DB);
    const stored = await findPatByPrefix(db, created.prefix);
    expect(stored?.hash).toMatch(/^[0-9a-f]{64}$/);

    // Bob cannot revoke Alice's token.
    const cross = await workerExports.default.fetch(
      `https://example.com/auth/api/tokens/${created.id}`,
      {
        method: "DELETE",
        headers: { Cookie: sessionCookieHeader(bobCookie), Origin: "https://example.com" },
      }
    );
    expect(cross.status).toBe(403);

    // Alice can.
    const revoke = await workerExports.default.fetch(
      `https://example.com/auth/api/tokens/${created.id}`,
      {
        method: "DELETE",
        headers: { Cookie: sessionCookieHeader(aliceCookie), Origin: "https://example.com" },
      }
    );
    expect(revoke.status).toBe(200);

    // Re-revoke is idempotent (200 ok with already-revoked treated as ok).
    const revoke2 = await workerExports.default.fetch(
      `https://example.com/auth/api/tokens/${created.id}`,
      {
        method: "DELETE",
        headers: { Cookie: sessionCookieHeader(aliceCookie), Origin: "https://example.com" },
      }
    );
    expect(revoke2.status).toBe(200);

    // The PAT row still exists with revokedAt set.
    const tokens = await listPatsForUser(db, stored!.userId);
    expect(tokens.find((row) => row.id === created.id)?.revokedAt).not.toBeNull();
  });

  it("rejects POST without same-origin Origin header", async () => {
    const cookie = await signInAndGetCookie("sub-cross", "pat-cross");
    const res = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://attacker.example",
        Cookie: sessionCookieHeader(cookie),
      },
      body: JSON.stringify({
        scope: "namespace",
        name: "ci",
        namespaceSlug: "pat-cross",
        level: "pull",
      }),
    });
    expect(res.status).toBe(403);
  });

  it("rejects POST when the namespace is not the viewer's", async () => {
    const cookie = await signInAndGetCookie("sub-other", "pat-other");
    const res = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://example.com",
        Cookie: sessionCookieHeader(cookie),
      },
      body: JSON.stringify({
        scope: "namespace",
        name: "ci",
        namespaceSlug: "does-not-exist",
        level: "pull",
      }),
    });
    expect(res.status).toBe(404);
  });

  it("rejects POST when scope is missing from the body", async () => {
    const cookie = await signInAndGetCookie("sub-no-scope", "pat-no-scope");
    const res = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://example.com",
        Cookie: sessionCookieHeader(cookie),
      },
      // No `scope` field — must be a 400, not coerced to namespace.
      body: JSON.stringify({
        name: "ci",
        namespaceSlug: "pat-no-scope",
        level: "pull",
      }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error?: string };
    expect(body.error).toMatch(/scope/i);
  });

  it("rejects POST when level is missing from the body", async () => {
    const cookie = await signInAndGetCookie("sub-no-level", "pat-no-level");
    const res = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://example.com",
        Cookie: sessionCookieHeader(cookie),
      },
      body: JSON.stringify({
        scope: "namespace",
        name: "ci",
        namespaceSlug: "pat-no-level",
      }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error?: string };
    expect(body.error).toMatch(/level/i);
  });

  it("rejects POST when level is not 'pull' or 'push'", async () => {
    const cookie = await signInAndGetCookie("sub-bad-level", "pat-bad-level");
    const res = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://example.com",
        Cookie: sessionCookieHeader(cookie),
      },
      body: JSON.stringify({
        scope: "namespace",
        name: "ci",
        namespaceSlug: "pat-bad-level",
        level: "admin",
      }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error?: string };
    expect(body.error).toMatch(/level/i);
  });

  it("creates a repo-scoped token; list shows repoGrants populated", async () => {
    const cookie = await signInAndGetCookie("sub-repo-pat", "pat-repo");
    // Seed a repository in the viewer's namespace so the create path can
    // resolve `(namespaceSlug, repoSlug)` to a repo row.
    const db = createDb(env.DB);
    const namespace = await findNamespaceBySlug(db, "pat-repo");
    expect(namespace).toBeDefined();
    await insertRepositoryIfNew(db, {
      id: "repo-pat-scope",
      namespaceId: namespace!.id,
      createdBy: namespace!.createdBy,
      slug: "site",
      doName: "pat-repo/site",
      visibility: "public",
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });

    const create = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://example.com",
        Cookie: sessionCookieHeader(cookie),
      },
      body: JSON.stringify({
        scope: "repo",
        name: "ci-repo",
        namespaceSlug: "pat-repo",
        repoSlug: "site",
        level: "push",
      }),
    });
    expect(create.status).toBe(200);
    const created = (await create.json()) as { id: string; plaintext: string; prefix: string };

    const list = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      headers: { Cookie: sessionCookieHeader(cookie) },
    });
    expect(list.status).toBe(200);
    const listed = (await list.json()) as {
      tokens: Array<{
        id: string;
        namespaceGrants: Array<{ namespaceSlug: string; level: "pull" | "push" }>;
        repoGrants: Array<{
          namespaceSlug: string;
          repoSlug: string;
          level: "pull" | "push";
        }>;
      }>;
    };
    const token = listed.tokens.find((row) => row.id === created.id);
    expect(token).toBeDefined();
    // Namespace grants stay empty; the repo grant carries the level.
    expect(token!.namespaceGrants).toEqual([]);
    expect(token!.repoGrants).toEqual([
      { namespaceSlug: "pat-repo", repoSlug: "site", level: "push" },
    ]);
  });

  it("rejects POST scope:'repo' when the repo does not exist", async () => {
    const cookie = await signInAndGetCookie("sub-repo-missing", "pat-rmissing");
    const res = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://example.com",
        Cookie: sessionCookieHeader(cookie),
      },
      body: JSON.stringify({
        scope: "repo",
        name: "ci",
        namespaceSlug: "pat-rmissing",
        repoSlug: "ghost",
        level: "pull",
      }),
    });
    expect(res.status).toBe(404);
  });

  it("rejects POST scope:'repo' for a namespace the viewer is not a member of", async () => {
    // Sign in as user A so the namespace + membership exist, then seed a
    // repo under A's namespace. The owner cookie itself isn't used; we only
    // need its side-effects (user, namespace, membership rows).
    await signInAndGetCookie("sub-repo-owner", "pat-rowner");
    const db = createDb(env.DB);
    const ownerNamespace = await findNamespaceBySlug(db, "pat-rowner");
    expect(ownerNamespace).toBeDefined();
    await insertRepositoryIfNew(db, {
      id: "repo-pat-cross",
      namespaceId: ownerNamespace!.id,
      createdBy: ownerNamespace!.createdBy,
      slug: "private-site",
      doName: "pat-rowner/private-site",
      visibility: "public",
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    // Then sign in as user B and try to mint a PAT against A's repo.
    const intruderCookie = await signInAndGetCookie("sub-repo-intruder", "pat-rintruder");
    const res = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://example.com",
        Cookie: sessionCookieHeader(intruderCookie),
      },
      body: JSON.stringify({
        scope: "repo",
        name: "ci",
        namespaceSlug: "pat-rowner",
        repoSlug: "private-site",
        level: "pull",
      }),
    });
    expect(res.status).toBe(403);
  });

  it("rejects POST scope:'repo' with a malformed repoSlug", async () => {
    const cookie = await signInAndGetCookie("sub-repo-badslug", "pat-rbadslug");
    const res = await workerExports.default.fetch("https://example.com/auth/api/tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://example.com",
        Cookie: sessionCookieHeader(cookie),
      },
      body: JSON.stringify({
        scope: "repo",
        name: "ci",
        namespaceSlug: "pat-rbadslug",
        // Uppercase + dot fails the strict slug policy.
        repoSlug: "Bad.Slug",
        level: "pull",
      }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error?: string };
    expect(body.error).toMatch(/repo slug/i);
  });
});
