import { applyD1Migrations } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

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

async function signIn(sub: string, preferredUsername?: string): Promise<string> {
  const state = `state-${sub}`;
  const cookie = await oidcTransactionCookieHeader(env.TESSERA_OIDC_CLIENT_SECRET, {
    state,
    nonce: "n",
    codeVerifier: "v",
    redirectUri: "https://example.com/auth/callback",
    createdAt: Date.now(),
  });
  oidcTest.setAuthorizationCodeGrantImpl(async () => {
    const claims = preferredUsername ? { sub, preferred_username: preferredUsername } : { sub };
    return {
      access_token: "fake",
      token_type: "Bearer",
      claims: () => claims,
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

describe("merged /auth/account hub", () => {
  it("renders identity, namespaces, repositories, and the tokens island on one page", async () => {
    const token = await signIn("sub-merged-1", "merged-rachel");
    const res = await workerExports.default.fetch("https://example.com/auth/account", {
      headers: { Cookie: sessionCookieHeader(token) },
    });
    expect(res.status).toBe(200);
    const html = await res.text();

    // Identity moment renders the namespace handle, not "Your account".
    expect(html).toContain("@merged-rachel");
    expect(html).toContain("Identity");
    expect(html).not.toContain("Your account");

    // The four sections all live on this single page.
    expect(html).toContain("Namespaces");
    expect(html).toContain("Repositories");
    expect(html).toContain("Tokens");

    // The tokens section is anchored and hosts the island for client hydration.
    expect(html).toContain('id="tokens"');
    expect(html).toContain('data-island="tokens"');
  });

  it("renders the merged page even when the user has no preferred_username claim", async () => {
    const token = await signIn("sub-merged-no-slug", undefined);
    const res = await workerExports.default.fetch("https://example.com/auth/account", {
      headers: { Cookie: sessionCookieHeader(token) },
    });
    expect(res.status).toBe(200);
    const html = await res.text();
    expect(html).toContain("Identity not yet claimed");
    expect(html).toContain('data-island="tokens"');
  });

  it("/auth/tokens no longer routes to the management page", async () => {
    const token = await signIn("sub-tokens-route-gone", "route-gone");
    const res = await workerExports.default.fetch("https://example.com/auth/tokens", {
      headers: { Cookie: sessionCookieHeader(token) },
      redirect: "manual",
    });
    // The management page is exclusively at /auth/account now. Whatever the
    // /:owner/:repo fallthrough returns for an unknown namespace, it must not
    // be the tokens management island.
    if (res.status === 200) {
      const html = await res.text();
      expect(html).not.toContain('data-island="tokens"');
      expect(html).not.toContain('id="tokens"');
    }
  });
});
