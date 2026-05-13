import { applyD1Migrations } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { __test as oidcTest } from "@/worker/auth/oidc";
import { SESSION_COOKIE_HEADER_NAME } from "@/worker/auth/cookies";

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

describe("session lifecycle", () => {
  it("issues a session cookie that authorizes /auth/account", async () => {
    const token = await signIn("sub-session-1", "session-rachel");
    const res = await workerExports.default.fetch("https://example.com/auth/account", {
      headers: { Cookie: sessionCookieHeader(token) },
    });
    expect(res.status).toBe(200);
    const html = await res.text();
    expect(html).toContain("@session-rachel");
  });

  it("/auth/account redirects to /auth when no cookie is present", async () => {
    const res = await workerExports.default.fetch("https://example.com/auth/account", {
      redirect: "manual",
    });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/auth");
  });

  it("rejects POST /auth/sign-out without same-origin", async () => {
    const token = await signIn("sub-signout-cross", undefined);
    const res = await workerExports.default.fetch("https://example.com/auth/sign-out", {
      method: "POST",
      headers: {
        Cookie: sessionCookieHeader(token),
        Origin: "https://attacker.example.com",
      },
      redirect: "manual",
    });
    expect(res.status).toBe(403);
    // Cookie remains valid for /auth/account.
    const followup = await workerExports.default.fetch("https://example.com/auth/account", {
      headers: { Cookie: sessionCookieHeader(token) },
    });
    expect(followup.status).toBe(200);
  });

  it("clears the browser cookie on same-origin sign-out", async () => {
    const token = await signIn("sub-signout-2", undefined);
    const res = await workerExports.default.fetch("https://example.com/auth/sign-out", {
      method: "POST",
      headers: {
        Cookie: sessionCookieHeader(token),
        Origin: "https://example.com",
      },
      redirect: "manual",
    });
    expect(res.status).toBe(303);
    expect(res.headers.get("location")).toBe("/");
    const setCookie = res.headers.get("set-cookie") ?? "";
    expect(setCookie).toContain(`${SESSION_COOKIE_HEADER_NAME}=`);
    expect(setCookie.toLowerCase()).toContain("max-age=0");
    // Stateless sealed sessions have no server-side revocation row. A copied
    // cookie remains valid until expiry or SESSION_SECRET rotation.
    const followup = await workerExports.default.fetch("https://example.com/auth/account", {
      headers: { Cookie: sessionCookieHeader(token) },
      redirect: "manual",
    });
    expect(followup.status).toBe(200);
  });

  it("treats a malformed session cookie as anonymous", async () => {
    const res = await workerExports.default.fetch("https://example.com/auth/account", {
      headers: { Cookie: `${SESSION_COOKIE_HEADER_NAME}=garbage` },
      redirect: "manual",
    });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/auth");
  });

  it("fails closed when SESSION_SECRET changes", async () => {
    const token = await signIn("sub-secret-rotation", undefined);
    const original = env.SESSION_SECRET;
    env.SESSION_SECRET = "different-session-secret";
    try {
      const res = await workerExports.default.fetch("https://example.com/auth/account", {
        headers: { Cookie: sessionCookieHeader(token) },
        redirect: "manual",
      });
      expect(res.status).toBe(302);
      expect(res.headers.get("location")).toBe("/auth");
    } finally {
      env.SESSION_SECRET = original;
    }
  });

  it("fails closed when SESSION_SECRET is missing", async () => {
    const token = await signIn("sub-secret-missing", undefined);
    const original = env.SESSION_SECRET;
    env.SESSION_SECRET = "";
    try {
      const res = await workerExports.default.fetch("https://example.com/auth/account", {
        headers: { Cookie: sessionCookieHeader(token) },
        redirect: "manual",
      });
      expect(res.status).toBe(302);
      expect(res.headers.get("location")).toBe("/auth");
    } finally {
      env.SESSION_SECRET = original;
    }
  });
});
