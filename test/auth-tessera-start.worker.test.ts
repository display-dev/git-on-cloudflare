import { applyD1Migrations } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";
import { afterEach, beforeAll, describe, expect, it } from "vitest";

import { __test as oidcTest } from "@/worker/auth/oidc";
import { OIDC_TX_COOKIE_HEADER_NAME } from "@/worker/auth/cookies";

import { fakeProvider } from "./util/oidcFake";
import { readAppD1Migrations } from "./util/d1Migrations";

beforeAll(async () => {
  await applyD1Migrations(env.DB, readAppD1Migrations());
});

afterEach(() => {
  oidcTest.clearProviderCache();
  oidcTest.setAuthorizationCodeGrantImpl(null);
});

describe("/auth/start", () => {
  it("returns 302 to the authorize URL with a signed transaction cookie", async () => {
    const provider = fakeProvider({
      authorizationEndpoint: "https://auth.example.com/oauth2/authorize",
      tokenEndpoint: "https://auth.example.com/oauth2/token",
      jwksUri: "https://auth.example.com/.well-known/jwks.json",
    });
    oidcTest.setProviderForTesting(
      {
        issuer: env.TESSERA_OIDC_ISSUER,
        clientId: env.TESSERA_OIDC_CLIENT_ID,
        clientSecret: env.TESSERA_OIDC_CLIENT_SECRET,
      },
      provider
    );
    const res = await workerExports.default.fetch("https://example.com/auth/start", {
      redirect: "manual",
    });
    expect(res.status).toBe(302);
    const location = res.headers.get("location") ?? "";
    expect(location.startsWith("https://auth.example.com/oauth2/authorize")).toBe(true);
    expect(location).toContain("response_type=code");
    expect(location).toContain("code_challenge_method=S256");
    expect(location).toContain("scope=openid");
    const setCookie = res.headers.get("set-cookie") ?? "";
    expect(setCookie).toContain(`${OIDC_TX_COOKIE_HEADER_NAME}=`);
    expect(setCookie).toContain("HttpOnly");
    expect(setCookie).toContain("Secure");
    expect(setCookie).toContain("SameSite=Lax");
    expect(setCookie).toContain("Path=/");
  });

  it("redirects to /auth?error=oidc_unavailable when discovery fails", async () => {
    // No fake provider injected and the configured issuer is not reachable
    // from inside the test pool, so discovery will fail.
    const res = await workerExports.default.fetch("https://example.com/auth/start", {
      redirect: "manual",
    });
    expect(res.status).toBe(302);
    const location = res.headers.get("location") ?? "";
    expect(location).toBe("/auth?error=oidc_unavailable");
  });
});
