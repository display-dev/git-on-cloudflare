import { describe, expect, it } from "vitest";

import { loadOidcConfig } from "@/worker/auth/oidc";

function envOf(overrides: Partial<Env>): Env {
  return {
    TESSERA_OIDC_ISSUER: "",
    TESSERA_OIDC_CLIENT_ID: "",
    TESSERA_OIDC_CLIENT_SECRET: "",
    ...overrides,
  } as unknown as Env;
}

describe("loadOidcConfig", () => {
  it("accepts production https issuer + client credentials", () => {
    const result = loadOidcConfig(
      envOf({
        TESSERA_OIDC_ISSUER: "https://auth.limic.dev",
        TESSERA_OIDC_CLIENT_ID: "goc-prod",
        TESSERA_OIDC_CLIENT_SECRET: "shh",
      })
    );
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.config.issuer).toBe("https://auth.limic.dev");
    expect(result.config.clientId).toBe("goc-prod");
  });

  it("accepts loopback HTTP issuer for local dev", () => {
    for (const host of ["localhost", "127.0.0.1", "[::1]"]) {
      const result = loadOidcConfig(
        envOf({
          TESSERA_OIDC_ISSUER: `http://${host}:5174`,
          TESSERA_OIDC_CLIENT_ID: "local-goc",
          TESSERA_OIDC_CLIENT_SECRET: "secret",
        })
      );
      expect(result.ok).toBe(true);
    }
  });

  it("rejects plaintext non-loopback issuers", () => {
    const result = loadOidcConfig(
      envOf({
        TESSERA_OIDC_ISSUER: "http://auth.example.com",
        TESSERA_OIDC_CLIENT_ID: "goc",
        TESSERA_OIDC_CLIENT_SECRET: "secret",
      })
    );
    expect(result).toEqual({ ok: false, reason: "insecure_issuer" });
  });

  it("rejects an issuer URL with query/hash/userinfo", () => {
    expect(
      loadOidcConfig(
        envOf({
          TESSERA_OIDC_ISSUER: "https://auth.limic.dev?x=1",
          TESSERA_OIDC_CLIENT_ID: "goc",
          TESSERA_OIDC_CLIENT_SECRET: "secret",
        })
      )
    ).toEqual({ ok: false, reason: "insecure_issuer" });
    expect(
      loadOidcConfig(
        envOf({
          TESSERA_OIDC_ISSUER: "https://user@auth.limic.dev",
          TESSERA_OIDC_CLIENT_ID: "goc",
          TESSERA_OIDC_CLIENT_SECRET: "secret",
        })
      )
    ).toEqual({ ok: false, reason: "insecure_issuer" });
  });

  it("reports missing issuer/client_id/client_secret distinctly", () => {
    expect(loadOidcConfig(envOf({}))).toEqual({ ok: false, reason: "missing_issuer" });
    expect(loadOidcConfig(envOf({ TESSERA_OIDC_ISSUER: "https://auth.limic.dev" }))).toEqual({
      ok: false,
      reason: "missing_client_id",
    });
    expect(
      loadOidcConfig(
        envOf({
          TESSERA_OIDC_ISSUER: "https://auth.limic.dev",
          TESSERA_OIDC_CLIENT_ID: "goc",
        })
      )
    ).toEqual({ ok: false, reason: "missing_client_secret" });
  });
});
