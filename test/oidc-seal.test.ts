import { describe, expect, it } from "vitest";

import { sealTransaction, unsealTransaction } from "@/worker/auth/oidc";

const SECRET = "test-secret-string";

describe("OIDC transaction seal/unseal", () => {
  it("round-trips a payload within TTL", async () => {
    const sealed = await sealTransaction(SECRET, {
      state: "abc",
      nonce: "nonce-1",
      codeVerifier: "verifier",
      redirectUri: "https://example.com/auth/callback",
      createdAt: Date.now(),
    });
    const result = await unsealTransaction(SECRET, sealed);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.payload.state).toBe("abc");
    expect(result.payload.nonce).toBe("nonce-1");
    expect(result.payload.codeVerifier).toBe("verifier");
    expect(result.payload.redirectUri).toBe("https://example.com/auth/callback");
  });

  it("rejects an expired sealed payload", async () => {
    const start = Date.now();
    const sealed = await sealTransaction(SECRET, {
      state: "abc",
      nonce: "n",
      codeVerifier: "v",
      redirectUri: "https://example.com/auth/callback",
      createdAt: start,
    });
    // 6 minutes later — beyond the 5-minute TTL.
    expect(await unsealTransaction(SECRET, sealed, start + 6 * 60 * 1000)).toEqual({
      ok: false,
      reason: "expired",
    });
  });

  it("rejects a sealed payload signed by a different secret", async () => {
    const sealed = await sealTransaction(SECRET, {
      state: "abc",
      nonce: "n",
      codeVerifier: "v",
      redirectUri: "https://example.com/auth/callback",
      createdAt: Date.now(),
    });
    expect(await unsealTransaction("OTHER", sealed)).toEqual({
      ok: false,
      reason: "decrypt_failed",
    });
  });

  it("rejects malformed input", async () => {
    expect(await unsealTransaction(SECRET, "not-base64-!!")).toEqual({
      ok: false,
      reason: "malformed",
    });
    expect(await unsealTransaction(SECRET, "AAAA")).toEqual({ ok: false, reason: "malformed" });
  });
});
