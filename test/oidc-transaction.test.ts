import { describe, expect, it } from "vitest";

import {
  decodeTransactionPayload,
  deriveTransactionCookieSecret,
  encodeTransactionPayload,
} from "@/worker/auth/oidc";

describe("OIDC transaction payload encoding", () => {
  it("derives a deterministic purpose-scoped signing secret", async () => {
    const first = await deriveTransactionCookieSecret("client-secret");
    const second = await deriveTransactionCookieSecret("client-secret");
    const different = await deriveTransactionCookieSecret("other-client-secret");
    expect(first.byteLength).toBe(32);
    expect([...first]).toEqual([...second]);
    expect([...first]).not.toEqual([...different]);
  });

  it("round-trips a payload within TTL", () => {
    const encoded = encodeTransactionPayload({
      state: "abc",
      nonce: "nonce-1",
      codeVerifier: "verifier",
      redirectUri: "https://example.com/auth/callback",
      createdAt: Date.now(),
    });
    const result = decodeTransactionPayload(encoded);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.payload.state).toBe("abc");
    expect(result.payload.nonce).toBe("nonce-1");
    expect(result.payload.codeVerifier).toBe("verifier");
    expect(result.payload.redirectUri).toBe("https://example.com/auth/callback");
  });

  it("rejects an expired payload", () => {
    const start = Date.now();
    const encoded = encodeTransactionPayload({
      state: "abc",
      nonce: "n",
      codeVerifier: "v",
      redirectUri: "https://example.com/auth/callback",
      createdAt: start,
    });
    // 6 minutes later - beyond the 5-minute TTL.
    expect(decodeTransactionPayload(encoded, start + 6 * 60 * 1000)).toEqual({
      ok: false,
      reason: "expired",
    });
  });

  it("rejects malformed input", () => {
    expect(decodeTransactionPayload("not-base64-!!")).toEqual({
      ok: false,
      reason: "malformed",
    });
    expect(decodeTransactionPayload("AAAA")).toEqual({ ok: false, reason: "invalid_payload" });
  });
});
