import { describe, expect, it } from "vitest";

import {
  generatePatPlaintext,
  hashPatPlaintext,
  parsePatPlaintext,
  validatePatName,
} from "@/worker/auth/pat";

describe("PAT format", () => {
  it("round-trips generated tokens through parse/hash", async () => {
    const generated = generatePatPlaintext();
    expect(generated.plaintext.startsWith(`${generated.publicPrefix}_`)).toBe(true);
    const parsed = parsePatPlaintext(generated.plaintext);
    expect(parsed.ok).toBe(true);
    if (!parsed.ok) return;
    expect(parsed.publicPrefix).toBe(generated.publicPrefix);
    expect(parsed.secret).toMatch(/^[abcdefghijklmnopqrstuvwxyz234567]{32}$/);
    const hash = await hashPatPlaintext(generated.plaintext);
    expect(hash).toMatch(/^[0-9a-f]{64}$/);
    expect(await hashPatPlaintext(generated.plaintext)).toBe(hash);
  });

  it("rejects malformed plaintexts", () => {
    expect(parsePatPlaintext("")).toEqual({ ok: false, reason: "malformed" });
    expect(parsePatPlaintext("goc_xx_yy")).toEqual({ ok: false, reason: "malformed" });
    expect(parsePatPlaintext("nope_aaaaaaaa_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")).toEqual({
      ok: false,
      reason: "malformed",
    });
    // Wrong segment lengths.
    expect(parsePatPlaintext("goc_aaa_bbbb")).toEqual({ ok: false, reason: "malformed" });
  });

  it("validates token names", () => {
    expect(validatePatName("ci")).toEqual({ ok: true, name: "ci" });
    expect(validatePatName("CI Push")).toEqual({ ok: true, name: "CI Push" });
    expect(validatePatName("")).toEqual({ ok: false, reason: "length" });
    expect(validatePatName(" ")).toEqual({ ok: false, reason: "length" });
    expect(validatePatName("a".repeat(41))).toEqual({ ok: false, reason: "length" });
    expect(validatePatName("$bad")).toEqual({ ok: false, reason: "format" });
  });
});
