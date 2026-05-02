import { describe, expect, it } from "vitest";

import { isValidSlug, RESERVED_SLUGS, validateSlugForRoute } from "@/shared/slugs";

describe("isValidSlug", () => {
  it("accepts simple lowercase ASCII slugs", () => {
    expect(isValidSlug("rachel")).toBe(true);
    expect(isValidSlug("git-on-cloudflare")).toBe(true);
    expect(isValidSlug("a")).toBe(true);
    expect(isValidSlug("a1")).toBe(true);
  });

  it("rejects empty, too long, and bad-shape slugs", () => {
    expect(isValidSlug("")).toBe(false);
    expect(isValidSlug("-leading")).toBe(false);
    expect(isValidSlug("trailing-")).toBe(false);
    expect(isValidSlug("Two--Caps")).toBe(false);
    expect(isValidSlug("space inside")).toBe(false);
    expect(isValidSlug("a".repeat(41))).toBe(false);
    expect(isValidSlug("émigré")).toBe(false);
  });

  it("rejects every reserved slug", () => {
    for (const reserved of RESERVED_SLUGS) {
      expect(isValidSlug(reserved)).toBe(false);
    }
  });
});

describe("validateSlugForRoute", () => {
  it("reports length, reserved, and format reasons distinctly", () => {
    expect(validateSlugForRoute("")).toEqual({ ok: false, reason: "length" });
    expect(validateSlugForRoute("auth")).toEqual({ ok: false, reason: "reserved" });
    expect(validateSlugForRoute("BadCase")).toEqual({ ok: false, reason: "format" });
    expect(validateSlugForRoute("ok-slug")).toEqual({ ok: true, slug: "ok-slug" });
  });
});
