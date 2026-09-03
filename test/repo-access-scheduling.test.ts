import { describe, expect, it } from "vitest";

import { shouldPreserveDueAlarm } from "@/worker/do/repo/repoDO/access";

describe("repository access alarm scheduling", () => {
  it("preserves a due alarm for its pending handler", () => {
    expect(shouldPreserveDueAlarm(999, 1_000)).toBe(true);
    expect(shouldPreserveDueAlarm(1_000, 1_000)).toBe(true);
    expect(shouldPreserveDueAlarm(1_001, 1_000)).toBe(false);
    expect(shouldPreserveDueAlarm(null, 1_000)).toBe(false);
  });
});
