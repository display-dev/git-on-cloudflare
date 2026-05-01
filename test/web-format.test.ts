import { assert, test } from "vitest";
import { formatWhen } from "@/shared/web";

const SAMPLE_EPOCH = 1774136183; // 2026-03-21 23:36:23 UTC

test("formatWhen renders commit-local time for negative offsets", () => {
  assert.strictEqual(formatWhen(SAMPLE_EPOCH, "-0700"), "2026-03-21 16:36:23 -0700");
});

test("formatWhen renders commit-local time for positive offsets", () => {
  assert.strictEqual(formatWhen(SAMPLE_EPOCH, "+0530"), "2026-03-22 05:06:23 +0530");
});

test("formatWhen falls back to UTC when offset is malformed", () => {
  assert.strictEqual(formatWhen(SAMPLE_EPOCH, "UTC"), "2026-03-21 23:36:23 UTC");
});
