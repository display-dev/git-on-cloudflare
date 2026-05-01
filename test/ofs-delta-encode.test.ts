import { assert, test } from "vitest";
import { encodeOfsDeltaDistance } from "@/worker/git";

function decodeOfsDeltaDistance(bytes: Uint8Array): number {
  let p = 0;
  let b = bytes[p++];
  let x = b & 0x7f;
  while (b & 0x80) {
    b = bytes[p++];
    x = ((x + 1) << 7) | (b & 0x7f);
  }
  return x >>> 0;
}

const cases = [1, 0x7f, 0x80, 0x1234, 0x1ffff, 0x20000, 0x3ffffff, 0x4000000];

test("encodeOfsDeltaDistance round-trips typical values", () => {
  for (const n of cases) {
    const enc = encodeOfsDeltaDistance(n);
    const dec = decodeOfsDeltaDistance(enc);
    assert.strictEqual(
      dec,
      n,
      `round-trip failed for ${n} (enc: ${Array.from(enc)
        .map((b) => b.toString(16))
        .join(",")})`
    );
  }
});
