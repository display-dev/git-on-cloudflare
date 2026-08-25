import { assert, test } from "vitest";
import {
  encodeOfsDeltaDistance,
  readPackHeaderExFromBuf,
} from "@/worker/git/pack/packMeta";

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

test("pack header decoder preserves OFS distances above signed 32-bit range", () => {
  const distance = 2_500_000_000;
  const encoded = encodeOfsDeltaDistance(distance);
  const header = new Uint8Array(1 + encoded.length);
  header[0] = 6 << 4;
  header.set(encoded, 1);

  assert.strictEqual(readPackHeaderExFromBuf(header, 0)?.baseRel, distance);
});
