import { assert, test } from "vitest";
import { encodeOfsDeltaDistance } from "@/worker/git";

const toHex = (u8: Uint8Array) =>
  Array.from(u8)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

// Mirror of the decoder in src/worker/git/pack/assembler.ts for verification
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

const known: { n: number; hex: string }[] = [
  { n: 0x01, hex: "01" },
  { n: 0x7f, hex: "7f" },
  { n: 0x80, hex: "8000" },
  { n: 0x81, hex: "8001" },
  { n: 0x100, hex: "8100" },
  { n: 0x3fff, hex: "fe7f" },
  { n: 0x4000, hex: "ff00" },
  { n: 0x1ffff, hex: "86fe7f" },
  { n: 0x20000, hex: "86ff00" },
];

test("ofs-delta encoder encodes known values to expected bytes", () => {
  for (const { n, hex } of known) {
    const enc = encodeOfsDeltaDistance(n);
    assert.strictEqual(toHex(enc), hex, `encoding mismatch for ${n}`);
  }
});

test("ofs-delta encoder round-trips a range of values", () => {
  const values = [
    1, 2, 3, 10, 0x7e, 0x7f, 0x80, 0x81, 0xff, 0x100, 0x1234, 0x3fff, 0x4000, 0x1ffff, 0x20000,
    0x1fffff, 0x200000,
  ];
  for (const n of values) {
    const enc = encodeOfsDeltaDistance(n);
    const dec = decodeOfsDeltaDistance(enc);
    assert.strictEqual(dec, n, `round-trip failed for ${n} (enc=${toHex(enc)})`);
  }
});
