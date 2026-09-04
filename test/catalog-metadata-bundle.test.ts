import { describe, expect, it } from "vitest";

import {
  catalogMetadataFingerprint,
  catalogMetadataBundleKey,
  decodeCatalogMetadataBundle,
  encodeCatalogMetadataBundle,
} from "@/worker/git/nativeReceive/catalogMetadataBundle";

const rows = [
  { packKey: "do/repo/objects/pack/b.pack", packBytes: 80, idxBytes: 3 },
  { packKey: "do/repo/objects/pack/a.pack", packBytes: 60, idxBytes: 2 },
];

describe("catalog metadata bundle", () => {
  it("uses an order-independent exact catalog fingerprint", async () => {
    expect(await catalogMetadataFingerprint(rows)).toBe(
      await catalogMetadataFingerprint([...rows].reverse())
    );
    expect(
      await catalogMetadataFingerprint([
        { ...rows[0]!, packBytes: rows[0]!.packBytes + 1 },
        rows[1]!,
      ])
    ).not.toBe(await catalogMetadataFingerprint(rows));
    expect(await catalogMetadataBundleKey(rows)).toMatch(
      /^do\/repo\/catalog-metadata\/[0-9a-f]{64}\.bin$/
    );
    await expect(
      catalogMetadataBundleKey([rows[0]!, { ...rows[1]!, packKey: "do/other/objects/pack/a.pack" }])
    ).rejects.toThrow("catalog-metadata:pack-prefix-mismatch");
  });

  it("round trips immutable pack metadata in catalog order", () => {
    const entries = rows.map((row, index) => ({
      packKey: row.packKey,
      packBytes: row.packBytes,
      idx: new Uint8Array(row.idxBytes).fill(index + 1),
      refs: new Uint8Array([index + 3, index + 4]),
    }));

    const decoded = decodeCatalogMetadataBundle(
      encodeCatalogMetadataBundle([...entries].reverse()),
      rows
    );

    expect(decoded.map((entry) => entry.packKey)).toEqual(rows.map((row) => row.packKey));
    expect(decoded[0]!.idx).toEqual(entries[0]!.idx);
    expect(decoded[1]!.refs).toEqual(entries[1]!.refs);
  });

  it("rejects a bundle against a different catalog", () => {
    const bytes = encodeCatalogMetadataBundle(
      rows.map((row) => ({
        packKey: row.packKey,
        packBytes: row.packBytes,
        idx: new Uint8Array(row.idxBytes),
        refs: new Uint8Array([1]),
      }))
    );

    expect(() =>
      decodeCatalogMetadataBundle(bytes, [
        { ...rows[0]!, idxBytes: rows[0]!.idxBytes + 1 },
        rows[1]!,
      ])
    ).toThrow("catalog-metadata:catalog-mismatch");
  });

  it("shares the planner's active-pack and metadata limits", () => {
    const entry = (index: number) => ({
      packKey: `do/repo/objects/pack/${index}.pack`,
      packBytes: 60,
      idx: new Uint8Array(2),
      refs: new Uint8Array(1),
    });
    expect(() =>
      encodeCatalogMetadataBundle(Array.from({ length: 65 }, (_, index) => entry(index)))
    ).toThrow("catalog-metadata:entry-count");
    expect(() =>
      encodeCatalogMetadataBundle([{ ...entry(0), refs: new Uint8Array(16 * 1024 * 1024) }])
    ).toThrow("catalog-metadata:bundle-size");
  });
});
