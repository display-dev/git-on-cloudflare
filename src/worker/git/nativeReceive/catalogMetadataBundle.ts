import type { PackCatalogRow } from "@/worker/do/repo/db/schema";
import type { Logger } from "@/worker/common/logger";
import type { Limiter } from "@/worker/git/operations/limits";

import { asBufferSource, bytesToHex } from "@/worker/common";
import {
  STOCK_ACTIVE_PACK_MAX_COUNT,
  STOCK_METADATA_MAX_BYTES,
} from "@/worker/git/nativeReceive/types";

const MAGIC = new TextEncoder().encode("GOCMETA1");
const FIXED_ENTRY_BYTES = 4 + 8 + 4 + 4;
const MAX_BUNDLE_BYTES = STOCK_METADATA_MAX_BYTES;
const MAX_ENTRIES = STOCK_ACTIVE_PACK_MAX_COUNT;

export type CatalogMetadataBundleEntry = {
  packKey: string;
  packBytes: number;
  idx: Uint8Array;
  refs: Uint8Array;
};

export function catalogMetadataBundleEnabled(env: Env): boolean {
  return (
    (env as Env & { STOCK_RECEIVE_CATALOG_METADATA_BUNDLE?: string })
      .STOCK_RECEIVE_CATALOG_METADATA_BUNDLE === "1"
  );
}

function writeUint64(view: DataView, offset: number, value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error("catalog-metadata:pack-size");
  view.setUint32(offset, Math.floor(value / 0x1_0000_0000), false);
  view.setUint32(offset + 4, value >>> 0, false);
}

function readUint64(view: DataView, offset: number): number {
  const value = view.getUint32(offset, false) * 0x1_0000_0000 + view.getUint32(offset + 4, false);
  if (!Number.isSafeInteger(value)) throw new Error("catalog-metadata:pack-size");
  return value;
}

function orderedCatalog(rows: Array<Pick<PackCatalogRow, "packKey" | "packBytes" | "idxBytes">>) {
  return [...rows].sort((left, right) =>
    left.packKey < right.packKey ? -1 : left.packKey > right.packKey ? 1 : 0
  );
}

export async function catalogMetadataFingerprint(
  rows: Array<Pick<PackCatalogRow, "packKey" | "packBytes" | "idxBytes">>
): Promise<string> {
  const descriptor = orderedCatalog(rows).map(({ packKey, packBytes, idxBytes }) => ({
    packKey,
    packBytes,
    idxBytes,
  }));
  const bytes = new TextEncoder().encode(JSON.stringify(descriptor));
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

export async function catalogMetadataBundleKey(
  rows: Array<Pick<PackCatalogRow, "packKey" | "packBytes" | "idxBytes">>
): Promise<string> {
  const marker = "/objects/pack/";
  const prefixes = new Set(
    rows.map((row) => {
      const markerIndex = row.packKey.indexOf(marker);
      if (markerIndex <= 0) throw new Error("catalog-metadata:pack-key-shape");
      return row.packKey.slice(0, markerIndex);
    })
  );
  if (prefixes.size !== 1) throw new Error("catalog-metadata:pack-prefix-mismatch");
  const prefix = prefixes.values().next().value;
  if (!prefix) throw new Error("catalog-metadata:empty-catalog");
  return `${prefix}/catalog-metadata/${await catalogMetadataFingerprint(rows)}.bin`;
}

export function encodeCatalogMetadataBundle(entries: CatalogMetadataBundleEntry[]): Uint8Array {
  if (entries.length > MAX_ENTRIES) throw new Error("catalog-metadata:entry-count");
  const encoder = new TextEncoder();
  const ordered = [...entries].sort((left, right) =>
    left.packKey < right.packKey ? -1 : left.packKey > right.packKey ? 1 : 0
  );
  const encodedKeys = ordered.map((entry) => encoder.encode(entry.packKey));
  const total = ordered.reduce((sum, entry, index) => {
    if (entry.idx.byteLength > 0xffff_ffff || entry.refs.byteLength > 0xffff_ffff) {
      throw new Error("catalog-metadata:sidecar-size");
    }
    return (
      sum +
      FIXED_ENTRY_BYTES +
      encodedKeys[index]!.byteLength +
      entry.idx.byteLength +
      entry.refs.byteLength
    );
  }, MAGIC.byteLength + 4);
  if (total > MAX_BUNDLE_BYTES) throw new Error("catalog-metadata:bundle-size");

  const bytes = new Uint8Array(total);
  const view = new DataView(bytes.buffer);
  bytes.set(MAGIC, 0);
  view.setUint32(MAGIC.byteLength, ordered.length, false);
  let offset = MAGIC.byteLength + 4;
  for (let index = 0; index < ordered.length; index++) {
    const entry = ordered[index]!;
    const key = encodedKeys[index]!;
    view.setUint32(offset, key.byteLength, false);
    writeUint64(view, offset + 4, entry.packBytes);
    view.setUint32(offset + 12, entry.idx.byteLength, false);
    view.setUint32(offset + 16, entry.refs.byteLength, false);
    offset += 20;
    bytes.set(key, offset);
    offset += key.byteLength;
    bytes.set(entry.idx, offset);
    offset += entry.idx.byteLength;
    bytes.set(entry.refs, offset);
    offset += entry.refs.byteLength;
  }
  return bytes;
}

export function decodeCatalogMetadataBundle(
  bytes: Uint8Array,
  expected: Array<Pick<PackCatalogRow, "packKey" | "packBytes" | "idxBytes">>
): CatalogMetadataBundleEntry[] {
  if (bytes.byteLength < MAGIC.byteLength + 4 || bytes.byteLength > MAX_BUNDLE_BYTES) {
    throw new Error("catalog-metadata:bundle-size");
  }
  if (MAGIC.some((value, index) => bytes[index] !== value)) {
    throw new Error("catalog-metadata:magic");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint32(MAGIC.byteLength, false);
  if (count > MAX_ENTRIES || count !== expected.length) {
    throw new Error("catalog-metadata:entry-count");
  }
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const entries: CatalogMetadataBundleEntry[] = [];
  let offset = MAGIC.byteLength + 4;
  for (let index = 0; index < count; index++) {
    if (offset + 20 > bytes.byteLength) throw new Error("catalog-metadata:truncated");
    const keyBytes = view.getUint32(offset, false);
    const packBytes = readUint64(view, offset + 4);
    const idxBytes = view.getUint32(offset + 12, false);
    const refsBytes = view.getUint32(offset + 16, false);
    offset += 20;
    const end = offset + keyBytes + idxBytes + refsBytes;
    if (end > bytes.byteLength) throw new Error("catalog-metadata:truncated");
    const packKey = decoder.decode(bytes.subarray(offset, offset + keyBytes));
    offset += keyBytes;
    const idx = bytes.slice(offset, offset + idxBytes);
    offset += idxBytes;
    const refs = bytes.slice(offset, offset + refsBytes);
    offset += refsBytes;
    entries.push({ packKey, packBytes, idx, refs });
  }
  if (offset !== bytes.byteLength) throw new Error("catalog-metadata:trailing-bytes");

  const orderedExpected = orderedCatalog(expected);
  for (let index = 0; index < entries.length; index++) {
    const entry = entries[index]!;
    const row = orderedExpected[index]!;
    if (
      entry.packKey !== row.packKey ||
      entry.packBytes !== row.packBytes ||
      entry.idx.byteLength !== row.idxBytes
    ) {
      throw new Error("catalog-metadata:catalog-mismatch");
    }
  }
  const byKey = new Map(entries.map((entry) => [entry.packKey, entry]));
  return expected.map((row) => {
    const entry = byKey.get(row.packKey);
    if (!entry) throw new Error("catalog-metadata:catalog-mismatch");
    return entry;
  });
}

export const CATALOG_METADATA_BUNDLE_MAX_BYTES = MAX_BUNDLE_BYTES;

async function sha256(bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

export async function readCatalogMetadataBundle(args: {
  env: Env;
  catalog: Array<Pick<PackCatalogRow, "packKey" | "packBytes" | "idxBytes">>;
  limiter: Limiter;
  countSubrequest: (n?: number) => void;
  observeBytes?: ((bytes: number) => void) | undefined;
  log: Logger;
}): Promise<
  | {
      key: string;
      bytes: number;
      sha256: string;
      etag: string;
      entries: CatalogMetadataBundleEntry[];
    }
  | undefined
> {
  const key = await catalogMetadataBundleKey(args.catalog);
  args.countSubrequest();
  try {
    const object = await args.limiter.run("r2:stock-catalog-metadata", () =>
      args.env.REPO_BUCKET.get(key)
    );
    if (!object) return undefined;
    if (object.size > MAX_BUNDLE_BYTES) throw new Error("catalog-metadata:bundle-size");
    const bytes = new Uint8Array(await object.arrayBuffer());
    args.observeBytes?.(bytes.byteLength);
    const digest = await sha256(bytes);
    if (object.customMetadata?.sha256 !== digest) {
      throw new Error("catalog-metadata:digest-mismatch");
    }
    return {
      key,
      bytes: bytes.byteLength,
      sha256: digest,
      etag: object.etag,
      entries: decodeCatalogMetadataBundle(bytes, args.catalog),
    };
  } catch (error) {
    args.log.warn("stock-plan:catalog-metadata-invalid", { key, error: String(error) });
    return undefined;
  }
}

export async function putCatalogMetadataBundle(args: {
  env: Env;
  entries: CatalogMetadataBundleEntry[];
  limiter: Limiter;
  countSubrequest: (n?: number) => void;
}): Promise<{ key: string; bytes: number; sha256: string }> {
  const catalog = args.entries.map((entry) => ({
    packKey: entry.packKey,
    packBytes: entry.packBytes,
    idxBytes: entry.idx.byteLength,
  }));
  const key = await catalogMetadataBundleKey(catalog);
  const bytes = encodeCatalogMetadataBundle(args.entries);
  const digest = await sha256(bytes);
  args.countSubrequest();
  const created = await args.limiter.run("r2:put-stock-catalog-metadata", () =>
    args.env.REPO_BUCKET.put(key, bytes, {
      onlyIf: { etagDoesNotMatch: "*" },
      httpMetadata: { contentType: "application/octet-stream" },
      customMetadata: { sha256: digest },
    })
  );
  if (!created) {
    args.countSubrequest();
    const existing = await args.limiter.run("r2:get-stock-catalog-metadata", () =>
      args.env.REPO_BUCKET.get(key)
    );
    if (!existing || existing.size !== bytes.byteLength) {
      throw new Error("catalog-metadata:immutable-conflict");
    }
    const existingBytes = new Uint8Array(await existing.arrayBuffer());
    if ((await sha256(existingBytes)) !== digest) {
      throw new Error("catalog-metadata:immutable-conflict");
    }
  }
  return { key, bytes: bytes.byteLength, sha256: digest };
}
