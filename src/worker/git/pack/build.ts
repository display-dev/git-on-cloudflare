import type { GitObjectType } from "@/worker/git/core";
import { objTypeCode, encodeObjHeader, concatChunks } from "@/worker/git/core";
import { asBufferSource, deflate } from "@/worker/common";
import { deflate as deflateWithOptions } from "pako";
import { computeOidBytes } from "@/worker/git/core/objects";
import { allocateEntryTable } from "@/worker/git/pack/indexer/types";
import { CRC32_INIT, crc32Finish, crc32Update } from "@/worker/git/pack/indexer/inflateCursor";
import { writeIdxV2 } from "@/worker/git/pack/indexer/writeIdx";
import { PackRefsBuilder } from "@/worker/git/pack/refIndex";

export type BuildPackV2Options = {
  /** Preserve the runtime default unless a protocol fixture requires exact zlib bytes. */
  compressionLevel?: 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | undefined;
};

export type PackV2Artifacts = {
  pack: Uint8Array;
  idx: Uint8Array;
  refs: Uint8Array;
};

/**
 * Builds a PACKv2 file from a list of thick objects (no deltas expected here).
 * Shared between upload-pack assembly and DO hydration segment builder.
 */
export async function buildPackV2(
  objs: { type: GitObjectType; payload: Uint8Array }[],
  options: BuildPackV2Options = {}
): Promise<Uint8Array> {
  if (
    options.compressionLevel !== undefined &&
    (!Number.isInteger(options.compressionLevel) ||
      options.compressionLevel < 0 ||
      options.compressionLevel > 9)
  ) {
    throw new Error("pack-build:compression-level");
  }
  // Header: 'PACK' + version (2) + number of objects (big-endian)
  const hdr = new Uint8Array(12);
  hdr.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(hdr.buffer);
  dv.setUint32(4, 2); // version 2
  dv.setUint32(8, objs.length);

  const parts: Uint8Array[] = [hdr];
  for (const o of objs) {
    const typeCode = objTypeCode(o.type);
    const head = encodeObjHeader(typeCode, o.payload.byteLength);
    parts.push(head);
    const comp =
      options.compressionLevel === undefined
        ? await deflate(o.payload)
        : deflateWithOptions(o.payload, { level: options.compressionLevel });
    parts.push(comp);
  }
  const body = concatChunks(parts);
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(body)));
  const out = new Uint8Array(body.length + 20);
  out.set(body, 0);
  out.set(sha, body.length);
  return out;
}

/** Build one self-contained pack and its deterministic IDX/PREF authority entirely in memory. */
export async function buildPackV2Artifacts(
  objs: { type: GitObjectType; payload: Uint8Array }[],
  options: BuildPackV2Options = {}
): Promise<PackV2Artifacts> {
  if (
    options.compressionLevel !== undefined &&
    (!Number.isInteger(options.compressionLevel) ||
      options.compressionLevel < 0 ||
      options.compressionLevel > 9)
  ) {
    throw new Error("pack-build:compression-level");
  }
  const header = new Uint8Array(12);
  header.set(new TextEncoder().encode("PACK"), 0);
  const headerView = new DataView(header.buffer);
  headerView.setUint32(4, 2);
  headerView.setUint32(8, objs.length);

  const table = allocateEntryTable(objs.length);
  const refsBuilder = new PackRefsBuilder(objs.length);
  const parts: Uint8Array[] = [header];
  let offset = header.byteLength;
  for (let index = 0; index < objs.length; index++) {
    const object = objs[index]!;
    const typeCode = objTypeCode(object.type);
    const entryHeader = encodeObjHeader(typeCode, object.payload.byteLength);
    const compressed =
      options.compressionLevel === undefined
        ? await deflate(object.payload)
        : deflateWithOptions(object.payload, { level: options.compressionLevel });
    const oid = await computeOidBytes(object.type, object.payload);
    let crc = crc32Update(CRC32_INIT, entryHeader);
    crc = crc32Update(crc, compressed);

    table.offsets[index] = offset;
    table.types[index] = typeCode;
    table.objectTypes[index] = typeCode;
    table.headerLens[index] = entryHeader.byteLength;
    table.spanEnds[index] = offset + entryHeader.byteLength + compressed.byteLength;
    table.crc32s[index] = crc32Finish(crc);
    table.oids.set(oid, index * 20);
    table.decompressedSizes[index] = object.payload.byteLength;
    table.resolved[index] = 1;
    refsBuilder.recordObject(index, object.type, object.payload);
    parts.push(entryHeader, compressed);
    offset = table.spanEnds[index]!;
  }

  const body = concatChunks(parts);
  const packChecksum = new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(body)));
  const pack = new Uint8Array(body.byteLength + packChecksum.byteLength);
  pack.set(body, 0);
  pack.set(packChecksum, body.byteLength);
  const idx = await writeIdxV2(table, objs.length, packChecksum);
  const refs = refsBuilder.build({
    table,
    objectCount: objs.length,
    packBytes: pack.byteLength,
    packChecksum,
    idxChecksum: idx.subarray(idx.byteLength - 20),
  }).bytes;
  return { pack, idx, refs };
}
