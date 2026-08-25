import type { GitObjectType } from "@/worker/git/core";
import { objTypeCode, encodeObjHeader, concatChunks } from "@/worker/git/core";
import { asBufferSource, deflate } from "@/worker/common";
import { deflate as deflateWithOptions } from "pako";

export type BuildPackV2Options = {
  /** Preserve the runtime default unless a protocol fixture requires exact zlib bytes. */
  compressionLevel?: 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | undefined;
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
