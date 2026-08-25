import { bytesToHex } from "@/worker/common";
import type { Logger } from "@/worker/common/logger";

export type PackReadOptions = {
  limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
  countSubrequest?: (n?: number) => boolean | void;
  signal?: AbortSignal;
  log?: Logger;
  exactLength?: boolean;
  onRead?: (read: { offset: number; length: number }) => void;
};

const RANGE_READ_ATTEMPTS = 3;
const RANGE_READ_RETRY_MS = 100;

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export async function readExactPackRangeWithRetry(
  readAttempt: () => Promise<Uint8Array | undefined>,
  expectedLength: number,
  options?: Pick<PackReadOptions, "signal" | "log"> & {
    retryContext?: () => Promise<Record<string, unknown>>;
  }
): Promise<Uint8Array | undefined> {
  for (let attempt = 1; attempt <= RANGE_READ_ATTEMPTS; attempt++) {
    if (options?.signal?.aborted) return undefined;
    try {
      const bytes = await readAttempt();
      if (!bytes || bytes.byteLength === expectedLength) return bytes;
      throw new Error(`R2 range returned ${bytes.byteLength} of ${expectedLength} bytes`);
    } catch (error) {
      if (options?.signal?.aborted) return undefined;
      if (attempt === RANGE_READ_ATTEMPTS) throw error;
      const context = options?.retryContext ? await options.retryContext() : {};
      options?.log?.warn("r2:range-read-retry", {
        attempt,
        length: expectedLength,
        ...context,
        error: String(error),
      });
      await delay(RANGE_READ_RETRY_MS * attempt);
    }
  }
  return undefined;
}

export type PackHeaderEx = {
  type: number;
  sizeVarBytes: Uint8Array;
  headerLen: number;
  baseOid?: string;
  baseRel?: number;
};

export function decodePackObjectSize(sizeVarBytes: Uint8Array): number | undefined {
  if (sizeVarBytes.byteLength === 0) return undefined;
  let size = sizeVarBytes[0]! & 0x0f;
  let shift = 4;
  for (let index = 1; index < sizeVarBytes.byteLength; index++) {
    size += (sizeVarBytes[index]! & 0x7f) * 2 ** shift;
    if (!Number.isSafeInteger(size)) return undefined;
    shift += 7;
  }
  return size;
}

/**
 * Read a byte range from an R2 `.pack` object.
 */
export async function readPackRange(
  env: Env,
  key: string,
  offset: number,
  length: number,
  options?: PackReadOptions
): Promise<Uint8Array | undefined> {
  if (options?.signal?.aborted) return undefined;

  const read = async () => {
    const run = async () => {
      const obj = await env.REPO_BUCKET.get(key, { range: { offset, length } });
      if (!obj) return undefined;
      const bytes = new Uint8Array(await obj.arrayBuffer());
      options?.onRead?.({ offset, length: bytes.byteLength });
      return bytes;
    };
    options?.countSubrequest?.();
    return options?.limiter ? await options.limiter.run("r2:get-range", run) : await run();
  };

  if (!options?.exactLength) return await read();
  return await readExactPackRangeWithRetry(read, length, {
    ...options,
    retryContext: async () => {
      const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key));
      return {
        packKeyHash: bytesToHex(new Uint8Array(digest)).slice(0, 12),
        offset,
      };
    },
  });
}

/**
 * Read and parse a pack entry header at the given offset.
 */
export async function readPackHeaderEx(
  env: Env,
  key: string,
  offset: number,
  options?: PackReadOptions
): Promise<PackHeaderEx | undefined> {
  const head = await readPackRange(env, key, offset, 128, options);
  if (!head) return undefined;
  return readPackHeaderExFromBuf(head, 0);
}

/**
 * Parse a pack entry header from an in-memory buffer.
 */
export function readPackHeaderExFromBuf(buf: Uint8Array, offset: number): PackHeaderEx | undefined {
  let p = offset;
  if (p >= buf.length) return undefined;

  const start = p;
  let c = buf[p++];
  const type = (c >> 4) & 0x07;
  while (c & 0x80) {
    if (p >= buf.length) return undefined;
    c = buf[p++];
  }

  const sizeVarBytes = buf.subarray(start, p);
  if (type === 7) {
    if (p + 20 > buf.length) return undefined;
    return {
      type,
      sizeVarBytes,
      headerLen: sizeVarBytes.length + 20,
      baseOid: bytesToHex(buf.subarray(p, p + 20)),
    };
  }

  if (type === 6) {
    const ofsStart = p;
    if (p >= buf.length) return undefined;

    let distance = 0;
    let byte = buf[p++];
    distance = byte & 0x7f;
    while (byte & 0x80) {
      if (p >= buf.length) return undefined;
      byte = buf[p++];
      distance = (distance + 1) * 128 + (byte & 0x7f);
      if (!Number.isSafeInteger(distance)) return undefined;
    }

    return {
      type,
      sizeVarBytes,
      headerLen: sizeVarBytes.length + (p - ofsStart),
      baseRel: distance,
    };
  }

  return { type, sizeVarBytes, headerLen: sizeVarBytes.length };
}

/**
 * Returns the encoded byte length of an OFS_DELTA distance without allocating.
 * Use this in convergence loops where only the length matters.
 */
export function ofsDeltaDistanceLength(rel: number): number {
  if (rel <= 0) return 1;
  let current = rel >>> 0;
  let count = 0;
  while (true) {
    count++;
    const group = current & 0x7f;
    current = ((current - group) >>> 7) - 1;
    if (current < 0) break;
  }
  return count;
}

/**
 * Encodes OFS_DELTA distance using Git's varint-with-add-one scheme.
 */
export function encodeOfsDeltaDistance(rel: number): Uint8Array {
  if (rel <= 0) return new Uint8Array([0]);

  let current = rel >>> 0;
  const groups: number[] = [];
  while (true) {
    const group = current & 0x7f;
    groups.push(group);
    current = ((current - group) >>> 7) - 1;
    if (current < 0) break;
  }

  groups.reverse();
  for (let index = 0; index < groups.length - 1; index++) {
    groups[index] |= 0x80;
  }
  return new Uint8Array(groups);
}
