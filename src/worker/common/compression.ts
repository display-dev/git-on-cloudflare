import { asByteTransformStream, createBlobFromBytes } from "./webtypes.ts";

/**
 * Compression and decompression utilities using Web Streams API
 */

async function transformBytes(
  data: Uint8Array,
  stream: CompressionStream | DecompressionStream
): Promise<Uint8Array> {
  const body = createBlobFromBytes(data).stream().pipeThrough(stream);
  const buf = await new Response(body).arrayBuffer();
  return new Uint8Array(buf);
}

/**
 * Compress data using deflate algorithm
 * @param data - Uint8Array to compress
 * @returns Compressed Uint8Array
 */
export async function deflate(data: Uint8Array): Promise<Uint8Array> {
  return transformBytes(data, new CompressionStream("deflate"));
}

/**
 * Decompress data using deflate algorithm
 * @param data - Compressed Uint8Array
 * @returns Decompressed Uint8Array
 */
export async function inflate(data: Uint8Array): Promise<Uint8Array> {
  return transformBytes(data, new DecompressionStream("deflate"));
}

/**
 * Compress data using gzip
 * @param data - Uint8Array to compress
 * @returns Compressed Uint8Array
 */
export async function gzip(data: Uint8Array): Promise<Uint8Array> {
  return transformBytes(data, new CompressionStream("gzip"));
}

/**
 * Decompress data using gzip
 * @param data - Compressed Uint8Array
 * @returns Decompressed Uint8Array
 */
export async function gunzip(data: Uint8Array): Promise<Uint8Array> {
  return transformBytes(data, new DecompressionStream("gzip"));
}

/**
 * Create a deflate compression transform stream
 * @returns TransformStream for compression
 */
export function createDeflateStream(): TransformStream<Uint8Array, Uint8Array> {
  return asByteTransformStream(new CompressionStream("deflate"));
}

/**
 * Create a deflate decompression transform stream
 * @returns TransformStream for decompression
 */
export function createInflateStream(): TransformStream<Uint8Array, Uint8Array> {
  return asByteTransformStream(new DecompressionStream("deflate"));
}
