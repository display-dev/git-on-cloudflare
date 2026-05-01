/**
 * Streaming pack indexer — public API.
 */

export { scanPack } from "./scan";
export { resolveDeltasAndWriteIdx } from "./resolve";
export { ResolveAbortedError, isResolveAbortedError } from "./resolve/errors";
export { runPackConnectivityCheck } from "./connectivity";
export { writeIdxV2 } from "./writeIdx";
export { InflateCursor, CRC32_INIT, crc32Update, crc32Finish } from "./inflateCursor";
export { allocateEntryTable, searchOffsetIndex, getRefBaseOidAt } from "./types";
export type {
  PackEntryTable,
  RefBaseOids,
  ScanResult,
  ResolveResult,
  IndexerOptions,
  ResolveOptions,
  ConnectivityCheckOptions,
} from "./types";
