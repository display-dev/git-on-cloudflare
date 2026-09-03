import type { CacheContext } from "@/worker/cache";
import type { Logger } from "@/worker/common/logger";
import type { Limiter } from "@/worker/git/operations/limits";
import type { ReceiveCommand } from "@/worker/git/operations/validation";
import type {
  IndexedPackSource,
  PackedObjectCandidate,
} from "@/worker/git/object-store/candidates";
import type { IdxView, PackCatalogRow } from "@/worker/git/object-store/types";
import type { PackRefView } from "@/worker/git/pack/refIndex";
import type {
  StockPhysicalDependencyEdge,
  StockPhysicalDependencyPlan,
  StockPhysicalNode,
} from "@/worker/git/nativeReceive/physicalDependencyPlan";

import { asBufferSource, bytesEqual, bytesToHex, createLogger } from "@/worker/common";
import { buildPackV2 } from "@/worker/git/pack/build";
import { createStockPhysicalDependencyPlanner } from "@/worker/git/nativeReceive/physicalDependencyPlan";
import { resolveDeltasAndWriteIdx } from "@/worker/git/pack/indexer/resolve";
import { scanPack } from "@/worker/git/pack/indexer/scan";
import { findOidIndex, getOidHexAt, parseIdxView } from "@/worker/git/object-store";
import { readExactPackRangeWithRetry, readPackRange } from "@/worker/git/pack/packMeta";
import {
  getPackRefObjectType,
  getPackRefRefsAt,
  parsePackRefView,
} from "@/worker/git/pack/refIndex";
import { packIndexKey, packRefsKey } from "@/worker/keys";

const MAX_ACTIVE_PACKS = 64;
const MAX_METADATA_BYTES = 16 * 1024 * 1024;
const MAX_MANIFEST_BYTES = 512 * 1024;
const MAX_INPUT_PACK_BYTES = 16 * 1024 * 1024;
const MAX_OBJECT_BYTES = 8 * 1024 * 1024;
const MAX_PREREQUISITE_PAYLOAD_BYTES = 16 * 1024 * 1024;
const MAX_CLOSURE_OBJECTS = 100_000;
const MAX_CLOSURE_EDGES = 500_000;
const MAX_RANGE_RECORDS = 256;
const WHOLE_ACTIVE_PACK_MAX_BYTES = 8 * 1024 * 1024;
const WHOLE_ACTIVE_PACK_TOTAL_BUDGET = 16 * 1024 * 1024;
const ZERO_OID = "0".repeat(40);

function wholeActivePackFits(packBytes: number, reservedBytes: number): boolean {
  return (
    packBytes <= WHOLE_ACTIVE_PACK_MAX_BYTES &&
    reservedBytes + packBytes <= WHOLE_ACTIVE_PACK_TOTAL_BUDGET
  );
}

function selectWholeActivePackKeys(
  active: Array<{ source: { packKey: string; packBytes: number }; packChecksum: string }>
): Set<string> {
  const selected = new Set<string>();
  let reservedBytes = 0;
  const ordered = [...active].sort(
    (left, right) =>
      left.packChecksum.localeCompare(right.packChecksum) ||
      left.source.packKey.localeCompare(right.source.packKey)
  );
  for (const pack of ordered) {
    if (!wholeActivePackFits(pack.source.packBytes, reservedBytes)) continue;
    selected.add(pack.source.packKey);
    reservedBytes += pack.source.packBytes;
  }
  return selected;
}

type WrongPrerequisiteRangeFault = {
  operationId: string;
  hook: "wrong-prerequisite-range" | "shifted-physical-base-range" | "touched-active-pack-entry";
  triggered: boolean;
  bytesMutated?: boolean | undefined;
  startedAt: number;
  requiredOid?: string | undefined;
  packKey?: string | undefined;
  packChecksum?: string | undefined;
  start?: number | undefined;
  end?: number | undefined;
  metadataBytes?: number | undefined;
  metadataRequests?: number | undefined;
  inputBytesRead?: number | undefined;
  inputRequests?: number | undefined;
  rangeBytes?: number | undefined;
  rangeRequests?: number | undefined;
  activePackReads?: StockObservedActivePackRead[] | undefined;
  activePackRangeBytes?: number | undefined;
  activePackRangeRequests?: number | undefined;
  activePackCount?: number | undefined;
  selectedPackCount?: number | undefined;
  selectedPackBytes?: number | undefined;
  elapsedMs?: number | undefined;
};

type TransientR2ReadFault = {
  operationId: string;
  triggered: boolean;
  startedAt: number;
  metadataBytes?: number | undefined;
  metadataRequests?: number | undefined;
  inputBytesRead?: number | undefined;
  inputRequests?: number | undefined;
  rangeBytes?: number | undefined;
  rangeRequests?: number | undefined;
  elapsedMs?: number | undefined;
};

let wrongPrerequisiteRangeFaultForTesting: WrongPrerequisiteRangeFault | undefined;
let transientR2ReadFaultForTesting: TransientR2ReadFault | undefined;
let unexpectedWholePackReadOperationForTesting: string | undefined;
let transientWholePackReadForTesting: { operationId: string; attempts: number } | undefined;
let shortWholePackReadForTesting: { operationId: string; attempts: number } | undefined;

export const __test = {
  failWrongPrerequisiteRange(operationId: string): void {
    wrongPrerequisiteRangeFaultForTesting = {
      operationId,
      hook: "wrong-prerequisite-range",
      triggered: false,
      startedAt: Date.now(),
    };
  },
  failShiftedPhysicalBaseRange(operationId: string): void {
    wrongPrerequisiteRangeFaultForTesting = {
      operationId,
      hook: "shifted-physical-base-range",
      triggered: false,
      startedAt: Date.now(),
    };
  },
  failTouchedActivePackEntry(operationId: string): void {
    wrongPrerequisiteRangeFaultForTesting = {
      operationId,
      hook: "touched-active-pack-entry",
      triggered: false,
      bytesMutated: false,
      startedAt: Date.now(),
    };
  },
  wrongPrerequisiteRangeFault(operationId: string): WrongPrerequisiteRangeFault | undefined {
    return wrongPrerequisiteRangeFaultForTesting?.operationId === operationId
      ? { ...wrongPrerequisiteRangeFaultForTesting }
      : undefined;
  },
  failTransientR2Read(operationId: string): void {
    transientR2ReadFaultForTesting = {
      operationId,
      triggered: false,
      startedAt: Date.now(),
    };
  },
  transientR2ReadFault(operationId: string): TransientR2ReadFault | undefined {
    return transientR2ReadFaultForTesting?.operationId === operationId
      ? { ...transientR2ReadFaultForTesting }
      : undefined;
  },
  readWholeActivePack(operationId: string): void {
    unexpectedWholePackReadOperationForTesting = operationId;
  },
  failTransientWholePackRead(operationId: string): void {
    transientWholePackReadForTesting = { operationId, attempts: 0 };
  },
  transientWholePackReadAttempts(operationId: string): number | undefined {
    return transientWholePackReadForTesting?.operationId === operationId
      ? transientWholePackReadForTesting.attempts
      : undefined;
  },
  returnShortWholePackRead(operationId: string): void {
    shortWholePackReadForTesting = { operationId, attempts: 0 };
  },
  shortWholePackReadAttempts(operationId: string): number | undefined {
    return shortWholePackReadForTesting?.operationId === operationId
      ? shortWholePackReadForTesting.attempts
      : undefined;
  },
  wholeActivePackFits(packBytes: number, reservedBytes: number): boolean {
    return wholeActivePackFits(packBytes, reservedBytes);
  },
  selectWholeActivePackKeys(
    active: Array<{ packKey: string; packBytes: number; packChecksum: string }>
  ): string[] {
    return [
      ...selectWholeActivePackKeys(
        active.map(({ packKey, packBytes, packChecksum }) => ({
          source: { packKey, packBytes },
          packChecksum,
        }))
      ),
    ].sort();
  },
  reset(): void {
    wrongPrerequisiteRangeFaultForTesting = undefined;
    transientR2ReadFaultForTesting = undefined;
    unexpectedWholePackReadOperationForTesting = undefined;
    transientWholePackReadForTesting = undefined;
    shortWholePackReadForTesting = undefined;
  },
};

function throwTransientR2ReadFault(
  operationId: string,
  counters: PlannerCounters,
  activePackCount: number
): void {
  const fault = transientR2ReadFaultForTesting;
  if (!fault || fault.operationId !== operationId || fault.triggered) return;
  fault.triggered = true;
  fault.metadataBytes = counters.metadataBytes;
  fault.metadataRequests = counters.metadataRequests;
  fault.inputBytesRead = counters.inputBytesRead;
  fault.inputRequests = counters.inputRequests;
  fault.rangeBytes = 0;
  fault.rangeRequests = 0;
  fault.elapsedMs = Math.max(0, Date.now() - fault.startedAt);
  throw new StockReceivePlannerError("r2-transient", {
    elapsedMs: fault.elapsedMs,
    metadataBytes: counters.metadataBytes,
    metadataRequests: counters.metadataRequests,
    inputBytesRead: counters.inputBytesRead,
    inputRequests: counters.inputRequests,
    ranges: [],
    rangeBytes: 0,
    rangeRequests: 0,
    activePackReads: [],
    activePackTrailerBytes: 0,
    activePackTrailerRequests: 0,
    activePackRangeBytes: 0,
    activePackRangeRequests: 0,
    activePackWholeBytes: 0,
    activePackWholeRequests: 0,
    activePackUnattributedBytes: 0,
    activePackUnattributedRequests: 0,
    packsTouched: 0,
    selectedPackBytes: 0,
    activePackCount,
  });
}

function candidateWithWrongRangeFault(
  operationId: string,
  requiredOid: string,
  candidate: PackedObjectCandidate,
  activePackCount: number
): PackedObjectCandidate {
  const fault = wrongPrerequisiteRangeFaultForTesting;
  if (!fault || fault.operationId !== operationId || fault.triggered) return candidate;
  if (fault.hook === "shifted-physical-base-range" && candidate.oid === requiredOid) {
    return candidate;
  }
  if (fault.hook === "touched-active-pack-entry") {
    fault.triggered = true;
    fault.requiredOid = requiredOid;
    fault.packKey = candidate.source.packKey;
    fault.packChecksum = bytesToHex(candidate.source.idx.packChecksum);
    fault.start = candidate.offset;
    fault.end = candidate.nextOffset;
    fault.activePackCount = activePackCount;
    fault.selectedPackCount = 1;
    fault.selectedPackBytes = candidate.source.packBytes;
    return candidate;
  }
  const shift = candidate.nextOffset < candidate.source.packBytes ? 1 : -1;
  if (candidate.offset + shift < 0 || candidate.nextOffset + shift > candidate.source.packBytes) {
    throw new Error("stock-plan:wrong-prerequisite-range-unavailable");
  }
  const shifted = {
    ...candidate,
    offset: candidate.offset + shift,
    nextOffset: candidate.nextOffset + shift,
  };
  fault.triggered = true;
  fault.requiredOid = requiredOid;
  fault.packKey = shifted.source.packKey;
  fault.packChecksum = bytesToHex(shifted.source.idx.packChecksum);
  fault.start = shifted.offset;
  fault.end = shifted.nextOffset;
  fault.activePackCount = activePackCount;
  fault.selectedPackCount = 1;
  fault.selectedPackBytes = shifted.source.packBytes;
  return shifted;
}

function recordWrongPrerequisiteRangeRead(
  operationId: string,
  requiredOid: string,
  candidate: PackedObjectCandidate,
  length: number,
  counters: PlannerCounters
): void {
  const fault = wrongPrerequisiteRangeFaultForTesting;
  if (
    !fault ||
    fault.operationId !== operationId ||
    !fault.triggered ||
    fault.requiredOid !== requiredOid ||
    fault.packKey !== candidate.source.packKey ||
    fault.start !== candidate.offset ||
    fault.end !== candidate.nextOffset
  ) {
    return;
  }
  fault.metadataBytes = counters.metadataBytes;
  fault.metadataRequests = counters.metadataRequests;
  fault.inputBytesRead = counters.inputBytesRead;
  fault.inputRequests = counters.inputRequests;
  fault.rangeBytes = length;
  fault.rangeRequests = 1;
  fault.activePackReads = [
    {
      packChecksum: bytesToHex(candidate.source.idx.packChecksum),
      start: candidate.offset,
      end: candidate.nextOffset,
      returnedBytes: length,
      kind: "required-object",
      requiredOid,
    },
  ];
  fault.activePackRangeBytes = length;
  fault.activePackRangeRequests = 1;
  fault.elapsedMs = Math.max(0, Date.now() - fault.startedAt);
}

export type StockPlannerActivePack = {
  packKey: string;
  packBytes: number;
  idxBytes: number;
  /** Expected authority digests supplied by the frozen layout manifest. */
  packChecksum?: string;
  idxSha256?: string;
  refsSha256?: string;
};

export type StockRequiredRange = {
  entryId: string;
  packChecksum: string;
  start: number;
  end: number;
  reason: "required-object";
  /** Canonical OID of this physical pack entry (legacy evidence name). */
  requiredOid: string;
  /** Semantic prerequisite roots whose encoding path crosses this entry. */
  semanticRootOids: string[];
};

export type StockObservedActivePackRead =
  | {
      packChecksum: string;
      start: number;
      end: number;
      returnedBytes: number;
      kind: "trailer";
    }
  | {
      packChecksum: string;
      start: number;
      end: number;
      returnedBytes: number;
      kind: "required-object";
      requiredOid: string;
    }
  | {
      packChecksum: string;
      start: number;
      end: number;
      returnedBytes: number;
      kind: "whole";
    };

export type StockReceivePlan = {
  executionMode: "stock-container" | "direct-pack";
  directPackKey?: string | undefined;
  directPackBytes?: number | undefined;
  directPackSha1?: string | undefined;
  directPackSha256?: string | undefined;
  directIdxKey?: string | undefined;
  directIdxBytes?: number | undefined;
  directIdxSha256?: string | undefined;
  directRefsKey?: string | undefined;
  directRefsBytes?: number | undefined;
  directRefsSha256?: string | undefined;
  directOutputUploadMs?: number | undefined;
  incomingOids?: string[] | undefined;
  prerequisitePackKey: string;
  prerequisitePackBytes: number;
  prerequisitePackSha256: string;
  prerequisitePackEtag: string;
  closureManifestKey: string;
  closureManifestBytes: number;
  closureManifestSha256: string;
  closureManifestEtag: string;
  planSha256: string;
  advertisedReachableOids: string[];
  semanticExternalOids: string[];
  thinDeltaBaseOids: string[];
  requiredRootOids: string[];
  physicalNodes: StockPhysicalNode[];
  dependencies: StockPhysicalDependencyEdge[];
  topologicalEntryIds: string[];
  selectedPackChecksums: string[];
  activePackBindings: Array<{
    packKey: string;
    packBytes: number;
    idxBytes: number;
    packChecksum: string;
    idxSha256: string;
    prefSha256: string;
  }>;
  incomingObjectCount: number;
  visitedIncomingObjectCount: number;
  logicalEdgeCount: number;
  internalEdgeCount: number;
  externalEdgeCount: number;
  missingObjectCount: number;
  objectTypeCounts: Record<"commit" | "tree" | "blob" | "tag", number>;
  ranges: StockRequiredRange[];
  rangeBytes: number;
  rangeRequests: number;
  activePackReads: StockObservedActivePackRead[];
  activePackTrailerBytes: number;
  activePackTrailerRequests: number;
  activePackRangeBytes: number;
  activePackRangeRequests: number;
  activePackWholeBytes: number;
  activePackWholeRequests: number;
  activePackUnattributedBytes: number;
  activePackUnattributedRequests: number;
  packsTouched: number;
  selectedPackBytes: number;
  activePackCount: number;
  metadataBytes: number;
  metadataRequests: number;
  inputBytesRead: number;
  inputRequests: number;
  prerequisitePayloadBytes: number;
  prerequisiteHydratedBytes: number;
};

export type StockPlannerFailureMetrics = Pick<
  StockReceivePlan,
  | "metadataBytes"
  | "metadataRequests"
  | "inputBytesRead"
  | "inputRequests"
  | "ranges"
  | "rangeBytes"
  | "rangeRequests"
  | "activePackReads"
  | "activePackTrailerBytes"
  | "activePackTrailerRequests"
  | "activePackRangeBytes"
  | "activePackRangeRequests"
  | "activePackWholeBytes"
  | "activePackWholeRequests"
  | "activePackUnattributedBytes"
  | "activePackUnattributedRequests"
  | "packsTouched"
  | "selectedPackBytes"
  | "activePackCount"
> & { elapsedMs: number };

export class StockReceivePlannerError extends Error {
  constructor(
    readonly code: "r2-transient" | "replacement-closure-invalid",
    readonly metrics: StockPlannerFailureMetrics
  ) {
    super(`stock-plan:${code}`);
    this.name = "StockReceivePlannerError";
  }
}

export type PlanStockReceiveArgs = {
  env: Env;
  repoId: string;
  operationId: string;
  inputRequestKey: string;
  inputRequestBytes: number;
  inputRequestSha256: string;
  outputPackKey?: string | undefined;
  outputIdxKey?: string | undefined;
  outputRefsKey?: string | undefined;
  packOffset: number;
  packBytes: number;
  advertisedRefs: Array<{ name: string; oid: string }>;
  commands: ReceiveCommand[];
  activePacks: StockPlannerActivePack[];
  cacheCtx: CacheContext;
  limiter: Limiter;
  countSubrequest: (n?: number) => void;
  /** E1-only root enumeration; it must exactly permute the roots derived below. */
  rootIterationOidsForTesting?: string[] | undefined;
  signal?: AbortSignal;
  log?: Logger;
};

type BoundActivePack = {
  source: IndexedPackSource;
  idxBytes: number;
  refs: PackRefView;
  packChecksum: string;
  idxSha256: string;
  refsSha256: string;
};

type PlannerCounters = {
  metadataBytes: number;
  metadataRequests: number;
  inputBytesRead: number;
  inputRequests: number;
};

type RuntimeActivePackReadKind = "trailer" | "body-range" | "whole" | "unattributed";

type RuntimeActivePackRead = {
  packKey: string;
  start?: number | undefined;
  end?: number | undefined;
  returnedBytes: number;
  consumed: boolean;
  kind: RuntimeActivePackReadKind;
};

type ActivePackReadObservation = {
  env: Env;
  reads: RuntimeActivePackRead[];
};

type ReconciledActivePackReads = {
  activePackReads: StockObservedActivePackRead[];
  ranges: StockRequiredRange[];
  trailerBytes: number;
  trailerRequests: number;
  rangeBytes: number;
  rangeRequests: number;
  wholeBytes: number;
  wholeRequests: number;
  unattributedBytes: number;
  unattributedRequests: number;
};

function assertSafeBytes(value: number, name: string, maximum: number): void {
  if (!Number.isSafeInteger(value) || value < 0 || value > maximum) {
    throw new Error(`stock-plan:${name}-limit`);
  }
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw new Error("stock-plan:aborted");
}

async function digestHex(algorithm: "SHA-1" | "SHA-256", bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest(algorithm, asBufferSource(bytes))));
}

function requestedActivePackRange(
  options: R2GetOptions | undefined
): { offset: number; length: number } | undefined {
  const range = options?.range;
  if (!range || range instanceof Headers || !("offset" in range) || !("length" in range)) {
    return undefined;
  }
  const offset = range.offset;
  const length = range.length;
  if (
    typeof offset !== "number" ||
    typeof length !== "number" ||
    !Number.isSafeInteger(offset) ||
    !Number.isSafeInteger(length) ||
    offset < 0 ||
    length <= 0
  ) {
    return undefined;
  }
  return { offset, length };
}

function observeReturnedActivePackBody(
  object: R2ObjectBody,
  read: RuntimeActivePackRead,
  discard: () => void,
  expectedLength?: number,
  beforeConsume?: () => void,
  transformBytes?: (bytes: Uint8Array) => Uint8Array
): R2ObjectBody {
  let observedBody: ReadableStream<Uint8Array> | undefined;
  let consumptionStarted = false;
  const startConsumption = (): void => {
    if (consumptionStarted) return;
    consumptionStarted = true;
    beforeConsume?.();
  };
  const record = (returnedBytes: number): void => {
    if (read.consumed) throw new Error("stock-plan:active-pack-body-consumed-twice");
    if (expectedLength !== undefined && returnedBytes !== expectedLength) {
      discard();
      return;
    }
    read.consumed = true;
    read.returnedBytes = returnedBytes;
  };
  const streamBody = (): ReadableStream<Uint8Array> => {
    if (observedBody) return observedBody;
    const reader: ReadableStreamDefaultReader<Uint8Array> = object.body.getReader();
    let returnedBytes = 0;
    observedBody = new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          startConsumption();
          const next = await reader.read();
          if (next.done) {
            record(returnedBytes);
            controller.close();
            return;
          }
          returnedBytes += next.value.byteLength;
          controller.enqueue(next.value);
        } catch (error) {
          discard();
          controller.error(error);
        }
      },
      async cancel(reason?: unknown) {
        await reader.cancel(reason);
        record(returnedBytes);
      },
    });
    return observedBody;
  };
  return new Proxy(object, {
    get(target, property) {
      if (property === "arrayBuffer") {
        return async (): Promise<ArrayBuffer> => {
          try {
            startConsumption();
            const raw = await target.arrayBuffer();
            if (!transformBytes) {
              record(raw.byteLength);
              return raw;
            }
            const returned = transformBytes(new Uint8Array(raw));
            record(returned.byteLength);
            return returned.buffer.slice(
              returned.byteOffset,
              returned.byteOffset + returned.byteLength
            ) as ArrayBuffer;
          } catch (error) {
            discard();
            throw error;
          }
        };
      }
      if (property === "bytes") {
        return async (): Promise<Uint8Array> => {
          try {
            startConsumption();
            const bytes = await target.bytes();
            const returned = transformBytes?.(bytes) ?? bytes;
            record(returned.byteLength);
            return returned;
          } catch (error) {
            discard();
            throw error;
          }
        };
      }
      if (property === "body") return streamBody();
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function observeActivePackReads(
  env: Env,
  activePacks: StockPlannerActivePack[],
  operationId: string
): ActivePackReadObservation {
  const byKey = new Map(activePacks.map((pack) => [pack.packKey, pack]));
  const reads: RuntimeActivePackRead[] = [];
  const bucket = new Proxy(env.REPO_BUCKET, {
    get(target, property) {
      if (property === "get") {
        return async (key: string, options?: R2GetOptions) => {
          const pack = byKey.get(key);
          let read: RuntimeActivePackRead | undefined;
          if (pack) {
            const range = requestedActivePackRange(options);
            if (!options?.range) {
              read = {
                packKey: key,
                returnedBytes: 0,
                consumed: false,
                kind: "whole",
              };
            } else if (!range) {
              read = {
                packKey: key,
                returnedBytes: 0,
                consumed: false,
                kind: "unattributed",
              };
            } else {
              const end = range.offset + range.length;
              const kind: RuntimeActivePackReadKind =
                range.offset === pack.packBytes - 20 && range.length === 20
                  ? "trailer"
                  : range.offset >= 0 && end <= pack.packBytes - 20
                    ? "body-range"
                    : "unattributed";
              read = {
                packKey: key,
                start: range.offset,
                end,
                returnedBytes: 0,
                consumed: false,
                kind,
              };
            }
            reads.push(read);
          }
          let object: R2ObjectBody | null;
          try {
            object = options ? await target.get(key, options) : await target.get(key);
          } catch (error) {
            const index = read ? reads.indexOf(read) : -1;
            if (index >= 0) reads.splice(index, 1);
            throw error;
          }
          if (!read || !object || !("arrayBuffer" in object)) return object;
          const discard = (): void => {
            const index = reads.indexOf(read!);
            if (index >= 0) reads.splice(index, 1);
          };
          const beforeConsume =
            read.kind === "whole"
              ? () => {
                  const fault = transientWholePackReadForTesting;
                  if (fault?.operationId !== operationId) return;
                  fault.attempts++;
                  if (fault.attempts === 1) {
                    throw new Error("injected transient whole-pack body read");
                  }
                }
              : undefined;
          const transformBytes =
            read.kind === "whole" && shortWholePackReadForTesting?.operationId === operationId
              ? (bytes: Uint8Array): Uint8Array => {
                  const fault = shortWholePackReadForTesting;
                  if (!fault) return bytes;
                  fault.attempts++;
                  return fault.attempts === 1 ? bytes.subarray(0, bytes.byteLength - 1) : bytes;
                }
              : undefined;
          const expectedLength =
            read.kind === "whole"
              ? pack?.packBytes
              : read.start !== undefined && read.end !== undefined
                ? read.end - read.start
                : undefined;
          return observeReturnedActivePackBody(
            object,
            read,
            discard,
            expectedLength,
            beforeConsume,
            transformBytes
          );
        };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const observedEnv = new Proxy(env, {
    get(target, property) {
      if (property === "REPO_BUCKET") return bucket;
      return Reflect.get(target, property, target);
    },
  });
  return { env: observedEnv, reads };
}

function compareRuntimeRead(
  left: { packChecksum: string; start: number; end: number },
  right: { packChecksum: string; start: number; end: number }
): number {
  return (
    left.packChecksum.localeCompare(right.packChecksum) ||
    left.start - right.start ||
    left.end - right.end
  );
}

function reconcileActivePackReads(args: {
  observation: ActivePackReadObservation;
  active: BoundActivePack[];
  ranges: StockRequiredRange[];
}): ReconciledActivePackReads {
  const checksumByKey = new Map(
    args.active.map((pack) => [pack.source.packKey, pack.packChecksum])
  );
  const trailerReads: StockObservedActivePackRead[] = [];
  const bodyReads: Array<{
    packChecksum: string;
    start: number;
    end: number;
    returnedBytes: number;
  }> = [];
  const wholeReads: StockObservedActivePackRead[] = [];
  let wholeBytes = 0;
  let wholeRequests = 0;
  let unattributedBytes = 0;
  let unattributedRequests = 0;

  for (const read of args.observation.reads) {
    const packChecksum = checksumByKey.get(read.packKey);
    if (!packChecksum) throw new Error("stock-plan:active-pack-read-source-missing");
    if (read.kind === "whole") {
      wholeRequests++;
      wholeBytes += read.returnedBytes;
      wholeReads.push({
        packChecksum,
        start: 0,
        end: read.returnedBytes,
        returnedBytes: read.returnedBytes,
        kind: "whole",
      });
      continue;
    }
    if (read.kind === "unattributed" || read.start === undefined || read.end === undefined) {
      unattributedRequests++;
      unattributedBytes += read.returnedBytes;
      continue;
    }
    if (!read.consumed || read.returnedBytes !== read.end - read.start) {
      throw new Error("stock-plan:active-pack-read-length-mismatch");
    }
    if (read.kind === "trailer") {
      trailerReads.push({
        packChecksum,
        start: read.start,
        end: read.end,
        returnedBytes: read.returnedBytes,
        kind: "trailer",
      });
      continue;
    }
    bodyReads.push({
      packChecksum,
      start: read.start,
      end: read.end,
      returnedBytes: read.returnedBytes,
    });
  }

  if (unattributedRequests !== 0) {
    throw new Error("stock-plan:active-pack-read-unattributed");
  }
  const activeByChecksum = new Map(args.active.map((pack) => [pack.packChecksum, pack]));
  const wholeChecksums = new Set<string>();
  wholeReads.sort(compareRuntimeRead);
  for (const read of wholeReads) {
    const pack = activeByChecksum.get(read.packChecksum);
    if (
      !pack ||
      wholeChecksums.has(read.packChecksum) ||
      read.start !== 0 ||
      read.end !== pack.source.packBytes ||
      read.returnedBytes !== pack.source.packBytes
    ) {
      throw new Error("stock-plan:active-pack-whole-observation-mismatch");
    }
    wholeChecksums.add(read.packChecksum);
  }
  const expectedTrailerPackKeys = args.active.map((pack) => pack.source.packKey).sort();
  const observedTrailerPackKeys = args.observation.reads
    .filter((read) => read.kind === "trailer")
    .map((read) => read.packKey)
    .sort();
  if (
    expectedTrailerPackKeys.length !== observedTrailerPackKeys.length ||
    expectedTrailerPackKeys.some((key, index) => key !== observedTrailerPackKeys[index])
  ) {
    throw new Error("stock-plan:active-pack-trailer-source-mismatch");
  }
  trailerReads.sort(compareRuntimeRead);
  const expectedTrailerChecksums = [...checksumByKey.values()].sort();
  if (
    trailerReads.length !== args.active.length ||
    trailerReads.some(
      (read, index) =>
        read.packChecksum !== expectedTrailerChecksums[index] || read.returnedBytes !== 20
    )
  ) {
    throw new Error("stock-plan:active-pack-trailer-observation-mismatch");
  }

  const expectedRanges = args.ranges
    .filter((range) => !wholeChecksums.has(range.packChecksum))
    .sort(
      (left, right) =>
        compareRuntimeRead(left, right) || left.requiredOid.localeCompare(right.requiredOid)
    );
  bodyReads.sort(compareRuntimeRead);
  if (
    bodyReads.length !== expectedRanges.length ||
    bodyReads.some((read, index) => {
      const expected = expectedRanges[index]!;
      return (
        read.packChecksum !== expected.packChecksum ||
        read.start !== expected.start ||
        read.end !== expected.end ||
        read.returnedBytes !== expected.end - expected.start
      );
    })
  ) {
    throw new Error("stock-plan:active-pack-range-observation-mismatch");
  }
  // Discovery may observe a child before its base. Evidence is deliberately
  // projected in the planner's deterministic base-first topology after the
  // exact observed multiset has been reconciled above.
  const observedRanges = args.ranges.map((range) => ({ ...range }));
  const requiredReads: StockObservedActivePackRead[] = observedRanges
    .filter((range) => !wholeChecksums.has(range.packChecksum))
    .map((range) => ({
      packChecksum: range.packChecksum,
      start: range.start,
      end: range.end,
      returnedBytes: range.end - range.start,
      kind: "required-object",
      requiredOid: range.requiredOid,
    }));
  return {
    activePackReads: [...trailerReads, ...requiredReads, ...wholeReads],
    ranges: observedRanges,
    trailerBytes: trailerReads.reduce((total, read) => total + read.returnedBytes, 0),
    trailerRequests: trailerReads.length,
    rangeBytes: requiredReads.reduce((total, read) => total + read.returnedBytes, 0),
    rangeRequests: requiredReads.length,
    wholeBytes,
    wholeRequests,
    unattributedBytes,
    unattributedRequests,
  };
}

function createBoundedWholePackReader(args: {
  env: Env;
  operationId: string;
  active: BoundActivePack[];
  limiter: Limiter;
  countSubrequest: (n?: number) => void;
  log: Logger;
  signal?: AbortSignal;
}): (candidate: PackedObjectCandidate) => Promise<Uint8Array | undefined> {
  const activeByKey = new Map(args.active.map((pack) => [pack.source.packKey, pack]));
  const selectedKeys = selectWholeActivePackKeys(args.active);
  const loadedByKey = new Map<string, Promise<Uint8Array>>();

  return async (candidate) => {
    if (wrongPrerequisiteRangeFaultForTesting?.operationId === args.operationId) {
      return undefined;
    }
    const pack = activeByKey.get(candidate.source.packKey);
    if (!pack || !selectedKeys.has(pack.source.packKey)) return undefined;

    let loaded = loadedByKey.get(pack.source.packKey);
    if (!loaded) {
      loaded = (async () => {
        const bytes = await readExactPackRangeWithRetry(
          async () => {
            args.countSubrequest();
            const object = await args.limiter.run("r2:stock-plan-whole-active-pack", async () => {
              return await args.env.REPO_BUCKET.get(pack.source.packKey);
            });
            if (!object) return undefined;
            return new Uint8Array(await object.arrayBuffer());
          },
          pack.source.packBytes,
          {
            signal: args.signal,
            log: args.log,
            retryContext: async () => ({
              kind: "whole-active-pack",
              packBytes: pack.source.packBytes,
            }),
          }
        );
        if (!bytes) {
          throw new Error("stock-plan:whole-active-pack-length");
        }
        if (
          bytes.byteLength !== pack.source.packBytes ||
          bytesToHex(bytes.subarray(bytes.byteLength - 20)) !== pack.packChecksum
        ) {
          throw new Error("stock-plan:whole-active-pack-checksum");
        }
        args.log.debug("stock-plan:whole-active-pack-loaded", {
          packKey: pack.source.packKey,
          packBytes: pack.source.packBytes,
        });
        return bytes;
      })();
      loadedByKey.set(pack.source.packKey, loaded);
    }

    const bytes = await loaded;
    return bytes.subarray(candidate.offset, candidate.nextOffset);
  };
}

function throwWrongRangePlannerFailure(args: {
  operationId: string;
  error: unknown;
  observation: ActivePackReadObservation;
  active: BoundActivePack[];
  counters: PlannerCounters;
  startedAt: number;
}): never {
  if (args.error instanceof StockReceivePlannerError) throw args.error;
  const fault = wrongPrerequisiteRangeFaultForTesting;
  if (
    fault?.operationId !== args.operationId ||
    !fault.triggered ||
    !fault.requiredOid ||
    !fault.packKey ||
    !fault.packChecksum ||
    fault.start === undefined ||
    fault.end === undefined
  ) {
    throw args.error;
  }
  const wrongReads = args.observation.reads.filter(
    (read) => read.packKey === fault.packKey && read.start === fault.start && read.end === fault.end
  );
  const trailerReads = args.observation.reads.filter((read) => read.kind === "trailer");
  const bodyReads = args.observation.reads.filter(
    (read) => read.kind === "body-range" || wrongReads.includes(read)
  );
  if (
    wrongReads.length !== 1 ||
    !wrongReads[0]!.consumed ||
    wrongReads[0]!.returnedBytes !== fault.end - fault.start ||
    bodyReads.length === 0 ||
    bodyReads.some(
      (read) =>
        !read.consumed ||
        read.start === undefined ||
        read.end === undefined ||
        read.returnedBytes !== read.end - read.start
    ) ||
    args.observation.reads.some(
      (read) =>
        read.kind === "whole" || (read.kind === "unattributed" && !wrongReads.includes(read))
    ) ||
    trailerReads.length !== args.active.length ||
    trailerReads.some(
      (read) =>
        !read.consumed ||
        read.start === undefined ||
        read.end === undefined ||
        read.returnedBytes !== 20 ||
        read.end - read.start !== 20
    )
  ) {
    throw new Error("stock-plan:wrong-prerequisite-range-observation-invalid");
  }
  const checksumByKey = new Map(
    args.active.map((pack) => [pack.source.packKey, pack.packChecksum])
  );
  const requiredOid = fault.requiredOid;
  const observedTrailers: StockObservedActivePackRead[] = trailerReads
    .map((read) => ({
      packChecksum: checksumByKey.get(read.packKey) ?? "",
      start: read.start!,
      end: read.end!,
      returnedBytes: read.returnedBytes,
      kind: "trailer" as const,
    }))
    .sort(compareRuntimeRead);
  if (observedTrailers.some((read) => !/^[0-9a-f]{40}$/.test(read.packChecksum))) {
    throw new Error("stock-plan:wrong-prerequisite-range-source-invalid");
  }
  const ranges: StockRequiredRange[] = bodyReads
    .map((read) => {
      const packChecksum = checksumByKey.get(read.packKey);
      if (!packChecksum || read.start === undefined || read.end === undefined) {
        throw new Error("stock-plan:wrong-prerequisite-range-source-invalid");
      }
      return {
        entryId: `fault:${packChecksum}:${read.start}:${read.end}:${requiredOid}`,
        packChecksum,
        start: read.start,
        end: read.end,
        reason: "required-object" as const,
        requiredOid,
        semanticRootOids: [requiredOid],
      };
    })
    .sort(
      (left, right) =>
        compareRuntimeRead(left, right) || left.requiredOid.localeCompare(right.requiredOid)
    );
  const requiredReads: StockObservedActivePackRead[] = ranges.map((range) => ({
    packChecksum: range.packChecksum,
    start: range.start,
    end: range.end,
    returnedBytes: range.end - range.start,
    kind: "required-object",
    requiredOid: range.requiredOid,
  }));
  const rangeBytes = requiredReads.reduce((total, read) => total + read.returnedBytes, 0);
  const touchedChecksums = new Set(requiredReads.map((read) => read.packChecksum));
  const selectedPackBytes = args.active
    .filter((pack) => touchedChecksums.has(pack.packChecksum))
    .reduce((total, pack) => total + pack.source.packBytes, 0);
  throw new StockReceivePlannerError("replacement-closure-invalid", {
    elapsedMs: Math.max(0, Date.now() - args.startedAt),
    metadataBytes: args.counters.metadataBytes,
    metadataRequests: args.counters.metadataRequests,
    inputBytesRead: args.counters.inputBytesRead,
    inputRequests: args.counters.inputRequests,
    ranges,
    rangeBytes,
    rangeRequests: requiredReads.length,
    activePackReads: [...observedTrailers, ...requiredReads],
    activePackTrailerBytes: observedTrailers.reduce((total, read) => total + read.returnedBytes, 0),
    activePackTrailerRequests: observedTrailers.length,
    activePackRangeBytes: rangeBytes,
    activePackRangeRequests: requiredReads.length,
    activePackWholeBytes: 0,
    activePackWholeRequests: 0,
    activePackUnattributedBytes: 0,
    activePackUnattributedRequests: 0,
    packsTouched: touchedChecksums.size,
    selectedPackBytes,
    activePackCount: args.active.length,
  });
}

async function getBoundedObject(args: {
  env: Env;
  key: string;
  maximumBytes: number;
  expectedBytes?: number;
  limiter: Limiter;
  countSubrequest: (n?: number) => void;
  counters: PlannerCounters;
}): Promise<{ object: R2ObjectBody; bytes: Uint8Array }> {
  args.countSubrequest();
  args.counters.metadataRequests++;
  const object = await args.limiter.run("r2:stock-plan-metadata", () =>
    args.env.REPO_BUCKET.get(args.key)
  );
  if (!object) throw new Error("stock-plan:metadata-missing");
  if (
    object.size > args.maximumBytes ||
    (args.expectedBytes !== undefined && object.size !== args.expectedBytes) ||
    object.size > MAX_METADATA_BYTES - args.counters.metadataBytes
  ) {
    throw new Error("stock-plan:metadata-size-limit");
  }
  args.counters.metadataBytes += object.size;
  return { object, bytes: new Uint8Array(await object.arrayBuffer()) };
}

async function readExactRange(args: {
  env: Env;
  key: string;
  offset: number;
  length: number;
  limiter: Limiter;
  countSubrequest: (n?: number) => void;
  kind: "metadata" | "input";
  counters: PlannerCounters;
}): Promise<Uint8Array> {
  args.countSubrequest();
  if (args.kind === "metadata") args.counters.metadataRequests++;
  else args.counters.inputRequests++;
  const object = await args.limiter.run(`r2:stock-plan-${args.kind}-range`, () =>
    args.env.REPO_BUCKET.get(args.key, {
      range: { offset: args.offset, length: args.length },
    })
  );
  if (!object) throw new Error(`stock-plan:${args.kind}-range-missing`);
  const bytes = new Uint8Array(await object.arrayBuffer());
  if (bytes.byteLength !== args.length) {
    throw new Error(`stock-plan:${args.kind}-range-length`);
  }
  if (args.kind === "metadata") args.counters.metadataBytes += bytes.byteLength;
  else args.counters.inputBytesRead += bytes.byteLength;
  return bytes;
}

async function putImmutableBytes(args: {
  env: Env;
  key: string;
  bytes: Uint8Array;
  sha256: string;
  limiter: Limiter;
  countSubrequest: (n?: number) => void;
}): Promise<{ etag: string; created: boolean }> {
  args.countSubrequest();
  const created = await args.limiter.run("r2:stock-plan-put-immutable", () =>
    args.env.REPO_BUCKET.put(args.key, args.bytes, {
      onlyIf: { etagDoesNotMatch: "*" },
      httpMetadata: { contentType: "application/octet-stream" },
      customMetadata: { sha256: args.sha256 },
    })
  );
  if (created) return { etag: created.etag, created: true };

  args.countSubrequest();
  const existing = await args.limiter.run("r2:stock-plan-get-immutable", () =>
    args.env.REPO_BUCKET.get(args.key)
  );
  if (
    !existing ||
    existing.size !== args.bytes.byteLength ||
    existing.customMetadata?.sha256 !== args.sha256
  ) {
    throw new Error("stock-plan:immutable-conflict");
  }
  const existingBytes = new Uint8Array(await existing.arrayBuffer());
  if ((await digestHex("SHA-256", existingBytes)) !== args.sha256) {
    throw new Error("stock-plan:immutable-conflict");
  }
  return { etag: existing.etag, created: false };
}

async function deletePlannerKeys(args: {
  env: Env;
  keys: string[];
  limiter: Limiter;
  countSubrequest: (n?: number) => void;
}): Promise<void> {
  if (args.keys.length === 0) return;
  args.countSubrequest(args.keys.length);
  const results = await Promise.allSettled(
    args.keys.map((key) =>
      args.limiter.run("r2:stock-plan-delete", () => args.env.REPO_BUCKET.delete(key))
    )
  );
  if (results.some((result) => result.status === "rejected")) {
    throw new Error("stock-plan:cleanup-failed");
  }
}

async function loadBoundActivePacks(
  args: PlanStockReceiveArgs,
  counters: PlannerCounters
): Promise<BoundActivePack[]> {
  if (args.activePacks.length > MAX_ACTIVE_PACKS) {
    throw new Error("stock-plan:active-pack-count-limit");
  }
  args.cacheCtx.memo = args.cacheCtx.memo || {};
  args.cacheCtx.memo.flags = args.cacheCtx.memo.flags || new Set();
  args.cacheCtx.memo.flags.add("no-isolate-idx-cache");
  args.cacheCtx.memo.idxViews = args.cacheCtx.memo.idxViews || new Map();
  const idxViews = args.cacheCtx.memo.idxViews;

  return await Promise.all(
    args.activePacks.map(async (pack): Promise<BoundActivePack> => {
      throwIfAborted(args.signal);
      assertSafeBytes(pack.packBytes, "active-pack-bytes", Number.MAX_SAFE_INTEGER);
      assertSafeBytes(pack.idxBytes, "active-idx-bytes", MAX_METADATA_BYTES);
      const idxObject = await getBoundedObject({
        env: args.env,
        key: packIndexKey(pack.packKey),
        maximumBytes: MAX_METADATA_BYTES,
        expectedBytes: pack.idxBytes,
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
        counters,
      });
      const idx = parseIdxView(pack.packKey, idxObject.bytes, pack.packBytes);
      if (!idx) throw new Error("stock-plan:idx-invalid");
      const computedIdxChecksum = await digestHex(
        "SHA-1",
        idxObject.bytes.subarray(0, idxObject.bytes.byteLength - 20)
      );
      if (computedIdxChecksum !== bytesToHex(idx.idxChecksum)) {
        throw new Error("stock-plan:idx-checksum-mismatch");
      }
      const idxSha256 = await digestHex("SHA-256", idxObject.bytes);
      if (pack.idxSha256 !== undefined && pack.idxSha256 !== idxSha256) {
        throw new Error("stock-plan:idx-authority-digest-mismatch");
      }

      const trailer = await readExactRange({
        env: args.env,
        key: pack.packKey,
        offset: pack.packBytes - 20,
        length: 20,
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
        kind: "metadata",
        counters,
      });
      if (!bytesEqual(trailer, idx.packChecksum)) {
        throw new Error("stock-plan:pack-trailer-mismatch");
      }
      if (pack.packChecksum !== undefined && pack.packChecksum !== bytesToHex(idx.packChecksum)) {
        throw new Error("stock-plan:pack-authority-checksum-mismatch");
      }

      const refsObject = await getBoundedObject({
        env: args.env,
        key: packRefsKey(pack.packKey),
        maximumBytes: MAX_METADATA_BYTES,
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
        counters,
      });
      const parsedRefs = parsePackRefView(pack.packKey, refsObject.bytes, idx);
      if (parsedRefs.type !== "Ready") throw new Error("stock-plan:pref-invalid");
      const refsSha256 = await digestHex("SHA-256", refsObject.bytes);
      if (pack.refsSha256 !== undefined && pack.refsSha256 !== refsSha256) {
        throw new Error("stock-plan:pref-authority-digest-mismatch");
      }
      idxViews.set(pack.packKey, idx);
      return {
        source: { packKey: pack.packKey, packBytes: pack.packBytes, idx },
        idxBytes: pack.idxBytes,
        refs: parsedRefs.view,
        packChecksum: bytesToHex(idx.packChecksum),
        idxSha256,
        refsSha256,
      };
    })
  );
}

function activeNode(
  packs: BoundActivePack[],
  oid: string
): { refs: string[]; type: "commit" | "tree" | "blob" | "tag" } | undefined {
  let found: { refs: string[]; type: "commit" | "tree" | "blob" | "tag" } | undefined;
  for (const pack of packs) {
    const index = findOidIndex(pack.source.idx, oid);
    if (index < 0) continue;
    const type = getPackRefObjectType(pack.refs, index);
    if (!type) throw new Error("stock-plan:active-type-missing");
    const refs = getPackRefRefsAt(pack.refs, index);
    if (
      found &&
      (found.type !== type ||
        found.refs.length !== refs.length ||
        found.refs.some((value, refIndex) => value !== refs[refIndex]))
    ) {
      throw new Error("stock-plan:duplicate-pref-mismatch");
    }
    found = { refs, type };
  }
  return found;
}

function deriveAdvertisedClosure(
  packs: BoundActivePack[],
  advertisedRefs: Array<{ name: string; oid: string }>
): string[] {
  const queue = advertisedRefs
    .map((ref) => ref.oid.toLowerCase())
    .filter((oid) => oid !== ZERO_OID);
  const seen = new Set<string>();
  let edges = 0;
  for (let cursor = 0; cursor < queue.length; cursor++) {
    const oid = queue[cursor]!;
    if (seen.has(oid)) continue;
    if (seen.size >= MAX_CLOSURE_OBJECTS) throw new Error("stock-plan:advertised-closure-limit");
    const node = activeNode(packs, oid);
    if (!node) throw new Error("stock-plan:advertised-object-missing");
    seen.add(oid);
    edges += node.refs.length;
    if (edges > MAX_CLOSURE_EDGES) throw new Error("stock-plan:advertised-edge-limit");
    queue.push(...node.refs);
  }
  return [...seen].sort();
}

function incomingNode(
  idx: IdxView,
  refs: PackRefView,
  oid: string
): { refs: string[]; type: "commit" | "tree" | "blob" | "tag" } | undefined {
  const index = findOidIndex(idx, oid);
  if (index < 0) return undefined;
  const type = getPackRefObjectType(refs, index);
  if (!type) throw new Error("stock-plan:incoming-type-missing");
  return { refs: getPackRefRefsAt(refs, index), type };
}

function deriveIncomingBoundary(args: {
  commands: ReceiveCommand[];
  incomingIdx: IdxView;
  incomingRefs: PackRefView;
  advertisedReachable: Set<string>;
}): Pick<
  StockReceivePlan,
  | "semanticExternalOids"
  | "visitedIncomingObjectCount"
  | "logicalEdgeCount"
  | "internalEdgeCount"
  | "externalEdgeCount"
  | "missingObjectCount"
  | "objectTypeCounts"
> & { visitedIncomingOids: string[] } {
  const queue = args.commands
    .map((command) => command.newOid.toLowerCase())
    .filter((oid) => oid !== ZERO_OID);
  const visited = new Set<string>();
  const external = new Set<string>();
  const objectTypeCounts = { commit: 0, tree: 0, blob: 0, tag: 0 };
  let logicalEdgeCount = 0;
  let internalEdgeCount = 0;
  let externalEdgeCount = 0;
  let missingObjectCount = 0;

  for (let cursor = 0; cursor < queue.length; cursor++) {
    const oid = queue[cursor]!;
    if (visited.has(oid) || external.has(oid)) continue;
    if (args.advertisedReachable.has(oid)) {
      external.add(oid);
      continue;
    }
    const node = incomingNode(args.incomingIdx, args.incomingRefs, oid);
    if (!node) {
      missingObjectCount++;
      throw new Error("stock-plan:incoming-closure-missing");
    }
    if (visited.size >= MAX_CLOSURE_OBJECTS) throw new Error("stock-plan:incoming-closure-limit");
    visited.add(oid);
    objectTypeCounts[node.type]++;
    logicalEdgeCount += node.refs.length;
    if (logicalEdgeCount > MAX_CLOSURE_EDGES) throw new Error("stock-plan:incoming-edge-limit");
    for (const target of node.refs) {
      if (findOidIndex(args.incomingIdx, target) >= 0) {
        internalEdgeCount++;
        queue.push(target);
      } else {
        externalEdgeCount++;
        if (!args.advertisedReachable.has(target)) {
          missingObjectCount++;
          throw new Error("stock-plan:external-edge-not-advertised");
        }
        external.add(target);
      }
    }
  }

  return {
    visitedIncomingOids: [...visited].sort(),
    semanticExternalOids: [...external].sort(),
    visitedIncomingObjectCount: visited.size,
    logicalEdgeCount,
    internalEdgeCount,
    externalEdgeCount,
    missingObjectCount,
    objectTypeCounts,
  };
}

function physicalRootIteration(
  requiredRootOids: string[],
  requestedRootOids: string[] | undefined
): string[] {
  if (requestedRootOids === undefined) return requiredRootOids;
  const requestedSet = [...new Set(requestedRootOids)];
  if (
    requestedSet.length !== requiredRootOids.length ||
    requestedRootOids.length !== requiredRootOids.length ||
    requestedSet.sort().some((oid, index) => oid !== requiredRootOids[index])
  ) {
    throw new Error("stock-plan:root-iteration-permutation");
  }
  return [...requestedRootOids];
}

function catalogRows(packs: BoundActivePack[]): PackCatalogRow[] {
  return packs.map((pack, index) => ({
    packKey: pack.source.packKey,
    kind: "receive",
    state: "active",
    tier: 0,
    seqLo: index,
    seqHi: index,
    objectCount: pack.source.idx.count,
    packBytes: pack.source.packBytes,
    idxBytes: 0,
    createdAt: 0,
    supersededBy: null,
  }));
}

export function stockReceivePlanKeys(
  inputRequestKey: string,
  inputSha256: string
): {
  temporaryPackKey: string;
  prerequisitePackKey: string;
  closureManifestKey: string;
} {
  if (!inputRequestKey.endsWith(".request")) throw new Error("stock-plan:input-key-shape");
  const stem = inputRequestKey.slice(0, -".request".length);
  return {
    temporaryPackKey: `${stem}-${inputSha256}.planning.pack`,
    prerequisitePackKey: `${stem}-${inputSha256}.prerequisite.pack`,
    closureManifestKey: `${stem}-${inputSha256}.closure.json`,
  };
}

async function planStockReceiveImpl(
  args: PlanStockReceiveArgs,
  ownedImmutableKeys: string[]
): Promise<StockReceivePlan> {
  const startedAt = Date.now();
  const log = args.log ?? createLogger(args.env.LOG_LEVEL, { service: "StockReceivePlanner" });
  assertSafeBytes(args.inputRequestBytes, "input-request-bytes", MAX_INPUT_PACK_BYTES);
  assertSafeBytes(args.packBytes, "input-pack-bytes", MAX_INPUT_PACK_BYTES);
  assertSafeBytes(args.packOffset, "input-pack-offset", args.inputRequestBytes);
  if (args.packOffset + args.packBytes !== args.inputRequestBytes) {
    throw new Error("stock-plan:input-pack-span");
  }
  if (!/^[0-9a-f]{64}$/.test(args.inputRequestSha256)) {
    throw new Error("stock-plan:input-sha256");
  }
  // Observe the platform response at the R2 bucket boundary rather than
  // trusting callbacks from the materializer. This binds every exact range or
  // bounded whole-pack preload to the active pack that actually supplied it.
  const activePackObservation = observeActivePackReads(
    args.env,
    args.activePacks,
    args.operationId
  );
  args = { ...args, env: activePackObservation.env };
  const counters: PlannerCounters = {
    metadataBytes: 0,
    metadataRequests: 0,
    inputBytesRead: 0,
    inputRequests: 0,
  };
  const keys = stockReceivePlanKeys(args.inputRequestKey, args.inputRequestSha256);
  // E1-only one-shot fault: fail immediately before the planner's first R2
  // metadata read. The failed attempt therefore cannot invoke host Go or Git,
  // and the same operation can be retried against the unchanged authority.
  throwTransientR2ReadFault(args.operationId, counters, args.activePacks.length);
  const active = await loadBoundActivePacks(args, counters);
  if (unexpectedWholePackReadOperationForTesting === args.operationId) {
    unexpectedWholePackReadOperationForTesting = undefined;
    args.countSubrequest();
    const unexpected = await args.limiter.run("r2:stock-plan-test-whole-pack", () =>
      args.env.REPO_BUCKET.get(active[0]!.source.packKey)
    );
    if (!unexpected) throw new Error("stock-plan:test-whole-pack-missing");
    await unexpected.arrayBuffer();
  }
  const advertisedReachableOids = deriveAdvertisedClosure(active, args.advertisedRefs);
  const advertisedReachable = new Set(advertisedReachableOids);
  const inputPack = await readExactRange({
    env: args.env,
    key: args.inputRequestKey,
    offset: args.packOffset,
    length: args.packBytes,
    limiter: args.limiter,
    countSubrequest: args.countSubrequest,
    kind: "input",
    counters,
  });
  if (inputPack.byteLength < 32) throw new Error("stock-plan:input-pack-truncated");
  const computedPackSha1 = await digestHex("SHA-1", inputPack.subarray(0, -20));
  if (computedPackSha1 !== bytesToHex(inputPack.subarray(-20))) {
    throw new Error("stock-plan:input-pack-checksum");
  }

  const tempSha256 = await digestHex("SHA-256", inputPack);
  const temporaryPut = await putImmutableBytes({
    env: args.env,
    key: keys.temporaryPackKey,
    bytes: inputPack,
    sha256: tempSha256,
    limiter: args.limiter,
    countSubrequest: args.countSubrequest,
  });
  if (temporaryPut.created) ownedImmutableKeys.push(keys.temporaryPackKey);

  const thinDeltaBaseOids = new Set<string>();
  const activeByChecksum = new Map(active.map((pack) => [pack.packChecksum, pack]));
  const directPackRequested =
    (args.env as Env & { STOCK_RECEIVE_DIRECT_PACK?: string }).STOCK_RECEIVE_DIRECT_PACK === "1";
  // Direct publication only needs byte-level proof for true thin-delta bases.
  // Loading a multi-megabyte pack to recover a few kilobytes of those entries
  // recreates the latency this path is designed to remove, so use exact ranges.
  const readFromBoundedWholePack = directPackRequested
    ? async (): Promise<Uint8Array | undefined> => undefined
    : createBoundedWholePackReader({
        env: args.env,
        operationId: args.operationId,
        active,
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
        log,
        signal: args.signal,
      });
  const physicalPlanner = createStockPhysicalDependencyPlanner({
    sources: active.map((pack) => ({
      source: pack.source,
      packChecksum: pack.packChecksum,
      idxSha256: pack.idxSha256,
      prefSha256: pack.refsSha256,
    })),
    maxEntryBytes: MAX_OBJECT_BYTES,
    maxInflatedBytes: MAX_OBJECT_BYTES,
    maxDeltaResultBytes: MAX_OBJECT_BYTES,
    signal: args.signal,
    log,
    readEntry: async (candidate, semanticRootOid) => {
      const selectedCandidate = candidateWithWrongRangeFault(
        args.operationId,
        semanticRootOid,
        candidate,
        active.length
      );
      const length = selectedCandidate.nextOffset - selectedCandidate.offset;
      const bytes =
        (await readFromBoundedWholePack(selectedCandidate)) ??
        (await readPackRange(
          args.env,
          selectedCandidate.source.packKey,
          selectedCandidate.offset,
          length,
          {
            limiter: args.limiter,
            countSubrequest: args.countSubrequest,
            signal: args.signal,
            exactLength: true,
            log,
          }
        ));
      if (bytes) {
        recordWrongPrerequisiteRangeRead(
          args.operationId,
          semanticRootOid,
          selectedCandidate,
          bytes.byteLength,
          counters
        );
      }
      const physicalFault = wrongPrerequisiteRangeFaultForTesting;
      if (
        bytes &&
        physicalFault?.operationId === args.operationId &&
        physicalFault.hook === "touched-active-pack-entry" &&
        physicalFault.triggered &&
        physicalFault.packKey === selectedCandidate.source.packKey &&
        physicalFault.start === selectedCandidate.offset &&
        physicalFault.end === selectedCandidate.nextOffset
      ) {
        const touched = bytes.slice();
        touched[touched.byteLength - 1] ^= 1;
        physicalFault.bytesMutated = true;
        return touched;
      }
      return bytes;
    },
  });

  let incomingIdx: IdxView;
  let incomingRefs: PackRefView;
  let incomingRefsBytes: Uint8Array | undefined;
  let incomingObjectCount = 0;
  let incomingIdxBytes = 0;
  const noteInputRead = (read: { length: number }): void => {
    counters.inputRequests++;
    counters.inputBytesRead += read.length;
  };
  try {
    const scan = await scanPack({
      env: args.env,
      packKey: keys.temporaryPackKey,
      packSize: args.packBytes,
      chunkSize: Math.min(args.packBytes, 1024 * 1024),
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
      log,
      signal: args.signal,
      maxObjectBytes: MAX_OBJECT_BYTES,
      onRead: noteInputRead,
    });
    const resolve = await resolveDeltasAndWriteIdx({
      env: args.env,
      packKey: keys.temporaryPackKey,
      packSize: args.packBytes,
      chunkSize: Math.min(args.packBytes, 1024 * 1024),
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
      log,
      signal: args.signal,
      maxObjectBytes: MAX_OBJECT_BYTES,
      scanResult: scan,
      activeCatalog: catalogRows(active),
      cacheCtx: args.cacheCtx,
      repoId: args.repoId,
      lruBudget: MAX_PREREQUISITE_PAYLOAD_BYTES,
      resolveExternalBase: async (requiredOid) => {
        thinDeltaBaseOids.add(requiredOid);
        try {
          return await physicalPlanner.materializeSemanticRoot(requiredOid);
        } catch (error) {
          throwWrongRangePlannerFailure({
            operationId: args.operationId,
            error,
            observation: activePackObservation,
            active,
            counters,
            startedAt,
          });
        }
      },
      onRead: noteInputRead,
    });
    incomingIdx = resolve.idxView;
    incomingIdxBytes = resolve.idxBytes;
    incomingObjectCount = resolve.objectCount;
    const generatedRefs = await getBoundedObject({
      env: args.env,
      key: packRefsKey(keys.temporaryPackKey),
      maximumBytes: MAX_METADATA_BYTES,
      expectedBytes: resolve.refIndexBytes,
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
      counters,
    });
    const parsed = parsePackRefView(keys.temporaryPackKey, generatedRefs.bytes, incomingIdx);
    if (parsed.type !== "Ready") throw new Error("stock-plan:generated-pref-invalid");
    incomingRefs = parsed.view;
    incomingRefsBytes = generatedRefs.bytes;
  } catch (error) {
    throwWrongRangePlannerFailure({
      operationId: args.operationId,
      error,
      observation: activePackObservation,
      active,
      counters,
      startedAt,
    });
  }

  const boundary = deriveIncomingBoundary({
    commands: args.commands,
    incomingIdx,
    incomingRefs,
    advertisedReachable,
  });
  const visitedIncoming = new Set(boundary.visitedIncomingOids);
  for (let index = 0; index < incomingIdx.count; index++) {
    const oid = getOidHexAt(incomingIdx, index);
    if (!visitedIncoming.has(oid) && !advertisedReachable.has(oid)) {
      throw new Error("stock-plan:unreachable-input-object");
    }
  }
  const directSemanticExternal = new Set(boundary.semanticExternalOids);
  const directThinBasesEligible = [...thinDeltaBaseOids].every(
    (oid) => advertisedReachable.has(oid) && !directSemanticExternal.has(oid)
  );

  // Qualification-only fast path: the incoming pack has already passed the
  // bounded scanner, delta resolver, object-graph closure proof, and exact
  // advertised-object boundary above. Publish those immutable bytes directly
  // instead of rebuilding the advertised repository into a prerequisite pack
  // and asking stock receive-pack to emit the same quarantine pack again.
  // RepoDO still owns exact-old admission/finalization; R2 remains the object
  // authority. Inputs outside the direct admission predicate below continue
  // through the existing stock-Container path.
  if (
    directPackRequested &&
    args.commands.length === 1 &&
    incomingObjectCount > 0 &&
    incomingObjectCount === boundary.visitedIncomingObjectCount &&
    directThinBasesEligible
  ) {
    if (!args.outputPackKey || !args.outputIdxKey || !args.outputRefsKey) {
      throw new Error("stock-plan:direct-output-keys-missing");
    }
    const requiredRootOids = [...thinDeltaBaseOids].sort();
    let physicalPlan: StockPhysicalDependencyPlan;
    try {
      await physicalPlanner.materializeSemanticRoots(requiredRootOids);
      physicalPlan = await physicalPlanner.finalize(requiredRootOids);
    } catch (error) {
      throwWrongRangePlannerFailure({
        operationId: args.operationId,
        error,
        observation: activePackObservation,
        active,
        counters,
        startedAt,
      });
    }
    const plannedRanges: StockRequiredRange[] = physicalPlan.ranges.map((range) => ({
      entryId: range.entryId,
      packChecksum: range.packChecksum,
      start: range.start,
      end: range.end,
      reason: "required-object",
      requiredOid: range.oid,
      semanticRootOids: range.semanticRootOids,
    }));
    const observed = reconcileActivePackReads({
      observation: activePackObservation,
      active,
      ranges: plannedRanges,
    });
    const idxObject = await getBoundedObject({
      env: args.env,
      key: packIndexKey(keys.temporaryPackKey),
      maximumBytes: MAX_METADATA_BYTES,
      expectedBytes: incomingIdxBytes,
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
      counters,
    });
    const idxBytes = idxObject.bytes;
    const refsBytes = incomingRefsBytes;
    if (!refsBytes) throw new Error("stock-plan:generated-pref-missing");
    const idxSha256 = await digestHex("SHA-256", idxBytes);
    const refsSha256 = await digestHex("SHA-256", refsBytes);
    const activePackBindings = active.map((pack) => ({
      packKey: pack.source.packKey,
      packBytes: pack.source.packBytes,
      idxBytes: pack.idxBytes,
      packChecksum: pack.packChecksum,
      idxSha256: pack.idxSha256,
      prefSha256: pack.refsSha256,
    }));
    const selectedPackChecksums = [
      ...new Set(physicalPlan.physicalNodes.map((node) => node.packChecksum)),
    ].sort();
    const selectedPackBytes = selectedPackChecksums.reduce((total, checksum) => {
      const pack = activeByChecksum.get(checksum);
      if (!pack) throw new Error("stock-plan:selected-pack-missing");
      return total + pack.source.packBytes;
    }, 0);
    const planDocument = {
      schemaVersion: 3,
      executionMode: "direct-pack",
      operationId: args.operationId,
      repositoryId: args.repoId,
      input: {
        key: args.inputRequestKey,
        bytes: args.inputRequestBytes,
        sha256: args.inputRequestSha256,
        packOffset: args.packOffset,
        packBytes: args.packBytes,
        packSha256: tempSha256,
        idxSha256,
        refsSha256,
      },
      commands: args.commands,
      advertisedRefs: args.advertisedRefs,
      activePackBindings,
      advertisedReachableOids,
      incomingOids: boundary.visitedIncomingOids,
      semanticExternalOids: boundary.semanticExternalOids,
      thinDeltaBaseOids: requiredRootOids,
      physicalNodes: physicalPlan.physicalNodes,
      dependencies: physicalPlan.dependencies,
      topologicalEntryIds: physicalPlan.topologicalEntryIds,
      ranges: observed.ranges,
      activePackReads: observed.activePackReads,
      closure: {
        incomingObjectCount,
        visitedIncomingObjectCount: boundary.visitedIncomingObjectCount,
        logicalEdgeCount: boundary.logicalEdgeCount,
        internalEdgeCount: boundary.internalEdgeCount,
        externalEdgeCount: boundary.externalEdgeCount,
        missingObjectCount: boundary.missingObjectCount,
        objectTypeCounts: boundary.objectTypeCounts,
      },
    };
    const manifest = new TextEncoder().encode(JSON.stringify(planDocument));
    if (manifest.byteLength > MAX_MANIFEST_BYTES) throw new Error("stock-plan:manifest-limit");
    const closureManifestSha256 = await digestHex("SHA-256", manifest);
    const closureManifestPut = await putImmutableBytes({
      env: args.env,
      key: keys.closureManifestKey,
      bytes: manifest,
      sha256: closureManifestSha256,
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
    });
    if (closureManifestPut.created) ownedImmutableKeys.push(keys.closureManifestKey);

    const directOutputUploadStartedAt = Date.now();
    const directArtifacts = [
      { key: args.outputPackKey, bytes: inputPack, sha256: tempSha256 },
      { key: args.outputIdxKey, bytes: idxBytes, sha256: idxSha256 },
      { key: args.outputRefsKey, bytes: refsBytes, sha256: refsSha256 },
    ];
    const directPuts = await Promise.allSettled(
      directArtifacts.map(async (artifact) => ({
        artifact,
        put: await putImmutableBytes({
          env: args.env,
          ...artifact,
          limiter: args.limiter,
          countSubrequest: args.countSubrequest,
        }),
      }))
    );
    for (const result of directPuts) {
      if (result.status !== "fulfilled") continue;
      const { artifact, put } = result.value;
      if (put.created) ownedImmutableKeys.push(artifact.key);
    }
    const directPutFailures = directPuts
      .filter((result): result is PromiseRejectedResult => result.status === "rejected")
      .map((result) => result.reason);
    if (directPutFailures.length > 0) {
      throw new AggregateError(directPutFailures, "stock-plan:direct-output-put-failed");
    }
    const directOutputUploadMs = Date.now() - directOutputUploadStartedAt;

    await deletePlannerKeys({
      env: args.env,
      keys: [
        keys.temporaryPackKey,
        packIndexKey(keys.temporaryPackKey),
        packRefsKey(keys.temporaryPackKey),
      ],
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
    });
    const temporaryOwnedIndex = ownedImmutableKeys.indexOf(keys.temporaryPackKey);
    if (temporaryOwnedIndex >= 0) ownedImmutableKeys.splice(temporaryOwnedIndex, 1);
    return {
      executionMode: "direct-pack",
      directPackKey: args.outputPackKey,
      directPackBytes: inputPack.byteLength,
      directPackSha1: bytesToHex(inputPack.subarray(-20)),
      directPackSha256: tempSha256,
      directIdxKey: args.outputIdxKey,
      directIdxBytes: idxBytes.byteLength,
      directIdxSha256: idxSha256,
      directRefsKey: args.outputRefsKey,
      directRefsBytes: refsBytes.byteLength,
      directRefsSha256: refsSha256,
      directOutputUploadMs,
      incomingOids: boundary.visitedIncomingOids,
      prerequisitePackKey: "",
      prerequisitePackBytes: 0,
      prerequisitePackSha256: "",
      prerequisitePackEtag: "",
      closureManifestKey: keys.closureManifestKey,
      closureManifestBytes: manifest.byteLength,
      closureManifestSha256,
      closureManifestEtag: closureManifestPut.etag,
      planSha256: closureManifestSha256,
      advertisedReachableOids,
      semanticExternalOids: boundary.semanticExternalOids,
      thinDeltaBaseOids: requiredRootOids,
      requiredRootOids,
      physicalNodes: physicalPlan.physicalNodes,
      dependencies: physicalPlan.dependencies,
      topologicalEntryIds: physicalPlan.topologicalEntryIds,
      selectedPackChecksums,
      activePackBindings,
      incomingObjectCount,
      visitedIncomingObjectCount: boundary.visitedIncomingObjectCount,
      logicalEdgeCount: boundary.logicalEdgeCount,
      internalEdgeCount: boundary.internalEdgeCount,
      externalEdgeCount: boundary.externalEdgeCount,
      missingObjectCount: boundary.missingObjectCount,
      objectTypeCounts: boundary.objectTypeCounts,
      ranges: observed.ranges,
      rangeBytes: observed.rangeBytes,
      rangeRequests: observed.rangeRequests,
      activePackReads: observed.activePackReads,
      activePackTrailerBytes: observed.trailerBytes,
      activePackTrailerRequests: observed.trailerRequests,
      activePackRangeBytes: observed.rangeBytes,
      activePackRangeRequests: observed.rangeRequests,
      activePackWholeBytes: observed.wholeBytes,
      activePackWholeRequests: observed.wholeRequests,
      activePackUnattributedBytes: observed.unattributedBytes,
      activePackUnattributedRequests: observed.unattributedRequests,
      packsTouched: selectedPackChecksums.length,
      selectedPackBytes,
      activePackCount: active.length,
      metadataBytes: counters.metadataBytes + manifest.byteLength,
      metadataRequests: counters.metadataRequests + 1,
      inputBytesRead: counters.inputBytesRead,
      inputRequests: counters.inputRequests,
      prerequisitePayloadBytes: 0,
      prerequisiteHydratedBytes: 0,
    };
  }
  // receive-pack validates each command against a disposable ref at the exact
  // old OID. A rollback target can be an advertised ancestor that does not
  // reach the current tip, so hydrate non-zero command old-OIDs independently
  // from the target closure.
  const commandOldOids = args.commands
    .map((command) => command.oldOid.toLowerCase())
    .filter((oid) => oid !== ZERO_OID);
  const requiredRootOids = [
    ...new Set([...boundary.semanticExternalOids, ...thinDeltaBaseOids, ...commandOldOids]),
  ].sort();
  if (requiredRootOids.length > MAX_RANGE_RECORDS) {
    throw new Error("stock-plan:required-root-limit");
  }
  const rootIterationOids = physicalRootIteration(
    requiredRootOids,
    args.rootIterationOidsForTesting
  );

  let prerequisitePayloadBytes = 0;
  let physicalPlan: StockPhysicalDependencyPlan;
  try {
    await physicalPlanner.materializeSemanticRoots(rootIterationOids);
    physicalPlan = await physicalPlanner.finalize(requiredRootOids);
  } catch (error) {
    throwWrongRangePlannerFailure({
      operationId: args.operationId,
      error,
      observation: activePackObservation,
      active,
      counters,
      startedAt,
    });
  }
  if (physicalPlan.ranges.length > MAX_RANGE_RECORDS) {
    throw new Error("stock-plan:range-record-limit");
  }
  const ranges: StockRequiredRange[] = physicalPlan.ranges.map((range) => ({
    entryId: range.entryId,
    packChecksum: range.packChecksum,
    start: range.start,
    end: range.end,
    reason: "required-object",
    requiredOid: range.oid,
    semanticRootOids: range.semanticRootOids,
  }));
  for (const oid of requiredRootOids) {
    const object = physicalPlan.semanticObjects.get(oid);
    if (!object) throw new Error("stock-plan:required-object-missing");
    prerequisitePayloadBytes += object.payload.byteLength;
    if (prerequisitePayloadBytes > MAX_PREREQUISITE_PAYLOAD_BYTES) {
      throw new Error("stock-plan:prerequisite-payload-limit");
    }
  }
  const observedActivePackReads = reconcileActivePackReads({
    observation: activePackObservation,
    active,
    ranges,
  });
  const prerequisitePack = await buildPackV2(
    requiredRootOids.map((oid) => {
      const object = physicalPlan.semanticObjects.get(oid);
      if (!object) throw new Error("stock-plan:required-object-missing");
      return { type: object.type, payload: object.payload };
    }),
    // The independent Git fixture uses pack.compression=9. Pinning the level
    // here makes prerequisite bytes reproducible across host and workerd while
    // leaving every other buildPackV2 caller on its existing runtime default.
    { compressionLevel: 9 }
  );
  if (prerequisitePack.byteLength > MAX_INPUT_PACK_BYTES) {
    throw new Error("stock-plan:prerequisite-pack-limit");
  }
  const prerequisitePackSha256 = await digestHex("SHA-256", prerequisitePack);
  const prerequisitePut = await putImmutableBytes({
    env: args.env,
    key: keys.prerequisitePackKey,
    bytes: prerequisitePack,
    sha256: prerequisitePackSha256,
    limiter: args.limiter,
    countSubrequest: args.countSubrequest,
  });
  if (prerequisitePut.created) ownedImmutableKeys.push(keys.prerequisitePackKey);

  const observedRanges = observedActivePackReads.ranges;
  const selectedChecksums = [
    ...new Set(physicalPlan.physicalNodes.map((node) => node.packChecksum)),
  ].sort();
  const selectedPackBytes = selectedChecksums.reduce((total, checksum) => {
    const pack = activeByChecksum.get(checksum);
    if (!pack) throw new Error("stock-plan:selected-pack-missing");
    return total + pack.source.packBytes;
  }, 0);
  const rangeBytes = observedActivePackReads.rangeBytes;
  const planDocument = {
    schemaVersion: 2,
    operationId: args.operationId,
    repositoryId: args.repoId,
    input: {
      key: args.inputRequestKey,
      bytes: args.inputRequestBytes,
      sha256: args.inputRequestSha256,
      packOffset: args.packOffset,
      packBytes: args.packBytes,
    },
    commands: args.commands,
    advertisedRefs: args.advertisedRefs,
    activePacks: active.map((pack) => ({
      packKey: pack.source.packKey,
      packBytes: pack.source.packBytes,
      objectCount: pack.source.idx.count,
      packChecksum: pack.packChecksum,
      idxChecksum: bytesToHex(pack.source.idx.idxChecksum),
      idxSha256: pack.idxSha256,
      refsSha256: pack.refsSha256,
    })),
    advertisedReachableOids,
    semanticExternalOids: boundary.semanticExternalOids,
    thinDeltaBaseOids: [...thinDeltaBaseOids].sort(),
    requiredRootOids,
    physicalNodes: physicalPlan.physicalNodes,
    dependencies: physicalPlan.dependencies,
    topologicalEntryIds: physicalPlan.topologicalEntryIds,
    selectedPackChecksums: selectedChecksums,
    activePackBindings: active.map((pack) => ({
      packKey: pack.source.packKey,
      packBytes: pack.source.packBytes,
      idxBytes: pack.idxBytes,
      packChecksum: pack.packChecksum,
      idxSha256: pack.idxSha256,
      prefSha256: pack.refsSha256,
    })),
    closure: {
      incomingObjectCount,
      visitedIncomingObjectCount: boundary.visitedIncomingObjectCount,
      logicalEdgeCount: boundary.logicalEdgeCount,
      internalEdgeCount: boundary.internalEdgeCount,
      externalEdgeCount: boundary.externalEdgeCount,
      missingObjectCount: boundary.missingObjectCount,
      objectTypeCounts: boundary.objectTypeCounts,
    },
    ranges: observedRanges,
    activePackReads: observedActivePackReads.activePackReads,
    activePackReadMetrics: {
      trailerBytes: observedActivePackReads.trailerBytes,
      trailerRequests: observedActivePackReads.trailerRequests,
      rangeBytes: observedActivePackReads.rangeBytes,
      rangeRequests: observedActivePackReads.rangeRequests,
      wholeBytes: observedActivePackReads.wholeBytes,
      wholeRequests: observedActivePackReads.wholeRequests,
      unattributedBytes: observedActivePackReads.unattributedBytes,
      unattributedRequests: observedActivePackReads.unattributedRequests,
    },
    prerequisite: {
      key: keys.prerequisitePackKey,
      bytes: prerequisitePack.byteLength,
      sha256: prerequisitePackSha256,
      objectOids: requiredRootOids,
    },
  };
  const manifest = new TextEncoder().encode(JSON.stringify(planDocument));
  if (manifest.byteLength > MAX_MANIFEST_BYTES) throw new Error("stock-plan:manifest-limit");
  const closureManifestSha256 = await digestHex("SHA-256", manifest);
  const closureManifestPut = await putImmutableBytes({
    env: args.env,
    key: keys.closureManifestKey,
    bytes: manifest,
    sha256: closureManifestSha256,
    limiter: args.limiter,
    countSubrequest: args.countSubrequest,
  });
  if (closureManifestPut.created) ownedImmutableKeys.push(keys.closureManifestKey);

  await deletePlannerKeys({
    env: args.env,
    keys: [
      keys.temporaryPackKey,
      packIndexKey(keys.temporaryPackKey),
      packRefsKey(keys.temporaryPackKey),
    ],
    limiter: args.limiter,
    countSubrequest: args.countSubrequest,
  });
  const temporaryOwnedIndex = ownedImmutableKeys.indexOf(keys.temporaryPackKey);
  if (temporaryOwnedIndex >= 0) ownedImmutableKeys.splice(temporaryOwnedIndex, 1);
  log.info("stock-plan:complete", {
    operationId: args.operationId,
    activePackCount: active.length,
    advertisedReachableCount: advertisedReachableOids.length,
    incomingObjectCount,
    semanticExternalCount: boundary.semanticExternalOids.length,
    thinDeltaBaseCount: thinDeltaBaseOids.size,
    requiredRootCount: requiredRootOids.length,
    rangeBytes,
    packsTouched: selectedChecksums.length,
    elapsedMs: Date.now() - startedAt,
  });

  return {
    executionMode: "stock-container",
    prerequisitePackKey: keys.prerequisitePackKey,
    prerequisitePackBytes: prerequisitePack.byteLength,
    prerequisitePackSha256,
    prerequisitePackEtag: prerequisitePut.etag,
    closureManifestKey: keys.closureManifestKey,
    closureManifestBytes: manifest.byteLength,
    closureManifestSha256,
    closureManifestEtag: closureManifestPut.etag,
    planSha256: closureManifestSha256,
    advertisedReachableOids,
    semanticExternalOids: boundary.semanticExternalOids,
    thinDeltaBaseOids: [...thinDeltaBaseOids].sort(),
    requiredRootOids,
    physicalNodes: physicalPlan.physicalNodes,
    dependencies: physicalPlan.dependencies,
    topologicalEntryIds: physicalPlan.topologicalEntryIds,
    selectedPackChecksums: selectedChecksums,
    activePackBindings: active.map((pack) => ({
      packKey: pack.source.packKey,
      packBytes: pack.source.packBytes,
      idxBytes: pack.idxBytes,
      packChecksum: pack.packChecksum,
      idxSha256: pack.idxSha256,
      prefSha256: pack.refsSha256,
    })),
    incomingObjectCount,
    visitedIncomingObjectCount: boundary.visitedIncomingObjectCount,
    logicalEdgeCount: boundary.logicalEdgeCount,
    internalEdgeCount: boundary.internalEdgeCount,
    externalEdgeCount: boundary.externalEdgeCount,
    missingObjectCount: boundary.missingObjectCount,
    objectTypeCounts: boundary.objectTypeCounts,
    ranges: observedRanges,
    rangeBytes,
    rangeRequests: observedActivePackReads.rangeRequests,
    activePackReads: observedActivePackReads.activePackReads,
    activePackTrailerBytes: observedActivePackReads.trailerBytes,
    activePackTrailerRequests: observedActivePackReads.trailerRequests,
    activePackRangeBytes: observedActivePackReads.rangeBytes,
    activePackRangeRequests: observedActivePackReads.rangeRequests,
    activePackWholeBytes: observedActivePackReads.wholeBytes,
    activePackWholeRequests: observedActivePackReads.wholeRequests,
    activePackUnattributedBytes: observedActivePackReads.unattributedBytes,
    activePackUnattributedRequests: observedActivePackReads.unattributedRequests,
    packsTouched: selectedChecksums.length,
    selectedPackBytes,
    activePackCount: active.length,
    metadataBytes: counters.metadataBytes,
    metadataRequests: counters.metadataRequests,
    inputBytesRead: counters.inputBytesRead,
    inputRequests: counters.inputRequests,
    prerequisitePayloadBytes,
    prerequisiteHydratedBytes: prerequisitePayloadBytes,
  };
}

export async function planStockReceive(args: PlanStockReceiveArgs): Promise<StockReceivePlan> {
  const ownedImmutableKeys: string[] = [];
  try {
    return await planStockReceiveImpl(args, ownedImmutableKeys);
  } catch (error) {
    const keys = stockReceivePlanKeys(args.inputRequestKey, args.inputRequestSha256);
    try {
      await deletePlannerKeys({
        env: args.env,
        keys: [
          keys.temporaryPackKey,
          packIndexKey(keys.temporaryPackKey),
          packRefsKey(keys.temporaryPackKey),
          ...ownedImmutableKeys.filter((key) => key !== keys.temporaryPackKey),
        ],
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
      });
    } catch (cleanupError) {
      throw new AggregateError([error, cleanupError], "stock-plan:failed-with-cleanup-error");
    }
    throw error;
  }
}
