import type { Logger } from "@/worker/common/logger";
import type { GitObjectType } from "@/worker/git/core/objects";
import type {
  IndexedPackSource,
  PackedObjectCandidate,
} from "@/worker/git/object-store/candidates";
import type { PackedObjectResult } from "@/worker/git/object-store/types";

import { asBufferSource, bytesToHex, inflate } from "@/worker/common";
import { computeOid } from "@/worker/git/core/objects";
import { collectPackedObjectCandidates } from "@/worker/git/object-store/candidates";
import { applyGitDelta } from "@/worker/git/object-store/delta";
import {
  findOffsetIndex,
  getNextOffsetByIndex,
  getOidHexAt,
} from "@/worker/git/object-store/idxView";
import { typeCodeToObjectType } from "@/worker/git/object-store/support";
import { decodePackObjectSize, readPackHeaderExFromBuf } from "@/worker/git/pack/packMeta";

export const STOCK_MAX_PHYSICAL_NODES = 256;
export const STOCK_MAX_DEPENDENCY_DEPTH = 255;

export type StockBoundPackSource = {
  source: IndexedPackSource;
  /** SHA-1 trailer already rebound to the IDX and PREF authority. */
  packChecksum: string;
  /** SHA-256 of the exact IDX bytes bound to the pack trailer. */
  idxSha256: string;
  /** SHA-256 of the exact PREF bytes bound to the IDX. */
  prefSha256: string;
};

export type StockPhysicalNodeEncoding = "full" | "ofs-delta" | "ref-delta";

export type StockPhysicalDependencyEdge = {
  dependentEntryId: string;
  baseEntryId: string;
  kind: "ofs" | "ref";
  baseOffset?: number | undefined;
  baseOid?: string | undefined;
};

export type StockPhysicalRange = {
  entryId: string;
  packChecksum: string;
  start: number;
  end: number;
  oid: string;
  semanticRootOids: string[];
};

export type StockPhysicalNode = {
  entryId: string;
  packChecksum: string;
  idxSha256: string;
  prefSha256: string;
  offset: number;
  end: number;
  oid: string;
  objectType: GitObjectType;
  encoding: StockPhysicalNodeEncoding;
  semanticRootOids: string[];
  oidVerified: true;
  integrityBound: true;
  baseEntryId?: string | undefined;
  baseOid?: string | undefined;
};

/** Canonical public ordering follows the independently auditable physical tuple. */
export function compareStockPhysicalNodes(
  left: StockPhysicalNode,
  right: StockPhysicalNode
): number {
  return (
    left.packChecksum.localeCompare(right.packChecksum) ||
    left.offset - right.offset ||
    left.end - right.end ||
    left.oid.localeCompare(right.oid)
  );
}

export type StockPhysicalDependencyPlan = {
  semanticRootOids: string[];
  physicalNodes: StockPhysicalNode[];
  dependencies: StockPhysicalDependencyEdge[];
  /** Entry ids in deterministic base-first order. */
  topologicalEntryIds: string[];
  /** Actual one-per-node reads in topological order. */
  ranges: StockPhysicalRange[];
  /** Only semantic prerequisite roots, never encoding-only bases. */
  semanticObjects: Map<string, PackedObjectResult>;
};

export type PlanStockPhysicalDependenciesArgs = {
  sources: readonly StockBoundPackSource[];
  semanticRootOids: readonly string[];
  readEntry: (
    candidate: PackedObjectCandidate,
    semanticRootOid: string
  ) => Promise<Uint8Array | undefined>;
  maxEntryBytes: number;
  maxInflatedBytes: number;
  maxDeltaResultBytes: number;
  signal?: AbortSignal | undefined;
  log?: Logger | undefined;
};

export type StockPhysicalDependencyPlanner = {
  materializeSemanticRoot(oid: string): Promise<PackedObjectResult>;
  finalize(semanticRootOids: readonly string[]): Promise<StockPhysicalDependencyPlan>;
};

type NodeColor = "gray" | "black";

type MutablePhysicalNode = {
  internalId: string;
  entryId?: string | undefined;
  candidate: PackedObjectCandidate;
  bound: StockBoundPackSource;
  color: NodeColor;
  encoding?: StockPhysicalNodeEncoding | undefined;
  baseInternalId?: string | undefined;
  baseOffset?: number | undefined;
  baseOid?: string | undefined;
  object?: PackedObjectResult | undefined;
  semanticRootOids: Set<string>;
};

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw new Error("stock-physical-plan:aborted");
}

function assertOid(oid: string, label: string): void {
  if (!/^[0-9a-f]{40}$/.test(oid)) {
    throw new Error(`stock-physical-plan:${label}-oid`);
  }
}

function assertSha256(value: string, label: string): void {
  if (!/^[0-9a-f]{64}$/.test(value)) {
    throw new Error(`stock-physical-plan:${label}-sha256`);
  }
}

function internalPhysicalNodeId(packChecksum: string, candidate: PackedObjectCandidate): string {
  return `${packChecksum}:${candidate.offset}:${candidate.nextOffset}`;
}

function candidatesBindEquivalentBytes(
  sourceByPackKey: ReadonlyMap<string, StockBoundPackSource>,
  left: PackedObjectCandidate,
  right: PackedObjectCandidate
): boolean {
  const leftBound = sourceByPackKey.get(left.source.packKey);
  const rightBound = sourceByPackKey.get(right.source.packKey);
  if (!leftBound || !rightBound) {
    throw new Error("stock-physical-plan:unbound-candidate");
  }
  return (
    leftBound.packChecksum === rightBound.packChecksum &&
    leftBound.idxSha256 === rightBound.idxSha256 &&
    leftBound.prefSha256 === rightBound.prefSha256 &&
    left.source.packBytes === right.source.packBytes &&
    left.objectIndex === right.objectIndex &&
    left.offset === right.offset &&
    left.nextOffset === right.nextOffset &&
    left.oid === right.oid
  );
}

async function sha256Text(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

export async function stockPhysicalEntryId(value: {
  packChecksum: string;
  idxSha256: string;
  prefSha256: string;
  offset: number;
  end: number;
  oid: string;
}): Promise<string> {
  return await sha256Text(
    [
      "stock-physical-entry-v1",
      value.packChecksum,
      value.idxSha256,
      value.prefSha256,
      String(value.offset),
      String(value.end),
      value.oid,
    ].join("\0")
  );
}

function compareCandidates(
  checksumByPackKey: ReadonlyMap<string, string>,
  left: PackedObjectCandidate,
  right: PackedObjectCandidate
): number {
  const leftChecksum = checksumByPackKey.get(left.source.packKey);
  const rightChecksum = checksumByPackKey.get(right.source.packKey);
  if (!leftChecksum || !rightChecksum) {
    throw new Error("stock-physical-plan:unbound-candidate");
  }
  return (
    leftChecksum.localeCompare(rightChecksum) ||
    left.source.packKey.localeCompare(right.source.packKey) ||
    left.offset - right.offset ||
    left.nextOffset - right.nextOffset ||
    left.objectIndex - right.objectIndex
  );
}

function makeOffsetCandidate(
  source: IndexedPackSource,
  packSlot: number,
  offset: number
): PackedObjectCandidate | undefined {
  const objectIndex = findOffsetIndex(source.idx, offset);
  if (objectIndex === undefined) return undefined;
  const nextOffset = getNextOffsetByIndex(source.idx, objectIndex);
  if (nextOffset === undefined) return undefined;
  return {
    source,
    packSlot,
    objectIndex,
    offset,
    nextOffset,
    oid: getOidHexAt(source.idx, objectIndex),
  };
}

function packedObject(
  candidate: PackedObjectCandidate,
  type: GitObjectType,
  payload: Uint8Array
): PackedObjectResult {
  return {
    packKey: candidate.source.packKey,
    objectIndex: candidate.objectIndex,
    offset: candidate.offset,
    nextOffset: candidate.nextOffset,
    oid: candidate.oid,
    type,
    payload,
  };
}

/**
 * Create one operation-scoped physical planner. Incoming thin-pack resolution
 * can add true external roots as it discovers them, and the semantic closure
 * walk can add the remaining roots. Physical nodes stay shared and are read
 * once across both phases. Finalization canonicalizes public ordering so it is
 * independent of discovery or caller root order.
 */
export function createStockPhysicalDependencyPlanner(
  args: Omit<PlanStockPhysicalDependenciesArgs, "semanticRootOids">
): StockPhysicalDependencyPlanner {
  if (
    !Number.isSafeInteger(args.maxEntryBytes) ||
    args.maxEntryBytes <= 0 ||
    !Number.isSafeInteger(args.maxInflatedBytes) ||
    args.maxInflatedBytes <= 0 ||
    !Number.isSafeInteger(args.maxDeltaResultBytes) ||
    args.maxDeltaResultBytes <= 0
  ) {
    throw new Error("stock-physical-plan:invalid-size-limit");
  }

  const sourceByPackKey = new Map<string, StockBoundPackSource>();
  const checksumByPackKey = new Map<string, string>();
  for (const bound of args.sources) {
    assertOid(bound.packChecksum, "pack-checksum");
    assertSha256(bound.idxSha256, "idx");
    assertSha256(bound.prefSha256, "pref");
    if (sourceByPackKey.has(bound.source.packKey)) {
      throw new Error("stock-physical-plan:duplicate-pack-key");
    }
    sourceByPackKey.set(bound.source.packKey, bound);
    checksumByPackKey.set(bound.source.packKey, bound.packChecksum);
  }

  const sources = [...args.sources]
    .sort(
      (left, right) =>
        left.packChecksum.localeCompare(right.packChecksum) ||
        left.idxSha256.localeCompare(right.idxSha256) ||
        left.prefSha256.localeCompare(right.prefSha256) ||
        left.source.packKey.localeCompare(right.source.packKey)
    )
    .map((bound) => bound.source);
  const nodes = new Map<string, MutablePhysicalNode>();
  const selectedCandidateByOid = new Map<string, PackedObjectCandidate>();
  const rootNodeByOid = new Map<string, MutablePhysicalNode>();
  let finalized = false;

  const selectCandidate = (oid: string): PackedObjectCandidate => {
    assertOid(oid, "dependency");
    const selected = selectedCandidateByOid.get(oid);
    if (selected) return selected;
    const candidates = collectPackedObjectCandidates(sources, oid).sort((left, right) =>
      compareCandidates(checksumByPackKey, left, right)
    );
    if (candidates.length === 0) {
      throw new Error("stock-physical-plan:dependency-missing");
    }
    const candidateByPhysicalId = new Map<string, PackedObjectCandidate>();
    for (const candidate of candidates) {
      const checksum = checksumByPackKey.get(candidate.source.packKey)!;
      const physicalId = internalPhysicalNodeId(checksum, candidate);
      const previous = candidateByPhysicalId.get(physicalId);
      if (previous && !candidatesBindEquivalentBytes(sourceByPackKey, previous, candidate)) {
        throw new Error("stock-physical-plan:duplicate-ambiguous");
      }
      candidateByPhysicalId.set(physicalId, previous ?? candidate);
    }
    const candidate = candidates[0]!;
    selectedCandidateByOid.set(oid, candidate);
    return candidate;
  };

  const visit = async (
    candidate: PackedObjectCandidate,
    depth: number,
    semanticRootOid: string
  ): Promise<MutablePhysicalNode> => {
    throwIfAborted(args.signal);
    if (depth > STOCK_MAX_DEPENDENCY_DEPTH) {
      throw new Error("stock-physical-plan:dependency-depth-limit");
    }
    const bound = sourceByPackKey.get(candidate.source.packKey);
    if (!bound) throw new Error("stock-physical-plan:unbound-candidate");
    const internalId = internalPhysicalNodeId(bound.packChecksum, candidate);
    const existing = nodes.get(internalId);
    if (existing) {
      if (
        existing.candidate.source.packKey !== candidate.source.packKey ||
        existing.candidate.objectIndex !== candidate.objectIndex ||
        existing.candidate.oid !== candidate.oid ||
        existing.candidate.offset !== candidate.offset ||
        existing.candidate.nextOffset !== candidate.nextOffset
      ) {
        throw new Error("stock-physical-plan:duplicate-mismatch");
      }
      if (existing.color === "gray") {
        throw new Error("stock-physical-plan:dependency-cycle");
      }
      return existing;
    }
    if (nodes.size >= STOCK_MAX_PHYSICAL_NODES) {
      throw new Error("stock-physical-plan:physical-node-limit");
    }
    const entryLength = candidate.nextOffset - candidate.offset;
    if (entryLength <= 0 || entryLength > args.maxEntryBytes) {
      throw new Error("stock-physical-plan:entry-size-limit");
    }

    const node: MutablePhysicalNode = {
      internalId,
      candidate,
      bound,
      color: "gray",
      semanticRootOids: new Set(),
    };
    nodes.set(internalId, node);
    args.log?.debug("stock-physical-plan:read-node", {
      physicalNodeId: internalId,
      objectOid: candidate.oid,
      packKey: candidate.source.packKey,
      offset: candidate.offset,
      length: entryLength,
    });
    const entry = await args.readEntry(candidate, semanticRootOid);
    if (!entry || entry.byteLength !== entryLength) {
      throw new Error("stock-physical-plan:entry-read-mismatch");
    }
    const header = readPackHeaderExFromBuf(entry, 0);
    if (!header) throw new Error("stock-physical-plan:malformed-header");
    const inflatedSize = decodePackObjectSize(header.sizeVarBytes);
    if (inflatedSize === undefined || inflatedSize > args.maxInflatedBytes) {
      throw new Error("stock-physical-plan:inflated-size-limit");
    }
    const inflated = await inflate(entry.subarray(header.headerLen));
    if (inflated.byteLength !== inflatedSize) {
      throw new Error("stock-physical-plan:inflated-size-mismatch");
    }

    const fullType = typeCodeToObjectType(header.type);
    if (fullType) {
      node.encoding = "full";
      node.object = packedObject(candidate, fullType, inflated);
    } else {
      let baseCandidate: PackedObjectCandidate;
      if (header.type === 6) {
        if (!header.baseRel || header.baseRel <= 0) {
          throw new Error("stock-physical-plan:ofs-base-distance");
        }
        const baseOffset = candidate.offset - header.baseRel;
        baseCandidate =
          makeOffsetCandidate(candidate.source, candidate.packSlot, baseOffset) ??
          (() => {
            throw new Error("stock-physical-plan:ofs-base-missing");
          })();
        node.encoding = "ofs-delta";
        node.baseOffset = baseOffset;
      } else if (header.type === 7 && header.baseOid) {
        assertOid(header.baseOid, "ref-base");
        baseCandidate = selectCandidate(header.baseOid);
        node.encoding = "ref-delta";
        node.baseOid = header.baseOid;
      } else {
        throw new Error("stock-physical-plan:unsupported-encoding");
      }

      const baseNode = await visit(baseCandidate, depth + 1, semanticRootOid);
      if (!baseNode.object) throw new Error("stock-physical-plan:base-unmaterialized");
      node.baseInternalId = baseNode.internalId;
      node.object = packedObject(
        candidate,
        baseNode.object.type,
        applyGitDelta(baseNode.object.payload, inflated, {
          maxResultBytes: args.maxDeltaResultBytes,
        })
      );
    }

    if (
      !node.object ||
      (await computeOid(node.object.type, node.object.payload)) !== candidate.oid
    ) {
      throw new Error("stock-physical-plan:canonical-oid-mismatch");
    }
    node.entryId = await stockPhysicalEntryId({
      packChecksum: bound.packChecksum,
      idxSha256: bound.idxSha256,
      prefSha256: bound.prefSha256,
      offset: candidate.offset,
      end: candidate.nextOffset,
      oid: candidate.oid,
    });
    node.color = "black";
    return node;
  };

  const materializeSemanticRoot = async (oid: string): Promise<PackedObjectResult> => {
    if (finalized) throw new Error("stock-physical-plan:already-finalized");
    assertOid(oid, "semantic-root");
    const existing = rootNodeByOid.get(oid);
    if (existing?.object) return existing.object;
    if (rootNodeByOid.size >= STOCK_MAX_PHYSICAL_NODES) {
      throw new Error("stock-physical-plan:semantic-root-limit");
    }
    const rootNode = await visit(selectCandidate(oid), 0, oid);
    if (rootNode.object?.oid !== oid) {
      throw new Error("stock-physical-plan:semantic-root-mismatch");
    }
    rootNodeByOid.set(oid, rootNode);
    return rootNode.object;
  };

  const finalize = async (
    requestedSemanticRootOids: readonly string[]
  ): Promise<StockPhysicalDependencyPlan> => {
    if (finalized) throw new Error("stock-physical-plan:already-finalized");
    const semanticRootOids = [...new Set(requestedSemanticRootOids)].sort();
    if (semanticRootOids.length > STOCK_MAX_PHYSICAL_NODES) {
      throw new Error("stock-physical-plan:semantic-root-limit");
    }
    for (const oid of semanticRootOids) {
      assertOid(oid, "semantic-root");
      await materializeSemanticRoot(oid);
    }
    finalized = true;

    const attachSemanticRoot = (internalId: string, rootOid: string): void => {
      const node = nodes.get(internalId);
      if (!node) throw new Error("stock-physical-plan:node-missing");
      if (node.semanticRootOids.has(rootOid)) return;
      node.semanticRootOids.add(rootOid);
      if (node.baseInternalId) attachSemanticRoot(node.baseInternalId, rootOid);
    };
    for (const rootOid of semanticRootOids) {
      const rootNode = rootNodeByOid.get(rootOid);
      if (!rootNode) throw new Error("stock-physical-plan:semantic-root-unmaterialized");
      attachSemanticRoot(rootNode.internalId, rootOid);
    }

    const entryIdByInternalId = new Map<string, string>();
    for (const node of nodes.values()) {
      if (!node.entryId || !node.object || !node.encoding || node.color !== "black") {
        throw new Error("stock-physical-plan:node-unmaterialized");
      }
      entryIdByInternalId.set(node.internalId, node.entryId);
    }

    const physicalNodes = [...nodes.values()]
      .map((node): StockPhysicalNode => {
        const baseNode = node.baseInternalId ? nodes.get(node.baseInternalId) : undefined;
        if (!node.object || !node.encoding) {
          throw new Error("stock-physical-plan:node-unmaterialized");
        }
        return {
          entryId: node.entryId!,
          packChecksum: node.bound.packChecksum,
          idxSha256: node.bound.idxSha256,
          prefSha256: node.bound.prefSha256,
          offset: node.candidate.offset,
          end: node.candidate.nextOffset,
          oid: node.object.oid,
          objectType: node.object.type,
          encoding: node.encoding,
          semanticRootOids: [...node.semanticRootOids].sort(),
          oidVerified: true,
          integrityBound: true,
          baseEntryId: node.baseInternalId
            ? entryIdByInternalId.get(node.baseInternalId)
            : undefined,
          baseOid: baseNode?.object?.oid,
        };
      })
      .sort(compareStockPhysicalNodes);
    const physicalNodeByEntryId = new Map(physicalNodes.map((node) => [node.entryId, node]));
    const compareEntryIds = (left: string, right: string): number => {
      const leftNode = physicalNodeByEntryId.get(left);
      const rightNode = physicalNodeByEntryId.get(right);
      if (!leftNode || !rightNode) throw new Error("stock-physical-plan:node-missing");
      return compareStockPhysicalNodes(leftNode, rightNode);
    };

    const dependencies = [...nodes.values()]
      .filter((node) => node.baseInternalId !== undefined)
      .map((node): StockPhysicalDependencyEdge => {
        const dependentEntryId = entryIdByInternalId.get(node.internalId);
        const baseEntryId = entryIdByInternalId.get(node.baseInternalId!);
        const baseNode = nodes.get(node.baseInternalId!);
        if (!dependentEntryId || !baseEntryId || !baseNode?.object || !node.encoding) {
          throw new Error("stock-physical-plan:dependency-unbound");
        }
        if (node.encoding === "ofs-delta") {
          if (node.baseOffset !== baseNode.candidate.offset) {
            throw new Error("stock-physical-plan:ofs-base-offset-mismatch");
          }
          return {
            baseEntryId,
            baseOffset: node.baseOffset,
            dependentEntryId,
            kind: "ofs",
          };
        }
        if (node.encoding !== "ref-delta" || node.baseOid !== baseNode.object.oid) {
          throw new Error("stock-physical-plan:ref-base-oid-mismatch");
        }
        return {
          baseEntryId,
          baseOid: node.baseOid,
          dependentEntryId,
          kind: "ref",
        };
      })
      .sort(
        (left, right) =>
          compareEntryIds(left.dependentEntryId, right.dependentEntryId) ||
          compareEntryIds(left.baseEntryId, right.baseEntryId) ||
          left.kind.localeCompare(right.kind)
      );

    const childrenByBase = new Map<string, string[]>();
    for (const node of nodes.values()) {
      if (!node.baseInternalId) continue;
      const children = childrenByBase.get(node.baseInternalId) ?? [];
      children.push(node.internalId);
      childrenByBase.set(node.baseInternalId, children);
    }
    for (const children of childrenByBase.values()) {
      children.sort((left, right) =>
        compareEntryIds(entryIdByInternalId.get(left)!, entryIdByInternalId.get(right)!)
      );
    }
    const ready = [...nodes.values()]
      .filter((node) => !node.baseInternalId)
      .map((node) => node.internalId)
      .sort((left, right) =>
        compareEntryIds(entryIdByInternalId.get(left)!, entryIdByInternalId.get(right)!)
      );
    const topologicalInternalIds: string[] = [];
    while (ready.length > 0) {
      const internalId = ready.shift()!;
      topologicalInternalIds.push(internalId);
      ready.push(...(childrenByBase.get(internalId) ?? []));
      ready.sort((left, right) =>
        compareEntryIds(entryIdByInternalId.get(left)!, entryIdByInternalId.get(right)!)
      );
    }
    if (topologicalInternalIds.length !== nodes.size) {
      throw new Error("stock-physical-plan:dependency-cycle");
    }
    const topologicalEntryIds = topologicalInternalIds.map(
      (internalId) => entryIdByInternalId.get(internalId)!
    );

    const ranges = topologicalInternalIds.map((internalId): StockPhysicalRange => {
      const node = nodes.get(internalId);
      if (!node?.object || !node.entryId) {
        throw new Error("stock-physical-plan:node-unmaterialized");
      }
      return {
        entryId: node.entryId,
        packChecksum: node.bound.packChecksum,
        start: node.candidate.offset,
        end: node.candidate.nextOffset,
        oid: node.object.oid,
        semanticRootOids: [...node.semanticRootOids].sort(),
      };
    });
    const semanticObjects = new Map<string, PackedObjectResult>();
    for (const rootOid of semanticRootOids) {
      const object = rootNodeByOid.get(rootOid)?.object;
      if (!object) throw new Error("stock-physical-plan:semantic-root-unmaterialized");
      semanticObjects.set(rootOid, object);
    }

    args.log?.info("stock-physical-plan:complete", {
      semanticRootCount: semanticRootOids.length,
      physicalNodeCount: physicalNodes.length,
      dependencyEdgeCount: dependencies.length,
    });
    return {
      semanticRootOids,
      physicalNodes,
      dependencies,
      topologicalEntryIds,
      ranges,
      semanticObjects,
    };
  };

  return { materializeSemanticRoot, finalize };
}

/** Build a complete order-independent physical plan from a known root set. */
export async function planStockPhysicalDependencies(
  args: PlanStockPhysicalDependenciesArgs
): Promise<StockPhysicalDependencyPlan> {
  const planner = createStockPhysicalDependencyPlanner(args);
  // Exercise the caller-supplied enumeration. finalize() canonicalizes the
  // retained identity after discovery, so permutations must converge without
  // disguising an order-sensitive traversal behind a pre-sort.
  for (const oid of args.semanticRootOids) {
    await planner.materializeSemanticRoot(oid);
  }
  return await planner.finalize(args.semanticRootOids);
}
