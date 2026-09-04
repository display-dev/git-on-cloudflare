import type {
  NativeReceiveActivePackBinding,
  NativeReceiveOperation,
  NativeReceiveProcessResult,
  NativeReceiveStockActivePackRead,
  NativeReceiveStockRange,
} from "./types";
import type { StockPhysicalDependencyEdge, StockPhysicalNode } from "./physicalDependencyPlan";
import { STOCK_METADATA_MAX_BYTES } from "./types";

import { compareStockPhysicalNodes, stockPhysicalEntryId } from "./physicalDependencyPlan";
import { catalogMetadataBundleKey, catalogMetadataFingerprint } from "./catalogMetadataBundle";

const OID = /^[0-9a-f]{40}$/;
const SHA256 = /^[0-9a-f]{64}$/;
const MAX_LOGICAL_OBJECTS = 100_000;
const MAX_LOGICAL_EDGES = 500_000;
const MAX_PHYSICAL_NODES = 256;
const MAX_DEPENDENCY_DEPTH = 255;
const MAX_ACTIVE_PACK_READS = 320;

async function activeMetadataBundleValid(
  operation: NativeReceiveOperation,
  result: NativeReceiveProcessResult
): Promise<boolean> {
  const bundle = result.activeMetadataBundle;
  if (!bundle) return true;
  if (operation.activeCatalog.length === 0) return false;
  const fingerprint = await catalogMetadataFingerprint(operation.activeCatalog);
  return (
    bundle.catalogFingerprint === fingerprint &&
    bundle.key === (await catalogMetadataBundleKey(operation.activeCatalog)) &&
    bundle.bytes > 0 &&
    bundle.bytes <= STOCK_METADATA_MAX_BYTES &&
    SHA256.test(bundle.sha256) &&
    bundle.etag.length > 0 &&
    bundle.etag.length <= 256 &&
    result.metadataRequests !== undefined &&
    result.metadataRequests >= 1 &&
    result.metadataBytes !== undefined &&
    result.metadataBytes >= bundle.bytes
  );
}

function sortedUnique(values: string[], pattern: RegExp, maximum: number): boolean {
  return (
    values.length <= maximum &&
    values.every(
      (value, index) => pattern.test(value) && (index === 0 || values[index - 1]! < value)
    )
  );
}

function equalStrings(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function rangeKey(value: {
  packChecksum: string;
  start: number;
  end: number;
  requiredOid: string;
}): string {
  return `${value.packChecksum}:${value.start}:${value.end}:${value.requiredOid}`;
}

type BoundStockRange = NativeReceiveStockRange & {
  entryId: string;
  semanticRootOids: string[];
};

function rangeValid(range: NativeReceiveStockRange): range is BoundStockRange {
  return (
    SHA256.test(range.entryId ?? "") &&
    OID.test(range.packChecksum) &&
    OID.test(range.requiredOid) &&
    Number.isSafeInteger(range.start) &&
    Number.isSafeInteger(range.end) &&
    range.start >= 0 &&
    range.end > range.start &&
    range.reason === "required-object" &&
    sortedUnique(range.semanticRootOids ?? [], OID, MAX_PHYSICAL_NODES)
  );
}

function readValid(read: NativeReceiveStockActivePackRead): boolean {
  return (
    OID.test(read.packChecksum) &&
    Number.isSafeInteger(read.start) &&
    Number.isSafeInteger(read.end) &&
    Number.isSafeInteger(read.returnedBytes) &&
    read.start >= 0 &&
    read.end > read.start &&
    read.returnedBytes === read.end - read.start &&
    (read.kind === "trailer"
      ? read.returnedBytes === 20
      : read.kind === "whole"
        ? read.start === 0
        : OID.test(read.requiredOid))
  );
}

function bindingValid(binding: NativeReceiveActivePackBinding): boolean {
  return (
    binding.packKey.length > 0 &&
    binding.packKey.length <= 1_024 &&
    Number.isSafeInteger(binding.packBytes) &&
    binding.packBytes > 20 &&
    Number.isSafeInteger(binding.idxBytes) &&
    binding.idxBytes > 0 &&
    OID.test(binding.packChecksum) &&
    SHA256.test(binding.idxSha256) &&
    SHA256.test(binding.prefSha256)
  );
}

function nodeShapeValid(node: StockPhysicalNode, roots: string[]): boolean {
  return (
    SHA256.test(node.entryId) &&
    OID.test(node.packChecksum) &&
    SHA256.test(node.idxSha256) &&
    SHA256.test(node.prefSha256) &&
    Number.isSafeInteger(node.offset) &&
    Number.isSafeInteger(node.end) &&
    node.offset >= 0 &&
    node.end > node.offset &&
    OID.test(node.oid) &&
    (node.objectType === "commit" ||
      node.objectType === "tree" ||
      node.objectType === "blob" ||
      node.objectType === "tag") &&
    (node.encoding === "full" || node.encoding === "ofs-delta" || node.encoding === "ref-delta") &&
    node.oidVerified === true &&
    node.integrityBound === true &&
    sortedUnique(node.semanticRootOids, OID, MAX_PHYSICAL_NODES) &&
    node.semanticRootOids.every((oid) => roots.includes(oid))
  );
}

function dependencyShapeValid(edge: StockPhysicalDependencyEdge): boolean {
  return (
    SHA256.test(edge.dependentEntryId) &&
    SHA256.test(edge.baseEntryId) &&
    edge.dependentEntryId !== edge.baseEntryId &&
    (edge.kind === "ofs"
      ? Number.isSafeInteger(edge.baseOffset) && edge.baseOffset! >= 0 && edge.baseOid === undefined
      : OID.test(edge.baseOid ?? "") && edge.baseOffset === undefined)
  );
}

async function physicalPlanValid(args: {
  operation: NativeReceiveOperation;
  result: NativeReceiveProcessResult;
  roots: string[];
  ranges: NativeReceiveStockRange[];
  reads: NativeReceiveStockActivePackRead[];
}): Promise<boolean> {
  const nodes = args.result.physicalNodes;
  const dependencies = args.result.physicalDependencies;
  const topology = args.result.topologicalEntryIds;
  const selected = args.result.selectedPackChecksums;
  const bindings = args.result.activePackBindings;
  if (
    !nodes ||
    !dependencies ||
    !topology ||
    !selected ||
    !bindings ||
    nodes.length > MAX_PHYSICAL_NODES ||
    dependencies.length > MAX_PHYSICAL_NODES - 1 ||
    topology.length !== nodes.length ||
    bindings.length !== args.operation.activeCatalog.length ||
    !sortedUnique(selected, OID, MAX_PHYSICAL_NODES)
  ) {
    return false;
  }

  const nodeById = new Map<string, StockPhysicalNode>();
  for (const node of nodes) {
    if (!nodeShapeValid(node, args.roots) || nodeById.has(node.entryId)) return false;
    if (
      (await stockPhysicalEntryId({
        packChecksum: node.packChecksum,
        idxSha256: node.idxSha256,
        prefSha256: node.prefSha256,
        offset: node.offset,
        end: node.end,
        oid: node.oid,
      })) !== node.entryId
    ) {
      return false;
    }
    nodeById.set(node.entryId, node);
  }
  const canonicalNodes = [...nodes].sort(compareStockPhysicalNodes);
  if (
    !equalStrings(
      nodes.map((node) => node.entryId),
      canonicalNodes.map((node) => node.entryId)
    )
  ) {
    return false;
  }

  const catalogByKey = new Map(args.operation.activeCatalog.map((row) => [row.packKey, row]));
  const bindingByChecksum = new Map<string, NativeReceiveActivePackBinding>();
  const bindingKeys = new Set<string>();
  for (const binding of bindings) {
    const catalog = catalogByKey.get(binding.packKey);
    const equivalent = bindingByChecksum.get(binding.packChecksum);
    if (
      !bindingValid(binding) ||
      bindingKeys.has(binding.packKey) ||
      !catalog ||
      catalog.packBytes !== binding.packBytes ||
      catalog.idxBytes !== binding.idxBytes ||
      (equivalent !== undefined &&
        (equivalent.packBytes !== binding.packBytes ||
          equivalent.idxBytes !== binding.idxBytes ||
          equivalent.idxSha256 !== binding.idxSha256 ||
          equivalent.prefSha256 !== binding.prefSha256))
    ) {
      return false;
    }
    bindingKeys.add(binding.packKey);
    bindingByChecksum.set(binding.packChecksum, equivalent ?? binding);
  }
  if (bindingKeys.size !== catalogByKey.size) return false;
  for (const node of nodes) {
    const binding = bindingByChecksum.get(node.packChecksum);
    if (
      !binding ||
      binding.idxSha256 !== node.idxSha256 ||
      binding.prefSha256 !== node.prefSha256 ||
      node.end > binding.packBytes - 20
    ) {
      return false;
    }
  }
  if (!equalStrings([...new Set(nodes.map((node) => node.packChecksum))].sort(), selected)) {
    return false;
  }

  const edgeByDependent = new Map<string, StockPhysicalDependencyEdge>();
  for (const edge of dependencies) {
    if (
      !dependencyShapeValid(edge) ||
      edgeByDependent.has(edge.dependentEntryId) ||
      !nodeById.has(edge.dependentEntryId) ||
      !nodeById.has(edge.baseEntryId)
    ) {
      return false;
    }
    edgeByDependent.set(edge.dependentEntryId, edge);
  }
  const canonicalDependencies = [...dependencies].sort((left, right) => {
    const leftDependent = nodeById.get(left.dependentEntryId)!;
    const rightDependent = nodeById.get(right.dependentEntryId)!;
    const leftBase = nodeById.get(left.baseEntryId)!;
    const rightBase = nodeById.get(right.baseEntryId)!;
    return (
      compareStockPhysicalNodes(leftDependent, rightDependent) ||
      compareStockPhysicalNodes(leftBase, rightBase) ||
      left.kind.localeCompare(right.kind)
    );
  });
  if (
    dependencies.some(
      (edge, index) =>
        edge.dependentEntryId !== canonicalDependencies[index]!.dependentEntryId ||
        edge.baseEntryId !== canonicalDependencies[index]!.baseEntryId ||
        edge.kind !== canonicalDependencies[index]!.kind
    )
  ) {
    return false;
  }
  for (const node of nodes) {
    const edge = edgeByDependent.get(node.entryId);
    if (node.encoding === "full") {
      if (edge || node.baseEntryId !== undefined || node.baseOid !== undefined) return false;
      continue;
    }
    if (!edge || node.baseEntryId !== edge.baseEntryId) return false;
    const base = nodeById.get(edge.baseEntryId)!;
    if (node.encoding === "ofs-delta") {
      if (
        edge.kind !== "ofs" ||
        edge.baseOffset !== base.offset ||
        base.offset >= node.offset ||
        base.packChecksum !== node.packChecksum ||
        node.baseOid !== base.oid
      ) {
        return false;
      }
    } else if (edge.kind !== "ref" || edge.baseOid !== base.oid || node.baseOid !== base.oid) {
      return false;
    }
  }

  const topologyIndex = new Map<string, number>();
  for (let index = 0; index < topology.length; index++) {
    const entryId = topology[index]!;
    if (!nodeById.has(entryId) || topologyIndex.has(entryId)) return false;
    topologyIndex.set(entryId, index);
  }
  for (const edge of dependencies) {
    if (topologyIndex.get(edge.baseEntryId)! >= topologyIndex.get(edge.dependentEntryId)!) {
      return false;
    }
  }
  const childrenByBase = new Map<string, string[]>();
  const ready = nodes
    .filter((node) => !edgeByDependent.has(node.entryId))
    .map((node) => node.entryId)
    .sort((left, right) => compareStockPhysicalNodes(nodeById.get(left)!, nodeById.get(right)!));
  for (const edge of dependencies) {
    const children = childrenByBase.get(edge.baseEntryId) ?? [];
    children.push(edge.dependentEntryId);
    childrenByBase.set(edge.baseEntryId, children);
  }
  for (const children of childrenByBase.values()) {
    children.sort((left, right) =>
      compareStockPhysicalNodes(nodeById.get(left)!, nodeById.get(right)!)
    );
  }
  const canonicalTopology: string[] = [];
  while (ready.length > 0) {
    const entryId = ready.shift()!;
    canonicalTopology.push(entryId);
    ready.push(...(childrenByBase.get(entryId) ?? []));
    ready.sort((left, right) =>
      compareStockPhysicalNodes(nodeById.get(left)!, nodeById.get(right)!)
    );
  }
  if (!equalStrings(canonicalTopology, topology)) return false;
  const depthById = new Map<string, number>();
  const depth = (entryId: string, visiting: Set<string>): number => {
    const cached = depthById.get(entryId);
    if (cached !== undefined) return cached;
    if (visiting.has(entryId)) return MAX_DEPENDENCY_DEPTH + 1;
    visiting.add(entryId);
    const edge = edgeByDependent.get(entryId);
    const value = edge ? depth(edge.baseEntryId, visiting) + 1 : 0;
    visiting.delete(entryId);
    depthById.set(entryId, value);
    return value;
  };
  if (nodes.some((node) => depth(node.entryId, new Set()) > MAX_DEPENDENCY_DEPTH)) return false;

  const expectedRootsById = new Map<string, Set<string>>(
    nodes.map((node) => [node.entryId, new Set<string>()])
  );
  for (const root of args.roots) {
    const rootNodes = nodes.filter((node) => node.oid === root);
    if (rootNodes.length !== 1) return false;
    let current: StockPhysicalNode | undefined = rootNodes[0];
    const visited = new Set<string>();
    while (current) {
      if (visited.has(current.entryId)) return false;
      visited.add(current.entryId);
      expectedRootsById.get(current.entryId)!.add(root);
      const edge = edgeByDependent.get(current.entryId);
      current = edge ? nodeById.get(edge.baseEntryId) : undefined;
    }
  }
  for (const node of nodes) {
    if (!equalStrings([...expectedRootsById.get(node.entryId)!].sort(), node.semanticRootOids)) {
      return false;
    }
  }

  if (args.ranges.length !== nodes.length) return false;
  const rangeByEntryId = new Map<string, BoundStockRange>();
  for (const range of args.ranges) {
    if (!rangeValid(range) || rangeByEntryId.has(range.entryId)) return false;
    rangeByEntryId.set(range.entryId, range);
  }
  for (const node of nodes) {
    const range = rangeByEntryId.get(node.entryId);
    if (
      !range ||
      range.packChecksum !== node.packChecksum ||
      range.start !== node.offset ||
      range.end !== node.end ||
      range.requiredOid !== node.oid ||
      !equalStrings(range.semanticRootOids, node.semanticRootOids)
    ) {
      return false;
    }
  }
  const requiredReads = args.reads.filter((read) => read.kind === "required-object");
  const wholeChecksums = new Set(
    args.reads.filter((read) => read.kind === "whole").map((read) => read.packChecksum)
  );
  return (
    args.ranges.every((range, index) => range.entryId === topology[index]) &&
    args.ranges.every((range) =>
      wholeChecksums.has(range.packChecksum)
        ? true
        : requiredReads.some((read) => rangeKey(read) === rangeKey(range))
    ) &&
    requiredReads.every((read) => args.ranges.some((range) => rangeKey(read) === rangeKey(range)))
  );
}

async function directPackPreparedProofFailure(
  operation: NativeReceiveOperation,
  result: NativeReceiveProcessResult
): Promise<string | undefined> {
  const proof = result.closureProof;
  const semantic = result.semanticExternalOids;
  const thin = result.thinDeltaBaseOids;
  const roots = result.requiredRootOids;
  const ranges = result.ranges;
  const reads = result.activePackReads;
  if (
    !operation.stockReceive ||
    result.operationId !== operation.id ||
    result.inputRequestSha256 !== operation.stockReceive.inputRequestSha256 ||
    result.resultKind !== "artifacts" ||
    !result.packSha256 ||
    !result.idxSha256 ||
    !result.refsSha256 ||
    !proof ||
    !semantic ||
    !thin ||
    !roots ||
    !ranges ||
    !reads ||
    !result.planSha256 ||
    result.planSha256 !== result.closureManifestSha256 ||
    proof.planSha256 !== result.planSha256 ||
    !result.closureManifestKey ||
    !result.closureManifestBytes ||
    !SHA256.test(result.closureManifestSha256 ?? "") ||
    !result.closureManifestEtag ||
    result.closureManifestEtag.length > 256 ||
    result.prerequisitePackBytes !== 0 ||
    result.outputBytesWritten !== result.packBytes + result.idxBytes + result.refsBytes ||
    result.outputRequests !== 3 ||
    !sortedUnique(proof.incomingOids, OID, MAX_LOGICAL_OBJECTS) ||
    !sortedUnique(semantic, OID, MAX_LOGICAL_OBJECTS) ||
    !sortedUnique(thin, OID, MAX_PHYSICAL_NODES) ||
    !sortedUnique(roots, OID, MAX_PHYSICAL_NODES) ||
    !equalStrings(roots, thin) ||
    !equalStrings(proof.semanticExternalOids, semantic) ||
    proof.visitedIncomingObjectCount !== result.visitedIncomingObjectCount ||
    proof.incomingOids.length !== result.visitedIncomingObjectCount ||
    result.inputPackObjectCount !== result.incomingObjectCount ||
    result.objectCount !== result.incomingObjectCount ||
    result.incomingObjectCount !== result.visitedIncomingObjectCount ||
    proof.logicalEdgeCount !== result.logicalEdgeCount ||
    proof.internalEdgeCount !== result.internalEdgeCount ||
    proof.externalEdgeCount !== result.externalEdgeCount ||
    proof.internalEdgeCount + proof.externalEdgeCount !== proof.logicalEdgeCount ||
    proof.missingObjectCount !== 0 ||
    result.missingObjectCount !== 0 ||
    JSON.stringify(proof.objectTypeCounts) !== JSON.stringify(result.objectTypeCounts) ||
    Object.values(proof.objectTypeCounts).reduce((total, count) => total + count, 0) !==
      proof.visitedIncomingObjectCount ||
    result.activePackCount !== operation.activeCatalog.length ||
    ranges.length > MAX_PHYSICAL_NODES ||
    reads.length > MAX_ACTIVE_PACK_READS
  ) {
    return "direct-identity-or-counts";
  }
  if (!(await activeMetadataBundleValid(operation, result))) {
    return "direct-metadata-bundle";
  }
  if (!(await physicalPlanValid({ operation, result, roots, ranges, reads }))) {
    return "direct-physical-plan";
  }
  const semanticSet = new Set(semantic);
  if (thin.some((oid) => semanticSet.has(oid))) return "direct-root-overlap";
  const trailerReads = reads.filter((read) => read.kind === "trailer");
  const rangeReads = reads.filter((read) => read.kind === "required-object");
  const wholeReads = reads.filter((read) => read.kind === "whole");
  const bindings = result.activePackBindings!;
  const bindingChecksums = bindings.map((binding) => binding.packChecksum).sort();
  const bindingByChecksum = new Map(bindings.map((binding) => [binding.packChecksum, binding]));
  const wholeChecksums = new Set(wholeReads.map((read) => read.packChecksum));
  const rangeBackedRanges = ranges.filter((range) => !wholeChecksums.has(range.packChecksum));
  if (
    reads.some((read) => !readValid(read)) ||
    trailerReads.length !== operation.activeCatalog.length ||
    !equalStrings(trailerReads.map((read) => read.packChecksum).sort(), bindingChecksums) ||
    wholeReads.some((read) => {
      const binding = bindingByChecksum.get(read.packChecksum);
      return !binding || read.start !== 0 || read.end !== binding.packBytes;
    }) ||
    rangeReads.length !== rangeBackedRanges.length ||
    rangeReads.some(
      (read) => !rangeBackedRanges.some((range) => rangeKey(read) === rangeKey(range))
    ) ||
    result.activePackTrailerRequests !== trailerReads.length ||
    result.activePackRangeRequests !== rangeReads.length ||
    result.activePackRangeBytes !==
      rangeReads.reduce((total, read) => total + read.returnedBytes, 0) ||
    result.activePackWholeRequests !== wholeReads.length ||
    result.activePackWholeBytes !==
      wholeReads.reduce((total, read) => total + read.returnedBytes, 0) ||
    result.activePackUnattributedBytes !== 0 ||
    result.activePackUnattributedRequests !== 0
  ) {
    return "direct-read-accounting";
  }
  const expectedTrace = ["worker_direct_closure_validated", "worker_direct_artifacts_published"];
  return result.stockTrace?.length === expectedTrace.length &&
    result.stockTrace.every(
      (event, index) => event.sequence === index + 1 && event.event === expectedTrace[index]
    )
    ? undefined
    : "direct-trace";
}

/** Pure, bounded proof diagnosis. It performs no I/O or runtime callbacks. */
export async function stockReceivePreparedProofFailure(
  operation: NativeReceiveOperation,
  result: NativeReceiveProcessResult
): Promise<string | undefined> {
  if (result.executionMode === "direct-pack") {
    return await directPackPreparedProofFailure(operation, result);
  }
  const proof = result.closureProof;
  const semantic = result.semanticExternalOids;
  const thin = result.thinDeltaBaseOids;
  const roots = result.requiredRootOids;
  const prerequisiteOids = result.prerequisiteObjectOids;
  const ranges = result.ranges;
  const reads = result.activePackReads;
  const resultKind = result.resultKind ?? "artifacts";
  const outputCountsValid =
    resultKind === "ref-only"
      ? result.objectCount === 0 &&
        result.visitedIncomingObjectCount === 0 &&
        proof?.incomingOids.length === 0
      : result.objectCount === result.visitedIncomingObjectCount! + thin!.length;
  if (
    !operation.stockReceive ||
    result.operationId !== operation.id ||
    result.inputRequestSha256 !== operation.stockReceive.inputRequestSha256 ||
    !proof ||
    !semantic ||
    !thin ||
    !roots ||
    !prerequisiteOids ||
    !ranges ||
    !reads ||
    !result.planSha256 ||
    result.planSha256 !== result.closureManifestSha256 ||
    proof.planSha256 !== result.planSha256 ||
    !result.closureManifestKey ||
    !result.closureManifestBytes ||
    !SHA256.test(result.closureManifestSha256 ?? "") ||
    !result.closureManifestEtag ||
    result.closureManifestEtag.length > 256 ||
    !result.prerequisitePackKey ||
    !result.prerequisitePackBytes ||
    !SHA256.test(result.prerequisitePackSha256 ?? "") ||
    !result.prerequisitePackEtag ||
    result.prerequisitePackEtag.length > 256 ||
    result.quarantinePathInsideOwnedWorkRoot !== true ||
    result.quarantineRemovedAfterReceive !== true ||
    result.quarantinePathNonEmpty !== true ||
    result.freshWorkDirectory !== true ||
    result.repositoryPackBytesBeforeHydration !== 0 ||
    result.sharedObjectCacheDisabled !== true ||
    result.skipConnectivityCheck !== false ||
    !sortedUnique(proof.incomingOids, OID, MAX_LOGICAL_OBJECTS) ||
    !sortedUnique(semantic, OID, MAX_LOGICAL_OBJECTS) ||
    !sortedUnique(thin, OID, MAX_PHYSICAL_NODES) ||
    !sortedUnique(roots, OID, MAX_LOGICAL_OBJECTS) ||
    !sortedUnique(prerequisiteOids, OID, MAX_LOGICAL_OBJECTS) ||
    !equalStrings(prerequisiteOids, roots) ||
    !equalStrings(proof.semanticExternalOids, semantic) ||
    proof.visitedIncomingObjectCount !== result.visitedIncomingObjectCount ||
    proof.visitedIncomingObjectCount! > result.incomingObjectCount! ||
    proof.incomingOids.length !== result.visitedIncomingObjectCount ||
    result.inputPackObjectCount !== result.incomingObjectCount ||
    !outputCountsValid ||
    proof.logicalEdgeCount !== result.logicalEdgeCount ||
    proof.internalEdgeCount !== result.internalEdgeCount ||
    proof.externalEdgeCount !== result.externalEdgeCount ||
    proof.logicalEdgeCount > MAX_LOGICAL_EDGES ||
    proof.internalEdgeCount + proof.externalEdgeCount !== proof.logicalEdgeCount ||
    proof.missingObjectCount !== 0 ||
    result.missingObjectCount !== 0 ||
    JSON.stringify(proof.objectTypeCounts) !== JSON.stringify(result.objectTypeCounts) ||
    Object.values(proof.objectTypeCounts).reduce((total, count) => total + count, 0) !==
      proof.visitedIncomingObjectCount ||
    result.activePackCount !== operation.activeCatalog.length ||
    ranges.length > MAX_PHYSICAL_NODES ||
    reads.length > MAX_ACTIVE_PACK_READS
  ) {
    return "identity-or-counts";
  }
  if (!(await activeMetadataBundleValid(operation, result))) {
    return "metadata-bundle";
  }
  const semanticSet = new Set(semantic);
  if (thin.some((oid) => semanticSet.has(oid))) return "root-overlap";
  const commandOldOids = operation.commands
    .map((command) => command.oldOid.toLowerCase())
    .filter((oid) => oid !== "0".repeat(40));
  const rootUnion = [...new Set([...semantic, ...thin, ...commandOldOids])].sort();
  if (!equalStrings(rootUnion, roots)) return "root-union";
  if (!(await physicalPlanValid({ operation, result, roots, ranges, reads }))) {
    return "physical-plan";
  }

  const trailerReads = reads.filter((read) => read.kind === "trailer");
  const rangeReads = reads.filter((read) => read.kind === "required-object");
  const wholeReads = reads.filter((read) => read.kind === "whole");
  const bindingChecksums = result.activePackBindings!.map((binding) => binding.packChecksum).sort();
  const bindingByChecksum = new Map(
    result.activePackBindings!.map((binding) => [binding.packChecksum, binding])
  );
  const wholeChecksums = wholeReads.map((read) => read.packChecksum);
  const rangeBackedRanges = ranges.filter((range) => !wholeChecksums.includes(range.packChecksum));
  if (
    reads.some((read) => !readValid(read)) ||
    trailerReads.length !== operation.activeCatalog.length ||
    !equalStrings(trailerReads.map((read) => read.packChecksum).sort(), bindingChecksums) ||
    new Set(wholeChecksums).size !== wholeChecksums.length ||
    wholeReads.some((read) => {
      const binding = bindingByChecksum.get(read.packChecksum);
      return (
        !binding ||
        !result.selectedPackChecksums!.includes(read.packChecksum) ||
        read.start !== 0 ||
        read.end !== binding.packBytes
      );
    }) ||
    rangeReads.length !== rangeBackedRanges.length ||
    rangeReads.some(
      (read) => !rangeBackedRanges.some((range) => rangeKey(read) === rangeKey(range))
    ) ||
    rangeBackedRanges.reduce((total, range) => total + range.end - range.start, 0) !==
      result.rangeBytes ||
    result.rangeRequests !== rangeBackedRanges.length ||
    result.packsTouched !== result.selectedPackChecksums!.length ||
    result.activePackTrailerRequests !== trailerReads.length ||
    result.activePackTrailerBytes !== trailerReads.length * 20 ||
    result.activePackRangeRequests !== rangeReads.length ||
    result.activePackRangeRequests !== result.rangeRequests ||
    result.activePackRangeBytes !== result.rangeBytes ||
    result.activePackWholeBytes !==
      wholeReads.reduce((total, read) => total + read.returnedBytes, 0) ||
    result.activePackWholeRequests !== wholeReads.length ||
    result.activePackUnattributedBytes !== 0 ||
    result.activePackUnattributedRequests !== 0
  ) {
    return "read-accounting";
  }
  const expectedTrace = [
    "receive_pack_invoked",
    "pre_receive_started",
    "pre_receive_quarantine_nonempty",
    "logical_closure_started_ref_still_old",
    "incoming_oid_visible_in_quarantine",
    "logical_closure_completed",
    "pre_receive_succeeded",
    "disposable_ref_update_observed",
  ];
  return result.stockTrace?.length === expectedTrace.length &&
    result.stockTrace.every(
      (event, index) => event.sequence === index + 1 && event.event === expectedTrace[index]
    )
    ? undefined
    : "trace";
}

/** Pure, bounded proof validation. It performs no I/O or runtime callbacks. */
export async function validateStockReceivePreparedProof(
  operation: NativeReceiveOperation,
  result: NativeReceiveProcessResult
): Promise<boolean> {
  return (await stockReceivePreparedProofFailure(operation, result)) === undefined;
}
