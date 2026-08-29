import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";

import { asBufferSource, bytesToHex, hexToBytes } from "@/worker/common";
import { computeOid } from "@/worker/git/core/objects";
import {
  planStockPhysicalDependencies,
  type StockBoundPackSource,
  type StockPhysicalDependencyPlan,
} from "@/worker/git/nativeReceive/physicalDependencyPlan";
import type { PackedObjectCandidate } from "@/worker/git/object-store/candidates";
import { buildPackV2 } from "@/worker/git/pack/build";
import { packIndexKey, packRefsKey } from "@/worker/keys";

import { buildAppendOnlyDelta, buildPack } from "./util/git-pack";
import { uniqueRepoId } from "./util/test-helpers";
import { indexTestPack } from "./util/test-indexer";

const MAX_TEST_OBJECT_BYTES = 8 * 1024 * 1024;

type FullBlobEntry = {
  type: "blob";
  payload: Uint8Array;
};

type OfsBlobEntry = {
  type: "ofs-delta";
  baseIndex: number;
  delta: Uint8Array;
};

type FixtureEntry = FullBlobEntry | OfsBlobEntry;

type FrozenFixture = {
  name: string;
  boundSource: StockBoundPackSource;
  packBytes: Uint8Array;
  semanticRootOids: string[];
  expectedPhysicalNodes: number;
  expectedDependencyEdges: number;
  expectedMaximumDepth: number;
};

type PlanEvidence = {
  planDigest: string;
  prerequisiteDigest: string;
  physicalNodeCount: number;
  dependencyEdgeCount: number;
  rangeCount: number;
  semanticRootCount: number;
};

const encoder = new TextEncoder();

async function sha256(bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

async function blob(payload: string): Promise<{ oid: string; payload: Uint8Array }> {
  const bytes = encoder.encode(payload);
  return { oid: await computeOid("blob", bytes), payload: bytes };
}

async function createFixture(args: {
  name: string;
  entries: FixtureEntry[];
  semanticRootOids: string[];
  expectedPhysicalNodes: number;
  expectedDependencyEdges: number;
  expectedMaximumDepth: number;
}): Promise<FrozenFixture> {
  const packBytes = await buildPack(args.entries);
  const packKey = `test/stock-physical/${uniqueRepoId()}/${args.name}.pack`;
  await env.REPO_BUCKET.put(packKey, packBytes);
  const indexed = await indexTestPack(env, packKey, packBytes.byteLength);
  const [idxObject, prefObject] = await Promise.all([
    env.REPO_BUCKET.get(packIndexKey(packKey)),
    env.REPO_BUCKET.get(packRefsKey(packKey)),
  ]);
  if (!idxObject || !prefObject) throw new Error("fixture index authority missing");
  return {
    name: args.name,
    boundSource: {
      source: {
        packKey,
        packBytes: packBytes.byteLength,
        idx: indexed.idxView,
      },
      packChecksum: bytesToHex(packBytes.subarray(-20)),
      idxSha256: await sha256(new Uint8Array(await idxObject.arrayBuffer())),
      prefSha256: await sha256(new Uint8Array(await prefObject.arrayBuffer())),
    },
    packBytes,
    semanticRootOids: args.semanticRootOids,
    expectedPhysicalNodes: args.expectedPhysicalNodes,
    expectedDependencyEdges: args.expectedDependencyEdges,
    expectedMaximumDepth: args.expectedMaximumDepth,
  };
}

async function reverseOrderFixture(): Promise<FrozenFixture> {
  const base = await blob("reverse-order-base\n");
  let child = await blob(`${new TextDecoder().decode(base.payload)}child-0\n`);
  let suffix = encoder.encode("child-0\n");
  for (let index = 1; child.oid >= base.oid; index++) {
    suffix = encoder.encode(`child-${index}\n`);
    const payload = new Uint8Array(base.payload.byteLength + suffix.byteLength);
    payload.set(base.payload);
    payload.set(suffix, base.payload.byteLength);
    child = { oid: await computeOid("blob", payload), payload };
  }
  return await createFixture({
    name: "reverse-order",
    entries: [
      { type: "blob", payload: base.payload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(base.payload, suffix) },
    ],
    semanticRootOids: [base.oid, child.oid],
    expectedPhysicalNodes: 2,
    expectedDependencyEdges: 1,
    expectedMaximumDepth: 1,
  });
}

async function baseNotRootFixture(): Promise<FrozenFixture> {
  const base = await blob("encoding-only-base\n");
  const suffix = encoder.encode("semantic-child\n");
  const childPayload = new Uint8Array(base.payload.byteLength + suffix.byteLength);
  childPayload.set(base.payload);
  childPayload.set(suffix, base.payload.byteLength);
  const childOid = await computeOid("blob", childPayload);
  return await createFixture({
    name: "base-not-root",
    entries: [
      { type: "blob", payload: base.payload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(base.payload, suffix) },
    ],
    semanticRootOids: [childOid],
    expectedPhysicalNodes: 2,
    expectedDependencyEdges: 1,
    expectedMaximumDepth: 1,
  });
}

async function multiLevelFixture(): Promise<FrozenFixture> {
  const payloads: Uint8Array[] = [encoder.encode("chain-a\n")];
  const entries: FixtureEntry[] = [{ type: "blob", payload: payloads[0]! }];
  for (const suffixText of ["chain-b\n", "chain-c\n", "chain-d\n"]) {
    const base = payloads[payloads.length - 1]!;
    const suffix = encoder.encode(suffixText);
    const payload = new Uint8Array(base.byteLength + suffix.byteLength);
    payload.set(base);
    payload.set(suffix, base.byteLength);
    entries.push({
      type: "ofs-delta",
      baseIndex: entries.length - 1,
      delta: buildAppendOnlyDelta(base, suffix),
    });
    payloads.push(payload);
  }
  return await createFixture({
    name: "multi-level",
    entries,
    semanticRootOids: [await computeOid("blob", payloads[3]!)],
    expectedPhysicalNodes: 4,
    expectedDependencyEdges: 3,
    expectedMaximumDepth: 3,
  });
}

async function sharedBaseFixture(): Promise<FrozenFixture> {
  const base = await blob("shared-encoding-base\n");
  const entries: FixtureEntry[] = [{ type: "blob", payload: base.payload }];
  const roots: string[] = [];
  for (const suffixText of ["child-one\n", "child-two\n", "child-three\n"]) {
    const suffix = encoder.encode(suffixText);
    entries.push({
      type: "ofs-delta",
      baseIndex: 0,
      delta: buildAppendOnlyDelta(base.payload, suffix),
    });
    const payload = new Uint8Array(base.payload.byteLength + suffix.byteLength);
    payload.set(base.payload);
    payload.set(suffix, base.payload.byteLength);
    roots.push(await computeOid("blob", payload));
  }
  return await createFixture({
    name: "shared-base",
    entries,
    semanticRootOids: roots,
    expectedPhysicalNodes: 4,
    expectedDependencyEdges: 3,
    expectedMaximumDepth: 1,
  });
}

async function mergeRegressionFixture(): Promise<FrozenFixture> {
  const base = await blob("merge-root-base\n");
  const suffix = encoder.encode("merge-child\n");
  const childPayload = new Uint8Array(base.payload.byteLength + suffix.byteLength);
  childPayload.set(base.payload);
  childPayload.set(suffix, base.payload.byteLength);
  const childOid = await computeOid("blob", childPayload);
  const extraOne = await blob("merge-root-extra-one\n");
  const extraTwo = await blob("merge-root-extra-two\n");
  return await createFixture({
    name: "merge-regression",
    entries: [
      { type: "blob", payload: base.payload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(base.payload, suffix) },
      { type: "blob", payload: extraOne.payload },
      { type: "blob", payload: extraTwo.payload },
    ],
    semanticRootOids: [base.oid, childOid, extraOne.oid, extraTwo.oid],
    expectedPhysicalNodes: 4,
    expectedDependencyEdges: 1,
    expectedMaximumDepth: 1,
  });
}

async function boundedParallelFixture(): Promise<FrozenFixture> {
  const objects = await Promise.all(
    Array.from({ length: 6 }, (_value, index) => blob(`bounded-parallel-${index}\n`))
  );
  return await createFixture({
    name: "bounded-parallel",
    entries: objects.map((object) => ({ type: "blob", payload: object.payload })),
    semanticRootOids: objects.map((object) => object.oid),
    expectedPhysicalNodes: objects.length,
    expectedDependencyEdges: 0,
    expectedMaximumDepth: 0,
  });
}

function permutations<T>(values: readonly T[]): T[][] {
  if (values.length <= 1) return [[...values]];
  const result: T[][] = [];
  for (let index = 0; index < values.length; index++) {
    const head = values[index]!;
    const tail = values.filter((_value, candidateIndex) => candidateIndex !== index);
    for (const permutation of permutations(tail)) result.push([head, ...permutation]);
  }
  return result;
}

function planMaximumDepth(plan: StockPhysicalDependencyPlan): number {
  const baseByChild = new Map(
    plan.dependencies.map((edge) => [edge.dependentEntryId, edge.baseEntryId])
  );
  let maximum = 0;
  for (const node of plan.physicalNodes) {
    let depth = 0;
    let current = node.entryId;
    while (baseByChild.has(current)) {
      depth++;
      current = baseByChild.get(current)!;
    }
    maximum = Math.max(maximum, depth);
  }
  return maximum;
}

async function executePermutation(
  fixture: FrozenFixture,
  semanticRootOids: string[],
  mutateEntry?: (candidate: PackedObjectCandidate, bytes: Uint8Array) => Uint8Array
): Promise<PlanEvidence> {
  const reads = new Map<string, number>();
  const plan = await planStockPhysicalDependencies({
    sources: [fixture.boundSource],
    semanticRootOids,
    maxEntryBytes: MAX_TEST_OBJECT_BYTES,
    maxInflatedBytes: MAX_TEST_OBJECT_BYTES,
    maxDeltaResultBytes: MAX_TEST_OBJECT_BYTES,
    readEntry: async (candidate) => {
      const id = `${fixture.boundSource.packChecksum}:${candidate.offset}:${candidate.nextOffset}`;
      reads.set(id, (reads.get(id) ?? 0) + 1);
      const object = await env.REPO_BUCKET.get(candidate.source.packKey, {
        range: {
          offset: candidate.offset,
          length: candidate.nextOffset - candidate.offset,
        },
      });
      if (!object) return undefined;
      const bytes = new Uint8Array(await object.arrayBuffer());
      return mutateEntry ? mutateEntry(candidate, bytes) : bytes;
    },
  });

  expect(plan.semanticRootOids).toEqual([...fixture.semanticRootOids].sort());
  expect([...plan.semanticObjects.keys()].sort()).toEqual([...fixture.semanticRootOids].sort());
  expect(plan.physicalNodes).toHaveLength(fixture.expectedPhysicalNodes);
  expect(plan.dependencies).toHaveLength(fixture.expectedDependencyEdges);
  expect(plan.ranges).toHaveLength(fixture.expectedPhysicalNodes);
  expect(planMaximumDepth(plan)).toBe(fixture.expectedMaximumDepth);
  expect([...reads.values()]).toEqual(
    Array.from({ length: fixture.expectedPhysicalNodes }, () => 1)
  );

  const topologicalIndex = new Map(
    plan.topologicalEntryIds.map((entryId, index) => [entryId, index])
  );
  for (const edge of plan.dependencies) {
    expect(topologicalIndex.get(edge.baseEntryId)).toBeLessThan(
      topologicalIndex.get(edge.dependentEntryId)!
    );
  }

  const prerequisitePack = await buildPackV2(
    plan.semanticRootOids.map((oid) => {
      const object = plan.semanticObjects.get(oid);
      if (!object) throw new Error("missing semantic object in test plan");
      return { type: object.type, payload: object.payload };
    })
  );
  const planDocument = new TextEncoder().encode(
    JSON.stringify({
      semanticRootOids: plan.semanticRootOids,
      physicalNodes: plan.physicalNodes,
      dependencies: plan.dependencies,
      topologicalEntryIds: plan.topologicalEntryIds,
      ranges: plan.ranges,
    })
  );
  return {
    planDigest: await sha256(planDocument),
    prerequisiteDigest: await sha256(prerequisitePack),
    physicalNodeCount: plan.physicalNodes.length,
    dependencyEdgeCount: plan.dependencies.length,
    rangeCount: plan.ranges.length,
    semanticRootCount: plan.semanticRootOids.length,
  };
}

async function cleanupFixture(fixture: FrozenFixture): Promise<void> {
  await env.REPO_BUCKET.delete([
    fixture.boundSource.source.packKey,
    packIndexKey(fixture.boundSource.source.packKey),
    packRefsKey(fixture.boundSource.source.packKey),
  ]);
}

describe("stock physical dependency plan", () => {
  it("rejects a cross-root REF_DELTA cycle without deadlocking concurrent roots", async () => {
    const leftOid = "1".repeat(40);
    const rightOid = "2".repeat(40);
    const delta = buildAppendOnlyDelta(encoder.encode("cycle-base\n"), encoder.encode("x"));
    const packs = new Map<string, Uint8Array>();
    const source = async (name: string, oid: string, baseOid: string) => {
      const packBytes = await buildPack([{ type: "ref-delta", baseOid, delta }]);
      const packKey = `test/stock-physical/${uniqueRepoId()}/${name}.pack`;
      packs.set(packKey, packBytes);
      const rawOid = hexToBytes(oid);
      const fanout = new Uint32Array(256);
      for (let index = rawOid[0]!; index < fanout.length; index++) fanout[index] = 1;
      return {
        source: {
          packKey,
          packBytes: packBytes.byteLength,
          idx: {
            packKey,
            count: 1,
            fanout,
            rawNames: rawOid,
            offsets: Float64Array.of(12),
            nextOffsetByIndex: Float64Array.of(packBytes.byteLength - 20),
            sortedOffsets: Float64Array.of(12),
            sortedOffsetIndices: Uint32Array.of(0),
            packSize: packBytes.byteLength,
            packChecksum: packBytes.subarray(-20),
            idxChecksum: new Uint8Array(20),
          },
        },
        packChecksum: bytesToHex(packBytes.subarray(-20)),
        idxSha256: "a".repeat(64),
        prefSha256: "b".repeat(64),
      } satisfies StockBoundPackSource;
    };
    const sources = [
      await source("cycle-left", leftOid, rightOid),
      await source("cycle-right", rightOid, leftOid),
    ];
    let readsStarted = 0;
    let releaseReads!: () => void;
    const bothReadsStarted = new Promise<void>((resolve) => {
      releaseReads = resolve;
    });

    const planning = planStockPhysicalDependencies({
      sources,
      semanticRootOids: [leftOid, rightOid],
      maxEntryBytes: MAX_TEST_OBJECT_BYTES,
      maxInflatedBytes: MAX_TEST_OBJECT_BYTES,
      maxDeltaResultBytes: MAX_TEST_OBJECT_BYTES,
      readEntry: async (candidate) => {
        readsStarted++;
        if (readsStarted === 2) releaseReads();
        await bothReadsStarted;
        const pack = packs.get(candidate.source.packKey)!;
        return pack.subarray(candidate.offset, candidate.nextOffset);
      },
    });

    await expect(
      Promise.race([
        planning,
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("planner-deadlocked")), 1_000)
        ),
      ])
    ).rejects.toThrow("stock-physical-plan:dependency-cycle");
  });

  it("settles started reads and stops dequeuing roots after the first failure", async () => {
    const fixture = await boundedParallelFixture();
    let readsStarted = 0;
    let planningSettled = false;
    let releaseFailure!: () => void;
    let releaseSiblings!: () => void;
    let signalFourStarted!: () => void;
    const failureGate = new Promise<void>((resolve) => {
      releaseFailure = resolve;
    });
    const siblingGate = new Promise<void>((resolve) => {
      releaseSiblings = resolve;
    });
    const fourStarted = new Promise<void>((resolve) => {
      signalFourStarted = resolve;
    });
    try {
      const planning = planStockPhysicalDependencies({
        sources: [fixture.boundSource],
        semanticRootOids: fixture.semanticRootOids,
        maxEntryBytes: MAX_TEST_OBJECT_BYTES,
        maxInflatedBytes: MAX_TEST_OBJECT_BYTES,
        maxDeltaResultBytes: MAX_TEST_OBJECT_BYTES,
        readEntry: async (candidate) => {
          const ordinal = readsStarted++;
          if (readsStarted === 4) signalFourStarted();
          if (ordinal === 0) {
            await failureGate;
            throw new Error("first-range-failed");
          }
          await siblingGate;
          const object = await env.REPO_BUCKET.get(candidate.source.packKey, {
            range: {
              offset: candidate.offset,
              length: candidate.nextOffset - candidate.offset,
            },
          });
          return object ? new Uint8Array(await object.arrayBuffer()) : undefined;
        },
      }).finally(() => {
        planningSettled = true;
      });

      await fourStarted;
      releaseFailure();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(readsStarted).toBe(4);
      expect(planningSettled).toBe(false);
      releaseSiblings();
      await expect(planning).rejects.toThrow("first-range-failed");
      expect(readsStarted).toBe(4);
    } finally {
      releaseFailure();
      releaseSiblings();
      await cleanupFixture(fixture);
    }
  });

  it("hydrates independent roots concurrently while reading a shared base once", async () => {
    const fixture = await sharedBaseFixture();
    const reads = new Map<string, number>();
    let activeReads = 0;
    let maximumConcurrency = 0;
    try {
      const plan = await planStockPhysicalDependencies({
        sources: [fixture.boundSource],
        semanticRootOids: fixture.semanticRootOids,
        maxEntryBytes: MAX_TEST_OBJECT_BYTES,
        maxInflatedBytes: MAX_TEST_OBJECT_BYTES,
        maxDeltaResultBytes: MAX_TEST_OBJECT_BYTES,
        readEntry: async (candidate) => {
          const id = `${fixture.boundSource.packChecksum}:${candidate.offset}:${candidate.nextOffset}`;
          reads.set(id, (reads.get(id) ?? 0) + 1);
          activeReads++;
          maximumConcurrency = Math.max(maximumConcurrency, activeReads);
          try {
            await new Promise((resolve) => setTimeout(resolve, 10));
            const object = await env.REPO_BUCKET.get(candidate.source.packKey, {
              range: {
                offset: candidate.offset,
                length: candidate.nextOffset - candidate.offset,
              },
            });
            return object ? new Uint8Array(await object.arrayBuffer()) : undefined;
          } finally {
            activeReads--;
          }
        },
      });

      expect(maximumConcurrency).toBeGreaterThan(1);
      expect(maximumConcurrency).toBeLessThanOrEqual(4);
      expect([...reads.values()]).toEqual(Array.from({ length: 4 }, () => 1));
      expect(plan.physicalNodes).toHaveLength(4);
      expect(plan.dependencies).toHaveLength(3);
    } finally {
      await cleanupFixture(fixture);
    }
  });

  it("limits independent range hydration to four concurrent reads", async () => {
    const fixture = await boundedParallelFixture();
    let activeReads = 0;
    let maximumConcurrency = 0;
    try {
      await planStockPhysicalDependencies({
        sources: [fixture.boundSource],
        semanticRootOids: fixture.semanticRootOids,
        maxEntryBytes: MAX_TEST_OBJECT_BYTES,
        maxInflatedBytes: MAX_TEST_OBJECT_BYTES,
        maxDeltaResultBytes: MAX_TEST_OBJECT_BYTES,
        readEntry: async (candidate) => {
          activeReads++;
          maximumConcurrency = Math.max(maximumConcurrency, activeReads);
          try {
            await new Promise((resolve) => setTimeout(resolve, 10));
            const object = await env.REPO_BUCKET.get(candidate.source.packKey, {
              range: {
                offset: candidate.offset,
                length: candidate.nextOffset - candidate.offset,
              },
            });
            return object ? new Uint8Array(await object.arrayBuffer()) : undefined;
          } finally {
            activeReads--;
          }
        },
      });

      expect(maximumConcurrency).toBe(4);
    } finally {
      await cleanupFixture(fixture);
    }
  });

  it("accepts byte-identical packs stored under different keys", async () => {
    const fixture = await baseNotRootFixture();
    const duplicatePackKey = `${fixture.boundSource.source.packKey}.duplicate`;
    await env.REPO_BUCKET.put(duplicatePackKey, fixture.packBytes);
    const duplicateSource: StockBoundPackSource = {
      ...fixture.boundSource,
      source: {
        ...fixture.boundSource.source,
        packKey: duplicatePackKey,
      },
    };
    const readPackKeys: string[] = [];
    try {
      const plan = await planStockPhysicalDependencies({
        sources: [duplicateSource, fixture.boundSource],
        semanticRootOids: fixture.semanticRootOids,
        maxEntryBytes: MAX_TEST_OBJECT_BYTES,
        maxInflatedBytes: MAX_TEST_OBJECT_BYTES,
        maxDeltaResultBytes: MAX_TEST_OBJECT_BYTES,
        readEntry: async (candidate) => {
          readPackKeys.push(candidate.source.packKey);
          const object = await env.REPO_BUCKET.get(candidate.source.packKey, {
            range: {
              offset: candidate.offset,
              length: candidate.nextOffset - candidate.offset,
            },
          });
          return object ? new Uint8Array(await object.arrayBuffer()) : undefined;
        },
      });

      expect(plan.physicalNodes).toHaveLength(fixture.expectedPhysicalNodes);
      expect(new Set(readPackKeys)).toEqual(
        new Set([[fixture.boundSource.source.packKey, duplicatePackKey].sort()[0]!])
      );
    } finally {
      await env.REPO_BUCKET.delete(duplicatePackKey);
      await cleanupFixture(fixture);
    }
  });

  it("rejects duplicate physical ranges with different integrity bindings", async () => {
    const fixture = await baseNotRootFixture();
    const duplicatePackKey = `${fixture.boundSource.source.packKey}.mismatched`;
    const duplicateSource: StockBoundPackSource = {
      ...fixture.boundSource,
      idxSha256: "f".repeat(64),
      source: {
        ...fixture.boundSource.source,
        packKey: duplicatePackKey,
      },
    };
    try {
      await expect(
        planStockPhysicalDependencies({
          sources: [fixture.boundSource, duplicateSource],
          semanticRootOids: fixture.semanticRootOids,
          maxEntryBytes: MAX_TEST_OBJECT_BYTES,
          maxInflatedBytes: MAX_TEST_OBJECT_BYTES,
          maxDeltaResultBytes: MAX_TEST_OBJECT_BYTES,
          readEntry: async () => undefined,
        })
      ).rejects.toThrow(/duplicate-ambiguous/);
    } finally {
      await cleanupFixture(fixture);
    }
  });

  it("is root-order independent across the exact five-fixture, 34-permutation gate", async () => {
    const fixtures = [
      { fixture: await reverseOrderFixture(), orders: 2 },
      { fixture: await baseNotRootFixture(), orders: 1 },
      { fixture: await multiLevelFixture(), orders: 1 },
      { fixture: await sharedBaseFixture(), orders: 6 },
      { fixture: await mergeRegressionFixture(), orders: 24 },
    ];
    expect(fixtures.reduce((total, item) => total + item.orders, 0)).toBe(34);

    let executed = 0;
    try {
      for (const { fixture, orders } of fixtures) {
        const rootOrders = permutations(fixture.semanticRootOids);
        expect(rootOrders).toHaveLength(orders);
        let frozenEvidence: PlanEvidence | undefined;
        for (const rootOrder of rootOrders) {
          const evidence = await executePermutation(fixture, rootOrder);
          if (!frozenEvidence) frozenEvidence = evidence;
          expect(evidence).toEqual(frozenEvidence);
          executed++;
        }
      }
      expect(executed).toBe(34);
    } finally {
      await Promise.all(fixtures.map(({ fixture }) => cleanupFixture(fixture)));
    }
  });

  it("rejects a shifted encoding-base range before producing a plan", async () => {
    const fixture = await baseNotRootFixture();
    try {
      await expect(
        executePermutation(fixture, fixture.semanticRootOids, (candidate, bytes) => {
          if (candidate.offset === 12) return bytes.subarray(1);
          return bytes;
        })
      ).rejects.toThrow(/entry-read-mismatch/);
    } finally {
      await cleanupFixture(fixture);
    }
  });

  it("rejects touched physical bytes by canonical object identity", async () => {
    const fixture = await baseNotRootFixture();
    try {
      await expect(
        executePermutation(fixture, fixture.semanticRootOids, (candidate, bytes) => {
          if (candidate.offset !== 12) return bytes;
          const changed = new Uint8Array(bytes);
          changed[changed.byteLength - 1] ^= 1;
          return changed;
        })
      ).rejects.toThrow();
    } finally {
      await cleanupFixture(fixture);
    }
  });
});
