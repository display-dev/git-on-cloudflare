import { it, expect } from "vitest";
import { encodeOfsDeltaDistance } from "@/worker/git";

/**
 * This is a logic-level test that simulates the header-length/offset
 * recomputation loop used by the single-pack assembler for OFS_DELTA entries.
 *
 * It verifies that when a delta's relative distance crosses a varint length
 * boundary due to offsets shifting, the iterative process converges to a
 * stable set of header lengths and offsets within the iteration cap.
 */
it("ofs-delta header varint length converges when distances cross boundary", () => {
  // Model three entries in order by original offsets: [base, delta]
  // Entry 0: base (non-delta)
  // Entry 1: OFS_DELTA referencing entry 0
  // Original offsets chosen so the initial guess uses a 1-byte ofs varint,
  // but the recomputed new offsets increase the distance such that the ofs varint
  // needs 2 bytes, forcing at least one iteration of recalculation.

  type Entry = {
    type: number; // 1..7 (we use 1 for normal, 6 for OFS_DELTA)
    baseIndex?: number;
    payloadLen: number;
    sizeVarBytesLen: number;
  };

  const entries: Entry[] = [
    { type: 1, payloadLen: 200, sizeVarBytesLen: 1 }, // base
    { type: 6, baseIndex: 0, payloadLen: 10, sizeVarBytesLen: 1 }, // ofs-delta
  ];

  // Original offsets in the source pack (arbitrary but consistent)
  // base at 100, delta at 140 -> original rel = 40 (1-byte ofs varint)
  const origOffsets = [100, 140];

  // Initial header-length guesses: for OFS delta, based on original rel distance
  const newHeaderLen = new Map<number, number>();
  for (let i = 0; i < entries.length; i++) {
    const e = entries[i];
    if (e.type === 6 && e.baseIndex !== undefined) {
      const guessRel = origOffsets[i] - origOffsets[e.baseIndex];
      newHeaderLen.set(i, e.sizeVarBytesLen + encodeOfsDeltaDistance(guessRel).length);
    } else if (e.type === 7) {
      newHeaderLen.set(i, e.sizeVarBytesLen + 20);
    } else {
      newHeaderLen.set(i, e.sizeVarBytesLen);
    }
  }

  // Iteratively recompute offsets and header lengths until convergence (cap 16)
  let iter = 0;
  let changed: boolean;
  let newOffsets: Map<number, number> = new Map();
  do {
    // Compute new offsets with current header-lengths
    let cur = 12; // PACK header size
    newOffsets = new Map<number, number>();
    for (let i = 0; i < entries.length; i++) {
      newOffsets.set(i, cur);
      cur += (newHeaderLen.get(i) || 0) + entries[i].payloadLen;
    }

    // Re-evaluate OFS varints with accurate distances
    changed = false;
    for (let i = 0; i < entries.length; i++) {
      const e = entries[i];
      if (e.type !== 6 || e.baseIndex === undefined) continue;
      const rel = (newOffsets.get(i) || 0) - (newOffsets.get(e.baseIndex) || 0);
      const desired = e.sizeVarBytesLen + encodeOfsDeltaDistance(rel).length;
      if (desired !== newHeaderLen.get(i)) {
        newHeaderLen.set(i, desired);
        changed = true;
      }
    }
  } while (changed && ++iter < 16);

  // Expectations:
  // - We must have iterated at least once (varint length changed due to larger distance)
  // - The delta's ofs varint length should now be 2 bytes (sizeVarBytesLen 1 + ofs 2 => header 3)
  // - Subsequent recomputation would not change it further (converged)
  expect(iter).toBeGreaterThanOrEqual(1);
  const finalHeaderLenDelta = newHeaderLen.get(1)!;
  expect(finalHeaderLenDelta).toBe(entries[1].sizeVarBytesLen + 2);
});
