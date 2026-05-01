import { assert, test } from "vitest";

import { createLogger } from "@/worker/common";
import { buildOutputOrder, compactDeadSlots } from "@/worker/git/pack/rewrite/plan";
import { allocateSelectionTable } from "@/worker/git/pack/rewrite/shared";

test("compactDeadSlots preserves base slots for live rows that move", () => {
  const table = allocateSelectionTable(4);
  table.count = 4;

  // Row 1 is dead and redirects to row 0. Rows 2 and 3 are live and will move
  // down one slot, so their dependency metadata must move with them.
  table.packSlots[0] = 0;
  table.entryIndices[0] = 0;
  table.offsets[0] = 10;
  table.oidsRaw[0] = 0x10;
  table.typeCodes[0] = 3;
  table.baseSlots[0] = -1;

  table.packSlots[1] = 0;
  table.entryIndices[1] = 1;
  table.offsets[1] = 20;
  table.oidsRaw[20] = 0x20;
  table.typeCodes[1] = 7;
  table.baseSlots[1] = 0;

  table.packSlots[2] = 0;
  table.entryIndices[2] = 2;
  table.offsets[2] = 30;
  table.oidsRaw[40] = 0x30;
  table.typeCodes[2] = 3;
  table.baseSlots[2] = -1;

  table.packSlots[3] = 0;
  table.entryIndices[3] = 3;
  table.offsets[3] = 40;
  table.oidsRaw[60] = 0x40;
  table.typeCodes[3] = 6;
  table.baseSlots[3] = 2;

  compactDeadSlots(table, new Map([[1, 0]]), createLogger("error", { service: "test" }));

  assert.strictEqual(table.count, 3);
  assert.deepEqual(Array.from(table.entryIndices.subarray(0, 3)), [0, 2, 3]);
  assert.deepEqual(Array.from(table.baseSlots.subarray(0, 3)), [-1, -1, 1]);
  assert.deepEqual([table.oidsRaw[0], table.oidsRaw[20], table.oidsRaw[40]], [0x10, 0x30, 0x40]);

  assert.isTrue(buildOutputOrder(table, createLogger("error", { service: "test" })));
  assert.deepEqual(Array.from(table.outputOrder.subarray(0, 3)), [0, 1, 2]);
});
