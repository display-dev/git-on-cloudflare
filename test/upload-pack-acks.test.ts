import { assert, test } from "vitest";
import type { PktItem } from "@/worker/git/core";
import { concatChunks, decodePktLines, flushPkt } from "@/worker/git";
import { buildAckOnlyResponse, buildAckSection } from "@/worker/git/operations/fetch/protocol";
import { createSidebandPacketChunks } from "@/worker/git/operations/fetch/sideband";

function findLine(items: PktItem[], text: string): number {
  return items.findIndex((item) => item.type === "line" && item.text === text);
}

function expectLine(item: PktItem | undefined) {
  if (!item || item.type !== "line") {
    throw new Error("expected pkt line");
  }
  return item;
}

function buildPacketizedResponse(
  packfile: Uint8Array,
  done: boolean,
  ackOids: string[]
): Uint8Array {
  return concatChunks([
    ...buildAckSection(ackOids, done),
    ...createSidebandPacketChunks(1, packfile),
    flushPkt(),
  ]);
}

test("packetized response emits NAK when no common haves and done=false", () => {
  const items = decodePktLines(
    buildPacketizedResponse(new Uint8Array([0x50, 0x41, 0x43, 0x4b]), false, [])
  );

  const ackIndex = findLine(items, "acknowledgments\n");
  assert.isTrue(ackIndex >= 0);
  assert.strictEqual(expectLine(items[ackIndex + 1]).text, "NAK\n");
  assert.strictEqual(items[ackIndex + 2]?.type, "delim");
  assert.strictEqual(expectLine(items[ackIndex + 3]).text, "packfile\n");
  const bandLine = expectLine(items[ackIndex + 4]);
  assert.isTrue((bandLine.raw?.length || 0) >= 1);
  assert.strictEqual(bandLine.raw?.[0], 0x01);
});

test("packetized response emits protocol-v2 ACK lines followed by ready", () => {
  const firstOid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const secondOid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  const items = decodePktLines(
    buildPacketizedResponse(new Uint8Array([0xaa]), false, [firstOid, secondOid])
  );

  const ackIndex = findLine(items, "acknowledgments\n");
  assert.isTrue(ackIndex >= 0);
  assert.strictEqual(expectLine(items[ackIndex + 1]).text, `ACK ${firstOid}\n`);
  assert.strictEqual(expectLine(items[ackIndex + 2]).text, `ACK ${secondOid}\n`);
  assert.strictEqual(expectLine(items[ackIndex + 3]).text, "ready\n");
  assert.strictEqual(items[ackIndex + 4]?.type, "delim");
});

test("packetized response omits acknowledgments when done=true", () => {
  const items = decodePktLines(buildPacketizedResponse(new Uint8Array([0xff, 0x00]), true, []));
  assert.isTrue(findLine(items, "packfile\n") >= 0);
  assert.strictEqual(findLine(items, "acknowledgments\n"), -1);
});

test("acknowledgment-only response does not claim readiness without a packfile", async () => {
  const oid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const response = buildAckOnlyResponse([oid]);
  const items = decodePktLines(new Uint8Array(await response.arrayBuffer()));

  assert.isTrue(findLine(items, `ACK ${oid}\n`) >= 0);
  assert.strictEqual(findLine(items, "ready\n"), -1);
  assert.strictEqual(findLine(items, "packfile\n"), -1);
});
