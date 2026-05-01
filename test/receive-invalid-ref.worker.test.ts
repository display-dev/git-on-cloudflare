import { it, expect } from "vitest";
import { SELF } from "cloudflare:test";
import { decodePktLines, pktLine, flushPkt, concatChunks } from "@/worker/git";
import { buildPack } from "./util/test-helpers";

function zero40() {
  return "0".repeat(40);
}

it("receive-pack: rejects update to HEAD ref as invalid", async () => {
  const owner = "o";
  const repo = "r-push-invalid-head";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Empty pack (no objects) is sufficient to trigger ref-name validation, which happens before unpack
  const pack = await buildPack([]);
  const cmd = `${zero40()} ${"a".repeat(40)} HEAD\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);

  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  expect(res.status).toBe(200);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  // Should report invalid ref
  const ng = lines.find((l) => l.startsWith("ng HEAD "));
  expect(ng && /invalid$/.test(ng)).toBeTruthy();
});

it("receive-pack: rejects invalid ref name with spaces", async () => {
  const owner = "o";
  const repo = "r-push-invalid-ref";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  const pack = await buildPack([]);
  const badRef = "refs/heads/invalid name"; // contains space
  const cmd = `${zero40()} ${"b".repeat(40)} ${badRef}\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);

  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  expect(res.status).toBe(200);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  const ng = lines.find((l) => l.startsWith(`ng ${badRef} `));
  expect(ng && /invalid$/.test(ng)).toBeTruthy();
});
