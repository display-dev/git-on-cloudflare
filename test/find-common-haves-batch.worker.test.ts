import { it, expect } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";
import { decodePktLines } from "@/worker/git";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers";
import { setupRepoForTests } from "./util/repoSeed";

function buildFetchBody({
  wants,
  haves,
  done,
}: {
  wants: string[];
  haves?: string[];
  done?: boolean;
}) {
  // Build protocol v2 fetch request body using pkt-line framing
  const chunks: Uint8Array[] = [];
  const enc = new TextEncoder();
  function pkt(s: string) {
    const bytes = enc.encode(s);
    const len = (bytes.length + 4).toString(16).padStart(4, "0");
    const out = new Uint8Array(4 + bytes.length);
    out.set(enc.encode(len), 0);
    out.set(bytes, 4);
    return out;
  }
  function flush() {
    return enc.encode("0000");
  }
  chunks.push(pkt("command=fetch\n"));
  chunks.push(pkt("agent=test\n"));
  chunks.push(enc.encode("0001")); // delim
  for (const w of wants) chunks.push(pkt(`want ${w}\n`));
  for (const h of haves || []) chunks.push(pkt(`have ${h}\n`));
  if (done) chunks.push(pkt("done\n"));
  chunks.push(flush());
  // concat
  let total = 0;
  for (const c of chunks) total += c.length;
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}

function parseResponseLines(respBytes: Uint8Array): string[] {
  const items = decodePktLines(respBytes);
  return items.filter((item) => item.type === "line").map((item) => item.text.trim());
}

function randomOid(seed: string) {
  return seed.repeat(40).slice(0, 40).toLowerCase();
}

it("findCommonHaves batches and ACKs present haves preserving order and de-dup", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-find-haves");
  await setupRepoForTests(env, owner, repo);
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const { commitOid, treeOid } = await runDOWithRetry(
    () => env.REPO_DO.get(id),
    async (instance) => instance.seedMinimalRepo()
  );

  // Build haves list with duplicates and missing entries interleaved
  const missing1 = randomOid("a");
  const missing2 = randomOid("b");
  const haves = [commitOid, missing1, treeOid, commitOid, missing2];

  // Wants commitOid so server will assemble a minimal pack
  const body = buildFetchBody({ wants: [commitOid], haves, done: false });
  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const res = await workerExports.default.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body,
  } as any);
  expect(res.status).toBe(200);

  const bytes = new Uint8Array(await res.arrayBuffer());
  const responseLines = parseResponseLines(bytes);
  const ackLines = responseLines.filter((line) => line.startsWith("ACK "));
  // Expect two ACKs for commitOid and treeOid, de-duplicated, preserving first-order appearance
  expect(ackLines).toEqual([`ACK ${commitOid}`, `ACK ${treeOid}`]);
  expect(responseLines).toContain("ready");
  expect(responseLines).toContain("packfile");
});
