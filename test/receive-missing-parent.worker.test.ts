import { it, expect } from "vitest";
import { env } from "cloudflare:workers";
import { pktLine, flushPkt, concatChunks } from "@/worker/git";
import { buildPack, makeTree, postReceivePack, uniqueRepoId, zero40 } from "./util/test-helpers";
import { setupRepoForTests } from "./util/repoSeed";

it("receive-pack connectivity: rejects commit whose parent is missing", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-missing-parent");
  await setupRepoForTests(env, owner, repo);
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Build an empty tree and a commit that points to a nonexistent parent
  const { oid: treeOid, payload: treePayload } = await makeTree();
  const missingParent = "d".repeat(40);
  const author = `You <you@example.com> 0 +0000`;
  const msg = "missing parent\n";
  const commitPayload = new TextEncoder().encode(
    `tree ${treeOid}\n` +
      `parent ${missingParent}\n` +
      `author ${author}\n` +
      `committer ${author}\n\n${msg}`
  );
  const commitHead = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
  const commitRaw = new Uint8Array(commitHead.length + commitPayload.length);
  commitRaw.set(commitHead, 0);
  commitRaw.set(commitPayload, commitHead.length);
  const commitOid = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-1", commitRaw)))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  // Build pack containing tree and the commit-with-missing-parent
  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
  ]);

  // Commands section then pack
  const cmd = `${zero40()} ${commitOid} refs/heads/dev\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);

  const res = await postReceivePack(url, body);
  expect(res.status).toBe(200);
  const bytes = new Uint8Array(await res.arrayBuffer());
  const lines = (await import("@/worker/git"))
    .decodePktLines(bytes)
    .filter((i: any) => i.type === "line")
    .map((i: any) => i.text.trim());
  const ng = lines.find((l) => l.startsWith("ng refs/heads/dev "));
  expect(ng && /missing-objects$/.test(ng)).toBeTruthy();
});
