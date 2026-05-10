import { it, expect } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";
import { uniqueRepoId, runDOWithRetry, toRequestBody } from "./util/test-helpers";
import { setupRepoForTests } from "./util/repoSeed";
import { decodePktLinePayloads } from "./util/fetch-protocol";

function pktLine(s: string | Uint8Array): Uint8Array {
  const enc = typeof s === "string" ? new TextEncoder().encode(s) : s;
  const len = enc.byteLength + 4;
  const hdr = new TextEncoder().encode(len.toString(16).padStart(4, "0"));
  const out = new Uint8Array(hdr.byteLength + enc.byteLength);
  out.set(hdr, 0);
  out.set(enc, hdr.byteLength);
  return out;
}
function delimPkt() {
  return new TextEncoder().encode("0001");
}
function flushPkt() {
  return new TextEncoder().encode("0000");
}
function concatChunks(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((a, c) => a + c.byteLength, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.byteLength;
  }
  return out;
}

function buildLsRefsBody(args: string[] = []) {
  const chunks: Uint8Array[] = [];
  chunks.push(pktLine("command=ls-refs\n"));
  chunks.push(delimPkt());
  for (const a of args) chunks.push(pktLine(a + "\n"));
  chunks.push(flushPkt());
  return concatChunks(chunks);
}

it("ls-refs: unborn HEAD advertises correctly", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-lsrefs-unborn");
  await setupRepoForTests(env, owner, repo);
  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const body = buildLsRefsBody(["ref-prefix refs/heads/"]);
  const res = await workerExports.default.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body: toRequestBody(body),
  });
  expect(res.status).toBe(200);
  const bytes = new Uint8Array(await res.arrayBuffer());
  const lines = decodePktLinePayloads(bytes);
  // First line should indicate unborn HEAD with symref target
  expect(lines[0]).toBe("unborn HEAD symref-target:refs/heads/main\n");
});

it("ls-refs: .git upload-pack URL resolves to the canonical repository", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-lsrefs-dotgit");
  await setupRepoForTests(env, owner, repo);
  const url = `https://example.com/${owner}/${repo}.git/git-upload-pack`;
  const body = buildLsRefsBody(["ref-prefix refs/heads/"]);
  const res = await workerExports.default.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body: toRequestBody(body),
  });
  expect(res.status).toBe(200);
  expect(res.headers.get("Content-Type")).toContain("git-upload-pack-result");
});

it("ls-refs: resolved HEAD and refs are listed after seeding", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-lsrefs-resolved");
  await setupRepoForTests(env, owner, repo);
  // Seed directly via DO (runInDurableObject)
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const { commitOid } = await runDOWithRetry(
    () => env.REPO_DO.get(id),
    async (instance) => instance.seedMinimalRepo()
  );

  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const body = buildLsRefsBody(["ref-prefix refs/heads/"]);
  const res = await workerExports.default.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body: toRequestBody(body),
  });
  expect(res.status).toBe(200);
  const bytes = new Uint8Array(await res.arrayBuffer());
  const lines = decodePktLinePayloads(bytes);
  // First line should show HEAD resolved with symref
  expect(lines[0]).toBe(`${commitOid} HEAD symref-target:refs/heads/main\n`);
  // There should be a line for refs/heads/main
  expect(lines.some((l) => l === `${commitOid} refs/heads/main\n`)).toBe(true);
});
