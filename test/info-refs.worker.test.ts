import { it, expect } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";
import { uniqueRepoId } from "./util/test-helpers";
import { setupRepoForTests } from "./util/repoSeed";
import { decodePktLinePayloads } from "./util/fetch-protocol";

it("advertises upload-pack v2 over info/refs", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-info-refs");
  await setupRepoForTests(env, owner, repo);

  const url = new URL(`https://example.com/${owner}/${repo}/info/refs`);
  url.searchParams.set("service", "git-upload-pack");
  const res = await workerExports.default.fetch(new Request(url, { method: "GET" }));
  expect(res.status).toBe(200);
  expect(res.headers.get("Content-Type")).toContain("git-upload-pack-advertisement");
  const bytes = new Uint8Array(await res.arrayBuffer());
  // The v2 prelude should start with an announcement and a flush, then version/agent lines
  const textLines = decodePktLinePayloads(bytes);
  // Look for "version 2" and "fetch" capability
  expect(textLines.some((l) => l === "version 2\n")).toBe(true);
  expect(textLines.some((l) => l === "fetch\n")).toBe(true);
  // And the features we advertise
  expect(textLines.some((l) => l === "ofs-delta\n")).toBe(true);
  expect(textLines.some((l) => l === "side-band-64k\n")).toBe(true);
});

it("advertises upload-pack v2 over .git info/refs", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-info-refs-dotgit");
  await setupRepoForTests(env, owner, repo);

  const url = new URL(`https://example.com/${owner}/${repo}.git/info/refs`);
  url.searchParams.set("service", "git-upload-pack");
  const res = await workerExports.default.fetch(new Request(url, { method: "GET" }));
  expect(res.status).toBe(200);
  expect(res.headers.get("Content-Type")).toContain("git-upload-pack-advertisement");
});
