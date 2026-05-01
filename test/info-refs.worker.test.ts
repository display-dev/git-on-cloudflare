import { it, expect } from "vitest";
import { SELF } from "cloudflare:test";
import { uniqueRepoId } from "./util/test-helpers.ts";
import { decodePktLines } from "@/worker/git";

it("advertises upload-pack v2 over info/refs", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-info-refs");

  const url = new URL(`https://example.com/${owner}/${repo}/info/refs`);
  url.searchParams.set("service", "git-upload-pack");
  const res = await SELF.fetch(new Request(url, { method: "GET" }));
  expect(res.status).toBe(200);
  expect(res.headers.get("Content-Type")).toContain("git-upload-pack-advertisement");
  const bytes = new Uint8Array(await res.arrayBuffer());
  const items = decodePktLines(bytes);
  // The v2 prelude should start with an announcement and a flush, then version/agent lines
  const textLines = items.filter((i) => i.type === "line").map((i: any) => i.text);
  // Look for "version 2" and "fetch" capability
  expect(textLines.some((l) => l === "version 2\n")).toBe(true);
  expect(textLines.some((l) => l === "fetch\n")).toBe(true);
  // And the features we advertise
  expect(textLines.some((l) => l === "ofs-delta\n")).toBe(true);
  expect(textLines.some((l) => l === "side-band-64k\n")).toBe(true);
});
