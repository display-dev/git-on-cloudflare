import { it, expect } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";
import { decodePktLines, pktLine, flushPkt, concatChunks } from "@/worker/git";
import { postReceivePack, uniqueRepoId, buildPack, zero40 } from "./util/test-helpers";
import { setupRepoForTests } from "./util/repoSeed";
import { bytesToHex } from "@/worker/common/hex";

it("receive-pack connectivity: rejects commit whose root tree is missing", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-connectivity-missing-tree");
  const seededRepo = await setupRepoForTests(env, owner, repo);
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Construct a commit that points to a tree OID that is NOT present anywhere on the server
  const missingTreeOid = "c".repeat(40);
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "missing tree\n";
  const commitPayload = new TextEncoder().encode(
    `tree ${missingTreeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );

  // Compute commit oid for assertions (like real git object oid)
  const commitOid = await (async () => {
    const head = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + commitPayload.length);
    raw.set(head, 0);
    raw.set(commitPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();

  // Create a PACK containing ONLY the commit (missing its root tree)
  const pack = await buildPack([{ type: "commit", payload: commitPayload }]);

  // Commands section (pkt-lines) followed by flush and then raw pack bytes
  const cmd = `${zero40()} ${commitOid} refs/heads/main\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);

  const res = await postReceivePack(url, body);
  expect(res.status).toBe(200);

  // report-status should contain ng for the ref due to missing-objects
  const bytes = new Uint8Array(await res.arrayBuffer());
  const items = decodePktLines(bytes);
  const lines = items.filter((i) => i.type === "line").map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  const ng = lines.find((l) => l.startsWith("ng refs/heads/main"));
  expect(ng && /missing-objects/.test(ng)).toBeTruthy();

  // Verify ref was NOT created
  const refsRes = await workerExports.default.fetch(
    `https://example.com/${owner}/${repo}/admin/refs`,
    { headers: { Cookie: seededRepo.cookieHeader } }
  );
  expect(refsRes.status).toBe(200);
  const refs = await refsRes.json<any>();
  expect(refs.find((r: any) => r.name === "refs/heads/main")).toBeUndefined();
});

it("receive-pack connectivity: accepts annotated tag pointing to commit with present tree", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-tag-commit-ok");
  await setupRepoForTests(env, owner, repo);
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Build empty tree and commit
  const treePayload = new Uint8Array(0);
  const treeOid = await (async () => {
    const head = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + treePayload.length);
    raw.set(head, 0);
    raw.set(treePayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "tag->commit ok\n";
  const commitPayload = new TextEncoder().encode(
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );
  const commitOid = await (async () => {
    const head = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + commitPayload.length);
    raw.set(head, 0);
    raw.set(commitPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();
  // Annotated tag pointing to the commit
  const tagPayload = new TextEncoder().encode(
    `object ${commitOid}\n` +
      `type commit\n` +
      `tag v1\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg\n`
  );
  const tagOid = await (async () => {
    const head = new TextEncoder().encode(`tag ${tagPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + tagPayload.length);
    raw.set(head, 0);
    raw.set(tagPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();

  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
    { type: "tag", payload: tagPayload },
  ]);

  const cmd = `${zero40()} ${tagOid} refs/tags/v1\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await postReceivePack(url, body);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/tags/v1");
});

it("receive-pack connectivity: accepts annotated tag pointing to tree present", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-tag-tree-ok");
  await setupRepoForTests(env, owner, repo);
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  const treePayload = new Uint8Array(0);
  const treeOid = await (async () => {
    const head = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + treePayload.length);
    raw.set(head, 0);
    raw.set(treePayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();
  const tagPayload = new TextEncoder().encode(
    `object ${treeOid}\n` +
      `type tree\n` +
      `tag v2\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg\n`
  );
  const tagOid = await (async () => {
    const head = new TextEncoder().encode(`tag ${tagPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + tagPayload.length);
    raw.set(head, 0);
    raw.set(tagPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();
  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "tag", payload: tagPayload },
  ]);
  const cmd = `${zero40()} ${tagOid} refs/tags/v2\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await postReceivePack(url, body);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/tags/v2");
});

it("receive-pack connectivity: rejects annotated tag pointing to commit with missing tree", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-tag-commit-missing-tree");
  await setupRepoForTests(env, owner, repo);
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;
  const missingTreeOid = "c".repeat(40);
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "tag->commit missing tree\n";
  const commitPayload = new TextEncoder().encode(
    `tree ${missingTreeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );
  const commitOid = await (async () => {
    const head = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + commitPayload.length);
    raw.set(head, 0);
    raw.set(commitPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();
  const tagPayload = new TextEncoder().encode(
    `object ${commitOid}\n` +
      `type commit\n` +
      `tag v3\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg\n`
  );
  const tagOid = await (async () => {
    const head = new TextEncoder().encode(`tag ${tagPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + tagPayload.length);
    raw.set(head, 0);
    raw.set(tagPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();
  const pack = await buildPack([
    { type: "commit", payload: commitPayload },
    { type: "tag", payload: tagPayload },
  ]);
  const cmd = `${zero40()} ${tagOid} refs/tags/v3\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await postReceivePack(url, body);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  const ng = lines.find((l) => l.startsWith("ng refs/tags/v3"));
  expect(ng && /missing-objects/.test(ng)).toBeTruthy();
});

it("receive-pack connectivity: accepts direct ref to tree present", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-tree-ref-ok");
  await setupRepoForTests(env, owner, repo);
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Build an empty tree
  const treePayload = new Uint8Array(0);
  const treeOid = await (async () => {
    const head = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + treePayload.length);
    raw.set(head, 0);
    raw.set(treePayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();

  const pack = await buildPack([{ type: "tree", payload: treePayload }]);

  const cmd = `${zero40()} ${treeOid} refs/tags/tree-only\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await postReceivePack(url, body);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/tags/tree-only");
});

it("receive-pack connectivity: accepts direct ref to blob present", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-blob-ref-ok");
  await setupRepoForTests(env, owner, repo);
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  const blobPayload = new TextEncoder().encode("hello\n");
  const blobOid = await (async () => {
    const head = new TextEncoder().encode(`blob ${blobPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + blobPayload.length);
    raw.set(head, 0);
    raw.set(blobPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();

  const pack = await buildPack([{ type: "blob", payload: blobPayload }]);

  const cmd = `${zero40()} ${blobOid} refs/tags/blob-only\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await postReceivePack(url, body);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/tags/blob-only");
});

it("receive-pack connectivity: accepts nested tag->tag->tree present", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-nested-tag-tree-ok");
  await setupRepoForTests(env, owner, repo);
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  const treePayload = new Uint8Array(0);
  const treeOid = await (async () => {
    const head = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + treePayload.length);
    raw.set(head, 0);
    raw.set(treePayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();
  const tag1Payload = new TextEncoder().encode(
    `object ${treeOid}\n` +
      `type tree\n` +
      `tag v1\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg\n`
  );
  const tag1Oid = await (async () => {
    const head = new TextEncoder().encode(`tag ${tag1Payload.byteLength}\0`);
    const raw = new Uint8Array(head.length + tag1Payload.length);
    raw.set(head, 0);
    raw.set(tag1Payload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();
  const tag2Payload = new TextEncoder().encode(
    `object ${tag1Oid}\n` +
      `type tag\n` +
      `tag v2\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg2\n`
  );
  const tag2Oid = await (async () => {
    const head = new TextEncoder().encode(`tag ${tag2Payload.byteLength}\0`);
    const raw = new Uint8Array(head.length + tag2Payload.length);
    raw.set(head, 0);
    raw.set(tag2Payload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return bytesToHex(new Uint8Array(hash));
  })();

  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "tag", payload: tag1Payload },
    { type: "tag", payload: tag2Payload },
  ]);

  const cmd = `${zero40()} ${tag2Oid} refs/tags/v2\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const res = await postReceivePack(url, body);
  const lines = decodePktLines(new Uint8Array(await res.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/tags/v2");
});
