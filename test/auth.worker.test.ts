import { it, expect } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";
import { pktLine, flushPkt, concatChunks } from "@/worker/git";
import { asBufferSource, deflate } from "@/worker/common";
import { setupRepoForTests } from "./util/repoSeed";

function encodeObjHeader(type: number, size: number): Uint8Array {
  let first = (type << 4) | (size & 0x0f);
  size >>= 4;
  const bytes: number[] = [];
  if (size > 0) first |= 0x80;
  bytes.push(first);
  while (size > 0) {
    let b = size & 0x7f;
    size >>= 7;
    if (size > 0) b |= 0x80;
    bytes.push(b);
  }
  return new Uint8Array(bytes);
}

async function buildPack(
  objects: { type: "commit" | "tree" | "blob" | "tag"; payload: Uint8Array }[]
): Promise<Uint8Array> {
  const hdr = new Uint8Array(12);
  hdr.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(hdr.buffer);
  dv.setUint32(4, 2);
  dv.setUint32(8, objects.length);
  const parts: Uint8Array[] = [hdr];
  for (const o of objects) {
    const typeCode = o.type === "commit" ? 1 : o.type === "tree" ? 2 : o.type === "blob" ? 3 : 4;
    parts.push(encodeObjHeader(typeCode, o.payload.byteLength));
    parts.push(await deflate(o.payload));
  }
  const body = concatChunks(parts);
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(body)));
  const out = new Uint8Array(body.byteLength + 20);
  out.set(body, 0);
  out.set(sha, body.byteLength);
  return out;
}

function zero40() {
  return "0".repeat(40);
}

function basicAuth(user: string, pass: string) {
  const pair = `${user}:${pass}`;
  const b64 = btoa(pair);
  return `Basic ${b64}`;
}

it("auth: owner token management API is absent", async () => {
  const res = await workerExports.default.fetch("https://example.com/auth/api/users", {
    headers: {
      Authorization: "Bearer admin",
    },
  } as any);
  expect(res.status).toBe(404);
});

it("auth: receive-pack requires a push PAT and rejects non-PAT Basic credentials", async () => {
  const owner = "alice";
  const repo = "auth-repo";
  const token = "alicesecret";
  await setupRepoForTests(env, owner, repo);

  const treePayload = new Uint8Array(0);
  const treeHeader = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
  const treeRaw = new Uint8Array(treeHeader.length + treePayload.length);
  treeRaw.set(treeHeader, 0);
  treeRaw.set(treePayload, treeHeader.length);
  const treeOid = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-1", treeRaw)))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  const author = `You <you@example.com> 0 +0000`;
  const commitPayload = new TextEncoder().encode(
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${author}\n\nmsg\n`
  );
  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
  ]);
  const commitOid = await (async () => {
    const head = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + commitPayload.length);
    raw.set(head, 0);
    raw.set(commitPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();

  const cmd = `${zero40()} ${commitOid} refs/heads/main\0 report-status\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // No auth → 401
  const r1 = await workerExports.default.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  expect(r1.status).toBe(401);

  // Wrong username → 401
  const r2 = await workerExports.default.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-receive-pack-request",
      Authorization: basicAuth("bob", token),
    },
    body,
  } as any);
  expect(r2.status).toBe(401);

  // Correct username but wrong token → 401 (malformed PAT shape)
  const r3 = await workerExports.default.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-receive-pack-request",
      Authorization: basicAuth(owner, "wrong"),
    },
    body,
  } as any);
  expect(r3.status).toBe(401);

  // Correct username + non-PAT password -> 401.
  const r4 = await workerExports.default.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-receive-pack-request",
      Authorization: basicAuth(owner, token),
    },
    body,
  } as any);
  expect(r4.status).toBe(401);
});

it("auth: per-repo admin endpoints reject Basic credentials and require session membership", async () => {
  const owner = "alice2";
  const repo = "auth-repo2";
  const token = "s3cr3t";
  const seededRepo = await setupRepoForTests(env, owner, repo);

  const refsUrl = `https://example.com/${owner}/${repo}/admin/refs`;

  // No auth -> 401
  const a1 = await workerExports.default.fetch(refsUrl);
  expect(a1.status).toBe(401);

  // Git Basic credentials do not authorize repo admin.
  const a2 = await workerExports.default.fetch(refsUrl, {
    headers: { Authorization: basicAuth(owner, token) },
  } as any);
  expect(a2.status).toBe(401);

  // Session cookie for a member -> 200.
  const a3 = await workerExports.default.fetch(refsUrl, {
    headers: { Cookie: seededRepo.cookieHeader },
  } as any);
  expect(a3.status).toBe(200);
  const refs = await a3.json();
  expect(Array.isArray(refs)).toBe(true);
});
