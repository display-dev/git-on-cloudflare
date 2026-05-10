import { it, expect } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";
import { pktLine, delimPkt, flushPkt, concatChunks, encodeGitObject } from "@/worker/git";
import { uniqueRepoId, runDOWithRetry, toRequestBody } from "./util/test-helpers";
import { setupRepoForTests } from "./util/repoSeed";
import { registerTestPack } from "./util/packed-repo";
import { decodePktLinePayloads } from "./util/fetch-protocol";

function buildLsRefsBody(args: string[] = []) {
  const chunks: Uint8Array[] = [];
  chunks.push(pktLine("command=ls-refs\n"));
  chunks.push(delimPkt());
  for (const a of args) chunks.push(pktLine(a + "\n"));
  chunks.push(flushPkt());
  return concatChunks(chunks);
}

it("ls-refs: ref-prefix filters refs and peel adds peeled attribute for annotated tags", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-lsrefs-filters");
  await setupRepoForTests(env, owner, repo);
  const repoId = `${owner}/${repo}`;

  // Seed minimal repo to create HEAD -> refs/heads/main
  const id = env.REPO_DO.idFromName(repoId);
  const { commitOid } = await runDOWithRetry(
    () => env.REPO_DO.get(id),
    async (instance) => instance.seedMinimalRepo()
  );

  // Create an annotated tag pointing to the commit
  const tagPayload = new TextEncoder().encode(
    `object ${commitOid}\n` +
      `type commit\n` +
      `tag v1\n` +
      `tagger You <you@example.com> 0 +0000\n\nmsg\n`
  );
  const { oid: tagOid } = await encodeGitObject("tag", tagPayload);

  // Register the annotated tag in a pack (post-closure: no loose objects from push)
  await registerTestPack({
    env,
    repoId,
    getStub: () => env.REPO_DO.get(id),
    packName: `pack-tag-${Date.now()}.pack`,
    objects: [{ type: "tag" as const, payload: tagPayload }],
  });
  // Add the tag ref
  await runDOWithRetry(
    () => env.REPO_DO.get(id),
    async (instance) => {
      const { refs } = await instance.getHeadAndRefs();
      refs.push({ name: "refs/tags/v1", oid: tagOid });
      await instance.setRefs(refs);
    }
  );

  // Ask only for refs/tags/* and request peeling
  const body = buildLsRefsBody(["ref-prefix refs/tags/", "peel"]);
  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const res = await workerExports.default.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body: toRequestBody(body),
  });
  expect(res.status).toBe(200);
  const lines = decodePktLinePayloads(new Uint8Array(await res.arrayBuffer()));

  // HEAD should be first and present
  expect(lines[0]?.startsWith("unborn HEAD") || lines[0]?.startsWith(commitOid + " HEAD")).toBe(
    true
  );
  // The tag must be listed and include peeled:<commitOid>
  const tagLine = lines.find((l: string) => l.includes(" refs/tags/v1"));
  expect(tagLine).toBeDefined();
  expect(tagLine!.includes(`peeled:${commitOid}`)).toBe(true);
  // No branch refs (refs/heads/*) beyond HEAD line
  expect(lines.slice(1).some((l: string) => /\srefs\/heads\//.test(l))).toBe(false);
});

it("ls-refs: peel resolves many annotated tags stored across multiple packs", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-lsrefs-many-tags");
  await setupRepoForTests(env, owner, repo);
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id);

  const { commitOid } = await runDOWithRetry(getStub, async (instance) =>
    instance.seedMinimalRepo()
  );

  const tagCount = 48;
  const refs: { name: string; oid: string }[] = [];
  const firstPackObjects: Array<{ type: "tag"; payload: Uint8Array }> = [];
  const secondPackObjects: Array<{ type: "tag"; payload: Uint8Array }> = [];

  for (let index = 0; index < tagCount; index++) {
    const tagName = `v${index.toString().padStart(2, "0")}`;
    const tagPayload = new TextEncoder().encode(
      `object ${commitOid}\n` +
        `type commit\n` +
        `tag ${tagName}\n` +
        `tagger You <you@example.com> 0 +0000\n\nmsg ${tagName}\n`
    );
    const { oid } = await encodeGitObject("tag", tagPayload);
    refs.push({ name: `refs/tags/${tagName}`, oid });
    const object = { type: "tag" as const, payload: tagPayload };
    if (index < tagCount / 2) {
      firstPackObjects.push(object);
    } else {
      secondPackObjects.push(object);
    }
  }

  await registerTestPack({
    env,
    repoId,
    getStub,
    packName: `pack-tags-a-${Date.now()}.pack`,
    objects: firstPackObjects,
  });
  await registerTestPack({
    env,
    repoId,
    getStub,
    packName: `pack-tags-b-${Date.now()}.pack`,
    objects: secondPackObjects,
  });

  await runDOWithRetry(getStub, async (instance) => {
    const current = await instance.getHeadAndRefs();
    await instance.setRefs([...current.refs, ...refs]);
  });

  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const res = await workerExports.default.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body: toRequestBody(buildLsRefsBody(["ref-prefix refs/tags/", "peel"])),
  });
  expect(res.status).toBe(200);

  const lines = decodePktLinePayloads(new Uint8Array(await res.arrayBuffer()));
  const tagLines = lines.filter((line) => line.includes(" refs/tags/"));
  expect(tagLines.length).toBe(tagCount);
  for (let index = 0; index < tagCount; index++) {
    const tagName = `refs/tags/v${index.toString().padStart(2, "0")}`;
    const line = tagLines.find((candidate) => candidate.includes(` ${tagName}`));
    expect(line, `missing ${tagName}`).toBeDefined();
    expect(line?.includes(`peeled:${commitOid}`), `missing peeled attr for ${tagName}`).toBe(true);
  }
});
