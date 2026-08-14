import { describe, expect, it } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { zeroOid } from "@/worker/common";
import { parseCommitRefs } from "@/worker/git/core";
import { readObject } from "@/worker/git/object-store";
import { parseTree } from "@/worker/git/operations/read/objects";
import { __test as receivePipelineTest } from "@/worker/git/receive/pipeline";
import { __test as receiveCatalogTest } from "@/worker/do/repo/catalog/receive";
import type {
  FinalizeReceiveResult,
  ReconcileReceiveResult,
} from "@/worker/do/repo/catalog/receive";
import type { BeginReceiveResult } from "@/worker/do/repo/catalog/shared";

import { createTestCacheContext } from "./util/pack-first";
import { setupRepoForTests } from "./util/repoSeed";
import { callStubWithRetry, uniqueRepoId, withEnvOverrides } from "./util/test-helpers";

type IngestionResponse = {
  acceptedWrite: {
    repositoryId: string;
    ref: string;
    beforeSha: string;
    afterSha: string;
    actor: string;
    sourceSurface: string;
    idempotencyKey: string | null;
  };
  treeSha: string;
  replayed: boolean;
};

type SnapshotManifest = {
  version: number;
  repositoryId: string;
  commitSha: string;
  treeSha: string;
  files: Array<{ path: string; bytes: number; sha256: string }>;
};

function ingestionForm(args?: {
  expectedOid?: string;
  committedAtSeconds?: number;
  message?: string;
  path?: string;
  content?: string;
  actor?: string;
  idempotencyKey?: string;
}): FormData {
  const form = new FormData();
  form.set("expectedOid", args?.expectedOid ?? zeroOid());
  form.set("actor", args?.actor ?? "user-ingestion-test");
  form.set("idempotencyKey", args?.idempotencyKey ?? "publish-request-1");
  form.set("committedAtSeconds", String(args?.committedAtSeconds ?? 1_700_000_000));
  form.set("message", args?.message ?? "Publish folder");
  form.append(
    "files",
    new Blob([args?.content ?? "hello from ingestion\n"]),
    args?.path ?? "nested/hello.txt"
  );
  form.append("files", new Blob(["<h1>index</h1>\n"]), "index.html");
  return form;
}

async function postIngestion(
  owner: string,
  repo: string,
  form: FormData,
  token = env.INGESTION_RPC_TOKEN
): Promise<Response> {
  return await workerExports.default.fetch(
    `https://example.com/_internal/ingestion/${owner}/${repo}`,
    {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: form,
    }
  );
}

describe("internal ingestion", () => {
  it("builds a nested Git commit, commits main through ref CAS, and replays without a new pack", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("round-trip");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });

    const response = await postIngestion(owner, repo, ingestionForm());
    expect(response.status).toBe(201);
    const body = (await response.json()) as IngestionResponse;
    expect(body).toMatchObject({
      acceptedWrite: {
        repositoryId: seeded.repositoryId,
        ref: "refs/heads/main",
        beforeSha: zeroOid(),
        actor: "user-ingestion-test",
        sourceSurface: "ingestion",
        idempotencyKey: "publish-request-1",
      },
      replayed: false,
    });
    expect(body.acceptedWrite.afterSha).toMatch(/^[0-9a-f]{40}$/);
    expect(body.treeSha).toMatch(/^[0-9a-f]{40}$/);

    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    const state = await callStubWithRetry(
      () => stub,
      (repoStub) => repoStub.getHeadAndRefs()
    );
    expect(state.refs).toContainEqual({
      name: "refs/heads/main",
      oid: body.acceptedWrite.afterSha,
    });

    const cacheCtx = createTestCacheContext("https://example.com/ingestion-object-check");
    const commit = await readObject(env, seeded.doName, body.acceptedWrite.afterSha, cacheCtx);
    expect(commit?.type).toBe("commit");
    const commitRefs = parseCommitRefs(commit!.payload);
    expect(commitRefs).toEqual({ tree: body.treeSha, parents: [] });

    const rootTree = await readObject(env, seeded.doName, body.treeSha, cacheCtx);
    expect(rootTree?.type).toBe("tree");
    const rootEntries = parseTree(rootTree!.payload);
    expect(rootEntries.map((entry) => [entry.mode, entry.name])).toEqual([
      ["100644", "index.html"],
      ["40000", "nested"],
    ]);
    const nestedTree = await readObject(env, seeded.doName, rootEntries[1]!.oid, cacheCtx);
    const nestedEntries = parseTree(nestedTree!.payload);
    expect(nestedEntries).toHaveLength(1);
    expect(nestedEntries[0]).toMatchObject({ mode: "100644", name: "hello.txt" });
    const blob = await readObject(env, seeded.doName, nestedEntries[0]!.oid, cacheCtx);
    expect(blob?.type).toBe("blob");
    expect(new TextDecoder().decode(blob!.payload)).toBe("hello from ingestion\n");

    const snapshotBase = `https://example.com/_internal/snapshots/${owner}/${repo}/${body.acceptedWrite.afterSha}`;
    const snapshotHeaders = { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` };
    const manifestResponse = await workerExports.default.fetch(`${snapshotBase}/manifest`, {
      headers: snapshotHeaders,
    });
    expect(manifestResponse.status).toBe(200);
    const manifest = (await manifestResponse.json()) as SnapshotManifest;
    expect(manifest).toMatchObject({
      version: 1,
      repositoryId: seeded.repositoryId,
      commitSha: body.acceptedWrite.afterSha,
      treeSha: body.treeSha,
    });
    expect(manifest.files.map((file) => file.path)).toEqual(["index.html", "nested/hello.txt"]);
    const servedBlob = await workerExports.default.fetch(
      `${snapshotBase}/file?path=${encodeURIComponent("nested/hello.txt")}`,
      { headers: snapshotHeaders }
    );
    expect(servedBlob.status).toBe(200);
    expect(await servedBlob.text()).toBe("hello from ingestion\n");
    expect(
      (
        await workerExports.default.fetch(`${snapshotBase}/manifest`, {
          headers: { Authorization: "Bearer wrong-token" },
        })
      ).status
    ).toBe(401);

    const catalogBeforeReplay = await callStubWithRetry(
      () => stub,
      (repoStub) => repoStub.getActivePackCatalog()
    );
    await env.REPO_BUCKET.delete(
      `test-snapshots/${encodeURIComponent(seeded.repositoryId)}/${body.acceptedWrite.afterSha}/manifest.json`
    );
    const replay = await postIngestion(owner, repo, ingestionForm());
    expect(replay.status).toBe(200);
    const replayBody = (await replay.json()) as IngestionResponse;
    expect(replayBody).toEqual({ ...body, replayed: true });
    expect(
      (
        await workerExports.default.fetch(`${snapshotBase}/manifest`, {
          headers: snapshotHeaders,
        })
      ).status
    ).toBe(200);
    const catalogAfterReplay = await callStubWithRetry(
      () => stub,
      (repoStub) => repoStub.getActivePackCatalog()
    );
    expect(catalogAfterReplay).toEqual(catalogBeforeReplay);

    const mismatchedReuse = await postIngestion(
      owner,
      repo,
      ingestionForm({ content: "different\n" })
    );
    expect(mismatchedReuse.status).toBe(409);

    const differentCaller = await postIngestion(
      owner,
      repo,
      ingestionForm({ actor: "another-caller", idempotencyKey: "publish-request-2" })
    );
    expect(differentCaller.status).toBe(409);
    expect(
      await callStubWithRetry(
        () => stub,
        (repoStub) => repoStub.getActivePackCatalog()
      )
    ).toEqual(catalogBeforeReplay);
  });

  it("reconciles a committed finalize when its RPC response is lost", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("finalize-response-loss");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    receivePipelineTest.failNextFinalizeResponse();

    const response = await postIngestion(owner, repo, ingestionForm()).finally(() =>
      receivePipelineTest.reset()
    );
    expect(response.status).toBe(201);
    const body = (await response.json()) as IngestionResponse;
    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    const catalog = await callStubWithRetry(
      () => stub,
      (repoStub) => repoStub.getActivePackCatalog()
    );
    expect(catalog).toHaveLength(1);
    expect(
      await readObject(
        env,
        seeded.doName,
        body.acceptedWrite.afterSha,
        createTestCacheContext("https://example.com/finalize-response-loss")
      )
    ).toMatchObject({ type: "commit" });

    const replay = await postIngestion(owner, repo, ingestionForm());
    expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual({ ...body, replayed: true });
    expect(
      await callStubWithRetry(
        () => stub,
        (repoStub) => repoStub.getActivePackCatalog()
      )
    ).toEqual(catalog);
  });

  it("backfills the key-bound receipt when outcome storage commits first", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("receipt-backfill");
    await setupRepoForTests(env, owner, repo, { doName: `repo:${owner}-${repo}` });
    receiveCatalogTest.skipNextReceiptStore();
    receivePipelineTest.failNextFinalizeResponse();

    const response = await postIngestion(owner, repo, ingestionForm()).finally(() => {
      receiveCatalogTest.reset();
      receivePipelineTest.reset();
    });
    expect(response.status).toBe(201);
    const body = (await response.json()) as IngestionResponse;

    const replay = await postIngestion(owner, repo, ingestionForm());
    expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual({ ...body, replayed: true });
  });

  it("clears the matching lease when recovery observes a stored outcome", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("post-outcome-recovery");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    receiveCatalogTest.failNextAfterOutcomeStore();

    const response = await postIngestion(owner, repo, ingestionForm()).finally(() => {
      receiveCatalogTest.reset();
    });
    expect(response.status).toBe(201);

    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    const nextReceive = await callStubWithRetry<BeginReceiveResult>(
      () => stub,
      async (repoStub) => await repoStub.beginReceive()
    );
    expect(nextReceive.ok).toBe(true);
    if (nextReceive.ok) {
      await callStubWithRetry(
        () => stub,
        async (repoStub) => await repoStub.abortReceive(nextReceive.lease.token)
      );
    }
  });

  it("atomically rolls back an outcome record when its index update fails", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("outcome-index-rollback");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    receiveCatalogTest.failNextOutcomeIndexStore();

    const response = await postIngestion(owner, repo, ingestionForm()).finally(() => {
      receiveCatalogTest.reset();
    });
    expect(response.status).toBe(201);
    const body = (await response.json()) as IngestionResponse;

    const replay = await postIngestion(owner, repo, ingestionForm());
    expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual({ ...body, replayed: true });
    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    expect(
      await callStubWithRetry(
        () => stub,
        (repoStub) => repoStub.getActivePackCatalog()
      )
    ).toHaveLength(1);
  });

  it("prunes individually keyed receive outcomes at the bounded index", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("outcome-pruning");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    let currentOid = zeroOid();
    let first:
      | { token: string; commands: Array<{ oldOid: string; newOid: string; ref: string }> }
      | undefined;
    let last = first;

    for (let index = 1; index <= 129; index++) {
      const begin = await callStubWithRetry<BeginReceiveResult>(
        () => stub,
        async (repoStub) => await repoStub.beginReceive()
      );
      expect(begin.ok).toBe(true);
      if (!begin.ok) throw new Error("expected receive lease");
      const nextOid = index.toString(16).padStart(40, "0");
      const commands = [{ oldOid: currentOid, newOid: nextOid, ref: "refs/heads/main" }];
      const finalized = await callStubWithRetry<FinalizeReceiveResult>(
        () => stub,
        async (repoStub) =>
          await repoStub.finalizeReceive({
            token: begin.lease.token,
            commands,
          })
      );
      expect(finalized.status).toBe("committed");
      const operation = { token: begin.lease.token, commands };
      first ??= operation;
      last = operation;
      currentOid = nextOid;
    }

    expect(
      await callStubWithRetry<ReconcileReceiveResult>(
        () => stub,
        async (repoStub) => await repoStub.reconcileReceive(first!)
      )
    ).toEqual({ status: "unknown" });
    expect(
      await callStubWithRetry<ReconcileReceiveResult>(
        () => stub,
        async (repoStub) => await repoStub.reconcileReceive(last!)
      )
    ).toMatchObject({ status: "committed" });
  });

  it("rejects a stale expected ref without changing main", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("conflict");
    const seeded = await setupRepoForTests(env, owner, repo);
    const accepted = await postIngestion(owner, repo, ingestionForm());
    expect(accepted.status).toBe(201);
    const acceptedBody = (await accepted.json()) as IngestionResponse;

    const conflict = await postIngestion(
      owner,
      repo,
      ingestionForm({ committedAtSeconds: 1_700_000_001, content: "changed\n" })
    );
    expect(conflict.status).toBe(409);

    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    const state = await callStubWithRetry(
      () => stub,
      (repoStub) => repoStub.getHeadAndRefs()
    );
    expect(state.refs).toContainEqual({
      name: "refs/heads/main",
      oid: acceptedBody.acceptedWrite.afterSha,
    });
  });

  it("authenticates before lookup and rejects unsafe paths before mutation", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("guards");
    const seeded = await setupRepoForTests(env, owner, repo);

    const unauthorized = await postIngestion(owner, repo, ingestionForm(), "wrong-token");
    expect(unauthorized.status).toBe(401);

    const invalidPath = await postIngestion(owner, repo, ingestionForm({ path: "../escape.txt" }));
    expect(invalidPath.status).toBe(400);

    const gitMetadataPath = await postIngestion(
      owner,
      repo,
      ingestionForm({ path: "nested/.GiT/config" })
    );
    expect(gitMetadataPath.status).toBe(400);

    const conflictingPaths = ingestionForm();
    conflictingPaths.append("files", new Blob(["conflict\n"]), "nested");
    const pathConflict = await postIngestion(owner, repo, conflictingPaths);
    expect(pathConflict.status).toBe(400);

    const deeplyNested = await postIngestion(
      owner,
      repo,
      ingestionForm({ path: `${Array.from({ length: 129 }, () => "a").join("/")}.txt` })
    );
    expect(deeplyNested.status).toBe(400);

    const overlongPath = await postIngestion(
      owner,
      repo,
      ingestionForm({ path: `${"a".repeat(4097)}.txt` })
    );
    expect(overlongPath.status).toBe(400);

    const oversized = await workerExports.default.fetch(
      `https://example.com/_internal/ingestion/${owner}/${repo}`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}`,
          "Content-Type": "multipart/form-data; boundary=oversized",
          "Content-Length": String(12 * 1024 * 1024),
        },
        body: "--oversized--\r\n",
      }
    );
    expect(oversized.status).toBe(413);

    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    const state = await callStubWithRetry(
      () => stub,
      (repoStub) => repoStub.getHeadAndRefs()
    );
    expect(state.refs).toEqual([]);
    expect(
      await callStubWithRetry(
        () => stub,
        (repoStub) => repoStub.getActivePackCatalog()
      )
    ).toEqual([]);
  });

  it("is hidden when the internal token is not configured", async () => {
    await withEnvOverrides(env, { INGESTION_RPC_TOKEN: "" }, async () => {
      const response = await postIngestion("missing", "missing", ingestionForm(), "anything");
      expect(response.status).toBe(404);
    });
  });

  it("keeps snapshot reads hidden when the benchmark prefix is not configured", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("snapshot-disabled");
    await setupRepoForTests(env, owner, repo);
    await withEnvOverrides(env, { SNAPSHOT_BENCHMARK_PREFIX: "" }, async () => {
      const response = await workerExports.default.fetch(
        `https://example.com/_internal/snapshots/${owner}/${repo}/${"a".repeat(40)}/manifest`,
        { headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` } }
      );
      expect(response.status).toBe(404);
    });
  });

  it("serves materialized binary bytes without transformation", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("snapshot-binary");
    await setupRepoForTests(env, owner, repo);
    const bytes = new Uint8Array(250_000);
    for (let index = 0; index < bytes.length; index++) {
      bytes[index] = index % 256;
    }
    const form = new FormData();
    form.set("expectedOid", zeroOid());
    form.set("actor", "binary-test");
    form.set("idempotencyKey", "binary-snapshot-request");
    form.set("committedAtSeconds", "1700000000");
    form.append("files", new Blob([bytes]), "assets/file.bin");
    const response = await postIngestion(owner, repo, form);
    expect(response.status).toBe(201);
    const body = (await response.json()) as IngestionResponse;
    const served = await workerExports.default.fetch(
      `https://example.com/_internal/snapshots/${owner}/${repo}/${body.acceptedWrite.afterSha}/file?path=${encodeURIComponent("assets/file.bin")}`,
      { headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` } }
    );
    expect(served.status).toBe(200);
    expect(new Uint8Array(await served.arrayBuffer())).toEqual(bytes);
  });
});
