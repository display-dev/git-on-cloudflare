import { describe, expect, it, vi } from "vitest";
import { createExecutionContext } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";

import { asBufferSource, bytesToHex, createLogger, zeroOid } from "@/worker/common";
import { encodeGitObject, parseCommitRefs } from "@/worker/git/core";
import { readObject } from "@/worker/git/object-store";
import { parseTree } from "@/worker/git/operations/read/objects";
import { getLimiter } from "@/worker/git/operations/limits";
import { RepoDurableObject } from "@/worker/do/repo/repoDO";
import {
  materializeAcceptedWrite,
  snapshotBundleObjectKey,
  snapshotObjectKey,
} from "@/worker/git/snapshot/materialize";
import { __test as receivePipelineTest } from "@/worker/git/receive/pipeline";
import { __test as receiveCatalogTest } from "@/worker/do/repo/catalog/receive";
import { __test as readBenchmarkTest } from "@/worker/routes/readBenchmark";
import type {
  FinalizeReceiveResult,
  ReconcileReceiveResult,
} from "@/worker/do/repo/catalog/receive";
import type { BeginReceiveResult } from "@/worker/do/repo/catalog/shared";

import { createTestCacheContext } from "./util/pack-first";
import { buildPack } from "./util/git-pack";
import { buildTreePayload, seedPackedRepoState } from "./util/packed-repo";
import { setupRepoForTests } from "./util/repoSeed";
import { callStubWithRetry, uniqueRepoId, withEnvOverrides } from "./util/test-helpers";

type IngestionResponse = {
  acceptedWrite: {
    repositoryId: string;
    ref: string;
    beforeSha: string;
    afterSha: string;
    actor: string;
    sourceSurface: "ingestion";
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
  files: Array<{ path: string; bytes: number; sha256: string; offset?: number }>;
  bundle?: { bytes: number; sha256: string };
};

type ReadBenchmarkResponse = {
  operation: string;
  operationMs: number;
  fileCount?: number;
  bytes?: number;
  sha256?: string;
  totalBytes?: number;
  matches?: number;
  digest?: string;
  commitCount?: number;
  total?: number;
  truncated?: boolean;
  head?: string;
  activePackCount?: number;
  packCatalogVersion?: number;
  compactionQueued?: boolean;
  compactionRunning?: boolean;
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

function multiFileIngestionForm(fileCount: number): FormData {
  const form = new FormData();
  form.set("expectedOid", zeroOid());
  form.set("actor", "snapshot-batch-test");
  form.set("idempotencyKey", `snapshot-batch-${fileCount}`);
  form.set("committedAtSeconds", "1700000000");
  form.set("message", "Publish batched snapshot");
  for (let index = 0; index < fileCount; index += 1) {
    form.append(
      "files",
      new Blob([`snapshot file ${index}\n`]),
      `file-${String(index).padStart(2, "0")}.txt`
    );
  }
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
  it("publishes a 20-file snapshot as one indexed bundle before its manifest", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("snapshot-write-batches");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const snapshotPrefix = `test-snapshots/${encodeURIComponent(seeded.repositoryId)}/`;
    const put = env.REPO_BUCKET.put.bind(env.REPO_BUCKET);
    const snapshotWrites: string[] = [];
    const putSpy = vi
      .spyOn(env.REPO_BUCKET, "put")
      .mockImplementation(async (...args: Parameters<typeof put>) => {
        const key = String(args[0]);
        if (key.startsWith(snapshotPrefix)) snapshotWrites.push(key);
        return await put(...args);
      });

    const response = await postIngestion(owner, repo, multiFileIngestionForm(20)).finally(() =>
      putSpy.mockRestore()
    );
    expect(response.status).toBe(201);
    const body = (await response.json()) as IngestionResponse;
    expect(snapshotWrites.map((key) => key.slice(snapshotPrefix.length))).toEqual([
      `${body.acceptedWrite.afterSha}/bundle.bin`,
      `${body.acceptedWrite.afterSha}/manifest.json`,
    ]);
    const manifestKey = snapshotObjectKey({
      env,
      repositoryId: seeded.repositoryId,
      commitSha: body.acceptedWrite.afterSha,
    });
    expect(manifestKey).not.toBeNull();
    const manifest = await env.REPO_BUCKET.get(manifestKey!);
    const parsedManifest = (await manifest!.json()) as SnapshotManifest;
    expect(parsedManifest.files).toHaveLength(20);
    expect(parsedManifest.bundle?.bytes).toBe(330);
    expect(parsedManifest.files.map((file) => file.offset)).toEqual(
      parsedManifest.files.map((_, index) => index * 16 + Math.max(0, index - 10))
    );
    const served = await workerExports.default.fetch(
      `https://example.com/_internal/snapshots/${owner}/${repo}/${body.acceptedWrite.afterSha}/file?path=file-19.txt`,
      { headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` } }
    );
    expect(await served.text()).toBe("snapshot file 19\n");
  });

  it("serves an all-empty bundled snapshot without an invalid R2 range", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("empty-snapshot-bundle");
    await setupRepoForTests(env, owner, repo, { doName: `repo:${owner}-${repo}` });
    const form = new FormData();
    form.set("expectedOid", zeroOid());
    form.set("actor", "snapshot-empty-test");
    form.set("idempotencyKey", "snapshot-empty");
    form.set("committedAtSeconds", "1700000000");
    form.set("message", "Publish empty snapshot");
    form.append("files", new Blob([]), "empty.txt");

    const response = await postIngestion(owner, repo, form);
    expect(response.status).toBe(201);
    const body = (await response.json()) as IngestionResponse;
    const headers = { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` };
    const snapshotBase = `https://example.com/_internal/snapshots/${owner}/${repo}/${body.acceptedWrite.afterSha}`;
    const served = await workerExports.default.fetch(`${snapshotBase}/file?path=empty.txt`, {
      headers,
    });
    expect(served.status).toBe(200);
    expect(served.headers.get("Content-Length")).toBe("0");
    expect(new Uint8Array(await served.arrayBuffer())).toHaveLength(0);

    const benchmark = await workerExports.default.fetch(
      `https://example.com/_internal/read-benchmark/${owner}/${repo}/${body.acceptedWrite.afterSha}/snapshot-blob?path=empty.txt`,
      { headers }
    );
    expect(benchmark.status).toBe(200);
    expect(await benchmark.json()).toMatchObject({
      operation: "snapshot-blob",
      bytes: 0,
      sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    });
  });

  it("keeps legacy per-file snapshots readable through both consumers", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("legacy-snapshot-layout");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const commitSha = "a".repeat(40);
    const treeSha = "b".repeat(40);
    const path = "legacy.txt";
    const bytes = new TextEncoder().encode("legacy snapshot\n");
    const sha256 = bytesToHex(
      new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes)))
    );
    const manifestKey = snapshotObjectKey({
      env,
      repositoryId: seeded.repositoryId,
      commitSha,
    })!;
    const fileKey = snapshotObjectKey({
      env,
      repositoryId: seeded.repositoryId,
      commitSha,
      path,
    })!;
    await env.REPO_BUCKET.put(fileKey, bytes);
    await env.REPO_BUCKET.put(
      manifestKey,
      JSON.stringify({
        version: 1,
        repositoryId: seeded.repositoryId,
        commitSha,
        treeSha,
        files: [{ path, bytes: bytes.byteLength, sha256 }],
      })
    );
    const headers = { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` };
    const snapshotBase = `https://example.com/_internal/snapshots/${owner}/${repo}/${commitSha}`;
    const served = await workerExports.default.fetch(`${snapshotBase}/file?path=${path}`, {
      headers,
    });
    expect(served.status).toBe(200);
    expect(await served.text()).toBe("legacy snapshot\n");

    const benchmark = await workerExports.default.fetch(
      `https://example.com/_internal/read-benchmark/${owner}/${repo}/${commitSha}/snapshot-blob?path=${path}`,
      { headers }
    );
    expect(benchmark.status).toBe(200);
    expect(await benchmark.json()).toMatchObject({
      operation: "snapshot-blob",
      bytes: bytes.byteLength,
      sha256,
    });
  });

  it("binds the ingestion source fast path to the committed tree", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("snapshot-source-binding");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const response = await postIngestion(owner, repo, ingestionForm());
    expect(response.status).toBe(201);
    const body = (await response.json()) as IngestionResponse;
    const prefix = `source-binding-${repo}-snapshots`;
    const source = {
      treeSha: body.treeSha,
      files: [
        { path: "nested/hello.txt", bytes: new TextEncoder().encode("hello from ingestion\n") },
        { path: "index.html", bytes: new TextEncoder().encode("<h1>index</h1>\n") },
      ],
    };
    const materialize = async (treeSha: string) =>
      await materializeAcceptedWrite({
        env,
        repoId: seeded.doName,
        fact: body.acceptedWrite,
        request: new Request("https://example.com/source-binding"),
        ctx: createExecutionContext(),
        limiter: getLimiter(),
        log: createLogger(env.LOG_LEVEL, { service: "SnapshotSourceBindingTest" }),
        source: { ...source, treeSha },
      });

    await withEnvOverrides(env, { SNAPSHOT_BENCHMARK_PREFIX: prefix }, async () => {
      await expect(materialize("f".repeat(40))).rejects.toThrow(
        "Snapshot source does not match the committed tree"
      );
      expect((await env.REPO_BUCKET.list({ prefix: `${prefix}/` })).objects).toHaveLength(0);

      await expect(materialize(source.treeSha)).resolves.toMatchObject({ treeSha: source.treeSha });
      const bundleKey = snapshotBundleObjectKey({
        env,
        repositoryId: seeded.repositoryId,
        commitSha: body.acceptedWrite.afterSha,
      })!;
      const bundle = await env.REPO_BUCKET.get(bundleKey);
      expect(await bundle!.text()).toBe("<h1>index</h1>\nhello from ingestion\n");
    });
  });

  it("keeps the snapshot lease until a started bundle write settles", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("snapshot-write-failure");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const snapshotPrefix = `test-snapshots/${encodeURIComponent(seeded.repositoryId)}/`;
    const put = env.REPO_BUCKET.put.bind(env.REPO_BUCKET);
    let releaseBundleWrite!: () => void;
    const heldBundleWrite = new Promise<void>((resolve) => {
      releaseBundleWrite = resolve;
    });
    let signalBundleStarted!: () => void;
    const bundleStarted = new Promise<void>((resolve) => {
      signalBundleStarted = resolve;
    });
    const putSpy = vi
      .spyOn(env.REPO_BUCKET, "put")
      .mockImplementation(async (...args: Parameters<typeof put>) => {
        const key = String(args[0]);
        if (!key.startsWith(snapshotPrefix) || !key.endsWith("/bundle.bin")) {
          return await put(...args);
        }
        signalBundleStarted();
        await heldBundleWrite;
        throw new Error("injected snapshot bundle failure");
      });

    const responsePromise = postIngestion(owner, repo, multiFileIngestionForm(5));
    await bundleStarted;
    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    await vi.waitFor(async () => {
      const state = await stub.getHeadAndRefs();
      expect(state.refs.find((ref) => ref.name === "refs/heads/main")?.oid).toMatch(
        /^[0-9a-f]{40}$/
      );
    });
    let probeLeaseToken: string | undefined;
    await vi.waitFor(async () => {
      const probe = await stub.beginReceive();
      expect(probe.ok).toBe(true);
      if (probe.ok) probeLeaseToken = probe.lease.token;
    });
    expect(probeLeaseToken).toBeDefined();
    await stub.abortReceive(probeLeaseToken!);
    const deletion = await stub.beginRepositoryDeletion();
    expect(deletion.ready).toBe(false);
    releaseBundleWrite();
    const response = await responsePromise.finally(() => putSpy.mockRestore());
    expect(response.status).toBe(500);
    await expect(stub.beginRepositoryDeletion()).resolves.toMatchObject({ ready: true });
    const objects = await env.REPO_BUCKET.list({ prefix: snapshotPrefix });
    expect(objects.objects.some((object) => object.key.endsWith("/manifest.json"))).toBe(false);
  });

  it("keeps an unpublished staged bundle hidden when accepted-pack publication fails", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("snapshot-stage-cleanup");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const snapshotPrefix = `test-snapshots/${encodeURIComponent(seeded.repositoryId)}/`;
    const put = env.REPO_BUCKET.put.bind(env.REPO_BUCKET);
    let bundleWritten = false;
    const putSpy = vi
      .spyOn(env.REPO_BUCKET, "put")
      .mockImplementation(async (...args: Parameters<typeof put>) => {
        const key = String(args[0]);
        if (key.startsWith(snapshotPrefix) && key.endsWith("/bundle.bin")) {
          const result = await put(...args);
          bundleWritten = true;
          return result;
        }
        if (key.includes("/objects/pack/") && key.endsWith(".pack")) {
          await new Response(args[1] as BodyInit).arrayBuffer();
          throw new Error("injected accepted-pack write failure");
        }
        return await put(...args);
      });

    const response = await postIngestion(owner, repo, multiFileIngestionForm(5)).finally(() =>
      putSpy.mockRestore()
    );
    expect(response.status).toBe(500);
    expect(bundleWritten).toBe(true);
    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    await expect(stub.beginRepositoryDeletion()).resolves.toMatchObject({ ready: true });
    const objects = await env.REPO_BUCKET.list({ prefix: snapshotPrefix });
    expect(objects.objects.map((object) => object.key)).toHaveLength(1);
    expect(objects.objects[0]!.key).toMatch(/\/bundle\.bin$/);
  });

  it("repairs a post-commit manifest failure through the durable receipt replay", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("snapshot-replay-repair");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const snapshotPrefix = `test-snapshots/${encodeURIComponent(seeded.repositoryId)}/`;
    const put = env.REPO_BUCKET.put.bind(env.REPO_BUCKET);
    let failManifest = true;
    const putSpy = vi
      .spyOn(env.REPO_BUCKET, "put")
      .mockImplementation(async (...args: Parameters<typeof put>) => {
        const key = String(args[0]);
        if (failManifest && key.startsWith(snapshotPrefix) && key.endsWith("/manifest.json")) {
          failManifest = false;
          throw new Error("injected snapshot manifest failure");
        }
        return await put(...args);
      });

    const failed = await postIngestion(owner, repo, ingestionForm());
    expect(failed.status).toBe(500);
    const hidden = await env.REPO_BUCKET.list({ prefix: snapshotPrefix });
    expect(hidden.objects.some((object) => object.key.endsWith("/bundle.bin"))).toBe(true);
    expect(hidden.objects.some((object) => object.key.endsWith("/manifest.json"))).toBe(false);

    const replay = await postIngestion(owner, repo, ingestionForm()).finally(() =>
      putSpy.mockRestore()
    );
    expect(replay.status).toBe(200);
    const body = (await replay.json()) as IngestionResponse;
    expect(body.replayed).toBe(true);
    const manifestKey = snapshotObjectKey({
      env,
      repositoryId: seeded.repositoryId,
      commitSha: body.acceptedWrite.afterSha,
    })!;
    await expect(env.REPO_BUCKET.head(manifestKey)).resolves.not.toBeNull();
    const served = await workerExports.default.fetch(
      `https://example.com/_internal/snapshots/${owner}/${repo}/${body.acceptedWrite.afterSha}/file?path=nested%2Fhello.txt`,
      { headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` } }
    );
    expect(served.status).toBe(200);
    expect(await served.text()).toBe("hello from ingestion\n");
  });

  it("returns the committed result when snapshot lease release fails after publication", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("snapshot-finish-failure");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const finishSpy = vi
      .spyOn(RepoDurableObject.prototype, "finishSnapshotMaterialization")
      .mockRejectedValueOnce(new Error("injected snapshot lease finish failure"));

    const response = await postIngestion(owner, repo, ingestionForm()).finally(() =>
      finishSpy.mockRestore()
    );
    expect(response.status).toBe(201);
    const body = (await response.json()) as IngestionResponse;
    const manifestKey = snapshotObjectKey({
      env,
      repositoryId: seeded.repositoryId,
      commitSha: body.acceptedWrite.afterSha,
    })!;
    await expect(env.REPO_BUCKET.head(manifestKey)).resolves.not.toBeNull();
  });

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

    const benchmarkBase = `https://example.com/_internal/read-benchmark/${owner}/${repo}/${body.acceptedWrite.afterSha}`;
    const runBenchmark = async (operation: string, query = "") => {
      const response = await workerExports.default.fetch(`${benchmarkBase}/${operation}${query}`, {
        headers: snapshotHeaders,
      });
      expect(response.status).toBe(200);
      return (await response.json()) as ReadBenchmarkResponse;
    };
    const treeBenchmark = await runBenchmark("tree", "?cold=1");
    expect(treeBenchmark).toMatchObject({ operation: "tree", fileCount: 2 });
    expect(treeBenchmark.operationMs).toBeGreaterThanOrEqual(0);
    const stateBenchmark = await runBenchmark("state");
    expect(stateBenchmark).toMatchObject({
      operation: "state",
      head: body.acceptedWrite.afterSha,
      activePackCount: 1,
      compactionQueued: false,
      compactionRunning: false,
    });
    expect(stateBenchmark.packCatalogVersion).toBeGreaterThan(0);
    const blobBenchmark = await runBenchmark(
      "blob",
      `?path=${encodeURIComponent("nested/hello.txt")}&cold=1`
    );
    expect(blobBenchmark).toMatchObject({
      operation: "blob",
      bytes: 21,
    });
    for (const invalidPath of [
      "/nested/hello.txt",
      "nested//hello.txt",
      "nested/hello.txt/",
      "nested/../hello.txt",
    ]) {
      const invalid = await workerExports.default.fetch(
        `${benchmarkBase}/blob?path=${encodeURIComponent(invalidPath)}&cold=1`,
        { headers: snapshotHeaders }
      );
      expect(invalid.status).toBe(422);
    }
    const logBenchmark = await runBenchmark("log", "?limit=20&cold=1");
    expect(logBenchmark).toMatchObject({ operation: "log", commitCount: 1 });
    const compareBenchmark = await runBenchmark("compare", "?cold=1");
    expect(compareBenchmark).toMatchObject({
      operation: "compare",
      total: 2,
      truncated: false,
    });
    const directSearch = await runBenchmark(
      "search",
      `?needle=${encodeURIComponent("hello")}&cold=1`
    );
    const snapshotTree = await runBenchmark("snapshot-tree", "?cold=1");
    expect(snapshotTree).toMatchObject({ operation: "snapshot-tree", fileCount: 2 });
    const snapshotBlob = await runBenchmark(
      "snapshot-blob",
      `?path=${encodeURIComponent("nested/hello.txt")}&needle=${encodeURIComponent("hello")}`
    );
    expect(snapshotBlob).toMatchObject({
      operation: "snapshot-blob",
      bytes: 21,
      sha256: blobBenchmark.sha256,
    });
    const snapshotSearch = await runBenchmark(
      "snapshot-search",
      `?needle=${encodeURIComponent("hello")}&cold=1`
    );
    expect(snapshotSearch).toMatchObject({
      operation: "snapshot-search",
      fileCount: 2,
      matches: 1,
      digest: directSearch.digest,
    });

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

  it("accepts exactly one of two same-base ingestion requests and rejects the loser", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("concurrent-conflict");
    const seeded = await setupRepoForTests(env, owner, repo);
    const candidates = [
      {
        committedAtSeconds: 1_700_000_010,
        content: "candidate a\n",
        idempotencyKey: "concurrent-a",
      },
      {
        committedAtSeconds: 1_700_000_011,
        content: "candidate b\n",
        idempotencyKey: "concurrent-b",
      },
    ];
    const responses = await Promise.all(
      candidates.map(
        async (candidate) => await postIngestion(owner, repo, ingestionForm(candidate))
      )
    );
    const acceptedIndex = responses.findIndex((response) => response.status === 201);
    expect(acceptedIndex).toBeGreaterThanOrEqual(0);
    expect(responses.filter((response) => response.status === 201)).toHaveLength(1);
    const rejectedIndex = acceptedIndex === 0 ? 1 : 0;
    expect([409, 503]).toContain(responses[rejectedIndex]!.status);

    const rejectedRetry = await postIngestion(
      owner,
      repo,
      ingestionForm(candidates[rejectedIndex]!)
    );
    expect(rejectedRetry.status).toBe(409);

    const accepted = (await responses[acceptedIndex]!.json()) as IngestionResponse;
    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    const state = await callStubWithRetry(
      () => stub,
      (repoStub) => repoStub.getHeadAndRefs()
    );
    expect(state.refs).toContainEqual({
      name: "refs/heads/main",
      oid: accepted.acceptedWrite.afterSha,
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
      const benchmark = await workerExports.default.fetch(
        `https://example.com/_internal/read-benchmark/${owner}/${repo}/${"a".repeat(40)}/tree`,
        { headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` } }
      );
      expect(benchmark.status).toBe(404);
      const anonymousBenchmark = await workerExports.default.fetch(
        `https://example.com/_internal/read-benchmark/${owner}/${repo}/${"a".repeat(40)}/tree`
      );
      expect(anonymousBenchmark.status).toBe(404);
      const wrongTokenBenchmark = await workerExports.default.fetch(
        `https://example.com/_internal/read-benchmark/${owner}/${repo}/${"a".repeat(40)}/tree`,
        { headers: { Authorization: "Bearer wrong-token" } }
      );
      expect(wrongTokenBenchmark.status).toBe(404);
    });
  });

  it("benchmarks direct and snapshot blobs above the UI preview cap equivalently", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("large-read-benchmark");
    await setupRepoForTests(env, owner, repo);
    const bytes = new Uint8Array(8 * 1024 * 1024);
    for (let index = 0; index < bytes.length; index++) bytes[index] = index % 251;
    const form = new FormData();
    form.set("expectedOid", zeroOid());
    form.set("actor", "large-read-test");
    form.set("idempotencyKey", "large-read-request");
    form.set("committedAtSeconds", "1700000000");
    form.append("files", new Blob([bytes]), "large.bin");
    const ingested = await postIngestion(owner, repo, form);
    expect(ingested.status).toBe(201);
    const body = (await ingested.json()) as IngestionResponse;
    const base = `https://example.com/_internal/read-benchmark/${owner}/${repo}/${body.acceptedWrite.afterSha}`;
    const headers = { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` };
    const directResponse = await workerExports.default.fetch(
      `${base}/blob?path=${encodeURIComponent("large.bin")}&cold=1`,
      { headers }
    );
    const snapshotResponse = await workerExports.default.fetch(
      `${base}/snapshot-blob?path=${encodeURIComponent("large.bin")}&cold=1`,
      { headers }
    );
    expect(directResponse.status).toBe(200);
    expect(snapshotResponse.status).toBe(200);
    const direct = (await directResponse.json()) as ReadBenchmarkResponse;
    const snapshot = (await snapshotResponse.json()) as ReadBenchmarkResponse;
    expect(direct).toMatchObject({ operation: "blob", bytes: bytes.byteLength });
    expect(snapshot).toMatchObject({
      operation: "snapshot-blob",
      bytes: bytes.byteLength,
      sha256: direct.sha256,
    });
  });

  it("rejects a snapshot manifest beyond the bounded benchmark file count", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("read-budget-bound");
    const seeded = await setupRepoForTests(env, owner, repo);
    const commitSha = "b".repeat(40);
    const manifest = {
      version: 1,
      repositoryId: seeded.repositoryId,
      commitSha,
      treeSha: "c".repeat(40),
      files: Array.from({ length: 101 }, (_, index) => ({
        path: `file-${index}.txt`,
        bytes: 1,
        sha256: "d".repeat(64),
      })),
    };
    await env.REPO_BUCKET.put(
      `test-snapshots/${encodeURIComponent(seeded.repositoryId)}/${commitSha}/manifest.json`,
      JSON.stringify(manifest)
    );
    const response = await workerExports.default.fetch(
      `https://example.com/_internal/read-benchmark/${owner}/${repo}/${commitSha}/snapshot-search`,
      { headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` } }
    );
    expect(response.status).toBe(422);
    expect(await response.json()).toEqual({ error: "Invalid snapshot manifest" });
  });

  it("admits exactly 1,000 metadata-only paths only through the scale snapshot seam", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("scale-snapshot-bound");
    const seeded = await setupRepoForTests(env, owner, repo);
    const commitSha = "9".repeat(40);
    const files = Array.from({ length: 1_000 }, (_, index) => ({
      path: `files/file-${String(index).padStart(4, "0")}.bin`,
      bytes: 5_000_000,
      sha256: "8".repeat(64),
    }));
    await env.REPO_BUCKET.put(
      `test-snapshots/${encodeURIComponent(seeded.repositoryId)}/${commitSha}/manifest.json`,
      JSON.stringify({
        version: 1,
        repositoryId: seeded.repositoryId,
        commitSha,
        treeSha: "7".repeat(40),
        files,
      })
    );
    const headers = { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` };
    const ordinary = await workerExports.default.fetch(
      `https://example.com/_internal/read-benchmark/${owner}/${repo}/${commitSha}/snapshot-tree`,
      { headers }
    );
    expect(ordinary.status).toBe(422);
    const scale = await workerExports.default.fetch(
      `https://example.com/_internal/read-benchmark/${owner}/${repo}/${commitSha}/scale-snapshot-tree`,
      { headers }
    );
    expect(scale.status).toBe(200);
    expect(await scale.json()).toMatchObject({
      operation: "scale-snapshot-tree",
      fileCount: 1_000,
      totalBytes: 5_000_000_000,
    });

    await env.REPO_BUCKET.put(
      `test-snapshots/${encodeURIComponent(seeded.repositoryId)}/${commitSha}/manifest.json`,
      JSON.stringify({
        version: 1,
        repositoryId: seeded.repositoryId,
        commitSha,
        treeSha: "7".repeat(40),
        files: [...files, { path: "files/file-1000.bin", bytes: 1, sha256: "8".repeat(64) }],
      })
    );
    const tooMany = await workerExports.default.fetch(
      `https://example.com/_internal/read-benchmark/${owner}/${repo}/${commitSha}/scale-snapshot-tree`,
      { headers }
    );
    expect(tooMany.status).toBe(422);

    await env.REPO_BUCKET.put(
      `test-snapshots/${encodeURIComponent(seeded.repositoryId)}/${commitSha}/manifest.json`,
      JSON.stringify({
        version: 1,
        repositoryId: seeded.repositoryId,
        commitSha,
        treeSha: "7".repeat(40),
        files: files.map((file, index) =>
          index === 0 ? { ...file, bytes: file.bytes + 1 } : file
        ),
      })
    );
    const tooLarge = await workerExports.default.fetch(
      `https://example.com/_internal/read-benchmark/${owner}/${repo}/${commitSha}/scale-snapshot-tree`,
      { headers }
    );
    expect(tooLarge.status).toBe(422);
  });

  it("enforces the direct scale-tree 1,000-file boundary", () => {
    expect(() =>
      readBenchmarkTest.assertBenchmarkFileCapacity(
        readBenchmarkTest.maxScaleBenchmarkFiles - 1,
        readBenchmarkTest.maxScaleBenchmarkFiles
      )
    ).not.toThrow();
    expect(() =>
      readBenchmarkTest.assertBenchmarkFileCapacity(
        readBenchmarkTest.maxScaleBenchmarkFiles,
        readBenchmarkTest.maxScaleBenchmarkFiles
      )
    ).toThrow("Benchmark file limit exceeded");
  });

  it("selects the expanded direct tree limit only for the authenticated scale operation", async () => {
    const owner = "ingest";
    const headers = { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` };
    const seedFlatTree = async (fileCount: number) => {
      const repo = uniqueRepoId(`scale-tree-${fileCount}`);
      const seeded = await setupRepoForTests(env, owner, repo);
      const blobPayload = new TextEncoder().encode("scale tree fixture\n");
      const blob = await encodeGitObject("blob", blobPayload);
      const treePayload = buildTreePayload(
        Array.from({ length: fileCount }, (_, index) => ({
          mode: "100644",
          name: `file-${String(index).padStart(4, "0")}.txt`,
          oid: blob.oid,
        }))
      );
      const tree = await encodeGitObject("tree", treePayload);
      const author = "Scale Test <scale@example.invalid> 0 +0000";
      const commitPayload = new TextEncoder().encode(
        `tree ${tree.oid}\nauthor ${author}\ncommitter ${author}\n\nscale tree boundary\n`
      );
      const commit = await encodeGitObject("commit", commitPayload);
      const getStub = () => env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
      await seedPackedRepoState({
        env,
        repoId: seeded.repositoryId,
        getStub,
        packs: [
          {
            name: `pack-scale-tree-${fileCount}.pack`,
            packBytes: await buildPack([
              { type: "blob", payload: blobPayload },
              { type: "tree", payload: treePayload },
              { type: "commit", payload: commitPayload },
            ]),
          },
        ],
        refs: [{ name: "refs/heads/main", oid: commit.oid }],
        head: { target: "refs/heads/main", oid: commit.oid },
      });
      return { repo, commitSha: commit.oid };
    };
    const request = async (repo: string, commitSha: string, operation: string) =>
      await workerExports.default.fetch(
        `https://example.com/_internal/read-benchmark/${owner}/${repo}/${commitSha}/${operation}`,
        { headers }
      );

    const atLimit = await seedFlatTree(1_000);
    const ordinary = await request(atLimit.repo, atLimit.commitSha, "tree");
    expect(ordinary.status).toBe(422);
    const scale = await request(atLimit.repo, atLimit.commitSha, "scale-tree");
    expect(scale.status).toBe(200);
    expect(await scale.json()).toMatchObject({ operation: "scale-tree", fileCount: 1_000 });

    const overLimit = await seedFlatTree(1_001);
    const tooMany = await request(overLimit.repo, overLimit.commitSha, "scale-tree");
    expect(tooMany.status).toBe(422);
  });

  it("rejects snapshot manifests outside canonical path and byte bounds", async () => {
    const owner = "ingest";
    const repo = uniqueRepoId("read-manifest-bounds");
    const seeded = await setupRepoForTests(env, owner, repo);
    const commitSha = "e".repeat(40);
    const invalidFiles = [
      [{ path: `${"a".repeat(4097)}.txt`, bytes: 1, sha256: "f".repeat(64) }],
      [
        {
          path: Array.from({ length: 129 }, () => "segment").join("/"),
          bytes: 1,
          sha256: "f".repeat(64),
        },
      ],
      [{ path: "too-large.bin", bytes: 512 * 1024 * 1024 + 1, sha256: "f".repeat(64) }],
    ];
    for (const files of invalidFiles) {
      await env.REPO_BUCKET.put(
        `test-snapshots/${encodeURIComponent(seeded.repositoryId)}/${commitSha}/manifest.json`,
        JSON.stringify({
          version: 1,
          repositoryId: seeded.repositoryId,
          commitSha,
          treeSha: "a".repeat(40),
          files,
        })
      );
      const response = await workerExports.default.fetch(
        `https://example.com/_internal/read-benchmark/${owner}/${repo}/${commitSha}/snapshot-tree`,
        { headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` } }
      );
      expect(response.status).toBe(422);
      expect(await response.json()).toEqual({ error: "Invalid snapshot manifest" });
    }
  });

  it("enforces the direct-search aggregate byte boundary before inflation", () => {
    expect(readBenchmarkTest.addBenchmarkBytes(504 * 1024 * 1024, 8 * 1024 * 1024)).toBe(
      512 * 1024 * 1024
    );
    expect(() => readBenchmarkTest.addBenchmarkBytes(512 * 1024 * 1024, 1)).toThrow(
      "Benchmark aggregate byte limit exceeded"
    );
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
