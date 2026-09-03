import { createExecutionContext } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";

import type { CacheContext } from "@/worker/cache";
import { createLogger, getRepoStub, zeroOid } from "@/worker/common";
import { __test as packCatalogTest } from "@/worker/do/repo/db/dal/packCatalog";
import { getDb, upsertPackCatalogRow } from "@/worker/do/repo/db";
import { concatChunks, flushPkt, pktLine } from "@/worker/git";
import { encodeGitObject, parseCommitRefs } from "@/worker/git/core";
import {
  __test as reachabilityGcTest,
  runReachabilityGc,
} from "@/worker/git/maintenance/reachabilityGc";
import { readObject, loadIdxView, getOidHexAt } from "@/worker/git/object-store";
import { type Limiter, SubrequestLimiter } from "@/worker/git/operations/limits";
import { stagePackToR2 } from "@/worker/git/receive/r2Upload";
import { doPrefix, packRefsKey, r2PackKey } from "@/worker/keys";
import {
  isReachabilityGcReservedSubrequest,
  ReachabilityGcBudgetExceededError,
  ReachabilityGcSubrequestBudget,
} from "@/worker/tasks/reachabilityGc";

import { compactOnce, deleteSupersededOnce } from "./util/compaction-helpers";
import { createQueueSendResponse, runQueueMessage } from "./util/queue";
import {
  buildAppendOnlyDelta,
  buildPack,
  postReceivePack,
  runDOWithRetry,
  uniqueRepoId,
} from "./util/test-helpers";
import { buildTreePayload, seedPackedRepoState } from "./util/packed-repo";
import { setupRepoForTests } from "./util/repoSeed";

type IngestionResponse = {
  acceptedWrite: { beforeSha: string; afterSha: string };
  treeSha: string;
  replayed: boolean;
};

function ingestionForm(args: {
  expectedOid: string;
  idempotencyKey: string;
  content: string;
  historyMode?: "append" | "epoch";
}): FormData {
  const form = new FormData();
  form.set("expectedOid", args.expectedOid);
  form.set("actor", "maintenance-test");
  form.set("idempotencyKey", args.idempotencyKey);
  form.set("committedAtSeconds", "1700000000");
  form.set("message", args.historyMode === "epoch" ? "Start new epoch" : "Append history");
  if (args.historyMode) form.set("historyMode", args.historyMode);
  form.append("files", new Blob([args.content]), "state.txt");
  return form;
}

async function ingest(
  owner: string,
  repo: string,
  args: Parameters<typeof ingestionForm>[0]
): Promise<IngestionResponse> {
  const response = await workerExports.default.fetch(
    `https://example.com/_internal/ingestion/${owner}/${repo}`,
    {
      method: "POST",
      headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` },
      body: ingestionForm(args),
    }
  );
  expect([200, 201]).toContain(response.status);
  return (await response.json()) as IngestionResponse;
}

async function sha256(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function gcContext(repoId: string): CacheContext {
  return {
    req: new Request(`https://maintenance.internal/${encodeURIComponent(repoId)}`),
    ctx: createExecutionContext(),
    memo: { flags: new Set<string>(), subreqBudget: 900 },
  };
}

async function listGcPackKeys(prefix: string): Promise<string[]> {
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await env.REPO_BUCKET.list({ prefix, cursor });
    keys.push(
      ...page.objects.map((object) => object.key).filter((key) => key.includes("pack-gc-"))
    );
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  return keys;
}

// Rewrite-specific recovery tests must contain no source pack that is already
// the exact closure. Add one unreachable blob to each tiny synthetic pack.
async function requireRewriteFixture(repoId: string): Promise<void> {
  const stub = getRepoStub(env, repoId);
  const packs: Array<{ name: string; packBytes: Uint8Array }> = [];
  const context = gcContext(repoId);
  for (const row of await stub.getActivePackCatalog()) {
    const idx = await loadIdxView(env, row.packKey, context, row.packBytes);
    if (!idx) throw new Error("missing fixture index");
    const objects: Parameters<typeof buildPack>[0] = [];
    for (let index = 0; index < idx.count; index++) {
      const object = await readObject(env, repoId, getOidHexAt(idx, index), context);
      if (!object) throw new Error("missing fixture object");
      objects.push({ type: object.type, payload: object.payload });
    }
    objects.push({
      type: "blob",
      payload: new TextEncoder().encode("unreachable rewrite fixture\n"),
    });
    packs.push({ name: row.packKey.split("/").at(-1)!, packBytes: await buildPack(objects) });
  }
  const id = env.REPO_DO.idFromName(repoId);
  await seedPackedRepoState({ env, repoId, getStub: () => env.REPO_DO.get(id), packs });
}

describe("candidate-native repository maintenance", () => {
  it("reuses an exact-closure pack, reconciles a lost commit, and never deletes the retained source", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-reuse-closure");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const payload = new TextEncoder().encode("reachable\n");
    const blob = await encodeGitObject("blob", payload);
    const packBytes = await buildPack([{ type: "blob", payload }]);
    const id = env.REPO_DO.idFromName(seeded.doName);
    await seedPackedRepoState({
      env,
      repoId: seeded.doName,
      getStub: () => env.REPO_DO.get(id),
      packs: [
        { name: "pack-original.pack", packBytes },
        { name: "pack-duplicate.pack", packBytes },
      ],
      refs: [{ name: "refs/heads/main", oid: blob.oid }],
      head: { target: "refs/heads/main", oid: blob.oid },
    });
    const stub = getRepoStub(env, seeded.doName);
    const before = await stub.getActivePackCatalog();
    const operations: string[] = [];
    const queueSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    reachabilityGcTest.failNextCommitResponse();
    try {
      const result = await runReachabilityGc({
        env,
        repoId: seeded.doName,
        cacheCtx: gcContext(seeded.doName),
        limiter: new SubrequestLimiter(6),
        log: createLogger("error", { service: "ReachabilityGcReuseTest" }),
        countSubrequest: (op) => operations.push(op),
      });
      expect(result.status).toBe("completed");
      const active = await stub.getActivePackCatalog();
      expect(active).toHaveLength(1);
      expect(before).toContainEqual(active[0]);
      expect(operations).toContain("do:reconcile-reachability-gc");
      expect(operations.some((op) => /rewrite-gc|scan-gc|index-gc|multipart/.test(op))).toBe(false);
      const superseded = await stub.listSupersededGcPacks();
      expect(superseded).toHaveLength(1);
      expect(superseded[0]!.packKey).not.toBe(active[0]!.packKey);
      expect(await listGcPackKeys(doPrefix(id.toString()))).toEqual([]);
      const repeated = await runReachabilityGc({
        env,
        repoId: seeded.doName,
        cacheCtx: gcContext(seeded.doName),
        limiter: new SubrequestLimiter(6),
        log: createLogger("error", { service: "ReachabilityGcReuseTest" }),
        countSubrequest: () => {},
      });
      // Existing delayed deletion must reconcile before another GC attempt.
      expect(repeated).toEqual({ status: "retry", reason: "cleanup-scheduled" });
      expect(await stub.getActivePackCatalog()).toEqual(active);
    } finally {
      reachabilityGcTest.reset();
      queueSpy.mockRestore();
    }
  });

  it("reserves cleanup capacity below the configured 10,000-subrequest limit", () => {
    const budget = new ReachabilityGcSubrequestBudget();
    budget.consume(5_000);
    budget.consume(3_900);
    expect(budget.used).toBe(8_900);
    expect(() => budget.consume(1)).toThrow("reachability GC subrequest limit exceeded");
    budget.consume(100, true);
    expect(budget.used).toBe(9_000);
    expect(() => budget.consume(1)).toThrow("reachability GC subrequest limit exceeded");
  });

  it("counts and limits multipart abort during failed staging", async () => {
    const labels: string[] = [];
    const counted: string[] = [];
    const limiter: Limiter = {
      async run<T>(label: string, fn: () => Promise<T>): Promise<T> {
        labels.push(label);
        return await fn();
      },
    };
    const id = env.REPO_DO.idFromName(uniqueRepoId("gc-multipart-abort"));
    const packKey = r2PackKey(doPrefix(id.toString()), "pack-invalid.pack");
    await expect(
      stagePackToR2({
        env,
        request: new Request("https://maintenance.internal/reachability-gc"),
        packStream: new Blob([new Uint8Array(12)]).stream(),
        packKey,
        bytesConsumed: 0,
        limiter,
        countSubrequest: (op) => counted.push(op),
      })
    ).rejects.toThrow("valid PACK header");
    expect(labels).toEqual([
      "r2:create-pack-multipart",
      "r2:abort-pack-multipart",
      "r2:delete-staged-pack",
    ]);
    expect(counted).toEqual(labels);
    expect(await env.REPO_BUCKET.head(packKey)).toBeNull();
  });

  it("pages 334 superseded rows through the DO and cleanup queue", async () => {
    const repoId = `repo:maintenance-${uniqueRepoId("gc-cleanup-pages")}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id);
    await runDOWithRetry(getStub, async (_instance, state) => {
      const db = getDb(state.storage);
      const prefix = doPrefix(state.id.toString());
      for (let index = 0; index < 334; index++) {
        await upsertPackCatalogRow(db, {
          packKey: r2PackKey(prefix, `pack-superseded-${index}.pack`),
          kind: "compact",
          state: "superseded",
          tier: 1,
          seqLo: index + 1,
          seqHi: index + 1,
          objectCount: 1,
          packBytes: 1,
          idxBytes: 1,
          createdAt: index,
          supersededBy: null,
        });
      }
    });
    const stub = getRepoStub(env, repoId);
    expect(await compactOnce(repoId)).toEqual({ acked: true, retried: false });
    const firstPage = await stub.listSupersededGcPacks();
    expect(firstPage).toHaveLength(250);
    expect(
      await deleteSupersededOnce(
        repoId,
        firstPage.map((row) => row.packKey),
        true
      )
    ).toEqual({ acked: true, retried: false });
    const secondPage = await stub.listSupersededGcPacks();
    expect(secondPage).toHaveLength(84);
    expect(
      await deleteSupersededOnce(
        repoId,
        secondPage.map((row) => row.packKey),
        true
      )
    ).toEqual({ acked: true, retried: false });
    expect(await stub.listSupersededGcPacks()).toEqual([]);
  });

  it("preserves the pre-historyMode append receipt fingerprint", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("legacy-append-replay");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const accepted = await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "legacy-append-replay",
      content: "state\n",
    });
    const keyHash = await sha256("legacy-append-replay");
    const receipt = await getRepoStub(env, seeded.doName).getIngestionReceipt(keyHash);
    const legacyFingerprint = await sha256(
      JSON.stringify([
        zeroOid(),
        "maintenance-test",
        1700000000,
        "Append history",
        accepted.acceptedWrite.afterSha,
        accepted.treeSha,
      ])
    );
    expect(receipt?.fingerprint).toBe(legacyFingerprint);

    const replay = await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "legacy-append-replay",
      content: "state\n",
    });
    expect(replay.replayed).toBe(true);
    expect(replay.acceptedWrite).toEqual(accepted.acceptedWrite);
  });

  it("fences receive and direct ref mutation while a reachability snapshot is active", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-write-fence");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const accepted = await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "gc-write-fence",
      content: "state\n",
    });
    const stub = getRepoStub(env, seeded.doName);
    const begin = await stub.beginReachabilityGc();
    expect(begin.ok).toBe(true);
    if (!begin.ok) throw new Error("reachability GC lease was not acquired");
    try {
      expect(await stub.beginReceive()).toMatchObject({ ok: false });
      expect(await stub.setRefs([{ name: "refs/heads/main", oid: "f".repeat(40) }])).toBe(false);
      expect(await stub.listRefs()).toEqual([
        { name: "refs/heads/main", oid: accepted.acceptedWrite.afterSha },
      ]);
    } finally {
      await stub.abortCompaction(begin.lease.token);
    }
  });

  it("enqueues through the authenticated admin route and executes through the real queue", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-queue-entry");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const first = await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "gc-queue-entry-first",
      content: "old\n",
    });
    await ingest(owner, repo, {
      expectedOid: first.acceptedWrite.afterSha,
      idempotencyKey: "gc-queue-entry-epoch",
      content: "new\n",
      historyMode: "epoch",
    });
    const doId = env.REPO_DO.idFromName(seeded.doName).toString();
    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      const response = await workerExports.default.fetch(
        `https://example.com/${owner}/${repo}/admin/reachability-gc`,
        {
          method: "POST",
          headers: { Cookie: seeded.cookieHeader, Origin: "https://example.com" },
        }
      );
      expect(response.status).toBe(202);
      const stub = getRepoStub(env, seeded.doName);
      const operation = await stub.getGcOperation();
      if (!operation) throw new Error("GC admission was not durable");
      expect(await response.json()).toEqual({ status: "queued", operationId: operation.id });
      expect(sendSpy).toHaveBeenCalledWith({
        kind: "reachability-gc",
        doId,
        repoId: seeded.doName,
        operationId: operation.id,
      });
      const message = {
        kind: "reachability-gc",
        doId,
        repoId: seeded.doName,
        operationId: operation.id,
      };
      expect(await runQueueMessage(message)).toEqual({ acked: false, retried: true });
      expect((await stub.getGcOperation())?.phase).toBe("publish");
      expect(await runQueueMessage(message)).toEqual({ acked: false, retried: true });
      expect((await stub.getGcOperation())?.phase).toBe("reclaim");
      expect(await runQueueMessage(message)).toEqual({ acked: false, retried: true });
      expect(await stub.getActivePackCatalog()).toHaveLength(1);
      expect(sendSpy).toHaveBeenCalledWith(
        expect.objectContaining({ kind: "compaction-delete", removeCatalogRows: true }),
        { delaySeconds: 60 }
      );
      const committed = await stub.getGcOperation();
      if (!committed?.commit) throw new Error("GC commit receipt missing");
      expect(
        await deleteSupersededOnce(
          seeded.doName,
          committed.commit.supersededPackKeys,
          true,
          committed.commit.packCatalogVersion
        )
      ).toEqual({ acked: true, retried: false });
      expect(await runQueueMessage(message)).toEqual({ acked: true, retried: false });
      expect((await stub.getGcOperation())?.phase).toBe("complete");
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("does not report an empty active catalog as success when refs remain", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-empty-catalog");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const stub = getRepoStub(env, seeded.doName);
    expect(await stub.setRefs([{ name: "refs/heads/main", oid: "f".repeat(40) }])).toBe(true);
    const result = await runReachabilityGc({
      env,
      repoId: seeded.doName,
      cacheCtx: gcContext(seeded.doName),
      limiter: new SubrequestLimiter(6),
      log: createLogger("error", { service: "ReachabilityGcEmptyCatalogTest" }),
      countSubrequest: () => {},
    });
    expect(result).toEqual({ status: "retry", reason: "active-catalog-empty" });
  });

  it("counts missing-sidecar backfill through the GC limiter", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-missing-sidecar");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "gc-missing-sidecar",
      content: "state\n",
    });
    const active = (await getRepoStub(env, seeded.doName).getActivePackCatalog())[0]!;
    await env.REPO_BUCKET.delete(packRefsKey(active.packKey));
    const counted: string[] = [];
    const queueSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      const result = await runReachabilityGc({
        env,
        repoId: seeded.doName,
        cacheCtx: gcContext(seeded.doName),
        limiter: new SubrequestLimiter(6),
        log: createLogger("error", { service: "ReachabilityGcMissingSidecarTest" }),
        countSubrequest: (op) => counted.push(op),
      });
      expect(result).toEqual({ status: "retry", reason: "missing-ref-index" });
      expect(counted).toContain("queue:reachability-gc-ref-backfill");
      expect(queueSpy).toHaveBeenCalledTimes(1);
      expect(queueSpy).toHaveBeenCalledWith({
        kind: "pack-ref-backfill",
        doId: env.REPO_DO.idFromName(seeded.doName).toString(),
        repoId: seeded.doName,
        packKey: active.packKey,
      });
    } finally {
      queueSpy.mockRestore();
    }
  });

  it("blocks exact GC when a reachable delta needs an unreachable encoding base", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-delta-base");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const basePayload = new TextEncoder().encode("unreachable base\n");
    const suffix = new TextEncoder().encode("reachable suffix\n");
    const childPayload = concatChunks([basePayload, suffix]);
    const child = await encodeGitObject("blob", childPayload);
    const packBytes = await buildPack([
      { type: "blob", payload: basePayload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(basePayload, suffix) },
    ]);
    const id = env.REPO_DO.idFromName(seeded.doName);
    await seedPackedRepoState({
      env,
      repoId: seeded.doName,
      getStub: () => env.REPO_DO.get(id),
      packs: [{ name: "pack-delta-base.pack", packBytes }],
      refs: [{ name: "refs/heads/main", oid: child.oid }],
      head: { target: "refs/heads/main", oid: child.oid },
    });
    const stub = getRepoStub(env, seeded.doName);
    const sourceCatalog = await stub.getActivePackCatalog();
    const queueSpy = vi.spyOn(env.REPO_TASKS_QUEUE, "send");
    try {
      const result = await runReachabilityGc({
        env,
        repoId: seeded.doName,
        cacheCtx: gcContext(seeded.doName),
        limiter: new SubrequestLimiter(6),
        log: createLogger("error", { service: "ReachabilityGcDeltaBaseTest" }),
        countSubrequest: () => {},
      });
      expect(result).toEqual({
        status: "blocked",
        reason: "delta-base-outside-reachability-closure",
      });
      expect(queueSpy).not.toHaveBeenCalled();
    } finally {
      queueSpy.mockRestore();
    }
    expect(await stub.getActivePackCatalog()).toEqual(sourceCatalog);
    expect(await env.REPO_BUCKET.head(sourceCatalog[0]!.packKey)).not.toBeNull();
  });

  it("starts a parentless epoch and physically removes unreachable canonical Git objects", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("epoch-gc");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const first = await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "epoch-gc-first",
      content: "private-old-state\n",
    });
    const second = await ingest(owner, repo, {
      expectedOid: first.acceptedWrite.afterSha,
      idempotencyKey: "epoch-gc-second",
      content: "private-old-state-v2\n",
    });
    const epoch = await ingest(owner, repo, {
      expectedOid: second.acceptedWrite.afterSha,
      idempotencyKey: "epoch-gc-reset",
      content: "new-epoch-state\n",
      historyMode: "epoch",
    });

    const beforeGc = await readObject(
      env,
      seeded.doName,
      second.acceptedWrite.afterSha,
      gcContext(seeded.doName)
    );
    const epochCommit = await readObject(
      env,
      seeded.doName,
      epoch.acceptedWrite.afterSha,
      gcContext(seeded.doName)
    );
    expect(beforeGc?.type).toBe("commit");
    expect(parseCommitRefs(epochCommit!.payload).parents).toEqual([]);

    await requireRewriteFixture(seeded.doName);
    const stub = getRepoStub(env, seeded.doName);
    const sourceCatalog = await stub.getActivePackCatalog();
    expect(sourceCatalog).toHaveLength(3);
    const oldCatalogCache = gcContext(seeded.doName);
    oldCatalogCache.memo!.packCatalog = sourceCatalog;
    const queueSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    const result = await runReachabilityGc({
      env,
      repoId: seeded.doName,
      cacheCtx: gcContext(seeded.doName),
      limiter: new SubrequestLimiter(6),
      log: createLogger("error", { service: "ReachabilityGcTest" }),
      countSubrequest: () => {},
    });
    expect(result).toMatchObject({
      status: "completed",
      sourcePacks: 3,
      scheduledArtifacts: 9,
    });
    expect(queueSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "compaction-delete",
        packKeys: sourceCatalog.map((row) => row.packKey),
        removeCatalogRows: true,
      }),
      { delaySeconds: 60 }
    );
    queueSpy.mockRestore();

    const activeCatalog = await stub.getActivePackCatalog();
    expect(activeCatalog).toHaveLength(1);
    expect(await stub.listSupersededGcPacks()).toHaveLength(sourceCatalog.length);
    expect(activeCatalog[0]!.objectCount).toBe(
      result.status === "completed" ? result.reachableObjects : -1
    );
    for (const row of sourceCatalog) {
      expect(await env.REPO_BUCKET.head(row.packKey)).not.toBeNull();
    }
    // A request that captured the old catalog before the swap remains readable
    // throughout the same 60-second drain window used by normal compaction.
    expect(
      await readObject(env, seeded.doName, second.acceptedWrite.afterSha, oldCatalogCache)
    ).toMatchObject({ type: "commit" });
    const deletion = await deleteSupersededOnce(
      seeded.doName,
      sourceCatalog.map((row) => row.packKey),
      true
    );
    expect(deletion).toEqual({ acked: true, retried: false });
    expect(await stub.listSupersededGcPacks()).toEqual([]);
    const deleteSpy = vi.spyOn(env.REPO_BUCKET, "delete");
    expect(
      await deleteSupersededOnce(
        seeded.doName,
        sourceCatalog.map((row) => row.packKey),
        true
      )
    ).toEqual({ acked: true, retried: false });
    expect(deleteSpy).not.toHaveBeenCalled();
    for (const row of sourceCatalog) {
      expect(await env.REPO_BUCKET.head(row.packKey)).toBeNull();
    }
    expect(
      await readObject(env, seeded.doName, second.acceptedWrite.afterSha, gcContext(seeded.doName))
    ).toBeUndefined();
    expect(
      await readObject(env, seeded.doName, epoch.acceptedWrite.afterSha, gcContext(seeded.doName))
    ).toMatchObject({ type: "commit" });
  });

  it("restores a complete pack and exact ref inventory through receive-pack", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("import-restore");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const staleOid = "a".repeat(40);
    expect(
      await getRepoStub(env, seeded.doName).setRefs([{ name: "refs/heads/stale", oid: staleOid }])
    ).toBe(true);
    const blobPayload = new TextEncoder().encode("restored private bytes\n");
    const blob = await encodeGitObject("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "secret.txt", oid: blob.oid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\nauthor Restore <restore@example.com> 0 +0000\n` +
        `committer Restore <restore@example.com> 0 +0000\n\nrestored\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: commitPayload },
    ]);
    const body = concatChunks([
      pktLine(
        `${staleOid} ${zeroOid()} refs/heads/stale\0 report-status ofs-delta agent=restore-test\n`
      ),
      pktLine(`${zeroOid()} ${commit.oid} refs/heads/main\n`),
      pktLine(`${zeroOid()} ${commit.oid} refs/tags/restore-point\n`),
      flushPkt(),
      pack,
    ]);
    const response = await postReceivePack(
      `https://example.com/${owner}/${repo}/git-receive-pack`,
      body
    );
    expect(response.status).toBe(200);

    const stub = getRepoStub(env, seeded.doName);
    expect(await stub.listRefs()).toEqual([
      { name: "refs/heads/main", oid: commit.oid },
      { name: "refs/tags/restore-point", oid: commit.oid },
    ]);
    const restoredBlob = await readObject(env, seeded.doName, blob.oid, gcContext(seeded.doName));
    expect(restoredBlob?.type).toBe("blob");
    expect(restoredBlob?.payload).toEqual(blobPayload);
  });

  it("reconciles a lost catalog-commit response without deleting the active rewrite", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-cleanup-retry");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const first = await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "gc-cleanup-first",
      content: "old\n",
    });
    const epoch = await ingest(owner, repo, {
      expectedOid: first.acceptedWrite.afterSha,
      idempotencyKey: "gc-cleanup-epoch",
      content: "new\n",
      historyMode: "epoch",
    });
    await requireRewriteFixture(seeded.doName);
    const stub = getRepoStub(env, seeded.doName);
    const sourceCatalog = await stub.getActivePackCatalog();
    const queueSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    const boundaryBudget = new ReachabilityGcSubrequestBudget();
    let appended: IngestionResponse | undefined;
    reachabilityGcTest.failNextCommitResponse(async () => {
      let currentOid = epoch.acceptedWrite.afterSha;
      for (let index = 0; index < 5; index++) {
        appended = await ingest(owner, repo, {
          expectedOid: currentOid,
          idempotencyKey: `gc-cleanup-after-commit-${index}`,
          content: `newer-${index}\n`,
        });
        currentOid = appended.acceptedWrite.afterSha;
      }
    });
    try {
      const result = await runReachabilityGc({
        env,
        repoId: seeded.doName,
        cacheCtx: gcContext(seeded.doName),
        limiter: new SubrequestLimiter(6),
        log: createLogger("error", { service: "ReachabilityGcCommitRetryTest" }),
        countSubrequest: (op, count = 1) => {
          if (op === "do:commit-reachability-gc") {
            boundaryBudget.consume(889 - boundaryBudget.used);
          }
          boundaryBudget.consume(count, isReachabilityGcReservedSubrequest(op));
        },
      });
      expect(result.status).toBe("completed");
      expect(boundaryBudget.used).toBeLessThanOrEqual(900);
    } finally {
      reachabilityGcTest.reset();
      queueSpy.mockRestore();
    }
    const activeCatalog = await stub.getActivePackCatalog();
    expect(activeCatalog).toHaveLength(6);
    expect(appended).toBeDefined();
    expect((await stub.debugState()).compaction.queued).toBe(true);
    for (const row of activeCatalog) {
      expect(await env.REPO_BUCKET.head(row.packKey)).not.toBeNull();
    }
    expect(
      await readObject(env, seeded.doName, epoch.acceptedWrite.afterSha, gcContext(seeded.doName))
    ).toMatchObject({ type: "commit" });
    expect(
      await readObject(
        env,
        seeded.doName,
        appended!.acceptedWrite.afterSha,
        gcContext(seeded.doName)
      )
    ).toMatchObject({ type: "commit" });
    expect(await stub.listSupersededGcPacks()).toHaveLength(sourceCatalog.length);
  });

  it("removes an orphan staged rewrite when neither ambiguous commit attempt reaches the DO", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-commit-never-reached");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const first = await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "gc-commit-never-reached-first",
      content: "old\n",
    });
    await ingest(owner, repo, {
      expectedOid: first.acceptedWrite.afterSha,
      idempotencyKey: "gc-commit-never-reached-epoch",
      content: "new\n",
      historyMode: "epoch",
    });
    await requireRewriteFixture(seeded.doName);
    const stub = getRepoStub(env, seeded.doName);
    const sourceCatalog = await stub.getActivePackCatalog();
    const prefix = doPrefix(env.REPO_DO.idFromName(seeded.doName).toString());
    const queueSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    reachabilityGcTest.failNextCommitTransports(2);
    try {
      await expect(
        runReachabilityGc({
          env,
          repoId: seeded.doName,
          cacheCtx: gcContext(seeded.doName),
          limiter: new SubrequestLimiter(6),
          log: createLogger("error", { service: "ReachabilityGcNoCommitTest" }),
          countSubrequest: () => {},
        })
      ).rejects.toThrow("transport failure before DO");
      const [orphanPackKey] = await listGcPackKeys(prefix);
      expect(orphanPackKey).toBeDefined();
      expect(await stub.getActivePackCatalog()).toEqual(sourceCatalog);

      reachabilityGcTest.reset();
      const retry = await runReachabilityGc({
        env,
        repoId: seeded.doName,
        cacheCtx: gcContext(seeded.doName),
        limiter: new SubrequestLimiter(6),
        log: createLogger("error", { service: "ReachabilityGcNoCommitRetryTest" }),
        countSubrequest: () => {},
      });
      expect(retry.status).toBe("completed");
      expect(await env.REPO_BUCKET.head(orphanPackKey!)).toBeNull();
      const [active] = await stub.getActivePackCatalog();
      expect(active?.packKey).not.toBe(orphanPackKey);
      expect(await env.REPO_BUCKET.head(active!.packKey)).not.toBeNull();
    } finally {
      reachabilityGcTest.reset();
      queueSpy.mockRestore();
    }
  });

  it("reconciles the staged rewrite after the work budget is exhausted before commit", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-budget-after-pending");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const first = await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "gc-budget-after-pending-first",
      content: "old\n",
    });
    await ingest(owner, repo, {
      expectedOid: first.acceptedWrite.afterSha,
      idempotencyKey: "gc-budget-after-pending-epoch",
      content: "new\n",
      historyMode: "epoch",
    });
    await requireRewriteFixture(seeded.doName);
    const prefix = doPrefix(env.REPO_DO.idFromName(seeded.doName).toString());
    const queueSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      await expect(
        runReachabilityGc({
          env,
          repoId: seeded.doName,
          cacheCtx: gcContext(seeded.doName),
          limiter: new SubrequestLimiter(6),
          log: createLogger("error", { service: "ReachabilityGcBudgetPendingTest" }),
          countSubrequest: (op) => {
            if (op === "do:commit-reachability-gc" || op === "do:reconcile-reachability-gc") {
              throw new ReachabilityGcBudgetExceededError("injected exhausted work budget");
            }
          },
        })
      ).rejects.toThrow("injected exhausted work budget");
      const [orphanPackKey] = await listGcPackKeys(prefix);
      expect(orphanPackKey).toBeDefined();

      const retry = await runReachabilityGc({
        env,
        repoId: seeded.doName,
        cacheCtx: gcContext(seeded.doName),
        limiter: new SubrequestLimiter(6),
        log: createLogger("error", { service: "ReachabilityGcBudgetPendingRetryTest" }),
        countSubrequest: () => {},
      });
      expect(retry.status).toBe("completed");
      expect(await env.REPO_BUCKET.head(orphanPackKey!)).toBeNull();
    } finally {
      queueSpy.mockRestore();
    }
  });

  it("retains an active rewrite when both successful commit responses are lost", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-both-responses-lost");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const first = await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "gc-both-responses-lost-first",
      content: "old\n",
    });
    await ingest(owner, repo, {
      expectedOid: first.acceptedWrite.afterSha,
      idempotencyKey: "gc-both-responses-lost-epoch",
      content: "new\n",
      historyMode: "epoch",
    });
    await requireRewriteFixture(seeded.doName);
    const stub = getRepoStub(env, seeded.doName);
    const queueSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    reachabilityGcTest.failNextCommitResponse(undefined, 2);
    try {
      await expect(
        runReachabilityGc({
          env,
          repoId: seeded.doName,
          cacheCtx: gcContext(seeded.doName),
          limiter: new SubrequestLimiter(6),
          log: createLogger("error", { service: "ReachabilityGcBothLostTest" }),
          countSubrequest: () => {},
        })
      ).rejects.toThrow("lost reachability GC commit response");
      const [active] = await stub.getActivePackCatalog();
      expect(active?.packKey).toContain("pack-gc-");
      expect(await env.REPO_BUCKET.head(active!.packKey)).not.toBeNull();

      reachabilityGcTest.reset();
      const retry = await runReachabilityGc({
        env,
        repoId: seeded.doName,
        cacheCtx: gcContext(seeded.doName),
        limiter: new SubrequestLimiter(6),
        log: createLogger("error", { service: "ReachabilityGcBothLostRetryTest" }),
        countSubrequest: () => {},
      });
      expect(retry).toEqual({ status: "retry", reason: "cleanup-scheduled" });
      expect((await stub.getActivePackCatalog())[0]?.packKey).toBe(active!.packKey);
      expect(await env.REPO_BUCKET.head(active!.packKey)).not.toBeNull();
    } finally {
      reachabilityGcTest.reset();
      queueSpy.mockRestore();
    }
  });

  it("retries delayed cleanup scheduling after a committed catalog swap", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-cleanup-retry");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const first = await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "gc-cleanup-first",
      content: "old\n",
    });
    await ingest(owner, repo, {
      expectedOid: first.acceptedWrite.afterSha,
      idempotencyKey: "gc-cleanup-epoch",
      content: "new\n",
      historyMode: "epoch",
    });
    await requireRewriteFixture(seeded.doName);
    const stub = getRepoStub(env, seeded.doName);
    const sourceCatalog = await stub.getActivePackCatalog();
    const queueSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockRejectedValueOnce(new Error("injected cleanup enqueue outage"))
      .mockImplementation(async () => createQueueSendResponse());
    const firstRun = await runReachabilityGc({
      env,
      repoId: seeded.doName,
      cacheCtx: gcContext(seeded.doName),
      limiter: new SubrequestLimiter(6),
      log: createLogger("error", { service: "ReachabilityGcCleanupRetryTest" }),
      countSubrequest: () => {},
    });
    expect(firstRun).toEqual({ status: "retry", reason: "cleanup-enqueue-failed" });
    expect(await stub.listSupersededGcPacks()).toHaveLength(sourceCatalog.length);
    const retry = await runReachabilityGc({
      env,
      repoId: seeded.doName,
      cacheCtx: gcContext(seeded.doName),
      limiter: new SubrequestLimiter(6),
      log: createLogger("error", { service: "ReachabilityGcCleanupRetryTest" }),
      countSubrequest: () => {},
    });
    expect(retry).toEqual({ status: "retry", reason: "cleanup-scheduled" });
    queueSpy.mockRestore();
    const deletion = await deleteSupersededOnce(
      seeded.doName,
      sourceCatalog.map((row) => row.packKey),
      true
    );
    expect(deletion).toEqual({ acked: true, retried: false });
    expect(await stub.listSupersededGcPacks()).toEqual([]);
    for (const row of sourceCatalog) {
      expect(await env.REPO_BUCKET.head(row.packKey)).toBeNull();
    }
  });

  it("rolls back an interrupted SQL catalog replacement", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-catalog-rollback");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "gc-catalog-rollback",
      content: "state\n",
    });
    const stub = getRepoStub(env, seeded.doName);
    const begin = await stub.beginReachabilityGc();
    expect(begin.ok).toBe(true);
    if (!begin.ok) throw new Error("reachability GC lease was not acquired");
    const targetPackKey = r2PackKey(
      doPrefix(env.REPO_DO.idFromName(seeded.doName).toString()),
      `pack-gc-${begin.lease.token}.pack`
    );
    packCatalogTest.failNextCatalogReplacement();
    try {
      expect(
        await stub.recordReachabilityGcPending({
          token: begin.lease.token,
          packKey: targetPackKey,
        })
      ).toEqual({ status: "recorded" });
      expect(
        await stub.commitReachabilityGc({
          token: begin.lease.token,
          refsVersion: begin.refsVersion,
          packsetVersion: begin.packsetVersion,
          sourcePacks: begin.activeCatalog,
          stagedPack: {
            packKey: targetPackKey,
            packBytes: 1,
            idxBytes: 1,
            objectCount: 1,
          },
        })
      ).toEqual({ status: "retry", reason: "catalog-replacement-failed" });
    } finally {
      packCatalogTest.reset();
      await stub.abortCompaction(begin.lease.token);
    }
    expect(await stub.getActivePackCatalog()).toEqual(begin.activeCatalog);
    expect((await stub.listSupersededGcPacks()).map((row) => row.packKey)).not.toContain(
      targetPackKey
    );
  });

  it("refuses cleanup of a key that the DO still marks active before deleting R2", async () => {
    const owner = "maintenance";
    const repo = uniqueRepoId("gc-cleanup-active-key");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    await ingest(owner, repo, {
      expectedOid: zeroOid(),
      idempotencyKey: "gc-cleanup-active-key",
      content: "state\n",
    });
    const active = (await getRepoStub(env, seeded.doName).getActivePackCatalog())[0]!;
    const result = await deleteSupersededOnce(seeded.doName, [active.packKey], true);
    expect(result).toEqual({ acked: false, retried: true });
    expect(await env.REPO_BUCKET.head(active.packKey)).not.toBeNull();
  });
});
