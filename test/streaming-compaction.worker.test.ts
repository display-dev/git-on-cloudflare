import { describe, expect, it, vi } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";
import { getRepoStub } from "@/worker/common";
import { bytesToHex } from "@/worker/common/hex";
import { encodeGitObject } from "@/worker/git/core/objects";
import { concatChunks, decodePktLines } from "@/worker/git";
import {
  doPrefix,
  nativeReceiveInputRequestKey,
  nativeReceiveOutputPackKey,
  packIndexKey,
  packRefsKey,
  r2PackKey,
  repositoryGenerationIndexKey,
  repositoryGenerationManifestKey,
} from "@/worker/keys";
import { buildFetchBody } from "./util/fetch-protocol";
import {
  deleteLooseObjectCopies,
  uniqueRepoId,
  runDOWithRetry,
  seedPackedRepoState,
  buildTreePayload,
  buildPack,
  buildAppendOnlyDelta,
} from "./util/test-helpers";
import { setupRepoForTests } from "./util/repoSeed";
import { seedPackFirstRepo } from "./util/pack-first";
import { indexTestPack } from "./util/test-indexer";
import { decodeReportStatus, promoteToStreaming } from "./util/streaming-helpers";
import { asTypedStorage, type RepoStateSchema } from "@/worker/do/repo/repoState";
import { createQueueSendResponse, runQueueMessage } from "./util/queue";
import { compactionDeleteRetryDelaySeconds } from "@/worker/tasks/compaction";
import {
  COMPACTION_ACTIVITY_QUIET_MS,
  COMPACTION_MAX_DEFERRAL_MS,
} from "@/worker/do/repo/catalog/shared";
import { getDb, upsertPackCatalogRow } from "@/worker/do/repo/db";
import * as packRewrite from "@/worker/git/pack/rewrite";
import type { CompactionDeleteQueueMessage } from "@/worker/tasks/types";
import {
  compactOnce,
  deleteSupersededOnce,
  collectPackObjects,
  expireCompactionQuietPeriod,
  pushOverflowingStreamingHistory,
} from "./util/compaction-helpers";

type DebugState = {
  activePacks?: Array<{ key: string; tier: number; kind: string }>;
  supersededPacks?: Array<{ key: string; tier: number; kind: string }>;
  compaction?: { queued?: boolean };
};

async function getDebugState(
  owner: string,
  repo: string,
  cookieHeader: string
): Promise<DebugState> {
  const response = await workerExports.default.fetch(
    `https://example.com/${owner}/${repo}/admin/debug-state`,
    { headers: { Cookie: cookieHeader } }
  );
  expect(response.status).toBe(200);
  return (await response.json()) as DebugState;
}

function isCompactionDeleteMessage(message: unknown): message is CompactionDeleteQueueMessage {
  return (
    typeof message === "object" &&
    message !== null &&
    "kind" in message &&
    message.kind === "compaction-delete"
  );
}

describe("streaming compaction", () => {
  it("deterministically spreads reader-fenced cleanup retries", () => {
    const body = {
      supersededAtGeneration: 42,
      packKeys: ["repos/repo/objects/pack/pack-a.pack"],
    };
    const first = compactionDeleteRetryDelaySeconds(20, body);
    expect(first).toBe(compactionDeleteRetryDelaySeconds(20, body));
    expect(first).toBeGreaterThanOrEqual(21);
    expect(first).toBeLessThanOrEqual(35);
    expect(
      compactionDeleteRetryDelaySeconds(20, {
        supersededAtGeneration: 43,
        packKeys: body.packKeys,
      })
    ).not.toBe(first);
  });

  it("defers superseded artifact deletion while an older native generation reader is active", async () => {
    const repoId = `o/${uniqueRepoId("native-reader-fence")}`;
    await setupRepoForTests(env, "o", repoId.slice(2));
    const stub = getRepoStub(env, repoId);
    const generationPrefix = doPrefix(stub.id.toString());
    const packKey = r2PackKey(generationPrefix, "reader-fence.pack");
    await env.REPO_BUCKET.put(packKey, new Uint8Array([1]));
    await env.REPO_BUCKET.put(packIndexKey(packKey), new Uint8Array([2]));
    await env.REPO_BUCKET.put(packRefsKey(packKey), new Uint8Array([3]));
    const generationManifestKey = repositoryGenerationManifestKey(generationPrefix, 5);
    await env.REPO_BUCKET.put(
      generationManifestKey,
      JSON.stringify({ schemaVersion: 1, generation: 5, packs: [] })
    );
    await env.REPO_BUCKET.put(
      repositoryGenerationIndexKey(generationPrefix),
      JSON.stringify({
        schemaVersion: 1,
        generation: 5,
        manifestKey: generationManifestKey,
        updatedAt: Date.now(),
      })
    );
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.put("nativeCatalogReaderLease", {
          token: "active-container",
          createdAt: Date.now(),
          expiresAt: Date.now() + 60_000,
          operation: "native-reader",
          generation: 4,
        });
      }
    );

    const foreignPackKey = r2PackKey(doPrefix("foreign-do"), "reader-fence.pack");
    await env.REPO_BUCKET.put(foreignPackKey, new Uint8Array([4]));
    expect(await deleteSupersededOnce(repoId, [foreignPackKey])).toEqual({
      acked: true,
      retried: false,
    });
    expect(await env.REPO_BUCKET.head(foreignPackKey)).not.toBeNull();

    const legacyDeferred = await deleteSupersededOnce(repoId, [packKey]);
    expect(legacyDeferred).toEqual({ acked: false, retried: true });
    expect(await env.REPO_BUCKET.head(packKey)).not.toBeNull();

    const deferred = await deleteSupersededOnce(repoId, [packKey], false, 5);
    expect(deferred).toEqual({ acked: false, retried: true });
    expect(await env.REPO_BUCKET.head(packKey)).not.toBeNull();

    await runDOWithRetry(
      () => stub,
      async (_instance, state) => await state.storage.delete("nativeCatalogReaderLease")
    );
    const fetchReader = await stub.beginRepositoryRead();
    expect(fetchReader.ok).toBe(true);
    const fetchDeferred = await deleteSupersededOnce(repoId, [packKey], false, 5);
    expect(fetchDeferred).toEqual({ acked: false, retried: true });
    if (fetchReader.ok) await stub.finishRepositoryRead(fetchReader.token);
    const unpublished = await deleteSupersededOnce(repoId, [packKey], false, 6);
    expect(unpublished).toEqual({ acked: false, retried: true });
    expect(await env.REPO_BUCKET.head(packKey)).not.toBeNull();
    const deleted = await deleteSupersededOnce(repoId, [packKey], false, 5);
    expect(deleted).toEqual({ acked: true, retried: false });
    expect(await env.REPO_BUCKET.head(packKey)).toBeNull();
  });

  it("does not delete a superseded row until the published manifest excludes its pack", async () => {
    const repoId = `o/${uniqueRepoId("published-generation-fence")}`;
    await setupRepoForTests(env, "o", repoId.slice(2));
    const stub = getRepoStub(env, repoId);
    const packKey = `${doPrefix(stub.id.toString())}/objects/pack/pending.pack`;
    await env.REPO_BUCKET.put(packKey, new Uint8Array([1]));
    await env.REPO_BUCKET.put(packIndexKey(packKey), new Uint8Array([2]));
    await env.REPO_BUCKET.put(packRefsKey(packKey), new Uint8Array([3]));

    const prefix = doPrefix(stub.id.toString());
    const generationFiveManifest = repositoryGenerationManifestKey(prefix, 5);
    await env.REPO_BUCKET.put(
      generationFiveManifest,
      JSON.stringify({ schemaVersion: 1, generation: 5, packs: [{ packKey }] })
    );
    await env.REPO_BUCKET.put(
      repositoryGenerationIndexKey(prefix),
      JSON.stringify({
        schemaVersion: 1,
        generation: 5,
        manifestKey: generationFiveManifest,
        updatedAt: Date.now(),
      })
    );
    expect(await deleteSupersededOnce(repoId, [packKey], false, 5)).toEqual({
      acked: false,
      retried: true,
    });
    expect(await env.REPO_BUCKET.head(packKey)).not.toBeNull();

    const generationSixManifest = repositoryGenerationManifestKey(prefix, 6);
    await env.REPO_BUCKET.put(
      generationSixManifest,
      JSON.stringify({ schemaVersion: 1, generation: 6, packs: [] })
    );
    await env.REPO_BUCKET.put(
      repositoryGenerationIndexKey(prefix),
      JSON.stringify({
        schemaVersion: 1,
        generation: 6,
        manifestKey: generationSixManifest,
        updatedAt: Date.now(),
      })
    );
    expect(await deleteSupersededOnce(repoId, [packKey], false, 5)).toEqual({
      acked: true,
      retried: false,
    });
    expect(await env.REPO_BUCKET.head(packKey)).toBeNull();
  });

  it("previews and requests real compaction work only after streaming overflow exists", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-admin");
    const seededRepo = await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: seeded.nextCommit.oid,
        updates: 4,
      });
      sendSpy.mockClear();

      const previewResponse = await workerExports.default.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "POST",
          headers: {
            Cookie: seededRepo.cookieHeader,
            "Content-Type": "application/json",
            Origin: "https://example.com",
          },
          body: JSON.stringify({}),
        }
      );
      expect(previewResponse.status).toBe(200);
      const previewJson = (await previewResponse.json()) as {
        action?: string;
        status?: string;
        plan?: {
          sourcePacks?: Array<{ packKey?: string }>;
          sourceTier?: number;
          targetTier?: number;
        };
      };
      expect(previewJson.action).toBe("preview");
      expect(previewJson.status).toBe("ok");
      expect(previewJson.plan?.sourcePacks?.length).toBe(4);
      expect(previewJson.plan?.sourceTier).toBe(0);
      expect(previewJson.plan?.targetTier).toBe(1);

      const requestResponse = await workerExports.default.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "POST",
          headers: {
            Cookie: seededRepo.cookieHeader,
            "Content-Type": "application/json",
            Origin: "https://example.com",
          },
          body: JSON.stringify({ dryRun: false }),
        }
      );
      expect(requestResponse.status).toBe(202);
      const requestJson = (await requestResponse.json()) as {
        action?: string;
        status?: string;
        shouldEnqueue?: boolean;
      };
      expect(requestJson.action).toBe("request");
      expect(requestJson.status).toBe("queued");
      expect(requestJson.shouldEnqueue).toBe(true);
      expect(sendSpy).toHaveBeenCalledWith({
        kind: "compaction",
        doId: env.REPO_DO.idFromName(repoId).toString(),
        repoId,
      });
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("schedules one final reconciliation and ignores unrequested duplicates", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-cleanup-fanout");
    await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const stub = getRepoStub(env, repoId);
    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      await promoteToStreaming(owner, repo);
      const firstPush = await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: seeded.nextCommit.oid,
        updates: 4,
      });
      expect(await compactOnce(repoId)).toEqual({ acked: true, retried: false });
      const previouslySuperseded = await stub.listSupersededGcPacks();
      expect(previouslySuperseded).toHaveLength(4);

      await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: firstPush.currentCommitOid,
        updates: 8,
      });
      sendSpy.mockClear();
      expect(await compactOnce(repoId)).toEqual({ acked: true, retried: false });
      const cleanupMessages = sendSpy.mock.calls
        .map(([message]) => message)
        .filter(isCompactionDeleteMessage);
      expect(cleanupMessages).toHaveLength(0);

      sendSpy.mockClear();
      expect(await compactOnce(repoId)).toEqual({ acked: true, retried: false });
      const supersededAfterFinalCompaction = await stub.listSupersededGcPacks();
      expect(sendSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          kind: "compaction-delete",
          packKeys: expect.arrayContaining(
            supersededAfterFinalCompaction.map((row) => row.packKey)
          ),
          removeCatalogRows: true,
        }),
        { delaySeconds: 60 }
      );

      sendSpy.mockClear();
      expect(await compactOnce(repoId)).toEqual({ acked: true, retried: false });
      expect(sendSpy.mock.calls.some(([message]) => isCompactionDeleteMessage(message))).toBe(
        false
      );
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("pages a no-work recovery backlog beyond one cleanup message", async () => {
    const repoId = `o/${uniqueRepoId("stream-compaction-cleanup-pages")}`;
    await setupRepoForTests(env, "o", repoId.slice(2));
    const stub = getRepoStub(env, repoId);
    const prefix = doPrefix(stub.id.toString());
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const db = getDb(state.storage);
        for (let index = 0; index < 251; index++) {
          await upsertPackCatalogRow(db, {
            packKey: `${prefix}/objects/pack/backlog-${String(index).padStart(3, "0")}.pack`,
            kind: "receive",
            state: "superseded",
            tier: 0,
            seqLo: index + 1,
            seqHi: index + 1,
            objectCount: 1,
            packBytes: 1,
            idxBytes: 1,
            createdAt: index + 1,
            supersededBy: "recovery",
          });
        }
      }
    );

    const firstPage = await stub.listSupersededGcPacks(undefined, 250);
    expect(firstPage).toHaveLength(250);
    const cursorRow = firstPage.at(-1)!;
    expect(await stub.removeSupersededGcPacks(firstPage.map((row) => row.packKey))).toMatchObject({
      status: "removed",
    });
    const secondPageAfterRemoval = await stub.listSupersededGcPacks(
      { seqHi: cursorRow.seqHi, tier: cursorRow.tier, packKey: cursorRow.packKey },
      250
    );
    expect(secondPageAfterRemoval).toHaveLength(1);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const db = getDb(state.storage);
        for (const row of firstPage) await upsertPackCatalogRow(db, row);
      }
    );

    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      await runDOWithRetry(
        () => stub,
        async (_instance, state) => {
          const requestedAt = Date.now();
          await state.storage.put("compactionWantedAt", requestedAt);
          await state.storage.put("compactionPendingSince", requestedAt);
        }
      );
      expect(await compactOnce(repoId)).toEqual({ acked: true, retried: false });
      const cleanupMessages = sendSpy.mock.calls
        .map(([message]) => message)
        .filter(
          (message): message is CompactionDeleteQueueMessage =>
            isCompactionDeleteMessage(message) && message.repoId === repoId
        );
      expect(cleanupMessages.map((message) => message.packKeys.length)).toEqual([250, 1]);
      expect(new Set(cleanupMessages.flatMap((message) => message.packKeys)).size).toBe(251);
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("reconciles earlier superseded rows when a later compaction pass is blocked", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-blocked-cleanup");
    await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const stub = getRepoStub(env, repoId);
    const prefix = doPrefix(stub.id.toString());
    await promoteToStreaming(owner, repo);
    await pushOverflowingStreamingHistory({
      owner,
      repo,
      repoId,
      startingCommitOid: seeded.nextCommit.oid,
      updates: 4,
    });
    const priorPackKey = `${prefix}/objects/pack/prior-superseded.pack`;
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await upsertPackCatalogRow(getDb(state.storage), {
          packKey: priorPackKey,
          kind: "receive",
          state: "superseded",
          tier: 0,
          seqLo: 0,
          seqHi: 0,
          objectCount: 1,
          packBytes: 1,
          idxBytes: 1,
          createdAt: 1,
          supersededBy: "blocked-pass",
        });
      }
    );

    const rewriteSpy = vi.spyOn(packRewrite, "rewritePackResult").mockResolvedValue({
      status: "failed",
      failure: { reason: "topology-incomplete", retryable: false },
    });
    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      expect(await compactOnce(repoId)).toEqual({ acked: true, retried: false });
      expect(sendSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          kind: "compaction-delete",
          packKeys: [priorPackKey],
          removeCatalogRows: true,
        }),
        { delaySeconds: 60 }
      );
      expect(await stub.previewCompaction()).toMatchObject({ queued: false });
    } finally {
      sendSpy.mockRestore();
      rewriteSpy.mockRestore();
    }
  });

  it("does not schedule a historical sweep for a duplicate while compaction is active", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-cleanup-busy");
    await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const stub = getRepoStub(env, repoId);
    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    let heldToken: string | undefined;
    try {
      await promoteToStreaming(owner, repo);
      await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: seeded.nextCommit.oid,
        updates: 4,
      });
      expect(await stub.beginCompaction()).toMatchObject({
        ok: false,
        status: "busy",
        reason: "recent-activity",
      });
      await expireCompactionQuietPeriod(repoId);
      const held = await stub.beginCompaction();
      expect(held.ok).toBe(true);
      if (!held.ok) throw new Error("expected compaction lease");
      heldToken = held.lease.token;
      sendSpy.mockClear();
      expect(await compactOnce(repoId)).toEqual({ acked: false, retried: true });
      expect(sendSpy.mock.calls.some(([message]) => isCompactionDeleteMessage(message))).toBe(
        false
      );
    } finally {
      sendSpy.mockRestore();
      if (heldToken) await stub.abortCompaction(heldToken);
    }
  });

  it("defers compaction for recent writes but cannot starve it indefinitely", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-activity-window");
    await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const doId = env.REPO_DO.idFromName(repoId).toString();
    const seeded = await seedPackFirstRepo(repoId);
    const stub = getRepoStub(env, repoId);
    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      await promoteToStreaming(owner, repo);
      const overflowing = await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: seeded.nextCommit.oid,
        updates: 4,
      });

      expect(await runQueueMessage({ kind: "compaction", doId, repoId })).toEqual({
        acked: false,
        retried: true,
      });

      const priorActivityAt = Date.now() - COMPACTION_ACTIVITY_QUIET_MS - 1;
      await runDOWithRetry(
        () => stub,
        async (_instance, state) => {
          await state.storage.put("compactionPendingSince", priorActivityAt);
          await state.storage.put("compactionWantedAt", priorActivityAt);
        }
      );
      await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: overflowing.currentCommitOid,
        updates: 1,
      });
      const movedSchedule = await runDOWithRetry(
        () => stub,
        async (_instance, state) => ({
          wantedAt: await state.storage.get<number>("compactionWantedAt"),
          pendingSince: await state.storage.get<number>("compactionPendingSince"),
        })
      );
      expect(movedSchedule.pendingSince).toBe(priorActivityAt);
      expect(movedSchedule.wantedAt).toBeGreaterThan(priorActivityAt);
      const moved = await stub.beginCompaction();
      expect(moved).toMatchObject({
        ok: false,
        status: "busy",
        reason: "recent-activity",
      });
      if (moved.ok || moved.status !== "busy") throw new Error("expected recent activity");
      expect(moved.retryAfter).toBeGreaterThanOrEqual(1);
      expect(moved.retryAfter).toBeLessThanOrEqual(COMPACTION_ACTIVITY_QUIET_MS / 1_000);

      // A repository that never becomes quiet still reaches the existing
      // alarm-recovery deadline and is allowed to compact.
      await runDOWithRetry(
        () => stub,
        async (_instance, state) => {
          await state.storage.put(
            "compactionPendingSince",
            Date.now() - COMPACTION_MAX_DEFERRAL_MS - 1
          );
          await state.storage.put("compactionWantedAt", Date.now());
        }
      );
      expect(await runQueueMessage({ kind: "compaction", doId, repoId })).toEqual({
        acked: true,
        retried: false,
      });
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("preserves the activity window across internal compaction passes", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-follow-up-window");
    await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const doId = env.REPO_DO.idFromName(repoId).toString();
    const seeded = await seedPackFirstRepo(repoId);
    const stub = getRepoStub(env, repoId);
    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      await promoteToStreaming(owner, repo);
      await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: seeded.nextCommit.oid,
        updates: 8,
      });
      await expireCompactionQuietPeriod(repoId);
      const before = await runDOWithRetry(
        () => stub,
        async (_instance, state) => ({
          wantedAt: await state.storage.get<number>("compactionWantedAt"),
          pendingSince: await state.storage.get<number>("compactionPendingSince"),
        })
      );

      expect(await runQueueMessage({ kind: "compaction", doId, repoId })).toEqual({
        acked: true,
        retried: false,
      });
      const after = await runDOWithRetry(
        () => stub,
        async (_instance, state) => ({
          wantedAt: await state.storage.get<number>("compactionWantedAt"),
          pendingSince: await state.storage.get<number>("compactionPendingSince"),
        })
      );
      expect(after).toEqual(before);
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("keeps the admin request queued when queue enqueue fails after DO state is recorded", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-admin-enqueue-failure");
    const seededRepo = await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: seeded.nextCommit.oid,
        updates: 4,
      });
      sendSpy.mockClear();
      sendSpy.mockRejectedValue(new Error("queue unavailable"));

      const requestResponse = await workerExports.default.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "POST",
          headers: {
            Cookie: seededRepo.cookieHeader,
            "Content-Type": "application/json",
            Origin: "https://example.com",
          },
          body: JSON.stringify({ dryRun: false }),
        }
      );
      expect(requestResponse.status).toBe(202);
      const requestJson = (await requestResponse.json()) as {
        action?: string;
        status?: string;
        shouldEnqueue?: boolean;
      };
      expect(requestJson.action).toBe("request");
      expect(requestJson.status).toBe("queued");
      expect(requestJson.shouldEnqueue).toBe(true);

      const stateAfterRequest = await getDebugState(owner, repo, seededRepo.cookieHeader);
      expect(stateAfterRequest.compaction?.queued).toBe(true);
      expect(sendSpy).toHaveBeenCalledWith({
        kind: "compaction",
        doId: env.REPO_DO.idFromName(repoId).toString(),
        repoId,
      });
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("previews compaction plan even after clearing compactionWantedAt", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-preview-cleared");
    const seededRepo = await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: seeded.nextCommit.oid,
        updates: 4,
      });
      sendSpy.mockClear();

      // Request compaction so compactionWantedAt is set.
      const requestResponse = await workerExports.default.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "POST",
          headers: {
            Cookie: seededRepo.cookieHeader,
            "Content-Type": "application/json",
            Origin: "https://example.com",
          },
          body: JSON.stringify({ dryRun: false }),
        }
      );
      expect(requestResponse.status).toBe(202);

      // Clear the recorded request.
      const clearResponse = await workerExports.default.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "DELETE",
          headers: { Cookie: seededRepo.cookieHeader, Origin: "https://example.com" },
        }
      );
      expect(clearResponse.status).toBe(200);
      const clearJson = (await clearResponse.json()) as { cleared?: boolean };
      expect(clearJson.cleared).toBe(true);
      await runDOWithRetry(
        () => getRepoStub(env, repoId),
        async (_instance, state) => {
          expect(await state.storage.get("compactionWantedAt")).toBeUndefined();
          expect(await state.storage.get("compactionPendingSince")).toBeUndefined();
        }
      );

      // Preview should still show the plan with queued: false.
      const previewResponse = await workerExports.default.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "POST",
          headers: {
            Cookie: seededRepo.cookieHeader,
            "Content-Type": "application/json",
            Origin: "https://example.com",
          },
          body: JSON.stringify({}),
        }
      );
      expect(previewResponse.status).toBe(200);
      const previewJson = (await previewResponse.json()) as {
        action?: string;
        status?: string;
        queued?: boolean;
        plan?: { sourcePacks?: unknown[] };
      };
      expect(previewJson.action).toBe("preview");
      expect(previewJson.status).toBe("ok");
      expect(previewJson.queued).toBe(false);
      expect(previewJson.plan?.sourcePacks?.length).toBe(4);
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("compacts superseded packs and keeps fetch and raw reads correct without loose objects", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-run");
    const seededRepo = await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const stub = getRepoStub(env, repoId);
    await promoteToStreaming(owner, repo);

    const pushed = await pushOverflowingStreamingHistory({
      owner,
      repo,
      repoId,
      startingCommitOid: seeded.nextCommit.oid,
      updates: 4,
    });

    await deleteLooseObjectCopies(env, seeded.getStub, [
      ...seeded.objectOids,
      ...pushed.objectOids,
    ]);

    const compacted = await compactOnce(repoId);
    expect(compacted.acked).toBe(true);
    expect(compacted.retried).toBe(false);
    const stateAfterCompaction = await getDebugState(owner, repo, seededRepo.cookieHeader);
    const generationIndexKey = repositoryGenerationIndexKey(doPrefix(stub.id.toString()));
    await vi.waitFor(
      async () => {
        expect(await env.REPO_BUCKET.head(generationIndexKey)).not.toBeNull();
        expect(await stub.getPendingGenerationPublication()).toBeNull();
      },
      { timeout: 2_000, interval: 10 }
    );

    expect(stateAfterCompaction.compaction?.queued).toBe(false);
    expect(stateAfterCompaction.activePacks?.some((pack) => pack.kind === "compact")).toBe(true);
    expect(stateAfterCompaction.supersededPacks?.length).toBe(4);

    const supersededPackKeys = (stateAfterCompaction.supersededPacks || []).map((pack) => pack.key);
    const beforeDelete = await collectPackObjects(supersededPackKeys);
    expect(beforeDelete.every((entry) => entry.exists && entry.idxExists && entry.refsExists)).toBe(
      true
    );

    const rawResponse = await workerExports.default.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${pushed.objectOids.at(-3)}&name=README.md`
    );
    expect(rawResponse.status).toBe(200);
    expect(await rawResponse.text()).toBe("streaming update 3\n");

    const fetchResponse = await workerExports.default.fetch(
      `https://example.com/${owner}/${repo}/git-upload-pack`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-git-upload-pack-request",
          "Git-Protocol": "version=2",
        },
        body: buildFetchBody({
          wants: [pushed.currentCommitOid],
          haves: [seeded.nextCommit.oid],
          done: true,
        }),
      } as any
    );
    expect(fetchResponse.status).toBe(200);
    expect(
      decodeReportStatus(new Uint8Array(await fetchResponse.arrayBuffer())).length
    ).toBeGreaterThan(0);

    const deleted = await deleteSupersededOnce(repoId, supersededPackKeys);
    expect(deleted.acked).toBe(true);
    expect(deleted.retried).toBe(false);

    const afterDelete = await collectPackObjects(supersededPackKeys);
    expect(
      afterDelete.every((entry) => !entry.exists && !entry.idxExists && !entry.refsExists)
    ).toBe(true);

    // The superseded catalog rows remain visible for admin/debug until explicit cleanup.
    const finalState = await getDebugState(owner, repo, seededRepo.cookieHeader);
    expect(finalState.supersededPacks?.length).toBe(4);

    void stub;
  });

  it("admin remove deletes pack, idx, and ref sidecar for a superseded pack", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-admin-remove");
    const seededRepo = await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    await pushOverflowingStreamingHistory({
      owner,
      repo,
      repoId,
      startingCommitOid: seeded.nextCommit.oid,
      updates: 4,
    });

    const compacted = await compactOnce(repoId);
    expect(compacted.acked).toBe(true);
    expect(compacted.retried).toBe(false);

    const stateAfterCompaction = await getDebugState(owner, repo, seededRepo.cookieHeader);
    const supersededPackKey = stateAfterCompaction.supersededPacks?.[0]?.key;
    if (!supersededPackKey) throw new Error("missing superseded pack");

    await expect(env.REPO_BUCKET.head(supersededPackKey)).resolves.toBeTruthy();
    await expect(env.REPO_BUCKET.head(packIndexKey(supersededPackKey))).resolves.toBeTruthy();
    await expect(env.REPO_BUCKET.head(packRefsKey(supersededPackKey))).resolves.toBeTruthy();

    const packName = supersededPackKey.split("/").pop();
    if (!packName) throw new Error("missing pack name");

    const deleteResponse = await workerExports.default.fetch(
      `https://example.com/${owner}/${repo}/admin/pack/${encodeURIComponent(packName)}`,
      {
        method: "DELETE",
        headers: { Cookie: seededRepo.cookieHeader, Origin: "https://example.com" },
      }
    );
    expect(deleteResponse.status).toBe(200);
    const deleteJson = (await deleteResponse.json()) as {
      ok?: boolean;
      deletedPack?: boolean;
      deletedIndex?: boolean;
      deletedRefs?: boolean;
      deletedMetadata?: boolean;
      packState?: string;
    };
    expect(deleteJson.ok).toBe(true);
    expect(deleteJson.packState).toBe("superseded");
    expect(deleteJson.deletedPack).toBe(true);
    expect(deleteJson.deletedIndex).toBe(true);
    expect(deleteJson.deletedRefs).toBe(true);
    expect(deleteJson.deletedMetadata).toBe(true);

    await expect(env.REPO_BUCKET.head(supersededPackKey)).resolves.toBeNull();
    await expect(env.REPO_BUCKET.head(packIndexKey(supersededPackKey))).resolves.toBeNull();
    await expect(env.REPO_BUCKET.head(packRefsKey(supersededPackKey))).resolves.toBeNull();
  });

  it("returns retry when stock preparation appears before compaction commit", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-receive-priority");
    const seededRepo = await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const getStub = () => env.REPO_DO.get(env.REPO_DO.idFromName(repoId));
    await promoteToStreaming(owner, repo);

    await pushOverflowingStreamingHistory({
      owner,
      repo,
      repoId,
      startingCommitOid: seeded.nextCommit.oid,
      updates: 4,
    });

    const stub = getStub();
    await expireCompactionQuietPeriod(repoId);
    const begin = await stub.beginCompaction();
    expect(begin.ok).toBe(true);
    if (!begin.ok) {
      throw new Error("expected compaction to begin");
    }

    const stock = await stub.beginReceive({ stockPreparation: true });
    expect(stock).toMatchObject({ ok: true, stockPreparationReserved: true });
    if (!stock.ok) throw new Error("expected stock preparation to preempt compaction");

    const result = await stub.commitCompaction({
      token: begin.lease.token,
      sourcePacks: begin.sourcePacks,
      targetTier: begin.targetTier,
      packsetVersion: begin.packsetVersion,
      stagedPack: {
        packKey: `${begin.sourcePacks[0]!.packKey}.fake-compaction`,
        packBytes: begin.sourcePacks[0]!.packBytes,
        idxBytes: begin.sourcePacks[0]!.idxBytes,
        objectCount: begin.sourcePacks[0]!.objectCount,
      },
    });
    expect(result.status).toBe("retry");
    if (result.status === "retry") {
      expect(result.reason).toBe("receive-active");
    }
    expect(await stub.abortReceive(stock.lease.token)).toBe(true);

    const state = await getDebugState(owner, repo, seededRepo.cookieHeader);
    expect(state.activePacks?.every((pack) => pack.kind !== "compact")).toBe(true);
    expect(
      begin.sourcePacks.every((source) =>
        state.activePacks?.some((pack) => pack.key === source.packKey)
      )
    ).toBe(true);

    const promotionCompaction = await stub.beginCompaction();
    expect(promotionCompaction.ok).toBe(true);
    if (!promotionCompaction.ok) throw new Error("expected promotion compaction to begin");
    const promotedStock = await stub.beginReceive({ stockPreparation: true });
    if (!promotedStock.ok) throw new Error("expected stock preparation for promotion");
    expect(await stub.promoteStockPreparation(promotedStock.lease.token)).toBe(true);
    const promotedRetry = await stub.commitCompaction({
      token: promotionCompaction.lease.token,
      sourcePacks: promotionCompaction.sourcePacks,
      targetTier: promotionCompaction.targetTier,
      packsetVersion: promotionCompaction.packsetVersion,
      stagedPack: {
        packKey: `${promotionCompaction.sourcePacks[0]!.packKey}.fake-promotion-compaction`,
        packBytes: promotionCompaction.sourcePacks[0]!.packBytes,
        idxBytes: promotionCompaction.sourcePacks[0]!.idxBytes,
        objectCount: promotionCompaction.sourcePacks[0]!.objectCount,
      },
    });
    expect(promotedRetry).toMatchObject({ status: "retry", reason: "receive-active" });
    const promotedState = await getDebugState(owner, repo, seededRepo.cookieHeader);
    expect(
      promotionCompaction.sourcePacks.every((source) =>
        promotedState.activePacks?.some((pack) => pack.key === source.packKey)
      )
    ).toBe(true);
    expect(await stub.abortReceive(promotedStock.lease.token)).toBe(true);

    const operationCompaction = await stub.beginCompaction();
    expect(operationCompaction.ok).toBe(true);
    if (!operationCompaction.ok) throw new Error("expected operation compaction to begin");
    const operationStock = await stub.beginReceive({ stockPreparation: true });
    if (!operationStock.ok) throw new Error("expected stock preparation for admission");
    const operationId = "stock-operation-priority";
    const fingerprint = "1".repeat(64);
    const outputPackKey = nativeReceiveOutputPackKey(
      doPrefix(stub.id.toString()),
      operationId,
      fingerprint
    );
    const admitted = await stub.admitStockReceive({
      id: operationId,
      fingerprint,
      leaseToken: operationStock.lease.token,
      repositoryId: repoId,
      state: "staged",
      inputPackKey: nativeReceiveInputRequestKey(
        doPrefix(stub.id.toString()),
        operationStock.lease.token
      ),
      inputBytes: 64,
      inputEtag: "stock-operation-input",
      stockReceive: {
        inputRequestSha256: "2".repeat(64),
        packOffset: 16,
        packBytes: 48,
        advertisedRefs: operationStock.refs,
      },
      outputPackKey,
      outputIdxKey: packIndexKey(outputPackKey),
      outputRefsKey: packRefsKey(outputPackKey),
      commands: [],
      acceptedWrites: [],
      activeCatalog: operationStock.activeCatalog,
      catalogGeneration: operationStock.packsetVersion,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      attempts: 0,
      cleanupPending: false,
    });
    expect(admitted.status).toBe("admitted");
    const operationRetry = await stub.commitCompaction({
      token: operationCompaction.lease.token,
      sourcePacks: operationCompaction.sourcePacks,
      targetTier: operationCompaction.targetTier,
      packsetVersion: operationCompaction.packsetVersion,
      stagedPack: {
        packKey: `${operationCompaction.sourcePacks[0]!.packKey}.fake-operation-compaction`,
        packBytes: operationCompaction.sourcePacks[0]!.packBytes,
        idxBytes: operationCompaction.sourcePacks[0]!.idxBytes,
        objectCount: operationCompaction.sourcePacks[0]!.objectCount,
      },
    });
    expect(operationRetry).toMatchObject({ status: "retry", reason: "receive-active" });
    const operationState = await getDebugState(owner, repo, seededRepo.cookieHeader);
    expect(
      operationCompaction.sourcePacks.every((source) =>
        operationState.activePacks?.some((pack) => pack.key === source.packKey)
      )
    ).toBe(true);
    if (admitted.status === "admitted") {
      await stub.rejectStockReceiveExecution(admitted.executionToken, "test-cleanup");
      await stub.completeStockReceiveCleanup(operationId, fingerprint);
    }
    await runDOWithRetry(getStub, async (_instance, durableState) => {
      expect(await durableState.storage.get("compactLease")).toBeUndefined();
    });
  });

  it("returns retry when packsetVersion changes before compaction commit", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-packset-changed");
    const seededRepo = await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const getStub = () => env.REPO_DO.get(env.REPO_DO.idFromName(repoId));
    await promoteToStreaming(owner, repo);

    await pushOverflowingStreamingHistory({
      owner,
      repo,
      repoId,
      startingCommitOid: seeded.nextCommit.oid,
      updates: 4,
    });

    const stub = getStub();
    await expireCompactionQuietPeriod(repoId);
    const begin = await stub.beginCompaction();
    expect(begin.ok).toBe(true);
    if (!begin.ok) {
      throw new Error("expected compaction to begin");
    }

    // Bump the packset version behind the compaction lease's back.
    await runDOWithRetry(getStub, async (_instance, state) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      const current = (await store.get("packsetVersion")) || 0;
      await store.put("packsetVersion", current + 1);
    });

    const result = await stub.commitCompaction({
      token: begin.lease.token,
      sourcePacks: begin.sourcePacks,
      targetTier: begin.targetTier,
      packsetVersion: begin.packsetVersion,
      stagedPack: {
        packKey: `${begin.sourcePacks[0]!.packKey}.fake-compaction`,
        packBytes: begin.sourcePacks[0]!.packBytes,
        idxBytes: begin.sourcePacks[0]!.idxBytes,
        objectCount: begin.sourcePacks[0]!.objectCount,
      },
    });
    expect(result.status).toBe("retry");
    if (result.status === "retry") {
      expect(result.reason).toBe("packset-changed");
    }

    const state = await getDebugState(owner, repo, seededRepo.cookieHeader);
    expect(state.activePacks?.every((pack) => pack.kind !== "compact")).toBe(true);
  });

  it("compacts successfully when a non-source pack contains a duplicate identity REF_DELTA", async () => {
    // Regression test for the compaction self-referential delta bug.
    //
    // When the active catalog snapshot is newest-first and a newer non-source
    // pack contains a REF_DELTA whose resolved OID equals its baseOid (an
    // identity delta), resolveOrderedEntryByOid picks that entry for a needed
    // OID and the base chase loops back to the same entry, creating a cycle
    // that the topology sort cannot order.
    //
    // The fix reorders the compaction snapshot so source packs are searched
    // first, ensuring the authoritative full-object entry is selected.

    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-self-ref");
    const seededRepo = await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id);

    const author = "You <you@example.com> 0 +0000";

    // Shared blob that will appear in both a source pack and the newer
    // non-source pack as an identity REF_DELTA.
    const sharedBlobPayload = new TextEncoder().encode("shared content\n");
    const sharedBlob = await encodeGitObject("blob", sharedBlobPayload);

    // Build 4 source packs (oldest) with distinct commits, one containing the
    // shared blob. Each pack needs at least one unique object so the OID sets
    // differ and compaction has real work to do.
    const sourcePacks: Array<{ name: string; packBytes: Uint8Array }> = [];
    let parentOid: string | undefined;
    const allObjectOids: string[] = [];

    for (let i = 0; i < 4; i++) {
      const blobPayload = new TextEncoder().encode(`source content ${i}\n`);
      const blob = await encodeGitObject("blob", blobPayload);

      // Include the shared blob in the first source pack so it is a needed OID.
      const treeEntries = [{ mode: "100644" as const, name: `file-${i}.txt`, oid: blob.oid }];
      if (i === 0) {
        treeEntries.push({ mode: "100644" as const, name: "shared.txt", oid: sharedBlob.oid });
      }
      const treePayload = buildTreePayload(treeEntries);
      const tree = await encodeGitObject("tree", treePayload);

      const commitText =
        `tree ${tree.oid}\n` +
        (parentOid ? `parent ${parentOid}\n` : "") +
        `author ${author}\ncommitter ${author}\n\nsource ${i}\n`;
      const commit = await encodeGitObject("commit", new TextEncoder().encode(commitText));
      parentOid = commit.oid;

      const objects: Array<{ type: "blob" | "tree" | "commit"; payload: Uint8Array }> = [
        { type: "blob", payload: blobPayload },
        { type: "tree", payload: treePayload },
        { type: "commit", payload: new TextEncoder().encode(commitText) },
      ];
      if (i === 0) {
        objects.unshift({ type: "blob", payload: sharedBlobPayload });
      }

      sourcePacks.push({ name: `pack-source-${i}.pack`, packBytes: await buildPack(objects) });
      allObjectOids.push(blob.oid, tree.oid, commit.oid);
      if (i === 0) allObjectOids.push(sharedBlob.oid);
    }

    // Build the newest pack with an identity REF_DELTA for the shared blob.
    // The delta copies the entire base content, so the resolved OID equals
    // the base OID — exactly the scenario that triggers the self-loop.
    const identityDelta = buildAppendOnlyDelta(sharedBlobPayload, new Uint8Array(0));
    const newestPackBytes = await buildPack([
      { type: "ref-delta", baseOid: sharedBlob.oid, delta: identityDelta },
    ]);
    const newestPack = { name: "pack-newest-dup.pack", packBytes: newestPackBytes };

    // seedPackedRepoState expects packs newest-first; it indexes oldest-first
    // internally so the REF_DELTA base in the source packs is available.
    const lastCommitOid = parentOid!;
    await seedPackedRepoState({
      env,
      repoId,
      getStub,
      packs: [newestPack, ...sourcePacks],
      refs: [{ name: "refs/heads/main", oid: lastCommitOid }],
      head: { target: "refs/heads/main", oid: lastCommitOid },
    });

    // Verify compaction plan selects the 4 source packs (oldest tier-0).
    const preState = await getDebugState(owner, repo, seededRepo.cookieHeader);
    expect(preState.activePacks?.length).toBe(5);
    expect(preState.activePacks?.filter((p) => p.tier === 0).length).toBe(5);

    // Request and run compaction.
    const stub = getStub();
    const request = await stub.requestCompaction();
    expect(request.status).toBe("queued");

    const result = await compactOnce(repoId);
    expect(result.acked).toBe(true);
    expect(result.retried).toBe(false);

    // Verify post-compaction state: one compacted pack, 4 source packs superseded.
    const postState = await getDebugState(owner, repo, seededRepo.cookieHeader);
    expect(postState.activePacks?.some((p) => p.kind === "compact")).toBe(true);
    expect(postState.supersededPacks?.length).toBe(4);
    expect(postState.compaction?.queued).toBe(false);
  });

  it("fetch after compaction does not produce duplicate REF_DELTA entries", async () => {
    // Regression test for broken `git pull` after compaction.
    //
    // After compaction, the active snapshot is newest-first:
    //   [compacted pack (newest seqHi), non-source pack (older)]
    // If the non-source pack has an OFS_DELTA whose in-pack base chain
    // reaches an entry with the same OID as a full object in the compacted
    // pack, the OFS_DELTA base chase adds that entry by pack-local offset —
    // bypassing OID-level dedup. The output pack then has two entries for
    // the same OID, causing git's index-pack to reject the pack with
    // "REF_DELTA at offset X already resolved (duplicate base Y)".
    //
    // The fix canonicalizes OFS_DELTA bases via resolveOrderedEntryByOid so
    // the position-based dedup in addEntry catches cross-pack duplicates.

    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-fetch-dup");
    const seededRepo = await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id);

    const author = "You <you@example.com> 0 +0000";

    // Shared blob: appears as a full object in a source pack, and as an
    // identity REF_DELTA in the non-source pack. After compaction, both
    // the compacted pack and the non-source pack contain this OID.
    const sharedBlobPayload = new TextEncoder().encode("shared content for fetch test\n");
    const sharedBlob = await encodeGitObject("blob", sharedBlobPayload);

    // Child blob: stored as OFS_DELTA based on the shared blob in the
    // non-source pack. Its base chase is the path that triggers the
    // cross-pack duplicate. Must be referenced by a tree to be needed.
    const childBlobFullPayload = new Uint8Array([
      ...sharedBlobPayload,
      ...new TextEncoder().encode("extra\n"),
    ]);
    const childBlob = await encodeGitObject("blob", childBlobFullPayload);

    // Build 4 source packs with distinct commits. Source pack 0 includes
    // the shared blob as a full object.
    const sourcePacks: Array<{ name: string; packBytes: Uint8Array }> = [];
    let parentOid: string | undefined;

    for (let i = 0; i < 4; i++) {
      const blobPayload = new TextEncoder().encode(`source content fetch ${i}\n`);
      const blob = await encodeGitObject("blob", blobPayload);

      const treeEntries = [{ mode: "100644" as const, name: `file-${i}.txt`, oid: blob.oid }];
      if (i === 0) {
        treeEntries.push({ mode: "100644" as const, name: "shared.txt", oid: sharedBlob.oid });
      }
      const treePayload = buildTreePayload(treeEntries);
      const tree = await encodeGitObject("tree", treePayload);

      const commitText =
        `tree ${tree.oid}\n` +
        (parentOid ? `parent ${parentOid}\n` : "") +
        `author ${author}\ncommitter ${author}\n\nsource fetch ${i}\n`;
      const commit = await encodeGitObject("commit", new TextEncoder().encode(commitText));
      parentOid = commit.oid;

      const objects: Array<{ type: "blob" | "tree" | "commit"; payload: Uint8Array }> = [
        { type: "blob", payload: blobPayload },
        { type: "tree", payload: treePayload },
        { type: "commit", payload: new TextEncoder().encode(commitText) },
      ];
      if (i === 0) {
        objects.unshift({ type: "blob", payload: sharedBlobPayload });
      }

      sourcePacks.push({ name: `pack-src-${i}.pack`, packBytes: await buildPack(objects) });
    }

    // Build a newest pack that contributes a real commit to the graph.
    // Layout (by entry index):
    //   0: identity REF_DELTA for sharedBlob (resolves to sharedBlob.oid)
    //   1: OFS_DELTA child blob based on entry 0 (resolves to childBlob.oid)
    //   2: tree referencing the child blob
    //   3: commit (child of last source commit) referencing tree 2
    //
    // During fetch, the child blob (entry 1) is needed because the tree
    // references it. Its OFS_DELTA base chase reaches entry 0 (shared blob
    // OID), which was already selected from the compacted pack — the
    // cross-pack duplicate that this test guards against.
    const identityDelta = buildAppendOnlyDelta(sharedBlobPayload, new Uint8Array(0));
    const childDelta = buildAppendOnlyDelta(sharedBlobPayload, new TextEncoder().encode("extra\n"));

    const newestTreePayload = buildTreePayload([
      { mode: "100644" as const, name: "child.txt", oid: childBlob.oid },
      { mode: "100644" as const, name: "shared.txt", oid: sharedBlob.oid },
    ]);
    const newestTree = await encodeGitObject("tree", newestTreePayload);

    const newestCommitText =
      `tree ${newestTree.oid}\n` +
      `parent ${parentOid}\n` +
      `author ${author}\ncommitter ${author}\n\nnewest with child blob\n`;
    const newestCommit = await encodeGitObject(
      "commit",
      new TextEncoder().encode(newestCommitText)
    );

    const newestPackBytes = await buildPack([
      { type: "ref-delta", baseOid: sharedBlob.oid, delta: identityDelta },
      { type: "ofs-delta", baseIndex: 0, delta: childDelta },
      { type: "tree", payload: newestTreePayload },
      { type: "commit", payload: new TextEncoder().encode(newestCommitText) },
    ]);
    const newestPack = { name: "pack-newest-fetch-dup.pack", packBytes: newestPackBytes };

    // HEAD = newest commit so the fetch graph reaches objects in every pack.
    await seedPackedRepoState({
      env,
      repoId,
      getStub,
      packs: [newestPack, ...sourcePacks],
      refs: [{ name: "refs/heads/main", oid: newestCommit.oid }],
      head: { target: "refs/heads/main", oid: newestCommit.oid },
    });

    // Run compaction — merges the 4 source packs into one compacted pack.
    // The non-source pack (newest) stays active.
    const stub = getStub();
    await stub.requestCompaction();
    const compactResult = await compactOnce(repoId);
    expect(compactResult.acked).toBe(true);
    expect(compactResult.retried).toBe(false);

    const postState = await getDebugState(owner, repo, seededRepo.cookieHeader);
    expect(postState.activePacks?.some((p) => p.kind === "compact")).toBe(true);

    // Fetch all objects (clone scenario) — this is the path that was broken.
    const fetchResponse = await workerExports.default.fetch(
      `https://example.com/${owner}/${repo}/git-upload-pack`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-git-upload-pack-request",
          "Git-Protocol": "version=2",
        },
        body: buildFetchBody({ wants: [newestCommit.oid], done: true }),
      } as any
    );
    expect(fetchResponse.status).toBe(200);

    // Extract sideband-encoded pack bytes from the response.
    const bytes = new Uint8Array(await fetchResponse.arrayBuffer());
    const lines = decodePktLines(bytes);
    const packChunks: Uint8Array[] = [];
    let inPackfile = false;
    for (const line of lines) {
      if (line.type === "line" && line.text === "packfile\n") {
        inPackfile = true;
        continue;
      }
      if (inPackfile && line.type === "line" && line.raw && line.raw[0] === 0x01) {
        packChunks.push(line.raw.subarray(1));
      }
    }
    const packOut = concatChunks(packChunks);

    // Basic pack header sanity check.
    expect(new TextDecoder().decode(packOut.subarray(0, 4))).toBe("PACK");

    // Index the returned pack and verify no duplicate OIDs.
    const verifyKey = `verify/compaction-fetch-dup-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(verifyKey, packOut);
    const verifyResult = await indexTestPack(env, verifyKey, packOut.byteLength);

    const oidSet = new Set<string>();
    for (let i = 0; i < verifyResult.idxView.count; i++) {
      const oidBytes = verifyResult.idxView.rawNames.subarray(i * 20, (i + 1) * 20);
      oidSet.add(bytesToHex(oidBytes));
    }
    // Intentionally stricter than git index-pack (which tolerates duplicate
    // full objects and OFS identity deltas, but rejects duplicate REF_DELTAs).
    // Our rewrite should never produce ANY duplicate OIDs in the output pack.
    expect(oidSet.size).toBe(verifyResult.idxView.count);

    // Also verify after superseded pack deletion — compacted pack is sole
    // source for the merged objects, no duplicates possible.
    const supersededKeys =
      postState.supersededPacks?.map((p) => p.key).filter((k): k is string => !!k) ?? [];
    if (supersededKeys.length > 0) {
      await deleteSupersededOnce(repoId, supersededKeys);
    }

    const fetchResponse2 = await workerExports.default.fetch(
      `https://example.com/${owner}/${repo}/git-upload-pack`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-git-upload-pack-request",
          "Git-Protocol": "version=2",
        },
        body: buildFetchBody({ wants: [newestCommit.oid], done: true }),
      } as any
    );
    expect(fetchResponse2.status).toBe(200);

    const bytes2 = new Uint8Array(await fetchResponse2.arrayBuffer());
    const lines2 = decodePktLines(bytes2);
    const packChunks2: Uint8Array[] = [];
    let inPackfile2 = false;
    for (const line of lines2) {
      if (line.type === "line" && line.text === "packfile\n") {
        inPackfile2 = true;
        continue;
      }
      if (inPackfile2 && line.type === "line" && line.raw && line.raw[0] === 0x01) {
        packChunks2.push(line.raw.subarray(1));
      }
    }
    const packOut2 = concatChunks(packChunks2);
    expect(new TextDecoder().decode(packOut2.subarray(0, 4))).toBe("PACK");

    const verifyKey2 = `verify/compaction-fetch-post-delete-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(verifyKey2, packOut2);
    const verifyResult2 = await indexTestPack(env, verifyKey2, packOut2.byteLength);

    const oidSet2 = new Set<string>();
    for (let i = 0; i < verifyResult2.idxView.count; i++) {
      const oidBytes = verifyResult2.idxView.rawNames.subarray(i * 20, (i + 1) * 20);
      oidSet2.add(bytesToHex(oidBytes));
    }
    expect(oidSet2.size).toBe(verifyResult2.idxView.count);
  });

  it("keeps active pack counts bounded after repeated pushes and compaction drains", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-bounded");
    const seededRepo = await setupRepoForTests(env, owner, repo);
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    await pushOverflowingStreamingHistory({
      owner,
      repo,
      repoId,
      startingCommitOid: seeded.nextCommit.oid,
      updates: 8,
    });

    for (let attempt = 0; attempt < 6; attempt++) {
      const queuedState = await getDebugState(owner, repo, seededRepo.cookieHeader);
      if (!queuedState.compaction?.queued) break;
      const result = await compactOnce(repoId);
      expect(result.acked || result.retried).toBe(true);
    }

    const finalState = await getDebugState(owner, repo, seededRepo.cookieHeader);
    const counts = new Map<number, number>();
    for (const pack of finalState.activePacks || []) {
      counts.set(pack.tier, (counts.get(pack.tier) || 0) + 1);
    }
    for (const count of counts.values()) {
      expect(count).toBeLessThanOrEqual(4);
    }
  });
});
