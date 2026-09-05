import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import { bytesToHex, getRepoStub } from "@/worker/common";
import { encodeGitObject } from "@/worker/git/core";
import {
  GC_OPERATION_KEY,
  GC_UNPUBLISHED_LIFETIME_MS,
  type GcOperation,
} from "@/worker/git/maintenance/gcOperation";
import { __test as nativeReceiveTest } from "@/worker/do/repo/nativeReceive";
import { NativeProcessorError } from "@/worker/do/repo/nativeReceive";
import { getOidHexAt, loadIdxView } from "@/worker/git/object-store";
import { __test as durableGcTest } from "@/worker/git/maintenance/durableGc";
import { recordAcceptedWrites } from "@/worker/do/repo/acceptedWrites";
import { asTypedStorage, type RepoStateSchema } from "@/worker/do/repo/repoState";
import { packIndexKey, packRefsKey } from "@/worker/keys";
import type { NativeReceiveProcessResult } from "@/worker/git/nativeReceive/types";
import { buildPack, uniqueRepoId } from "./util/test-helpers";
import { seedPackedRepoState } from "./util/packed-repo";
import { indexTestPack } from "./util/test-indexer";
import { runQueueMessage } from "./util/queue";
import { deleteSupersededOnce } from "./util/compaction-helpers";
import * as packRewrite from "@/worker/git/pack/rewrite";
import { runAlarmWithRetry } from "./util/test-helpers";
import { deletePackCatalogRows, getDb } from "@/worker/do/repo/db";

async function verifiedPut(key: string, bytes: Uint8Array) {
  await env.REPO_BUCKET.put(key, bytes, {
    customMetadata: {
      sha256: bytesToHex(
        new Uint8Array(await crypto.subtle.digest("SHA-256", Uint8Array.from(bytes)))
      ),
    },
  });
}

describe("durable GC queue lifecycle", () => {
  it("persists snapshot pins in the durable source and includes them in closure planning", async () => {
    const repoId = uniqueRepoId("durable-pinned-root");
    const stub = getRepoStub(env, repoId);
    const refPayload = new TextEncoder().encode("ref root\n");
    const pinPayload = new TextEncoder().encode("snapshot-only root\n");
    const refObject = await encodeGitObject("blob", refPayload);
    const pinObject = await encodeGitObject("blob", pinPayload);
    const seeded = await seedPackedRepoState({
      env,
      repoId,
      getStub: () => stub,
      packs: [
        {
          name: "ref-and-pin.pack",
          packBytes: await buildPack([
            { type: "blob", payload: refPayload },
            { type: "blob", payload: pinPayload },
          ]),
        },
      ],
      refs: [{ name: "refs/heads/main", oid: refObject.oid }],
    });
    await runInDurableObject(stub, async (_, state) => {
      await state.storage.put(`snapshotPin:${pinObject.oid}`, {
        commitSha: pinObject.oid,
        treeSha: "a".repeat(40),
        ref: "refs/heads/previous",
        beforeSha: "0".repeat(40),
        firstSequence: 1,
        acceptedAt: Date.now(),
        actor: "durable-gc-test",
        sourceSurface: "ingestion",
        idempotencyKey: "durable-gc-test",
      });
    });
    await stub.registerGcOperation(repoId, "durable-pinned-root");
    const message = {
      kind: "reachability-gc" as const,
      repoId,
      doId: stub.id.toString(),
      operationId: "durable-pinned-root",
    };
    await runQueueMessage(message);
    expect(await stub.getGcOperation()).toMatchObject({
      phase: "publish",
      snapshot: {
        snapshotPinOids: [pinObject.oid],
      },
      closure: { objectCount: 2 },
      retainedPackKey: seeded.packKeys[0],
    });
  });

  it("releases its new lease and claim when pin roots change before snapshot registration", async () => {
    const repoId = uniqueRepoId("durable-pin-registration-race");
    const stub = getRepoStub(env, repoId);
    const payload = new TextEncoder().encode("retained\n");
    const retained = await encodeGitObject("blob", payload);
    await seedPackedRepoState({
      env,
      repoId,
      getStub: () => stub,
      packs: [{ name: "retained.pack", packBytes: await buildPack([{ type: "blob", payload }]) }],
      refs: [{ name: "refs/tags/retained", oid: retained.oid }],
    });
    await stub.registerGcOperation(repoId, "pin-registration-race");
    const commitSha = "d".repeat(40);
    durableGcTest.setBeforeSnapshotRegistrationObserver(async () => {
      await runInDurableObject(stub, async (_instance, state) => {
        await state.storage.transaction(async (transaction) => {
          await recordAcceptedWrites(
            asTypedStorage<RepoStateSchema>(transaction),
            2,
            [
              {
                repositoryId: repoId,
                ref: "refs/heads/archived",
                beforeSha: "0".repeat(40),
                afterSha: commitSha,
                actor: "durable-pin-race-test",
                sourceSurface: "git-push",
                idempotencyKey: null,
              },
            ],
            Date.now(),
            true,
            { commitSha, treeSha: "e".repeat(40), materializedAt: Date.now() }
          );
        });
      });
    });
    try {
      await expect(
        runQueueMessage({
          kind: "reachability-gc",
          repoId,
          doId: stub.id.toString(),
          operationId: "pin-registration-race",
        })
      ).resolves.toEqual({ acked: false, retried: true });
      await runInDurableObject(stub, async (_instance, state) => {
        await expect(state.storage.get("compactLease")).resolves.toBeUndefined();
      });
      const operation = await stub.getGcOperation();
      expect(operation).toMatchObject({ phase: "queued" });
      expect(operation?.claim).toBeUndefined();
    } finally {
      durableGcTest.setBeforeSnapshotRegistrationObserver(undefined);
    }
  });

  it("bounds ordinary retryable native failure, drains its claim and restores the source write path", async () => {
    const repoId = uniqueRepoId("ordinary-gc-deadline");
    const stub = getRepoStub(env, repoId);
    const payload = new TextEncoder().encode("retained\n");
    const retained = await encodeGitObject("blob", payload);
    const seeded = await seedPackedRepoState({
      env,
      repoId,
      getStub: () => stub,
      packs: [
        {
          name: "mixed.pack",
          packBytes: await buildPack([
            { type: "blob", payload },
            { type: "blob", payload: new TextEncoder().encode("discarded\n") },
          ]),
        },
      ],
      refs: [{ name: "refs/tags/retained", oid: retained.oid }],
    });
    const admitted = await stub.registerGcOperation(repoId, "deadline");
    if (admitted.status !== "ready") throw new Error("setup failed");
    const run = () =>
      runQueueMessage({
        kind: "reachability-gc",
        repoId,
        doId: stub.id.toString(),
        operationId: "deadline",
      });
    await run();
    const native = vi.fn(async () => {
      throw new NativeProcessorError("native_processor_failed", "native timed out", true);
    });
    nativeReceiveTest.setNativeProcessor(native);
    try {
      await run();
      expect(await stub.getGcOperation()).toMatchObject({ phase: "index" });
      expect(await stub.setRefs([{ name: "refs/tags/retained", oid: retained.oid }])).toBe(false);
      await runInDurableObject(stub, async (_, state) => {
        const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
        operation!.createdAt = Date.now() - GC_UNPUBLISHED_LIFETIME_MS - 1;
        operation!.claim!.expiresAt = Date.now() - 1;
        await state.storage.put(GC_OPERATION_KEY, operation);
      });
      await run();
      expect(await stub.getGcOperation()).toMatchObject({ phase: "index" });
      expect(await env.REPO_BUCKET.head(admitted.operation.inputPackKey)).not.toBeNull();
      await runInDurableObject(stub, async (_, state) => {
        const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
        operation!.claim!.safeRetryAt = Date.now() - 1;
        await state.storage.put(GC_OPERATION_KEY, operation);
      });
      await runAlarmWithRetry(() => stub);
      await run();
      expect(await stub.getGcOperation()).toMatchObject({
        phase: "blocked",
        blockedReason: "operation-deadline",
        measurements: { rewrite: { attempts: 1 } },
      });
      expect(native).toHaveBeenCalledTimes(1);
      expect(await env.REPO_BUCKET.head(admitted.operation.inputPackKey)).toBeNull();
      expect(await env.REPO_BUCKET.head(seeded.packKeys[0]!)).not.toBeNull();
      expect(await stub.listRefs()).toEqual([{ name: "refs/tags/retained", oid: retained.oid }]);
      expect(
        await runInDurableObject(stub, async (_, state) => state.storage.get("compactLease"))
      ).toBeUndefined();
      expect(await stub.setRefs([{ name: "refs/tags/retained", oid: retained.oid }])).toBe(true);
    } finally {
      nativeReceiveTest.reset();
    }
  });

  it.each(["invalid-receipt", "permanent-upload-rejection"])(
    "drains and discards %s without repeating rewrite or touching sources",
    async (failure) => {
      const repoId = uniqueRepoId("invalid-native-receipt");
      const stub = getRepoStub(env, repoId);
      const payload = new TextEncoder().encode("retained\n");
      const retained = await encodeGitObject("blob", payload);
      const source = await buildPack([
        { type: "blob", payload },
        { type: "blob", payload: new TextEncoder().encode("discarded\n") },
      ]);
      const seeded = await seedPackedRepoState({
        env,
        repoId,
        getStub: () => stub,
        packs: [{ name: "mixed.pack", packBytes: source }],
        refs: [{ name: "refs/tags/retained", oid: retained.oid }],
      });
      const admitted = await stub.registerGcOperation(repoId, "invalid-receipt");
      if (admitted.status !== "ready") throw new Error("setup failed");
      const run = () =>
        runQueueMessage({
          kind: "reachability-gc",
          repoId,
          doId: stub.id.toString(),
          operationId: "invalid-receipt",
        });
      await run();
      expect(await stub.getGcOperation()).toMatchObject({ phase: "index" });
      if (failure === "invalid-receipt")
        await verifiedPut(admitted.operation.outputResultKey, new TextEncoder().encode("{}"));
      else
        nativeReceiveTest.setNativeProcessor(async () => {
          throw new NativeProcessorError("invalid-pack", "permanent bridge rejection", false);
        });
      try {
        await run();
      } finally {
        nativeReceiveTest.reset();
      }
      expect(await stub.getGcOperation()).toMatchObject({
        phase: "discard",
        blockedReason: "native-rejected",
      });
      expect(await env.REPO_BUCKET.head(admitted.operation.inputPackKey)).not.toBeNull();
      await runInDurableObject(stub, async (_, state) => {
        const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
        operation!.discardAfter = Date.now() - 1;
        await state.storage.put(GC_OPERATION_KEY, operation);
      });
      await run();
      expect(await stub.getGcOperation()).toMatchObject({
        phase: "blocked",
        blockedReason: "native-rejected",
        measurements: { rewrite: { attempts: 1 } },
      });
      expect(await env.REPO_BUCKET.head(admitted.operation.inputPackKey)).toBeNull();
      expect(await env.REPO_BUCKET.head(admitted.operation.outputResultKey)).toBeNull();
      expect(await env.REPO_BUCKET.head(seeded.packKeys[0]!)).not.toBeNull();
      expect(
        await runInDurableObject(stub, async (_, state) => await state.storage.get("compactLease"))
      ).toBeUndefined();
    }
  );

  it("terminates a permanent planner rejection and releases its write fence without deleting source data", async () => {
    const repoId = uniqueRepoId("permanent-rewrite-failure");
    const stub = getRepoStub(env, repoId);
    const payload = new TextEncoder().encode("retained\n");
    const retained = await encodeGitObject("blob", payload);
    const source = await buildPack([
      { type: "blob", payload },
      { type: "blob", payload: new TextEncoder().encode("discarded\n") },
    ]);
    const seeded = await seedPackedRepoState({
      env,
      repoId,
      getStub: () => stub,
      packs: [{ name: "mixed.pack", packBytes: source }],
      refs: [{ name: "refs/tags/retained", oid: retained.oid }],
    });
    await stub.registerGcOperation(repoId, "permanent-failure");
    const planner = vi.spyOn(packRewrite, "rewritePackResult").mockResolvedValue({
      status: "failed",
      failure: { reason: "synthetic-object-too-large", retryable: false },
    });
    try {
      await runQueueMessage({
        kind: "reachability-gc",
        repoId,
        doId: stub.id.toString(),
        operationId: "permanent-failure",
      });
      expect(await stub.getGcOperation()).toMatchObject({
        phase: "blocked",
        blockedReason: "synthetic-object-too-large",
      });
      expect(
        await runInDurableObject(stub, async (_, state) => await state.storage.get("compactLease"))
      ).toBeUndefined();
      await runAlarmWithRetry(() => stub);
      await runQueueMessage({
        kind: "reachability-gc",
        repoId,
        doId: stub.id.toString(),
        operationId: "permanent-failure",
      });
      expect(planner).toHaveBeenCalledTimes(1);
      expect(await env.REPO_BUCKET.head(seeded.packKeys[0]!)).not.toBeNull();
      expect((await stub.beginReachabilityGc()).ok).toBe(true);
    } finally {
      planner.mockRestore();
    }
  });

  it("automatically discards an expired rewrite after another generation replaced and reclaimed its sources", async () => {
    const repoId = uniqueRepoId("stale-rewrite-source");
    const stub = getRepoStub(env, repoId);
    const payload = new TextEncoder().encode("retained\n");
    const retained = await encodeGitObject("blob", payload);
    const source = await buildPack([
      { type: "blob", payload },
      { type: "blob", payload: new TextEncoder().encode("discarded\n") },
    ]);
    const seeded = await seedPackedRepoState({
      env,
      repoId,
      getStub: () => stub,
      packs: [{ name: "mixed.pack", packBytes: source }],
      refs: [{ name: "refs/tags/retained", oid: retained.oid }],
    });
    const admitted = await stub.registerGcOperation(repoId, "stale-rewrite");
    const claim = await stub.claimGcOperation("stale-rewrite");
    const begin = await stub.beginReachabilityGc();
    if (
      admitted.status !== "ready" ||
      claim.status !== "ready" ||
      !claim.operation.claim ||
      !begin.ok
    )
      throw new Error("setup failed");
    await stub.recordGcProgress("stale-rewrite", claim.operation.claim.id, {
      kind: "snapshot",
      snapshot: {
        token: begin.lease.token,
        refs: begin.refs,
        snapshotPinVersion: begin.snapshotPinVersion,
        refsVersion: begin.refsVersion,
        packsetVersion: begin.packsetVersion,
        sourcePacks: begin.activeCatalog,
      },
    });
    await runInDurableObject(stub, async (_, state) => {
      const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
      operation!.claim!.expiresAt = Date.now() - 1;
      operation!.claim!.safeRetryAt = Date.now() - 1;
      await state.storage.put(GC_OPERATION_KEY, operation);
      await state.storage.delete("compactLease");
    });
    // The fixture helper installs the intervening writer's new catalog. The
    // old source artifacts really disappear before the durable wakeup runs.
    const replacement = await buildPack([{ type: "blob", payload }]);
    const newer = await seedPackedRepoState({
      env,
      repoId,
      getStub: () => stub,
      packs: [{ name: "newer.pack", packBytes: replacement }],
      refs: [{ name: "refs/tags/retained", oid: retained.oid }],
    });
    await runInDurableObject(stub, async (_, state) => {
      await deletePackCatalogRows(getDb(state.storage), seeded.packKeys);
      await state.storage.put("packsetVersion", begin.packsetVersion + 1);
    });
    await env.REPO_BUCKET.delete(
      seeded.packKeys.flatMap((key) => [key, packIndexKey(key), packRefsKey(key)])
    );
    await runQueueMessage({
      kind: "reachability-gc",
      repoId,
      doId: stub.id.toString(),
      operationId: "stale-rewrite",
    });
    expect(await stub.getGcOperation()).toMatchObject({
      phase: "blocked",
      blockedReason: "source-changed",
    });
    expect(await env.REPO_BUCKET.head(newer.packKeys[0]!)).not.toBeNull();
    expect(await env.REPO_BUCKET.head(admitted.operation.outputPackKey)).toBeNull();
    expect(await stub.listRefs()).toEqual([{ name: "refs/tags/retained", oid: retained.oid }]);
  });

  it("reuses a completed rewrite and native receipt after lost replies, publishes once and waits for its real reader", async () => {
    const repoId = uniqueRepoId("durable-rewrite");
    const stub = getRepoStub(env, repoId);
    const payload = new TextEncoder().encode("retained content\n");
    const retained = await encodeGitObject("blob", payload);
    const sourcePack = await buildPack([
      { type: "blob", payload },
      { type: "blob", payload: new TextEncoder().encode("unreachable content\n") },
    ]);
    const seeded = await seedPackedRepoState({
      env,
      repoId,
      getStub: () => stub,
      packs: [{ name: "mixed.pack", packBytes: sourcePack }],
      refs: [{ name: "refs/tags/retained", oid: retained.oid }],
    });
    const registered = await stub.registerGcOperation(repoId, "rewrite-recovery");
    if (registered.status !== "ready") throw new Error("admission failed");
    const inputKey = registered.operation.inputPackKey;
    const run = () =>
      runQueueMessage({
        kind: "reachability-gc",
        repoId,
        doId: stub.id.toString(),
        operationId: "rewrite-recovery",
      });
    const put = env.REPO_BUCKET.put.bind(env.REPO_BUCKET);
    let inputWrites = 0;
    const lostUpload = vi
      .spyOn(env.REPO_BUCKET, "put")
      .mockImplementation(async (...args: Parameters<typeof put>) => {
        const result = await put(...args);
        if (args[0] === inputKey && ++inputWrites === 1)
          throw new Error("lost completed upload response");
        return result;
      });
    let nativeCalls = 0;
    // This is a protocol double, not a native benchmark. The Go tests execute
    // native Git and independently cover the maintained index/sidecar builder.
    nativeReceiveTest.setNativeProcessor(async ({ request }) => {
      nativeCalls++;
      expect(request.commands).toEqual([]);
      expect(request.activePacks).toEqual([]);
      const input = await env.REPO_BUCKET.get(request.inputPackKey);
      const bytes = new Uint8Array(await input!.arrayBuffer());
      await verifiedPut(request.outputPackKey, bytes);
      const index = await indexTestPack(env, request.outputPackKey, bytes.length);
      const idxObject = await env.REPO_BUCKET.get(packIndexKey(request.outputPackKey));
      const refsObject = await env.REPO_BUCKET.get(packRefsKey(request.outputPackKey));
      const idx = new Uint8Array(await idxObject!.arrayBuffer());
      const refs = new Uint8Array(await refsObject!.arrayBuffer());
      await verifiedPut(request.outputIdxKey, idx);
      await verifiedPut(request.outputRefsKey, refs);
      const result: NativeReceiveProcessResult = {
        operationId: request.operationId,
        packBytes: bytes.length,
        idxBytes: idx.length,
        refsBytes: refs.length,
        objectCount: index.objectCount,
        packSha1: bytesToHex(bytes.subarray(-20)),
        elapsedMs: 1,
        scratchBytes: bytes.length + idx.length + refs.length,
        hydratedBytes: 0,
        downloadedBytes: bytes.length,
        cacheHitBytes: 0,
        maintenance: {
          objectSetSha256: request.maintenance!.objectSetSha256,
          downloadMs: 0,
          indexMs: 0,
          validationMs: 0,
          referenceMs: 0,
          uploadMs: 0,
          downloadBytes: bytes.length,
          uploadBytes: bytes.length + idx.length + refs.length,
          downloadRequests: 1,
          uploadRequests: 3,
        },
      };
      await verifiedPut(
        request.maintenance!.resultKey,
        new TextEncoder().encode(JSON.stringify(result))
      );
      throw new Error("lost native completion response");
    });
    try {
      await run();
      expect(await env.REPO_BUCKET.head(inputKey)).not.toBeNull();
      expect(await stub.getGcOperation()).toMatchObject({
        phase: "rewrite",
        rewriteIntent: { packBytes: expect.any(Number) },
      });
      await runInDurableObject(stub, async (_, state) => {
        const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
        operation!.claim!.expiresAt = Date.now() - 1;
        operation!.claim!.safeRetryAt = Date.now() - 1;
        await state.storage.put(GC_OPERATION_KEY, operation);
      });
      await run();
      expect(await stub.getGcOperation()).toMatchObject({ phase: "index" });
      expect(inputWrites).toBe(1);
      await run();
      expect(await stub.getGcOperation()).toMatchObject({ phase: "publish" });
      expect(nativeCalls).toBe(1);
      const reader = await stub.beginRepositoryRead();
      if (!reader.ok) throw new Error("reader admission failed");
      await run();
      const published = await stub.getGcOperation();
      expect(published).toMatchObject({
        phase: "reclaim",
        commit: { status: "committed", supersededPackKeys: seeded.packKeys },
      });
      await run();
      await deleteSupersededOnce(
        repoId,
        seeded.packKeys,
        true,
        published!.commit!.packCatalogVersion
      );
      expect(await env.REPO_BUCKET.head(seeded.packKeys[0]!)).not.toBeNull();
      await stub.finishRepositoryRead(reader.token);
      await deleteSupersededOnce(
        repoId,
        seeded.packKeys,
        true,
        published!.commit!.packCatalogVersion
      );
      await run();
      expect(await stub.getGcOperation()).toMatchObject({ phase: "complete" });
      expect(await stub.listRefs()).toEqual([{ name: "refs/tags/retained", oid: retained.oid }]);
      expect(await env.REPO_BUCKET.head(inputKey)).toBeNull();
      expect(await env.REPO_BUCKET.head(registered.operation.outputResultKey)).toBeNull();
      expect(await env.REPO_BUCKET.head(seeded.packKeys[0]!)).toBeNull();
      const outputIndex = await loadIdxView(
        env,
        registered.operation.outputPackKey,
        undefined,
        published!.nativeResult!.packBytes
      );
      expect(outputIndex!.count).toBe(1);
      expect(getOidHexAt(outputIndex!, 0)).toBe(retained.oid);
      await run();
      expect(nativeCalls).toBe(1);
      expect(inputWrites).toBe(1);
    } finally {
      lostUpload.mockRestore();
      nativeReceiveTest.reset();
    }
  });
});
