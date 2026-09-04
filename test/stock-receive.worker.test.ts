import { afterEach, describe, expect, it, vi } from "vitest";
import { createExecutionContext, runDurableObjectAlarm } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";

import { asBufferSource, bytesToHex, createLogger, getRepoStub } from "@/worker/common";
import { __admissionTest as receiveLeaseTest } from "@/worker/do/repo/catalog/leases";
import { __test as receiveCatalogTest } from "@/worker/do/repo/catalog/receive";
import { __test as stockAuthorityTest } from "@/worker/do/repo/stockReceiveAuthority";
import { __test as stockContainerHostTest } from "@/worker/do/stockReceiveContainerHost";
import {
  asTypedStorage,
  nativeReceiveOperationKey,
  type RepoStateSchema,
} from "@/worker/do/repo/repoState";
import { EXPIRED_WRITER_DRAIN_MS } from "@/worker/do/repo/repositoryLifecycle";
import { concatChunks, flushPkt, pktLine } from "@/worker/git/core";
import { encodeGitObject } from "@/worker/git/core/objects";
import { SubrequestLimiter } from "@/worker/git/operations/limits";
import { readObject } from "@/worker/git/object-store";
import { publishRepositoryGeneration } from "@/worker/git/generation/publish";
import { nativeReceiveOperationEvidenceMatches } from "@/worker/git/nativeReceive/types";
import {
  __test as stockDataPlaneTest,
  cleanupStockReceiveWorkerDataPlane,
  executeStockReceiveWorkerDataPlane,
} from "@/worker/git/nativeReceive/stockDataPlane";
import type {
  AdmitStockReceiveResult,
  NativeReceiveAuthorityPublication,
  NativeReceiveOperation,
  NativeReceiveOperationMetrics,
  NativeReceivePrepared,
  NativeReceiveProcessResult,
} from "@/worker/git/nativeReceive/types";
import {
  __test as stockPlannerTest,
  planStockReceive,
  stockReceivePlanKeys,
  StockReceivePlannerError,
} from "@/worker/git/nativeReceive/stockPlanner";
import { validateStockReceivePreparedProof } from "@/worker/git/nativeReceive/stockProof";
import { __test as nativePipelineTest } from "@/worker/git/receive/nativePipeline";
import { handleStreamingReceivePackPOST } from "@/worker/git/receive/streamReceivePack";
import { buildSidebandReceiveBody } from "@/worker/git/receive/response";
import {
  doPrefix,
  nativeReceiveInputRequestKey,
  nativeReceiveOutputPackKey,
  packIndexKey,
  packRefsKey,
  r2PackKey,
} from "@/worker/keys";
import { buildAppendOnlyDelta, buildPack } from "./util/git-pack";
import { buildTreePayload, readRepoCatalogState, seedPackedRepoState } from "./util/packed-repo";
import { setupRepoForTests } from "./util/repoSeed";
import { runDOWithRetry, toRequestBody, uniqueRepoId, withEnvOverrides } from "./util/test-helpers";
import { createQueueSendResponse, runQueueMessage } from "./util/queue";

const stockTrace = [
  "receive_pack_invoked",
  "pre_receive_started",
  "pre_receive_quarantine_nonempty",
  "logical_closure_started_ref_still_old",
  "incoming_oid_visible_in_quarantine",
  "logical_closure_completed",
  "pre_receive_succeeded",
  "disposable_ref_update_observed",
] as const;

async function sha256(bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

function emptyPlannerFailureMetrics(activePackCount: number): NativeReceiveOperationMetrics {
  return {
    elapsedMs: 0,
    scratchBytes: 0,
    hydratedBytes: 0,
    downloadedBytes: 0,
    cacheHitBytes: 0,
    metadataBytes: 0,
    metadataRequests: 0,
    inputBytesRead: 0,
    inputRequests: 0,
    rangeBytes: 0,
    rangeRequests: 0,
    packsTouched: 0,
    ranges: [],
    activePackReads: [],
    activePackTrailerBytes: 0,
    activePackTrailerRequests: 0,
    activePackRangeBytes: 0,
    activePackRangeRequests: 0,
    activePackWholeBytes: 0,
    activePackWholeRequests: 0,
    activePackUnattributedBytes: 0,
    activePackUnattributedRequests: 0,
    selectedPackBytes: 0,
    activePackCount,
    outputValidationBytes: 0,
    outputValidationRequests: 0,
    outputBytesWritten: 0,
    outputRequests: 0,
  };
}

function syntheticRefOnlyPrepared(operation: NativeReceiveOperation): NativeReceivePrepared {
  const planSha256 = "e".repeat(64);
  return {
    operationId: operation.id,
    fingerprint: operation.fingerprint,
    processorResult: {
      operationId: operation.id,
      resultKind: "ref-only",
      packBytes: 0,
      idxBytes: 0,
      refsBytes: 0,
      objectCount: 0,
      inputPackObjectCount: 0,
      packSha1: "",
      elapsedMs: 1,
      scratchBytes: 2,
      hydratedBytes: 0,
      downloadedBytes: 2,
      cacheHitBytes: 0,
      inputRequestSha256: operation.stockReceive!.inputRequestSha256,
      stockTrace: stockTrace.map((event, index) => ({ sequence: index + 1, event })),
      metadataBytes: 1,
      metadataRequests: 1,
      inputBytesRead: 1,
      inputRequests: 1,
      rangeBytes: 0,
      rangeRequests: 0,
      packsTouched: 0,
      quarantinePathInsideOwnedWorkRoot: true,
      quarantineRemovedAfterReceive: true,
      quarantinePathNonEmpty: true,
      freshWorkDirectory: true,
      repositoryPackBytesBeforeHydration: 0,
      sharedObjectCacheDisabled: true,
      skipConnectivityCheck: false,
      planSha256,
      closureProof: {
        planSha256,
        incomingOids: [],
        semanticExternalOids: [],
        visitedIncomingObjectCount: 0,
        logicalEdgeCount: 0,
        internalEdgeCount: 0,
        externalEdgeCount: 0,
        missingObjectCount: 0,
        objectTypeCounts: { commit: 0, tree: 0, blob: 0, tag: 0 },
      },
      semanticExternalOids: [],
      thinDeltaBaseOids: [],
      requiredRootOids: [],
      prerequisiteObjectOids: [],
      physicalNodes: [],
      physicalDependencies: [],
      topologicalEntryIds: [],
      selectedPackChecksums: [],
      activePackBindings: [],
      ranges: [],
      activePackReads: [],
      activePackTrailerBytes: 0,
      activePackTrailerRequests: 0,
      activePackRangeBytes: 0,
      activePackRangeRequests: 0,
      activePackWholeBytes: 0,
      activePackWholeRequests: 0,
      activePackUnattributedBytes: 0,
      activePackUnattributedRequests: 0,
      closureManifestKey: `${operation.id}-closure`,
      closureManifestBytes: 1,
      closureManifestSha256: planSha256,
      closureManifestEtag: `${operation.id}-closure-etag`,
      prerequisitePackKey: `${operation.id}-prerequisite`,
      prerequisitePackBytes: 1,
      prerequisitePackSha256: "f".repeat(64),
      prerequisitePackEtag: `${operation.id}-prerequisite-etag`,
      incomingObjectCount: 0,
      visitedIncomingObjectCount: 0,
      logicalEdgeCount: 0,
      internalEdgeCount: 0,
      externalEdgeCount: 0,
      missingObjectCount: 0,
      objectTypeCounts: { commit: 0, tree: 0, blob: 0, tag: 0 },
      selectedPackBytes: 0,
      activePackCount: 0,
      outputValidationBytes: 0,
      outputValidationRequests: 0,
      outputBytesWritten: 0,
      outputRequests: 0,
    },
  };
}

afterEach(() => {
  stockDataPlaneTest.reset();
  receiveCatalogTest.reset();
  stockPlannerTest.reset();
  nativePipelineTest.reset();
  stockAuthorityTest.reset();
  receiveLeaseTest.reset();
});

describe("stock Smart HTTP receive spike", () => {
  it("bounds parallel stock preparation while maintenance and deletion remain fenced", async () => {
    const repo = uniqueRepoId("stock-parallel-admission");
    const seeded = await setupRepoForTests(env, "o", repo);
    const stub = getRepoStub(env, seeded.doName);
    const active = await runDOWithRetry(
      () => stub,
      async (instance) => await instance.seedMinimalRepo()
    );

    await withEnvOverrides(env, { STOCK_RECEIVE_PREPARATION_CONCURRENCY: "2" }, async () => {
      const preparations = [];
      for (let index = 0; index < 2; index++) {
        const begin = await stub.beginReceive({ stockPreparation: true });
        if (!begin.ok) throw new Error("expected bounded stock staging reservation");
        expect(begin.concurrentStockPreparation).toBe(index === 0 ? undefined : true);
        preparations.push(begin);
      }
      expect(await stub.getRepoActivity()).toMatchObject({ state: "receiving" });
      expect(await stub.beginReceive({ stockPreparation: true })).toMatchObject({ ok: false });
      expect(await stub.beginReceive()).toMatchObject({ ok: false });
      expect(await stub.beginReachabilityGc()).toMatchObject({
        ok: false,
        reason: "receive-active",
      });

      const admitted: Array<Extract<AdmitStockReceiveResult, { status: "admitted" }>> = [];
      for (const [index, begin] of preparations.entries()) {
        const operationId = `parallel-stock-${index}`;
        const fingerprint = String(index + 1).repeat(64);
        const outputPackKey = nativeReceiveOutputPackKey(
          doPrefix(stub.id.toString()),
          operationId,
          fingerprint
        );
        const newOid = String(index + 2).repeat(40);
        const admission = await stub.admitStockReceive({
          id: operationId,
          fingerprint,
          leaseToken: begin.lease.token,
          repositoryId: seeded.doName,
          state: "staged",
          inputPackKey: nativeReceiveInputRequestKey(
            doPrefix(stub.id.toString()),
            begin.lease.token
          ),
          inputBytes: 64,
          inputEtag: `input-${index}`,
          stockReceive: {
            inputRequestSha256: String(index + 3).repeat(64),
            packOffset: 16,
            packBytes: 48,
            advertisedRefs: [{ name: "refs/heads/main", oid: active.commitOid }],
          },
          outputPackKey,
          outputIdxKey: packIndexKey(outputPackKey),
          outputRefsKey: packRefsKey(outputPackKey),
          commands: [
            {
              oldOid: "0".repeat(40),
              newOid,
              ref: `refs/heads/parallel-${index}`,
            },
          ],
          acceptedWrites: [
            {
              repositoryId: seeded.doName,
              ref: `refs/heads/parallel-${index}`,
              beforeSha: "0".repeat(40),
              afterSha: newOid,
              actor: "stock-parallel-test",
              sourceSurface: "git-push",
              idempotencyKey: null,
            },
          ],
          activeCatalog: begin.activeCatalog,
          catalogGeneration: begin.packsetVersion,
          createdAt: Date.now(),
          updatedAt: Date.now(),
          attempts: 0,
          cleanupPending: false,
        });
        if (admission.status !== "admitted") {
          throw new Error(`unexpected stock admission ${admission.status}`);
        }
        admitted.push(admission);
      }

      expect(await stub.getRepoActivity()).toMatchObject({ state: "receiving" });

      let releasePublication: (() => void) | undefined;
      let publicationLeaseHeld: (() => void) | undefined;
      const publicationHeld = new Promise<void>((resolve) => {
        publicationLeaseHeld = resolve;
      });
      const publicationRelease = new Promise<void>((resolve) => {
        releasePublication = resolve;
      });
      stockAuthorityTest.afterPublicationLeaseOnce(async () => {
        publicationLeaseHeld?.();
        await publicationRelease;
      });
      await runDOWithRetry(
        () => stub,
        async (instance) => {
          const firstFinalize = instance.finalizeStockReceive(admitted[0]!.executionToken);
          await publicationHeld;
          expect(await instance.finalizeStockReceive(admitted[1]!.executionToken)).toEqual({
            status: "busy",
            retryAfter: 10,
          });
          releasePublication?.();
          expect(await firstFinalize).toMatchObject({
            status: "rejected",
            code: "prepared-output-invalid",
          });
        }
      );

      for (const admission of admitted) {
        expect(
          await stub.rejectStockReceiveExecution(admission.executionToken, "finalize-rejected")
        ).toMatchObject({ status: "failed" });
      }
      expect(await stub.getRepoActivity()).toBeNull();
      expect(await stub.beginRepositoryDeletion()).toMatchObject({ ready: false });
      for (const [index, admission] of admitted.entries()) {
        expect(
          await stub.completeStockReceiveCleanup(
            `parallel-stock-${index}`,
            admission.operation.fingerprint
          )
        ).toMatchObject({ status: "complete" });
      }
      expect(await stub.beginRepositoryDeletion()).toMatchObject({ ready: true });
    });
  });

  it("keeps repository deletion fenced by an active stock preparation", async () => {
    const repo = uniqueRepoId("stock-preparation-deletion-fence");
    const seeded = await setupRepoForTests(env, "o", repo);
    const stub = getRepoStub(env, seeded.doName);
    const begin = await stub.beginReceive({ stockPreparation: true });
    if (!begin.ok) throw new Error("expected stock preparation reservation");

    expect(await stub.beginRepositoryDeletion()).toMatchObject({ ready: false });
  });

  it("promotes a sole misclassified stock preparation back to an exclusive receive", async () => {
    const repo = uniqueRepoId("stock-preparation-promotion");
    const seeded = await setupRepoForTests(env, "o", repo);
    const stub = getRepoStub(env, seeded.doName);
    const begin = await stub.beginReceive({ stockPreparation: true });
    if (!begin.ok) throw new Error("expected stock preparation reservation");

    expect(await stub.promoteStockPreparation(begin.lease.token)).toBe(true);
    expect(await stub.beginReceive({ stockPreparation: true })).toMatchObject({ ok: false });
    expect(await stub.abortReceive(begin.lease.token)).toBe(true);
    const generic = await stub.beginReceive();
    expect(generic).toMatchObject({ ok: true });
    if (generic.ok) expect(await stub.abortReceive(generic.lease.token)).toBe(true);
  });

  it("returns one authority generation when publication follows preparation handoff", async () => {
    const repo = uniqueRepoId("stock-preparation-snapshot-generation");
    const seeded = await setupRepoForTests(env, "o", repo);
    const stub = getRepoStub(env, seeded.doName);
    const before = await runDOWithRetry(
      () => stub,
      async (instance, state) => {
        await instance.seedMinimalRepo();
        const store = asTypedStorage<RepoStateSchema>(state.storage);
        return {
          refs: (await store.get("refs")) ?? [],
          refsVersion: (await store.get("refsVersion")) ?? 0,
          packsetVersion: (await store.get("packsetVersion")) ?? 0,
          nextPackSeq: (await store.get("nextPackSeq")) ?? 1,
        };
      }
    );
    receiveLeaseTest.afterStockPreparationHandoffOnce(async (state) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      await store.put("refs", [{ name: "refs/heads/concurrent", oid: "f".repeat(40) }]);
      await store.put("refsVersion", before.refsVersion + 1);
      await store.put("packsetVersion", before.packsetVersion + 1);
      await store.put("nextPackSeq", before.nextPackSeq + 1);
    });

    const begin = await stub.beginReceive({ stockPreparation: true });
    expect(begin).toMatchObject({
      ok: true,
      refs: before.refs,
      refsVersion: before.refsVersion,
      packsetVersion: before.packsetVersion,
      nextPackSeq: before.nextPackSeq,
    });
    if (begin.ok) expect(await stub.abortReceive(begin.lease.token)).toBe(true);
  });

  it("keeps deletion fenced when an alarm observes an expired preparation", async () => {
    const repo = uniqueRepoId("stock-preparation-expiry");
    const seeded = await setupRepoForTests(env, "o", repo);
    const stub = getRepoStub(env, seeded.doName);
    const begin = await stub.beginReceive({ stockPreparation: true });
    if (!begin.ok) throw new Error("expected stock preparation reservation");
    const inputKey = nativeReceiveInputRequestKey(doPrefix(stub.id.toString()), begin.lease.token);
    await env.REPO_BUCKET.put(inputKey, new Uint8Array([1, 2, 3]));
    expect(await env.REPO_BUCKET.head(inputKey)).not.toBeNull();
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const store = asTypedStorage<RepoStateSchema>(state.storage);
        await store.put("stockReceivePreparationLeases", [
          { ...begin.lease, expiresAt: Date.now() - 1 },
        ]);
        await store.put("lastAccessMs", Date.now() - 30 * 60_000 - 1);
      }
    );

    await runDurableObjectAlarm(stub);

    expect(await env.REPO_BUCKET.head(inputKey)).not.toBeNull();
    expect(await stub.getRepoActivity()).toBeNull();
    expect(await stub.beginRepositoryDeletion()).toMatchObject({ ready: false });
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        expect(await state.storage.get("stockReceivePreparationLeases")).toBeDefined();
        expect(await state.storage.getAlarm()).toBeGreaterThan(Date.now());
      }
    );
    await env.REPO_BUCKET.delete(inputKey);
  });

  it("deletes an abandoned stock preparation after its writer drain", async () => {
    const repo = uniqueRepoId("stock-preparation-drained-cleanup");
    const seeded = await setupRepoForTests(env, "o", repo);
    const stub = getRepoStub(env, seeded.doName);
    await runDOWithRetry(
      () => stub,
      async (instance) => await instance.seedMinimalRepo()
    );
    const begin = await stub.beginReceive({ stockPreparation: true });
    if (!begin.ok) throw new Error("expected stock preparation reservation");
    const inputKey = nativeReceiveInputRequestKey(doPrefix(stub.id.toString()), begin.lease.token);
    await env.REPO_BUCKET.put(inputKey, new Uint8Array([1, 2, 3]));
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const store = asTypedStorage<RepoStateSchema>(state.storage);
        await store.put("stockReceivePreparationLeases", [
          {
            ...begin.lease,
            expiresAt: Date.now() - EXPIRED_WRITER_DRAIN_MS - 1,
          },
        ]);
      }
    );

    await runDurableObjectAlarm(stub);

    expect(await env.REPO_BUCKET.head(inputKey)).toBeNull();
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        expect(await state.storage.get("stockReceivePreparationLeases")).toBeUndefined();
      }
    );
  });

  it("recovers ready publication before processing expiry and eventually drains orphaned deletion", async () => {
    const repo = uniqueRepoId("stock-parallel-recovery-order");
    const seeded = await setupRepoForTests(env, "o", repo);
    const stub = getRepoStub(env, seeded.doName);
    const now = Date.now();
    const operation = (id: string, state: "processing" | "ready"): NativeReceiveOperation => ({
      id,
      fingerprint: (state === "ready" ? "a" : "b").repeat(64),
      leaseToken: `${id}-lease`,
      repositoryId: seeded.doName,
      state,
      inputPackKey: `${id}-input`,
      inputBytes: 1,
      inputEtag: `${id}-input-etag`,
      stockReceive: {
        inputRequestSha256: (state === "ready" ? "c" : "d").repeat(64),
        packOffset: 1,
        packBytes: 1,
        advertisedRefs: [],
      },
      outputPackKey: `${id}-output.pack`,
      outputIdxKey: `${id}-output.idx`,
      outputRefsKey: `${id}-output.refs`,
      commands: [
        {
          oldOid: "0".repeat(40),
          newOid: (state === "ready" ? "1" : "2").repeat(40),
          ref: `refs/heads/${id}`,
        },
      ],
      acceptedWrites: [],
      activeCatalog: [],
      catalogGeneration: 0,
      createdAt: now,
      updatedAt: now,
      attempts: 1,
      cleanupPending: false,
      claimId: `${id}-claim`,
      claimExpiresAt: now + 15 * 60_000,
    });
    const processing = operation("processing", "processing");
    const ready = operation("ready", "ready");
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const store = asTypedStorage<RepoStateSchema>(state.storage);
        await store.put(nativeReceiveOperationKey(processing.id), processing);
        await store.put(nativeReceiveOperationKey(ready.id), ready);
        // Put processing first to prove alarm recovery does not depend on the
        // operation-index order when a ready WAL needs immediate attention.
        await store.put("nativeReceiveOperationIndex", [processing.id, ready.id]);
        await state.storage.setAlarm(now);
      }
    );

    await runDurableObjectAlarm(stub);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const store = asTypedStorage<RepoStateSchema>(state.storage);
        expect((await store.get(nativeReceiveOperationKey(ready.id)))?.state).toBe("aborted");
        expect((await store.get(nativeReceiveOperationKey(processing.id)))?.state).toBe(
          "processing"
        );
        const expiredAt = Date.now() - 21 * 60_000;
        for (const id of [processing.id, ready.id]) {
          const current = await store.get(nativeReceiveOperationKey(id));
          if (!current) throw new Error("expected synthetic stock operation");
          await store.put(nativeReceiveOperationKey(id), {
            ...current,
            updatedAt: expiredAt,
            claimExpiresAt: expiredAt,
          });
        }
      }
    );

    expect(await stub.beginRepositoryDeletion()).toMatchObject({ ready: false });
    expect(await stub.beginRepositoryDeletion()).toMatchObject({ ready: true });
  });

  it("wakes the next expired stock operation after queue cleanup completes", async () => {
    const repo = uniqueRepoId("stock-cleanup-recovery-wakeup");
    const seeded = await setupRepoForTests(env, "o", repo);
    const stub = getRepoStub(env, seeded.doName);
    const now = Date.now();
    const operation = (id: string, state: "failed" | "processing"): NativeReceiveOperation => ({
      id,
      fingerprint: (state === "failed" ? "a" : "b").repeat(64),
      leaseToken: `${id}-lease`,
      repositoryId: seeded.doName,
      state,
      inputPackKey: `${id}-input`,
      inputBytes: 1,
      inputEtag: `${id}-input-etag`,
      stockReceive: {
        inputRequestSha256: (state === "failed" ? "c" : "d").repeat(64),
        packOffset: 1,
        packBytes: 1,
        advertisedRefs: [],
      },
      outputPackKey: `${id}-output.pack`,
      outputIdxKey: `${id}-output.idx`,
      outputRefsKey: `${id}-output.refs`,
      commands: [
        {
          oldOid: "0".repeat(40),
          newOid: (state === "failed" ? "1" : "2").repeat(40),
          ref: `refs/heads/${id}`,
        },
      ],
      acceptedWrites: [],
      activeCatalog: [],
      catalogGeneration: 0,
      createdAt: now - 15 * 60_000,
      updatedAt: now - 15 * 60_000,
      attempts: 1,
      cleanupPending: state === "failed",
      claimId: `${id}-claim`,
      claimExpiresAt: now - 1,
      ...(state === "failed" ? { errorCode: "execution-claim-expired" } : {}),
    });
    const cleaned = operation("cleaned", "failed");
    const waiting = operation("waiting", "processing");
    const active = {
      ...operation("active", "processing"),
      claimExpiresAt: now + 5 * 60_000,
    };
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const store = asTypedStorage<RepoStateSchema>(state.storage);
        await store.put(nativeReceiveOperationKey(cleaned.id), cleaned);
        await store.put(nativeReceiveOperationKey(waiting.id), waiting);
        await store.put(nativeReceiveOperationKey(active.id), active);
        await store.put("nativeReceiveOperationIndex", [waiting.id, active.id, cleaned.id]);
        await store.put("receiveLease", {
          token: waiting.leaseToken,
          operation: "receive",
          createdAt: now,
          expiresAt: now + 30 * 60_000,
        });
        await state.storage.deleteAlarm();
      }
    );

    const cleanupStartedAt = Date.now();
    expect(await stub.completeStockReceiveCleanup(cleaned.id, cleaned.fingerprint)).toMatchObject({
      status: "complete",
    });
    const cleanupCompletedAt = Date.now();
    const scheduled = await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const store = asTypedStorage<RepoStateSchema>(state.storage);
        return {
          alarm: await state.storage.getAlarm(),
          waiting: await store.get(nativeReceiveOperationKey(waiting.id)),
        };
      }
    );
    expect(scheduled).toMatchObject({
      alarm: expect.any(Number),
      waiting: { state: "processing" },
    });
    expect(scheduled.alarm).toBeGreaterThanOrEqual(cleanupStartedAt + 900);
    expect(scheduled.alarm).toBeLessThanOrEqual(cleanupCompletedAt + 1_100);
    await runDurableObjectAlarm(stub);

    expect(await stub.getNativeReceiveOperation(waiting.id)).toMatchObject({
      state: "aborted",
      errorCode: "execution-claim-expired",
    });
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const store = asTypedStorage<RepoStateSchema>(state.storage);
        expect(await state.storage.get("receiveLease")).toBeUndefined();
        expect(await store.get(nativeReceiveOperationKey(active.id))).toMatchObject({
          state: "processing",
          claimExpiresAt: active.claimExpiresAt,
        });
      }
    );

    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const store = asTypedStorage<RepoStateSchema>(state.storage);
        await store.delete(nativeReceiveOperationKey(active.id));
        await store.put("nativeReceiveOperationIndex", [waiting.id, cleaned.id]);
      }
    );
    expect(await stub.registerGcOperation(seeded.doName, "cleanup-shared-alarm")).toMatchObject({
      status: "ready",
    });
    const queueSend = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    try {
      await runDurableObjectAlarm(stub);
      expect(queueSend).toHaveBeenCalledWith({
        kind: "native-receive",
        doId: expect.any(String),
        operationId: waiting.id,
      });
      queueSend.mockClear();

      const lastCleanupStartedAt = Date.now();
      expect(await stub.completeStockReceiveCleanup(waiting.id, waiting.fingerprint)).toMatchObject(
        { status: "complete" }
      );
      const lastCleanupCompletedAt = Date.now();
      const sharedAlarm = await runDOWithRetry(
        () => stub,
        async (_instance, state) => await state.storage.getAlarm()
      );
      expect(sharedAlarm).toBeGreaterThanOrEqual(lastCleanupStartedAt + 900);
      expect(sharedAlarm).toBeLessThanOrEqual(lastCleanupCompletedAt + 1_100);

      await runDurableObjectAlarm(stub);
      expect(queueSend).toHaveBeenCalledWith({
        kind: "reachability-gc",
        doId: expect.any(String),
        repoId: seeded.doName,
        operationId: "cleanup-shared-alarm",
      });
    } finally {
      queueSend.mockRestore();
    }
  });

  it("enters two independent immutable stock preparations before either completes", async () => {
    const repo = uniqueRepoId("stock-parallel-worker");
    const seeded = await setupRepoForTests(env, "o", repo);
    const stub = getRepoStub(env, seeded.doName);
    const active = await runDOWithRetry(
      () => stub,
      async (instance) => await instance.seedMinimalRepo()
    );
    const author = "Display <display@example.invalid> 0 +0000";
    const commitPayload = new TextEncoder().encode(
      `tree ${active.treeOid}\nparent ${active.commitOid}\nauthor ${author}\ncommitter ${author}\n\nparallel\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([{ type: "commit", payload: commitPayload }]);
    let activePreparations = 0;
    let maximumPreparations = 0;
    let releaseBoth: (() => void) | undefined;
    const bothEntered = new Promise<void>((resolve) => {
      releaseBoth = resolve;
    });
    stockDataPlaneTest.setWorkerExecutor(async () => {
      activePreparations++;
      maximumPreparations = Math.max(maximumPreparations, activePreparations);
      if (activePreparations === 2) releaseBoth?.();
      await Promise.race([
        bothEntered,
        new Promise<never>((_resolve, reject) =>
          setTimeout(() => reject(new Error("parallel preparation did not overlap")), 2_000)
        ),
      ]);
      activePreparations--;
      throw new Error("stock-data-plane:container-transient");
    });

    const push = async (index: number) => {
      const prefix = concatChunks([
        pktLine(
          `${"0".repeat(40)} ${commit.oid} refs/heads/parallel-${index}\0 report-status agent=git/2.50.1\n`
        ),
        flushPkt(),
      ]);
      const body = concatChunks([prefix, pack]);
      return await handleStreamingReceivePackPOST(
        { ...env, NATIVE_RECEIVE_CONTAINER: "1", STOCK_RECEIVE_PREPARATION_CONCURRENCY: "2" },
        seeded.doName,
        new Request(`https://example.invalid/o/${repo}/git-receive-pack`, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-git-receive-pack-request",
            "Content-Length": String(body.byteLength),
            "X-Display-Operation-ID": `parallel-worker-${index}`,
          },
          body: toRequestBody(body),
        }),
        createExecutionContext(),
        { limiter: new SubrequestLimiter(900) }
      );
    };

    const responses = await Promise.all([push(0), push(1)]);
    expect(maximumPreparations).toBe(2);
    expect(responses.map((response) => response.status)).toEqual([503, 503]);
    expect(await stub.getRepoActivity()).toBeNull();
    expect(await stub.beginReceive({ stockPreparation: true })).toMatchObject({ ok: true });
  });

  it("serializes concurrent publication into stale same-base and durable independent outcomes", async () => {
    const runScenario = async (sameRef: boolean) => {
      const repo = uniqueRepoId(sameRef ? "stock-same-base" : "stock-independent-refs");
      const seeded = await setupRepoForTests(env, "o", repo);
      const stub = getRepoStub(env, seeded.doName);
      const targetOid = "7".repeat(40);
      await stub.setRefs([{ name: "refs/heads/main", oid: targetOid }]);

      const admissions: Array<Extract<AdmitStockReceiveResult, { status: "admitted" }>> = [];
      for (let index = 0; index < 2; index++) {
        const begin = await stub.beginReceive({ stockPreparation: true });
        if (!begin.ok) throw new Error("expected stock publication admission");
        const id = `${sameRef ? "same" : "independent"}-${index}`;
        const fingerprint = String(index + 8).repeat(64);
        const ref = sameRef ? "refs/heads/contended" : `refs/heads/independent-${index}`;
        const outputPackKey = nativeReceiveOutputPackKey(
          doPrefix(stub.id.toString()),
          id,
          fingerprint
        );
        const admitted = await stub.admitStockReceive({
          id,
          fingerprint,
          leaseToken: begin.lease.token,
          repositoryId: seeded.doName,
          state: "staged",
          inputPackKey: `${id}-input`,
          inputBytes: 1,
          inputEtag: `${id}-input-etag`,
          stockReceive: {
            inputRequestSha256: String(index + 10).repeat(64),
            packOffset: 1,
            packBytes: 1,
            advertisedRefs: begin.refs,
          },
          outputPackKey,
          outputIdxKey: packIndexKey(outputPackKey),
          outputRefsKey: packRefsKey(outputPackKey),
          commands: [{ oldOid: "0".repeat(40), newOid: targetOid, ref }],
          acceptedWrites: [
            {
              repositoryId: seeded.doName,
              ref,
              beforeSha: "0".repeat(40),
              afterSha: targetOid,
              actor: "stock-publication-test",
              sourceSurface: "git-push",
              idempotencyKey: null,
            },
          ],
          activeCatalog: begin.activeCatalog,
          catalogGeneration: begin.packsetVersion,
          createdAt: Date.now(),
          updatedAt: Date.now(),
          attempts: 0,
          cleanupPending: false,
        });
        if (admitted.status !== "admitted") {
          throw new Error(`unexpected stock publication admission ${admitted.status}`);
        }
        expect(
          await validateStockReceivePreparedProof(
            admitted.operation,
            syntheticRefOnlyPrepared(admitted.operation).processorResult
          )
        ).toBe(true);
        admissions.push(admitted);
      }

      const finalize = async (admission: (typeof admissions)[number]) => {
        for (let attempt = 0; attempt < 20; attempt++) {
          const result = await stub.finalizeStockReceive(
            admission.executionToken,
            syntheticRefOnlyPrepared(admission.operation)
          );
          if (result.status !== "busy") return result;
          await new Promise((resolve) => setTimeout(resolve, 1));
        }
        throw new Error("publication lease did not drain");
      };
      if (sameRef) {
        await runDOWithRetry(
          () => stub,
          async (_instance, state) => {
            const store = asTypedStorage<RepoStateSchema>(state.storage);
            const leaseNow = Date.now();
            await store.put("stockReceivePublicationLease", {
              token: "lost-publication-owner",
              operation: "receive",
              createdAt: leaseNow,
              expiresAt: leaseNow + 30_000,
            });
          }
        );
        expect(
          await stub.finalizeStockReceive(
            admissions[0]!.executionToken,
            syntheticRefOnlyPrepared(admissions[0]!.operation)
          )
        ).toEqual({ status: "busy", retryAfter: 10 });
        expect(await stub.getNativeReceiveOperation(admissions[0]!.operation.id)).toMatchObject({
          state: "ready",
          metrics: expect.any(Object),
        });
        const recovery = await stub.beginStockReceiveRecovery(admissions[0]!.operation.id);
        expect(recovery.status).toBe("recovery");
        if (recovery.status !== "recovery") throw new Error("expected retained-proof recovery");
        expect(
          await stub.admitStockReceive({
            ...admissions[0]!.operation,
            leaseToken: recovery.begin.lease.token,
          })
        ).toEqual({
          status: "finalize_pending",
          executionToken: admissions[0]!.executionToken,
        });
        expect(
          await stub.completeStockReceiveRecovery(
            admissions[0]!.operation.id,
            recovery.begin.lease.token
          )
        ).toBe(true);
        expect(
          await stub.finalizeStockReceive(
            admissions[1]!.executionToken,
            syntheticRefOnlyPrepared(admissions[1]!.operation)
          )
        ).toEqual({ status: "busy", retryAfter: 10 });
        await runDOWithRetry(
          () => stub,
          async (_instance, state) => {
            const store = asTypedStorage<RepoStateSchema>(state.storage);
            const leaseNow = Date.now();
            await store.put("receiveLease", {
              token: admissions[1]!.operation.leaseToken,
              operation: "receive",
              createdAt: leaseNow,
              expiresAt: leaseNow + 30 * 60_000,
            });
            await store.put("stockReceivePublicationLease", {
              token: admissions[1]!.executionToken,
              operation: "receive",
              createdAt: leaseNow - 30_001,
              expiresAt: leaseNow - 1,
            });
            await state.storage.setAlarm(leaseNow);
          }
        );
        await runDurableObjectAlarm(stub);
        expect(await stub.getNativeReceiveOperation(admissions[0]!.operation.id)).toMatchObject({
          state: "ready",
        });
        expect(await stub.getNativeReceiveOperation(admissions[1]!.operation.id)).toMatchObject({
          state: "finalizing",
        });
      }
      const results = await Promise.all(admissions.map(finalize));
      const pending = results.filter(
        (result): result is Extract<typeof result, { status: "publication_pending" }> =>
          result.status === "publication_pending"
      );
      for (const result of pending) {
        const proof: NativeReceiveAuthorityPublication = {
          refs: result.publication.refs.map((ref) => ({ ...ref, etag: "test-ref-etag" })),
          receipt: { ...result.publication.receipt, etag: "test-receipt-etag" },
        };
        const confirmation = await stub.confirmStockReceivePublication(
          result.publicationToken,
          proof
        );
        // The alarm-owned queue may confirm the same immutable proof first;
        // both dispositions bind the identical committed operation.
        expect(["committed", "replayed"]).toContain(confirmation.status);
      }

      const refs = await stub.listRefs();
      if (sameRef) {
        expect(results.map((result) => result.status).sort()).toEqual([
          "publication_pending",
          "ref_conflict",
        ]);
        expect(refs.filter((ref) => ref.name === "refs/heads/contended")).toEqual([
          { name: "refs/heads/contended", oid: targetOid },
        ]);
      } else {
        expect(results.map((result) => result.status)).toEqual([
          "publication_pending",
          "publication_pending",
        ]);
        expect(refs).toEqual(
          expect.arrayContaining([
            { name: "refs/heads/independent-0", oid: targetOid },
            { name: "refs/heads/independent-1", oid: targetOid },
          ])
        );
      }
    };

    await runScenario(true);
    await runScenario(false);
  });

  it("preserves explicit selective receive length rejection semantics", async () => {
    const seeded = await setupRepoForTests(env, "o", uniqueRepoId("stock-length"));
    const request = async (contentLength: string) =>
      await handleStreamingReceivePackPOST(
        { ...env, NATIVE_RECEIVE_CONTAINER: "1" },
        seeded.doName,
        new Request("https://example.invalid/o/stock-length/git-receive-pack", {
          method: "POST",
          headers: {
            "Content-Type": "application/x-git-receive-pack-request",
            "Content-Length": contentLength,
            "X-Display-Spike1b-Stock": "1",
          },
          body: new Uint8Array([0]),
        }),
        createExecutionContext(),
        { limiter: new SubrequestLimiter(4) }
      );

    expect((await request("invalid")).status).toBe(411);
    expect((await request("0")).status).toBe(413);
    expect((await request(String(Number.MAX_SAFE_INTEGER + 1))).status).toBe(413);
  });

  it("classifies streaming Container failures without exposing provider details", () => {
    const privateDetail = "private-resource-name signed-url credential-value";
    const classified = stockDataPlaneTest.streamingContainerPhaseError(
      "container-rpc",
      new TypeError(privateDetail)
    );
    expect(classified.message).toBe("stock-data-plane:container-rpc-failed");
    expect(classified.message).not.toContain(privateDetail);

    const existing = new Error("stock-data-plane:input-authority-mismatch");
    expect(stockDataPlaneTest.streamingContainerPhaseError("bundle-read", existing)).toBe(existing);
    const physical = new Error("stock-physical-plan:dependency-missing");
    expect(stockDataPlaneTest.streamingContainerPhaseError("bundle-read", physical)).toBe(physical);
    expect(
      stockDataPlaneTest.streamingContainerPhaseError(
        "container-rpc",
        new Error("stock-data-plane:container-readiness-failed")
      ).message
    ).toBe("stock-data-plane:container-readiness-failed");
    for (const [diagnostic, expected] of [
      ["readiness-failed", "stock-data-plane:container-readiness-failed"],
      ["forward-failed", "stock-data-plane:container-forward-failed"],
      ["unknown", "stock-data-plane:container-transient"],
    ] as const) {
      expect(
        stockDataPlaneTest.containerFailureCode(
          new Response("unavailable\n", {
            status: 503,
            headers: { "X-Display-Stock-Container-Diagnostic": diagnostic },
          })
        )
      ).toBe(expected);
    }
    expect(
      stockDataPlaneTest.containerFailureCode(new Response("rejected\n", { status: 422 }))
    ).toBe("stock-data-plane:container-rejected");
  });

  it("accepts legacy artifact host results and normalizes their omitted discriminator", () => {
    const digest = "a".repeat(64);
    const parsed = stockDataPlaneTest.parseHostResult({
      operationId: "legacy-artifact-result",
      receivePackResponse: "AA==",
      receiveResponseBytes: 1,
      inputRequestSha256: digest,
      packBytes: 1,
      idxBytes: 1,
      refsBytes: 1,
      packSha1: "b".repeat(40),
      packSha256: digest,
      idxSha256: digest,
      refsSha256: digest,
      objectCount: 1,
      inputPackObjectCount: 1,
      elapsedMs: 1,
      trace: [],
      quarantinePathInsideOwnedWorkRoot: true,
      quarantineRemovedAfterReceive: true,
      quarantinePathNonEmpty: true,
      freshWorkDirectory: true,
      repositoryPackBytesBeforeHydration: 0,
      sharedObjectCacheDisabled: true,
      skipConnectivityCheck: false,
      planSha256: digest,
      closureProof: {
        planSha256: digest,
        incomingOids: ["b".repeat(40)],
        semanticExternalOids: [],
        visitedIncomingObjectCount: 1,
        logicalEdgeCount: 0,
        internalEdgeCount: 0,
        externalEdgeCount: 0,
        missingObjectCount: 0,
        objectTypeCounts: { commit: 1, tree: 0, blob: 0, tag: 0 },
      },
    });
    expect(parsed.resultKind).toBe("artifacts");
  });

  it("retains only a bounded schema category for invalid host result evidence", () => {
    expect(() => stockDataPlaneTest.parseHostResultPayload(new TextEncoder().encode("{"))).toThrow(
      "stock-data-plane:response-header-json-invalid"
    );
    expect(() =>
      stockDataPlaneTest.parseHostResultPayload(
        new TextEncoder().encode(JSON.stringify({ operationId: "private-provider-value" }))
      )
    ).toThrow("stock-data-plane:response-header-receivepackresponse");
  });

  it("forwards and accepts only a complete bounded Container lifecycle timing projection", async () => {
    const forwarded = await stockContainerHostTest.forwardResponse(
      new Response(new Uint8Array([1]), {
        headers: {
          "Content-Type": "application/x-display-stock-receive-output",
          "Content-Length": "1",
        },
      }),
      {
        ready: true,
        wasRunning: true,
        readinessMs: 7,
        startAttempts: 0,
        probeAttempts: 1,
      }
    );
    const headers = forwarded.headers;
    expect(new Uint8Array(await forwarded.arrayBuffer())).toEqual(new Uint8Array([1]));
    expect(headers.get("Content-Length")).toBe("1");
    expect(stockDataPlaneTest.containerLifecycleTiming(headers)).toEqual({
      containerReadinessMs: 7,
      containerStartAttempts: 0,
      containerProbeAttempts: 1,
      containerWasRunning: true,
    });

    const generatedHeaders = stockContainerHostTest.timingHeaders({
      ready: true,
      wasRunning: true,
      readinessMs: 7,
      startAttempts: 0,
      probeAttempts: 1,
    });
    generatedHeaders.set("X-Display-Stock-Container-Probe-Attempts", "121");
    expect(stockDataPlaneTest.containerLifecycleTiming(generatedHeaders)).toBeUndefined();
    generatedHeaders.set("X-Display-Stock-Container-Probe-Attempts", "1");
    generatedHeaders.delete("X-Display-Stock-Container-Was-Running");
    expect(stockDataPlaneTest.containerLifecycleTiming(generatedHeaders)).toBeUndefined();
  });

  it("records the Worker data-plane total once while retaining Container process time", () => {
    expect(
      stockDataPlaneTest.measuredStockTiming({
        processorStartedAt: 1_787_731_200_100,
        elapsedMs: 100,
        planningMs: 11,
        containerProcessMs: 40,
        timing: {
          bundleReadMs: 7,
          containerRpcMs: 53,
          outputUploadMs: 9,
          containerReadinessMs: 3,
          containerStartAttempts: 0,
          containerProbeAttempts: 1,
          containerWasRunning: true,
        },
      })
    ).toEqual({
      elapsedMs: 100,
      processorStartedAt: 1_787_731_200_100,
      stockTiming: {
        planningMs: 11,
        bundleReadMs: 7,
        containerRpcMs: 53,
        containerProcessMs: 40,
        containerReadinessMs: 3,
        outputUploadMs: 9,
        outputVerificationMs: 0,
        proofValidationMs: 0,
        containerStartAttempts: 0,
        containerProbeAttempts: 1,
        containerWasRunning: true,
      },
    });
    expect(
      stockDataPlaneTest.measuredStockTiming({
        processorStartedAt: 1_787_731_200_100,
        elapsedMs: 300_001,
        planningMs: 300_001,
        containerProcessMs: 40,
        timing: {
          bundleReadMs: 7,
          containerRpcMs: 53,
          outputUploadMs: 9,
          containerReadinessMs: 3,
          containerStartAttempts: 0,
          containerProbeAttempts: 1,
          containerWasRunning: true,
        },
      })
    ).toEqual({ elapsedMs: 300_001, processorStartedAt: 1_787_731_200_100 });
  });

  it("restarts a Container that exits during readiness before forwarding receive bytes", async () => {
    let running = true;
    let starts = 0;
    let probes = 0;
    const container = {
      get running() {
        return running;
      },
      start() {
        starts++;
        running = true;
      },
      getTcpPort() {
        return {
          async fetch() {
            probes++;
            if (probes === 1) {
              running = false;
              throw new Error("simulated process exit");
            }
            return new Response("ready\n", { status: 200 });
          },
        } as unknown as Fetcher;
      },
    };

    expect(await stockContainerHostTest.waitForReady(container)).toBe(true);
    expect(starts).toBe(1);
    expect(probes).toBe(2);
  });

  it("installs a finite bounded idle-retention policy", async () => {
    const configured: Array<number | bigint> = [];
    const container = {
      async setInactivityTimeout(durationMs: number | bigint) {
        configured.push(durationMs);
      },
    };

    await stockContainerHostTest.configureIdleRetention(container, undefined);
    await stockContainerHostTest.configureIdleRetention(container, "30");
    await stockContainerHostTest.configureIdleRetention(container, "0");
    await stockContainerHostTest.configureIdleRetention(container, "3600");

    expect(configured).toEqual([120_000, 30_000, 5_000, 900_000]);
    expect(stockContainerHostTest.idleRetentionMs("invalid")).toBe(120_000);
  });

  it("does not reactivate an already-running Container", async () => {
    let starts = 0;
    const container = {
      running: true,
      start() {
        starts++;
      },
      getTcpPort() {
        return {
          async fetch() {
            return new Response("ready\n", { status: 200 });
          },
        } as unknown as Fetcher;
      },
    };

    expect(await stockContainerHostTest.waitForReady(container)).toBe(true);
    expect(starts).toBe(0);
  });

  it("reissues a lost Container start at a bounded cadence", async () => {
    let running = false;
    let starts = 0;
    const container = {
      get running() {
        return running;
      },
      start() {
        starts++;
        if (starts === 1) throw new Error("simulated lost lifecycle start");
        running = true;
      },
      getTcpPort() {
        return {
          async fetch() {
            if (!running) throw new Error("not ready");
            return new Response("ready\n", { status: 200 });
          },
        } as unknown as Fetcher;
      },
    };

    expect(await stockContainerHostTest.waitForReady(container)).toBe(true);
    expect(starts).toBe(2);
  });

  it("restarts after every successful start exits before readiness", async () => {
    let running = false;
    let starts = 0;
    let probes = 0;
    const container = {
      get running() {
        return running;
      },
      start() {
        starts++;
        running = true;
      },
      getTcpPort() {
        return {
          async fetch() {
            probes++;
            running = false;
            throw new Error("simulated exit before readiness");
          },
        } as unknown as Fetcher;
      },
    };

    expect(await stockContainerHostTest.waitForReady(container)).toBe(false);
    expect(starts).toBe(6);
    expect(probes).toBe(120);
  });

  it("bounds repeated starts while a Container remains not running", async () => {
    let starts = 0;
    let probes = 0;
    const container = {
      running: false,
      start() {
        starts++;
        throw new Error("simulated start already in flight");
      },
      getTcpPort() {
        return {
          async fetch() {
            probes++;
            throw new Error("not ready");
          },
        } as unknown as Fetcher;
      },
    };

    expect(await stockContainerHostTest.waitForReady(container)).toBe(false);
    expect(starts).toBe(6);
    expect(probes).toBe(120);
  });

  it("retains bounded readiness counters without provider detail", async () => {
    let starts = 0;
    const result = await stockContainerHostTest.readiness({
      running: false,
      start() {
        starts++;
      },
      getTcpPort() {
        return {
          async fetch() {
            throw new Error("private provider failure");
          },
        } as unknown as Fetcher;
      },
    });

    expect(result).toEqual({ ready: false, startAttempts: 6, probeAttempts: 120 });
    expect(starts).toBe(6);
  });

  it("plans the first push into a repository with no active packs", async () => {
    const seeded = await setupRepoForTests(env, "o", uniqueRepoId("stock-plan-empty"));
    const stub = getRepoStub(env, seeded.doName);
    const treePayload = new Uint8Array(0);
    const tree = await encodeGitObject("tree", treePayload);
    const author = "Display <display@example.invalid> 0 +0000";
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\nauthor ${author}\ncommitter ${author}\n\ninitial\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([
      { type: "commit", payload: commitPayload },
      { type: "tree", payload: treePayload },
    ]);
    const inputRequestKey = `${doPrefix(stub.id.toString())}/native-receive/empty.request`;
    const inputRequestSha256 = await sha256(pack);
    await env.REPO_BUCKET.put(inputRequestKey, pack, {
      customMetadata: { sha256: inputRequestSha256 },
    });

    const plan = await planStockReceive({
      env,
      repoId: seeded.doName,
      operationId: "stock-plan-empty-operation",
      inputRequestKey,
      inputRequestBytes: pack.byteLength,
      inputRequestSha256,
      packOffset: 0,
      packBytes: pack.byteLength,
      advertisedRefs: [],
      commands: [{ oldOid: "0".repeat(40), newOid: commit.oid, ref: "refs/heads/main" }],
      activePacks: [],
      cacheCtx: {
        req: new Request("https://example.invalid/stock-plan-empty"),
        ctx: createExecutionContext(),
        memo: {},
      },
      limiter: new SubrequestLimiter(6),
      countSubrequest() {},
    });

    expect(plan.activePackCount).toBe(0);
    expect(plan.requiredRootOids).toEqual([]);
    expect(plan.ranges).toEqual([]);
    const operation = {
      id: "stock-plan-empty-operation",
      fingerprint: "f".repeat(64),
      leaseToken: "lease",
      repositoryId: seeded.doName,
      state: "processing",
      inputPackKey: inputRequestKey,
      inputBytes: pack.byteLength,
      inputEtag: "input-etag",
      stockReceive: {
        inputRequestSha256,
        packOffset: 0,
        packBytes: pack.byteLength,
        advertisedRefs: [],
      },
      outputPackKey: "output.pack",
      outputIdxKey: "output.idx",
      outputRefsKey: "output.refs",
      commands: [{ oldOid: "0".repeat(40), newOid: commit.oid, ref: "refs/heads/main" }],
      acceptedWrites: [],
      activeCatalog: [],
      catalogGeneration: 0,
      createdAt: 1,
      updatedAt: 1,
      attempts: 1,
      cleanupPending: false,
    } satisfies NativeReceiveOperation;
    const result = {
      operationId: operation.id,
      packBytes: pack.byteLength,
      idxBytes: 1,
      refsBytes: 1,
      objectCount: plan.incomingObjectCount,
      inputPackObjectCount: plan.incomingObjectCount,
      packSha1: "0".repeat(40),
      elapsedMs: 1,
      scratchBytes: pack.byteLength,
      hydratedBytes: 0,
      downloadedBytes: pack.byteLength,
      cacheHitBytes: 0,
      inputRequestSha256,
      stockTrace: stockTrace.map((event, index) => ({ sequence: index + 1, event })),
      quarantinePathInsideOwnedWorkRoot: true,
      quarantineRemovedAfterReceive: true,
      quarantinePathNonEmpty: true,
      freshWorkDirectory: true,
      repositoryPackBytesBeforeHydration: 0,
      sharedObjectCacheDisabled: true,
      skipConnectivityCheck: false,
      planSha256: plan.planSha256,
      closureProof: {
        planSha256: plan.planSha256,
        incomingOids: [commit.oid, tree.oid].sort(),
        semanticExternalOids: [],
        visitedIncomingObjectCount: plan.visitedIncomingObjectCount,
        logicalEdgeCount: plan.logicalEdgeCount,
        internalEdgeCount: plan.internalEdgeCount,
        externalEdgeCount: plan.externalEdgeCount,
        missingObjectCount: plan.missingObjectCount,
        objectTypeCounts: plan.objectTypeCounts,
      },
      semanticExternalOids: [],
      thinDeltaBaseOids: [],
      requiredRootOids: [],
      prerequisiteObjectOids: [],
      physicalNodes: [],
      physicalDependencies: [],
      topologicalEntryIds: [],
      selectedPackChecksums: [],
      activePackBindings: [],
      ranges: [],
      activePackReads: [],
      activePackTrailerBytes: 0,
      activePackTrailerRequests: 0,
      activePackRangeBytes: 0,
      activePackRangeRequests: 0,
      activePackWholeBytes: 0,
      activePackWholeRequests: 0,
      activePackUnattributedBytes: 0,
      activePackUnattributedRequests: 0,
      closureManifestKey: plan.closureManifestKey,
      closureManifestBytes: plan.closureManifestBytes,
      closureManifestSha256: plan.closureManifestSha256,
      closureManifestEtag: plan.closureManifestEtag,
      prerequisitePackKey: plan.prerequisitePackKey,
      prerequisitePackBytes: plan.prerequisitePackBytes,
      prerequisitePackSha256: plan.prerequisitePackSha256,
      prerequisitePackEtag: plan.prerequisitePackEtag,
      incomingObjectCount: plan.incomingObjectCount,
      visitedIncomingObjectCount: plan.visitedIncomingObjectCount,
      logicalEdgeCount: plan.logicalEdgeCount,
      internalEdgeCount: plan.internalEdgeCount,
      externalEdgeCount: plan.externalEdgeCount,
      missingObjectCount: plan.missingObjectCount,
      objectTypeCounts: plan.objectTypeCounts,
      selectedPackBytes: 0,
      activePackCount: 0,
      rangeBytes: 0,
      rangeRequests: 0,
      packsTouched: 0,
    } satisfies NativeReceiveProcessResult;
    expect(await validateStockReceivePreparedProof(operation, result)).toBe(true);
    await env.REPO_BUCKET.delete([
      inputRequestKey,
      plan.prerequisitePackKey,
      plan.closureManifestKey,
    ]);
  });

  it("derives exact semantic prerequisite ranges from bound active IDX/PREF metadata", async () => {
    expect(stockPlannerTest.wholeActivePackFits(8 * 1024 * 1024, 8 * 1024 * 1024)).toBe(true);
    expect(stockPlannerTest.wholeActivePackFits(8 * 1024 * 1024 + 1, 0)).toBe(false);
    expect(stockPlannerTest.wholeActivePackFits(8 * 1024 * 1024, 8 * 1024 * 1024 + 1)).toBe(false);
    const eligiblePacks = [
      { packKey: "pack-c", packBytes: 8 * 1024 * 1024, packChecksum: "c".repeat(40) },
      { packKey: "pack-a", packBytes: 8 * 1024 * 1024, packChecksum: "a".repeat(40) },
      { packKey: "pack-b", packBytes: 8 * 1024 * 1024, packChecksum: "b".repeat(40) },
    ];
    expect(stockPlannerTest.selectWholeActivePackKeys(eligiblePacks)).toEqual(["pack-a", "pack-b"]);
    expect(stockPlannerTest.selectWholeActivePackKeys([...eligiblePacks].reverse())).toEqual([
      "pack-a",
      "pack-b",
    ]);
    const owner = "o";
    const repo = uniqueRepoId("stock-plan");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    const active = await runDOWithRetry(
      () => stub,
      async (instance) => await instance.seedMinimalRepo()
    );
    const [catalog] = await stub.getActivePackCatalog();
    expect(catalog).toBeDefined();
    const author = "Display <display@example.invalid> 0 +0000";
    const commitPayload = new TextEncoder().encode(
      `tree ${active.treeOid}\nparent ${active.commitOid}\nauthor ${author}\ncommitter ${author}\n\nplanned\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([{ type: "commit", payload: commitPayload }]);
    const prefix = catalog!.packKey.slice(0, catalog!.packKey.indexOf("/objects/pack/"));
    const inputRequestKey = `${prefix}/native-receive/input-stock-plan.request`;
    const inputRequestSha256 = await sha256(pack);
    await env.REPO_BUCKET.put(inputRequestKey, pack, {
      customMetadata: { sha256: inputRequestSha256 },
    });
    const cacheCtx = {
      req: new Request("https://example.invalid/stock-plan"),
      ctx: createExecutionContext(),
      memo: {},
    };
    const planArgs = {
      env,
      repoId: seeded.doName,
      operationId: "stock-plan-operation",
      inputRequestKey,
      inputRequestBytes: pack.byteLength,
      inputRequestSha256,
      packOffset: 0,
      packBytes: pack.byteLength,
      advertisedRefs: [{ name: "refs/heads/main", oid: active.commitOid }],
      commands: [{ oldOid: active.commitOid, newOid: commit.oid, ref: "refs/heads/main" }],
      activePacks: [catalog!],
      cacheCtx,
      limiter: new SubrequestLimiter(6),
      countSubrequest() {},
    };

    stockPlannerTest.failTransientR2Read(planArgs.operationId);
    const transientError = await planStockReceive(planArgs).catch((error: unknown) => error);
    expect(transientError).toBeInstanceOf(StockReceivePlannerError);
    expect(transientError).toMatchObject({
      code: "r2-transient",
      metrics: {
        metadataBytes: 0,
        metadataRequests: 0,
        inputBytesRead: 0,
        inputRequests: 0,
        rangeBytes: 0,
        rangeRequests: 0,
      },
    });
    expect(stockPlannerTest.transientR2ReadFault(planArgs.operationId)).toMatchObject({
      triggered: true,
      metadataBytes: 0,
      metadataRequests: 0,
      inputBytesRead: 0,
      inputRequests: 0,
      rangeBytes: 0,
      rangeRequests: 0,
      elapsedMs: expect.any(Number),
    });

    stockPlannerTest.failWrongPrerequisiteRange(planArgs.operationId);
    const wrongRangeError = await planStockReceive(planArgs).catch((error: unknown) => error);
    expect(wrongRangeError).toBeInstanceOf(StockReceivePlannerError);
    expect(wrongRangeError).toMatchObject({
      code: "replacement-closure-invalid",
      metrics: {
        rangeRequests: 2,
        activePackRangeRequests: 2,
        activePackWholeRequests: 0,
        activePackUnattributedRequests: 0,
      },
    });
    const wrongRange = stockPlannerTest.wrongPrerequisiteRangeFault(planArgs.operationId);
    expect(wrongRange).toMatchObject({
      triggered: true,
      activePackRangeBytes: expect.any(Number),
      activePackRangeRequests: 1,
      activePackReads: [
        {
          packChecksum: expect.stringMatching(/^[0-9a-f]{40}$/),
          start: expect.any(Number),
          end: expect.any(Number),
          returnedBytes: expect.any(Number),
          kind: "required-object",
          requiredOid: expect.stringMatching(/^[0-9a-f]{40}$/),
        },
      ],
    });
    expect(wrongRange).not.toHaveProperty("activePackTrailerBytes");
    const wrongRead = wrongRange!.activePackReads![0]!;
    expect(wrongRead.returnedBytes).toBe(wrongRead.end - wrongRead.start);
    expect(wrongRange!.activePackRangeBytes).toBe(wrongRead.returnedBytes);
    stockPlannerTest.reset();
    cacheCtx.memo = {};

    stockPlannerTest.failTransientWholePackRead(planArgs.operationId);
    const retriedWholePlan = await planStockReceive(planArgs);
    expect(stockPlannerTest.transientWholePackReadAttempts(planArgs.operationId)).toBe(2);
    expect(retriedWholePlan.activePackWholeRequests).toBe(1);
    expect(retriedWholePlan.activePackRangeRequests).toBe(0);
    stockPlannerTest.reset();
    cacheCtx.memo = {};

    stockPlannerTest.returnShortWholePackRead(planArgs.operationId);
    const retriedShortWholePlan = await planStockReceive(planArgs);
    expect(stockPlannerTest.shortWholePackReadAttempts(planArgs.operationId)).toBe(2);
    expect(retriedShortWholePlan.activePackWholeRequests).toBe(1);
    expect(retriedShortWholePlan.activePackRangeRequests).toBe(0);
    stockPlannerTest.reset();
    cacheCtx.memo = {};

    stockPlannerTest.readWholeActivePack(planArgs.operationId);
    await expect(planStockReceive(planArgs)).rejects.toThrow(
      "stock-plan:active-pack-whole-observation-mismatch"
    );
    cacheCtx.memo = {};

    const plan = await planStockReceive(planArgs);

    expect(plan.semanticExternalOids).toEqual([active.commitOid, active.treeOid].sort());
    expect(plan.thinDeltaBaseOids).toEqual([]);
    expect(plan.requiredRootOids).toEqual(plan.semanticExternalOids);
    expect(plan.ranges).toHaveLength(2);
    expect(plan.rangeBytes).toBe(0);
    expect(plan.rangeRequests).toBe(0);
    expect(plan.activePackRangeBytes).toBe(0);
    expect(plan.activePackRangeRequests).toBe(0);
    expect(plan.activePackTrailerBytes).toBe(20);
    expect(plan.activePackTrailerRequests).toBe(1);
    expect(plan.activePackWholeBytes).toBe(catalog!.packBytes);
    expect(plan.activePackWholeRequests).toBe(1);
    expect(plan.activePackUnattributedBytes).toBe(0);
    expect(plan.activePackUnattributedRequests).toBe(0);
    expect(plan.activePackReads.filter((read) => read.kind === "required-object")).toEqual([]);
    expect(plan.activePackReads.filter((read) => read.kind === "whole")).toEqual([
      {
        packChecksum: expect.stringMatching(/^[0-9a-f]{40}$/),
        start: 0,
        end: catalog!.packBytes,
        returnedBytes: catalog!.packBytes,
        kind: "whole",
      },
    ]);
    const activePackObject = await env.REPO_BUCKET.get(catalog!.packKey);
    expect(activePackObject).not.toBeNull();
    const originalActivePack = new Uint8Array(await activePackObject!.arrayBuffer());
    const corruptedActivePack = originalActivePack.slice();
    const selectedRange = plan.ranges[0]!;
    corruptedActivePack[selectedRange.end - 1] ^= 1;
    await env.REPO_BUCKET.put(catalog!.packKey, corruptedActivePack);
    try {
      await expect(planStockReceive(planArgs)).rejects.toThrow("Decompression failed.");
    } finally {
      await env.REPO_BUCKET.put(catalog!.packKey, originalActivePack);
    }
    expect(plan.incomingObjectCount).toBe(1);
    expect(plan.visitedIncomingObjectCount).toBe(1);
    expect(plan.logicalEdgeCount).toBe(2);
    expect(plan.externalEdgeCount).toBe(2);
    expect(plan.internalEdgeCount).toBe(0);
    expect(plan.missingObjectCount).toBe(0);
    expect(await env.REPO_BUCKET.head(plan.prerequisitePackKey)).toMatchObject({
      size: plan.prerequisitePackBytes,
      customMetadata: { sha256: plan.prerequisitePackSha256 },
    });
    expect(await env.REPO_BUCKET.head(plan.closureManifestKey)).toMatchObject({
      size: plan.closureManifestBytes,
      customMetadata: { sha256: plan.closureManifestSha256 },
    });
    cacheCtx.memo = {};
    const emptyPack = await buildPack([]);
    const emptyRequestKey = `${prefix}/native-receive/input-stock-empty.request`;
    const emptySha256 = await sha256(emptyPack);
    await env.REPO_BUCKET.put(emptyRequestKey, emptyPack, {
      customMetadata: { sha256: emptySha256 },
    });
    const emptyPlan = await planStockReceive({
      ...planArgs,
      operationId: "stock-empty-plan-operation",
      inputRequestKey: emptyRequestKey,
      inputRequestBytes: emptyPack.byteLength,
      inputRequestSha256: emptySha256,
      packBytes: emptyPack.byteLength,
      commands: [{ oldOid: "0".repeat(40), newOid: active.commitOid, ref: "refs/heads/new" }],
    });
    expect(emptyPlan.incomingObjectCount).toBe(0);
    expect(emptyPlan.visitedIncomingObjectCount).toBe(0);
    expect(emptyPlan.semanticExternalOids).toEqual([active.commitOid]);
    expect(emptyPlan.requiredRootOids).toEqual([active.commitOid]);
    cacheCtx.memo = {};
    const directEmptyRequestKey = `${prefix}/native-receive/input-stock-direct-empty.request`;
    const directEnv = { ...env, STOCK_RECEIVE_DIRECT_PACK: "1" } as Env & {
      STOCK_RECEIVE_DIRECT_PACK: string;
    };
    const directEmptyPlan = await planStockReceive({
      ...planArgs,
      env: directEnv,
      operationId: "stock-direct-empty-plan-operation",
      inputRequestKey: directEmptyRequestKey,
      inputRequestBytes: emptyPack.byteLength,
      inputRequestSha256: emptySha256,
      inputPackBytes: emptyPack,
      packBytes: emptyPack.byteLength,
      commands: [{ oldOid: active.commitOid, newOid: active.commitOid, ref: "refs/heads/main" }],
    });
    expect(directEmptyPlan.executionMode).toBe("stock-container");
    expect(directEmptyPlan.incomingObjectCount).toBe(0);
    expect(directEmptyPlan.visitedIncomingObjectCount).toBe(0);
    expect(directEmptyPlan.semanticExternalOids).toEqual([active.commitOid]);
    expect(directEmptyPlan.requiredRootOids).toEqual([active.commitOid]);
    await env.REPO_BUCKET.delete([
      inputRequestKey,
      plan.prerequisitePackKey,
      plan.closureManifestKey,
      emptyRequestKey,
      emptyPlan.prerequisitePackKey,
      emptyPlan.closureManifestKey,
      directEmptyRequestKey,
      directEmptyPlan.prerequisitePackKey,
      directEmptyPlan.closureManifestKey,
    ]);
  });

  it("publishes a proven ordinary pack directly without rebuilding advertised history", async () => {
    const seeded = await setupRepoForTests(env, "o", uniqueRepoId("stock-direct-pack"));
    const stub = getRepoStub(env, seeded.doName);
    const active = await runDOWithRetry(
      () => stub,
      async (instance) => await instance.seedMinimalRepo()
    );
    const [catalog] = await stub.getActivePackCatalog();
    if (!catalog) throw new Error("expected seeded active pack");
    const author = "Display <display@example.invalid> 0 +0000";
    const commitPayload = new TextEncoder().encode(
      `tree ${active.treeOid}\nparent ${active.commitOid}\nauthor ${author}\ncommitter ${author}\n\ndirect\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([{ type: "commit", payload: commitPayload }]);
    const prefix = catalog.packKey.slice(0, catalog.packKey.indexOf("/objects/pack/"));
    const inputPackKey = `${prefix}/native-receive/direct-pack.request`;
    const inputRequestSha256 = await sha256(pack);
    const input = await env.REPO_BUCKET.put(inputPackKey, pack, {
      customMetadata: { sha256: inputRequestSha256 },
    });
    const outputPackKey = nativeReceiveOutputPackKey(
      prefix,
      "direct-pack-operation",
      "f".repeat(64)
    );
    const operation = {
      id: "direct-pack-operation",
      fingerprint: "f".repeat(64),
      leaseToken: "lease",
      repositoryId: seeded.doName,
      state: "processing",
      inputPackKey,
      inputBytes: pack.byteLength,
      inputEtag: input.etag,
      stockReceive: {
        inputRequestSha256,
        packOffset: 0,
        packBytes: pack.byteLength,
        advertisedRefs: [{ name: "refs/heads/main", oid: active.commitOid }],
      },
      outputPackKey,
      outputIdxKey: packIndexKey(outputPackKey),
      outputRefsKey: packRefsKey(outputPackKey),
      commands: [{ oldOid: active.commitOid, newOid: commit.oid, ref: "refs/heads/main" }],
      acceptedWrites: [],
      activeCatalog: [catalog],
      catalogGeneration: 1,
      createdAt: 1,
      updatedAt: 1,
      attempts: 1,
      cleanupPending: false,
    } satisfies NativeReceiveOperation;
    let directSubrequests = 0;
    const result = await executeStockReceiveWorkerDataPlane({
      env: { ...env, STOCK_RECEIVE_DIRECT_PACK: "1" } as Env,
      operation,
      inputPackBytes: pack,
      cacheCtx: {
        req: new Request("https://example.invalid/stock-direct-pack"),
        ctx: createExecutionContext(),
        memo: {},
      },
      limiter: new SubrequestLimiter(40),
      countSubrequest(_operation, n = 1) {
        directSubrequests += n;
      },
      logger: createLogger(env.LOG_LEVEL, { service: "StockDirectPackTest" }),
    });

    expect(result.processorResult).toMatchObject({
      executionMode: "direct-pack",
      packBytes: expect.any(Number),
      objectCount: 1,
      inputPackObjectCount: 1,
      hydratedBytes: 0,
      activePackWholeBytes: 0,
      activePackWholeRequests: 0,
      prerequisitePackBytes: 0,
      outputRequests: 3,
      outputValidationRequests: 3,
    });
    // This exact request count includes the bounded metadata reads plus
    // publication and body/head verification of the three authoritative outputs.
    // The ingress invocation retains the authenticated input bytes, so it does
    // not reread the durable recovery copy. A temporary planning-pack PUT, an
    // input reread, or its three cleanup deletes increases this count.
    expect(directSubrequests).toBe(13);
    expect(await validateStockReceivePreparedProof(operation, result.processorResult)).toBe(true);
    let recoverySubrequests = 0;
    const recovered = await executeStockReceiveWorkerDataPlane({
      env: { ...env, STOCK_RECEIVE_DIRECT_PACK: "1" } as Env,
      operation,
      cacheCtx: {
        req: new Request("https://example.invalid/stock-direct-pack-recovery"),
        ctx: createExecutionContext(),
        memo: {},
      },
      limiter: new SubrequestLimiter(40),
      countSubrequest(_operation, n = 1) {
        recoverySubrequests += n;
      },
      logger: createLogger(env.LOG_LEVEL, { service: "StockDirectPackRecoveryTest" }),
    });
    // Recovery rereads durable input and verifies the already-published exact
    // immutable artifacts instead of trusting invocation-local memory.
    expect(recoverySubrequests).toBe(18);
    expect(recovered.processorResult).toMatchObject({
      packSha256: result.processorResult.packSha256,
      idxSha256: result.processorResult.idxSha256,
      refsSha256: result.processorResult.refsSha256,
      inputRequests: 1,
    });
    expect(
      new Uint8Array(await (await env.REPO_BUCKET.get(outputPackKey))!.arrayBuffer())
    ).not.toEqual(pack);
    expect(await env.REPO_BUCKET.head(packIndexKey(outputPackKey))).not.toBeNull();
    expect(await env.REPO_BUCKET.head(packRefsKey(outputPackKey))).not.toBeNull();

    for (const failedRole of ["manifest", "pack"] as const) {
      stockPlannerTest.reset();
      const failedId = `direct-pack-partial-${failedRole}`;
      const failedInputPackKey = `${prefix}/native-receive/${failedId}.request`;
      const failedInput = await env.REPO_BUCKET.put(failedInputPackKey, pack, {
        customMetadata: { sha256: inputRequestSha256 },
      });
      const failedOutputPackKey = nativeReceiveOutputPackKey(
        prefix,
        failedId,
        (failedRole === "manifest" ? "c" : "d").repeat(64)
      );
      const failedOperation = {
        ...operation,
        id: failedId,
        fingerprint: (failedRole === "manifest" ? "c" : "d").repeat(64),
        inputPackKey: failedInputPackKey,
        inputEtag: failedInput.etag,
        outputPackKey: failedOutputPackKey,
        outputIdxKey: packIndexKey(failedOutputPackKey),
        outputRefsKey: packRefsKey(failedOutputPackKey),
      } satisfies NativeReceiveOperation;
      const failedPlanKeys = stockReceivePlanKeys(failedInputPackKey, inputRequestSha256);
      stockPlannerTest.failImmutablePut(
        failedId,
        failedRole === "manifest" ? failedPlanKeys.closureManifestKey : failedOutputPackKey
      );
      let partialFailureSubrequests = 0;

      await expect(
        executeStockReceiveWorkerDataPlane({
          env: { ...env, STOCK_RECEIVE_DIRECT_PACK: "1" } as Env,
          operation: failedOperation,
          inputPackBytes: pack,
          cacheCtx: {
            req: new Request(`https://example.invalid/${failedId}`),
            ctx: createExecutionContext(),
            memo: {},
          },
          limiter: new SubrequestLimiter(40),
          countSubrequest(_operation, n = 1) {
            partialFailureSubrequests += n;
          },
          logger: createLogger(env.LOG_LEVEL, { service: "StockDirectPackPartialFailureTest" }),
        })
      ).rejects.toThrow("stock-plan:direct-publication-failed");
      // Three active-pack metadata reads, all four concurrent publication
      // attempts, and deletion of the three successfully created siblings.
      expect(partialFailureSubrequests).toBe(10);

      for (const key of [
        failedPlanKeys.closureManifestKey,
        failedOutputPackKey,
        packIndexKey(failedOutputPackKey),
        packRefsKey(failedOutputPackKey),
      ]) {
        expect(await env.REPO_BUCKET.head(key)).toBeNull();
      }
      await env.REPO_BUCKET.delete(failedInputPackKey);
    }

    const failedOutputPackKey = nativeReceiveOutputPackKey(
      prefix,
      "direct-pack-integrity-failure",
      "e".repeat(64)
    );
    const failedInputPackKey = `${prefix}/native-receive/direct-pack-integrity.request`;
    const failedInput = await env.REPO_BUCKET.put(failedInputPackKey, pack, {
      customMetadata: { sha256: inputRequestSha256 },
    });
    const failedOperation = {
      ...operation,
      id: "direct-pack-integrity-failure",
      fingerprint: "e".repeat(64),
      inputPackKey: failedInputPackKey,
      inputEtag: failedInput.etag,
      outputPackKey: failedOutputPackKey,
      outputIdxKey: packIndexKey(failedOutputPackKey),
      outputRefsKey: packRefsKey(failedOutputPackKey),
    } satisfies NativeReceiveOperation;
    stockDataPlaneTest.failOutputIntegrity(failedOperation.id, "pack");
    const integrityFailure = await executeStockReceiveWorkerDataPlane({
      env: { ...env, STOCK_RECEIVE_DIRECT_PACK: "1" } as Env,
      operation: failedOperation,
      cacheCtx: {
        req: new Request("https://example.invalid/stock-direct-pack-integrity"),
        ctx: createExecutionContext(),
        memo: {},
      },
      limiter: new SubrequestLimiter(50),
      countSubrequest() {},
      logger: createLogger(env.LOG_LEVEL, { service: "StockDirectPackIntegrityTest" }),
    }).catch((error: unknown) => error);
    expect(integrityFailure).toMatchObject({
      rejection: {
        code: "output-integrity-invalid",
        processorResult: {
          outputValidationBytes: pack.byteLength,
          outputValidationRequests: 1,
          outputIntegrityRejectedRole: "pack",
          outputIntegrityRejectedAt: "body",
        },
      },
    });
    expect(stockDataPlaneTest.outputMutationFault(failedOperation.id)).toEqual({
      operationId: failedOperation.id,
      role: "pack",
      triggered: true,
      bytesMutated: true,
      staleCustomMetadataSha256Preserved: true,
    });
    await cleanupStockReceiveWorkerDataPlane({
      env,
      operation: failedOperation,
      limiter: new SubrequestLimiter(20),
      countSubrequest() {},
      logger: createLogger(env.LOG_LEVEL, { service: "StockDirectPackFailureCleanupTest" }),
      includeOutputs: true,
    });

    await cleanupStockReceiveWorkerDataPlane({
      env,
      operation,
      limiter: new SubrequestLimiter(20),
      countSubrequest() {},
      logger: createLogger(env.LOG_LEVEL, { service: "StockDirectPackCleanupTest" }),
      includeOutputs: true,
    });
  });

  it("commits and reads a thin direct pack while returning negotiated sideband status", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stock-direct-thin");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    const active = await runDOWithRetry(
      () => stub,
      async (instance) => await instance.seedMinimalRepo()
    );
    const author = "You <you@example.com> 0 +0000";
    const basePayload = new TextEncoder().encode(
      `tree ${active.treeOid}\nauthor ${author}\ncommitter ${author}\n\ninitial\n`
    );
    const suffix = new TextEncoder().encode("direct thin\n");
    const nextPayload = concatChunks([basePayload, suffix]);
    const next = await encodeGitObject("commit", nextPayload);
    const pack = await buildPack([
      {
        type: "ref-delta",
        baseOid: active.commitOid,
        delta: buildAppendOnlyDelta(basePayload, suffix),
      },
    ]);
    const rawPrefix = concatChunks([
      pktLine(
        `${active.commitOid} ${next.oid} refs/heads/main\0 report-status side-band-64k agent=git/2.50.1\n`
      ),
      flushPkt(),
    ]);
    const requestBody = concatChunks([rawPrefix, pack]);
    const plainStatus = concatChunks([
      pktLine("unpack ok\n"),
      pktLine("ok refs/heads/main\n"),
      flushPkt(),
    ]);
    const response = await handleStreamingReceivePackPOST(
      {
        ...env,
        NATIVE_RECEIVE_CONTAINER: "1",
        STOCK_RECEIVE_DIRECT_PACK: "1",
      } as Env,
      seeded.doName,
      new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-git-receive-pack-request",
          "Content-Length": String(requestBody.byteLength),
          "X-Display-Operation-ID": "stock-direct-thin-operation",
        },
        body: toRequestBody(requestBody),
      }),
      createExecutionContext(),
      {
        limiter: new SubrequestLimiter(900),
        acceptedWriteContext: {
          repositoryId: seeded.doName,
          actor: "stock-direct-thin-test",
          sourceSurface: "git-push",
          idempotencyKey: null,
        },
      }
    );

    expect(response.status).toBe(200);
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(
      buildSidebandReceiveBody(plainStatus)
    );
    expect((await stub.listRefs()).find((ref) => ref.name === "refs/heads/main")?.oid).toBe(
      next.oid
    );
    const committed = await stub.getNativeReceiveOperation("stock-direct-thin-operation");
    expect(committed).toMatchObject({
      state: "committed",
      metrics: {
        executionMode: "direct-pack",
        activePackWholeRequests: 0,
        activePackRangeRequests: 1,
        inputRequests: 0,
        outputValidationRequests: 3,
        stockTiming: {
          planningPhases: {
            activeMetadataMs: expect.any(Number),
            advertisedClosureMs: expect.any(Number),
            inputStagingMs: expect.any(Number),
            incomingAnalysisMs: expect.any(Number),
            boundaryValidationMs: expect.any(Number),
            physicalPlanMs: expect.any(Number),
            canonicalizationMs: expect.any(Number),
            manifestPublishMs: expect.any(Number),
            postManifestCleanupAndOverheadMs: expect.any(Number),
          },
        },
      },
    });
    const canonicalPackKey = committed?.result?.packKey;
    if (!canonicalPackKey) throw new Error("expected canonical direct pack");
    const canonicalPackObject = await env.REPO_BUCKET.get(canonicalPackKey);
    if (!canonicalPackObject) throw new Error("expected canonical direct pack bytes");
    const canonicalPackBytes = new Uint8Array(await canonicalPackObject.arrayBuffer());
    expect((canonicalPackBytes[12]! >> 4) & 0x07).toBe(1);
    const loaded = await readObject(env, seeded.doName, next.oid, {
      req: new Request("https://example.invalid/stock-direct-thin-read"),
      ctx: createExecutionContext(),
      memo: {},
    });
    expect(loaded?.type).toBe("commit");
    expect(loaded?.payload).toEqual(nextPayload);

    const secondSuffix = new TextEncoder().encode("second direct thin\n");
    const secondPayload = concatChunks([nextPayload, secondSuffix]);
    const second = await encodeGitObject("commit", secondPayload);
    const secondPack = await buildPack([
      {
        type: "ref-delta",
        baseOid: next.oid,
        delta: buildAppendOnlyDelta(nextPayload, secondSuffix),
      },
    ]);
    const secondPrefix = concatChunks([
      pktLine(
        `${next.oid} ${second.oid} refs/heads/main\0 report-status side-band-64k agent=git/2.50.1\n`
      ),
      flushPkt(),
    ]);
    const secondResponse = await handleStreamingReceivePackPOST(
      {
        ...env,
        NATIVE_RECEIVE_CONTAINER: "1",
        STOCK_RECEIVE_DIRECT_PACK: "1",
      } as Env,
      seeded.doName,
      new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-git-receive-pack-request",
          "Content-Length": String(secondPrefix.byteLength + secondPack.byteLength),
          "X-Display-Operation-ID": "stock-direct-thin-operation-2",
        },
        body: toRequestBody(concatChunks([secondPrefix, secondPack])),
      }),
      createExecutionContext(),
      {
        limiter: new SubrequestLimiter(900),
        acceptedWriteContext: {
          repositoryId: seeded.doName,
          actor: "stock-direct-thin-test",
          sourceSurface: "git-push",
          idempotencyKey: null,
        },
      }
    );
    expect(secondResponse.status).toBe(200);
    expect(new Uint8Array(await secondResponse.arrayBuffer())).toEqual(
      buildSidebandReceiveBody(plainStatus)
    );
    expect(await stub.getNativeReceiveOperation("stock-direct-thin-operation-2")).toMatchObject({
      state: "committed",
      metrics: {
        executionMode: "direct-pack",
        activePackRangeRequests: 1,
      },
    });
    const secondLoaded = await readObject(env, seeded.doName, second.oid, {
      req: new Request("https://example.invalid/stock-direct-thin-read-2"),
      ctx: createExecutionContext(),
      memo: {},
    });
    expect(secondLoaded?.payload).toEqual(secondPayload);
  });

  it("selectively hydrates an ordinary bounded receive and buffers success until RepoDO commit", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stock-receive");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    const active = await runDOWithRetry(
      () => stub,
      async (instance) => await instance.seedMinimalRepo()
    );
    const gc = await stub.beginReachabilityGc();
    if (!gc.ok) throw new Error("expected GC lease for published-catalog receive test");
    const [sourcePack] = gc.activeCatalog;
    if (!sourcePack) throw new Error("expected seeded source pack");
    const replacementPackKey = r2PackKey(
      doPrefix(stub.id.toString()),
      `pack-gc-${gc.lease.token}.pack`
    );
    for (const [sourceKey, targetKey] of [
      [sourcePack.packKey, replacementPackKey],
      [packIndexKey(sourcePack.packKey), packIndexKey(replacementPackKey)],
      [packRefsKey(sourcePack.packKey), packRefsKey(replacementPackKey)],
    ] as const) {
      const sourceObject = await env.REPO_BUCKET.get(sourceKey);
      if (!sourceObject) throw new Error("expected seeded GC artifact");
      await env.REPO_BUCKET.put(targetKey, await sourceObject.arrayBuffer(), {
        customMetadata: sourceObject.customMetadata,
      });
    }
    expect(
      await stub.recordReachabilityGcPending({
        token: gc.lease.token,
        packKey: replacementPackKey,
      })
    ).toEqual({ status: "recorded" });
    const gcCommit = await stub.commitReachabilityGc({
      token: gc.lease.token,
      refsVersion: gc.refsVersion,
      packsetVersion: gc.packsetVersion,
      sourcePacks: gc.activeCatalog,
      stagedPack: {
        packKey: replacementPackKey,
        packBytes: sourcePack.packBytes,
        idxBytes: sourcePack.idxBytes,
        objectCount: sourcePack.objectCount,
      },
    });
    if (gcCommit.status !== "committed") {
      throw new Error(`GC catalog publication failed: ${gcCommit.reason}`);
    }
    expect(
      await publishRepositoryGeneration({
        env,
        doId: stub.id.toString(),
        generation: gcCommit.packCatalogVersion,
        activePackKeys: [replacementPackKey],
        limiter: new SubrequestLimiter(20),
        countSubrequest() {},
        log: createLogger(env.LOG_LEVEL, { service: "StockReceivePublishedGcCatalogTest" }),
      })
    ).toBe("published");
    expect(await stub.completeGenerationPublication(gcCommit.packCatalogVersion)).toBe(true);
    expect(await stub.getActivePackCatalog()).toEqual([
      expect.objectContaining({ packKey: replacementPackKey, state: "active" }),
    ]);
    const author = "Display <display@example.invalid> 0 +0000";
    const commitPayload = new TextEncoder().encode(
      `tree ${active.treeOid}\nparent ${active.commitOid}\nauthor ${author}\ncommitter ${author}\n\nstock\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([{ type: "commit", payload: commitPayload }]);
    const prefix = concatChunks([
      pktLine(
        `${active.commitOid} ${commit.oid} refs/heads/main\0 report-status agent=git/2.50.1\n`
      ),
      flushPkt(),
    ]);
    const requestBody = concatChunks([prefix, pack]);
    const receivePackResponse = concatChunks([
      pktLine("unpack ok\n"),
      pktLine("ok refs/heads/main\n"),
      flushPkt(),
    ]);
    let stagedRequest: Uint8Array | undefined;
    let expectedPlan: Awaited<ReturnType<typeof planStockReceive>> | undefined;
    let workerExecutionCount = 0;

    stockDataPlaneTest.setWorkerExecutor(
      async ({ operation, cacheCtx, limiter, countSubrequest, logger }) => {
        workerExecutionCount++;
        const request = {
          operationId: operation.id,
          inputPackKey: operation.inputPackKey,
          inputBytes: operation.inputBytes,
          activePacks: operation.activeCatalog.map((activePack) => ({
            packKey: activePack.packKey,
            packBytes: activePack.packBytes,
            idxBytes: activePack.idxBytes,
          })),
          commands: operation.commands,
          outputPackKey: operation.outputPackKey,
          outputIdxKey: operation.outputIdxKey,
          outputRefsKey: operation.outputRefsKey,
          stockReceive: operation.stockReceive,
        };
        expect(request.stockReceive).toMatchObject({
          packOffset: prefix.byteLength,
          packBytes: pack.byteLength,
          advertisedRefs: [{ name: "refs/heads/main", oid: active.commitOid }],
        });
        const staged = await env.REPO_BUCKET.get(request.inputPackKey);
        if (!staged) throw new Error("stock request was not staged");
        stagedRequest = new Uint8Array(await staged.arrayBuffer());
        expect(stagedRequest).toEqual(requestBody);
        expect(request.stockReceive?.inputRequestSha256).toBe(await sha256(requestBody));

        const stock = operation.stockReceive!;
        const plan = await planStockReceive({
          env,
          repoId: operation.repositoryId,
          operationId: operation.id,
          inputRequestKey: operation.inputPackKey,
          inputRequestBytes: operation.inputBytes,
          inputRequestSha256: stock.inputRequestSha256,
          packOffset: stock.packOffset,
          packBytes: stock.packBytes,
          advertisedRefs: stock.advertisedRefs,
          commands: operation.commands,
          activePacks: request.activePacks,
          cacheCtx,
          limiter,
          countSubrequest: (count) => countSubrequest("test-stock-plan", count),
          log: logger,
        });
        expectedPlan = plan;
        const artifacts = [
          [request.outputPackKey, pack],
          [request.outputIdxKey, new Uint8Array([2])],
          [request.outputRefsKey, new Uint8Array([3])],
        ] as const;
        for (const [key, bytes] of artifacts) {
          await env.REPO_BUCKET.put(key, bytes, {
            customMetadata: { sha256: await sha256(bytes) },
          });
        }
        const result = {
          operationId: request.operationId,
          packBytes: pack.byteLength,
          idxBytes: 1,
          refsBytes: 1,
          objectCount: 1,
          inputPackObjectCount: 1,
          packSha1: bytesToHex(pack.subarray(pack.byteLength - 20)),
          packSha256: await sha256(pack),
          idxSha256: await sha256(new Uint8Array([2])),
          refsSha256: await sha256(new Uint8Array([3])),
          inputRequestSha256: await sha256(requestBody),
          receivePackResponse: btoa(String.fromCharCode(...receivePackResponse)),
          stockTrace: stockTrace.map((event, index) => ({ sequence: index + 1, event })),
          elapsedMs: 1,
          processorStartedAt: operation.createdAt,
          stockTiming: {
            planningMs: 1,
            bundleReadMs: 2,
            containerRpcMs: 3,
            containerProcessMs: 2,
            containerReadinessMs: 1,
            outputUploadMs: 1,
            outputVerificationMs: 1,
            proofValidationMs: 1,
            containerStartAttempts: 0,
            containerProbeAttempts: 1,
            containerWasRunning: true,
          },
          scratchBytes: requestBody.byteLength + pack.byteLength + 2,
          hydratedBytes: 0,
          downloadedBytes: requestBody.byteLength,
          cacheHitBytes: 0,
          metadataBytes: 0,
          quarantinePathInsideOwnedWorkRoot: true,
          quarantineRemovedAfterReceive: true,
          quarantinePathNonEmpty: true,
          freshWorkDirectory: true,
          repositoryPackBytesBeforeHydration: 0,
          sharedObjectCacheDisabled: true,
          skipConnectivityCheck: false,
          planSha256: plan.planSha256,
          closureProof: {
            planSha256: plan.planSha256,
            incomingOids: [commit.oid],
            semanticExternalOids: plan.semanticExternalOids,
            visitedIncomingObjectCount: plan.visitedIncomingObjectCount,
            logicalEdgeCount: plan.logicalEdgeCount,
            internalEdgeCount: plan.internalEdgeCount,
            externalEdgeCount: plan.externalEdgeCount,
            missingObjectCount: plan.missingObjectCount,
            objectTypeCounts: plan.objectTypeCounts,
          },
          semanticExternalOids: plan.semanticExternalOids,
          thinDeltaBaseOids: plan.thinDeltaBaseOids,
          requiredRootOids: plan.requiredRootOids,
          prerequisiteObjectOids: plan.requiredRootOids,
          physicalNodes: plan.physicalNodes,
          physicalDependencies: plan.dependencies,
          topologicalEntryIds: plan.topologicalEntryIds,
          selectedPackChecksums: plan.selectedPackChecksums,
          activePackBindings: plan.activePackBindings,
          ranges: plan.ranges,
          activePackReads: plan.activePackReads,
          activePackTrailerBytes: plan.activePackTrailerBytes,
          activePackTrailerRequests: plan.activePackTrailerRequests,
          activePackRangeBytes: plan.activePackRangeBytes,
          activePackRangeRequests: plan.activePackRangeRequests,
          activePackWholeBytes: plan.activePackWholeBytes,
          activePackWholeRequests: plan.activePackWholeRequests,
          activePackUnattributedBytes: plan.activePackUnattributedBytes,
          activePackUnattributedRequests: plan.activePackUnattributedRequests,
          closureManifestKey: plan.closureManifestKey,
          closureManifestBytes: plan.closureManifestBytes,
          closureManifestSha256: plan.closureManifestSha256,
          closureManifestEtag: plan.closureManifestEtag,
          prerequisitePackKey: plan.prerequisitePackKey,
          prerequisitePackBytes: plan.prerequisitePackBytes,
          prerequisitePackSha256: plan.prerequisitePackSha256,
          prerequisitePackEtag: plan.prerequisitePackEtag,
          incomingObjectCount: plan.incomingObjectCount,
          visitedIncomingObjectCount: plan.visitedIncomingObjectCount,
          logicalEdgeCount: plan.logicalEdgeCount,
          internalEdgeCount: plan.internalEdgeCount,
          externalEdgeCount: plan.externalEdgeCount,
          missingObjectCount: plan.missingObjectCount,
          objectTypeCounts: plan.objectTypeCounts,
          selectedPackBytes: plan.selectedPackBytes,
          activePackCount: plan.activePackCount,
          rangeBytes: plan.rangeBytes,
          rangeRequests: plan.rangeRequests,
          packsTouched: plan.packsTouched,
        };
        expect(await validateStockReceivePreparedProof(operation, result)).toBe(true);
        const [catalogRow] = operation.activeCatalog;
        const [binding] = plan.activePackBindings;
        const trailerRead = plan.activePackReads.find((read) => read.kind === "trailer");
        expect(catalogRow).toBeDefined();
        expect(binding).toBeDefined();
        expect(trailerRead).toBeDefined();
        const duplicatePackKey = `${catalogRow!.packKey}.duplicate`;
        expect(
          await validateStockReceivePreparedProof(
            {
              ...operation,
              activeCatalog: [
                ...operation.activeCatalog,
                { ...catalogRow!, packKey: duplicatePackKey },
              ],
            },
            {
              ...result,
              activePackBindings: [
                ...plan.activePackBindings,
                { ...binding!, packKey: duplicatePackKey },
              ],
              activePackReads: [...plan.activePackReads, trailerRead!],
              activePackTrailerBytes: plan.activePackTrailerBytes + 20,
              activePackTrailerRequests: plan.activePackTrailerRequests + 1,
              activePackCount: plan.activePackCount + 1,
            }
          )
        ).toBe(true);
        expect(
          await validateStockReceivePreparedProof(
            {
              ...operation,
              activeCatalog: [
                ...operation.activeCatalog,
                { ...catalogRow!, packKey: duplicatePackKey },
              ],
            },
            {
              ...result,
              activePackBindings: [
                ...plan.activePackBindings,
                { ...binding!, packKey: duplicatePackKey, idxSha256: "f".repeat(64) },
              ],
              activePackReads: [...plan.activePackReads, trailerRead!],
              activePackTrailerBytes: plan.activePackTrailerBytes + 20,
              activePackTrailerRequests: plan.activePackTrailerRequests + 1,
              activePackCount: plan.activePackCount + 1,
            }
          )
        ).toBe(false);
        expect(
          await validateStockReceivePreparedProof(operation, {
            ...result,
            thinDeltaBaseOids: [plan.semanticExternalOids[0]!],
            objectCount: result.incomingObjectCount + 1,
          })
        ).toBe(false);
        return result;
      }
    );

    const routeStartedAt = Date.now();
    const nowSpy = vi
      .spyOn(Date, "now")
      .mockReturnValueOnce(routeStartedAt)
      .mockReturnValue(routeStartedAt + 1_000);
    let response: Response;
    try {
      response = await handleStreamingReceivePackPOST(
        { ...env, NATIVE_RECEIVE_CONTAINER: "1" },
        seeded.doName,
        new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-git-receive-pack-request",
            "Content-Length": String(requestBody.byteLength),
            "X-Display-Operation-ID": "stock-tiny-operation",
          },
          body: toRequestBody(requestBody),
        }),
        createExecutionContext(),
        {
          limiter: new SubrequestLimiter(900),
          acceptedWriteContext: {
            repositoryId: seeded.doName,
            actor: "stock-spike-test",
            sourceSurface: "git-push",
            idempotencyKey: null,
          },
        }
      );
    } finally {
      nowSpy.mockRestore();
      nativePipelineTest.reset();
    }

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("application/x-git-receive-pack-result");
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(receivePackResponse);
    expect(stagedRequest).toEqual(requestBody);
    expect((await stub.listRefs()).find((ref) => ref.name === "refs/heads/main")?.oid).toBe(
      commit.oid
    );
    const committedOperation = await stub.getNativeReceiveOperation("stock-tiny-operation");
    expect(committedOperation?.createdAt).toBe(routeStartedAt + 1_000);
    expect(committedOperation?.events?.[0]).toEqual({
      sequence: 1,
      phase: "worker-route-receive-start",
      at: routeStartedAt,
    });
    expect(committedOperation).toMatchObject({
      state: "committed",
      result: {
        changed: true,
        receivePackResponse: btoa(String.fromCharCode(...receivePackResponse)),
        authorityPublication: {
          refs: [{ name: "refs/heads/main", oid: commit.oid }],
          receipt: {
            disposition: "committed",
            refName: "refs/heads/main",
            newOid: commit.oid,
          },
        },
      },
      clientAckReadyAt: expect.any(Number),
      metrics: {
        elapsedMs: 1,
        processorStartedAt: expect.any(Number),
        stockTiming: {
          planningMs: 1,
          bundleReadMs: 2,
          containerRpcMs: 3,
          containerProcessMs: 2,
          containerReadinessMs: 1,
          outputUploadMs: 1,
          outputVerificationMs: 1,
          proofValidationMs: 1,
          containerStartAttempts: 0,
          containerProbeAttempts: 1,
          containerWasRunning: true,
        },
        activePackReads: expectedPlan!.activePackReads,
        activePackTrailerBytes: expectedPlan!.activePackTrailerBytes,
        activePackTrailerRequests: expectedPlan!.activePackTrailerRequests,
        activePackRangeBytes: expectedPlan!.activePackRangeBytes,
        activePackRangeRequests: expectedPlan!.activePackRangeRequests,
        activePackWholeBytes: expectedPlan!.activePackWholeBytes,
        activePackWholeRequests: expectedPlan!.activePackWholeRequests,
        activePackUnattributedBytes: 0,
        activePackUnattributedRequests: 0,
        outputValidationBytes: pack.byteLength + 2,
        outputValidationRequests: 3,
      },
    });
    expect(committedOperation?.events?.map((event) => event.phase)).toEqual([
      "worker-route-receive-start",
      "repo-do-operation-staged",
      "go-processor-start",
      "receive-pack-start",
      "pre-receive-start",
      "quarantine-visible",
      "replacement-closure-start",
      "replacement-closure-complete",
      "pre-receive-complete",
      "disposable-ref-updated",
      "output-integrity-verified",
      "wal-put-complete",
      "authoritative-ref-cas",
      "receipt-committed",
      "worker-response-ack",
    ]);
    const timestampedEvents = (committedOperation?.events ?? []).filter(
      (event): event is typeof event & { at: number } => event.at !== undefined
    );
    expect(timestampedEvents.map((event) => event.phase)).toEqual([
      "worker-route-receive-start",
      "go-processor-start",
      "output-integrity-verified",
      "worker-response-ack",
    ]);
    expect(
      timestampedEvents.every(
        (event, index, events) => index === 0 || event.at >= events[index - 1]!.at
      )
    ).toBe(true);
    expect(await stub.getActivePackCatalog()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ packKey: replacementPackKey, state: "active" }),
        expect.objectContaining({ kind: "receive", state: "active" }),
      ])
    );
    const queryResponse = await workerExports.default.fetch(
      new Request(
        `https://worker.invalid/_internal/receives/${seeded.namespaceSlug}/${seeded.repoSlug}/stock-tiny-operation`,
        { headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` } }
      )
    );
    expect(queryResponse.status).toBe(200);
    expect(await queryResponse.json()).toStrictEqual(
      JSON.parse(JSON.stringify(committedOperation))
    );
    const matchingView = structuredClone(committedOperation!);
    expect(nativeReceiveOperationEvidenceMatches(matchingView, committedOperation!)).toBe(true);
    const reverseKeyOrder = (value: unknown): unknown => {
      if (Array.isArray(value)) return value.map(reverseKeyOrder);
      if (!value || typeof value !== "object") return value;
      return Object.fromEntries(
        Object.entries(value)
          .reverse()
          .map(([key, member]) => [key, reverseKeyOrder(member)])
      );
    };
    expect(
      nativeReceiveOperationEvidenceMatches(
        reverseKeyOrder(matchingView) as typeof matchingView,
        committedOperation!
      )
    ).toBe(true);
    expect(
      nativeReceiveOperationEvidenceMatches(
        { ...matchingView, attempts: matchingView.attempts + 1 },
        committedOperation!
      )
    ).toBe(false);
    expect(
      nativeReceiveOperationEvidenceMatches(
        { ...matchingView, updatedAt: matchingView.updatedAt + 1 },
        committedOperation!
      )
    ).toBe(false);
    const authority = committedOperation?.result?.authorityPublication;
    expect(authority).toBeDefined();
    const storedRef = await env.REPO_BUCKET.get(authority!.refs[0]!.key);
    const storedReceipt = await env.REPO_BUCKET.get(authority!.receipt.key);
    expect(await storedRef!.json()).toEqual({
      schemaVersion: 1,
      kind: "authoritative-ref",
      name: "refs/heads/main",
      oid: commit.oid,
    });
    expect(await storedReceipt!.json()).toEqual({
      schemaVersion: 1,
      kind: "operation-receipt",
      disposition: "committed",
      refName: "refs/heads/main",
      newOid: commit.oid,
      digest: authority!.receipt.digest,
    });

    let deferredInputKey = "";
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const operation = await state.storage.get<NativeReceiveOperation>(
          nativeReceiveOperationKey("stock-tiny-operation")
        );
        expect(operation).toMatchObject({ state: "committed", cleanupPending: true });
        deferredInputKey = operation!.inputPackKey;
      }
    );
    expect(await env.REPO_BUCKET.head(deferredInputKey)).not.toBeNull();
    const cleanupSendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    await runDurableObjectAlarm(stub);
    const cleanupMessage = cleanupSendSpy.mock.calls
      .map(([message]) => message)
      .find(
        (message): message is { kind: string; doId: string; operationId: string } =>
          typeof message === "object" &&
          message !== null &&
          "kind" in message &&
          message.kind === "native-receive" &&
          "operationId" in message &&
          message.operationId === "stock-tiny-operation" &&
          "doId" in message &&
          typeof message.doId === "string"
      );
    cleanupSendSpy.mockRestore();
    expect(await runQueueMessage(cleanupMessage)).toEqual({ acked: true, retried: false });
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        expect(
          await state.storage.get<NativeReceiveOperation>(
            nativeReceiveOperationKey("stock-tiny-operation")
          )
        ).toMatchObject({ state: "committed", cleanupPending: false });
      }
    );
    expect(await env.REPO_BUCKET.head(deferredInputKey)).toBeNull();
    expect(await env.REPO_BUCKET.head(committedOperation!.result!.packKey!)).not.toBeNull();
    expect(
      await env.REPO_BUCKET.head(packIndexKey(committedOperation!.result!.packKey!))
    ).not.toBeNull();
    expect(
      await env.REPO_BUCKET.head(packRefsKey(committedOperation!.result!.packKey!))
    ).not.toBeNull();

    await env.REPO_BUCKET.delete([
      ...authority!.refs.map((ref) => ref.key),
      authority!.receipt.key,
    ]);
    await env.REPO_BUCKET.put(
      authority!.refs[0]!.key,
      JSON.stringify({
        schemaVersion: 1,
        kind: "authoritative-ref",
        name: "refs/heads/main",
        oid: commit.oid,
      })
    );
    await env.REPO_BUCKET.put(
      authority!.receipt.key,
      JSON.stringify({
        schemaVersion: 1,
        kind: "operation-receipt",
        disposition: "committed",
        refName: "refs/heads/main",
        newOid: commit.oid,
        digest: authority!.receipt.digest,
      })
    );
    let recoveryPublicationToken = "";
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const key = nativeReceiveOperationKey("stock-tiny-operation");
        const operation = await state.storage.get<NativeReceiveOperation>(key);
        if (!operation?.publicationPlan || !operation.result) {
          throw new Error("expected committed stock publication state");
        }
        recoveryPublicationToken = operation.publicationPlan.token;
        await state.storage.put(key, {
          ...operation,
          state: "finalizing",
          cleanupPending: true,
          clientAckReadyAt: undefined,
          result: { ...operation.result, authorityPublication: undefined },
        });
      }
    );
    expect((await stub.beginReceive()).ok).toBe(false);
    expect(await stub.beginStockReceiveRecovery("different-operation")).toEqual({
      status: "not_found",
    });

    const sendSpy = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockImplementation(async () => createQueueSendResponse());
    await runDurableObjectAlarm(stub);
    const recoveryMessage = sendSpy.mock.calls
      .map(([message]) => message)
      .find(
        (message): message is { kind: string; doId: string; operationId: string } =>
          typeof message === "object" &&
          message !== null &&
          "kind" in message &&
          message.kind === "native-receive" &&
          "operationId" in message &&
          message.operationId === "stock-tiny-operation" &&
          "doId" in message &&
          typeof message.doId === "string"
      );
    sendSpy.mockRestore();
    expect(recoveryMessage).toEqual({
      kind: "native-receive",
      doId: stub.id.toString(),
      operationId: "stock-tiny-operation",
    });
    await stub.getNativeReceiveOperation("stock-tiny-operation");
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        expect(await state.storage.getAlarm()).toBeGreaterThan(Date.now() + 1_000);
      }
    );
    expect(await runQueueMessage(recoveryMessage)).toEqual({ acked: true, retried: false });
    const queueConfirmed = await stub.getNativeReceiveOperation("stock-tiny-operation");
    expect(queueConfirmed).toMatchObject({
      state: "committed",
      clientAckReadyAt: undefined,
      result: { authorityPublication: expect.any(Object) },
    });
    expect(
      await stub.confirmStockReceivePublication(recoveryPublicationToken, authority!, true)
    ).toMatchObject({
      status: "replayed",
      operation: {
        clientAckReadyAt: expect.any(Number),
        events: expect.arrayContaining([
          expect.objectContaining({ phase: "worker-response-ack", at: expect.any(Number) }),
        ]),
      },
    });
    // Restore the queue-confirmed/no-ack state so the following route-led
    // recovery continues to prove the retained acknowledgement RPC.
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const key = nativeReceiveOperationKey("stock-tiny-operation");
        const operation = await state.storage.get<NativeReceiveOperation>(key);
        if (!operation) throw new Error("expected queue-confirmed stock operation");
        await state.storage.put(key, {
          ...operation,
          clientAckReadyAt: undefined,
          events: operation.events?.filter((event) => event.phase !== "worker-response-ack"),
        });
      }
    );

    const publicationRecovery = await handleStreamingReceivePackPOST(
      { ...env, NATIVE_RECEIVE_CONTAINER: "1" },
      seeded.doName,
      new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-git-receive-pack-request",
          "Content-Length": String(requestBody.byteLength),
          "X-Display-Operation-ID": "stock-tiny-operation",
          "X-Display-Spike1b-Stock": "1",
        },
        body: toRequestBody(requestBody),
      }),
      createExecutionContext(),
      {
        limiter: new SubrequestLimiter(900),
        acceptedWriteContext: {
          repositoryId: seeded.doName,
          actor: "stock-spike-test",
          sourceSurface: "git-push",
          idempotencyKey: null,
        },
      }
    );
    expect(publicationRecovery.status).toBe(200);
    expect(new Uint8Array(await publicationRecovery.arrayBuffer())).toEqual(receivePackResponse);
    expect(workerExecutionCount).toBe(1);
    expect(await env.REPO_BUCKET.head(authority!.refs[0]!.key)).not.toBeNull();
    expect(await env.REPO_BUCKET.head(authority!.receipt.key)).not.toBeNull();
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        expect(await state.storage.get("stockReceiveRecoveryLease")).toBeUndefined();
      }
    );

    const replay = await handleStreamingReceivePackPOST(
      { ...env, NATIVE_RECEIVE_CONTAINER: "1" },
      seeded.doName,
      new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-git-receive-pack-request",
          "Content-Length": String(requestBody.byteLength),
          "X-Display-Operation-ID": "stock-tiny-operation",
          "X-Display-Spike1b-Stock": "1",
        },
        body: toRequestBody(requestBody),
      }),
      createExecutionContext(),
      {
        limiter: new SubrequestLimiter(900),
        acceptedWriteContext: {
          repositoryId: seeded.doName,
          actor: "stock-spike-test",
          sourceSurface: "git-push",
          idempotencyKey: null,
        },
      }
    );
    expect(new Uint8Array(await replay.arrayBuffer())).toEqual(receivePackResponse);
    expect(workerExecutionCount).toBe(1);
    expect(
      (await env.REPO_BUCKET.list()).objects
        .filter((object) => object.key.includes("/native-receive/input-"))
        .map((object) => object.key)
    ).toEqual([]);
  });

  it("commits and replays a ref-only rollback without catalog or artifact mutation", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stock-ref-only");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    const author = "Display <display@example.invalid> 0 +0000";
    const blobPayload = new TextEncoder().encode("ref-only\n");
    const blob = await encodeGitObject("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "state.txt", oid: blob.oid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const basePayload = new TextEncoder().encode(
      `tree ${tree.oid}\nauthor ${author}\ncommitter ${author}\n\nbase\n`
    );
    const base = await encodeGitObject("commit", basePayload);
    const currentPayload = new TextEncoder().encode(
      `tree ${tree.oid}\nparent ${base.oid}\nauthor ${author}\ncommitter ${author}\n\ncurrent\n`
    );
    const current = await encodeGitObject("commit", currentPayload);
    const authoritativePack = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: basePayload },
      { type: "commit", payload: currentPayload },
    ]);
    await seedPackedRepoState({
      env,
      repoId: seeded.doName,
      getStub: () => stub,
      packs: [{ name: "pack-ref-only-authority.pack", packBytes: authoritativePack }],
      refs: [{ name: "refs/heads/main", oid: current.oid }],
      head: { target: "refs/heads/main", oid: current.oid },
    });
    const before = await readRepoCatalogState(() => stub);

    // The input deliberately duplicates objects already reachable from the
    // advertised current tip. The native result therefore has no new output.
    const duplicatePack = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: basePayload },
    ]);
    const commandPrefix = concatChunks([
      pktLine(`${current.oid} ${base.oid} refs/heads/main\0 report-status agent=git/2.50.1\n`),
      flushPkt(),
    ]);
    const requestBody = concatChunks([commandPrefix, duplicatePack]);
    const receivePackResponse = concatChunks([
      pktLine("unpack ok\n"),
      pktLine("ok refs/heads/main\n"),
      flushPkt(),
    ]);
    let executionCount = 0;
    let plannedWholePackBytes = -1;
    stockDataPlaneTest.setWorkerExecutor(
      async ({ operation, cacheCtx, limiter, countSubrequest, logger }) => {
        executionCount++;
        const stock = operation.stockReceive!;
        const plan = await planStockReceive({
          env,
          repoId: operation.repositoryId,
          operationId: operation.id,
          inputRequestKey: operation.inputPackKey,
          inputRequestBytes: operation.inputBytes,
          inputRequestSha256: stock.inputRequestSha256,
          packOffset: stock.packOffset,
          packBytes: stock.packBytes,
          advertisedRefs: stock.advertisedRefs,
          commands: operation.commands,
          activePacks: operation.activeCatalog,
          cacheCtx,
          limiter,
          countSubrequest: (count) => countSubrequest("test-stock-ref-only-plan", count),
          log: logger,
        });
        plannedWholePackBytes = plan.activePackWholeBytes;
        expect(plan.incomingObjectCount).toBe(3);
        expect(plan.visitedIncomingObjectCount).toBe(0);
        expect(plan.semanticExternalOids).toEqual([base.oid]);
        expect(plan.requiredRootOids).toEqual([base.oid, current.oid].sort());
        const result = {
          operationId: operation.id,
          resultKind: "ref-only",
          packBytes: 0,
          idxBytes: 0,
          refsBytes: 0,
          objectCount: 0,
          inputPackObjectCount: plan.incomingObjectCount,
          packSha1: "",
          elapsedMs: 1,
          scratchBytes: requestBody.byteLength + plan.prerequisitePackBytes,
          hydratedBytes: plan.prerequisiteHydratedBytes,
          downloadedBytes:
            plan.inputBytesRead +
            plan.rangeBytes +
            plan.activePackWholeBytes +
            plan.metadataBytes +
            operation.inputBytes +
            plan.prerequisitePackBytes +
            plan.closureManifestBytes,
          cacheHitBytes: 0,
          receivePackResponse: btoa(String.fromCharCode(...receivePackResponse)),
          inputRequestSha256: await sha256(requestBody),
          stockTrace: stockTrace.map((event, index) => ({ sequence: index + 1, event })),
          metadataBytes: plan.metadataBytes + plan.closureManifestBytes,
          metadataRequests: plan.metadataRequests + 1,
          inputBytesRead: plan.inputBytesRead + operation.inputBytes + plan.prerequisitePackBytes,
          inputRequests: plan.inputRequests + 2,
          rangeBytes: plan.rangeBytes,
          rangeRequests: plan.rangeRequests,
          packsTouched: plan.packsTouched,
          quarantinePathInsideOwnedWorkRoot: true,
          quarantineRemovedAfterReceive: true,
          quarantinePathNonEmpty: true,
          freshWorkDirectory: true,
          repositoryPackBytesBeforeHydration: 0,
          sharedObjectCacheDisabled: true,
          skipConnectivityCheck: false,
          planSha256: plan.planSha256,
          closureProof: {
            planSha256: plan.planSha256,
            incomingOids: [],
            semanticExternalOids: plan.semanticExternalOids,
            visitedIncomingObjectCount: 0,
            logicalEdgeCount: 0,
            internalEdgeCount: 0,
            externalEdgeCount: 0,
            missingObjectCount: 0,
            objectTypeCounts: { commit: 0, tree: 0, blob: 0, tag: 0 },
          },
          semanticExternalOids: plan.semanticExternalOids,
          thinDeltaBaseOids: plan.thinDeltaBaseOids,
          requiredRootOids: plan.requiredRootOids,
          prerequisiteObjectOids: plan.requiredRootOids,
          physicalNodes: plan.physicalNodes,
          physicalDependencies: plan.dependencies,
          topologicalEntryIds: plan.topologicalEntryIds,
          selectedPackChecksums: plan.selectedPackChecksums,
          activePackBindings: plan.activePackBindings,
          ranges: plan.ranges,
          activePackReads: plan.activePackReads,
          activePackTrailerBytes: plan.activePackTrailerBytes,
          activePackTrailerRequests: plan.activePackTrailerRequests,
          activePackRangeBytes: plan.activePackRangeBytes,
          activePackRangeRequests: plan.activePackRangeRequests,
          activePackWholeBytes: plan.activePackWholeBytes,
          activePackWholeRequests: plan.activePackWholeRequests,
          activePackUnattributedBytes: 0,
          activePackUnattributedRequests: 0,
          closureManifestKey: plan.closureManifestKey,
          closureManifestBytes: plan.closureManifestBytes,
          closureManifestSha256: plan.closureManifestSha256,
          closureManifestEtag: plan.closureManifestEtag,
          prerequisitePackKey: plan.prerequisitePackKey,
          prerequisitePackBytes: plan.prerequisitePackBytes,
          prerequisitePackSha256: plan.prerequisitePackSha256,
          prerequisitePackEtag: plan.prerequisitePackEtag,
          incomingObjectCount: plan.incomingObjectCount,
          visitedIncomingObjectCount: 0,
          logicalEdgeCount: 0,
          internalEdgeCount: 0,
          externalEdgeCount: 0,
          missingObjectCount: 0,
          objectTypeCounts: { commit: 0, tree: 0, blob: 0, tag: 0 },
          selectedPackBytes: plan.selectedPackBytes,
          activePackCount: plan.activePackCount,
          outputValidationBytes: 0,
          outputValidationRequests: 0,
          outputBytesWritten: 0,
          outputRequests: 0,
        } satisfies NativeReceiveProcessResult;
        expect(await validateStockReceivePreparedProof(operation, result)).toBe(true);
        const wholeRead = plan.activePackReads.find((read) => read.kind === "whole");
        expect(wholeRead).toBeDefined();
        expect(
          await validateStockReceivePreparedProof(operation, {
            ...result,
            activePackReads: plan.activePackReads.filter((read) => read !== wholeRead),
            activePackWholeBytes: 0,
            activePackWholeRequests: 0,
          })
        ).toBe(false);
        expect(
          await validateStockReceivePreparedProof(operation, {
            ...result,
            activePackReads: plan.activePackReads.map((read) =>
              read === wholeRead
                ? { ...read, end: read.end - 1, returnedBytes: read.returnedBytes - 1 }
                : read
            ),
            activePackWholeBytes: plan.activePackWholeBytes - 1,
          })
        ).toBe(false);
        expect(
          await validateStockReceivePreparedProof(operation, {
            ...result,
            visitedIncomingObjectCount: 1,
            closureProof: {
              ...result.closureProof,
              incomingOids: [base.oid],
              visitedIncomingObjectCount: 1,
              objectTypeCounts: { commit: 1, tree: 0, blob: 0, tag: 0 },
            },
            objectTypeCounts: { commit: 1, tree: 0, blob: 0, tag: 0 },
          })
        ).toBe(false);
        expect(
          await validateStockReceivePreparedProof(operation, {
            ...result,
            thinDeltaBaseOids: [base.oid],
          })
        ).toBe(false);
        expect(
          await validateStockReceivePreparedProof(operation, {
            ...result,
            missingObjectCount: 1,
            closureProof: { ...result.closureProof, missingObjectCount: 1 },
          })
        ).toBe(false);
        expect(
          await validateStockReceivePreparedProof(operation, {
            ...result,
            quarantineRemovedAfterReceive: false,
          })
        ).toBe(false);
        return result;
      }
    );

    const push = async (operationId: string) =>
      await handleStreamingReceivePackPOST(
        { ...env, NATIVE_RECEIVE_CONTAINER: "1" },
        seeded.doName,
        new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-git-receive-pack-request",
            "Content-Length": String(requestBody.byteLength),
            "X-Display-Operation-ID": operationId,
          },
          body: toRequestBody(requestBody),
        }),
        createExecutionContext(),
        {
          limiter: new SubrequestLimiter(900),
          acceptedWriteContext: {
            repositoryId: seeded.doName,
            actor: "stock-ref-only-test",
            sourceSurface: "git-push",
            idempotencyKey: null,
          },
        }
      );

    const response = await push("stock-ref-only-operation");
    expect(response.status).toBe(200);
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(receivePackResponse);
    expect((await stub.listRefs()).find((ref) => ref.name === "refs/heads/main")?.oid).toBe(
      base.oid
    );
    const committed = await stub.getNativeReceiveOperation("stock-ref-only-operation");
    expect(committed).toMatchObject({
      state: "committed",
      result: {
        changed: true,
        packKey: undefined,
        packBytes: undefined,
        authorityPublication: {
          refs: [{ name: "refs/heads/main", oid: base.oid }],
          receipt: { disposition: "committed", newOid: base.oid },
        },
      },
      metrics: {
        outputValidationBytes: 0,
        outputValidationRequests: 0,
        outputBytesWritten: 0,
        outputRequests: 0,
      },
    });
    expect(plannedWholePackBytes).toBeGreaterThan(0);
    const after = await readRepoCatalogState(() => stub);
    expect(after).toEqual(before);
    expect(
      (await env.REPO_BUCKET.list()).objects.filter(
        (object) =>
          object.key.includes("stock-ref-only-operation") && /\.(?:pack|idx|refs)$/.test(object.key)
      )
    ).toEqual([]);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        expect((await state.storage.list({ prefix: "acceptedWrite:" })).size).toBe(1);
      }
    );

    // A lost response retries by operation identity and must not repeat native
    // processing or create a second accepted-write fact.
    const replay = await push("stock-ref-only-operation");
    expect(replay.status).toBe(200);
    expect(new Uint8Array(await replay.arrayBuffer())).toEqual(receivePackResponse);
    expect(executionCount).toBe(1);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        expect((await state.storage.list({ prefix: "acceptedWrite:" })).size).toBe(1);
      }
    );

    const stale = await push("stock-ref-only-stale-old");
    expect(stale.status).toBe(200);
    expect(new TextDecoder().decode(await stale.arrayBuffer())).toContain(
      "ng refs/heads/main stale info"
    );
    expect(await stub.getNativeReceiveOperation("stock-ref-only-stale-old")).toMatchObject({
      state: "aborted",
      errorCode: "exact-old-ref-conflict",
    });
    expect((await stub.listRefs()).find((ref) => ref.name === "refs/heads/main")?.oid).toBe(
      base.oid
    );
    expect(await readRepoCatalogState(() => stub)).toEqual(before);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        expect((await state.storage.list({ prefix: "acceptedWrite:" })).size).toBe(1);
      }
    );
  });

  it("fences a late expired execution claim from a newer attempt's output keys", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stock-claim-fence");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    const active = await runDOWithRetry(
      () => stub,
      async (instance) => await instance.seedMinimalRepo()
    );
    const firstBegin = await stub.beginReceive();
    if (!firstBegin.ok) throw new Error("expected first receive lease");
    const operationId = "stock-claim-fence-operation";
    const fingerprint = "1".repeat(64);
    const prefix = doPrefix(stub.id.toString());
    const baseOutputPackKey = nativeReceiveOutputPackKey(prefix, operationId, fingerprint);
    const newOid = "f".repeat(40);
    const baseOperation: NativeReceiveOperation = {
      id: operationId,
      fingerprint,
      leaseToken: firstBegin.lease.token,
      repositoryId: seeded.doName,
      state: "staged",
      inputPackKey: nativeReceiveInputRequestKey(prefix, firstBegin.lease.token),
      inputBytes: 64,
      inputEtag: "input-a",
      stockReceive: {
        inputRequestSha256: "a".repeat(64),
        packOffset: 16,
        packBytes: 48,
        advertisedRefs: [{ name: "refs/heads/main", oid: active.commitOid }],
      },
      outputPackKey: baseOutputPackKey,
      outputIdxKey: packIndexKey(baseOutputPackKey),
      outputRefsKey: packRefsKey(baseOutputPackKey),
      commands: [{ oldOid: active.commitOid, newOid, ref: "refs/heads/main" }],
      acceptedWrites: [
        {
          repositoryId: seeded.doName,
          ref: "refs/heads/main",
          beforeSha: active.commitOid,
          afterSha: newOid,
          actor: "stock-spike-test",
          sourceSurface: "git-push",
          idempotencyKey: null,
        },
      ],
      activeCatalog: firstBegin.activeCatalog,
      catalogGeneration: firstBegin.packsetVersion,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      attempts: 0,
      cleanupPending: false,
      events: [{ sequence: 1, phase: "worker-route-receive-start" }],
    };

    const first = await stub.admitStockReceive(baseOperation);
    if (first.status !== "admitted") throw new Error(`unexpected first ${first.status}`);
    expect(first.operation.outputPackKey).not.toBe(baseOutputPackKey);
    expect(
      await stub.rejectStockReceiveExecution(first.executionToken, {
        code: "r2-transient",
        metrics: emptyPlannerFailureMetrics(first.operation.activeCatalog.length),
      })
    ).toMatchObject({
      status: "failed",
      operation: {
        errorCode: "r2-transient",
        events: expect.arrayContaining([expect.objectContaining({ phase: "r2-read-retryable" })]),
      },
    });
    expect(await stub.completeStockReceiveCleanup(operationId, fingerprint)).toMatchObject({
      status: "complete",
    });

    const secondBegin = await stub.beginReceive();
    if (!secondBegin.ok) throw new Error("expected retry receive lease");
    const secondOperation: NativeReceiveOperation = {
      ...baseOperation,
      leaseToken: secondBegin.lease.token,
      inputPackKey: nativeReceiveInputRequestKey(prefix, secondBegin.lease.token),
      inputEtag: "input-b",
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    const second = await stub.admitStockReceive(secondOperation);
    if (second.status !== "admitted") throw new Error(`unexpected second ${second.status}`);
    expect(second.operation.outputPackKey).not.toBe(first.operation.outputPackKey);

    await env.REPO_BUCKET.put(first.operation.outputPackKey, new Uint8Array([1]));
    await env.REPO_BUCKET.put(second.operation.outputPackKey, new Uint8Array([2]));
    await cleanupStockReceiveWorkerDataPlane({
      env,
      operation: first.operation,
      limiter: new SubrequestLimiter(20),
      countSubrequest() {},
      logger: createLogger(env.LOG_LEVEL, { service: "StockClaimFenceTest" }),
      includeOutputs: true,
    });
    expect(await env.REPO_BUCKET.head(first.operation.outputPackKey)).toBeNull();
    expect(await env.REPO_BUCKET.head(second.operation.outputPackKey)).not.toBeNull();

    expect(
      await stub.rejectStockReceiveExecution(first.executionToken, "late-expired-worker")
    ).toEqual({ status: "rejected", code: "operation-not-found" });
    expect(await stub.getNativeReceiveOperation(operationId)).toMatchObject({
      state: "processing",
      attempts: 2,
    });
    expect(
      await stub.rejectStockReceiveExecution(second.executionToken, {
        code: "native-data-plane-failed",
        diagnosticCode: "stock-data-plane:container-readiness-failed",
      })
    ).toMatchObject({
      status: "failed",
      operation: {
        events: expect.arrayContaining([
          expect.objectContaining({
            phase: "worker-data-plane-rejected",
            detailCode: "stock-data-plane:container-readiness-failed",
          }),
          expect.objectContaining({
            phase: "worker-data-plane-rejected-attempt-2",
            detailCode: "stock-data-plane:container-readiness-failed",
          }),
        ]),
      },
    });
    await cleanupStockReceiveWorkerDataPlane({
      env,
      operation: second.operation,
      limiter: new SubrequestLimiter(20),
      countSubrequest() {},
      logger: createLogger(env.LOG_LEVEL, { service: "StockClaimFenceTest" }),
      includeOutputs: true,
    });
    expect(await stub.completeStockReceiveCleanup(operationId, fingerprint)).toMatchObject({
      status: "complete",
    });
  });
});
