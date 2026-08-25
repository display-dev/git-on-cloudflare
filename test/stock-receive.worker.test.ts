import { afterEach, describe, expect, it } from "vitest";
import { createExecutionContext } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";

import { asBufferSource, bytesToHex, createLogger, getRepoStub } from "@/worker/common";
import { __test as receiveCatalogTest } from "@/worker/do/repo/catalog/receive";
import { nativeReceiveOperationKey } from "@/worker/do/repo/repoState";
import { concatChunks, flushPkt, pktLine } from "@/worker/git/core";
import { encodeGitObject } from "@/worker/git/core/objects";
import { SubrequestLimiter } from "@/worker/git/operations/limits";
import { nativeReceiveOperationEvidenceMatches } from "@/worker/git/nativeReceive/types";
import {
  __test as stockDataPlaneTest,
  cleanupStockReceiveWorkerDataPlane,
} from "@/worker/git/nativeReceive/stockDataPlane";
import type {
  NativeReceiveOperation,
  NativeReceiveOperationMetrics,
  NativeReceiveProcessResult,
} from "@/worker/git/nativeReceive/types";
import {
  __test as stockPlannerTest,
  planStockReceive,
  StockReceivePlannerError,
} from "@/worker/git/nativeReceive/stockPlanner";
import { validateStockReceivePreparedProof } from "@/worker/git/nativeReceive/stockProof";
import { handleStreamingReceivePackPOST } from "@/worker/git/receive/streamReceivePack";
import {
  doPrefix,
  nativeReceiveInputRequestKey,
  nativeReceiveOutputPackKey,
  packIndexKey,
  packRefsKey,
} from "@/worker/keys";
import { buildPack } from "./util/git-pack";
import { setupRepoForTests } from "./util/repoSeed";
import { runDOWithRetry, toRequestBody, uniqueRepoId } from "./util/test-helpers";

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

afterEach(() => {
  stockDataPlaneTest.reset();
  receiveCatalogTest.reset();
  stockPlannerTest.reset();
});

describe("stock Smart HTTP receive spike", () => {
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
        rangeRequests: 1,
        activePackRangeRequests: 1,
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

    stockPlannerTest.readWholeActivePack(planArgs.operationId);
    await expect(planStockReceive(planArgs)).rejects.toThrow(
      "stock-plan:active-pack-read-unattributed"
    );
    cacheCtx.memo = {};

    const plan = await planStockReceive(planArgs);

    expect(plan.semanticExternalOids).toEqual([active.commitOid, active.treeOid].sort());
    expect(plan.thinDeltaBaseOids).toEqual([]);
    expect(plan.requiredRootOids).toEqual(plan.semanticExternalOids);
    expect(plan.ranges).toHaveLength(2);
    expect(plan.rangeBytes).toBe(
      plan.ranges.reduce((total, range) => total + range.end - range.start, 0)
    );
    expect(plan.activePackRangeBytes).toBe(plan.rangeBytes);
    expect(plan.activePackRangeRequests).toBe(plan.ranges.length);
    expect(plan.activePackTrailerBytes).toBe(20);
    expect(plan.activePackTrailerRequests).toBe(1);
    expect(plan.activePackWholeBytes).toBe(0);
    expect(plan.activePackWholeRequests).toBe(0);
    expect(plan.activePackUnattributedBytes).toBe(0);
    expect(plan.activePackUnattributedRequests).toBe(0);
    expect(plan.activePackReads.filter((read) => read.kind === "required-object")).toEqual(
      plan.ranges.map((range) => ({
        packChecksum: range.packChecksum,
        start: range.start,
        end: range.end,
        returnedBytes: range.end - range.start,
        kind: "required-object",
        requiredOid: range.requiredOid,
      }))
    );
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
    await env.REPO_BUCKET.delete([
      inputRequestKey,
      plan.prerequisitePackKey,
      plan.closureManifestKey,
    ]);
  });

  it("buffers byte-identical receive-pack success until RepoDO commit and ACK authorization", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stock-receive");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    const active = await runDOWithRetry(
      () => stub,
      async (instance) => await instance.seedMinimalRepo()
    );
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

    const response = await handleStreamingReceivePackPOST(
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

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("application/x-git-receive-pack-result");
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(receivePackResponse);
    expect(stagedRequest).toEqual(requestBody);
    expect((await stub.listRefs()).find((ref) => ref.name === "refs/heads/main")?.oid).toBe(
      commit.oid
    );
    const committedOperation = await stub.getNativeReceiveOperation("stock-tiny-operation");
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
        activePackReads: expectedPlan!.activePackReads,
        activePackTrailerBytes: expectedPlan!.activePackTrailerBytes,
        activePackTrailerRequests: expectedPlan!.activePackTrailerRequests,
        activePackRangeBytes: expectedPlan!.activePackRangeBytes,
        activePackRangeRequests: expectedPlan!.activePackRangeRequests,
        activePackWholeBytes: 0,
        activePackWholeRequests: 0,
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

    await env.REPO_BUCKET.delete([
      ...authority!.refs.map((ref) => ref.key),
      authority!.receipt.key,
    ]);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const key = nativeReceiveOperationKey("stock-tiny-operation");
        const operation = await state.storage.get<NativeReceiveOperation>(key);
        if (!operation?.publicationPlan || !operation.result) {
          throw new Error("expected committed stock publication state");
        }
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
      await stub.rejectStockReceiveExecution(second.executionToken, "native-data-plane-failed")
    ).toMatchObject({ status: "failed" });
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
