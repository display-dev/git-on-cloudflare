import { runInDurableObject } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MaintenanceContainerHost } from "@/worker/do/maintenanceContainerHost";
import { getRepoStub } from "@/worker/common";
import { uniqueRepoId } from "./util/test-helpers";
import { withEnvOverrides } from "./util/test-helpers";
import {
  holdNativeInput,
  releaseNativeInput,
  nativeInputHeld,
  qualificationNativeExecutions,
} from "@/worker/do/repo/nativeExecutionQualification";
import {
  beginNativeExecution,
  authorizeNativeExecution,
  finishNativeExecution,
} from "@/worker/do/repo/nativeExecution";
import {
  __test as processor,
  runNativeExecution,
  cancelNativeExecution,
  reconcileNativeExecutionLeases,
  stopNativeReceiveContainerState,
} from "@/worker/do/repo/nativeReceive";
import {
  startNativeProcessorSlot,
  stopNativeProcessorSlot,
  finishNativeProcessorSlot,
  deleteNativeProcessorSlot,
} from "@/worker/do/nativeProcessorSlot";
import type {
  NativeExecutionIdentity,
  NativeExecutionLane,
} from "@/worker/git/nativeReceive/execution";
import type {
  NativeReceiveOperation,
  NativeReceiveProcessRequest,
  NativeReceiveProcessResult,
  RepositoryContainerBridgeProps,
} from "@/worker/git/nativeReceive/types";
import { GC_OPERATION_KEY, type GcOperation } from "@/worker/git/maintenance/gcOperation";
import { nativeReceiveOperationKey } from "@/worker/do/repo/repoState";

afterEach(() => {
  processor.reset();
  vi.restoreAllMocks();
});

function deferred() {
  let release = () => {};
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}
function grant(lane: NativeExecutionLane): RepositoryContainerBridgeProps {
  return {
    operationId: lane,
    readKeys: [{ key: `${lane}/input`, expectedBytes: 1 }],
    writeKeys: [{ key: `${lane}/output`, maxBytes: 1 }],
  };
}
function request(lane: NativeExecutionLane): NativeReceiveProcessRequest {
  return {
    operationId: lane,
    inputPackKey: `${lane}/input`,
    inputBytes: 1,
    activePacks: [],
    commands: [],
    outputPackKey: `${lane}/output`,
    outputIdxKey: `${lane}/index`,
    outputRefsKey: `${lane}/refs`,
    ...(lane === "maintenance"
      ? {
          maintenance: {
            roots: [],
            objectCount: 1,
            objectSetSha256: "a".repeat(64),
            packSha1: "b".repeat(40),
            resultKey: "maintenance/result",
          },
        }
      : {}),
  };
}
function result(operationId: string): NativeReceiveProcessResult {
  return {
    operationId,
    packBytes: 1,
    idxBytes: 1,
    refsBytes: 1,
    objectCount: 1,
    packSha1: "b".repeat(40),
    elapsedMs: 1,
    scratchBytes: 3,
    hydratedBytes: 0,
    downloadedBytes: 1,
    cacheHitBytes: 0,
  };
}
async function claims(ctx: DurableObjectState) {
  const now = Date.now();
  const receive: NativeReceiveOperation = {
    id: "foreground",
    fingerprint: "test",
    repositoryId: ctx.id.toString(),
    leaseToken: "receive",
    state: "processing",
    inputPackKey: "foreground/input",
    inputBytes: 1,
    inputEtag: "test",
    outputPackKey: "foreground/output",
    outputIdxKey: "foreground/index",
    outputRefsKey: "foreground/refs",
    commands: [],
    acceptedWrites: [],
    activeCatalog: [],
    catalogGeneration: 0,
    createdAt: now,
    updatedAt: now,
    attempts: 1,
    cleanupPending: false,
    claimId: "receive-claim",
    claimExpiresAt: now + 180_000,
  };
  const gc: GcOperation = {
    schemaVersion: 1,
    id: "maintenance",
    repositoryId: ctx.id.toString(),
    phase: "index",
    createdAt: now,
    updatedAt: now,
    inputPackKey: "maintenance/input",
    outputPackKey: "maintenance/output",
    outputResultKey: "maintenance/result",
    measurements: {},
    claim: { id: "gc-claim", expiresAt: now + 1_200_000, safeRetryAt: now + 1_500_000 },
  };
  await ctx.storage.put(nativeReceiveOperationKey("foreground"), receive);
  await ctx.storage.put(GC_OPERATION_KEY, gc);
}
async function identity(
  ctx: DurableObjectState,
  lane: NativeExecutionLane
): Promise<NativeExecutionIdentity> {
  const job = await beginNativeExecution(
    ctx,
    lane,
    lane,
    lane === "foreground" ? "receive-claim" : "gc-claim",
    grant(lane)
  );
  if (!job) throw new Error("execution admission failed");
  return job;
}

describe("native execution isolation", () => {
  it("finishes foreground alarm cleanup while maintenance cancellation is still waiting", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-alarm-independent"));
    let maintenance: NativeExecutionIdentity | undefined;
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      maintenance = await identity(ctx, "maintenance");
      await finishNativeExecution(ctx, maintenance, "revoked");
      const receive = await ctx.storage.get<NativeReceiveOperation>(
        nativeReceiveOperationKey("foreground")
      );
      if (!receive) throw new Error("missing receive");
      await ctx.storage.put(nativeReceiveOperationKey("foreground"), {
        ...receive,
        state: "committed",
        cleanupPending: true,
      });
      await ctx.storage.put("nativeReceiveOperationIndex", ["foreground"]);
    });
    if (!maintenance) throw new Error("missing maintenance");
    const entered = deferred();
    const release = deferred();
    vi.spyOn(MaintenanceContainerHost.prototype, "cancel").mockImplementation(async () => {
      entered.release();
      await release.promise;
      return true;
    });
    try {
      await runInDurableObject(stub, async (instance, ctx) => {
        const alarm = instance.alarm();
        try {
          await entered.promise;
          await expect
            .poll(
              async () =>
                (
                  await ctx.storage.get<NativeReceiveOperation>(
                    nativeReceiveOperationKey("foreground")
                  )
                )?.cleanupPending
            )
            .toBe(false);
          expect(await ctx.storage.get("nativeExecution:maintenance")).toMatchObject({
            stopPending: true,
          });
        } finally {
          release.release();
          await alarm;
        }
      });
    } finally {
      release.release();
    }
  });

  it("does not settle output after revocation or replacement of the domain claim", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-settlement"));
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      const foreground = await identity(ctx, "foreground");
      const maintenance = await identity(ctx, "maintenance");
      expect(await authorizeNativeExecution(ctx, foreground)).toBe(true);
      expect(await finishNativeExecution(ctx, foreground, "revoked")).toBe(true);
      expect(await finishNativeExecution(ctx, foreground, "completed")).toBe(false);
      const gc = await ctx.storage.get<GcOperation>(GC_OPERATION_KEY);
      if (!gc?.claim) throw new Error("missing claim");
      await ctx.storage.put(GC_OPERATION_KEY, { ...gc, claim: { ...gc.claim, id: "replacement" } });
      expect(await finishNativeExecution(ctx, maintenance, "completed")).toBe(false);
      expect(await ctx.storage.get("nativeExecution:maintenance")).toMatchObject({
        state: "active",
      });
      await ctx.storage.put(GC_OPERATION_KEY, gc);
      await ctx.storage.put("repositoryDeleting", true);
      expect(await finishNativeExecution(ctx, maintenance, "completed")).toBe(false);
    });
  });

  it("does not return late foreground output after exact-job cancellation", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-late-output"));
    const entered = deferred();
    const release = deferred();
    processor.setNativeProcessor(async ({ request }) => {
      entered.release();
      await release.promise;
      // Model a processor whose response arrives despite cancellation.
      return result(request.operationId);
    });
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      const running = runNativeExecution({
        ctx,
        env,
        lane: "foreground",
        claimId: "receive-claim",
        request: request("foreground"),
        bridgeProps: grant("foreground"),
      }).then(
        () => "returned-output",
        () => "rejected-output"
      );
      await entered.promise;
      const record = await ctx.storage.get<{ identity: NativeExecutionIdentity }>(
        "nativeExecution:foreground"
      );
      if (!record) throw new Error("missing execution");
      await cancelNativeExecution(ctx, env, record.identity);
      release.release();
      expect(await running).toBe("rejected-output");
      expect(await ctx.storage.get("nativeExecution:foreground")).toMatchObject({
        state: "revoked",
      });
      expect(await ctx.storage.get(nativeReceiveOperationKey("foreground"))).toMatchObject({
        state: "processing",
      });
    });
  });

  it("bounds exact-job holds and keeps private execution authority out of observations", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-hold"));
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      const job = await identity(ctx, "maintenance");
      expect(
        await holdNativeInput(ctx, env, "maintenance", job.operationId, Date.now() + 10_000)
      ).toBe(false);
      await withEnvOverrides(
        env,
        { QUALIFICATION_MODE: "1", QUALIFICATION_SECRET: "test-only" },
        async () => {
          expect(
            await holdNativeInput(ctx, env, "maintenance", job.operationId, Date.now() + 121_000)
          ).toBe(false);
          expect(
            await holdNativeInput(ctx, env, "maintenance", job.operationId, Date.now() + 10_000)
          ).toBe(true);
          expect(await nativeInputHeld(ctx, env, job)).toBe(true);
          expect(
            await holdNativeInput(
              ctx,
              env,
              "maintenance",
              "different-operation",
              Date.now() + 10_000
            )
          ).toBe(false);
          expect(await releaseNativeInput(ctx, env, "maintenance", "different-operation")).toBe(
            false
          );
          const serialized = JSON.stringify(await qualificationNativeExecutions(ctx, env));
          expect(serialized).not.toContain(job.claimId);
          expect(serialized).not.toContain(job.repositoryId);
          expect(serialized).not.toContain("maintenance/input");
          expect(await releaseNativeInput(ctx, env, "maintenance", job.operationId)).toBe(true);
          expect(await nativeInputHeld(ctx, env, job)).toBe(false);
        }
      );
    });
  });
  it("reconciles only the requested lane and retains an unacknowledged stop until retried", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-reconcile"));
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      const job = await identity(ctx, "maintenance");
      await finishNativeExecution(ctx, job, "revoked");
      const before = await ctx.storage.get("nativeExecution:maintenance");
      await reconcileNativeExecutionLeases(ctx, env, "foreground");
      expect(await ctx.storage.get("nativeExecution:maintenance")).toEqual(before);
      expect(
        await beginNativeExecution(
          ctx,
          "maintenance",
          job.operationId,
          job.claimId,
          grant("maintenance")
        )
      ).toBeNull();
      await reconcileNativeExecutionLeases(ctx, env, "maintenance");
      expect(await ctx.storage.get("nativeExecution:maintenance")).toMatchObject({
        state: "revoked",
        stopPending: false,
      });
    });
  });

  it("keeps the successor expiry alarm after a delayed stale dispatch", async () => {
    const repo = getRepoStub(env, uniqueRepoId("native-expiry"));
    let job: NativeExecutionIdentity | undefined;
    await runInDurableObject(repo, async (_instance, ctx) => {
      await claims(ctx);
      job = await identity(ctx, "maintenance");
    });
    if (!job) throw new Error("missing native identity");
    const host = env.MAINTENANCE_CONTAINER_HOST.getByName(job.repositoryId);
    const original = job;
    await runInDurableObject(host, async (instance, ctx) => {
      const successor = {
        ...original,
        generation: original.generation + 1,
        expiresAt: original.expiresAt + 1000,
      };
      expect(await startNativeProcessorSlot(ctx, successor, async () => {})).not.toBeNull();
      expect(await startNativeProcessorSlot(ctx, original, async () => {})).toBeNull();
      expect(await ctx.storage.getAlarm()).toBe(successor.expiresAt);
      await ctx.storage.setAlarm(original.expiresAt);
      await instance.alarm();
      expect(await ctx.storage.getAlarm()).toBe(successor.expiresAt);
      await stopNativeProcessorSlot(ctx, successor);
    });
  });

  it("settles both acknowledged repository stops while preserving their writer drains", async () => {
    const repo = getRepoStub(env, uniqueRepoId("native-delete-both"));
    await runInDurableObject(repo, async (_instance, ctx) => {
      await claims(ctx);
      await identity(ctx, "foreground");
      await identity(ctx, "maintenance");
      await ctx.storage.put("repositoryDeleting", true);
      await stopNativeReceiveContainerState(ctx, env);
      for (const lane of ["foreground", "maintenance"]) {
        const record = await ctx.storage.get<{
          state: string;
          stopPending: boolean;
          drainUntil: number;
        }>(`nativeExecution:${lane}`);
        expect(record).toMatchObject({ state: "revoked", stopPending: false });
        expect(record?.drainUntil).toBeGreaterThan(Date.now());
      }
    });
  });

  it("dispatches receive and maintenance concurrently to different execution hosts", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-concurrent"));
    const bothStarted = deferred();
    const release = deferred();
    const hosts = new Set<string>();
    processor.setNativeProcessor(async ({ ctx, request }) => {
      hosts.add(ctx.id.toString());
      if (hosts.size === 2) bothStarted.release();
      await release.promise;
      return result(request.operationId);
    });
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      const foreground = runNativeExecution({
        ctx,
        env,
        lane: "foreground",
        claimId: "receive-claim",
        request: request("foreground"),
        bridgeProps: grant("foreground"),
      });
      const maintenance = runNativeExecution({
        ctx,
        env,
        lane: "maintenance",
        claimId: "gc-claim",
        request: request("maintenance"),
        bridgeProps: grant("maintenance"),
      });
      await bothStarted.promise;
      expect(hosts.size).toBe(2);
      expect(hosts.has(ctx.id.toString())).toBe(true);
      release.release();
      const results = await Promise.all([foreground, maintenance]);
      expect(results.map((value) => value.operationId)).toEqual(["foreground", "maintenance"]);
    });
  });

  it("cancels maintenance without aborting foreground processing or removing its reader lease", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-cancel"));
    const entered = deferred();
    const releaseForeground = deferred();
    let count = 0;
    let foregroundAborted = false;
    processor.setNativeProcessor(async ({ request, signal }) => {
      if (++count === 2) entered.release();
      if (request.operationId === "foreground") {
        signal.addEventListener("abort", () => {
          foregroundAborted = true;
        });
        await releaseForeground.promise;
        return result(request.operationId);
      }
      await new Promise<void>((_resolve, reject) =>
        signal.addEventListener("abort", () => reject(new Error("cancelled")), { once: true })
      );
      throw new Error("unreachable");
    });
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      await ctx.storage.put("nativeCatalogReaderLease", {
        token: "foreground-reader",
        expiresAt: Date.now() + 120_000,
      });
      const foreground = runNativeExecution({
        ctx,
        env,
        lane: "foreground",
        claimId: "receive-claim",
        request: request("foreground"),
        bridgeProps: grant("foreground"),
      });
      const maintenance = runNativeExecution({
        ctx,
        env,
        lane: "maintenance",
        claimId: "gc-claim",
        request: request("maintenance"),
        bridgeProps: grant("maintenance"),
      }).catch(() => "cancelled");
      await entered.promise;
      const record = await ctx.storage.get<{ identity: NativeExecutionIdentity }>(
        "nativeExecution:maintenance"
      );
      if (!record) throw new Error("missing maintenance identity");
      await cancelNativeExecution(ctx, env, record.identity);
      expect(await maintenance).toBe("cancelled");
      expect(foregroundAborted).toBe(false);
      expect(await ctx.storage.get("nativeCatalogReaderLease")).toBeDefined();
      releaseForeground.release();
      expect((await foreground).operationId).toBe("foreground");
    });
  });

  it("rejects changed and revoked bridge grants while the other lane stays authorized", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-grants"));
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      const foreground = await identity(ctx, "foreground");
      const maintenance = await identity(ctx, "maintenance");
      await env.REPO_BUCKET.put("foreground/input", new Uint8Array([7]));
      const bridge = workerExports.RepositoryContainerBridge({
        props: { ...grant("foreground"), execution: foreground },
      });
      const path = "https://bridge.invalid/r2/" + btoa("foreground/input").replaceAll("=", "");
      const response = await bridge.fetch(path);
      expect(response.status).toBe(200);
      expect(new Uint8Array(await response.arrayBuffer())).toEqual(new Uint8Array([7]));
      const changed = workerExports.RepositoryContainerBridge({
        props: {
          ...grant("foreground"),
          readKeys: [{ key: "maintenance/input", expectedBytes: 1 }],
          execution: foreground,
        },
      });
      expect((await changed.fetch(path)).status).toBe(409);
      await finishNativeExecution(ctx, foreground, "revoked");
      expect((await bridge.fetch(path)).status).toBe(409);
      expect(await authorizeNativeExecution(ctx, maintenance)).toBe(true);
      expect(await authorizeNativeExecution(ctx, foreground)).toBe(false);
    });
  });

  it("rejects duplicate dispatch and cancel-before-start without changing a newer slot", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-slot"));
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      const first = await identity(ctx, "foreground");
      let configured = 0;
      expect(
        await startNativeProcessorSlot(ctx, first, async () => {
          configured++;
        })
      ).not.toBeNull();
      expect(
        await startNativeProcessorSlot(ctx, first, async () => {
          configured++;
        })
      ).toBeNull();
      expect(
        await startNativeProcessorSlot(ctx, { ...first, grantSha256: "f".repeat(64) }, async () => {
          configured++;
        })
      ).toBeNull();
      await finishNativeProcessorSlot(ctx, first);
      const second = { ...first, generation: first.generation + 1 };
      expect(
        await startNativeProcessorSlot(ctx, second, async () => {
          configured++;
        })
      ).not.toBeNull();
      expect(await stopNativeProcessorSlot(ctx, first)).toBe(false);
      expect(configured).toBe(2);
      await stopNativeProcessorSlot(ctx, second);
      const third = { ...first, generation: first.generation + 2 };
      await stopNativeProcessorSlot(ctx, third);
      expect(
        await startNativeProcessorSlot(ctx, third, async () => {
          configured++;
        })
      ).toBeNull();
      expect(configured).toBe(2);
    });
  });

  it("serializes handler installation with cancellation and rejects starts after deletion", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-install"));
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      const job = await identity(ctx, "foreground");
      const entered = deferred();
      const release = deferred();
      const starting = startNativeProcessorSlot(ctx, job, async () => {
        entered.release();
        await release.promise;
      });
      await entered.promise;
      const cancelled = stopNativeProcessorSlot(ctx, job);
      release.release();
      const signal = await starting;
      await cancelled;
      expect(signal?.aborted).toBe(true);
      await deleteNativeProcessorSlot(ctx);
      expect(
        await startNativeProcessorSlot(
          ctx,
          { ...job, generation: job.generation + 1 },
          async () => {}
        )
      ).toBeNull();
    });
  });

  it("rejects stale domain claims and repository deletion at bridge authorization", async () => {
    const stub = getRepoStub(env, uniqueRepoId("native-authority"));
    await runInDurableObject(stub, async (_instance, ctx) => {
      await claims(ctx);
      const job = await identity(ctx, "maintenance");
      expect(await authorizeNativeExecution(ctx, job)).toBe(true);
      const gc = await ctx.storage.get<GcOperation>(GC_OPERATION_KEY);
      if (!gc?.claim) throw new Error("missing claim");
      await ctx.storage.put(GC_OPERATION_KEY, { ...gc, claim: { ...gc.claim, id: "replacement" } });
      expect(await authorizeNativeExecution(ctx, job)).toBe(false);
      await ctx.storage.put(GC_OPERATION_KEY, gc);
      await ctx.storage.put("repositoryDeleting", true);
      expect(await authorizeNativeExecution(ctx, job)).toBe(false);
    });
  });
});
