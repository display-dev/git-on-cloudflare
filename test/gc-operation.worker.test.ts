import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import { getRepoStub } from "@/worker/common";
import { GC_UNPUBLISHED_LIFETIME_MS, type GcOperation } from "@/worker/git/maintenance/gcOperation";
import { GC_OPERATION_KEY } from "@/worker/do/repo/catalog/gcOperation";
import { uniqueRepoId, withEnvOverrides, runAlarmWithRetry } from "./util/test-helpers";
import { gcOperationStatus } from "@/worker/git/maintenance/gcStatus";

describe("durable garbage collection ownership", () => {
  async function emptyPublication(name: string, qualify = false) {
    const repositoryId = uniqueRepoId(name);
    const stub = getRepoStub(env, repositoryId);
    if (qualify)
      await stub.registerQualificationGc(repositoryId, name, {
        faults: ["before-publication", "after-publication"],
        holdReader: false,
        deadlineAt: Date.now() + 3_600_000,
      });
    else await stub.registerGcOperation(repositoryId, name);
    const claimed = await stub.claimGcOperation(name);
    if (claimed.status !== "ready" || !claimed.operation.claim) throw new Error("claim failed");
    const begin = await stub.beginReachabilityGc();
    if (!begin.ok) throw new Error("snapshot failed");
    const claimId = claimed.operation.claim.id;
    await stub.recordGcProgress(name, claimId, {
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
    await stub.recordGcProgress(name, claimId, {
      kind: "plan",
      closure: { objectCount: 0, objectSetSha256: "0".repeat(64) },
    });
    return stub;
  }

  it("rearms its durable alarm when queue enqueue fails", async () => {
    const repositoryId = uniqueRepoId("durable-gc-wake");
    const stub = getRepoStub(env, repositoryId);
    await stub.registerGcOperation(repositoryId, "wake");
    const send = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockRejectedValue(new Error("queue unavailable"));
    try {
      expect(await runAlarmWithRetry(() => stub)).toBe(true);
      expect(await stub.getGcOperation()).toMatchObject({ id: "wake", phase: "queued" });
      expect(
        await runInDurableObject(stub, async (_, state) => await state.storage.getAlarm())
      ).toBeTypeOf("number");
    } finally {
      send.mockRestore();
    }
  });

  it("reconciles publication faults under the same operation without changing refs or no-op catalog version", async () => {
    await withEnvOverrides(
      env,
      { QUALIFICATION_MODE: "1", QUALIFICATION_SECRET: "gc-control-test" },
      async () => {
        const stub = await emptyPublication("publication-faults", true);
        const before = await stub.getQualificationInventory();
        const claimed = await stub.claimGcOperation("publication-faults");
        if (claimed.status !== "ready" || !claimed.operation.claim) throw new Error("claim failed");
        await expect(
          (async () =>
            await stub.commitGcOperation("publication-faults", claimed.operation!.claim!.id))()
        ).rejects.toThrow("before GC publication");
        expect((await stub.getGcOperation())?.commit).toBeUndefined();
        // Simulate delivery after the ordinary claim and writer-drain period.
        await runInDurableObject(stub, async (_, state) => {
          const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
          operation!.claim!.expiresAt = Date.now() - 1;
          operation!.claim!.safeRetryAt = Date.now() - 1;
          await state.storage.put(GC_OPERATION_KEY, operation);
        });
        const retry = await stub.claimGcOperation("publication-faults");
        if (retry.status !== "ready" || !retry.operation.claim) throw new Error("retry failed");
        await expect(
          (async () =>
            await stub.commitGcOperation("publication-faults", retry.operation!.claim!.id))()
        ).rejects.toThrow("after GC publication");
        const operation = await stub.getGcOperation();
        expect(operation).toMatchObject({
          phase: "reclaim",
          commit: { status: "committed", packCatalogVersion: 0, supersededPackKeys: [] },
        });
        expect(
          await stub.commitGcOperation("publication-faults", claimed.operation.claim.id)
        ).toEqual(operation!.commit);
        expect(await stub.listRefs()).toEqual([]);
        expect((await stub.getQualificationInventory()).refStateDigest).toBe(before.refStateDigest);
        const view = JSON.stringify(gcOperationStatus(operation!));
        expect(view).not.toContain(operation!.snapshot!.token);
        expect(view).not.toContain(operation!.outputPackKey);
      }
    );
  });

  it("does not publish a changed source and keeps discard behind the writer drain", async () => {
    const stub = await emptyPublication("source-race");
    await runInDurableObject(stub, async (_, state) => {
      const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
      await state.storage.put("refsVersion", operation!.snapshot!.refsVersion + 1);
    });
    const claimed = await stub.claimGcOperation("source-race");
    if (claimed.status !== "ready" || !claimed.operation.claim) throw new Error("claim failed");
    const claim = claimed.operation.claim.id;
    expect(await stub.commitGcOperation("source-race", claim)).toEqual({
      status: "retry",
      reason: "refs-changed",
    });
    expect(
      await stub.recordGcProgress("source-race", claim, {
        kind: "discard",
        reason: "source-changed",
      })
    ).toMatchObject({ status: "ready", operation: { phase: "discard" } });
    expect(await stub.gcDiscardKeys("source-race", claim)).toBeNull();
    expect(await stub.claimGcOperation("source-race")).toMatchObject({ status: "busy" });
    await runInDurableObject(stub, async (_, state) => {
      const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
      operation!.discardAfter = Date.now() - 1;
      await state.storage.put(GC_OPERATION_KEY, operation);
    });
    const discard = await stub.claimGcOperation("source-race");
    if (discard.status !== "ready" || !discard.operation.claim)
      throw new Error("discard claim failed");
    expect(await stub.gcDiscardKeys("source-race", discard.operation.claim.id)).toHaveLength(5);
    expect(await stub.commitGcOperation("source-race", claim)).toMatchObject({ status: "retry" });
  });

  it("atomically registers output ownership and a wakeup, and replays admission", async () => {
    const repositoryId = uniqueRepoId("durable-gc-admission");
    const stub = getRepoStub(env, repositoryId);
    const first = await stub.registerGcOperation(repositoryId, "operation-one");
    expect(first.status).toBe("ready");
    if (first.status !== "ready") throw new Error("registration failed");
    expect(first.operation.phase).toBe("queued");
    expect(first.operation.inputPackKey).not.toBe(first.operation.outputPackKey);
    expect(
      await runInDurableObject(stub, async (_, state) => await state.storage.getAlarm())
    ).toBeTypeOf("number");
    expect(await stub.registerGcOperation(repositoryId, "operation-one")).toEqual(first);
    expect(await stub.registerGcOperation(repositoryId, "operation-two")).toMatchObject({
      status: "busy",
    });
    expect(await env.REPO_BUCKET.head(first.operation.inputPackKey)).toBeNull();
  });

  it("preserves the completed rewrite across expired claims and rejects late progress", async () => {
    const repositoryId = uniqueRepoId("durable-gc-handoff");
    const stub = getRepoStub(env, repositoryId);
    await stub.registerGcOperation(repositoryId, "handoff");
    const claimed = await stub.claimGcOperation("handoff");
    if (claimed.status !== "ready" || !claimed.operation.claim) throw new Error("claim failed");
    const oldClaim = claimed.operation.claim.id;
    const begin = await stub.beginReachabilityGc();
    if (!begin.ok) throw new Error("snapshot failed");
    expect(
      (
        await stub.recordGcProgress("handoff", oldClaim, {
          kind: "snapshot",
          snapshot: {
            token: begin.lease.token,
            refs: begin.refs,
            snapshotPinVersion: begin.snapshotPinVersion,
            refsVersion: begin.refsVersion,
            packsetVersion: begin.packsetVersion,
            sourcePacks: begin.activeCatalog,
          },
        })
      ).status
    ).toBe("ready");
    await stub.recordGcProgress("handoff", oldClaim, {
      kind: "plan",
      closure: { objectCount: 1, objectSetSha256: "1".repeat(64) },
    });
    const identity = { packBytes: 100, packSha1: "2".repeat(40) };
    await stub.recordGcProgress("handoff", oldClaim, { kind: "rewrite-intent", identity });
    const completed = await stub.recordGcProgress("handoff", oldClaim, {
      kind: "rewrite-complete",
      identity,
      etag: "verified-etag",
    });
    expect(completed).toMatchObject({
      status: "ready",
      operation: { phase: "index", rewrite: identity },
    });
    const indexing = await stub.claimGcOperation("handoff");
    if (indexing.status !== "ready" || !indexing.operation.claim)
      throw new Error("index claim failed");
    expect(indexing.operation.claim.id).not.toBe(oldClaim);
    expect(await stub.claimGcOperation("handoff")).toMatchObject({ status: "busy" });
    await runInDurableObject(stub, async (_, state) => {
      const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
      if (!operation?.claim) throw new Error("missing claim");
      operation.claim.expiresAt = Date.now() - 1;
      // Expiry alone does not waive the established writer drain fence.
      await state.storage.put(GC_OPERATION_KEY, operation);
    });
    expect(await stub.claimGcOperation("handoff")).toMatchObject({ status: "busy" });
    await runInDurableObject(stub, async (_, state) => {
      const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
      if (!operation?.claim) throw new Error("missing claim");
      operation.claim.safeRetryAt = Date.now() - 1;
      await state.storage.put(GC_OPERATION_KEY, operation);
    });
    expect(await stub.claimGcOperation("handoff")).toMatchObject({
      status: "ready",
      operation: { phase: "index", rewrite: identity },
    });
    expect(await stub.recordGcProgress("handoff", oldClaim, { kind: "yield" })).toEqual({
      status: "rejected",
      reason: "claim-mismatch",
    });
  });

  it("refuses admission and claims after the repository deletion fence", async () => {
    const repositoryId = uniqueRepoId("durable-gc-deletion");
    const stub = getRepoStub(env, repositoryId);
    await stub.registerGcOperation(repositoryId, "deletion");
    await runInDurableObject(stub, async (_, state) => {
      await state.storage.put("repositoryDeleting", true);
    });
    expect(await stub.claimGcOperation("deletion")).toEqual({
      status: "rejected",
      reason: "repository-deleting",
    });
    expect(await stub.registerGcOperation(repositoryId, "another")).toEqual({
      status: "rejected",
      reason: "repository-deleting",
    });
  });

  it("stops new qualified work at its deadline only after writer drain, without blocking publication reconciliation", async () => {
    await withEnvOverrides(
      env,
      { QUALIFICATION_MODE: "1", QUALIFICATION_SECRET: "gc-control-test" },
      async () => {
        const repositoryId = uniqueRepoId("gc-budget");
        const stub = getRepoStub(env, repositoryId);
        await stub.registerQualificationGc(repositoryId, "budget", {
          faults: [],
          holdReader: false,
          deadlineAt: Date.now() + 60_000,
        });
        const first = await stub.claimGcOperation("budget");
        if (first.status !== "ready") throw new Error("claim failed");
        await runInDurableObject(stub, async (_, state) => {
          const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
          operation!.qualification!.deadlineAt = Date.now() - 1;
          operation!.claim!.expiresAt = Date.now() - 1;
          await state.storage.put(GC_OPERATION_KEY, operation);
        });
        expect(await stub.claimGcOperation("budget")).toMatchObject({ status: "busy" });
        await runInDurableObject(stub, async (_, state) => {
          const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
          operation!.claim!.safeRetryAt = Date.now() - 1;
          await state.storage.put(GC_OPERATION_KEY, operation);
        });
        expect(await stub.claimGcOperation("budget")).toMatchObject({
          status: "ready",
          operation: { phase: "discard", blockedReason: "qualification-deadline" },
        });
        const publishing = await emptyPublication("budget-publication", true);
        await runInDurableObject(publishing, async (_, state) => {
          const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
          operation!.qualification!.deadlineAt = Date.now() - 1;
          await state.storage.put(GC_OPERATION_KEY, operation);
        });
        expect(await publishing.claimGcOperation("budget-publication")).toMatchObject({
          status: "ready",
          operation: { phase: "publish" },
        });
      }
    );
  });

  it("reconciles ordinary publication after its unpublished-work deadline", async () => {
    const stub = await emptyPublication("ordinary-deadline-publication");
    await runInDurableObject(stub, async (_, state) => {
      const operation = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
      operation!.createdAt = Date.now() - GC_UNPUBLISHED_LIFETIME_MS - 1;
      await state.storage.put(GC_OPERATION_KEY, operation);
    });
    const claim = await stub.claimGcOperation("ordinary-deadline-publication");
    if (claim.status !== "ready" || !claim.operation.claim) throw new Error("claim failed");
    expect(claim.operation.phase).toBe("publish");
    expect(
      await stub.commitGcOperation("ordinary-deadline-publication", claim.operation.claim.id)
    ).toMatchObject({ status: "committed" });
    expect(await stub.getGcOperation()).toMatchObject({ phase: "reclaim" });
  });
});
