import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { getRepoStub, zeroOid } from "@/worker/common";
import type { RepoDurableObject } from "@/worker/do/repo/repoDO";
import { encodeGitObject } from "@/worker/git/core";
import { GC_OPERATION_KEY, type GcOperation } from "@/worker/git/maintenance/gcOperation";
import { __test as catalogTest } from "@/worker/do/repo/db/dal/packCatalog";
import { __test as publicationTest } from "@/worker/do/repo/catalog/gcPublication";
import { __admissionTest as admissionTest } from "@/worker/do/repo/catalog/leases";
import { __test as receiveTest, finalizeReceiveState } from "@/worker/do/repo/catalog/receive";
import { getDb, getPackCatalogRow, upsertPackCatalogRow } from "@/worker/do/repo/db";
import { doPrefix, r2PackKey } from "@/worker/keys";
import { buildPack, buildAppendOnlyDelta, uniqueRepoId } from "./util/test-helpers";
import { buildTreePayload, seedPackedRepoState } from "./util/packed-repo";
import { indexTestPack } from "./util/test-indexer";
import { runQueueMessage } from "./util/queue";
import {
  getQualificationRepositoryInventory,
  settleQualificationCompaction,
} from "@/worker/do/repo/qualification";

const encode = (text: string) => new TextEncoder().encode(text);

async function checkpoint(label: string, parent?: string) {
  const payload = encode(label);
  const blob = await encodeGitObject("blob", payload);
  const treePayload = buildTreePayload([{ mode: "100644", name: "state", oid: blob.oid }]);
  const tree = await encodeGitObject("tree", treePayload);
  const commitPayload = encode(
    `tree ${tree.oid}\n${parent ? `parent ${parent}\n` : ""}author Test <test@example.com> 1 +0000\ncommitter Test <test@example.com> 1 +0000\n\n${label}\n`
  );
  const commit = await encodeGitObject("commit", commitPayload);
  return {
    oid: commit.oid,
    payload,
    treePayload,
    commitPayload,
    pack: await buildPack([
      { type: "blob", payload },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: commitPayload },
    ]),
  };
}

async function fixture(rewriteRequired = false) {
  const repoId = uniqueRepoId("gc-concurrent");
  const stub = getRepoStub(env, repoId);
  const base = await checkpoint("base");
  const garbagePayload = encode("deliberately unreachable");
  const garbage = await encodeGitObject("blob", garbagePayload);
  // Exact-pack reuse keeps these coordination tests tiny. Existing durable GC
  // tests exercise real rewriting and native validation; live uses mixed 3 GB.
  const seeded = await seedPackedRepoState({
    env,
    repoId,
    getStub: () => stub,
    packs: [
      {
        name: "base.pack",
        packBytes: rewriteRequired
          ? await buildPack([
              { type: "blob", payload: base.payload },
              { type: "tree", payload: base.treePayload },
              { type: "commit", payload: base.commitPayload },
              { type: "blob", payload: garbagePayload },
            ])
          : base.pack,
      },
      {
        name: "garbage.pack",
        packBytes: await buildPack([{ type: "blob", payload: garbagePayload }]),
      },
    ],
    refs: [{ name: "refs/heads/main", oid: base.oid }],
  });
  await stub.registerGcOperation(repoId, "concurrent");
  await runQueueMessage({
    kind: "reachability-gc",
    repoId,
    doId: stub.id.toString(),
    operationId: "concurrent",
  });
  expect(await stub.getGcOperation()).toMatchObject({
    phase: rewriteRequired ? "index" : "publish",
    coordination: { acceptedReceives: 0 },
  });
  return { stub, repoId, base, garbage, packKeys: seeded.packKeys };
}

async function receive(
  f: { stub: DurableObjectStub<RepoDurableObject> },
  label: string,
  parent: string
) {
  const next = await checkpoint(label, parent);
  const lease = await f.stub.beginReceive();
  if (!lease.ok) throw new Error("foreground receive rejected");
  const key = r2PackKey(doPrefix(f.stub.id.toString()), `${label}.pack`);
  await env.REPO_BUCKET.put(key, next.pack);
  const index = await indexTestPack(env, key, next.pack.byteLength);
  const result = await f.stub.finalizeReceive({
    token: lease.lease.token,
    commands: [{ ref: "refs/heads/main", oldOid: parent, newOid: next.oid }],
    stagedPack: {
      packKey: key,
      packBytes: next.pack.byteLength,
      idxBytes: index.idxBytes,
      objectCount: index.objectCount,
    },
  });
  expect(result.status).toBe("committed");
  return { ...next, key };
}

describe("GC source protection and foreground publication", () => {
  it("keeps a bounded candidate exclusive at a coordinated GC checkpoint", async () => {
    const f = await fixture();

    const begin = await f.stub.beginReceive({ stockPreparation: true });

    expect(begin.ok).toBe(true);
    if (!begin.ok) throw new Error("foreground receive rejected");
    expect(begin.stockPreparationReserved).toBeUndefined();
    expect(await f.stub.abortReceive(begin.lease.token)).toBe(true);
  });

  it("settles only pending setup compaction without releasing writers or readers", async () => {
    const stub = getRepoStub(env, uniqueRepoId("settle-setup"));
    await runInDurableObject(stub, async (_instance, ctx) => {
      const fixtureEnv = { ...env, QUALIFICATION_MODE: "1", QUALIFICATION_SECRET: "test" };
      const lease = {
        token: "active",
        createdAt: Date.now(),
        expiresAt: Date.now() + 1_200_000,
        operation: "compaction",
      };
      await ctx.storage.put({
        refs: [],
        compactionWantedAt: 1,
        compactLease: lease,
        receiveLease: { ...lease, operation: "receive" },
        "read:protected": { expiresAt: Date.now() + 900_000 },
      });
      const digest = (await getQualificationRepositoryInventory(ctx)).refStateDigest;
      const before = await ctx.storage.list();
      expect(await settleQualificationCompaction(ctx, env, digest)).toMatchObject({
        status: "conflict",
      });
      expect(await settleQualificationCompaction(ctx, fixtureEnv, "0".repeat(64))).toMatchObject({
        status: "conflict",
      });
      expect(await ctx.storage.list()).toEqual(before);
      await ctx.storage.put(GC_OPERATION_KEY, { id: "measured", phase: "queued" });
      expect(await settleQualificationCompaction(ctx, fixtureEnv, digest)).toMatchObject({
        status: "conflict",
      });
      expect(await ctx.storage.get("compactionWantedAt")).toBe(1);
      await ctx.storage.delete(GC_OPERATION_KEY);
      await ctx.storage.put("repositoryDeleting", true);
      expect(await settleQualificationCompaction(ctx, fixtureEnv, digest)).toMatchObject({
        status: "conflict",
      });
      await ctx.storage.delete("repositoryDeleting");
      expect(await settleQualificationCompaction(ctx, fixtureEnv, digest)).toEqual({
        schemaVersion: 1,
        status: "request-cleared",
        cleared: true,
        writerActive: true,
      });
      before.delete("compactionWantedAt");
      expect(await ctx.storage.list()).toEqual(before);
      expect(await settleQualificationCompaction(ctx, fixtureEnv, digest)).toMatchObject({
        status: "request-cleared",
        cleared: false,
        writerActive: true,
      });
    });
  });
  it("holds only the designated real reader, including newer packs during publication", async () => {
    const f = await fixture();
    await runInDurableObject(f.stub, async (_instance, state) => {
      const operation = (await state.storage.get<GcOperation>(GC_OPERATION_KEY))!;
      operation.qualification = { faults: {}, reader: {}, deadlineAt: Date.now() + 60_000 };
      await state.storage.put(GC_OPERATION_KEY, operation);
    });
    const lease = await f.stub.beginRepositoryRead();
    if (!lease.ok) throw new Error("reader admission failed");
    const { gcReaderLatch } = await import("@/worker/do/repo/gcQualification");
    await runInDurableObject(f.stub, async (_instance, state) => {
      const qualified = { ...env, QUALIFICATION_MODE: "1", QUALIFICATION_SECRET: "test-only" };
      const keys = [...f.packKeys, "new-checkpoint-pack"];
      expect(await gcReaderLatch(state, qualified, lease.token, keys, "ordinary-reader")).toBe(
        false
      );
      expect(
        await gcReaderLatch(state, qualified, "missing-lease", keys, "concurrent-reader")
      ).toBe(false);
      expect(await gcReaderLatch(state, qualified, lease.token, keys, "concurrent-reader")).toBe(
        true
      );
    });
    await f.stub.finishRepositoryRead(lease.token);
  });
  it.each([false, true])(
    "renews source protection across receive WAL recovery (real rewrite: %s)",
    async (rewriteRequired) => {
      const f = await fixture(rewriteRequired);
      const next = await checkpoint("interrupted-receive", f.base.oid);
      const lease = await f.stub.beginReceive();
      if (!lease.ok) throw new Error("receive failed");
      const key = r2PackKey(doPrefix(f.stub.id.toString()), "interrupted-receive.pack");
      await env.REPO_BUCKET.put(key, next.pack);
      const index = await indexTestPack(env, key, next.pack.byteLength);
      const request = {
        token: lease.lease.token,
        commands: [{ ref: "refs/heads/main", oldOid: f.base.oid, newOid: next.oid }],
        stagedPack: {
          packKey: key,
          packBytes: next.pack.byteLength,
          idxBytes: index.idxBytes,
          objectCount: index.objectCount,
        },
      };
      receiveTest.failNextAfterCatalogActivation();
      try {
        expect(
          await runInDurableObject(f.stub, async (_, state) => {
            try {
              await finalizeReceiveState({ ...request, ctx: state, env });
              return false;
            } catch (error) {
              return String(error).includes("injected post-catalog receive failure");
            }
          })
        ).toBe(true);
        await runInDurableObject(f.stub, async (_, state) => {
          const compact = await state.storage.get<{ expiresAt: number }>("compactLease");
          await state.storage.put("compactLease", { ...compact, expiresAt: Date.now() - 1 });
        });
        const claim = await f.stub.claimGcOperation("concurrent");
        if (claim.status !== "ready" || !claim.operation.claim) throw new Error("claim failed");
        expect((await f.stub.finalizeReceive(request)).status).toBe("committed");
        if (rewriteRequired) {
          expect(await f.stub.getGcOperation()).toMatchObject({
            phase: "index",
            coordination: { acceptedReceives: 1 },
          });
          expect(await env.REPO_BUCKET.head(claim.operation.inputPackKey)).not.toBeNull();
        } else {
          expect(
            await f.stub.commitGcOperation("concurrent", claim.operation.claim.id)
          ).toMatchObject({ status: "committed" });
        }
        expect(await f.stub.listRefs()).toEqual([{ name: "refs/heads/main", oid: next.oid }]);
      } finally {
        receiveTest.reset();
      }
    }
  );
  it.each([false, true])(
    "hands publication a turn at admission without blocking on its failure (%s)",
    async (failPublication) => {
      const f = await fixture();
      const claim = await f.stub.claimGcOperation("concurrent");
      if (claim.status !== "ready" || !claim.operation.claim) throw new Error("claim failed");
      const preceding = await f.stub.beginReceive();
      if (!preceding.ok) throw new Error("receive failed");
      expect(await f.stub.commitGcOperation("concurrent", claim.operation.claim.id)).toEqual({
        status: "retry",
        reason: "receive-active",
      });
      admissionTest.beforeAdmissionOnce(async (ctx) => {
        const result = await finalizeReceiveState({
          ctx,
          env,
          token: preceding.lease.token,
          commands: [],
        });
        expect(result.status).toBe("committed");
      });
      try {
        if (failPublication) publicationTest.failAfterPublicationWrites();
        const admitted = await f.stub.beginReceive();
        expect(admitted.ok).toBe(true);
        expect((await f.stub.getGcOperation())?.phase).toBe(
          failPublication ? "publish" : "reclaim"
        );
        if (admitted.ok) await f.stub.abortReceive(admitted.lease.token);
        const next = await f.stub.beginReceive();
        expect(next.ok).toBe(true);
        expect(await f.stub.getGcOperation()).toMatchObject({
          phase: "reclaim",
          commit: { status: "committed" },
        });
        if (next.ok) await f.stub.abortReceive(next.lease.token);
      } finally {
        admissionTest.reset();
        publicationTest.reset();
      }
    }
  );

  it("protects an external encoding base even when it is not a logical reference", async () => {
    const f = await fixture();
    const base = encode("deliberately unreachable");
    const suffix = encode(" plus new content");
    const result = await encodeGitObject("blob", new Uint8Array([...base, ...suffix]));
    const pack = await buildPack([
      { type: "ref-delta", baseOid: f.garbage.oid, delta: buildAppendOnlyDelta(base, suffix) },
    ]);
    const lease = await f.stub.beginReceive();
    if (!lease.ok) throw new Error("receive failed");
    const key = r2PackKey(doPrefix(f.stub.id.toString()), "thin-checkpoint.pack");
    await env.REPO_BUCKET.put(key, pack);
    const index = await indexTestPack(env, key, pack.byteLength, lease.activeCatalog);
    expect(
      (
        await f.stub.finalizeReceive({
          token: lease.lease.token,
          commands: [{ ref: "refs/tags/delta", oldOid: zeroOid(), newOid: result.oid }],
          stagedPack: {
            packKey: key,
            packBytes: pack.byteLength,
            idxBytes: index.idxBytes,
            objectCount: index.objectCount,
          },
        })
      ).status
    ).toBe("committed");
    expect(await f.stub.getGcOperation()).toMatchObject({
      coordination: {
        conservativeRetentionReason: "new-source-reachability",
        retainedSourcePackKeys: expect.arrayContaining(f.packKeys),
      },
    });
  });
  it("keeps every checkpoint and progresses through an admission-assisted metadata turn", async () => {
    const f = await fixture();
    let oid = f.base.oid;
    const newKeys: string[] = [];
    for (let i = 0; i < 8; i++) {
      const next = await receive(f, `checkpoint-${i}`, oid);
      oid = next.oid;
      newKeys.push(next.key);
    }
    expect(await f.stub.getGcOperation()).toMatchObject({
      coordination: {
        acceptedReceives: 8,
        retainedSourcePackKeys: [],
      },
    });
    const claim = await f.stub.claimGcOperation("concurrent");
    if (claim.status !== "ready" || !claim.operation.claim) throw new Error("claim failed");
    const held = await f.stub.beginReceive();
    if (!held.ok) throw new Error("receive failed");
    expect(await f.stub.commitGcOperation("concurrent", claim.operation.claim.id)).toEqual({
      status: "retry",
      reason: "receive-active",
    });
    await f.stub.abortReceive(held.lease.token);
    const admitted = await f.stub.beginReceive();
    expect(admitted.ok).toBe(true);
    const gc = await f.stub.getGcOperation();
    expect(gc).toMatchObject({ phase: "reclaim", commit: { supersededPackKeys: [f.packKeys[1]] } });
    expect(await f.stub.listRefs()).toEqual([{ name: "refs/heads/main", oid }]);
    expect((await f.stub.getActivePackCatalog()).map((row) => row.packKey).sort()).toEqual(
      [f.packKeys[0], ...newKeys].sort()
    );
    expect(await f.stub.commitGcOperation("concurrent", claim.operation.claim.id)).toEqual(
      gc!.commit
    );
    if (admitted.ok) await f.stub.abortReceive(admitted.lease.token);
  });

  it("allows checkpoints throughout expired-claim drain and reuses the prepared source", async () => {
    const f = await fixture();
    const claim = await f.stub.claimGcOperation("concurrent");
    if (claim.status !== "ready" || !claim.operation.claim) throw new Error("claim failed");
    await runInDurableObject(f.stub, async (_, state) => {
      const op = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
      op!.claim!.expiresAt = Date.now() - 1;
      await state.storage.put(GC_OPERATION_KEY, op);
      const lease = await state.storage.get<{ expiresAt: number }>("compactLease");
      await state.storage.put("compactLease", { ...lease, expiresAt: Date.now() - 1 });
    });
    const next = await receive(f, "during-drain", f.base.oid);
    expect((await f.stub.claimGcOperation("concurrent")).status).toBe("busy");
    expect(await f.stub.setRefs([])).toBe(false);
    await runInDurableObject(f.stub, async (_, state) => {
      const op = await state.storage.get<GcOperation>(GC_OPERATION_KEY);
      op!.claim!.safeRetryAt = Date.now() - 1;
      await state.storage.put(GC_OPERATION_KEY, op);
    });
    const recovered = await f.stub.claimGcOperation("concurrent");
    if (recovered.status !== "ready" || !recovered.operation.claim)
      throw new Error("recovery failed");
    expect(await f.stub.commitGcOperation("concurrent", claim.operation.claim.id)).toMatchObject({
      status: "retry",
    });
    expect(
      await f.stub.commitGcOperation("concurrent", recovered.operation.claim.id)
    ).toMatchObject({ status: "committed" });
    expect(await f.stub.listRefs()).toEqual([{ name: "refs/heads/main", oid: next.oid }]);
    expect((await f.stub.getGcOperation())!.measurements.rewrite!.attempts).toBe(1);
  });

  it("protects a permitted new root into previously unreachable source objects before ACK", async () => {
    const f = await fixture();
    const lease = await f.stub.beginReceive();
    if (!lease.ok) throw new Error("receive failed");
    expect(
      (
        await f.stub.finalizeReceive({
          token: lease.lease.token,
          commands: [{ ref: "refs/tags/resurrected", oldOid: zeroOid(), newOid: f.garbage.oid }],
        })
      ).status
    ).toBe("committed");
    expect(await f.stub.getGcOperation()).toMatchObject({
      coordination: {
        conservativeRetentionReason: "new-source-reachability",
        retainedSourcePackKeys: expect.arrayContaining(f.packKeys),
      },
    });
    const claim = await f.stub.claimGcOperation("concurrent");
    if (claim.status !== "ready" || !claim.operation.claim) throw new Error("claim failed");
    expect(await f.stub.commitGcOperation("concurrent", claim.operation.claim.id)).toMatchObject({
      status: "committed",
      supersededPackKeys: [],
    });
    expect((await f.stub.getActivePackCatalog()).map((row) => row.packKey).sort()).toEqual(
      f.packKeys.sort()
    );
  });

  it.each(["catalog", "after-publication-writes"])(
    "rolls back catalog plus receipt after %s failure and retries",
    async (failure) => {
      const f = await fixture();
      const claim = await f.stub.claimGcOperation("concurrent");
      if (claim.status !== "ready" || !claim.operation.claim) throw new Error("claim failed");
      const rowsBefore = await f.stub.getActivePackCatalog();
      const operationBefore = await f.stub.getGcOperation();
      const readAuthority = () =>
        runInDurableObject(f.stub, async (_, state) =>
          state.storage.get([
            "refs",
            "head",
            "refsVersion",
            "packsetVersion",
            "generationPublicationPending",
            "compactLease",
            "reachabilityGcPending",
          ])
        );
      const authorityBefore = await readAuthority();
      if (failure === "catalog") catalogTest.failNextCatalogReplacement();
      else publicationTest.failAfterPublicationWrites();
      try {
        expect(await f.stub.commitGcOperation("concurrent", claim.operation.claim.id)).toEqual({
          status: "retry",
          reason: "catalog-replacement-failed",
        });
        expect(await f.stub.getActivePackCatalog()).toEqual(rowsBefore);
        expect(await readAuthority()).toEqual(authorityBefore);
        expect(await f.stub.getGcOperation()).toEqual({
          ...operationBefore,
          coordination: {
            ...operationBefore?.coordination,
            publicationClaimId: claim.operation.claim.id,
          },
        });
        expect(publicationTest.failureCount()).toBe(failure === "after-publication-writes" ? 1 : 0);
        expect(
          await f.stub.commitGcOperation("concurrent", claim.operation.claim.id)
        ).toMatchObject({
          status: "committed",
        });
      } finally {
        catalogTest.reset();
        publicationTest.reset();
      }
    }
  );

  it.each(["refs", "source", "deleting"])(
    "rejects unaccounted %s changes immediately before publication",
    async (change) => {
      const f = await fixture();
      const claim = await f.stub.claimGcOperation("concurrent");
      if (claim.status !== "ready" || !claim.operation.claim) throw new Error("claim failed");
      await runInDurableObject(f.stub, async (_, state) => {
        if (change === "refs") await state.storage.put("refsVersion", 100);
        if (change === "deleting") await state.storage.put("repositoryDeleting", true);
        if (change === "source") {
          const db = getDb(state.storage);
          const row = await getPackCatalogRow(db, f.packKeys[1]!);
          await upsertPackCatalogRow(db, { ...row!, packBytes: row!.packBytes + 1 });
        }
      });
      expect(await f.stub.commitGcOperation("concurrent", claim.operation.claim.id)).toMatchObject({
        status: "retry",
        reason:
          change === "refs"
            ? "refs-changed"
            : change === "source"
              ? "source-changed"
              : "repository-deleting",
      });
      expect((await f.stub.getGcOperation())!.commit).toBeUndefined();
      expect(await env.REPO_BUCKET.head(f.packKeys[1]!)).not.toBeNull();
    }
  );
});
