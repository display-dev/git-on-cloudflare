import { beforeAll, describe, expect, it } from "vitest";
import { env } from "cloudflare:workers";

import { newPrefixedId, getRepoStub } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import { findPatGrantForRepo, insertPatWithGrants } from "@/worker/db/d1/dal";
import { routeCacheKey } from "@/worker/repositories/routeCache";
import { findRepositoryById } from "@/worker/db/d1/dal/repositories";
import { generatePatPlaintext, hashPatPlaintext } from "@/worker/auth/pat";
import { doPrefix, repositoryImportPackKey } from "@/worker/keys";
import { snapshotObjectKey, snapshotRepositoryPrefix } from "@/worker/git/snapshot/materialize";
import type { RepositoryDeleteMessage } from "@/worker/tasks/queue";
import { getDb as getRepoDb, getPackCatalogCount, upsertPackCatalogRow } from "@/worker/do/repo/db";

import { ensureD1Migrations } from "./util/d1Setup";
import { seedRepo, type SeededRepo } from "./util/repoSeed";
import { runQueueMessage } from "./util/queue";
import { runAlarmWithRetry, runDOWithRetry } from "./util/do-retry";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

function uniqueNs(): string {
  return `rd-${Math.random().toString(36).slice(2, 8)}`;
}

async function seedDeletable(): Promise<
  SeededRepo & { patId: string; snapshotKey: string; importKey: string }
> {
  const s = await seedRepo(env, {
    namespaceSlug: uniqueNs(),
    repoSlug: "site",
    visibility: "public",
    doName: `repo:${newPrefixedId("repo").slice("repo_".length)}`,
  });
  // Add a repo-scoped PAT grant so we can verify the cascade on D1 delete.
  const db = createDb(env.DB);
  const patId = newPrefixedId("pat");
  const generated = generatePatPlaintext();
  const hash = await hashPatPlaintext(generated.plaintext);
  await insertPatWithGrants(db, {
    pat: {
      id: patId,
      userId: s.userId,
      name: "delete-test",
      prefix: generated.publicPrefix,
      hash,
      createdAt: Date.now(),
      expiresAt: null,
      revokedAt: null,
      lastUsedAt: null,
    },
    namespaceGrants: [],
    repoGrants: [{ patId, repoId: s.repositoryId, level: "push" }],
  });
  const doId = env.REPO_DO.idFromName(s.doName).toString();
  const packKey = `${doPrefix(doId)}/pack/synthetic-delete-probe.pack`;
  await env.REPO_BUCKET.put(packKey, "synthetic");
  const snapshotKey = snapshotObjectKey({
    env,
    repositoryId: s.repositoryId,
    commitSha: "2".repeat(40),
  });
  if (!snapshotKey) throw new Error("snapshot prefix must be configured for deletion test");
  await env.REPO_BUCKET.put(snapshotKey, "synthetic snapshot");
  const importKey = repositoryImportPackKey(s.doName, "synthetic-delete-probe");
  await env.REPO_BUCKET.put(importKey, "synthetic import");
  const stub = getRepoStub(env, s.doName);
  await stub.setRefs([{ name: "refs/heads/main", oid: "1".repeat(40) }]);
  await runDOWithRetry(
    () => stub,
    async (_instance, state) => {
      await upsertPackCatalogRow(getRepoDb(state.storage), {
        packKey,
        kind: "receive",
        state: "active",
        tier: 0,
        seqLo: 1,
        seqHi: 1,
        objectCount: 1,
        packBytes: 9,
        idxBytes: 0,
        createdAt: Date.now(),
        supersededBy: null,
      });
    }
  );
  return { ...s, patId, snapshotKey, importKey };
}

async function packCatalogCount(doName: string): Promise<number> {
  return await runDOWithRetry(
    () => getRepoStub(env, doName),
    async (_instance, state) => await getPackCatalogCount(getRepoDb(state.storage))
  );
}

function deleteMessage(s: SeededRepo): RepositoryDeleteMessage {
  return {
    kind: "repository-delete",
    repositoryId: s.repositoryId,
    namespaceId: s.namespaceId,
    namespaceSlug: s.namespaceSlug,
    repoSlug: s.repoSlug,
    doName: s.doName,
    actor: s.userId,
    requestedAt: Date.now(),
  };
}

async function r2HasObjectsForDoName(doName: string): Promise<boolean> {
  const doId = env.REPO_DO.idFromName(doName).toString();
  const listing = await env.REPO_BUCKET.list({ prefix: doPrefix(doId) });
  return (listing.objects?.length ?? 0) > 0;
}

describe("repository-delete consumer", () => {
  it("intact state -> deletes D1 row, cascades repo grants, removes ROUTES, clears DO", async () => {
    const s = await seedDeletable();
    const stub = getRepoStub(env, s.doName);
    expect(await stub.listRefs()).toHaveLength(1);
    const db = createDb(env.DB);
    expect(await findRepositoryById(db, s.repositoryId)).toBeDefined();
    expect(await findPatGrantForRepo(db, s.patId, s.repositoryId)).toBeDefined();
    expect(await env.ROUTES.get(routeCacheKey(s.namespaceSlug, s.repoSlug))).not.toBeNull();
    expect(await packCatalogCount(s.doName)).toBe(1);

    const result = await runQueueMessage(deleteMessage(s));
    expect(result.acked).toBe(true);
    expect(await findRepositoryById(db, s.repositoryId)).toBeUndefined();
    expect(await findPatGrantForRepo(db, s.patId, s.repositoryId)).toBeUndefined();
    expect(await env.ROUTES.get(routeCacheKey(s.namespaceSlug, s.repoSlug))).toBeNull();
    expect(await r2HasObjectsForDoName(s.doName)).toBe(false);
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).toBeNull();
    expect(await env.REPO_BUCKET.head(s.importKey)).toBeNull();
    expect(await stub.listRefs()).toEqual([]);
    expect(await packCatalogCount(s.doName)).toBe(0);
  });

  it("replay after D1 row already gone is a clean no-op ack", async () => {
    const s = await seedDeletable();
    const message = deleteMessage(s);
    const first = await runQueueMessage(message);
    expect(first.acked).toBe(true);
    const second = await runQueueMessage(message);
    expect(second.acked).toBe(true);
    expect(second.retried).toBe(false);
  });

  it("R2 list outage retries and a replay converges every repository surface to absent", async () => {
    const s = await seedDeletable();
    const message = deleteMessage(s);
    const failingEnv: Env = {
      ...env,
      REPO_BUCKET: {
        ...env.REPO_BUCKET,
        async list() {
          throw new Error("simulated r2 outage");
        },
      } as R2Bucket,
    } as Env;
    const result = await runQueueMessage(message, failingEnv);
    expect(result.retried).toBe(true);
    expect(result.acked).toBe(false);
    const db = createDb(env.DB);
    expect(await findRepositoryById(db, s.repositoryId)).toBeUndefined();
    expect(await env.ROUTES.get(routeCacheKey(s.namespaceSlug, s.repoSlug))).toBeNull();
    expect(await r2HasObjectsForDoName(s.doName)).toBe(true);
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).not.toBeNull();
    expect(await getRepoStub(env, s.doName).listRefs()).toHaveLength(1);

    const replay = await runQueueMessage(message);
    expect(replay.acked).toBe(true);
    expect(replay.retried).toBe(false);
    expect(await findRepositoryById(db, s.repositoryId)).toBeUndefined();
    expect(await env.ROUTES.get(routeCacheKey(s.namespaceSlug, s.repoSlug))).toBeNull();
    expect(await r2HasObjectsForDoName(s.doName)).toBe(false);
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).toBeNull();
    const refs = await getRepoStub(env, s.doName).listRefs();
    expect(refs).toEqual([]);
  });

  it("snapshot-prefix outage retries after Git objects are gone and replay finishes deletion", async () => {
    const s = await seedDeletable();
    const message = deleteMessage(s);
    const snapshotPrefix = snapshotRepositoryPrefix(env, s.repositoryId);
    if (!snapshotPrefix) throw new Error("snapshot prefix must be configured for deletion test");
    const failingEnv: Env = {
      ...env,
      REPO_BUCKET: {
        ...env.REPO_BUCKET,
        async list(options?: R2ListOptions) {
          if (options?.prefix === snapshotPrefix) {
            throw new Error("simulated snapshot list outage");
          }
          return await env.REPO_BUCKET.list(options);
        },
        async delete(keys: string | string[]) {
          return await env.REPO_BUCKET.delete(keys);
        },
      } as R2Bucket,
    } as Env;

    const result = await runQueueMessage(message, failingEnv);
    expect(result.retried).toBe(true);
    expect(result.acked).toBe(false);
    expect(await r2HasObjectsForDoName(s.doName)).toBe(false);
    expect(await env.REPO_BUCKET.head(s.importKey)).toBeNull();
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).not.toBeNull();
    expect(await getRepoStub(env, s.doName).listRefs()).toHaveLength(1);

    const replay = await runQueueMessage(message);
    expect(replay.acked).toBe(true);
    expect(replay.retried).toBe(false);
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).toBeNull();
    expect(await getRepoStub(env, s.doName).listRefs()).toEqual([]);
  });

  it("waits for an in-flight snapshot materializer before changing any repository surface", async () => {
    const s = await seedDeletable();
    const stub = getRepoStub(env, s.doName);
    const prefix = snapshotRepositoryPrefix(env, s.repositoryId);
    if (!prefix) throw new Error("snapshot prefix must be configured for deletion test");
    const lease = await stub.beginSnapshotMaterialization(prefix);
    if (!lease.ok) throw new Error("snapshot materialization lease must be granted");

    const waiting = await runQueueMessage(deleteMessage(s));
    expect(waiting.retried).toBe(true);
    expect(waiting.acked).toBe(false);
    expect(await findRepositoryById(createDb(env.DB), s.repositoryId)).toBeDefined();
    expect(await env.ROUTES.get(routeCacheKey(s.namespaceSlug, s.repoSlug))).not.toBeNull();
    expect(await r2HasObjectsForDoName(s.doName)).toBe(true);
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).not.toBeNull();
    expect(await stub.listRefs()).toHaveLength(1);
    expect(await stub.renewSnapshotMaterialization(lease.token)).toBe(false);

    expect(await stub.finishSnapshotMaterialization(lease.token)).toBe(true);
    const replay = await runQueueMessage(deleteMessage(s));
    expect(replay.acked).toBe(true);
    expect(await r2HasObjectsForDoName(s.doName)).toBe(false);
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).toBeNull();
    expect(await stub.listRefs()).toEqual([]);
    expect(await stub.beginSnapshotMaterialization(prefix)).toEqual({
      ok: false,
      reason: "repository-deleting",
    });
    expect(await stub.beginReceive()).toEqual(expect.objectContaining({ ok: false }));
    expect(await stub.beginCompaction()).toEqual(
      expect.objectContaining({
        ok: false,
        reason: "repository-deleting",
      })
    );
  });

  it("rejects expired receive and compaction commits after the deletion fence", async () => {
    const s = await seedDeletable();
    const stub = getRepoStub(env, s.doName);
    const expiredAt = Date.now() - 10 * 60_000;
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.put({
          receiveLease: {
            token: "expired-receive",
            createdAt: expiredAt - 1,
            expiresAt: expiredAt,
          },
          compactLease: {
            token: "expired-compaction",
            createdAt: expiredAt - 1,
            expiresAt: expiredAt,
          },
        });
      }
    );

    const deleted = await runQueueMessage(deleteMessage(s));
    expect(deleted.acked).toBe(true);
    expect(await stub.finalizeReceive({ token: "expired-receive", commands: [] })).toEqual(
      expect.objectContaining({ status: "lease_mismatch" })
    );
    expect(
      await stub.commitCompaction({
        token: "expired-compaction",
        sourcePacks: [],
        targetTier: 1,
        packsetVersion: 0,
        stagedPack: { packKey: "staged", packBytes: 1, idxBytes: 1, objectCount: 1 },
      })
    ).toEqual(expect.objectContaining({ status: "retry", reason: "repository-deleting" }));
  });

  it("keeps deletion fenced while an expired writer is inside the drain window", async () => {
    const s = await seedDeletable();
    const stub = getRepoStub(env, s.doName);
    const justExpired = Date.now() - 1;
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.put("receiveLease", {
          token: "draining-receive",
          createdAt: justExpired - 1,
          expiresAt: justExpired,
        });
      }
    );

    const waiting = await runQueueMessage(deleteMessage(s));
    expect(waiting.retried).toBe(true);
    expect(waiting.acked).toBe(false);
    expect(await findRepositoryById(createDb(env.DB), s.repositoryId)).toBeDefined();
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).not.toBeNull();

    expect(await stub.abortReceive("draining-receive")).toBe(true);
    const replay = await runQueueMessage(deleteMessage(s));
    expect(replay.acked).toBe(true);
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).toBeNull();
  });

  it("keeps the expired-writer drain marker when an alarm fires after fencing", async () => {
    const s = await seedDeletable();
    const getStub = () => getRepoStub(env, s.doName);
    const justExpired = Date.now() - 1;
    await runDOWithRetry(getStub, async (_instance, state) => {
      await state.storage.put("receiveLease", {
        token: "alarm-draining-receive",
        createdAt: justExpired - 1,
        expiresAt: justExpired,
      });
    });
    expect((await runQueueMessage(deleteMessage(s))).retried).toBe(true);
    await runDOWithRetry(getStub, async (_instance, state) => {
      await state.storage.setAlarm(Date.now() + 100);
    });
    expect(await runAlarmWithRetry(getStub)).toBe(true);
    await runDOWithRetry(getStub, async (_instance, state) => {
      expect(await state.storage.get("receiveLease")).toEqual(
        expect.objectContaining({ token: "alarm-draining-receive" })
      );
    });
    expect((await runQueueMessage(deleteMessage(s))).retried).toBe(true);
    expect(await getStub().abortReceive("alarm-draining-receive")).toBe(true);
    expect((await runQueueMessage(deleteMessage(s))).acked).toBe(true);
  });

  it("waits for R2 maintenance and prevents renewal after deletion starts", async () => {
    const s = await seedDeletable();
    const stub = getRepoStub(env, s.doName);
    const maintenance = await stub.beginRepositoryMaintenance();
    if (!maintenance.ok) throw new Error("maintenance lease must be granted");

    const waiting = await runQueueMessage(deleteMessage(s));
    expect(waiting.retried).toBe(true);
    expect(waiting.acked).toBe(false);
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).not.toBeNull();
    expect(await stub.renewRepositoryMaintenance(maintenance.token)).toBe(false);

    expect(await stub.finishRepositoryMaintenance(maintenance.token)).toBe(true);
    const replay = await runQueueMessage(deleteMessage(s));
    expect(replay.acked).toBe(true);
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).toBeNull();
  });

  it("preserves the deletion tombstone across alarms and post-delete mutation attempts", async () => {
    const s = await seedDeletable();
    const getStub = () => getRepoStub(env, s.doName);
    expect((await runQueueMessage(deleteMessage(s))).acked).toBe(true);
    await runDOWithRetry(getStub, async (_instance, state) => {
      await state.storage.setAlarm(Date.now() + 100);
    });
    expect(await runAlarmWithRetry(getStub)).toBe(true);
    await getStub().setRefs([{ name: "refs/heads/recreated", oid: "4".repeat(40) }]);
    await getStub().setHead({ target: "refs/heads/recreated", oid: "4".repeat(40) });
    expect(await getStub().listRefs()).toEqual([]);
    expect(await getStub().beginReceive()).toEqual(expect.objectContaining({ ok: false }));
    await runDOWithRetry(getStub, async (_instance, state) => {
      expect(await state.storage.get("repositoryDeleting")).toBe(true);
      expect(await state.storage.getAlarm()).toBeNull();
    });
  });

  it("fails closed on a snapshot prefix outside the repository scope", async () => {
    const s = await seedDeletable();
    const stub = getRepoStub(env, s.doName);
    const unrelatedKey = "snapshots/another-repository/manifest.json";
    await env.REPO_BUCKET.put(unrelatedKey, "unrelated");
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.put("snapshotPrefixes", ["snapshots/another-repository/"]);
      }
    );

    const result = await runQueueMessage(deleteMessage(s));
    expect(result.retried).toBe(true);
    expect(result.acked).toBe(false);
    expect(await env.REPO_BUCKET.head(unrelatedKey)).not.toBeNull();
    expect(await findRepositoryById(createDb(env.DB), s.repositoryId)).toBeDefined();
    await env.REPO_BUCKET.delete(unrelatedKey);
  });

  it("rejects a canonical-R2-shaped prefix even with the current repository ID", async () => {
    const s = await seedDeletable();
    const stub = getRepoStub(env, s.doName);
    const poisonedPrefix = `do/unrelated/pack/${encodeURIComponent(s.repositoryId)}/`;
    const unrelatedKey = `${poisonedPrefix}manifest.json`;
    await env.REPO_BUCKET.put(unrelatedKey, "unrelated");
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.put("snapshotPrefixes", [poisonedPrefix]);
      }
    );

    const result = await runQueueMessage(deleteMessage(s));
    expect(result.retried).toBe(true);
    expect(result.acked).toBe(false);
    expect(await env.REPO_BUCKET.head(unrelatedKey)).not.toBeNull();
    expect(await findRepositoryById(createDb(env.DB), s.repositoryId)).toBeDefined();
    await env.REPO_BUCKET.delete(unrelatedKey);
  });

  it("deletes every snapshot prefix recorded before configuration drift", async () => {
    const s = await seedDeletable();
    const stub = getRepoStub(env, s.doName);
    const currentPrefix = snapshotRepositoryPrefix(env, s.repositoryId);
    if (!currentPrefix) throw new Error("snapshot prefix must be configured for deletion test");
    const currentLease = await stub.beginSnapshotMaterialization(currentPrefix);
    if (!currentLease.ok) throw new Error("current snapshot lease must be granted");
    await stub.finishSnapshotMaterialization(currentLease.token);

    const oldPrefix = `retired-snapshots/${encodeURIComponent(s.repositoryId)}/`;
    const oldKey = `${oldPrefix}${"3".repeat(40)}/manifest.json`;
    await env.REPO_BUCKET.put(oldKey, "retired snapshot");
    const oldLease = await stub.beginSnapshotMaterialization(oldPrefix);
    if (!oldLease.ok) throw new Error("retired snapshot lease must be granted");
    await stub.finishSnapshotMaterialization(oldLease.token);

    const driftedEnv: Env = { ...env, SNAPSHOT_BENCHMARK_PREFIX: "" };
    const result = await runQueueMessage(deleteMessage(s), driftedEnv);
    expect(result.acked).toBe(true);
    expect(await env.REPO_BUCKET.head(s.snapshotKey)).toBeNull();
    expect(await env.REPO_BUCKET.head(oldKey)).toBeNull();
  });
});
