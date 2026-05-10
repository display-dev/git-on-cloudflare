import { beforeAll, describe, expect, it } from "vitest";
import { env } from "cloudflare:workers";

import { newPrefixedId, getRepoStub } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import { findPatGrantForRepo, insertPatWithGrants } from "@/worker/db/d1/dal";
import { routeCacheKey } from "@/worker/repositories/routeCache";
import { findRepositoryById } from "@/worker/db/d1/dal/repositories";
import { generatePatPlaintext, hashPatPlaintext } from "@/worker/auth/pat";
import { doPrefix } from "@/worker/keys";
import type { RepositoryDeleteMessage } from "@/worker/tasks/queue";

import { ensureD1Migrations } from "./util/d1Setup";
import { seedRepo, type SeededRepo } from "./util/repoSeed";
import { runQueueMessage } from "./util/queue";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

function uniqueNs(): string {
  return `rd-${Math.random().toString(36).slice(2, 8)}`;
}

async function seedDeletable(): Promise<SeededRepo & { patId: string }> {
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
  return { ...s, patId };
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
    // Touch DO storage so the consumer's clear path has something to remove.
    const stub = getRepoStub(env, s.doName);
    await stub.listRefs();
    const db = createDb(env.DB);
    expect(await findRepositoryById(db, s.repositoryId)).toBeDefined();
    expect(await findPatGrantForRepo(db, s.patId, s.repositoryId)).toBeDefined();
    expect(await env.ROUTES.get(routeCacheKey(s.namespaceSlug, s.repoSlug))).not.toBeNull();

    const result = await runQueueMessage(deleteMessage(s));
    expect(result.acked).toBe(true);
    expect(await findRepositoryById(db, s.repositoryId)).toBeUndefined();
    expect(await findPatGrantForRepo(db, s.patId, s.repositoryId)).toBeUndefined();
    expect(await env.ROUTES.get(routeCacheKey(s.namespaceSlug, s.repoSlug))).toBeNull();
    expect(await r2HasObjectsForDoName(s.doName)).toBe(false);
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

  it("R2 list failure -> retries the message; D1 already deleted from step 1", async () => {
    const s = await seedDeletable();
    const failingEnv: Env = {
      ...env,
      REPO_BUCKET: {
        ...env.REPO_BUCKET,
        async list() {
          throw new Error("simulated r2 outage");
        },
      } as R2Bucket,
    } as Env;
    const result = await runQueueMessage(deleteMessage(s), failingEnv);
    expect(result.retried).toBe(true);
    expect(result.acked).toBe(false);
    const db = createDb(env.DB);
    expect(await findRepositoryById(db, s.repositoryId)).toBeUndefined();
  });
});
