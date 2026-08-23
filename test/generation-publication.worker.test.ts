import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";
import { z } from "zod";

import { createLogger, getRepoStub } from "@/worker/common";
import { publishRepositoryGeneration } from "@/worker/git/generation/publish";
import { SubrequestLimiter } from "@/worker/git/operations/limits";
import type { PackCatalogRow } from "@/worker/do/repo/db/schema";
import { getDb, upsertPackCatalogRow } from "@/worker/do/repo/db";
import {
  completeGenerationPublicationState,
  getPendingGenerationPublicationState,
} from "@/worker/do/repo/catalog";
import { asTypedStorage, type RepoStateSchema } from "@/worker/do/repo/repoState";
import {
  doPrefix,
  repositoryGenerationIndexKey,
  repositoryGenerationManifestKey,
} from "@/worker/keys";
import { runDOWithRetry } from "./util/test-helpers";

const indexSchema = z.object({
  schemaVersion: z.literal(1),
  generation: z.number().int(),
  manifestKey: z.string(),
});

function catalog(packKey: string, sequence: number): PackCatalogRow {
  return {
    packKey,
    kind: "compact",
    state: "active",
    tier: 1,
    seqLo: sequence,
    seqHi: sequence,
    objectCount: 3,
    packBytes: 100,
    idxBytes: 50,
    createdAt: sequence,
    supersededBy: null,
  };
}

describe("repository generation publication", () => {
  it("bootstraps a first generation for an upgraded catalog with superseded rows", async () => {
    const repoId = `generation-bootstrap-${crypto.randomUUID()}`;
    const stub = getRepoStub(env, repoId);
    const prefix = doPrefix(stub.id.toString());
    const active = catalog(`${prefix}/objects/pack/active.pack`, 2);
    const superseded = {
      ...catalog(`${prefix}/objects/pack/superseded.pack`, 1),
      state: "superseded" as const,
    };
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const db = getDb(state.storage);
        await upsertPackCatalogRow(db, active);
        await upsertPackCatalogRow(db, superseded);
        await state.storage.put("packsetVersion", 7);
      }
    );

    expect(await stub.ensureGenerationPublicationPending()).toEqual({
      generation: 7,
      activePackKeys: [active.packKey],
    });
  });

  it("keeps the catalog snapshot bound to its generation while a later version advances", async () => {
    const repoId = `generation-snapshot-${crypto.randomUUID()}`;
    const stub = getRepoStub(env, repoId);
    const prefix = doPrefix(stub.id.toString());
    const generationOne = [catalog(`${prefix}/objects/pack/one.pack`, 1)];
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const store = asTypedStorage<RepoStateSchema>(state.storage);
        await store.put("packsetVersion", 1);
        await store.put("generationPublicationPending", {
          generation: 1,
          activePackKeys: generationOne.map((row) => row.packKey),
        });
        await store.put("packsetVersion", 2);
        const pending = await getPendingGenerationPublicationState(state);
        expect(pending).toEqual({
          generation: 1,
          activePackKeys: generationOne.map((row) => row.packKey),
        });
        expect(await completeGenerationPublicationState(state, 1)).toBe(true);
        expect(await getPendingGenerationPublicationState(state)).toBeNull();
      }
    );
  });

  it("CAS-publishes immutable generations and refuses to move the pointer backward", async () => {
    const doId = `generation-${crypto.randomUUID()}`;
    const prefix = doPrefix(doId);
    const limiter = new SubrequestLimiter(6);
    const countSubrequest = () => {};
    const log = createLogger("error", { service: "GenerationPublicationTest" });
    const generationOne = [catalog(`${prefix}/objects/pack/one.pack`, 1)];
    const generationTwo = [catalog(`${prefix}/objects/pack/two.pack`, 2)];

    expect(
      await publishRepositoryGeneration({
        env,
        doId,
        generation: 1,
        activePackKeys: generationOne.map((row) => row.packKey),
        limiter,
        countSubrequest,
        log,
      })
    ).toBe("published");
    expect(
      await publishRepositoryGeneration({
        env,
        doId,
        generation: 2,
        activePackKeys: generationTwo.map((row) => row.packKey),
        limiter,
        countSubrequest,
        log,
      })
    ).toBe("published");
    expect(
      await publishRepositoryGeneration({
        env,
        doId,
        generation: 1,
        activePackKeys: generationOne.map((row) => row.packKey),
        limiter,
        countSubrequest,
        log,
      })
    ).toBe("superseded");

    const indexObject = await env.REPO_BUCKET.get(repositoryGenerationIndexKey(prefix));
    if (!indexObject) throw new Error("generation index missing");
    const index = indexSchema.parse(await indexObject.json());
    expect(index).toEqual({
      schemaVersion: 1,
      generation: 2,
      manifestKey: repositoryGenerationManifestKey(prefix, 2),
    });
    expect(await env.REPO_BUCKET.head(repositoryGenerationManifestKey(prefix, 1))).not.toBeNull();
    expect(await env.REPO_BUCKET.head(repositoryGenerationManifestKey(prefix, 2))).not.toBeNull();

    await env.REPO_BUCKET.delete([
      repositoryGenerationIndexKey(prefix),
      repositoryGenerationManifestKey(prefix, 1),
      repositoryGenerationManifestKey(prefix, 2),
    ]);
  });
});
