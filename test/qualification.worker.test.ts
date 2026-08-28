import { describe, expect, it, vi } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { createLogger, getRepoStub } from "@/worker/common";
import { doPrefix } from "@/worker/keys";
import { getDb, upsertPackCatalogRow, deletePackCatalogRows } from "@/worker/do/repo/db";
import { setupRepoForTests } from "./util/repoSeed";
import { buildPack, runDOWithRetry, withEnvOverrides } from "./util/test-helpers";
import { runQueueMessage } from "./util/queue";
import { seedPackedRepoState } from "./util/packed-repo";
import { publishRepositoryGeneration } from "@/worker/git/generation/publish";
import { SubrequestLimiter } from "@/worker/git/operations/limits";

const namespace = `qual-${"a".repeat(32)}`;
const repository = `repo-${"b".repeat(24)}`;
const secret = "qualification-control-test-secret";
const observerSecret = "qualification-observer-test-secret";

async function qualificationRequest(
  path = "",
  init: RequestInit = {},
  token = secret
): Promise<Response> {
  return await workerExports.default.fetch(
    `https://example.com/_internal/qualification/${namespace}/${repository}${path}`,
    { ...init, headers: { Authorization: `Bearer ${token}`, ...(init.headers ?? {}) } }
  );
}

async function enabled<T>(fn: () => Promise<T>): Promise<T> {
  return await withEnvOverrides(
    env,
    {
      QUALIFICATION_MODE: "1",
      QUALIFICATION_NAMESPACE: namespace,
      QUALIFICATION_REPOSITORY: repository,
      QUALIFICATION_SECRET: secret,
      QUALIFICATION_OBSERVER_SECRET: observerSecret,
      QUALIFICATION_TARGET_REVISION: "1".repeat(40),
      QUALIFICATION_CONTAINER_IMAGE_DIGEST: `sha256:${"2".repeat(64)}`,
    },
    fn
  );
}

describe("qualification repository controls", () => {
  it.each(["complete", "cancel"])(
    "streams the current GC source despite an older R2 generation and releases its reader on %s",
    async (ending) => {
      const artifactNamespace = `qual-${crypto.randomUUID().replaceAll("-", "")}`;
      const repo = await setupRepoForTests(env, artifactNamespace, repository, {
        doName: `repo:qualification-artifact-${crypto.randomUUID()}`,
      });
      const stub = getRepoStub(env, repo.doName);
      const payload = new Uint8Array(1024 * 1024);
      for (let offset = 0; offset < payload.byteLength; offset += 65536)
        crypto.getRandomValues(payload.subarray(offset, offset + 65536));
      const pack = await buildPack([{ type: "blob", payload }]);
      await seedPackedRepoState({
        env,
        repoId: repo.doName,
        getStub: () => stub,
        packs: [{ name: "artifact.pack", packBytes: pack }],
      });
      await publishRepositoryGeneration({
        env,
        doId: stub.id.toString(),
        generation: 0,
        activePackKeys: [],
        limiter: new SubrequestLimiter(6),
        countSubrequest: () => {},
        log: createLogger("error", { service: "QualificationArtifactTest" }),
      });
      await enabled(async () =>
        withEnvOverrides(env, { QUALIFICATION_NAMESPACE: artifactNamespace }, async () => {
          const source = await workerExports.default.fetch(
            `https://example.com/_internal/qualification/${artifactNamespace}/${repository}/gc-source`,
            { headers: { Authorization: `Bearer ${secret}` } }
          );
          expect(source.status).toBe(200);
          expect(await source.json()).toMatchObject({
            schemaVersion: 1,
            generation: 1,
            packs: [{ packBytes: pack.byteLength, objectCount: 1 }],
          });
          const artifactRequest = (generation: number) =>
            workerExports.default.fetch(
              `https://example.com/_internal/qualification/${artifactNamespace}/${repository}/gc-source/0/artifacts/pack?generation=${generation}`,
              { headers: { Authorization: `Bearer ${secret}` } }
            );
          const writer = await stub.beginReceive();
          expect(writer.ok).toBe(true);
          if (!writer.ok) throw new Error("test writer unavailable");
          expect((await artifactRequest(1)).status).toBe(409);
          await stub.abortReceive(writer.lease.token);
          expect((await artifactRequest(2)).status).toBe(409);
          const response = await artifactRequest(1);
          expect(response.status).toBe(200);
          expect(response.headers.get("Content-Length")).toBe(String(pack.byteLength));
          expect(response.headers.get("Cache-Control")).toBe("no-store");
          if (ending === "complete")
            expect(new Uint8Array(await response.arrayBuffer())).toEqual(pack);
          else {
            const reader = response.body!.getReader();
            expect((await reader.read()).done).toBe(false);
            await runDOWithRetry(
              () => stub,
              async (_, state) => {
                expect(await state.storage.get("repositoryReadLeases")).toBeDefined();
              }
            );
            await reader.cancel();
          }
          await vi.waitFor(async () =>
            runDOWithRetry(
              () => stub,
              async (_, state) => {
                expect(await state.storage.get("repositoryReadLeases")).toBeUndefined();
              }
            )
          );
          await runDOWithRetry(
            () => stub,
            async (_, state) => {
              await state.storage.put("repositoryDeleting", true);
            }
          );
          try {
            const deleting = await workerExports.default.fetch(
              `https://example.com/_internal/qualification/${artifactNamespace}/${repository}/gc-source`,
              { headers: { Authorization: `Bearer ${secret}` } }
            );
            expect(deleting.status).toBe(409);
            expect((await artifactRequest(1)).status).toBe(409);
          } finally {
            await runDOWithRetry(
              () => stub,
              async (_, state) => {
                await state.storage.delete("repositoryDeleting");
              }
            );
          }
        })
      );
    }
  );
  it("admits only a closed exact-target GC request and exposes no private output keys", async () => {
    const seeded = await setupRepoForTests(env, namespace, repository, {
      doName: `repo:qualification-gc-${crypto.randomUUID()}`,
    });
    const stub = getRepoStub(env, seeded.doName);
    const request = {
      schemaVersion: 1,
      operationId: "qualified-gc",
      faults: ["after-rewrite"],
      holdReader: true,
      deadlineAt: Date.now() + 3_600_000,
    };
    const post = (value: unknown) => {
      const body = JSON.stringify(value);
      return {
        method: "POST",
        body,
        headers: { "Content-Type": "application/json", "Content-Length": String(body.length) },
      };
    };
    expect((await qualificationRequest("/gc", post(request))).status).toBe(404);
    await enabled(async () => {
      expect((await qualificationRequest("/gc", post(request), "wrong")).status).toBe(401);
      expect(
        (await qualificationRequest("/gc", post({ ...request, arbitraryKey: "forbidden" }))).status
      ).toBe(400);
      expect(
        (await qualificationRequest("/gc", post({ ...request, faults: ["delete-anything"] })))
          .status
      ).toBe(400);
      expect((await qualificationRequest("/gc", post(request))).status).toBe(202);
      expect((await qualificationRequest("/gc", post(request))).status).toBe(202);
      expect(
        (await qualificationRequest("/gc", post({ ...request, holdReader: false }))).status
      ).toBe(409);
      const operation = await stub.getGcOperation();
      expect(operation).toMatchObject({
        phase: "queued",
        qualification: { faults: { "after-rewrite": {} }, reader: {} },
      });
      const response = await qualificationRequest("/gc/qualified-gc");
      expect(response.status).toBe(200);
      const text = await response.text();
      expect(text).not.toContain(operation!.inputPackKey);
      expect(text).not.toContain(secret);
      expect((await qualificationRequest("/gc/other-operation")).status).toBe(404);
      expect((await qualificationRequest("/gc/qualified-gc/artifacts/pack")).status).toBe(409);
    });
    // The exact route is reused by later control fixtures. Finish our admitted
    // empty-repository operation rather than leaving an active write fence.
    for (let stage = 0; stage < 3; stage++)
      await runQueueMessage({
        kind: "reachability-gc",
        repoId: seeded.doName,
        doId: stub.id.toString(),
        operationId: "qualified-gc",
      });
    expect(await stub.getGcOperation()).toMatchObject({ phase: "complete" });
    await stub.resetQualificationState((await stub.getQualificationInventory()).refStateDigest);
  });
  it("recovers only aged uncatalogued artifacts under an exact idle-state fence", async () => {
    const seeded = await setupRepoForTests(env, namespace, repository, {
      doName: `repo:qualification-storage-${crypto.randomUUID()}`,
    });
    const stub = getRepoStub(env, seeded.doName);
    const prefix = doPrefix(stub.id.toString());
    await env.REPO_BUCKET.put(
      `${prefix}/generations/0.json`,
      JSON.stringify({
        schemaVersion: 1,
        generation: 0,
        packs: [],
      })
    );
    await env.REPO_BUCKET.put(
      `${prefix}/generation-index.json`,
      JSON.stringify({
        schemaVersion: 1,
        generation: 0,
        manifestKey: `${prefix}/generations/0.json`,
        updatedAt: Date.now(),
      })
    );
    const orphan = `${prefix}/objects/pack/pack-cmp-${crypto.randomUUID()}.pack`;
    await env.REPO_BUCKET.put(orphan, new Uint8Array(17));
    const authority = `${prefix}/native-receive/authority/qualification-test-${"a".repeat(64)}/ref-0.json`;
    await env.REPO_BUCKET.put(
      authority,
      JSON.stringify({
        schemaVersion: 1,
        kind: "authoritative-ref",
        name: "refs/heads/qual-finished",
        oid: "a".repeat(40),
      })
    );
    await enabled(async () => {
      const inventory = await stub.getQualificationInventory();
      const body = JSON.stringify({
        schemaVersion: 1,
        expectedRefStateDigest: inventory.refStateDigest,
        expectedObjectCount: 4,
      });
      const init = {
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": String(body.length) },
        body,
      };
      expect((await qualificationRequest("/storage-recovery", init, "wrong")).status).toBe(401);
      const young = await qualificationRequest("/storage-recovery", init);
      expect(await young.json()).toMatchObject({
        status: "conflict",
        reason: "orphan_writer_drain",
      });
      expect(await env.REPO_BUCKET.head(orphan)).not.toBeNull();
      const now = Date.now();
      const clock = vi.spyOn(Date, "now").mockReturnValue(now + 6 * 60_000);
      try {
        const reader = await stub.beginRepositoryRead();
        expect(reader.ok).toBe(true);
        const active = await qualificationRequest("/storage-recovery", init);
        expect(await active.json()).toMatchObject({
          status: "conflict",
          reason: "repository_active_or_changed",
        });
        if (reader.ok) await stub.finishRepositoryRead(reader.token);
        await runDOWithRetry(
          () => stub,
          async (_instance, state) => {
            await state.storage.put("compactLease", {
              token: "qualification-drain",
              operation: "reachability-gc",
              createdAt: now - 18 * 60_000,
              expiresAt: now + 2 * 60_000,
            });
          }
        );
        const draining = await qualificationRequest("/storage-recovery", init);
        expect(await draining.json()).toMatchObject({
          status: "conflict",
          reason: "repository_active_or_changed",
        });
        clock.mockReturnValue(now + 8 * 60_000);
        const unknown = `${prefix}/unknown-object`;
        await env.REPO_BUCKET.put(unknown, new Uint8Array(3));
        const bodyWithUnknown = JSON.stringify({ ...JSON.parse(body), expectedObjectCount: 5 });
        const refused = await qualificationRequest("/storage-recovery", {
          ...init,
          body: bodyWithUnknown,
          headers: { ...init.headers, "Content-Length": String(bodyWithUnknown.length) },
        });
        expect(await refused.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_orphan",
        });
        expect(await env.REPO_BUCKET.head(orphan)).not.toBeNull();
        await env.REPO_BUCKET.delete(unknown);
        const recovered = await qualificationRequest("/storage-recovery", init);
        expect(recovered.status).toBe(200);
        expect(await recovered.json()).toMatchObject({
          status: "recovered",
          deletedObjectCount: 2,
          remainingObjectCount: 2,
        });
        expect(await env.REPO_BUCKET.head(orphan)).toBeNull();
        expect(await env.REPO_BUCKET.head(authority)).toBeNull();
        const inventoryAfter = await stub.getQualificationInventory();
        expect(inventoryAfter).toEqual(inventory);
        await env.REPO_BUCKET.put(orphan, new Uint8Array(17));
        await runDOWithRetry(
          () => stub,
          async (_instance, state) => {
            await upsertPackCatalogRow(getDb(state.storage), {
              packKey: orphan,
              kind: "compact",
              state: "active",
              tier: 1,
              seqLo: 1,
              seqHi: 1,
              objectCount: 1,
              packBytes: 17,
              idxBytes: 1,
              createdAt: now,
              supersededBy: null,
            });
          }
        );
        const protectedBody = JSON.stringify({ ...JSON.parse(body), expectedObjectCount: 3 });
        const protectedResult = await qualificationRequest("/storage-recovery", {
          ...init,
          body: protectedBody,
          headers: { ...init.headers, "Content-Length": String(protectedBody.length) },
        });
        expect(await protectedResult.json()).toMatchObject({
          status: "recovered",
          deletedObjectCount: 0,
        });
        expect(await env.REPO_BUCKET.head(orphan)).not.toBeNull();
      } finally {
        clock.mockRestore();
        await runDOWithRetry(
          () => stub,
          async (_instance, state) => {
            await deletePackCatalogRows(getDb(state.storage), [orphan]);
          }
        );
        await env.REPO_BUCKET.delete([
          orphan,
          `${prefix}/generations/0.json`,
          `${prefix}/generation-index.json`,
        ]);
      }
    });
  });

  it("exposes a separately authenticated bounded operation observation", async () => {
    const seeded = await setupRepoForTests(env, namespace, repository, {
      doName: `repo:qualification-observer-${crypto.randomUUID()}`,
    });
    const stub = getRepoStub(env, seeded.doName);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.put("nativeReceiveOperation:qualification-observed", {
          id: "qualification-observed",
          state: "committed",
          createdAt: 1787731200000,
          updatedAt: 1787731201000,
          attempts: 1,
        });
      }
    );
    await enabled(async () => {
      const path = `/operations/qualification-observed`;
      const controlCredential = await qualificationRequest(path);
      expect(controlCredential.status).toBe(401);
      const response = await qualificationRequest(path, {}, observerSecret);
      expect(response.status).toBe(200);
      expect(response.headers.get("Cache-Control")).toBe("no-store");
      expect(await response.json()).toEqual({
        schemaVersion: 1,
        id: "qualification-observed",
        state: "committed",
        createdAt: 1787731200000,
        updatedAt: 1787731201000,
        attempts: 1,
      });
      const missing = await qualificationRequest(
        "/operations/qualification-missing",
        {},
        observerSecret
      );
      expect(missing.status).toBe(404);
      expect(missing.headers.get("Cache-Control")).toBe("no-store");
    });
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.delete("nativeReceiveOperation:qualification-observed");
      }
    );
  });

  it("is absent by default and authenticates before repository lookup", async () => {
    const disabled = await qualificationRequest();
    expect(disabled.status).toBe(404);
    expect(disabled.headers.get("Cache-Control")).toBe("no-store");
    const disabledObserver = await qualificationRequest(
      "/operations/qualification-missing",
      {},
      observerSecret
    );
    expect(disabledObserver.status).toBe(404);
    expect(disabledObserver.headers.get("Cache-Control")).toBe("no-store");
    await enabled(async () => {
      const denied = await qualificationRequest("", {}, "wrong-secret");
      expect(denied.status).toBe(401);
      expect(denied.headers.get("Cache-Control")).toBe("no-store");
      const wrongNamespace = await workerExports.default.fetch(
        `https://example.com/_internal/qualification/qual-${"c".repeat(32)}/${repository}`,
        { headers: { Authorization: `Bearer ${secret}` } }
      );
      expect(wrongNamespace.status).toBe(404);
      expect(wrongNamespace.headers.get("Cache-Control")).toBe("no-store");
      const wrongRepository = await workerExports.default.fetch(
        `https://example.com/_internal/qualification/${namespace}/repo-${"d".repeat(24)}`,
        { headers: { Authorization: `Bearer ${secret}` } }
      );
      expect(wrongRepository.status).toBe(404);
    });
  });

  it("returns bounded identity-free inventory and resets only exact idle state", async () => {
    const seeded = await setupRepoForTests(env, namespace, repository, {
      doName: `repo:qualification-${crypto.randomUUID()}`,
    });
    const stub = getRepoStub(env, seeded.doName);
    await stub.seedMinimalRepo(true);
    const r2Prefix = doPrefix(stub.id.toString());
    await env.REPO_BUCKET.put(`${r2Prefix}/generation-index.json`, new Uint8Array(7));
    await env.REPO_BUCKET.put(`${r2Prefix}/generations/7.json`, new Uint8Array(11));
    await env.REPO_BUCKET.put(`${r2Prefix}/generations/not-a-generation.json`, new Uint8Array(13));
    const projectedOid = "3".repeat(40);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.put("nativeReceiveOperation:qualification-operation", {
          synthetic: true,
        });
        await state.storage.put("acceptedWrite:qualification-snapshot", {
          id: "qualification-snapshot",
          sequence: 2,
          fact: {
            ref: "refs/heads/qual-snapshot",
            beforeSha: "0".repeat(40),
            afterSha: projectedOid,
          },
          acceptedAt: Date.now(),
        });
        await state.storage.put("snapshotCurrent:refs%2Fheads%2Fqual-snapshot", {
          ref: "refs/heads/qual-snapshot",
          commitSha: projectedOid,
          sequence: 2,
          updatedAt: Date.now(),
        });
        await state.storage.put(`materializedSnapshot:${projectedOid}`, {
          commitSha: projectedOid,
          firstSequence: 2,
          materializedAt: Date.now(),
        });
        await state.storage.put("receiveLease", {
          token: "drained-qualification-lease",
          expiresAt: Date.now() - 5 * 60_000 - 1,
        });
        await state.storage.put("compactionWantedAt", Date.now());
      }
    );

    await enabled(async () => {
      const response = await qualificationRequest();
      expect(response.status).toBe(200);
      expect(response.headers.get("Cache-Control")).toBe("no-store");
      const raw = await response.text();
      expect(raw).not.toContain(namespace);
      expect(raw).not.toContain(repository);
      expect(raw).not.toContain(seeded.doName);
      const inventory = JSON.parse(raw) as {
        schemaVersion: number;
        status: string;
        repository: { refStateDigest: string; transientStateCount: number };
        storage: {
          objectCount: number;
          objectBytes: number;
          repositoryObjects: { objectCount: number; objectBytes: number };
          durableGenerationMetadata: { objectCount: number; objectBytes: number };
          complete: boolean;
        };
      };
      expect(inventory).toMatchObject({
        schemaVersion: 2,
        status: "ready",
        targetRevision: "1".repeat(40),
        containerImageDigest: `sha256:${"2".repeat(64)}`,
      });
      expect(inventory.repository.transientStateCount).toBe(3);
      expect(inventory.repository.refStateDigest).toMatch(/^[a-f0-9]{64}$/);
      expect(inventory.storage.complete).toBe(true);
      expect(inventory.storage.durableGenerationMetadata).toEqual({
        objectCount: 2,
        objectBytes: 18,
      });
      expect(
        inventory.storage.repositoryObjects.objectCount +
          inventory.storage.durableGenerationMetadata.objectCount
      ).toBe(inventory.storage.objectCount);
      expect(
        inventory.storage.repositoryObjects.objectBytes +
          inventory.storage.durableGenerationMetadata.objectBytes
      ).toBe(inventory.storage.objectBytes);

      const wrongBody = JSON.stringify({
        schemaVersion: 1,
        expectedRefStateDigest: "0".repeat(64),
        expectedObjectCount: inventory.storage.objectCount,
      });
      const conflict = await qualificationRequest("/reset", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": String(new TextEncoder().encode(wrongBody).byteLength),
        },
        body: wrongBody,
      });
      expect(conflict.status).toBe(409);

      const body = JSON.stringify({
        schemaVersion: 1,
        expectedRefStateDigest: inventory.repository.refStateDigest,
        expectedObjectCount: inventory.storage.objectCount,
      });
      const reset = await qualificationRequest("/reset", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": String(new TextEncoder().encode(body).byteLength),
        },
        body,
      });
      expect(reset.status).toBe(202);
      expect(reset.headers.get("Cache-Control")).toBe("no-store");
      expect(await reset.json()).toMatchObject({
        schemaVersion: 1,
        status: "reset",
        deletedStateCount: 4,
        reachabilityGc: "queued",
      });

      await runDOWithRetry(
        () => stub,
        async (_instance, state) => {
          expect(
            await state.storage.get("snapshotCurrent:refs%2Fheads%2Fqual-snapshot")
          ).toBeUndefined();
          expect(await state.storage.get(`materializedSnapshot:${projectedOid}`)).toBeUndefined();
          expect(await state.storage.get("compactionWantedAt")).toBeUndefined();
        }
      );

      const after = (await (await qualificationRequest()).json()) as {
        repository: { transientStateCount: number };
      };
      // Reset now admits a durable operation. Its unfinished work must remain
      // visible until Queue processing, rather than look like a clean baseline.
      expect(after.repository.transientStateCount).toBe(1);
      const queued = await stub.getGcOperation();
      expect(queued).toMatchObject({ phase: "queued" });
      const repeated = await qualificationRequest("/reset", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": String(new TextEncoder().encode(body).byteLength),
        },
        body,
      });
      const repeatedBody = (await repeated.json()) as {
        status: string;
        reason?: string;
        deletedStateCount?: number;
      };
      expect(repeated.status).toBe(409);
      expect(repeatedBody).toMatchObject({ status: "conflict", reason: "active" });
    });
  });

  it("keeps just-expired writers fenced through the canonical drain window", async () => {
    const seeded = await setupRepoForTests(env, namespace, repository, {
      doName: `repo:qualification-drain-${crypto.randomUUID()}`,
    });
    const stub = getRepoStub(env, seeded.doName);
    await stub.seedMinimalRepo(true);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.put("receiveLease", {
          token: "just-expired-writer",
          expiresAt: Date.now() - 1,
        });
        await state.storage.put("repositoryReadLeases", [
          {
            token: "reader",
            operation: "git-fetch",
            createdAt: Date.now(),
            expiresAt: Date.now() + 60_000,
          },
        ]);
      }
    );
    await enabled(async () => {
      const inventory = (await (await qualificationRequest()).json()) as {
        repository: { refStateDigest: string };
        storage: { objectCount: number };
      };
      const result = await stub.resetQualificationState(inventory.repository.refStateDigest);
      expect(result).toMatchObject({
        schemaVersion: 1,
        status: "conflict",
        reason: "active",
      });
    });
  });
});
