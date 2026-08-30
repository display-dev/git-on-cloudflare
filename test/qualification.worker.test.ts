import { describe, expect, it, vi } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { createLogger, getRepoStub } from "@/worker/common";
import { doPrefix, packIndexKey, packRefsKey } from "@/worker/keys";
import {
  getDb,
  listPackCatalog,
  upsertPackCatalogRow,
  deletePackCatalogRows,
} from "@/worker/do/repo/db";
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

async function signQualificationRecoveryJournal(payload: unknown): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(env.SESSION_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = new Uint8Array(
    await crypto.subtle.sign(
      "HMAC",
      key,
      new TextEncoder().encode(`qualification-storage-recovery\0${JSON.stringify(payload)}`)
    )
  );
  return [...signature].map((byte) => byte.toString(16).padStart(2, "0")).join("");
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
  it("recovers only aged authority pairs under an exact idle-state fence", async () => {
    const seeded = await setupRepoForTests(env, namespace, repository, {
      doName: `repo:qualification-storage-${crypto.randomUUID()}`,
    });
    const stub = getRepoStub(env, seeded.doName);
    const prefix = doPrefix(stub.id.toString());
    const publishedOnlyPack = `${prefix}/objects/pack/pack-cmp-${crypto.randomUUID()}.pack`;
    await env.REPO_BUCKET.put(
      `${prefix}/generations/0.json`,
      JSON.stringify({
        schemaVersion: 1,
        generation: 0,
        packs: [{ packKey: publishedOnlyPack }],
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
    const operationId = "qualification-test";
    const fingerprint = "a".repeat(64);
    const orphan = `${prefix}/objects/pack/pack-native-${operationId}-${fingerprint}-claim-${crypto.randomUUID()}.pack`;
    const orphanIdx = packIndexKey(orphan);
    const orphanRefs = packRefsKey(orphan);
    await env.REPO_BUCKET.put(orphan, new Uint8Array(17));
    await env.REPO_BUCKET.put(orphanIdx, new Uint8Array(1));
    await env.REPO_BUCKET.put(orphanRefs, new Uint8Array(1));
    const authorityPrefix = `${prefix}/native-receive/authority/${operationId}-${fingerprint}`;
    const authority = `${authorityPrefix}/ref-0.json`;
    const receipt = `${authorityPrefix}/receipt.json`;
    await env.REPO_BUCKET.put(
      authority,
      JSON.stringify({
        schemaVersion: 1,
        kind: "authoritative-ref",
        name: "refs/heads/qual-finished",
        oid: "a".repeat(40),
      })
    );
    await env.REPO_BUCKET.put(
      receipt,
      JSON.stringify({
        schemaVersion: 1,
        kind: "operation-receipt",
        disposition: "committed",
        refName: "refs/heads/qual-finished",
        newOid: "a".repeat(40),
        digest: "b".repeat(64),
      })
    );
    await enabled(async () => {
      const inventory = await stub.getQualificationInventory();
      const body = JSON.stringify({
        schemaVersion: 1,
        expectedRefStateDigest: inventory.refStateDigest,
        expectedObjectCount: 7,
      });
      const init = {
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": String(body.length) },
        body,
      };
      expect((await qualificationRequest("/storage-recovery", init, "wrong")).status).toBe(401);
      await withEnvOverrides(env, { SESSION_SECRET: "   " }, async () => {
        const unsigned = await qualificationRequest("/storage-recovery", init);
        expect(await unsigned.json()).toMatchObject({
          status: "conflict",
          reason: "recovery_signing_unavailable",
        });
      });
      expect(await env.REPO_BUCKET.head(orphan)).not.toBeNull();
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
        const unknown = `${prefix}/objects/pack/pack-unknown-${crypto.randomUUID()}.pack`;
        await env.REPO_BUCKET.put(unknown, new Uint8Array(3));
        const bodyWithUnknown = JSON.stringify({ ...JSON.parse(body), expectedObjectCount: 8 });
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
        const forgedJournal = `${prefix}/qualification/storage-recovery-plan.json`;
        await env.REPO_BUCKET.put(forgedJournal, "{");
        const malformedJournalBody = JSON.stringify({
          ...JSON.parse(body),
          expectedObjectCount: 8,
        });
        const malformedJournal = await qualificationRequest("/storage-recovery", {
          ...init,
          body: malformedJournalBody,
          headers: {
            ...init.headers,
            "Content-Length": String(malformedJournalBody.length),
          },
        });
        expect(await malformedJournal.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_authority",
        });
        await env.REPO_BUCKET.delete(forgedJournal);
        await env.REPO_BUCKET.put(receipt, "{");
        const malformedAuthority = await qualificationRequest("/storage-recovery", init);
        expect(await malformedAuthority.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_authority",
        });
        await env.REPO_BUCKET.put(
          receipt,
          JSON.stringify({
            schemaVersion: 1,
            kind: "authoritative-ref",
            name: "refs/heads/qual-finished",
            oid: "a".repeat(40),
          })
        );
        const roleMismatch = await qualificationRequest("/storage-recovery", init);
        expect(await roleMismatch.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_authority",
        });
        await env.REPO_BUCKET.put(
          receipt,
          JSON.stringify({
            schemaVersion: 1,
            kind: "operation-receipt",
            disposition: "committed",
            refName: "refs/heads/qual-finished",
            newOid: "a".repeat(40),
            digest: "b".repeat(64),
          })
        );
        await env.REPO_BUCKET.put(
          forgedJournal,
          JSON.stringify({
            schemaVersion: 1,
            kind: "qualification-storage-recovery-plan",
            expectedRefStateDigest: inventory.refStateDigest,
            packsetVersion: 0,
            unchangedInventoryDigest: "a".repeat(64),
            ownerGroups: [
              {
                ownerKey: `${operationId}\0${fingerprint}`,
                objects: [
                  { key: orphan, etag: (await env.REPO_BUCKET.head(orphan))!.etag, size: 17 },
                ],
              },
            ],
            signature: "b".repeat(64),
          })
        );
        const forgedBody = JSON.stringify({ ...JSON.parse(body), expectedObjectCount: 8 });
        const forgedResult = await qualificationRequest("/storage-recovery", {
          ...init,
          body: forgedBody,
          headers: { ...init.headers, "Content-Length": String(forgedBody.length) },
        });
        expect(await forgedResult.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_authority",
        });
        expect(await env.REPO_BUCKET.head(orphan)).not.toBeNull();
        await env.REPO_BUCKET.delete(forgedJournal);
        const forgedArtifactJournalPayload = {
          schemaVersion: 1 as const,
          kind: "qualification-storage-recovery-plan" as const,
          expectedRefStateDigest: inventory.refStateDigest,
          packsetVersion: 0,
          unchangedInventoryDigest: "a".repeat(64),
          ownerGroups: [
            {
              ownerKey: `${operationId}\0${fingerprint}`,
              objects: [
                { key: orphan, etag: (await env.REPO_BUCKET.head(orphan))!.etag, size: 17 },
              ],
            },
          ],
        };
        await env.REPO_BUCKET.put(
          forgedJournal,
          JSON.stringify({
            ...forgedArtifactJournalPayload,
            signature: await signQualificationRecoveryJournal(forgedArtifactJournalPayload),
          })
        );
        const signedArtifactJournal = await qualificationRequest("/storage-recovery", {
          ...init,
          body: forgedBody,
          headers: { ...init.headers, "Content-Length": String(forgedBody.length) },
        });
        expect(await signedArtifactJournal.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_authority",
        });
        expect(await env.REPO_BUCKET.head(orphan)).not.toBeNull();
        await env.REPO_BUCKET.delete(forgedJournal);
        // The content-valid legacy authority pair cannot authorize deletion of
        // a pack triple carrying the same operation identity.
        const forgedOwnership = await qualificationRequest("/storage-recovery", init);
        expect(await forgedOwnership.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_orphan",
        });
        for (const key of [orphan, orphanIdx, orphanRefs]) {
          expect(await env.REPO_BUCKET.head(key)).not.toBeNull();
        }
        await env.REPO_BUCKET.delete([orphan, orphanIdx, orphanRefs]);
        const authorityOnlyBody = JSON.stringify({
          ...JSON.parse(body),
          expectedObjectCount: 4,
        });
        const recovered = await qualificationRequest("/storage-recovery", {
          ...init,
          body: authorityOnlyBody,
          headers: { ...init.headers, "Content-Length": String(authorityOnlyBody.length) },
        });
        expect(recovered.status).toBe(200);
        expect(await recovered.json()).toMatchObject({
          status: "recovered",
          deletedObjectCount: 2,
          remainingObjectCount: 2,
        });
        expect(await env.REPO_BUCKET.head(authority)).toBeNull();
        const inventoryAfter = await stub.getQualificationInventory();
        expect(inventoryAfter).toEqual(inventory);
        const abandonedCompaction = `${prefix}/objects/pack/pack-cmp-${crypto.randomUUID()}.pack`;
        await env.REPO_BUCKET.put(abandonedCompaction, new Uint8Array(19));
        const abandonedCompactionBody = JSON.stringify({
          ...JSON.parse(body),
          expectedObjectCount: 3,
        });
        const deleteCompaction = env.REPO_BUCKET.delete.bind(env.REPO_BUCKET);
        const lostCompactionDelete = vi
          .spyOn(env.REPO_BUCKET, "delete")
          .mockImplementationOnce(async () => {
            throw new Error("simulated lost compaction-delete acknowledgement");
          });
        try {
          const interruptedCompaction = await qualificationRequest("/storage-recovery", {
            ...init,
            body: abandonedCompactionBody,
            headers: {
              ...init.headers,
              "Content-Length": String(abandonedCompactionBody.length),
            },
          });
          expect(await interruptedCompaction.json()).toMatchObject({
            status: "inconclusive",
            reason: "deletion_not_proven",
          });
        } finally {
          lostCompactionDelete.mockRestore();
        }
        expect(await env.REPO_BUCKET.head(abandonedCompaction)).not.toBeNull();
        const resumeCompactionBody = JSON.stringify({
          ...JSON.parse(body),
          expectedObjectCount: 4,
        });
        const reclaimedCompaction = await qualificationRequest("/storage-recovery", {
          ...init,
          body: resumeCompactionBody,
          headers: {
            ...init.headers,
            "Content-Length": String(resumeCompactionBody.length),
          },
        });
        expect(await reclaimedCompaction.json()).toMatchObject({
          status: "recovered",
          deletedObjectCount: 1,
          remainingObjectCount: 2,
        });
        expect(await env.REPO_BUCKET.head(abandonedCompaction)).toBeNull();

        const incompleteCompaction = `${prefix}/objects/pack/pack-cmp-${crypto.randomUUID()}.pack`;
        const incompleteCompactionIndex = packIndexKey(incompleteCompaction);
        await env.REPO_BUCKET.put(incompleteCompaction, new Uint8Array(23));
        await env.REPO_BUCKET.put(incompleteCompactionIndex, new Uint8Array(1));
        // The index sorts before its pack and must be rejected by the generic
        // unknown-artifact boundary before pack-only recovery is considered.
        const incompleteCompactionBody = JSON.stringify({
          ...JSON.parse(body),
          expectedObjectCount: 4,
        });
        const refusedCompaction = await qualificationRequest("/storage-recovery", {
          ...init,
          body: incompleteCompactionBody,
          headers: {
            ...init.headers,
            "Content-Length": String(incompleteCompactionBody.length),
          },
        });
        expect(await refusedCompaction.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_orphan",
        });
        expect(await env.REPO_BUCKET.head(incompleteCompaction)).not.toBeNull();
        expect(await env.REPO_BUCKET.head(incompleteCompactionIndex)).not.toBeNull();
        await env.REPO_BUCKET.delete(incompleteCompactionIndex);
        const incompleteCompactionRefs = packRefsKey(incompleteCompaction);
        await env.REPO_BUCKET.put(incompleteCompactionRefs, new Uint8Array(1));
        const refusedReferences = await qualificationRequest("/storage-recovery", {
          ...init,
          body: incompleteCompactionBody,
          headers: {
            ...init.headers,
            "Content-Length": String(incompleteCompactionBody.length),
          },
        });
        expect(await refusedReferences.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_orphan",
        });
        await deleteCompaction([incompleteCompaction, incompleteCompactionRefs]);
        expect(await stub.getQualificationInventory()).toEqual(inventoryAfter);
        const activePack = `${prefix}/objects/pack/pack-cmp-${crypto.randomUUID()}.pack`;
        const activeIdx = packIndexKey(activePack);
        const activeRefs = packRefsKey(activePack);
        await env.REPO_BUCKET.put(activePack, new Uint8Array(17));
        await env.REPO_BUCKET.put(activeIdx, new Uint8Array(1));
        await env.REPO_BUCKET.put(activeRefs, new Uint8Array(1));
        await runDOWithRetry(
          () => stub,
          async (_instance, state) => {
            await state.storage.put("refs", [{ name: "refs/heads/first", oid: "c".repeat(40) }]);
            await upsertPackCatalogRow(getDb(state.storage), {
              packKey: activePack,
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
        const protectedJournalPayload = {
          schemaVersion: 1 as const,
          kind: "qualification-storage-recovery-plan" as const,
          expectedRefStateDigest: (await stub.getQualificationInventory()).refStateDigest,
          packsetVersion: 0,
          unchangedInventoryDigest: "a".repeat(64),
          ownerGroups: [
            {
              ownerKey: "forged-protected-owner",
              objects: [
                {
                  key: activeIdx,
                  etag: (await env.REPO_BUCKET.head(activeIdx))!.etag,
                  size: 1,
                },
              ],
            },
          ],
        };
        await env.REPO_BUCKET.put(
          forgedJournal,
          JSON.stringify({
            ...protectedJournalPayload,
            signature: await signQualificationRecoveryJournal(protectedJournalPayload),
          })
        );
        const protectedJournalBody = JSON.stringify({
          schemaVersion: 1,
          expectedRefStateDigest: protectedJournalPayload.expectedRefStateDigest,
          expectedObjectCount: 6,
        });
        const protectedJournal = await qualificationRequest("/storage-recovery", {
          ...init,
          body: protectedJournalBody,
          headers: {
            ...init.headers,
            "Content-Length": String(protectedJournalBody.length),
          },
        });
        expect(await protectedJournal.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_authority",
        });
        expect(await env.REPO_BUCKET.head(activeIdx)).not.toBeNull();
        await env.REPO_BUCKET.delete(forgedJournal);
        const runAuthorities: string[] = [];
        for (const [suffix, oid] of [
          ["first", "c".repeat(40)],
          ["second", "d".repeat(40)],
        ] as const) {
          const owner = `${prefix}/native-receive/authority/${suffix}-${"e".repeat(64)}`;
          const refKey = `${owner}/ref-0.json`;
          const receiptKey = `${owner}/receipt.json`;
          runAuthorities.push(refKey, receiptKey);
          await env.REPO_BUCKET.put(
            refKey,
            JSON.stringify({
              schemaVersion: 1,
              kind: "authoritative-ref",
              name: `refs/heads/${suffix}`,
              oid,
            })
          );
          await env.REPO_BUCKET.put(
            receiptKey,
            JSON.stringify({
              schemaVersion: 1,
              kind: "operation-receipt",
              disposition: "committed",
              refName: `refs/heads/${suffix}`,
              newOid: oid,
              digest: "f".repeat(64),
            })
          );
        }
        const protectedInventory = await stub.getQualificationInventory();
        const protectedBody = JSON.stringify({
          schemaVersion: 1,
          expectedRefStateDigest: protectedInventory.refStateDigest,
          expectedObjectCount: 9,
        });
        const protectedResult = await qualificationRequest("/storage-recovery", {
          ...init,
          body: protectedBody,
          headers: { ...init.headers, "Content-Length": String(protectedBody.length) },
        });
        expect(await protectedResult.json()).toMatchObject({
          status: "recovered",
          deletedObjectCount: 4,
          remainingObjectCount: 5,
        });
        for (const key of [activePack, activeIdx, activeRefs]) {
          expect(await env.REPO_BUCKET.head(key)).not.toBeNull();
        }
        for (const key of runAuthorities) expect(await env.REPO_BUCKET.head(key)).toBeNull();
        expect(await stub.getQualificationInventory()).toEqual(protectedInventory);

        await env.REPO_BUCKET.put(publishedOnlyPack, new Uint8Array(19));
        const publishedInventory = await stub.getQualificationInventory();
        const publishedBody = JSON.stringify({
          schemaVersion: 1,
          expectedRefStateDigest: publishedInventory.refStateDigest,
          expectedObjectCount: 6,
        });
        const publishedResult = await qualificationRequest("/storage-recovery", {
          ...init,
          body: publishedBody,
          headers: { ...init.headers, "Content-Length": String(publishedBody.length) },
        });
        expect(await publishedResult.json()).toMatchObject({
          status: "recovered",
          deletedObjectCount: 0,
          remainingObjectCount: 6,
        });
        expect(await env.REPO_BUCKET.head(publishedOnlyPack)).not.toBeNull();
        await env.REPO_BUCKET.delete(publishedOnlyPack);

        const batchedAuthorities: Array<[string, string]> = [];
        for (let index = 0; index < 68; index++) {
          const operation = `batch-${index.toString().padStart(2, "0")}`;
          const owner = `${prefix}/native-receive/authority/${operation}-${"a".repeat(64)}`;
          const refKey = `${owner}/ref-0.json`;
          const receiptKey = `${owner}/receipt.json`;
          batchedAuthorities.push([refKey, receiptKey]);
          await env.REPO_BUCKET.put(
            refKey,
            JSON.stringify({
              schemaVersion: 1,
              kind: "authoritative-ref",
              name: `refs/heads/${operation}`,
              oid: "b".repeat(40),
            })
          );
          await env.REPO_BUCKET.put(
            receiptKey,
            JSON.stringify({
              schemaVersion: 1,
              kind: "operation-receipt",
              disposition: "committed",
              refName: `refs/heads/${operation}`,
              newOid: "b".repeat(40),
              digest: "c".repeat(64),
            })
          );
        }
        const firstBatchBody = JSON.stringify({
          schemaVersion: 1,
          expectedRefStateDigest: protectedInventory.refStateDigest,
          expectedObjectCount: 141,
        });
        const deleteFromBucket = env.REPO_BUCKET.delete.bind(env.REPO_BUCKET);
        const partialDelete = vi
          .spyOn(env.REPO_BUCKET, "delete")
          .mockImplementationOnce(async (keys) => {
            const selected = typeof keys === "string" ? [keys] : keys;
            await deleteFromBucket([selected[0]!, activeIdx]);
            throw new Error("simulated lost partial-delete acknowledgement");
          });
        try {
          const interruptedBatch = await qualificationRequest("/storage-recovery", {
            ...init,
            body: firstBatchBody,
            headers: { ...init.headers, "Content-Length": String(firstBatchBody.length) },
          });
          expect(await interruptedBatch.json()).toMatchObject({
            status: "conflict",
            reason: "storage_state_mismatch",
          });
        } finally {
          partialDelete.mockRestore();
        }
        expect(await env.REPO_BUCKET.head(activeIdx)).toBeNull();
        await env.REPO_BUCKET.put(activeIdx, new Uint8Array(1));
        const resumedBatchBody = JSON.stringify({
          schemaVersion: 1,
          expectedRefStateDigest: protectedInventory.refStateDigest,
          expectedObjectCount: 141,
        });
        const resumedBatch = await qualificationRequest("/storage-recovery", {
          ...init,
          body: resumedBatchBody,
          headers: { ...init.headers, "Content-Length": String(resumedBatchBody.length) },
        });
        expect(await resumedBatch.json()).toMatchObject({
          status: "recovered",
          deletedObjectCount: 100,
          remainingObjectCount: 41,
        });
        for (const [refKey, receiptKey] of batchedAuthorities) {
          const retained = [
            await env.REPO_BUCKET.head(refKey),
            await env.REPO_BUCKET.head(receiptKey),
          ];
          expect(retained[0] === null).toBe(retained[1] === null);
        }
        const secondBatchBody = JSON.stringify({
          schemaVersion: 1,
          expectedRefStateDigest: protectedInventory.refStateDigest,
          expectedObjectCount: 41,
        });
        const secondBatchResult = await qualificationRequest("/storage-recovery", {
          ...init,
          body: secondBatchBody,
          headers: { ...init.headers, "Content-Length": String(secondBatchBody.length) },
        });
        expect(await secondBatchResult.json()).toMatchObject({
          status: "recovered",
          deletedObjectCount: 36,
          remainingObjectCount: 5,
        });
        for (const keys of batchedAuthorities) {
          for (const key of keys) expect(await env.REPO_BUCKET.head(key)).toBeNull();
        }
        for (const key of [activePack, activeIdx, activeRefs]) {
          expect(await env.REPO_BUCKET.head(key)).not.toBeNull();
        }
        expect(await stub.getQualificationInventory()).toEqual(protectedInventory);

        const incompleteOperation = "incomplete-output";
        const incompleteFingerprint = "d".repeat(64);
        const incompleteOwner = `${prefix}/native-receive/authority/${incompleteOperation}-${incompleteFingerprint}`;
        const incompleteRef = `${incompleteOwner}/ref-0.json`;
        const incompleteReceipt = `${incompleteOwner}/receipt.json`;
        const incompletePack = `${prefix}/objects/pack/pack-native-${incompleteOperation}-${incompleteFingerprint}-claim-${crypto.randomUUID()}.pack`;
        await env.REPO_BUCKET.put(
          incompleteRef,
          JSON.stringify({
            schemaVersion: 1,
            kind: "authoritative-ref",
            name: "refs/heads/incomplete-output",
            oid: "d".repeat(40),
          })
        );
        await env.REPO_BUCKET.put(
          incompleteReceipt,
          JSON.stringify({
            schemaVersion: 1,
            kind: "operation-receipt",
            disposition: "committed",
            refName: "refs/heads/incomplete-output",
            newOid: "d".repeat(40),
            digest: "e".repeat(64),
          })
        );
        await env.REPO_BUCKET.put(incompletePack, new Uint8Array(1));
        const incompleteBody = JSON.stringify({
          schemaVersion: 1,
          expectedRefStateDigest: protectedInventory.refStateDigest,
          expectedObjectCount: 8,
        });
        const incompleteResult = await qualificationRequest("/storage-recovery", {
          ...init,
          body: incompleteBody,
          headers: { ...init.headers, "Content-Length": String(incompleteBody.length) },
        });
        expect(await incompleteResult.json()).toMatchObject({
          status: "conflict",
          reason: "unrecognized_orphan",
        });
        for (const key of [incompleteRef, incompleteReceipt, incompletePack]) {
          expect(await env.REPO_BUCKET.head(key)).not.toBeNull();
        }
        await env.REPO_BUCKET.delete([incompleteRef, incompleteReceipt, incompletePack]);
        expect(await stub.getQualificationInventory()).toEqual(protectedInventory);
      } finally {
        clock.mockRestore();
        await runDOWithRetry(
          () => stub,
          async (_instance, state) => {
            const catalog = await listPackCatalog(getDb(state.storage));
            await env.REPO_BUCKET.delete(
              catalog.flatMap((row) => [
                row.packKey,
                packIndexKey(row.packKey),
                packRefsKey(row.packKey),
              ])
            );
            await deletePackCatalogRows(
              getDb(state.storage),
              catalog.filter((row) => row.packBytes === 17).map((row) => row.packKey)
            );
          }
        );
        await env.REPO_BUCKET.delete([
          orphan,
          orphanIdx,
          orphanRefs,
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
          result: {
            statuses: [{ ref: "refs/heads/qual-observed", ok: true }],
            changed: true,
            empty: false,
            refPublication: { at: 1787731200900, refsVersion: 7 },
          },
          rejectionMetrics: {
            downloadedBytes: 4096,
            hydratedBytes: 4096,
            metadataBytes: 512,
            metadataRequests: 3,
            rangeBytes: 2048,
            rangeRequests: 2,
            activePackRangeBytes: 2048,
            activePackRangeRequests: 2,
            activePackWholeBytes: 0,
            activePackWholeRequests: 0,
            activePackUnattributedBytes: 0,
            activePackUnattributedRequests: 0,
            selectedPackBytes: 3000000000,
            activePackCount: 1,
          },
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
        result: {
          statuses: [{ ref: "refs/heads/qual-observed", ok: true }],
          changed: true,
          empty: false,
          refPublication: { at: 1787731200900, refsVersion: 7 },
        },
        metrics: {
          downloadedBytes: 4096,
          hydratedBytes: 4096,
          metadataBytes: 512,
          metadataRequests: 3,
          rangeBytes: 2048,
          rangeRequests: 2,
          activePackRangeBytes: 2048,
          activePackRangeRequests: 2,
          activePackWholeBytes: 0,
          activePackWholeRequests: 0,
          activePackUnattributedBytes: 0,
          activePackUnattributedRequests: 0,
          selectedPackBytes: 3000000000,
          activePackCount: 1,
        },
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
    const settleInit = {
      method: "POST",
      headers: { "Content-Type": "application/json", "Content-Length": "0" },
    };
    expect((await qualificationRequest("/gc-source/settle", settleInit)).status).toBe(404);
    expect((await qualificationRequest("/native-executions")).status).toBe(404);
    expect((await qualificationRequest("/native-executions", settleInit)).status).toBe(404);
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
      expect((await qualificationRequest("/native-executions", {}, "wrong-secret")).status).toBe(
        401
      );
      expect(
        (await qualificationRequest("/native-executions", settleInit, "wrong-secret")).status
      ).toBe(401);
      for (const extra of [
        { objectKey: "not-accepted" },
        { providerId: "not-accepted" },
        { claimId: "not-accepted" },
      ]) {
        const body = JSON.stringify({
          schemaVersion: 1,
          action: "cancel",
          lane: "maintenance",
          operationId: "test",
          generation: 1,
          ...extra,
        });
        expect(
          (
            await qualificationRequest("/native-executions", {
              method: "POST",
              body,
              headers: {
                "Content-Type": "application/json",
                "Content-Length": String(body.length),
              },
            })
          ).status
        ).toBe(400);
      }
      expect(
        (await qualificationRequest("/gc-source/settle", settleInit, "wrong-secret")).status
      ).toBe(401);
      expect((await qualificationRequest("/gc-source/settle", settleInit)).status).toBe(400);
      const invalid = JSON.stringify({
        schemaVersion: 1,
        expectedRefStateDigest: "a".repeat(64),
        extra: true,
      });
      expect(
        (
          await qualificationRequest("/gc-source/settle", {
            method: "POST",
            body: invalid,
            headers: {
              "Content-Type": "application/json",
              "Content-Length": String(invalid.length),
            },
          })
        ).status
      ).toBe(400);
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
