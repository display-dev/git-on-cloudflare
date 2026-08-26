import { describe, expect, it } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { getRepoStub } from "@/worker/common";
import { doPrefix } from "@/worker/keys";
import { setupRepoForTests } from "./util/repoSeed";
import { runDOWithRetry, withEnvOverrides } from "./util/test-helpers";

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
      expect(inventory.repository.transientStateCount).toBe(2);
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
        }
      );

      const after = (await (await qualificationRequest()).json()) as {
        repository: { transientStateCount: number };
      };
      expect(after.repository.transientStateCount).toBe(0);
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
      if (repeated.status === 409) {
        expect(repeatedBody).toMatchObject({ status: "conflict", reason: "active" });
      } else {
        expect(repeated.status).toBe(202);
        expect(repeatedBody).toMatchObject({ status: "already_reset", deletedStateCount: 0 });
      }
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
