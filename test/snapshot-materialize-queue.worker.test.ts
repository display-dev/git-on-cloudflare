import { afterEach, describe, expect, it, vi } from "vitest";
import { createMessageBatch } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";

import { createLogger, zeroOid } from "@/worker/common";
import { concatChunks, encodeGitObject, flushPkt, pktLine } from "@/worker/git/core";
import { snapshotObjectKey } from "@/worker/git/snapshot/materialize";
import {
  SNAPSHOT_MAX_DELIVERY_ATTEMPTS,
  __test as snapshotTaskTest,
} from "@/worker/tasks/snapshotMaterialize";
import {
  SnapshotMaterializeQueueMessageSchema,
  type SnapshotMaterializeQueueMessage,
} from "@/worker/tasks/types";
import type { TreeEntry } from "@/worker/git/operations/read/types";

import { buildPack } from "./util/git-pack";
import { buildTreePayload } from "./util/packed-repo";
import { createQueueSendResponse, runQueueMessage } from "./util/queue";
import { setupRepoForTests } from "./util/repoSeed";
import { seedPackFirstRepo } from "./util/pack-first";
import { decodeReportStatus, pushStreamingUpdate } from "./util/streaming-helpers";
import { toRequestBody, uniqueRepoId, withEnvOverrides } from "./util/test-helpers";

const REF = "refs/heads/main";
const VALID_MESSAGE: SnapshotMaterializeQueueMessage = {
  kind: "snapshot-materialize",
  doName: "snapshot/queue",
  repositoryId: "repo_test",
  ref: REF,
  beforeSha: "1".repeat(40),
  afterSha: "2".repeat(40),
  actor: "user_test",
  sourceSurface: "git-push",
  idempotencyKey: null,
};

type SnapshotPackObject = {
  type: "blob" | "tree" | "commit";
  payload: Uint8Array;
};

function findSnapshotMessage(
  calls: ReadonlyArray<readonly unknown[]>,
  index = 0
): SnapshotMaterializeQueueMessage {
  const messages: SnapshotMaterializeQueueMessage[] = [];
  for (const call of calls) {
    const parsed = SnapshotMaterializeQueueMessageSchema.safeParse(call[0]);
    if (parsed.success) messages.push(parsed.data);
  }
  const message = messages[index];
  if (!message) throw new Error(`missing snapshot queue message at index ${index}`);
  return message;
}

async function deleteMainRef(args: {
  owner: string;
  repo: string;
  oldOid: string;
  authHeader: string;
}): Promise<void> {
  const body = concatChunks([
    pktLine(`${args.oldOid} ${zeroOid()} ${REF}\0 report-status agent=test\n`),
    flushPkt(),
  ]);
  const response = await workerExports.default.fetch(
    `https://example.com/${args.owner}/${args.repo}/git-receive-pack`,
    {
      method: "POST",
      headers: {
        Authorization: args.authHeader,
        "Content-Type": "application/x-git-receive-pack-request",
      },
      body: toRequestBody(body),
    }
  );
  expect(response.status).toBe(200);
  expect(decodeReportStatus(new Uint8Array(await response.arrayBuffer()))).toContain(`ok ${REF}`);
}

async function pushOversizedSnapshot(args: {
  owner: string;
  repo: string;
  parentOid: string;
  authHeader: string;
}): Promise<string> {
  const blobs: Array<{ payload: Uint8Array; oid: string }> = [];
  for (let index = 0; index < 101; index++) {
    const payload = new TextEncoder().encode(`snapshot file ${index}\n`);
    const blob = await encodeGitObject("blob", payload);
    blobs.push({ payload, oid: blob.oid });
  }
  const entries: TreeEntry[] = blobs.map((blob, index) => ({
    mode: "100644",
    name: `file-${String(index).padStart(3, "0")}.txt`,
    oid: blob.oid,
  }));
  const treePayload = buildTreePayload(entries);
  const tree = await encodeGitObject("tree", treePayload);
  const author = "Queue <queue@example.com> 0 +0000";
  const commitPayload = new TextEncoder().encode(
    `tree ${tree.oid}\n` +
      `parent ${args.parentOid}\n` +
      `author ${author}\n` +
      `committer ${author}\n\n` +
      `oversized snapshot\n`
  );
  const commit = await encodeGitObject("commit", commitPayload);
  const packObjects: SnapshotPackObject[] = blobs.map((blob) => ({
    type: "blob",
    payload: blob.payload,
  }));
  packObjects.push(
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload }
  );
  const pack = await buildPack(packObjects);
  const body = concatChunks([
    pktLine(`${args.parentOid} ${commit.oid} ${REF}\0 report-status agent=test\n`),
    flushPkt(),
    pack,
  ]);
  const response = await workerExports.default.fetch(
    `https://example.com/${args.owner}/${args.repo}/git-receive-pack`,
    {
      method: "POST",
      headers: {
        Authorization: args.authHeader,
        "Content-Type": "application/x-git-receive-pack-request",
      },
      body: toRequestBody(body),
    }
  );
  expect(response.status).toBe(200);
  expect(decodeReportStatus(new Uint8Array(await response.arrayBuffer()))).toContain(`ok ${REF}`);
  return commit.oid;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("snapshot materialization queue", () => {
  it("freezes a strict, bounded queue-body contract", () => {
    expect(SnapshotMaterializeQueueMessageSchema.parse(VALID_MESSAGE)).toEqual(VALID_MESSAGE);
    expect(Object.keys(VALID_MESSAGE)).toEqual([
      "kind",
      "doName",
      "repositoryId",
      "ref",
      "beforeSha",
      "afterSha",
      "actor",
      "sourceSurface",
      "idempotencyKey",
    ]);
    expect(
      SnapshotMaterializeQueueMessageSchema.safeParse({ ...VALID_MESSAGE, unexpected: true })
        .success
    ).toBe(false);
    expect(
      SnapshotMaterializeQueueMessageSchema.safeParse({
        ...VALID_MESSAGE,
        afterSha: "not-an-object-id",
      }).success
    ).toBe(false);
    expect(
      SnapshotMaterializeQueueMessageSchema.safeParse({
        ...VALID_MESSAGE,
        actor: "a".repeat(257),
      }).success
    ).toBe(false);
  });

  it("enqueues after an accepted push, materializes once, and converges on replay and deletion", async () => {
    const owner = "snapshot-queue";
    const repo = uniqueRepoId("delivery");
    const route = await setupRepoForTests(env, owner, repo);
    const seeded = await seedPackFirstRepo(route.doName);
    const send = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockResolvedValue(createQueueSendResponse());

    const pushed = await pushStreamingUpdate(owner, repo, seeded.nextCommit.oid, "queued\n");
    const message = findSnapshotMessage(send.mock.calls);
    expect(message).toEqual({
      kind: "snapshot-materialize",
      doName: route.doName,
      repositoryId: route.repositoryId,
      ref: REF,
      beforeSha: seeded.nextCommit.oid,
      afterSha: pushed.commitOid,
      actor: route.userId,
      sourceSurface: "git-push",
      idempotencyKey: null,
    });

    const manifestKey = snapshotObjectKey({
      env,
      repositoryId: route.repositoryId,
      commitSha: pushed.commitOid,
    });
    expect(manifestKey).not.toBeNull();
    await expect(env.REPO_BUCKET.head(manifestKey!)).resolves.toBeNull();
    await expect(runQueueMessage(message)).resolves.toEqual({ acked: true, retried: false });
    await expect(env.REPO_BUCKET.head(manifestKey!)).resolves.toBeTruthy();
    await expect(runQueueMessage(message)).resolves.toEqual({ acked: true, retried: false });

    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(route.doName));
    await expect(stub.getSnapshotProjection(REF)).resolves.toMatchObject({
      snapshotCount: 1,
      current: { commitSha: pushed.commitOid },
    });

    await deleteMainRef({
      owner,
      repo,
      oldOid: pushed.commitOid,
      authHeader: route.pushAuthHeader,
    });
    const deletion = findSnapshotMessage(send.mock.calls, 1);
    expect(deletion.afterSha).toBe(zeroOid());
    await expect(runQueueMessage(deletion)).resolves.toEqual({ acked: true, retried: false });
    await expect(stub.getSnapshotProjection(REF)).resolves.toMatchObject({
      snapshotCount: 1,
      current: { commitSha: zeroOid() },
    });
  });

  it("does not enqueue when snapshots are disabled and terminally acknowledges a stale delivery", async () => {
    const owner = "snapshot-queue";
    const repo = uniqueRepoId("disabled");
    const route = await setupRepoForTests(env, owner, repo);
    const seeded = await seedPackFirstRepo(route.doName);
    const send = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockResolvedValue(createQueueSendResponse());

    await withEnvOverrides(env, { SNAPSHOT_BENCHMARK_PREFIX: "" }, async () => {
      const pushed = await pushStreamingUpdate(owner, repo, seeded.nextCommit.oid, "disabled\n");
      expect(() => findSnapshotMessage(send.mock.calls)).toThrow("missing snapshot queue message");
      const staleMessage: SnapshotMaterializeQueueMessage = {
        ...VALID_MESSAGE,
        doName: route.doName,
        repositoryId: route.repositoryId,
        beforeSha: seeded.nextCommit.oid,
        afterSha: pushed.commitOid,
        actor: route.userId,
      };
      await expect(runQueueMessage(staleMessage)).resolves.toEqual({
        acked: true,
        retried: false,
      });
    });
  });

  it("terminally acknowledges an oversized snapshot without advancing projection", async () => {
    const owner = "snapshot-queue";
    const repo = uniqueRepoId("oversized");
    const route = await setupRepoForTests(env, owner, repo);
    const seeded = await seedPackFirstRepo(route.doName);
    const send = vi
      .spyOn(env.REPO_TASKS_QUEUE, "send")
      .mockResolvedValue(createQueueSendResponse());

    const commitSha = await pushOversizedSnapshot({
      owner,
      repo,
      parentOid: seeded.nextCommit.oid,
      authHeader: route.pushAuthHeader,
    });
    const message = findSnapshotMessage(send.mock.calls);
    expect(message.afterSha).toBe(commitSha);
    await expect(runQueueMessage(message)).resolves.toEqual({ acked: true, retried: false });

    const manifestKey = snapshotObjectKey({
      env,
      repositoryId: route.repositoryId,
      commitSha,
    });
    expect(manifestKey).not.toBeNull();
    await expect(env.REPO_BUCKET.head(manifestKey!)).resolves.toBeNull();
    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(route.doName));
    await expect(stub.getSnapshotProjection(REF)).resolves.toEqual({ snapshotCount: 0 });
  });

  it("retries transient failures only through the frozen bounded-attempt policy", () => {
    expect(SNAPSHOT_MAX_DELIVERY_ATTEMPTS).toBe(5);
    expect(
      snapshotTaskTest.isTerminalSnapshotError(new Error("Snapshot exceeds benchmark limits"))
    ).toBe(true);
    expect(snapshotTaskTest.isTerminalSnapshotError(new Error("temporary R2 outage"))).toBe(false);

    const retryBatch = createMessageBatch("git-on-cloudflare-repo-maint", [
      {
        id: "snapshot-retry",
        timestamp: new Date(),
        attempts: SNAPSHOT_MAX_DELIVERY_ATTEMPTS - 1,
        body: VALID_MESSAGE,
      },
    ]);
    const retryMessage = retryBatch.messages[0]!;
    const retry = vi.spyOn(retryMessage, "retry");
    const retryAck = vi.spyOn(retryMessage, "ack");
    snapshotTaskTest.retryOrAck(
      retryMessage,
      createLogger(env.LOG_LEVEL, { service: "SnapshotQueueTest" }),
      VALID_MESSAGE,
      "transient-delivery-failure",
      new Error("temporary R2 outage")
    );
    expect(retry).toHaveBeenCalledWith({ delaySeconds: 30 });
    expect(retryAck).not.toHaveBeenCalled();

    const exhaustedBatch = createMessageBatch("git-on-cloudflare-repo-maint", [
      {
        id: "snapshot-exhausted",
        timestamp: new Date(),
        attempts: SNAPSHOT_MAX_DELIVERY_ATTEMPTS,
        body: VALID_MESSAGE,
      },
    ]);
    const exhaustedMessage = exhaustedBatch.messages[0]!;
    const exhaustedRetry = vi.spyOn(exhaustedMessage, "retry");
    const exhaustedAck = vi.spyOn(exhaustedMessage, "ack");
    snapshotTaskTest.retryOrAck(
      exhaustedMessage,
      createLogger(env.LOG_LEVEL, { service: "SnapshotQueueTest" }),
      VALID_MESSAGE,
      "transient-delivery-failure",
      new Error("temporary R2 outage")
    );
    expect(exhaustedRetry).not.toHaveBeenCalled();
    expect(exhaustedAck).toHaveBeenCalledOnce();
  });
});
