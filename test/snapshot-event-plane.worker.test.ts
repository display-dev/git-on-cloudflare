import { describe, expect, it } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { zeroOid } from "@/worker/common";
import type {
  SnapshotProjectionResult,
  SnapshotReconcilePlan,
} from "@/worker/do/repo/acceptedWrites";
import { concatChunks, flushPkt, pktLine } from "@/worker/git/core";
import { setupRepoForTests } from "./util/repoSeed";
import { decodeReportStatus, pushStreamingUpdate } from "./util/streaming-helpers";
import { lookupPushAuth } from "./util/repoSeed";
import { buildPack } from "./util/git-pack";
import { uniqueRepoId, withEnvOverrides } from "./util/test-helpers";

type IngestionResponse = {
  acceptedWrite: { afterSha: string };
};

type JournalEntry = {
  id: string;
  sequence: number;
  fact: { afterSha: string; sourceSurface: "ingestion" | "git-push" };
  materializedAt?: number;
};

type EventState = {
  entries: JournalEntry[];
  plan: SnapshotReconcilePlan;
  projection: {
    snapshotCount: number;
    current?: { commitSha: string; sequence: number };
  };
};

type EventDeliveryResponse = {
  projection: SnapshotProjectionResult;
};

function ingestionForm(args: {
  expectedOid: string;
  idempotencyKey: string;
  content: string;
}): FormData {
  const form = new FormData();
  form.set("expectedOid", args.expectedOid);
  form.set("actor", "event-probe-ingestion");
  form.set("idempotencyKey", args.idempotencyKey);
  form.set("committedAtSeconds", "1700000000");
  form.set("message", args.idempotencyKey);
  form.append("files", new Blob([args.content]), "index.html");
  return form;
}

async function ingest(owner: string, repo: string, form: FormData): Promise<string> {
  const response = await workerExports.default.fetch(
    `https://example.com/_internal/ingestion/${owner}/${repo}`,
    {
      method: "POST",
      headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` },
      body: form,
    }
  );
  expect(response.status).toBe(201);
  return ((await response.json()) as IngestionResponse).acceptedWrite.afterSha;
}

async function eventRequest(
  owner: string,
  repo: string,
  body?: Record<string, unknown>,
  token = env.INGESTION_RPC_TOKEN
): Promise<Response> {
  const payload = body ? JSON.stringify(body) : undefined;
  return await workerExports.default.fetch(
    `https://example.com/_internal/event-probe/${owner}/${repo}`,
    payload
      ? {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
            "Content-Length": String(new TextEncoder().encode(payload).byteLength),
          },
          body: payload,
        }
      : { headers: { Authorization: `Bearer ${token}` } }
  );
}

async function eventState(owner: string, repo: string): Promise<EventState> {
  const response = await eventRequest(owner, repo);
  expect(response.status).toBe(200);
  return (await response.json()) as EventState;
}

async function snapshotStatus(owner: string, repo: string, commitSha: string): Promise<number> {
  return (
    await workerExports.default.fetch(
      `https://example.com/_internal/snapshots/${owner}/${repo}/${commitSha}/manifest`,
      { headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` } }
    )
  ).status;
}

async function moveRef(owner: string, repo: string, oldOid: string, newOid: string): Promise<void> {
  const pack = await buildPack([
    { type: "blob", payload: new TextEncoder().encode(`ref move ${oldOid} ${newOid}`) },
  ]);
  const response = await workerExports.default.fetch(
    `https://example.com/${owner}/${repo}/git-receive-pack`,
    {
      method: "POST",
      headers: {
        Authorization: lookupPushAuth(owner, repo)!,
        "Content-Type": "application/x-git-receive-pack-request",
      },
      body: concatChunks([
        pktLine(`${oldOid} ${newOid} refs/heads/main\0 report-status agent=test\n`),
        flushPkt(),
        pack,
      ]) as any,
    }
  );
  expect(response.status).toBe(200);
  expect(decodeReportStatus(new Uint8Array(await response.arrayBuffer()))).toContain(
    "ok refs/heads/main"
  );
}

describe("snapshot event plane", () => {
  it("stays absent before configuration and authenticates before repository lookup", async () => {
    const missing = await eventRequest("missing", "repo");
    expect(missing.status).toBe(404);
    await withEnvOverrides(env, { SNAPSHOT_EVENT_PROBE: "1" }, async () => {
      const denied = await eventRequest("missing", "repo", undefined, "wrong-token");
      expect(denied.status).toBe(401);
    });
  });

  it("reconciles authoritative refs across feature-gate transitions", async () => {
    const owner = "events";
    const repo = uniqueRepoId("gate-transition");
    const seeded = await setupRepoForTests(env, owner, repo, {
      doName: `repo:${owner}-${repo}`,
    });
    const first = await ingest(
      owner,
      repo,
      ingestionForm({ expectedOid: zeroOid(), idempotencyKey: "gate-1", content: "one\n" })
    );

    let plannedSequence = 0;
    await withEnvOverrides(env, { SNAPSHOT_EVENT_PROBE: "1" }, async () => {
      const state = await eventState(owner, repo);
      expect(state.plan).toMatchObject({
        status: "head_only",
        afterSha: first,
        sequence: 1,
      });
      if (state.plan.status !== "head_only") throw new Error("expected a head-only plan");
      plannedSequence = state.plan.sequence;
    });

    const second = (await pushStreamingUpdate(owner, repo, first, "two\n")).commitOid;
    await moveRef(owner, repo, second, first);
    const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
    expect(
      await stub.projectReconciledHead({
        ref: "refs/heads/main",
        commitSha: first,
        sequence: plannedSequence,
        materializedAt: Date.now(),
      })
    ).toEqual({ status: "stale" });

    await withEnvOverrides(env, { SNAPSHOT_EVENT_PROBE: "1" }, async () => {
      let state = await eventState(owner, repo);
      expect(state.plan).toMatchObject({
        status: "head_only",
        afterSha: first,
        sequence: 3,
      });
      expect((await eventRequest(owner, repo, { action: "reconcile" })).status).toBe(200);
      state = await eventState(owner, repo);
      expect(state.projection.current).toMatchObject({ commitSha: first, sequence: 3 });
      expect(state.plan.status).toBe("up_to_date");
    });
  });

  it("handles reverse, duplicate, dropped, and crash-interrupted delivery", async () => {
    await withEnvOverrides(env, { SNAPSHOT_EVENT_PROBE: "1" }, async () => {
      const owner = "events";
      const repo = uniqueRepoId("projection");
      const seeded = await setupRepoForTests(env, owner, repo, {
        doName: `repo:${owner}-${repo}`,
      });

      const first = await ingest(
        owner,
        repo,
        ingestionForm({ expectedOid: zeroOid(), idempotencyKey: "event-1", content: "one\n" })
      );
      const second = (await pushStreamingUpdate(owner, repo, first, "two\n")).commitOid;
      let state = await eventState(owner, repo);
      expect(state.entries.map((entry) => [entry.sequence, entry.fact.sourceSurface])).toEqual([
        [1, "ingestion"],
        [2, "git-push"],
      ]);
      const secondEntry = state.entries[1]!;
      const firstEntry = state.entries[0]!;
      const concurrentSecondDeliveries = await Promise.all([
        eventRequest(owner, repo, { action: "deliver", entryId: secondEntry.id }),
        eventRequest(owner, repo, { action: "deliver", entryId: secondEntry.id }),
      ]);
      expect(concurrentSecondDeliveries.map((response) => response.status)).toEqual([200, 200]);
      const concurrentSecondResults: EventDeliveryResponse[] = await Promise.all(
        concurrentSecondDeliveries.map(async (response) => await response.json())
      );
      // The production queue is enabled in probe mode. It may win the race and
      // project this accepted write before either manual delivery returns; the
      // invariant is that manual concurrent delivery never creates it twice.
      expect(
        concurrentSecondResults.filter((result) => result.projection.snapshotCreated).length
      ).toBeLessThanOrEqual(1);
      expect(
        (
          await eventRequest(owner, repo, {
            action: "deliver",
            entryId: firstEntry.id,
          })
        ).status
      ).toBe(200);
      state = await eventState(owner, repo);
      expect(state.projection).toMatchObject({
        snapshotCount: 2,
        current: { commitSha: second, sequence: 2 },
      });

      const duplicateResponses = await Promise.all([
        eventRequest(owner, repo, { action: "deliver", entryId: secondEntry.id }),
        eventRequest(owner, repo, { action: "deliver", entryId: secondEntry.id }),
      ]);
      expect(duplicateResponses.map((response) => response.status)).toEqual([200, 200]);
      state = await eventState(owner, repo);
      expect(state.projection.snapshotCount).toBe(2);
      expect(state.projection.current?.commitSha).toBe(second);

      const third = await ingest(
        owner,
        repo,
        ingestionForm({ expectedOid: second, idempotencyKey: "event-3", content: "three\n" })
      );
      state = await eventState(owner, repo);
      const thirdEntry = state.entries.find((entry) => entry.fact.afterSha === third)!;
      expect(
        (
          await eventRequest(owner, repo, {
            action: "deliver",
            entryId: thirdEntry.id,
            crash: "before_snapshot",
          })
        ).status
      ).toBe(503);
      expect(await snapshotStatus(owner, repo, third)).toBe(404);
      expect(
        (
          await eventRequest(owner, repo, {
            action: "deliver",
            entryId: thirdEntry.id,
            crash: "after_snapshot",
          })
        ).status
      ).toBe(503);
      expect(await snapshotStatus(owner, repo, third)).toBe(200);
      state = await eventState(owner, repo);
      expect(
        state.entries.find((entry) => entry.id === thirdEntry.id)?.materializedAt
      ).toBeUndefined();
      expect(
        (
          await eventRequest(owner, repo, {
            action: "deliver",
            entryId: thirdEntry.id,
          })
        ).status
      ).toBe(200);

      const fourth = (await pushStreamingUpdate(owner, repo, third, "four\n")).commitOid;
      state = await eventState(owner, repo);
      expect(state.plan.status).toBe("deliver");
      const reconcile = await eventRequest(owner, repo, { action: "reconcile" });
      expect(reconcile.status).toBe(200);
      state = await eventState(owner, repo);
      expect(state.projection).toMatchObject({
        snapshotCount: 4,
        current: { commitSha: fourth, sequence: 4 },
      });

      const fifth = (await pushStreamingUpdate(owner, repo, fourth, "five\n")).commitOid;
      state = await eventState(owner, repo);
      const fifthEntry = state.entries.find((entry) => entry.fact.afterSha === fifth)!;
      expect(
        (await eventRequest(owner, repo, { action: "drop", entryId: fifthEntry.id })).status
      ).toBe(200);
      const stub = env.REPO_DO.get(env.REPO_DO.idFromName(seeded.doName));
      const headOnly = await eventRequest(owner, repo, { action: "reconcile" });
      expect(headOnly.status).toBe(200);
      expect(await headOnly.json()).toMatchObject({
        plan: { status: "head_only", afterSha: fifth },
        historyComplete: false,
      });
      state = await eventState(owner, repo);
      expect(state.projection).toMatchObject({
        snapshotCount: 5,
        current: { commitSha: fifth, sequence: 5 },
      });
      expect(state.plan.status).toBe("up_to_date");

      const sixth = (await pushStreamingUpdate(owner, repo, fifth, "six\n")).commitOid;
      await moveRef(owner, repo, sixth, fifth);
      state = await eventState(owner, repo);
      expect(state.plan.status).toBe("deliver");
      expect((await eventRequest(owner, repo, { action: "reconcile" })).status).toBe(200);
      state = await eventState(owner, repo);
      expect(state.projection.current).toMatchObject({ commitSha: fifth, sequence: 7 });
      const delayedSixth = state.entries.find((entry) => entry.fact.afterSha === sixth)!;
      expect(
        (await eventRequest(owner, repo, { action: "deliver", entryId: delayedSixth.id })).status
      ).toBe(200);
      state = await eventState(owner, repo);
      expect(state.projection).toMatchObject({
        snapshotCount: 6,
        current: { commitSha: fifth, sequence: 7 },
      });

      const staleProjection = await stub.projectReconciledHead({
        ref: "refs/heads/main",
        commitSha: fifth,
        sequence: 6,
        materializedAt: Date.now(),
      });
      expect(staleProjection).toEqual({ status: "stale" });

      await moveRef(owner, repo, fifth, zeroOid());
      state = await eventState(owner, repo);
      expect(state.plan.status).toBe("deliver");
      expect((await eventRequest(owner, repo, { action: "reconcile" })).status).toBe(200);
      state = await eventState(owner, repo);
      expect(state.projection).toMatchObject({
        snapshotCount: 6,
        current: { commitSha: zeroOid(), sequence: 8 },
      });
      expect(state.plan.status).toBe("up_to_date");
    });
  });
});
