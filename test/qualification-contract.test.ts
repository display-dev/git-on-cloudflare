import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

function contract(name: string): Record<string, unknown> {
  return JSON.parse(readFileSync(resolve("contracts", name), "utf8")) as Record<string, unknown>;
}

describe("qualification contract fixtures", () => {
  it("pins the schema-v2 inventory and schema-v1 reset boundaries", () => {
    const inventory = contract("qualification-inventory-v2.json");
    expect(Object.keys(inventory).sort()).toEqual([
      "containerImageDigest",
      "repository",
      "schemaVersion",
      "status",
      "storage",
      "targetRevision",
    ]);
    expect(inventory).toMatchObject({
      schemaVersion: 2,
      status: "ready",
      storage: {
        repositoryObjects: { objectCount: 3, objectBytes: 1_048_576 },
        durableGenerationMetadata: { objectCount: 2, objectBytes: 512 },
      },
    });
    expect(contract("qualification-reset-request-v1.json")).toEqual({
      schemaVersion: 1,
      expectedRefStateDigest: "3".repeat(64),
      expectedObjectCount: 3,
    });
    expect(contract("qualification-reset-result-v1.json")).toMatchObject({
      schemaVersion: 1,
      status: "reset",
      reachabilityGc: "queued",
    });
    const operation = contract("native-receive-operation-view-v1.json");
    expect(Object.keys(operation).sort()).toEqual([
      "attempts",
      "createdAt",
      "events",
      "id",
      "metrics",
      "result",
      "schemaVersion",
      "state",
      "updatedAt",
    ]);
    expect(operation).toMatchObject({
      schemaVersion: 1,
      id: "qualification-operation-v1",
      state: "committed",
      attempts: 1,
      metrics: {
        elapsedMs: 100,
        processorStartedAt: 1787731200100,
        stockTiming: {
          containerProcessMs: 40,
          containerWasRunning: true,
        },
      },
    });
  });

  it("pins schema-discriminated active and every terminal operation variant", () => {
    const variants = [
      ["native-receive-operation-active-v1.json", "processing"],
      ["native-receive-operation-view-v1.json", "committed"],
      ["native-receive-operation-aborted-v1.json", "aborted"],
      ["native-receive-operation-failed-v1.json", "failed"],
    ] as const;
    const allowed = new Set([
      "schemaVersion",
      "id",
      "state",
      "createdAt",
      "updatedAt",
      "attempts",
      "errorCode",
      "clientAckReadyAt",
      "events",
      "result",
      "metrics",
    ]);
    for (const [name, state] of variants) {
      const operation = contract(name);
      expect(operation).toMatchObject({ schemaVersion: 1, state });
      expect(Object.keys(operation).every((key) => allowed.has(key))).toBe(true);
    }
    const committed = contract("native-receive-operation-view-v1.json");
    expect(Object.keys(committed.result as Record<string, unknown>).sort()).toEqual([
      "changed",
      "empty",
      "statuses",
    ]);
    const statuses = (committed.result as { statuses: Array<Record<string, unknown>> }).statuses;
    expect(statuses.length).toBeGreaterThan(0);
    for (const status of statuses) {
      expect(Object.keys(status).sort()).toEqual(["ok", "ref"]);
      expect(status.ref).toMatch(/^refs\//);
      expect(typeof status.ok).toBe("boolean");
    }
    expect(contract("native-receive-operation-active-v1.json")).toMatchObject({
      events: [{ sequence: 1, phase: "worker-route-receive-start", at: expect.any(Number) }],
    });
  });
});
