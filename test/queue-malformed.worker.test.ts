import { describe, expect, it } from "vitest";

import { runQueueMessage } from "./util/queue";

// Dashboard-sent or replayed payloads can drift from the current schema.
// The dispatcher must ack-and-discard malformed bodies rather than retry
// forever or throw on `body.kind` access.
describe("repo task queue: malformed message handling", () => {
  it("null body -> ack, no retry", async () => {
    const result = await runQueueMessage(null);
    expect(result.acked).toBe(true);
    expect(result.retried).toBe(false);
  });

  it("primitive body -> ack, no retry", async () => {
    const result = await runQueueMessage("not-a-message");
    expect(result.acked).toBe(true);
    expect(result.retried).toBe(false);
  });

  it("object with unknown kind -> ack, no retry", async () => {
    const result = await runQueueMessage({ kind: "unknown-kind" });
    expect(result.acked).toBe(true);
    expect(result.retried).toBe(false);
  });

  it("removed import message kind -> ack, no retry", async () => {
    const result = await runQueueMessage({
      kind: "legacy-backfill",
      userId: "user_x",
      namespaceSlug: "ns",
    });
    expect(result.acked).toBe(true);
    expect(result.retried).toBe(false);
  });

  it("known kind with missing required field -> ack, no retry", async () => {
    // `route-cache-sync` requires repositoryId/namespaceSlug/repoSlug/enqueuedAt.
    const result = await runQueueMessage({
      kind: "route-cache-sync",
      repositoryId: "repo_x",
    });
    expect(result.acked).toBe(true);
    expect(result.retried).toBe(false);
  });

  it("known kind with wrong field type -> ack, no retry", async () => {
    const result = await runQueueMessage({
      kind: "repository-delete",
      repositoryId: 42,
      namespaceId: "ns_x",
      namespaceSlug: "ns",
      repoSlug: "site",
      doName: "ns/site",
      actor: "user",
      requestedAt: "now",
    });
    expect(result.acked).toBe(true);
    expect(result.retried).toBe(false);
  });
});
