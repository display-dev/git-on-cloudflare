import { describe, expect, it, vi } from "vitest";

import { acceptedWriteFactsForCommands, emitAcceptedWriteFacts } from "@/worker/git/acceptedWrite";
import type { Logger } from "@/worker/common/logger";

describe("accepted-write facts", () => {
  it("uses one canonical shape for protocol and ingestion emissions", () => {
    const info = vi.fn();
    const logger: Logger = {
      debug: vi.fn(),
      info,
      warn: vi.fn(),
      error: vi.fn(),
    };
    const commands = [
      {
        oldOid: "0".repeat(40),
        newOid: "a".repeat(40),
        ref: "refs/heads/main",
      },
    ];

    const ingestion = acceptedWriteFactsForCommands({
      repositoryId: "repo_1",
      commands,
      actor: "user_1",
      sourceSurface: "ingestion",
      idempotencyKey: "request_1",
    });
    const protocol = acceptedWriteFactsForCommands({
      repositoryId: "repo_1",
      commands,
      actor: "user_1",
      sourceSurface: "git-push",
      idempotencyKey: null,
    });

    expect(protocol[0]).toEqual({
      ...ingestion[0],
      sourceSurface: "git-push",
      idempotencyKey: null,
    });
    emitAcceptedWriteFacts(logger, [...ingestion, ...protocol]);
    expect(info).toHaveBeenNthCalledWith(1, "accepted-write:emitted", {
      sourceSurface: "ingestion",
      idempotent: true,
    });
    expect(info).toHaveBeenNthCalledWith(2, "accepted-write:emitted", {
      sourceSurface: "git-push",
      idempotent: false,
    });
    expect(JSON.stringify(info.mock.calls)).not.toContain("repo_1");
    expect(JSON.stringify(info.mock.calls)).not.toContain("request_1");
  });

  it("emits only the authoritative transition for repeated refs and drops no-ops", () => {
    const facts = acceptedWriteFactsForCommands({
      repositoryId: "repo_1",
      commands: [
        { oldOid: "a".repeat(40), newOid: "b".repeat(40), ref: "refs/heads/main" },
        { oldOid: "a".repeat(40), newOid: "c".repeat(40), ref: "refs/heads/main" },
        { oldOid: "d".repeat(40), newOid: "d".repeat(40), ref: "refs/heads/no-op" },
      ],
      actor: "user_1",
      sourceSurface: "git-push",
      idempotencyKey: null,
    });

    expect(facts).toEqual([
      {
        repositoryId: "repo_1",
        ref: "refs/heads/main",
        beforeSha: "a".repeat(40),
        afterSha: "c".repeat(40),
        actor: "user_1",
        sourceSurface: "git-push",
        idempotencyKey: null,
      },
    ]);
  });
});
