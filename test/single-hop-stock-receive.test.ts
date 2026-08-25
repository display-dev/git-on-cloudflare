import { describe, expect, it } from "vitest";

import type { Logger } from "@/worker/common/logger";
import {
  executeStockReceiveSingleHop,
  STOCK_RECEIVE_SINGLE_HOP_SHAPE,
  type StockReceiveSingleHopAuthority,
} from "@/worker/git/nativeReceive/singleHop";
import { SubrequestLimiter } from "@/worker/git/operations/limits";

type Request = { operationId: string };
type Work = { inputKey: string };
type Prepared = { outputDigest: string };
type Publication = { receiptKey: string };
type Proof = { receiptDigest: string };
type Committed = { disposition: "committed" };

const logger: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
};

describe("stock receive single-hop seam", () => {
  it("keeps RepoDO state-only and orders Worker data-plane publication after exact-old finalize", async () => {
    const calls: string[] = [];
    const authority: StockReceiveSingleHopAuthority<
      Request,
      Work,
      Prepared,
      Publication,
      Proof,
      Committed
    > = {
      async admit() {
        calls.push("repo-do:admit");
        return { status: "admitted", executionToken: "execution-1", work: { inputKey: "input" } };
      },
      async finalize(token, prepared) {
        calls.push(`repo-do:finalize:${token}:${prepared.outputDigest}`);
        return {
          status: "publication_pending",
          publicationToken: "publication-1",
          publication: { receiptKey: "receipt" },
        };
      },
      async confirmPublication(token, proof) {
        calls.push(`repo-do:confirm:${token}:${proof.receiptDigest}`);
        return { status: "committed", committed: { disposition: "committed" } };
      },
    };

    const result = await executeStockReceiveSingleHop({
      request: { operationId: "operation-1" },
      authority,
      dataPlane: {
        async execute(work) {
          calls.push(`worker:native-r2:${work.inputKey}`);
          return { outputDigest: "output" };
        },
        async publish(publication) {
          calls.push(`worker:r2-publish:${publication.receiptKey}`);
          return { receiptDigest: "receipt-digest" };
        },
        async cleanup() {
          calls.push("worker:r2-cleanup");
        },
      },
      limiter: new SubrequestLimiter(6),
      logger,
    });

    expect(result).toEqual({ status: "committed", committed: { disposition: "committed" } });
    expect(calls).toEqual([
      "repo-do:admit",
      "worker:native-r2:input",
      "repo-do:finalize:execution-1:output",
      "worker:r2-publish:receipt",
      "repo-do:confirm:publication-1:receipt-digest",
      "worker:r2-cleanup",
    ]);
    expect(STOCK_RECEIVE_SINGLE_HOP_SHAPE).toEqual({
      exactOldOwner: "repo-do",
      workerTargets: ["repo-do", "native-data-plane", "r2"],
      repoDoTargets: [],
      taggedAdmission: true,
      taggedFinalize: true,
    });
  });

  it("resumes an admitted exact-old commit at Worker-owned R2 publication", async () => {
    const calls: string[] = [];
    const result = await executeStockReceiveSingleHop({
      request: { operationId: "operation-1" },
      authority: {
        async admit() {
          calls.push("repo-do:admit");
          return {
            status: "publication_pending" as const,
            publicationToken: "publication-1",
            publication: { receiptKey: "receipt" },
          };
        },
        async finalize() {
          throw new Error("finalize must not repeat");
        },
        async confirmPublication() {
          calls.push("repo-do:confirm");
          return { status: "committed" as const, committed: { disposition: "committed" as const } };
        },
      },
      dataPlane: {
        async execute() {
          throw new Error("native processing must not repeat");
        },
        async publish() {
          calls.push("worker:r2-publish");
          return { receiptDigest: "receipt-digest" };
        },
        async cleanup() {
          calls.push("worker:r2-cleanup");
        },
      },
      limiter: new SubrequestLimiter(6),
      logger,
    });

    expect(result.status).toBe("committed");
    expect(calls).toEqual([
      "repo-do:admit",
      "worker:r2-publish",
      "repo-do:confirm",
      "worker:r2-cleanup",
    ]);
  });
});
