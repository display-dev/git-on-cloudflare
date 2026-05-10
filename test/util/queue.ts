import { createExecutionContext, createMessageBatch, getQueueResult } from "cloudflare:test";
import { env as testEnv } from "cloudflare:workers";
import worker from "@/worker/index";

export type QueueRunResult = {
  acked: boolean;
  retried: boolean;
};

// `getQueueResult()` returns more state than most tests need. Keep the
// asserted subset named here so the helper stays typed without reaching for
// casts when the generated Worker types do not expose `FetcherQueueResult`.
type QueueResultState = {
  ackAll: boolean;
  explicitAcks: string[];
  retryBatch: {
    retry: boolean;
  };
  retryMessages: Array<{
    msgId: string;
  }>;
};

function createQueueMetrics(): MessageBatchMetrics {
  return {
    backlogBytes: 0,
    backlogCount: 0,
  };
}

function createMessageBatchMetadata(): MessageBatchMetadata {
  return {
    metrics: createQueueMetrics(),
  };
}

/**
 * Wrangler's Queue test/runtime types include delivery metadata on both
 * batches and send responses. Tests that stub queue behavior do not care
 * about those live backlog values, but they still need to provide the same
 * shape so mocked bindings stay honest with the Worker API.
 */
export function createQueueSendResponse(): QueueSendResponse {
  return {
    metadata: createMessageBatchMetadata(),
  };
}

/**
 * Run a single repo task queue message through the handler and return
 * whether it was acked or retried. Uses the real test `env` by default;
 * pass `overrideEnv` for tests that stub bindings. The Cloudflare Queue
 * helpers own ack/retry tracking and wait for queue `ctx.waitUntil()` work.
 */
export async function runQueueMessage(body: unknown, overrideEnv?: Env): Promise<QueueRunResult> {
  const messageId = "queue-1";
  const messages = [
    {
      id: messageId,
      timestamp: new Date(),
      attempts: 1,
      body,
    },
  ];
  const batch = createMessageBatch("git-on-cloudflare-repo-maint", messages);

  const ctx = createExecutionContext();
  await worker.queue(batch, overrideEnv ?? testEnv, ctx);
  const result: QueueResultState = await getQueueResult(batch, ctx);

  return {
    acked: result.ackAll || result.explicitAcks.includes(messageId),
    retried:
      result.retryBatch.retry ||
      result.retryMessages.some((message) => message.msgId === messageId),
  };
}
