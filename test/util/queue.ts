import { createExecutionContext } from "cloudflare:test";
import { env as testEnv } from "cloudflare:workers";
import { handleRepoTaskQueue, type RepoTaskQueueMessage } from "@/worker/tasks/queue";

export type QueueRunResult = {
  acked: boolean;
  retried: boolean;
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
 * pass `overrideEnv` for tests that stub bindings.
 */
export async function runQueueMessage(
  body: RepoTaskQueueMessage,
  overrideEnv?: Env
): Promise<QueueRunResult> {
  let acked = false;
  let retried = false;
  const batch: MessageBatch<RepoTaskQueueMessage> = {
    queue: "git-on-cloudflare-repo-maint",
    metadata: createMessageBatchMetadata(),
    messages: [
      {
        id: "queue-1",
        timestamp: new Date(),
        body,
        attempts: 1,
        retry() {
          retried = true;
        },
        ack() {
          acked = true;
        },
      },
    ],
    retryAll() {},
    ackAll() {},
  };

  await handleRepoTaskQueue(batch, overrideEnv ?? testEnv, createExecutionContext());
  return { acked, retried };
}
