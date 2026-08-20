import type { RepoDurableObject } from "@/worker/do/repo/repoDO";

import { getRepoStubByDoId } from "@/worker/common";
import { isNativeReceiveTerminal } from "@/worker/git/nativeReceive/types";
import { createQueueTaskContext, retryQueueMessage } from "./context";
import type { NativeReceiveQueueMessage, RepoQueueMessageHandle } from "./types";

const NATIVE_RECEIVE_RETRY_DELAY_SECONDS = 5;

export async function handleNativeReceiveMessage(
  message: Omit<RepoQueueMessageHandle<NativeReceiveQueueMessage>, "body">,
  body: NativeReceiveQueueMessage,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const task = createQueueTaskContext({
    env,
    ctx,
    repoLabel: "repository",
    operation: "native-receive",
    subrequestBudget: 100,
  });
  const log = task.logFor({
    service: "NativeReceiveQueue",
  });
  const stub = getRepoStubByDoId(env, body.doId) as DurableObjectStub<RepoDurableObject>;

  try {
    const operation = await task.limiter.run("do:run-native-receive", () =>
      stub.runNativeReceiveOperation(body.operationId)
    );
    if (!operation || isNativeReceiveTerminal(operation.state)) {
      log.info("native-receive:queue-complete", {
        operationId: body.operationId,
        state: operation?.state ?? "missing",
      });
      message.ack();
      return;
    }
    retryQueueMessage(message, NATIVE_RECEIVE_RETRY_DELAY_SECONDS);
  } catch (error) {
    log.warn("native-receive:queue-retry", {
      operationId: body.operationId,
      error: String(error),
    });
    retryQueueMessage(message, NATIVE_RECEIVE_RETRY_DELAY_SECONDS);
  }
}
