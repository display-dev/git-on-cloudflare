import type { RepoDurableObject } from "@/worker/do/repo/repoDO";

import { getRepoStubByDoId } from "@/worker/common";
import { publishNativeReceiveAuthorityPlan } from "@/worker/git/nativeReceive/authorityPublication";
import { cleanupStockReceiveWorkerDataPlane } from "@/worker/git/nativeReceive/stockDataPlane";
import { isNativeReceiveTerminal } from "@/worker/git/nativeReceive/types";
import type {
  CompleteStockReceiveCleanupResult,
  ConfirmStockReceivePublicationResult,
  RecoverStockReceivePublicationResult,
} from "@/worker/git/nativeReceive/types";
import { createQueueTaskContext, logSoftBudgetExhausted, retryQueueMessage } from "./context";
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
    const countSubrequest = (op: string, count = 1): void =>
      logSoftBudgetExhausted({
        cacheCtx: task.cacheCtx,
        log,
        flagPrefix: "native-receive-soft-budget",
        op,
        count,
      });
    countSubrequest("do:recover-stock-receive-publication");
    const recovery = await task.limiter.run<RecoverStockReceivePublicationResult>(
      "do:recover-stock-receive-publication",
      () => stub.recoverStockReceivePublication(body.operationId)
    );
    let cleanup = recovery.status === "none" ? undefined : recovery.cleanup;
    let includeOutputs = recovery.status === "cleanup_pending" && recovery.includeOutputs;
    if (recovery.status === "publication_pending") {
      const proof = await publishNativeReceiveAuthorityPlan({
        env,
        limiter: task.limiter,
        plan: recovery.publication,
        countSubrequest,
        logger: log,
      });
      countSubrequest("do:confirm-stock-receive-publication");
      const confirmed = await task.limiter.run<ConfirmStockReceivePublicationResult>(
        "do:confirm-stock-receive-publication",
        () => stub.confirmStockReceivePublication(recovery.publicationToken, proof)
      );
      if (confirmed.status === "rejected") {
        throw new Error(`stock-receive:publication-confirm-${confirmed.code}`);
      }
      cleanup = confirmed.cleanup;
      includeOutputs = false;
    }
    if (cleanup) {
      await cleanupStockReceiveWorkerDataPlane({
        env,
        operation: cleanup,
        limiter: task.limiter,
        countSubrequest,
        logger: log,
        includeOutputs,
      });
      countSubrequest("do:complete-stock-receive-cleanup");
      const completed = await task.limiter.run<CompleteStockReceiveCleanupResult>(
        "do:complete-stock-receive-cleanup",
        () => stub.completeStockReceiveCleanup(body.operationId, cleanup.fingerprint)
      );
      if (completed.status !== "complete") {
        throw new Error(`stock-receive:cleanup-state-${completed.code}`);
      }
      message.ack();
      return;
    }
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
