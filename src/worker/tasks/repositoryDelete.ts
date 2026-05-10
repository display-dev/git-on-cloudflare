import { type RepoQueueMessageHandle, type RepositoryDeleteMessage } from "./types";

import { getRepoStub } from "@/worker/common";
import { deleteRepositoryById } from "@/worker/db/d1/dal";
import { deleteRouteCacheRecord } from "@/worker/repositories/routeCache";
import { countSubrequest } from "@/worker/git/operations/limits";
import { doPrefix } from "@/worker/keys";
import { createQueueTaskContext, retryQueueMessage } from "./context";

// Repository delete owns D1 row removal, ROUTES KV cleanup, R2 enumeration,
// and DO storage clearing. The request handler only authorizes and
// enqueues; failure of any step retries the message. Keeping D1 deletion
// in the consumer means a queue send failure cannot orphan storage cleanup,
// and replay after partial completion still converges to the empty state.

const DELETE_RETRY_DELAY_SECONDS = 30;
// Bounded per-message work so a very large repo cannot exhaust the
// 1000-subrequest budget; the message is retried with the same body if we
// run out and the next pass picks up where R2 left off.
const DELETE_SUBREQUEST_BUDGET = 800;

export async function handleRepositoryDeleteMessage(
  message: Omit<RepoQueueMessageHandle<RepositoryDeleteMessage>, "body">,
  body: RepositoryDeleteMessage,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const task = createQueueTaskContext({
    env,
    ctx,
    repoLabel: body.repositoryId,
    operation: "delete",
    subrequestBudget: DELETE_SUBREQUEST_BUDGET,
  });
  const log = task.logFor({
    service: "RepositoryDelete",
    repoId: body.doName,
  });
  log.info("repo-delete:start", {
    repositoryId: body.repositoryId,
    namespaceSlug: body.namespaceSlug,
    repoSlug: body.repoSlug,
    actor: body.actor,
    requestedAt: body.requestedAt,
  });

  const { cacheCtx, limiter } = task;

  // Step 1: D1 row delete. Cascade removes repo-scoped PAT grants;
  // namespace-scoped grants are intentionally preserved.
  try {
    const deleted = await deleteRepositoryById(task.db, body.repositoryId);
    if (deleted) {
      log.info("repo-delete:d1-deleted");
    } else {
      log.info("repo-delete:d1-replay-skip");
    }
  } catch (error) {
    log.warn("repo-delete:retry", { step: "d1", error: String(error) });
    retryQueueMessage(message, DELETE_RETRY_DELAY_SECONDS);
    return;
  }

  // Step 2: ROUTES KV cleanup at the captured key. The route-cache-sync
  // consumer is the canonical converger but a repo-delete message captured
  // these slugs at enqueue time, so dropping them here is safe and avoids
  // an extra D1 read.
  try {
    await deleteRouteCacheRecord(env, body.namespaceSlug, body.repoSlug);
    log.info("repo-delete:routes-deleted");
  } catch (error) {
    log.warn("repo-delete:retry", { step: "routes", error: String(error) });
    retryQueueMessage(message, DELETE_RETRY_DELAY_SECONDS);
    return;
  }

  // Step 3: R2 enumerate + delete under do/<doId>/. The DO id is derived
  // synchronously from `doName`; no DO subrequest is consumed here.
  const doId = env.REPO_DO.idFromName(body.doName).toString();
  const prefix = doPrefix(doId);
  let cumulativeR2Deleted = 0;
  try {
    let cursor: string | undefined;
    do {
      if (!countSubrequest(cacheCtx, 1)) {
        log.warn("repo-delete:budget-exhausted", { step: "r2-list" });
        retryQueueMessage(message, DELETE_RETRY_DELAY_SECONDS);
        return;
      }
      const listing = await limiter.run("r2:repo-delete-list", () =>
        env.REPO_BUCKET.list({ prefix, cursor })
      );
      const objects = listing.objects ?? [];
      if (objects.length > 0) {
        if (!countSubrequest(cacheCtx, 1)) {
          log.warn("repo-delete:budget-exhausted", { step: "r2-delete" });
          retryQueueMessage(message, DELETE_RETRY_DELAY_SECONDS);
          return;
        }
        const keys = objects.map((object) => object.key);
        await limiter.run("r2:repo-delete-batch", () => env.REPO_BUCKET.delete(keys));
        cumulativeR2Deleted += keys.length;
        log.info("repo-delete:r2-batch", {
          count: keys.length,
          cumulative: cumulativeR2Deleted,
        });
      }
      cursor = listing.truncated ? listing.cursor : undefined;
    } while (cursor);
  } catch (error) {
    log.warn("repo-delete:retry", { step: "r2", error: String(error) });
    retryQueueMessage(message, DELETE_RETRY_DELAY_SECONDS);
    return;
  }

  // Step 4: clear DO storage + alarm.
  try {
    if (!countSubrequest(cacheCtx, 1)) {
      log.warn("repo-delete:budget-exhausted", { step: "do-clear" });
      retryQueueMessage(message, DELETE_RETRY_DELAY_SECONDS);
      return;
    }
    const stub = getRepoStub(env, body.doName);
    await limiter.run("do:repo-delete-clear-storage", () => stub.clearRepositoryStorage());
    log.info("repo-delete:do-cleared");
  } catch (error) {
    log.warn("repo-delete:retry", { step: "do", error: String(error) });
    retryQueueMessage(message, DELETE_RETRY_DELAY_SECONDS);
    return;
  }

  log.info("repo-delete:end", {
    cumulativeR2Deleted,
  });
  message.ack();
}
