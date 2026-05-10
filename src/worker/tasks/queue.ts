import { createLogger } from "@/worker/common";

import { handleCompactionDeleteMessage, handleCompactionMessage } from "./compaction";
import { handlePackRefBackfillMessage } from "./refBackfill";
import { handleRouteCacheSyncMessage } from "./routeCacheSync";
import { handleRepositoryDeleteMessage } from "./repositoryDelete";
import { RepoTaskQueueMessageSchema } from "./types";

export type { RepoTaskQueueMessage, RepositoryDeleteMessage, RouteCacheSyncMessage } from "./types";

// The queue carries repo lifecycle work as well as maintenance: compaction,
// pack-ref backfill, route-cache repair, and repository deletion. Producers
// use the `REPO_TASKS_QUEUE` binding; the physical queue name remains
// `git-on-cloudflare-repo-maint` for continuity. Schemas live in
// `./types.ts`; this file dispatches.
export async function handleRepoTaskQueue(
  batch: MessageBatch<unknown>,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const log = createLogger(env.LOG_LEVEL, { service: "RepoTaskQueue" });
  for (const message of batch.messages) {
    const parsed = RepoTaskQueueMessageSchema.safeParse(message.body);
    if (!parsed.success) {
      log.warn("queue:malformed-message", {
        messageId: message.id,
        attempts: message.attempts,
        issues: parsed.error.issues.map((i) => ({ path: i.path.join("."), code: i.code })),
      });
      message.ack();
      continue;
    }
    const body = parsed.data;
    switch (body.kind) {
      case "compaction":
        await handleCompactionMessage(message, body, env, ctx);
        break;
      case "compaction-delete":
        await handleCompactionDeleteMessage(message, body, env, ctx);
        break;
      case "pack-ref-backfill":
        await handlePackRefBackfillMessage(message, body, env, ctx);
        break;
      case "route-cache-sync":
        await handleRouteCacheSyncMessage(message, body, env, ctx);
        break;
      case "repository-delete":
        await handleRepositoryDeleteMessage(message, body, env, ctx);
        break;
      default: {
        const _exhaustive: never = body;
        void _exhaustive;
      }
    }
  }
}
