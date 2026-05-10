import { type RepoQueueMessageHandle, type RouteCacheSyncMessage } from "./types";

import { createLogger } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import {
  deleteRouteCacheRecord,
  findNamespaceById,
  findRepositoryById,
  putRouteCacheRecord,
  routeCacheKey,
  type RouteCacheRecord,
} from "@/worker/db/d1/dal";

// State-converging route-cache repair.
//
// The consumer reads D1 by `repositoryId` at execution time and reconciles
// ROUTES KV against the canonical row, regardless of what the message body
// says. That makes delivery order, replays, and stale captured slugs all
// safe: the queue body is read-only metadata, and D1 is the source of truth.
//
// Captured slugs (`namespaceSlug`, `repoSlug`) only matter when the row's
// canonical (namespace, slug) has shifted - either because the row is gone,
// or because a future rename will land. In that case we delete the captured
// key in addition to the canonical action.

const SYNC_RETRY_DELAY_SECONDS = 30;

export async function handleRouteCacheSyncMessage(
  message: Omit<RepoQueueMessageHandle<RouteCacheSyncMessage>, "body">,
  body: RouteCacheSyncMessage,
  env: Env,
  _ctx: ExecutionContext
): Promise<void> {
  const log = createLogger(env.LOG_LEVEL, {
    service: "RouteCacheSync",
  });
  log.debug("route-sync:start", {
    repositoryId: body.repositoryId,
    namespaceSlug: body.namespaceSlug,
    repoSlug: body.repoSlug,
    enqueuedAt: body.enqueuedAt,
  });

  try {
    const db = createDb(env.DB);
    const repository = await findRepositoryById(db, body.repositoryId);

    // D1 row missing: the repo was deleted (or never existed). Drop the
    // captured key. We have no canonical (namespace, slug) to address, but
    // the captured pair is what the request that enqueued this message
    // observed, so deleting it is what the operator expects to converge.
    if (!repository) {
      await deleteRouteCacheRecord(env, body.namespaceSlug, body.repoSlug);
      log.info("route-sync:end", { action: "missing-d1-delete-captured" });
      message.ack();
      return;
    }

    const namespace = await findNamespaceById(db, repository.namespaceId);
    if (!namespace) {
      // Defensive: a repository row pointing to a missing namespace is
      // structurally inconsistent. Drop the captured key and ack so we
      // don't retry forever.
      await deleteRouteCacheRecord(env, body.namespaceSlug, body.repoSlug);
      log.warn("route-sync:end", {
        action: "missing-namespace-delete-captured",
        repositoryNamespaceId: repository.namespaceId,
      });
      message.ack();
      return;
    }

    const canonicalNamespaceSlug = namespace.slug;
    const canonicalRepoSlug = repository.slug;
    const capturedKey = routeCacheKey(body.namespaceSlug, body.repoSlug);
    const canonicalKey = routeCacheKey(canonicalNamespaceSlug, canonicalRepoSlug);
    const capturedDiffersFromCanonical = capturedKey !== canonicalKey;

    if (repository.visibility === "private") {
      // Private rows must never expose a public route candidate.
      if (capturedDiffersFromCanonical) {
        await deleteRouteCacheRecord(env, body.namespaceSlug, body.repoSlug);
      }
      await deleteRouteCacheRecord(env, canonicalNamespaceSlug, canonicalRepoSlug);
      log.info("route-sync:end", {
        action: "private-delete",
        canonicalNamespaceSlug,
        canonicalRepoSlug,
        deletedCaptured: capturedDiffersFromCanonical,
      });
      message.ack();
      return;
    }

    // visibility === "public"
    if (capturedDiffersFromCanonical) {
      await deleteRouteCacheRecord(env, body.namespaceSlug, body.repoSlug);
    }
    const record: RouteCacheRecord = {
      repositoryId: repository.id,
      namespaceId: repository.namespaceId,
      doName: repository.doName,
      updatedAt: repository.updatedAt,
    };
    await putRouteCacheRecord(env, canonicalNamespaceSlug, canonicalRepoSlug, record);
    log.info("route-sync:end", {
      action: "public-put",
      canonicalNamespaceSlug,
      canonicalRepoSlug,
      deletedCaptured: capturedDiffersFromCanonical,
    });
    message.ack();
  } catch (error) {
    log.warn("route-sync:retry", { error: String(error) });
    message.retry({ delaySeconds: SYNC_RETRY_DELAY_SECONDS });
  }
}
