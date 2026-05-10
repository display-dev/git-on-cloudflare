import { type LegacyBackfillMessage, type RepoQueueMessageHandle } from "./types";

import { validateSlugForRoute } from "@/shared/slugs";
import { createLogger, newPrefixedId } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import {
  findNamespaceBySlug,
  findRepositoryByDoName,
  findUserById,
  insertMembershipIfMissing,
  insertRepositoryIfNew,
  putRouteCacheRecord,
} from "@/worker/db/d1/dal";

// Legacy ownership backfill: when a user signs in for the first time and
// claims a namespace whose slug matches a key in the legacy `OWNER_REGISTRY`
// KV namespace, we replicate those repositories into D1 + ROUTES so the
// new resolver can address them. The handler is intentionally idempotent:
// D1 unique constraints + KV last-write-wins make replays safe.
//
// Invariant: D1 is the source of truth. ROUTES KV must NOT contain
// `visibility`. If any D1 upsert or KV put fails, we retry the message so
// the cursor cannot be lost between pages.

const RETRY_DELAY_SECONDS = 30;
// Conservative cap so a namespace with thousands of legacy repos cannot
// exceed the per-request subrequest budget in a single backfill message.
const BACKFILL_PAGE_LIMIT = 50;

function legacyKeyPrefix(namespaceSlug: string): string {
  return `owner:${namespaceSlug}:`;
}

function legacyDoName(namespaceSlug: string, repoSlug: string): string {
  return `${namespaceSlug}/${repoSlug}`;
}

export async function handleLegacyBackfillMessage(
  message: Omit<RepoQueueMessageHandle<LegacyBackfillMessage>, "body">,
  body: LegacyBackfillMessage,
  env: Env,
  _ctx: ExecutionContext
): Promise<void> {
  const log = createLogger(env.LOG_LEVEL, { service: "LegacyBackfill" });
  const db = createDb(env.DB);

  const user = await findUserById(db, body.userId);
  if (!user) {
    // Ack: the user has been removed; replays cannot recover.
    log.warn("backfill:user-missing", { userId: body.userId });
    message.ack();
    return;
  }
  const namespace = await findNamespaceBySlug(db, body.namespaceSlug);
  if (!namespace) {
    log.warn("backfill:namespace-missing", { slug: body.namespaceSlug });
    message.ack();
    return;
  }
  // Ensure membership exists. The sign-in handler already created it for
  // the originating callback, but a replay against a re-issued queue
  // message must still arrive at the same state.
  await insertMembershipIfMissing(db, {
    namespaceId: namespace.id,
    userId: user.id,
    createdAt: Date.now(),
  });

  let listResult: KVNamespaceListResult<unknown, string>;
  try {
    listResult = await env.OWNER_REGISTRY.list({
      prefix: legacyKeyPrefix(body.namespaceSlug),
      cursor: body.cursor,
      // Bound per-page work so a busy namespace cannot blow past the Worker
      // 1000-subrequest budget. Each entry produces one D1 upsert (which
      // does not count) plus one ROUTES.put (which does). Continuation is
      // re-enqueued via the existing cursor flow.
      limit: BACKFILL_PAGE_LIMIT,
    });
  } catch (error) {
    log.warn("backfill:kv-list-failed", { error: String(error) });
    message.retry({ delaySeconds: RETRY_DELAY_SECONDS });
    return;
  }

  const prefix = legacyKeyPrefix(body.namespaceSlug);
  for (const entry of listResult.keys) {
    const rawSuffix = entry.name.startsWith(prefix) ? entry.name.slice(prefix.length) : "";
    if (!rawSuffix) continue;
    // The legacy OWNER_REGISTRY policy was permissive (uppercase, dots,
    // underscores, length up to 100). The shared slug validator is strict
    // (lowercase ASCII, length 1-40, reserved names rejected). Backfill
    // must not seed authoritative route metadata for slugs the new
    // resolver cannot address. Validate the raw suffix as-is — do not
    // lowercase, because that would silently collide with sibling repos
    // that already use the lower-cased form. Skip + log so the operator
    // can decide whether to rename in the legacy store before retrying.
    const validation = validateSlugForRoute(rawSuffix);
    if (!validation.ok) {
      log.warn("backfill:invalid-suffix-skipped", {
        userId: user.id,
        slug: body.namespaceSlug,
        rawSuffix,
        reason: validation.reason,
      });
      continue;
    }
    const repoSlug = validation.slug;
    const doName = legacyDoName(body.namespaceSlug, repoSlug);
    const now = Date.now();
    let repositoryId: string;
    try {
      const inserted = await insertRepositoryIfNew(db, {
        id: newPrefixedId("repo"),
        namespaceId: namespace.id,
        createdBy: user.id,
        slug: repoSlug,
        doName,
        visibility: "public",
        createdAt: now,
        updatedAt: now,
      });
      if (inserted) {
        repositoryId = inserted.id;
      } else {
        // Already exists; reuse existing id by reading back. We must set
        // ROUTES KV against the canonical id, not the speculative one above.
        const found = await findRepositoryByDoName(db, doName);
        if (!found) {
          throw new Error(`legacy repository ${doName} disappeared mid-backfill`);
        }
        repositoryId = found.id;
      }
    } catch (error) {
      log.warn("backfill:d1-upsert-failed", {
        userId: user.id,
        slug: body.namespaceSlug,
        repoSlug,
        error: String(error),
      });
      message.retry({ delaySeconds: RETRY_DELAY_SECONDS });
      return;
    }
    try {
      await putRouteCacheRecord(env, body.namespaceSlug, repoSlug, {
        repositoryId,
        namespaceId: namespace.id,
        doName,
        updatedAt: now,
      });
    } catch (error) {
      log.warn("backfill:route-cache-put-failed", {
        userId: user.id,
        slug: body.namespaceSlug,
        repoSlug,
        repositoryId,
        error: String(error),
      });
      message.retry({ delaySeconds: RETRY_DELAY_SECONDS });
      return;
    }
  }

  if (!listResult.list_complete) {
    const continuation: LegacyBackfillMessage = {
      kind: "legacy-backfill",
      userId: body.userId,
      namespaceSlug: body.namespaceSlug,
      cursor: listResult.cursor,
    };
    try {
      await env.REPO_TASKS_QUEUE.send(continuation);
    } catch (error) {
      log.warn("backfill:continuation-enqueue-failed", {
        userId: user.id,
        slug: body.namespaceSlug,
        error: String(error),
      });
      message.retry({ delaySeconds: RETRY_DELAY_SECONDS });
      return;
    }
  }

  log.info("backfill:page-acked", {
    userId: user.id,
    slug: body.namespaceSlug,
    items: listResult.keys.length,
    moreToCome: !listResult.list_complete,
  });
  message.ack();
}
