/**
 * Pack operations for repository maintenance
 *
 * This module provides operations for managing packs and clearing
 * Durable Object storage. R2 enumeration during a full repo delete is
 * handled by the `repository-delete` queue consumer to keep the
 * Worker -> DO -> R2 boundary clean.
 */

import type { Logger } from "@/worker/common/logger";

import { createLogger } from "@/worker/common";
import { MAX_SIMULTANEOUS_CONNECTIONS, SubrequestLimiter } from "@/worker/git/operations/limits";
import { doPrefix, packIndexKey, packRefsKey } from "@/worker/keys";
import { deletePackCatalogRows, getDb, getPackCatalogCount, getPackCatalogRow } from "./db";
import { getActivePackCatalogSnapshot } from "./catalog";

export type RemovePackResult = {
  removed: boolean;
  deletedPack: boolean;
  deletedIndex: boolean;
  deletedRefs: boolean;
  deletedMetadata: boolean;
  rejected?: "active-pack" | "non-superseded-pack";
  packState?: "active" | "superseded" | "unknown";
};

async function deletePackArtifact(args: {
  bucket: R2Bucket;
  limiter: SubrequestLimiter;
  key: string;
  op: string;
  log: Logger;
  deletedMessage: string;
  failedMessage: string;
}): Promise<boolean> {
  try {
    await args.limiter.run(args.op, async () => {
      await args.bucket.delete(args.key);
    });
    args.log.info(args.deletedMessage, { key: args.key, op: args.op });
    return true;
  } catch (error) {
    args.log.error(args.failedMessage, {
      key: args.key,
      op: args.op,
      error: String(error),
    });
    return false;
  }
}

/**
 * Remove a specific pack file and its associated data
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param packKey - The pack key to remove (can be either short name or full R2 key)
 * @returns Object with removal statistics
 */
export async function removePack(
  ctx: DurableObjectState,
  env: Env,
  packKey: string
): Promise<RemovePackResult> {
  const log = createLogger(env.LOG_LEVEL, {
    service: "packOperations:removePack",
    doId: ctx.id.toString(),
  });
  const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);

  const result: RemovePackResult = {
    removed: false,
    deletedPack: false,
    deletedIndex: false,
    deletedRefs: false,
    deletedMetadata: false,
  };

  try {
    const prefix = doPrefix(ctx.id.toString());
    let fullPackKey = packKey;
    const db = getDb(ctx.storage);

    if (!packKey.startsWith(prefix)) {
      fullPackKey = `${prefix}/objects/pack/${packKey}`;
    }

    if ((await getPackCatalogCount(db)) === 0) {
      await getActivePackCatalogSnapshot(ctx);
    }

    const packCatalogRow = await getPackCatalogRow(db, fullPackKey);
    const packState: "active" | "superseded" | "unknown" =
      packCatalogRow?.state === "active"
        ? "active"
        : packCatalogRow?.state === "superseded"
          ? "superseded"
          : "unknown";
    result.packState = packState;
    if (packState !== "superseded") {
      const rejected = packState === "active" ? "active-pack" : "non-superseded-pack";
      log.warn("reject-pack-delete", {
        packKey: fullPackKey,
        packState,
        rejected,
      });
      return {
        ...result,
        rejected,
      };
    }

    log.info("removing-pack", { packKey: fullPackKey });

    result.deletedPack = await deletePackArtifact({
      bucket: env.REPO_BUCKET,
      limiter,
      key: fullPackKey,
      op: "r2:delete-pack",
      log,
      deletedMessage: "deleted-pack-file",
      failedMessage: "failed-to-delete-pack",
    });

    const indexKey = packIndexKey(fullPackKey);
    result.deletedIndex = await deletePackArtifact({
      bucket: env.REPO_BUCKET,
      limiter,
      key: indexKey,
      op: "r2:delete-pack-idx",
      log,
      deletedMessage: "deleted-index-file",
      failedMessage: "failed-to-delete-index",
    });

    const refsKey = packRefsKey(fullPackKey);
    result.deletedRefs = await deletePackArtifact({
      bucket: env.REPO_BUCKET,
      limiter,
      key: refsKey,
      op: "r2:delete-pack-refs",
      log,
      deletedMessage: "deleted-ref-index-file",
      failedMessage: "failed-to-delete-ref-index",
    });

    // Remove from pack catalog metadata
    await deletePackCatalogRows(db, [fullPackKey]);
    result.deletedMetadata = true;

    result.removed =
      result.deletedPack || result.deletedIndex || result.deletedRefs || result.deletedMetadata;

    log.info("pack-removal-complete", result);
  } catch (e) {
    log.error("pack-removal-error", { packKey, error: String(e) });
    throw e;
  }

  return result;
}

/**
 * Clears the per-repo Durable Object storage and any pending alarm.
 * R2 cleanup is owned by the `repository-delete` queue consumer so we keep
 * the DO call free of cross-runtime hops (no Worker -> DO -> R2 chain).
 */
export async function clearRepositoryStorage(
  ctx: DurableObjectState,
  env: Env
): Promise<{ deletedDO: boolean }> {
  const log = createLogger(env.LOG_LEVEL, {
    service: "packOperations:clearRepositoryStorage",
    doId: ctx.id.toString(),
  });

  // The 2026-05-13 compatibility date includes `delete_all_deletes_alarm`,
  // so this also clears any pending alarm for the deleted repository.
  await ctx.storage.deleteAll();
  log.info("clear:storage-deleted-all");

  return { deletedDO: true };
}
