import type { CacheContext } from "@/worker/cache";
import type { Logger } from "@/worker/common/logger";
import type {
  OrderedPackSnapshot,
  OrderedPackSnapshotEntry,
} from "@/worker/git/operations/fetch/types";

import { loadActivePackCatalog, loadIdxView } from "@/worker/git/object-store";
import { hasPrimedIdxView, parseIdxView, primeIdxView } from "@/worker/git/object-store/idxView";
import { parsePackRefView, primePackRefView } from "@/worker/git/pack/refIndex";
import {
  catalogMetadataBundleEnabled,
  readCatalogMetadataBundle,
} from "@/worker/git/nativeReceive/catalogMetadataBundle";
import { countSubrequest, getLimiter } from "@/worker/git/operations/limits";

export type SnapshotLoadResult =
  | {
      type: "Ready";
      snapshot: OrderedPackSnapshot;
    }
  | {
      type: "RepositoryNotReady";
      reason: "no-active-packs" | "snapshot-missing-idx";
    };

export async function loadOrderedPackSnapshot(
  env: Env,
  repoId: string,
  cacheCtx: CacheContext | undefined,
  log: Logger
): Promise<SnapshotLoadResult> {
  const rows = await loadActivePackCatalog(env, repoId, cacheCtx);
  if (rows.length === 0) {
    return {
      type: "RepositoryNotReady",
      reason: "no-active-packs",
    };
  }

  const metadataCacheMiss = rows.some(
    (row) =>
      cacheCtx?.memo?.idxViews?.has(row.packKey) !== true &&
      !hasPrimedIdxView(row.packKey, row.packBytes)
  );
  if (catalogMetadataBundleEnabled(env) && metadataCacheMiss) {
    const bundle = await readCatalogMetadataBundle({
      env,
      catalog: rows,
      limiter: getLimiter(cacheCtx),
      countSubrequest: (n) => {
        countSubrequest(cacheCtx, n);
      },
      log,
    });
    if (bundle) {
      try {
        for (const entry of bundle.entries) {
          const idx = parseIdxView(entry.packKey, entry.idx, entry.packBytes);
          if (!idx) throw new Error("bundle idx is invalid");
          const refs = parsePackRefView(entry.packKey, entry.refs, idx);
          if (refs.type !== "Ready") throw new Error("bundle refs are invalid");
          primeIdxView(entry.packKey, entry.packBytes, idx);
          primePackRefView(entry.packKey, idx.idxChecksum, refs.view);
        }
        log.info("stream:plan:catalog-metadata-primed", {
          packs: bundle.entries.length,
          bytes: bundle.bytes,
        });
      } catch (error) {
        log.warn("stream:plan:catalog-metadata-prime-failed", { error: String(error) });
      }
    }
  }

  let idxMemoHits = 0;
  let idxLoads = 0;
  let indexedObjects = 0;
  const packs: OrderedPackSnapshotEntry[] = [];

  for (const row of rows) {
    const memoHit = cacheCtx?.memo?.idxViews?.has(row.packKey) === true;
    if (memoHit) idxMemoHits++;

    const idx = await loadIdxView(env, row.packKey, cacheCtx, row.packBytes);
    idxLoads++;
    if (!idx) {
      log.warn("stream:plan:snapshot-missing-idx", { packKey: row.packKey });
      return {
        type: "RepositoryNotReady",
        reason: "snapshot-missing-idx",
      };
    }

    indexedObjects += idx.count;
    packs.push({
      packKey: row.packKey,
      packBytes: row.packBytes,
      idx,
    });
  }

  log.info("stream:plan:snapshot", {
    packs: packs.length,
    idxLoads,
    idxMemoHits,
    idxMemoMisses: idxLoads - idxMemoHits,
    indexedObjects,
  });

  return {
    type: "Ready",
    snapshot: { packs },
  };
}
