import type { CacheContext } from "@/worker/cache";

import { loadActivePackCatalog } from "./catalog";
import { findFirstPackedObjectCandidate, type IndexedPackSource } from "./candidates";
import { loadIdxView } from "./idxView";
import { ensureMemo, getPackedObjectStoreLogger, logOnce, type ResolvedLocation } from "./support";

export async function findObject(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<ResolvedLocation | undefined> {
  ensureMemo(cacheCtx, repoId);
  const log = getPackedObjectStoreLogger(env, repoId);
  const packs = await loadActivePackCatalog(env, repoId, cacheCtx);
  let packsScanned = 0;

  for (const pack of packs) {
    packsScanned++;
    const idx = await loadIdxView(env, pack.packKey, cacheCtx, pack.packBytes);
    if (!idx) continue;

    const source: IndexedPackSource = {
      packKey: pack.packKey,
      packBytes: pack.packBytes,
      idx,
    };
    const candidate = findFirstPackedObjectCandidate([source], oid);
    if (!candidate) continue;

    logOnce(cacheCtx, "packed-chosen-pack-logged", () => {
      log.debug("chosen-pack", {
        oid: oid.toLowerCase(),
        packKey: pack.packKey,
        packsScanned,
      });
    });

    return {
      ...candidate,
      packSlot: packsScanned - 1,
    };
  }

  log.debug("packed-object:miss", { oid: oid.toLowerCase(), packsScanned });
  return undefined;
}
