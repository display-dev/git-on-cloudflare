import type { PackCatalogRow } from "../db/schema";

import { getDb, listActivePackCatalog } from "../db";

export async function getActivePackCatalogSnapshot(
  ctx: DurableObjectState
): Promise<PackCatalogRow[]> {
  const db = getDb(ctx.storage);
  return await listActivePackCatalog(db);
}
