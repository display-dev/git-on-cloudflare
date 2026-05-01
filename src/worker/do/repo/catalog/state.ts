import type { PackCatalogRow } from "../db/schema.ts";

import { getDb, listActivePackCatalog } from "../db/index.ts";

export async function getActivePackCatalogSnapshot(
  ctx: DurableObjectState
): Promise<PackCatalogRow[]> {
  const db = getDb(ctx.storage);
  return await listActivePackCatalog(db);
}
