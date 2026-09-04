import type { DrizzleSqliteDODatabase } from "drizzle-orm/durable-sqlite";
import type { PackCatalogRow } from "../schema";

import { and, desc, eq, inArray, lt, or, sql } from "drizzle-orm";
import { packCatalog } from "../schema";
import { SAFE_ROWS_1COL } from "./shared";

let failNextCatalogReplacementForTesting = false;

export const __test = {
  failNextCatalogReplacement(): void {
    failNextCatalogReplacementForTesting = true;
  },
  reset(): void {
    failNextCatalogReplacementForTesting = false;
  },
};

export async function getPackCatalogCount(db: DrizzleSqliteDODatabase): Promise<number> {
  return await db.$count(packCatalog);
}

/** Count only active (non-superseded) packs — used by idle cleanup to determine repo emptiness. */
export async function getActivePackCatalogCount(db: DrizzleSqliteDODatabase): Promise<number> {
  const rows = await db
    .select({ count: sql<number>`count(*)` })
    .from(packCatalog)
    .where(eq(packCatalog.state, "active"));
  return rows[0]?.count ?? 0;
}

export async function listPackCatalog(db: DrizzleSqliteDODatabase): Promise<PackCatalogRow[]> {
  return await db
    .select()
    .from(packCatalog)
    .orderBy(desc(packCatalog.seqHi), desc(packCatalog.tier));
}

export async function listActivePackCatalog(
  db: DrizzleSqliteDODatabase
): Promise<PackCatalogRow[]> {
  return await db
    .select()
    .from(packCatalog)
    .where(eq(packCatalog.state, "active"))
    .orderBy(desc(packCatalog.seqHi), desc(packCatalog.tier));
}

export async function listSupersededPackCatalog(
  db: DrizzleSqliteDODatabase,
  limit: number,
  cursor?: { seqHi: number; tier: number; packKey: string }
): Promise<PackCatalogRow[]> {
  const afterCursor = cursor
    ? or(
        lt(packCatalog.seqHi, cursor.seqHi),
        and(eq(packCatalog.seqHi, cursor.seqHi), lt(packCatalog.tier, cursor.tier)),
        and(
          eq(packCatalog.seqHi, cursor.seqHi),
          eq(packCatalog.tier, cursor.tier),
          lt(packCatalog.packKey, cursor.packKey)
        )
      )
    : undefined;
  return await db
    .select()
    .from(packCatalog)
    .where(
      afterCursor
        ? and(eq(packCatalog.state, "superseded"), afterCursor)
        : eq(packCatalog.state, "superseded")
    )
    .orderBy(desc(packCatalog.seqHi), desc(packCatalog.tier), desc(packCatalog.packKey))
    .limit(limit);
}

export async function getPackCatalogRow(
  db: DrizzleSqliteDODatabase,
  packKey: string
): Promise<PackCatalogRow | undefined> {
  const rows = await db.select().from(packCatalog).where(eq(packCatalog.packKey, packKey)).limit(1);
  return rows[0];
}

export async function getPackCatalogSeqMax(db: DrizzleSqliteDODatabase): Promise<number> {
  const rows = await db
    .select({
      maxSeqHi: sql<number>`coalesce(max(${packCatalog.seqHi}), 0)`,
    })
    .from(packCatalog);
  return rows[0]?.maxSeqHi || 0;
}

export async function upsertPackCatalogRow(
  db: DrizzleSqliteDODatabase,
  row: PackCatalogRow
): Promise<void> {
  await db
    .insert(packCatalog)
    .values(row)
    .onConflictDoUpdate({
      target: packCatalog.packKey,
      set: {
        kind: row.kind,
        state: row.state,
        tier: row.tier,
        seqLo: row.seqLo,
        seqHi: row.seqHi,
        objectCount: row.objectCount,
        packBytes: row.packBytes,
        idxBytes: row.idxBytes,
        createdAt: row.createdAt,
        supersededBy: row.supersededBy,
      },
    });
}

export async function supersedePackCatalogRows(
  db: DrizzleSqliteDODatabase,
  packKeys: string[],
  supersededBy: string | null = null
): Promise<void> {
  if (!packKeys.length) return;
  for (let i = 0; i < packKeys.length; i += SAFE_ROWS_1COL) {
    const batch = packKeys.slice(i, i + SAFE_ROWS_1COL);
    await db
      .update(packCatalog)
      .set({ state: "superseded", supersededBy })
      .where(inArray(packCatalog.packKey, batch));
  }
}

export async function deletePackCatalogRows(
  db: DrizzleSqliteDODatabase,
  packKeys: string[]
): Promise<void> {
  if (!packKeys.length) return;
  for (let i = 0; i < packKeys.length; i += SAFE_ROWS_1COL) {
    const batch = packKeys.slice(i, i + SAFE_ROWS_1COL);
    await db.delete(packCatalog).where(inArray(packCatalog.packKey, batch));
  }
}

export async function deleteAllPackCatalogRows(db: DrizzleSqliteDODatabase): Promise<void> {
  await db.delete(packCatalog);
}

/** Atomically activate a rewritten pack and supersede its complete source catalog. */
export function replaceActivePackCatalog(args: {
  db: DrizzleSqliteDODatabase;
  sourcePackKeys: string[];
  targetPack?: PackCatalogRow;
}): void {
  args.db.transaction((transaction) => {
    if (args.targetPack) {
      transaction
        .insert(packCatalog)
        .values(args.targetPack)
        .onConflictDoUpdate({
          target: packCatalog.packKey,
          set: {
            kind: args.targetPack.kind,
            state: args.targetPack.state,
            tier: args.targetPack.tier,
            seqLo: args.targetPack.seqLo,
            seqHi: args.targetPack.seqHi,
            objectCount: args.targetPack.objectCount,
            packBytes: args.targetPack.packBytes,
            idxBytes: args.targetPack.idxBytes,
            createdAt: args.targetPack.createdAt,
            supersededBy: args.targetPack.supersededBy,
          },
        })
        .run();
    }
    if (failNextCatalogReplacementForTesting) {
      failNextCatalogReplacementForTesting = false;
      throw new Error("injected reachability GC catalog replacement failure");
    }
    for (let index = 0; index < args.sourcePackKeys.length; index += SAFE_ROWS_1COL) {
      const batch = args.sourcePackKeys.slice(index, index + SAFE_ROWS_1COL);
      transaction
        .update(packCatalog)
        .set({ state: "superseded", supersededBy: args.targetPack?.packKey ?? null })
        .where(inArray(packCatalog.packKey, batch))
        .run();
    }
  });
}
