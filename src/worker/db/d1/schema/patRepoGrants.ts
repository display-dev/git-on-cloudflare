import { sql } from "drizzle-orm";
import { check, index, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { type PatGrantLevel } from "./patNamespaceGrants";
import { personalAccessTokens } from "./personalAccessTokens";
import { repositories } from "./repositories";

// PAT scoped to a single repository. Reuses `PatGrantLevel` from the
// namespace grant module so both grant tables stay on the same canonical
// type and CHECK shape.
export const patRepoGrants = sqliteTable(
  "pat_repo_grants",
  {
    patId: text("pat_id")
      .notNull()
      .references(() => personalAccessTokens.id, { onDelete: "cascade" }),
    repoId: text("repo_id")
      .notNull()
      .references(() => repositories.id, { onDelete: "cascade" }),
    level: text("level").$type<PatGrantLevel>().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.patId, table.repoId] }),
    index("idx_pat_repo_grants_repo").on(table.repoId),
    check("chk_pat_repo_grants_level", sql`"level" IN ('pull','push')`),
  ]
);

export type PatRepoGrantRow = typeof patRepoGrants.$inferSelect;
export type NewPatRepoGrantRow = typeof patRepoGrants.$inferInsert;
