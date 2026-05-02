import { sql } from "drizzle-orm";
import { check, index, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { namespaces } from "./namespaces";
import { personalAccessTokens } from "./personalAccessTokens";

// Permission level granted by a PAT to a namespace or repository. `push`
// includes everything `pull` allows; absence of a grant row encodes
// no-access. The DB CHECK constraint is the structural guard so callers
// (DAL, verifier, API, UI) can trust the value without defensive code.
export type PatGrantLevel = "pull" | "push";

// PAT scoped to an entire namespace.
export const patNamespaceGrants = sqliteTable(
  "pat_namespace_grants",
  {
    patId: text("pat_id")
      .notNull()
      .references(() => personalAccessTokens.id, { onDelete: "cascade" }),
    namespaceId: text("namespace_id")
      .notNull()
      .references(() => namespaces.id, { onDelete: "cascade" }),
    level: text("level").$type<PatGrantLevel>().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.patId, table.namespaceId] }),
    index("idx_pat_namespace_grants_namespace").on(table.namespaceId),
    check("chk_pat_namespace_grants_level", sql`"level" IN ('pull','push')`),
  ]
);

export type PatNamespaceGrantRow = typeof patNamespaceGrants.$inferSelect;
export type NewPatNamespaceGrantRow = typeof patNamespaceGrants.$inferInsert;
