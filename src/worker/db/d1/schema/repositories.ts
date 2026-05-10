import { sql, desc } from "drizzle-orm";
import { check, index, integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";

import { namespaces } from "./namespaces";
import { users } from "./users";

export type RepositoryVisibility = "public" | "private";

// A repository row identifies a repo by `(namespace_id, slug)` and binds
// it to a Durable Object via `do_name`. `do_name` is the value passed to
// `env.REPO_DO.idFromName()`. Legacy/imported rows use the historical
// `<namespace>/<repo>` form so that existing storage stays addressable;
// new repos created post-migration use a `repo:<uuid>` form.
export const repositories = sqliteTable(
  "repositories",
  {
    id: text("id").primaryKey(),
    namespaceId: text("namespace_id")
      .notNull()
      .references(() => namespaces.id, { onDelete: "cascade" }),
    createdBy: text("created_by")
      .notNull()
      .references(() => users.id, { onDelete: "restrict" }),
    slug: text("slug").notNull(),
    doName: text("do_name").notNull(),
    visibility: text("visibility").notNull().$type<RepositoryVisibility>(),
    createdAt: integer("created_at").notNull(),
    updatedAt: integer("updated_at").notNull(),
  },
  (table) => [
    uniqueIndex("uq_repositories_namespace_slug").on(table.namespaceId, table.slug),
    uniqueIndex("uq_repositories_do_name").on(table.doName),
    // Owner page / namespace listing without scanning the namespace.
    index("idx_repositories_namespace_updated").on(
      table.namespaceId,
      desc(table.updatedAt),
      table.slug
    ),
    check("chk_repositories_visibility", sql`"visibility" IN ('public','private')`),
  ]
);

export type RepositoryRow = typeof repositories.$inferSelect;
export type NewRepositoryRow = typeof repositories.$inferInsert;
