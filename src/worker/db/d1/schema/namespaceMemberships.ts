import { index, integer, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { namespaces } from "./namespaces";
import { users } from "./users";

// Any membership row implies owner-level access. Roles are intentionally
// deferred — only the user/namespace edge is recorded today.
export const namespaceMemberships = sqliteTable(
  "namespace_memberships",
  {
    namespaceId: text("namespace_id")
      .notNull()
      .references(() => namespaces.id, { onDelete: "cascade" }),
    userId: text("user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    createdAt: integer("created_at").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.namespaceId, table.userId] }),
    // Reverse-direction index for "my namespaces" / "my repositories" pages.
    // The PK already covers (namespace_id, user_id) lookups.
    index("idx_namespace_memberships_user_ns").on(table.userId, table.namespaceId),
  ]
);

export type NamespaceMembershipRow = typeof namespaceMemberships.$inferSelect;
export type NewNamespaceMembershipRow = typeof namespaceMemberships.$inferInsert;
