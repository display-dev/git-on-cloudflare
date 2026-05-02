import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { users } from "./users";

// A namespace owns repositories and is addressed in URLs as `/:owner` (the
// `:owner` segment equals `slug`). On first sign-in we may create one
// namespace whose slug equals the verified `preferred_username` claim,
// race-protected against concurrent claims via `ON CONFLICT DO NOTHING`.
export const namespaces = sqliteTable("namespaces", {
  id: text("id").primaryKey(),
  slug: text("slug").notNull().unique(),
  createdBy: text("created_by")
    .notNull()
    .references(() => users.id, { onDelete: "restrict" }),
  createdAt: integer("created_at").notNull(),
});

export type NamespaceRow = typeof namespaces.$inferSelect;
export type NewNamespaceRow = typeof namespaces.$inferInsert;
