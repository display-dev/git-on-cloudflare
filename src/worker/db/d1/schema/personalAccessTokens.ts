import { desc } from "drizzle-orm";
import { index, integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { users } from "./users";

// Personal Access Token. The plaintext token has the shape
// `goc_<8 hex prefix>_<32 base32 secret>`. We index on the public prefix to
// look up by the leading bytes presented over Basic auth, then verify the
// SHA-256 hex of the full plaintext against `hash`. Plaintext is shown to
// the user once at creation and never stored.
export const personalAccessTokens = sqliteTable(
  "personal_access_tokens",
  {
    id: text("id").primaryKey(),
    userId: text("user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    name: text("name").notNull(),
    prefix: text("prefix").notNull().unique(),
    hash: text("hash").notNull(),
    createdAt: integer("created_at").notNull(),
    expiresAt: integer("expires_at"),
    revokedAt: integer("revoked_at"),
    lastUsedAt: integer("last_used_at"),
  },
  (table) => [index("idx_pats_user_created").on(table.userId, desc(table.createdAt))]
);

export type PersonalAccessTokenRow = typeof personalAccessTokens.$inferSelect;
export type NewPersonalAccessTokenRow = typeof personalAccessTokens.$inferInsert;
