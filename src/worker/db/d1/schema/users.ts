import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

// Global user identity. `tessera_sub` is the verified `sub` claim from
// tessera; it is opaque, stable, and is the only thing we trust as identity.
// `preferred_username` from the ID token is treated as a slug candidate at
// signup and is not stored here.
export const users = sqliteTable("users", {
  id: text("id").primaryKey(),
  tesseraSub: text("tessera_sub").notNull().unique(),
  createdAt: integer("created_at").notNull(),
});

export type UserRow = typeof users.$inferSelect;
export type NewUserRow = typeof users.$inferInsert;
