import { type D1Migration } from "cloudflare:test";

import journal from "../../drizzle/d1/meta/_journal.json";

// Drizzle's `meta/_journal.json` lists migrations in tag order; each `tag`
// matches a sibling `<tag>.sql` file. We load every SQL file at vitest
// import time (`eager: true`) so workerd does not need filesystem access.
const sqlFiles = import.meta.glob("../../drizzle/d1/*.sql", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

interface JournalEntry {
  tag: string;
}

interface MigrationJournal {
  entries: JournalEntry[];
}

function splitStatements(sql: string): string[] {
  return sql
    .split("--> statement-breakpoint")
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);
}

export function readAppD1Migrations(): D1Migration[] {
  const typedJournal = journal as MigrationJournal;
  return typedJournal.entries.map((entry) => {
    const fileName = `${entry.tag}.sql`;
    const sql = sqlFiles[`../../drizzle/d1/${fileName}`];
    if (!sql) {
      throw new Error(`Missing D1 migration file for ${fileName}.`);
    }
    return { name: fileName, queries: splitStatements(sql) };
  });
}
