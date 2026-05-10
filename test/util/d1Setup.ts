import { applyD1Migrations } from "cloudflare:test";

import { readAppD1Migrations } from "./d1Migrations";

// Phase 2 worker tests require D1 rows for repo-serving routes (the
// resolver returns null when D1 is empty). `applyD1Migrations` is itself
// idempotent on the journal but the workerd cost is non-trivial; cache the
// "applied" state at the worker pool level so each test file only pays
// once.
let applied = false;

export async function ensureD1Migrations(env: Env): Promise<void> {
  if (applied) return;
  await applyD1Migrations(env.DB, readAppD1Migrations());
  applied = true;
}

// Test-only escape hatch when an isolated test wants to re-apply migrations
// (e.g. after dropping tables manually). Production code never calls this.
export function __resetD1MigrationCache(): void {
  applied = false;
}
