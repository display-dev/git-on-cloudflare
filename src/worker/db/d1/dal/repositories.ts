import { and, desc, eq } from "drizzle-orm";

import type { Db } from "@/worker/db/d1/client";
import { namespaceMemberships } from "@/worker/db/d1/schema/namespaceMemberships";
import { namespaces } from "@/worker/db/d1/schema/namespaces";
import {
  type NewRepositoryRow,
  type RepositoryRow,
  repositories,
} from "@/worker/db/d1/schema/repositories";

export async function findRepositoryById(
  db: Db,
  repositoryId: string
): Promise<RepositoryRow | undefined> {
  const rows = await db
    .select()
    .from(repositories)
    .where(eq(repositories.id, repositoryId))
    .limit(1);
  return rows[0];
}

export async function findRepositoryByDoName(
  db: Db,
  doName: string
): Promise<RepositoryRow | undefined> {
  const rows = await db.select().from(repositories).where(eq(repositories.doName, doName)).limit(1);
  return rows[0];
}

export async function findRepositoryByNamespaceAndSlug(
  db: Db,
  namespaceId: string,
  slug: string
): Promise<RepositoryRow | undefined> {
  const rows = await db
    .select()
    .from(repositories)
    .where(and(eq(repositories.namespaceId, namespaceId), eq(repositories.slug, slug)))
    .limit(1);
  return rows[0];
}

// Used by the legacy-backfill queue handler and (later) the repo-create
// route. The conflict path keeps existing rows untouched, so queue replays
// and re-runs are idempotent.
export async function insertRepositoryIfNew(
  db: Db,
  row: NewRepositoryRow
): Promise<RepositoryRow | undefined> {
  const inserted = await db.insert(repositories).values(row).onConflictDoNothing().returning();
  return inserted[0];
}

export type RepositoryListing = {
  repository: RepositoryRow;
  namespace: { id: string; slug: string };
};

// All repositories owned by a user via namespace memberships, newest first
// per namespace. Ordering matches the supplementary index
// `idx_repositories_namespace_updated`.
export async function listRepositoriesForUser(
  db: Db,
  userId: string
): Promise<RepositoryListing[]> {
  const rows = await db
    .select({ repository: repositories, namespaceSlug: namespaces.slug })
    .from(repositories)
    .innerJoin(namespaces, eq(repositories.namespaceId, namespaces.id))
    .innerJoin(namespaceMemberships, eq(repositories.namespaceId, namespaceMemberships.namespaceId))
    .where(eq(namespaceMemberships.userId, userId))
    .orderBy(repositories.namespaceId, desc(repositories.updatedAt), repositories.slug);
  return rows.map((row) => ({
    repository: row.repository,
    namespace: { id: row.repository.namespaceId, slug: row.namespaceSlug },
  }));
}
