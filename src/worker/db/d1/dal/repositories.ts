import { and, eq, exists, or, sql } from "drizzle-orm";

import type { Db } from "@/worker/db/d1/client";
import { namespaceMemberships } from "@/worker/db/d1/schema/namespaceMemberships";
import { namespaces } from "@/worker/db/d1/schema/namespaces";
import {
  type NewRepositoryRow,
  type RepositoryRow,
  type RepositoryVisibility,
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

// Used by repository creation and operator/test seeding. The conflict path
// keeps existing rows untouched, so safe replays and re-runs are idempotent.
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

// All repositories owned by a user via namespace memberships, ordered by the
// user-facing full repository name (`namespace/repo`). This keeps account
// listings stable even when pushes update repository activity timestamps.
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
    .orderBy(namespaces.slug, repositories.slug);
  return rows.map((row) => ({
    repository: row.repository,
    namespace: { id: row.repository.namespaceId, slug: row.namespaceSlug },
  }));
}

// Membership is owner-equivalent in this migration; an `EXISTS` subquery
// lets the same owner page query include private rows only for members.
export async function listRepositoriesForNamespace(
  db: Db,
  namespaceId: string,
  viewerUserId: string | null
): Promise<RepositoryRow[]> {
  if (viewerUserId === null) {
    return await db
      .select()
      .from(repositories)
      .where(and(eq(repositories.namespaceId, namespaceId), eq(repositories.visibility, "public")))
      .orderBy(repositories.slug);
  }
  const memberClause = exists(
    db
      .select({ one: sql`1` })
      .from(namespaceMemberships)
      .where(
        and(
          eq(namespaceMemberships.namespaceId, namespaceId),
          eq(namespaceMemberships.userId, viewerUserId)
        )
      )
  );
  return await db
    .select()
    .from(repositories)
    .where(
      and(
        eq(repositories.namespaceId, namespaceId),
        or(eq(repositories.visibility, "public"), memberClause)
      )
    )
    .orderBy(repositories.slug);
}

export async function touchRepositoryUpdatedAt(
  db: Db,
  repositoryId: string,
  now: number
): Promise<void> {
  await db.update(repositories).set({ updatedAt: now }).where(eq(repositories.id, repositoryId));
}

// Deletes a repository row by id. Schema FK cascades remove `pat_repo_grants`
// for that repo. Namespace-scoped grants in `pat_namespace_grants` are
// intentionally untouched: they still cover any other or future repo in the
// same namespace.
//
// Returns true on first run, false on replay (row already absent). Callers
// must enforce membership before invoking; this DAL has no auth opinion so
// the queue consumer can use it after the request-path gate has already
// run.
export async function deleteRepositoryById(db: Db, repositoryId: string): Promise<boolean> {
  const result = await db
    .delete(repositories)
    .where(eq(repositories.id, repositoryId))
    .returning({ id: repositories.id });
  return result.length === 1;
}

export type UpdateRepositoryVisibilityResult =
  | { ok: true; previous: RepositoryVisibility; current: RepositoryVisibility }
  | { ok: false; reason: "not-found" };

// Caller must verify membership before calling. Returns previous visibility
// so the caller can decide whether to clear the route KV (privacy hygiene).
export async function updateRepositoryVisibility(
  db: Db,
  repositoryId: string,
  visibility: RepositoryVisibility,
  now: number
): Promise<UpdateRepositoryVisibilityResult> {
  const rows = await db
    .select({ visibility: repositories.visibility })
    .from(repositories)
    .where(eq(repositories.id, repositoryId))
    .limit(1);
  const existing = rows[0];
  if (!existing) return { ok: false, reason: "not-found" };
  await db
    .update(repositories)
    .set({ visibility, updatedAt: now })
    .where(eq(repositories.id, repositoryId));
  return {
    ok: true,
    previous: existing.visibility,
    current: visibility,
  };
}
