import { and, eq } from "drizzle-orm";

import type { Db } from "@/worker/db/d1/client";
import {
  namespaceMemberships,
  type NamespaceMembershipRow,
  type NewNamespaceMembershipRow,
} from "@/worker/db/d1/schema/namespaceMemberships";
import {
  namespaces,
  type NamespaceRow,
  type NewNamespaceRow,
} from "@/worker/db/d1/schema/namespaces";

export async function findNamespaceBySlug(db: Db, slug: string): Promise<NamespaceRow | undefined> {
  const rows = await db.select().from(namespaces).where(eq(namespaces.slug, slug)).limit(1);
  return rows[0];
}

export async function findNamespaceById(
  db: Db,
  namespaceId: string
): Promise<NamespaceRow | undefined> {
  const rows = await db.select().from(namespaces).where(eq(namespaces.id, namespaceId)).limit(1);
  return rows[0];
}

// Attempt to claim an unused namespace slug for a user. Returns the inserted
// row on success; returns undefined when another row already holds the slug
// (race lost or pre-existing). Callers must NOT treat undefined as failure
// of the surrounding sign-in flow.
export async function claimNamespace(
  db: Db,
  row: NewNamespaceRow
): Promise<NamespaceRow | undefined> {
  const inserted = await db
    .insert(namespaces)
    .values(row)
    .onConflictDoNothing({ target: namespaces.slug })
    .returning();
  return inserted[0];
}

export async function insertMembershipIfMissing(
  db: Db,
  row: NewNamespaceMembershipRow
): Promise<void> {
  await db.insert(namespaceMemberships).values(row).onConflictDoNothing();
}

// Namespaces a user is a member of, ordered by slug for stable rendering.
export async function listNamespacesForUser(db: Db, userId: string): Promise<NamespaceRow[]> {
  const rows = await db
    .select({ ns: namespaces })
    .from(namespaceMemberships)
    .innerJoin(namespaces, eq(namespaceMemberships.namespaceId, namespaces.id))
    .where(eq(namespaceMemberships.userId, userId));
  return rows.map((row) => row.ns).sort((a, b) => a.slug.localeCompare(b.slug));
}

export async function findMembership(
  db: Db,
  namespaceId: string,
  userId: string
): Promise<NamespaceMembershipRow | undefined> {
  const rows = await db
    .select()
    .from(namespaceMemberships)
    .where(
      and(
        eq(namespaceMemberships.namespaceId, namespaceId),
        eq(namespaceMemberships.userId, userId)
      )
    )
    .limit(1);
  return rows[0];
}
