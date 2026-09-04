import { and, eq, sql } from "drizzle-orm";

import type { Db } from "@/worker/db/d1/client";
import { namespaces } from "@/worker/db/d1/schema/namespaces";
import {
  patNamespaceGrants,
  type PatNamespaceGrantRow,
} from "@/worker/db/d1/schema/patNamespaceGrants";
import { patRepoGrants, type PatRepoGrantRow } from "@/worker/db/d1/schema/patRepoGrants";
import {
  personalAccessTokens,
  type PersonalAccessTokenRow,
} from "@/worker/db/d1/schema/personalAccessTokens";
import { repositories, type RepositoryRow } from "@/worker/db/d1/schema/repositories";

export type GitAuthRows = {
  repository: RepositoryRow | undefined;
  pat: PersonalAccessTokenRow | undefined;
  repoGrant: PatRepoGrantRow | undefined;
  namespaceGrant: PatNamespaceGrantRow | undefined;
};

// Each statement selects from one canonical table so Drizzle retains its
// schema-owned result mapping. Scalar subqueries bind route slugs and the PAT
// prefix without making any statement depend on another statement's result,
// allowing D1 to execute the complete authorization read set in one session batch.
export async function findGitAuthRows(
  db: Db,
  args: { namespaceSlug: string; repositorySlug: string; patPrefix: string }
): Promise<GitAuthRows> {
  const namespaceId = sql<string>`(
    SELECT ${namespaces.id}
    FROM ${namespaces}
    WHERE ${namespaces.slug} = ${args.namespaceSlug}
    LIMIT 1
  )`;
  const repositoryId = sql<string>`(
    SELECT ${repositories.id}
    FROM ${repositories}
    WHERE ${repositories.namespaceId} = ${namespaceId}
      AND ${repositories.slug} = ${args.repositorySlug}
    LIMIT 1
  )`;
  const patId = sql<string>`(
    SELECT ${personalAccessTokens.id}
    FROM ${personalAccessTokens}
    WHERE ${personalAccessTokens.prefix} = ${args.patPrefix}
    LIMIT 1
  )`;
  const [repositoryRows, patRows, repoGrantRows, namespaceGrantRows] = await db.batch([
    db
      .select()
      .from(repositories)
      .where(
        and(eq(repositories.namespaceId, namespaceId), eq(repositories.slug, args.repositorySlug))
      )
      .limit(1),
    db
      .select()
      .from(personalAccessTokens)
      .where(eq(personalAccessTokens.prefix, args.patPrefix))
      .limit(1),
    db
      .select()
      .from(patRepoGrants)
      .where(and(eq(patRepoGrants.patId, patId), eq(patRepoGrants.repoId, repositoryId)))
      .limit(1),
    db
      .select()
      .from(patNamespaceGrants)
      .where(
        and(eq(patNamespaceGrants.patId, patId), eq(patNamespaceGrants.namespaceId, namespaceId))
      )
      .limit(1),
  ]);

  return {
    repository: repositoryRows[0],
    pat: patRows[0],
    repoGrant: repoGrantRows[0],
    namespaceGrant: namespaceGrantRows[0],
  };
}
