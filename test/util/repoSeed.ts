import { newPrefixedId } from "@/worker/common";
import { SESSION_COOKIE_NAME } from "@/worker/auth/cookies";
import { __test as sessionTest } from "@/worker/auth/session";
import { createDb } from "@/worker/db/d1/client";
import {
  claimNamespace,
  findNamespaceBySlug,
  findRepositoryByNamespaceAndSlug,
  insertMembershipIfMissing,
  insertRepositoryIfNew,
  insertUserIfNew,
  putRouteCacheRecord,
  routeCacheKey,
} from "@/worker/db/d1/dal";

import { ensureD1Migrations } from "./d1Setup";

// Seed a single user, namespace, membership, and repository row plus the
// route-cache KV record. Idempotent: re-runs against an existing namespace
// reuse it, and re-runs against an existing repo return its canonical id.
//
// Default `doName` follows the legacy `<namespaceSlug>/<repoSlug>` shape so
// existing fetch/push/UI tests can reuse their `uniqueRepoId(...)` patterns
// unchanged. Pass `doName: "repo:<id>"` to exercise the new-repo path.

export type SeedRepoArgs = {
  // Optional ids; tests usually let the helper generate them.
  userId?: string;
  namespaceId?: string;
  repositoryId?: string;
  // Logical inputs.
  tesseraSub?: string;
  namespaceSlug: string;
  repoSlug: string;
  visibility?: "public" | "private";
  doName?: string;
  // Skip the ROUTES KV write (some tests want to assert the resolver's D1
  // fallback without a KV candidate).
  skipRouteCache?: boolean;
};

export type SeededRepo = {
  userId: string;
  namespaceId: string;
  repositoryId: string;
  doName: string;
  namespaceSlug: string;
  repoSlug: string;
  visibility: "public" | "private";
  routeCacheKey: string;
};

export async function seedRepo(env: Env, args: SeedRepoArgs): Promise<SeededRepo> {
  const db = createDb(env.DB);
  const now = Date.now();
  const visibility = args.visibility ?? "public";
  const doName = args.doName ?? `${args.namespaceSlug}/${args.repoSlug}`;

  const userId = args.userId ?? newPrefixedId("user");
  await insertUserIfNew(db, {
    id: userId,
    tesseraSub: args.tesseraSub ?? `seed-${userId}`,
    createdAt: now,
  });

  let namespaceId = args.namespaceId;
  if (!namespaceId) {
    namespaceId = newPrefixedId("ns");
  }
  const claimed = await claimNamespace(db, {
    id: namespaceId,
    slug: args.namespaceSlug,
    createdBy: userId,
    createdAt: now,
  });
  let resolvedNamespaceId: string;
  if (claimed) {
    resolvedNamespaceId = claimed.id;
  } else {
    const existing = await findNamespaceBySlug(db, args.namespaceSlug);
    if (!existing) {
      throw new Error(`seedRepo: namespace ${args.namespaceSlug} disappeared mid-claim`);
    }
    resolvedNamespaceId = existing.id;
  }

  await insertMembershipIfMissing(db, {
    namespaceId: resolvedNamespaceId,
    userId,
    createdAt: now,
  });

  const repositoryId = args.repositoryId ?? newPrefixedId("repo");
  const inserted = await insertRepositoryIfNew(db, {
    id: repositoryId,
    namespaceId: resolvedNamespaceId,
    createdBy: userId,
    slug: args.repoSlug,
    doName,
    visibility,
    createdAt: now,
    updatedAt: now,
  });
  let resolvedRepoId: string;
  let resolvedDoName: string;
  if (inserted) {
    resolvedRepoId = inserted.id;
    resolvedDoName = inserted.doName;
  } else {
    const existing = await findRepositoryByNamespaceAndSlug(db, resolvedNamespaceId, args.repoSlug);
    if (!existing) {
      throw new Error(`seedRepo: repository ${args.namespaceSlug}/${args.repoSlug} disappeared`);
    }
    resolvedRepoId = existing.id;
    resolvedDoName = existing.doName;
  }

  if (!args.skipRouteCache) {
    await putRouteCacheRecord(env, args.namespaceSlug, args.repoSlug, {
      repositoryId: resolvedRepoId,
      namespaceId: resolvedNamespaceId,
      doName: resolvedDoName,
      updatedAt: now,
    });
  }

  return {
    userId,
    namespaceId: resolvedNamespaceId,
    repositoryId: resolvedRepoId,
    doName: resolvedDoName,
    namespaceSlug: args.namespaceSlug,
    repoSlug: args.repoSlug,
    visibility,
    routeCacheKey: routeCacheKey(args.namespaceSlug, args.repoSlug),
  };
}

export type SetupRepoForTestsResult = SeededRepo & {
  // Pre-baked Cookie header value for the seeded user's session. Tests that
  // hit member-gated routes (admin JSON, private UI) attach this to fetches.
  cookieHeader: string;
};

// Migrations + repo seed + session cookie minted for the seeded user.
export async function setupRepoForTests(
  env: Env,
  namespaceSlug: string,
  repoSlug: string,
  opts: Partial<Omit<SeedRepoArgs, "namespaceSlug" | "repoSlug">> = {}
): Promise<SetupRepoForTestsResult> {
  await ensureD1Migrations(env);
  const seeded = await seedRepo(env, { namespaceSlug, repoSlug, ...opts });
  const cookieHeader = await mintSessionCookie(env, seeded.userId);
  return { ...seeded, cookieHeader };
}

// Mints a sealed session cookie for `userId`. Tests that hit member-gated
// routes (admin JSON, private UI) attach the returned `Cookie` header to
// their `fetch()` calls.
export async function mintSessionCookie(env: Env, userId: string): Promise<string> {
  const secret = env.SESSION_SECRET;
  if (!secret) throw new Error("mintSessionCookie: SESSION_SECRET not set");
  const now = Date.now();
  const token = await sessionTest.sealSession(secret, {
    version: 1,
    userId,
    createdAt: now,
    expiresAt: now + 60 * 60 * 1000,
  });
  return `${SESSION_COOKIE_NAME}=${token}`;
}
