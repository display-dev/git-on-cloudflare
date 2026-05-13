import { newPrefixedId } from "@/worker/common";
import { SESSION_COOKIE_HEADER_NAME } from "@/worker/auth/cookies";
import { generatePatPlaintext, hashPatPlaintext } from "@/worker/auth/pat";
import { __test as sessionTest } from "@/worker/auth/session";
import { createDb } from "@/worker/db/d1/client";
import {
  claimNamespace,
  findNamespaceBySlug,
  findRepositoryByNamespaceAndSlug,
  insertMembershipIfMissing,
  insertPatWithGrants,
  insertRepositoryIfNew,
  insertUserIfNew,
} from "@/worker/db/d1/dal";
import { putRouteCacheRecord, routeCacheKey } from "@/worker/repositories/routeCache";

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
  cookieHeader: string;
  pushAuthHeader: string;
  patPlaintext: string;
};

export async function setupRepoForTests(
  env: Env,
  namespaceSlug: string,
  repoSlug: string,
  opts: Partial<Omit<SeedRepoArgs, "namespaceSlug" | "repoSlug">> = {}
): Promise<SetupRepoForTestsResult> {
  await ensureD1Migrations(env);
  const seeded = await seedRepo(env, { namespaceSlug, repoSlug, ...opts });
  const cookieHeader = await mintSessionCookie(env, seeded.userId);
  const pat = await mintNamespacePushPat(env, seeded.userId, seeded.namespaceId);
  const pushAuthHeader = `Basic ${btoa(`${namespaceSlug}:${pat.plaintext}`)}`;
  rememberPushAuth(namespaceSlug, repoSlug, pushAuthHeader);
  return { ...seeded, cookieHeader, pushAuthHeader, patPlaintext: pat.plaintext };
}

async function mintNamespacePushPat(
  env: Env,
  userId: string,
  namespaceId: string
): Promise<{ patId: string; plaintext: string }> {
  const db = createDb(env.DB);
  const generated = generatePatPlaintext();
  const hash = await hashPatPlaintext(generated.plaintext);
  const patId = newPrefixedId("pat");
  const now = Date.now();
  await insertPatWithGrants(db, {
    pat: {
      id: patId,
      userId,
      name: "test-system-push",
      prefix: generated.publicPrefix,
      hash,
      createdAt: now,
      expiresAt: null,
      revokedAt: null,
      lastUsedAt: null,
    },
    namespaceGrants: [{ patId, namespaceId, level: "push" }],
    repoGrants: [],
  });
  return { patId, plaintext: generated.plaintext };
}

// Lookup table so push helpers (`pushBody`, `pushStreamingUpdate`) can
// retrieve the seeded namespace's push PAT without each test threading the
// header through.
const pushAuthByRepo = new Map<string, string>();

export function rememberPushAuth(owner: string, repo: string, header: string): void {
  pushAuthByRepo.set(`${owner}/${repo}`, header);
}

export function lookupPushAuth(owner: string, repo: string): string | undefined {
  return pushAuthByRepo.get(`${owner}/${repo}`);
}

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
  return `${SESSION_COOKIE_HEADER_NAME}=${token}`;
}
