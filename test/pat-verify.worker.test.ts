import { applyD1Migrations } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { beforeAll, beforeEach, describe, expect, it } from "vitest";

import { createDb } from "@/worker/db/d1/client";
import { generatePatPlaintext, hashPatPlaintext, verifyPat } from "@/worker/auth/pat";
import {
  insertMembershipIfMissing,
  insertPatWithGrants,
  insertRepositoryIfNew,
  insertUserIfNew,
} from "@/worker/db/d1/dal";
import { claimNamespace } from "@/worker/db/d1/dal/namespaces";

import { readAppD1Migrations } from "./util/d1Migrations";

beforeAll(async () => {
  await applyD1Migrations(env.DB, readAppD1Migrations());
});

const USER_ID = "user-pv";
const NAMESPACE_ID = "ns-pv";
const NAMESPACE_SLUG = "verify-rachel";
const REPO_ID = "repo-pv";
const REPO_SLUG = "site";
const DO_NAME = "verify-rachel/site";

beforeEach(async () => {
  const db = createDb(env.DB);
  const now = Date.now();
  await insertUserIfNew(db, { id: USER_ID, tesseraSub: "sub-pv", createdAt: now });
  await claimNamespace(db, {
    id: NAMESPACE_ID,
    slug: NAMESPACE_SLUG,
    createdBy: USER_ID,
    createdAt: now,
  });
  await insertMembershipIfMissing(db, {
    namespaceId: NAMESPACE_ID,
    userId: USER_ID,
    createdAt: now,
  });
  await insertRepositoryIfNew(db, {
    id: REPO_ID,
    namespaceId: NAMESPACE_ID,
    createdBy: USER_ID,
    slug: REPO_SLUG,
    doName: DO_NAME,
    visibility: "public",
    createdAt: now,
    updatedAt: now,
  });
});

async function seedPat(args: {
  patId: string;
  namespaceGrants?: Array<{ namespaceId: string; level: "pull" | "push" }>;
  repoGrants?: Array<{ repoId: string; level: "pull" | "push" }>;
  revokedAt?: number;
  expiresAt?: number;
}): Promise<string> {
  const db = createDb(env.DB);
  const generated = generatePatPlaintext();
  const hash = await hashPatPlaintext(generated.plaintext);
  await insertPatWithGrants(db, {
    pat: {
      id: args.patId,
      userId: USER_ID,
      name: "ci",
      prefix: generated.publicPrefix,
      hash,
      createdAt: Date.now(),
      expiresAt: args.expiresAt ?? null,
      revokedAt: args.revokedAt ?? null,
      lastUsedAt: null,
    },
    namespaceGrants: args.namespaceGrants?.map((g) => ({ patId: args.patId, ...g })) ?? [],
    repoGrants: args.repoGrants?.map((g) => ({ patId: args.patId, ...g })) ?? [],
  });
  return generated.plaintext;
}

describe("verifyPat", () => {
  it("ok via repo grant returns level=push when granted", async () => {
    const plaintext = await seedPat({
      patId: "pat-repo-ok",
      repoGrants: [{ repoId: REPO_ID, level: "push" }],
    });
    const result = await verifyPat(env, {
      username: NAMESPACE_SLUG,
      plaintext,
      namespaceId: NAMESPACE_ID,
      repositoryId: REPO_ID,
    });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.repositoryId).toBe(REPO_ID);
    expect(result.level).toBe("push");
  });

  it("ok via namespace grant returns level=pull for pull-only", async () => {
    const plaintext = await seedPat({
      patId: "pat-ns-ok",
      namespaceGrants: [{ namespaceId: NAMESPACE_ID, level: "pull" }],
    });
    const result = await verifyPat(env, {
      username: NAMESPACE_SLUG,
      plaintext,
      namespaceId: NAMESPACE_ID,
      repositoryId: REPO_ID,
    });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.level).toBe("pull");
    expect(result.repositoryId).toBe(REPO_ID);
  });

  it("returns malformed for an obviously bad token", async () => {
    expect(await verifyPat(env, { username: NAMESPACE_SLUG, plaintext: "" })).toEqual({
      ok: false,
      reason: "malformed",
    });
  });

  it("returns token-not-found for a well-formed token with no DB row", async () => {
    const orphan = generatePatPlaintext();
    expect(await verifyPat(env, { username: NAMESPACE_SLUG, plaintext: orphan.plaintext })).toEqual(
      { ok: false, reason: "token-not-found" }
    );
  });

  it("returns token-revoked when the PAT is marked revoked", async () => {
    const plaintext = await seedPat({
      patId: "pat-revoked",
      namespaceGrants: [{ namespaceId: NAMESPACE_ID, level: "pull" }],
      revokedAt: Date.now() - 10,
    });
    expect(await verifyPat(env, { username: NAMESPACE_SLUG, plaintext })).toEqual({
      ok: false,
      reason: "token-revoked",
    });
  });

  it("returns token-expired when expires_at is in the past", async () => {
    const plaintext = await seedPat({
      patId: "pat-expired",
      namespaceGrants: [{ namespaceId: NAMESPACE_ID, level: "pull" }],
      expiresAt: Date.now() - 10,
    });
    expect(await verifyPat(env, { username: NAMESPACE_SLUG, plaintext })).toEqual({
      ok: false,
      reason: "token-expired",
    });
  });

  it("returns username-mismatch when the username segment does not match the namespace", async () => {
    const plaintext = await seedPat({
      patId: "pat-username-mismatch",
      namespaceGrants: [{ namespaceId: NAMESPACE_ID, level: "pull" }],
    });
    expect(
      await verifyPat(env, {
        username: "someone-else",
        plaintext,
        namespaceId: NAMESPACE_ID,
        repositoryId: REPO_ID,
      })
    ).toEqual({ ok: false, reason: "username-mismatch" });
  });

  it("returns grant-missing when no grant covers the requested resource", async () => {
    const plaintext = await seedPat({ patId: "pat-no-grant" });
    expect(
      await verifyPat(env, {
        username: NAMESPACE_SLUG,
        plaintext,
        namespaceId: NAMESPACE_ID,
        repositoryId: REPO_ID,
      })
    ).toEqual({ ok: false, reason: "grant-missing" });
  });
});
