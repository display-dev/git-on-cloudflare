import { beforeAll, describe, expect, it } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { newPrefixedId } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import { generatePatPlaintext, hashPatPlaintext } from "@/worker/auth/pat";
import { insertPatWithGrants } from "@/worker/db/d1/dal";

import { ensureD1Migrations } from "./util/d1Setup";
import { seedRepo, type SeededRepo } from "./util/repoSeed";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

function basicAuth(user: string, pass: string): string {
  return `Basic ${btoa(`${user}:${pass}`)}`;
}

async function seedPat(args: {
  userId: string;
  level: "pull" | "push";
  scope: { kind: "repo"; repoId: string } | { kind: "namespace"; namespaceId: string };
  revokedAt?: number;
  expiresAt?: number;
}): Promise<string> {
  const db = createDb(env.DB);
  const generated = generatePatPlaintext();
  const hash = await hashPatPlaintext(generated.plaintext);
  const patId = newPrefixedId("pat");
  await insertPatWithGrants(db, {
    pat: {
      id: patId,
      userId: args.userId,
      name: "ci",
      prefix: generated.publicPrefix,
      hash,
      createdAt: Date.now(),
      expiresAt: args.expiresAt ?? null,
      revokedAt: args.revokedAt ?? null,
      lastUsedAt: null,
    },
    namespaceGrants:
      args.scope.kind === "namespace"
        ? [{ patId, namespaceId: args.scope.namespaceId, level: args.level }]
        : [],
    repoGrants:
      args.scope.kind === "repo" ? [{ patId, repoId: args.scope.repoId, level: args.level }] : [],
  });
  return generated.plaintext;
}

function infoRefs(seed: SeededRepo, service: "git-upload-pack" | "git-receive-pack"): string {
  return `https://example.com/${seed.namespaceSlug}/${seed.repoSlug}/info/refs?service=${service}`;
}

function uploadPackUrl(seed: SeededRepo): string {
  return `https://example.com/${seed.namespaceSlug}/${seed.repoSlug}/git-upload-pack`;
}

function receivePackUrl(seed: SeededRepo): string {
  return `https://example.com/${seed.namespaceSlug}/${seed.repoSlug}/git-receive-pack`;
}

const FETCH_BODY = new TextEncoder().encode("");

describe("Git ACL: read paths", () => {
  it("public + anonymous + info-refs upload-pack -> 200", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-pub-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "public",
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-upload-pack"));
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toContain("git-upload-pack-advertisement");
  });

  it("public + anonymous + missing route cache -> 404", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-pub-miss-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "public",
      skipRouteCache: true,
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-upload-pack"));
    expect(res.status).toBe(404);
  });

  it("public + valid PAT + missing route cache -> 200", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-pat-miss-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "public",
      skipRouteCache: true,
    });
    const plaintext = await seedPat({
      userId: seed.userId,
      level: "pull",
      scope: { kind: "repo", repoId: seed.repositoryId },
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-upload-pack"), {
      headers: { Authorization: basicAuth(seed.namespaceSlug, plaintext) },
    });
    expect(res.status).toBe(200);
  });

  it("public + malformed PAT + missing route cache -> 401", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-badpat-miss-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "public",
      skipRouteCache: true,
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-upload-pack"), {
      headers: { Authorization: basicAuth(seed.namespaceSlug, "not-a-pat") },
    });
    expect(res.status).toBe(401);
  });

  it("private + anonymous + info-refs upload-pack -> 404 (non-disclosure)", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-priv-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-upload-pack"));
    expect(res.status).toBe(404);
  });

  it("private + valid PAT (pull) + info-refs upload-pack -> 200", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-pat-pull-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
    });
    const plaintext = await seedPat({
      userId: seed.userId,
      level: "pull",
      scope: { kind: "repo", repoId: seed.repositoryId },
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-upload-pack"), {
      headers: { Authorization: basicAuth(seed.namespaceSlug, plaintext) },
    });
    expect(res.status).toBe(200);
  });

  it("private + valid PAT + missing route cache -> 200", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-priv-pat-miss-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
      skipRouteCache: true,
    });
    const plaintext = await seedPat({
      userId: seed.userId,
      level: "pull",
      scope: { kind: "repo", repoId: seed.repositoryId },
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-upload-pack"), {
      headers: { Authorization: basicAuth(seed.namespaceSlug, plaintext) },
    });
    expect(res.status).toBe(200);
  });

  it("private + valid PAT (push) + info-refs upload-pack -> 200", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-pat-push-r-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
    });
    const plaintext = await seedPat({
      userId: seed.userId,
      level: "push",
      scope: { kind: "repo", repoId: seed.repositoryId },
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-upload-pack"), {
      headers: { Authorization: basicAuth(seed.namespaceSlug, plaintext) },
    });
    expect(res.status).toBe(200);
  });

  it("private + Basic username mismatch -> 401", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-mismatch-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
    });
    const plaintext = await seedPat({
      userId: seed.userId,
      level: "pull",
      scope: { kind: "repo", repoId: seed.repositoryId },
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-upload-pack"), {
      headers: { Authorization: basicAuth("not-the-namespace", plaintext) },
    });
    expect(res.status).toBe(401);
  });

  it("private + git-upload-pack POST without creds -> 404", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-priv-post-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
    });
    const res = await workerExports.default.fetch(uploadPackUrl(seed), {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: FETCH_BODY,
    });
    expect(res.status).toBe(404);
  });
});

describe("Git ACL: push paths", () => {
  it("private + anonymous + info-refs receive-pack -> 401 challenge", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-priv-recv-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-receive-pack"));
    expect(res.status).toBe(401);
    expect(res.headers.get("WWW-Authenticate")).toMatch(/Basic/);
  });

  it("private + anonymous + missing route cache + info-refs receive-pack -> 401 challenge", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-priv-recv-miss-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
      skipRouteCache: true,
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-receive-pack"));
    expect(res.status).toBe(401);
    expect(res.headers.get("WWW-Authenticate")).toMatch(/Basic/);
  });

  it("public + anonymous + missing route cache + info-refs receive-pack -> 401 challenge", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-pub-recv-miss-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "public",
      skipRouteCache: true,
    });
    const res = await workerExports.default.fetch(infoRefs(seed, "git-receive-pack"));
    expect(res.status).toBe(401);
    expect(res.headers.get("WWW-Authenticate")).toMatch(/Basic/);
  });

  it("private + anonymous + missing route cache + receive-pack POST -> 401 challenge", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-priv-post-miss-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
      skipRouteCache: true,
    });
    const res = await workerExports.default.fetch(receivePackUrl(seed), {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-receive-pack-request",
      },
      body: new Uint8Array(),
    });
    expect(res.status).toBe(401);
    expect(res.headers.get("WWW-Authenticate")).toMatch(/Basic/);
  });

  it("private + PAT pull-only + receive-pack POST -> 403", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-pull-push-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
    });
    const plaintext = await seedPat({
      userId: seed.userId,
      level: "pull",
      scope: { kind: "repo", repoId: seed.repositoryId },
    });
    const res = await workerExports.default.fetch(receivePackUrl(seed), {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-receive-pack-request",
        Authorization: basicAuth(seed.namespaceSlug, plaintext),
      },
      body: new Uint8Array(),
    });
    expect(res.status).toBe(403);
  });

  it("private + revoked PAT + receive-pack POST -> 401 challenge", async () => {
    const seed = await seedRepo(env, {
      namespaceSlug: `acl-revoked-${Math.random().toString(36).slice(2, 8)}`,
      repoSlug: "site",
      visibility: "private",
    });
    const plaintext = await seedPat({
      userId: seed.userId,
      level: "push",
      scope: { kind: "repo", repoId: seed.repositoryId },
      revokedAt: Date.now() - 1,
    });
    const res = await workerExports.default.fetch(receivePackUrl(seed), {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-receive-pack-request",
        Authorization: basicAuth(seed.namespaceSlug, plaintext),
      },
      body: new Uint8Array(),
    });
    expect(res.status).toBe(401);
  });
});
