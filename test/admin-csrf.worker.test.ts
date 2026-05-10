import { beforeAll, describe, expect, it } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { ensureD1Migrations } from "./util/d1Setup";
import { setupRepoForTests } from "./util/repoSeed";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

type Mutating = {
  label: string;
  method: "POST" | "PUT" | "DELETE";
  path: (owner: string, repo: string) => string;
  body?: unknown;
};

const MUTATING_ROUTES: Mutating[] = [
  {
    label: "compaction POST",
    method: "POST",
    path: (o, r) => `/${o}/${r}/admin/compact`,
    body: {},
  },
  {
    label: "compaction DELETE",
    method: "DELETE",
    path: (o, r) => `/${o}/${r}/admin/compact`,
  },
  {
    label: "refs PUT",
    method: "PUT",
    path: (o, r) => `/${o}/${r}/admin/refs`,
    body: [],
  },
  {
    label: "head PUT",
    method: "PUT",
    path: (o, r) => `/${o}/${r}/admin/head`,
    body: { target: "refs/heads/main" },
  },
  {
    label: "pack DELETE",
    method: "DELETE",
    path: (o, r) => `/${o}/${r}/admin/pack/pack-deadbeef.pack`,
  },
  {
    label: "purge DELETE",
    method: "DELETE",
    path: (o, r) => `/${o}/${r}/admin/purge`,
    body: { confirm: "purge-PLACEHOLDER" },
  },
];

async function call(opts: {
  url: string;
  method: "POST" | "PUT" | "DELETE";
  cookieHeader: string;
  origin?: string;
  body?: unknown;
}): Promise<Response> {
  const headers: Record<string, string> = { Cookie: opts.cookieHeader };
  if (opts.body !== undefined) headers["Content-Type"] = "application/json";
  if (opts.origin) headers.Origin = opts.origin;
  return await workerExports.default.fetch(opts.url, {
    method: opts.method,
    headers,
    body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
  });
}

describe("admin mutating routes: CSRF (sameOriginViolation)", () => {
  for (const route of MUTATING_ROUTES) {
    it(`${route.label}: missing Origin -> 403`, async () => {
      const owner = `csrf-${Math.random().toString(36).slice(2, 8)}`;
      const repo = "site";
      const seeded = await setupRepoForTests(env, owner, repo);
      const url = `https://example.com${route.path(owner, repo)}`;
      const body =
        route.label === "purge DELETE" ? { confirm: `purge-${owner}/${repo}` } : route.body;
      const res = await call({
        url,
        method: route.method,
        cookieHeader: seeded.cookieHeader,
        body,
      });
      expect(res.status).toBe(403);
    });

    it(`${route.label}: cross-origin Origin -> 403`, async () => {
      const owner = `csrf-${Math.random().toString(36).slice(2, 8)}`;
      const repo = "site";
      const seeded = await setupRepoForTests(env, owner, repo);
      const url = `https://example.com${route.path(owner, repo)}`;
      const body =
        route.label === "purge DELETE" ? { confirm: `purge-${owner}/${repo}` } : route.body;
      const res = await call({
        url,
        method: route.method,
        cookieHeader: seeded.cookieHeader,
        origin: "https://attacker.example",
        body,
      });
      expect(res.status).toBe(403);
    });
  }
});
