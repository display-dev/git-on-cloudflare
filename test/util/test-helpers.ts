/**
 * Shared test utilities for Git operations.
 */

import { exports as workerExports } from "cloudflare:workers";
import { lookupPushAuth } from "./repoSeed";

type StringEnvKey = {
  [K in keyof Env]: Env[K] extends string | undefined ? K : never;
}[keyof Env];

/**
 * Temporarily override selected env bindings for the duration of fn(), restoring afterwards.
 */
export async function withEnvOverrides<T, K extends StringEnvKey>(
  env: Env,
  overrides: Pick<Env, K>,
  fn: () => Promise<T>
): Promise<T> {
  const keys = Object.keys(overrides) as K[];
  const prev = new Map<K, Env[K]>();
  for (const key of keys) {
    prev.set(key, env[key]);
    env[key] = overrides[key];
  }
  try {
    return await fn();
  } finally {
    for (const key of keys) {
      const value = prev.get(key);
      if (value !== undefined) {
        env[key] = value;
      } else {
        delete env[key];
      }
    }
  }
}

export * from "./do-retry";
export * from "./git-pack";
export * from "./packed-repo";

export function toRequestBody(bytes: Uint8Array): ArrayBuffer {
  const body = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(body).set(bytes);
  return body;
}

/**
 * Generate a per-test unique repo id suffix to avoid shared storage collisions
 * when isolatedStorage is disabled.
 */
export function uniqueRepoId(prefix = "r"): string {
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

// POST to git-receive-pack with the seeded push PAT auto-attached. The owner
// and repo are parsed from the URL and matched against the auth registered
// by `setupRepoForTests`. Tests that want to assert auth failures should
// build the request themselves rather than going through this helper.
export async function postReceivePack(
  url: string,
  body: Uint8Array,
  options?: { authHeader?: string; signal?: AbortSignal }
): Promise<Response> {
  const headers: Record<string, string> = {
    "Content-Type": "application/x-git-receive-pack-request",
  };
  const match = /https?:\/\/[^/]+\/([^/]+)\/([^/]+)\/git-receive-pack/.exec(url);
  const auth = options?.authHeader ?? (match ? lookupPushAuth(match[1]!, match[2]!) : undefined);
  if (auth) headers.Authorization = auth;
  return await workerExports.default.fetch(url, {
    method: "POST",
    headers,
    body: toRequestBody(body),
    signal: options?.signal,
  });
}
