import { createLogger } from "@/worker/common";
import { createDb, type Db } from "@/worker/db/d1/client";
import { updatePatLastUsedAt } from "@/worker/db/d1/dal/tokens";
import type { Logger } from "@/worker/common/logger";
import type { RepositoryRoute } from "@/worker/repositories/route";
import { findGitAuthRows } from "@/worker/db/d1/dal/gitAuth";
import { getRouteCacheRecord } from "@/worker/repositories/routeCache";

import {
  parsePatPlaintext,
  PAT_LAST_USED_READ_THROTTLE_MS,
  shouldTouchPatLastUsedAt,
  completeStoredPatAuthorization,
  verifyStoredPatCredential,
  verifyPat,
  type PatLastUsedOp,
  type PatVerifyError,
  type PatVerifyOk,
} from "./pat";

// Decode `Authorization: Basic <b64>` into `{ username, password }`. The
// caller decides whether the credentials are valid; this helper does no
// authorization. Used by Git endpoints that accept PAT credentials over
// HTTP Basic.
export function getBasicCredentials(req: Request): { username: string; password: string } | null {
  const header = req.headers.get("Authorization") || "";
  const match = /^Basic\s+(.+)$/i.exec(header);
  if (!match) return null;
  try {
    const decoded = atob(match[1]!);
    const idx = decoded.indexOf(":");
    if (idx === -1) return { username: decoded, password: "" };
    return {
      username: decoded.slice(0, idx),
      password: decoded.slice(idx + 1),
    };
  } catch {
    return null;
  }
}

// UI handlers must not import this module; it is the only path that reaches
// `verifyPat`. The kept invariant: PAT credentials authorize git endpoints
// only, never browser surfaces.
export type GitAuthResult =
  | { kind: "anonymous" }
  | { kind: "missing-credentials" }
  | { kind: "pat"; verified: PatVerifyOk }
  | { kind: "pat-rejected"; reason: PatVerifyError["reason"] };

export type BatchedGitAuthenticationResult =
  | { kind: "miss" }
  | { kind: "not-found" }
  | { kind: "resolved"; route: RepositoryRoute; auth: GitAuthResult };

// Canonical route and PAT/grant reads depend only on URL slugs and the public
// PAT prefix, so D1 can execute them in one batch. A parallel KV read preserves
// public anonymous-read behavior without extending the critical path. D1
// remains authoritative for visibility and route identity because KV carries
// neither and may briefly retain stale entries after a visibility change.
export async function authenticateGitRequestFromCanonicalBatch(
  env: Env,
  request: Request,
  namespaceSlug: string,
  repoSlug: string,
  options: { db?: Db; now?: number; log?: Logger } = {}
): Promise<BatchedGitAuthenticationResult> {
  const basic = getBasicCredentials(request);
  if (!basic?.password) return { kind: "miss" };
  const parsed = parsePatPlaintext(basic.password);
  if (!parsed.ok) return { kind: "miss" };
  const [cached, rows] = await Promise.all([
    getRouteCacheRecord(env, namespaceSlug, repoSlug),
    findGitAuthRows(options.db ?? createDb(env.DB), {
      namespaceSlug,
      repositorySlug: repoSlug,
      patPrefix: parsed.publicPrefix,
    }),
  ]);
  const log = options.log ?? createLogger(env.LOG_LEVEL, { service: "GitAuthBatch" });
  if (!rows.repository) {
    log.debug("git-auth-batch:repository-not-found", { namespaceSlug, repoSlug });
    return { kind: "not-found" };
  }
  const cacheMatchesCanonical =
    cached?.repositoryId === rows.repository.id &&
    cached.namespaceId === rows.repository.namespaceId &&
    cached.doName === rows.repository.doName;
  const route: RepositoryRoute = {
    routeNamespaceSlug: namespaceSlug,
    routeRepoSlug: repoSlug,
    namespaceId: rows.repository.namespaceId,
    repositoryId: rows.repository.id,
    doName: rows.repository.doName,
    visibility: rows.repository.visibility,
    source: cacheMatchesCanonical ? "kv" : "d1",
  };
  log.debug("git-auth-batch:resolved", {
    namespaceSlug,
    repoSlug,
    repositoryId: route.repositoryId,
    routeSource: route.source,
  });
  const now = options.now ?? Date.now();
  const storedCredential = await verifyStoredPatCredential(rows.pat, basic.password, now);
  if (!storedCredential.ok) {
    return {
      kind: "resolved",
      route,
      auth: { kind: "pat-rejected", reason: storedCredential.reason },
    };
  }
  const pat = storedCredential.pat;
  const verified = completeStoredPatAuthorization(pat, {
    usernameMatches: basic.username === namespaceSlug,
    namespaceId: route.namespaceId,
    repositoryId: route.repositoryId,
    repoGrantLevel: rows.repoGrant?.level,
    namespaceGrantLevel: rows.namespaceGrant?.level,
  });
  if (!verified.ok) {
    return { kind: "resolved", route, auth: { kind: "pat-rejected", reason: verified.reason } };
  }
  return {
    kind: "resolved",
    route,
    auth: {
      kind: "pat",
      verified,
    },
  };
}

export async function authenticateGitRequest(
  env: Env,
  request: Request,
  route: RepositoryRoute,
  options: { db?: Db } = {}
): Promise<GitAuthResult> {
  const basic = getBasicCredentials(request);
  if (!basic) return { kind: "anonymous" };
  if (!basic.password) return { kind: "missing-credentials" };
  const verified = await verifyPat(env, {
    username: basic.username,
    plaintext: basic.password,
    namespaceId: route.namespaceId,
    repositoryId: route.repositoryId,
    db: options.db,
  });
  if (verified.ok) return { kind: "pat", verified };
  return { kind: "pat-rejected", reason: verified.reason };
}

// Returns true when the update was scheduled so tests can assert the
// throttle decision without leaning on D1.
export function scheduleTouchPatLastUsedAt(
  env: Env,
  ctx: ExecutionContext,
  verified: PatVerifyOk,
  op: PatLastUsedOp,
  now: number = Date.now(),
  options: { db?: Db; log?: Logger } = {}
): boolean {
  if (!shouldTouchPatLastUsedAt(verified.lastUsedAt, op, now)) return false;
  const log = options.log ?? createLogger(env.LOG_LEVEL, { service: "GitAuth" });
  const db = options.db ?? createDb(env.DB);
  ctx.waitUntil(
    (async () => {
      try {
        await updatePatLastUsedAt(db, verified.patId, now);
        log.debug("pat:last-used-update-ok", { patId: verified.patId, op });
      } catch (error) {
        log.warn("pat:last-used-update-failed", {
          patId: verified.patId,
          op,
          error: String(error),
        });
      }
    })()
  );
  return true;
}

export { PAT_LAST_USED_READ_THROTTLE_MS };
