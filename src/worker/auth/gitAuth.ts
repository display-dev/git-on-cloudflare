import { createLogger } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import { updatePatLastUsedAt } from "@/worker/db/d1/dal/tokens";
import type { RepositoryRoute } from "@/worker/repositories/route";

import {
  PAT_LAST_USED_READ_THROTTLE_MS,
  shouldTouchPatLastUsedAt,
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

export async function authenticateGitRequest(
  env: Env,
  request: Request,
  route: RepositoryRoute
): Promise<GitAuthResult> {
  const basic = getBasicCredentials(request);
  if (!basic) return { kind: "anonymous" };
  if (!basic.password) return { kind: "missing-credentials" };
  const verified = await verifyPat(env, {
    username: basic.username,
    plaintext: basic.password,
    namespaceId: route.namespaceId,
    repositoryId: route.repositoryId,
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
  now: number = Date.now()
): boolean {
  if (!shouldTouchPatLastUsedAt(verified.lastUsedAt, op, now)) return false;
  const log = createLogger(env.LOG_LEVEL, { service: "GitAuth" });
  ctx.waitUntil(
    (async () => {
      try {
        await updatePatLastUsedAt(createDb(env.DB), verified.patId, now);
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
