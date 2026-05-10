import type { Viewer } from "@/client/server/viewer";
import { findMembership } from "@/worker/db/d1/dal/namespaces";
import type { AppContext } from "@/worker/routes/hono";

import { loadViewer } from "./session";

// PAT credentials are deliberately not consulted here; the structural rule
// "PAT cannot authorize a browser surface" relies on UI/admin handlers
// importing only this module, never `gitAuth.ts`.
export type SessionMembershipResult =
  | { kind: "anonymous" }
  | { kind: "signed-in-non-member"; viewer: Viewer }
  | { kind: "member"; viewer: Viewer };

export async function loadSessionMembership(
  c: AppContext,
  namespaceId: string
): Promise<SessionMembershipResult> {
  const viewer = await loadViewer(c);
  if (!viewer) return { kind: "anonymous" };
  const membership = await findMembership(c.var.db, namespaceId, viewer.userId);
  if (!membership) return { kind: "signed-in-non-member", viewer };
  return { kind: "member", viewer };
}
