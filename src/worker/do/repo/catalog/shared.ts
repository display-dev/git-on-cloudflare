import type { Head, RepoStateSchema, RepoLease, TypedStorage } from "../repoState";
import type { PackCatalogRow } from "../db/schema";
import type { Ref } from "../repoState";

export const RECEIVE_LEASE_TTL_MS = 30 * 60_000;
export const COMPACT_LEASE_TTL_MS = 20 * 60_000;
export const LEASE_RETRY_AFTER_SECONDS = 10;
export const COMPACTION_REARM_DELAY_MS = 60_000;
export const COMPACTION_WAKE_DELAY_MS = 5_000;
export const COMPACTION_ACTIVITY_QUIET_MS = 15_000;
// Do not let a continuously written repository postpone maintenance forever.
// Reuse the existing recovery cadence as the longest activity deferral so an
// alarm cycle is also an upper bound on accumulated uncompacted packs.
export const COMPACTION_MAX_DEFERRAL_MS = COMPACTION_REARM_DELAY_MS;
export const DEFAULT_HEAD: Head = { target: "refs/heads/main", unborn: true };
export const IDX_HEADER_LEN = 8 + 256 * 4;

/** Earliest safe start: after a quiet window, but never beyond the hard deferral. */
export function compactionStartAt(wantedAt: number, pendingSince = wantedAt): number {
  return Math.min(
    wantedAt + COMPACTION_ACTIVITY_QUIET_MS,
    pendingSince + COMPACTION_MAX_DEFERRAL_MS
  );
}

export type BeginReceiveResult =
  | { ok: false; retryAfter: number }
  | {
      ok: true;
      lease: RepoLease;
      refs: Ref[];
      head: Head;
      refsVersion: number;
      packsetVersion: number;
      nextPackSeq: number;
      activeCatalog: PackCatalogRow[];
      concurrentStockPreparation?: boolean | undefined;
      stockPreparationReserved?: true | undefined;
      stockRecovery?: { operationId: string; token: string } | undefined;
    };

export type BeginStockReceiveRecoveryResult =
  | { status: "not_found" }
  | { status: "busy"; retryAfter: number }
  | { status: "cleanup_required"; operationId: string; token: string }
  | { status: "recovery"; begin: Extract<BeginReceiveResult, { ok: true }> };

export function uniq(items: Array<string | undefined>): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const item of items) {
    if (!item || seen.has(item)) continue;
    seen.add(item);
    out.push(item);
  }
  return out;
}

/**
 * Ensures all required repo metadata keys exist with sensible defaults.
 * Initializes refsVersion, packsetVersion, and nextPackSeq if not already set.
 */
export async function ensureRepoMetadataDefaults(
  store: TypedStorage<RepoStateSchema>
): Promise<void> {
  if ((await store.get("refsVersion")) === undefined) await store.put("refsVersion", 0);
  if ((await store.get("packsetVersion")) === undefined) await store.put("packsetVersion", 0);
  if ((await store.get("nextPackSeq")) === undefined) await store.put("nextPackSeq", 1);
}

export async function bumpPacksetVersion(store: TypedStorage<RepoStateSchema>): Promise<number> {
  const next = ((await store.get("packsetVersion")) || 0) + 1;
  await store.put("packsetVersion", next);
  return next;
}

/** Record the latest repository activity without moving the first pending deadline. */
export async function markCompactionActivity(
  store: TypedStorage<RepoStateSchema>,
  activityAt: number
): Promise<void> {
  // Publish the backward-compatible schedule key first. Repositories without
  // the newer pending boundary safely use wantedAt for both deadline inputs.
  await store.put("compactionWantedAt", activityAt);
  if (typeof (await store.get("compactionPendingSince")) !== "number") {
    await store.put("compactionPendingSince", activityAt);
  }
}

/** Clear both timestamps that make up one pending compaction schedule. */
export async function clearCompactionSchedule(store: TypedStorage<RepoStateSchema>): Promise<void> {
  await store.delete("compactionWantedAt");
  await store.delete("compactionPendingSince");
}
