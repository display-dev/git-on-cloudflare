import type {
  RepoLease,
  RepoStateSchema,
  RepositoryMaintenanceLease,
  SnapshotMaterializationLease,
} from "./repoState";

import { asTypedStorage } from "./repoState";

const SNAPSHOT_MATERIALIZATION_LEASE_TTL_MS = 30 * 60_000;
// Receive and compaction R2 subrequests are platform-bounded, but their lease
// owner may lose the response at expiry. Keep deletion fenced for a further
// bounded drain window so an already-started upload cannot finish after the
// final R2 sweep.
const EXPIRED_WRITER_DRAIN_MS = 5 * 60_000;

export type BeginSnapshotMaterializationResult =
  | { ok: true; token: string }
  | { ok: false; reason: "repository-deleting" };

export type BeginRepositoryDeletionResult = {
  ready: boolean;
  snapshotPrefixes: string[];
};

export type BeginRepositoryMaintenanceResult =
  | { ok: true; token: string }
  | { ok: false; reason: "repository-deleting" };

function writerMayStillBeDraining(lease: RepoLease | undefined, now: number): boolean {
  return Boolean(lease && lease.expiresAt + EXPIRED_WRITER_DRAIN_MS > now);
}

function activeSnapshotLeases(
  leases: SnapshotMaterializationLease[] | undefined,
  now: number
): SnapshotMaterializationLease[] {
  return (leases ?? []).filter((lease) => lease.expiresAt + EXPIRED_WRITER_DRAIN_MS > now);
}

function activeMaintenanceLeases(
  leases: RepositoryMaintenanceLease[] | undefined,
  now: number
): RepositoryMaintenanceLease[] {
  return (leases ?? []).filter((lease) => lease.expiresAt + EXPIRED_WRITER_DRAIN_MS > now);
}

export async function beginRepositoryMaintenanceState(
  ctx: DurableObjectState
): Promise<BeginRepositoryMaintenanceResult> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) {
      return { ok: false, reason: "repository-deleting" };
    }
    const now = Date.now();
    const leases = activeMaintenanceLeases(await store.get("repositoryMaintenanceLeases"), now);
    const token = crypto.randomUUID();
    leases.push({
      token,
      operation: "pack-ref-backfill",
      createdAt: now,
      expiresAt: now + SNAPSHOT_MATERIALIZATION_LEASE_TTL_MS,
    });
    await store.put("repositoryMaintenanceLeases", leases);
    return { ok: true, token };
  });
}

export async function renewRepositoryMaintenanceState(
  ctx: DurableObjectState,
  token: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    const now = Date.now();
    const leases = (await store.get("repositoryMaintenanceLeases")) ?? [];
    const index = leases.findIndex((lease) => lease.token === token);
    if (index < 0 || leases[index]!.expiresAt <= now) return false;
    leases[index] = {
      ...leases[index]!,
      expiresAt: now + SNAPSHOT_MATERIALIZATION_LEASE_TTL_MS,
    };
    await store.put("repositoryMaintenanceLeases", leases);
    return true;
  });
}

export async function finishRepositoryMaintenanceState(
  ctx: DurableObjectState,
  token: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const leases = (await store.get("repositoryMaintenanceLeases")) ?? [];
    const next = leases.filter((lease) => lease.token !== token);
    if (next.length === leases.length) return false;
    if (next.length > 0) await store.put("repositoryMaintenanceLeases", next);
    else await store.delete("repositoryMaintenanceLeases");
    return true;
  });
}

export async function beginSnapshotMaterializationState(
  ctx: DurableObjectState,
  prefix: string
): Promise<BeginSnapshotMaterializationResult> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) {
      return { ok: false, reason: "repository-deleting" };
    }
    const now = Date.now();
    const leases = activeSnapshotLeases(await store.get("snapshotMaterializationLeases"), now);
    const token = crypto.randomUUID();
    leases.push({
      token,
      prefix,
      createdAt: now,
      expiresAt: now + SNAPSHOT_MATERIALIZATION_LEASE_TTL_MS,
    });
    const prefixes = Array.from(
      new Set([...((await store.get("snapshotPrefixes")) ?? []), prefix])
    );
    await store.put("snapshotMaterializationLeases", leases);
    await store.put("snapshotPrefixes", prefixes);
    return { ok: true, token };
  });
}

export async function finishSnapshotMaterializationState(
  ctx: DurableObjectState,
  token: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const leases = (await store.get("snapshotMaterializationLeases")) ?? [];
    const next = leases.filter((lease) => lease.token !== token);
    if (next.length === leases.length) return false;
    if (next.length > 0) await store.put("snapshotMaterializationLeases", next);
    else await store.delete("snapshotMaterializationLeases");
    return true;
  });
}

export async function renewSnapshotMaterializationState(
  ctx: DurableObjectState,
  token: string,
  now: number = Date.now()
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    const leases = (await store.get("snapshotMaterializationLeases")) ?? [];
    const index = leases.findIndex((lease) => lease.token === token);
    if (index < 0 || leases[index]!.expiresAt <= now) return false;
    leases[index] = {
      ...leases[index]!,
      expiresAt: now + SNAPSHOT_MATERIALIZATION_LEASE_TTL_MS,
    };
    await store.put("snapshotMaterializationLeases", leases);
    return true;
  });
}

export async function beginRepositoryDeletionState(
  ctx: DurableObjectState
): Promise<BeginRepositoryDeletionResult> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const now = Date.now();
    const snapshotLeases = activeSnapshotLeases(
      await store.get("snapshotMaterializationLeases"),
      now
    );
    const maintenanceLeases = activeMaintenanceLeases(
      await store.get("repositoryMaintenanceLeases"),
      now
    );
    if (snapshotLeases.length > 0) {
      await store.put("snapshotMaterializationLeases", snapshotLeases);
    } else {
      await store.delete("snapshotMaterializationLeases");
    }
    if (maintenanceLeases.length > 0) {
      await store.put("repositoryMaintenanceLeases", maintenanceLeases);
    } else {
      await store.delete("repositoryMaintenanceLeases");
    }
    await store.put("repositoryDeleting", true);
    return {
      ready:
        !writerMayStillBeDraining(await store.get("receiveLease"), now) &&
        !writerMayStillBeDraining(await store.get("compactLease"), now) &&
        snapshotLeases.length === 0 &&
        maintenanceLeases.length === 0,
      snapshotPrefixes: (await store.get("snapshotPrefixes")) ?? [],
    };
  });
}
