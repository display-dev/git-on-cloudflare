import { zeroOid } from "@/worker/common";
import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type { ReceiveCommand } from "@/worker/git/operations/validation";
import {
  acceptedWriteJournalKey,
  acceptedWriteHeadKey,
  asTypedStorage,
  materializedSnapshotKey,
  releasedSnapshotKey,
  snapshotPinKey,
  snapshotCurrentKey,
  type AcceptedWriteJournalEntry,
  type RepoStateSchema,
  type SnapshotCurrent,
  type SnapshotPin,
  type TypedStorage,
} from "./repoState";

export type SnapshotProjectionResult = {
  snapshotCreated: boolean;
  pointerAdvanced: boolean;
  current: SnapshotCurrent;
};

export type AcceptedWriteProjectionResult =
  | SnapshotProjectionResult
  | { status: "repository-deleting" | "invalid-snapshot" | "projection-lease-expired" };

export type SnapshotProjectionState = {
  snapshotCount: number;
  current?: SnapshotCurrent;
};

export type SnapshotPinInput = {
  commitSha: string;
  treeSha: string;
  materializedAt: number;
  advanceCurrent?: boolean;
  qualificationOwned?: boolean;
};

type SnapshotPinFact = Pick<
  SnapshotPin,
  "ref" | "beforeSha" | "commitSha" | "actor" | "sourceSurface" | "idempotencyKey"
>;

export type SnapshotReconcilePlan =
  | { status: "up_to_date"; current: SnapshotCurrent }
  | { status: "deliver"; entry: AcceptedWriteJournalEntry }
  | {
      status: "head_only";
      ref: string;
      afterSha: string;
      sequence: number;
      beforeSha: string;
    }
  | { status: "unborn"; ref: string };

function journalId(sequence: number, ref: string): string {
  return `${String(sequence).padStart(16, "0")}:${encodeURIComponent(ref)}`;
}

export function acceptedWritesMatchCommands(
  commands: ReceiveCommand[],
  facts: AcceptedWriteFact[]
): boolean {
  const transitions = new Map<string, { beforeSha: string; afterSha: string }>();
  for (const command of commands) {
    const existing = transitions.get(command.ref);
    if (existing) existing.afterSha = command.newOid;
    else transitions.set(command.ref, { beforeSha: command.oldOid, afterSha: command.newOid });
  }
  const changed = [...transitions.entries()].filter(
    ([, transition]) => transition.beforeSha !== transition.afterSha
  );
  if (changed.length !== facts.length) return false;
  const seen = new Set<string>();
  return facts.every((fact) => {
    if (seen.has(fact.ref)) return false;
    seen.add(fact.ref);
    const transition = transitions.get(fact.ref);
    return transition?.beforeSha === fact.beforeSha && transition.afterSha === fact.afterSha;
  });
}

export async function recordAcceptedWrites(
  store: TypedStorage<RepoStateSchema>,
  sequence: number,
  facts: AcceptedWriteFact[],
  acceptedAt: number,
  journal: boolean,
  snapshotPin?: SnapshotPinInput
): Promise<boolean> {
  if (facts.length === 0) return true;
  if (
    snapshotPin &&
    (!isValidPinnedObjectSha(snapshotPin.commitSha) ||
      !isValidPinnedObjectSha(snapshotPin.treeSha) ||
      !facts.some((fact) => fact.afterSha === snapshotPin.commitSha))
  ) {
    throw new Error("FUBAR: snapshot pin does not match an accepted write");
  }
  let snapshotPinRetained = true;
  for (const fact of facts) {
    await store.put(acceptedWriteHeadKey(fact.ref), {
      ref: fact.ref,
      beforeSha: fact.beforeSha,
      afterSha: fact.afterSha,
      sequence,
    });
    if (snapshotPin && fact.afterSha === snapshotPin.commitSha) {
      const retained = await retainSnapshotPin(
        store,
        sequence,
        {
          ref: fact.ref,
          beforeSha: fact.beforeSha,
          commitSha: fact.afterSha,
          actor: fact.actor,
          sourceSurface: fact.sourceSurface,
          idempotencyKey: fact.idempotencyKey,
        },
        snapshotPin
      );
      if (!retained) snapshotPinRetained = false;
    }
    if (!journal) continue;
    const id = journalId(sequence, fact.ref);
    const key = acceptedWriteJournalKey(id);
    const existing = await store.get(key);
    if (existing) {
      if (
        existing.sequence !== sequence ||
        existing.fact.ref !== fact.ref ||
        existing.fact.beforeSha !== fact.beforeSha ||
        existing.fact.afterSha !== fact.afterSha
      ) {
        throw new Error("accepted-write journal identity conflict");
      }
      continue;
    }
    await store.put(key, { id, sequence, fact, acceptedAt });
  }
  return snapshotPinRetained;
}

function isValidPinnedObjectSha(sha: string): boolean {
  return /^[0-9a-f]{40}$/.test(sha) && sha !== zeroOid();
}

async function hasActiveProjectionLease(
  store: TypedStorage<RepoStateSchema>,
  token: string | undefined
): Promise<boolean> {
  if (!token) return false;
  const now = Date.now();
  const leases = ((await store.get("repositoryReadLeases")) ?? []).filter(
    (lease) => lease.expiresAt > now
  );
  if (leases.length > 0) await store.put("repositoryReadLeases", leases);
  else await store.delete("repositoryReadLeases");
  return leases.some((lease) => lease.token === token && lease.operation === "snapshot-projection");
}

/**
 * Idempotently installs one immutable commit/tree root. Reusing a commit with
 * a different tree is authority corruption, while reaccepting the same
 * identity clears an earlier release tombstone and preserves first provenance.
 */
async function retainSnapshotPin(
  store: TypedStorage<RepoStateSchema>,
  sequence: number,
  fact: SnapshotPinFact,
  input: SnapshotPinInput
): Promise<boolean> {
  if (fact.commitSha !== input.commitSha || !isValidPinnedObjectSha(input.treeSha)) {
    return false;
  }
  const key = snapshotPinKey(fact.commitSha);
  const existing = await store.get(key);
  if (existing) {
    if (existing.treeSha !== input.treeSha || existing.commitSha !== fact.commitSha) {
      return false;
    }
  } else {
    await store.put(key, {
      commitSha: fact.commitSha,
      treeSha: input.treeSha,
      ref: fact.ref,
      beforeSha: fact.beforeSha,
      firstSequence: sequence,
      acceptedAt: input.materializedAt,
      actor: fact.actor,
      sourceSurface: fact.sourceSurface,
      idempotencyKey: fact.idempotencyKey,
      ...(input.qualificationOwned ? { qualificationOwned: true } : {}),
    });
    await store.put("snapshotPinVersion", ((await store.get("snapshotPinVersion")) ?? 0) + 1);
  }
  await store.delete(releasedSnapshotKey(fact.commitSha));
  const materializedKey = materializedSnapshotKey(fact.commitSha);
  if (!(await store.get(materializedKey))) {
    await store.put(materializedKey, {
      commitSha: fact.commitSha,
      firstSequence: sequence,
      materializedAt: input.materializedAt,
    });
  }
  if (input.advanceCurrent) {
    const currentKey = snapshotCurrentKey(fact.ref);
    const current = await store.get(currentKey);
    if (!current || sequence > current.sequence) {
      await store.put(currentKey, {
        ref: fact.ref,
        commitSha: fact.commitSha,
        sequence,
        updatedAt: input.materializedAt,
      });
    } else if (sequence === current.sequence && current.commitSha !== fact.commitSha) {
      throw new Error("snapshot current-pointer sequence conflict");
    }
  }
  return true;
}

export async function getSnapshotPinState(
  ctx: DurableObjectState,
  commitSha: string
): Promise<SnapshotPin | null> {
  return (
    (await asTypedStorage<RepoStateSchema>(ctx.storage).get(snapshotPinKey(commitSha))) ?? null
  );
}

export type SnapshotResolution =
  | { status: "pinned"; pin: SnapshotPin }
  | { status: "released" }
  | { status: "legacy" };

export async function getSnapshotResolutionState(
  ctx: DurableObjectState,
  commitSha: string
): Promise<SnapshotResolution> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const pin = await store.get(snapshotPinKey(commitSha));
  if (pin) return { status: "pinned", pin };
  if (await store.get(releasedSnapshotKey(commitSha))) return { status: "released" };
  return { status: "legacy" };
}

export async function listSnapshotPinsState(ctx: DurableObjectState): Promise<SnapshotPin[]> {
  return [...(await ctx.storage.list<SnapshotPin>({ prefix: "snapshotPin:" })).values()];
}

export type ReleaseSnapshotPinResult =
  | { released: true }
  | { released: false; reason: "repository-deleting" | "ref-referenced" };

/**
 * Releases only unreferenced pins. A tombstone prevents the legacy R2 fallback
 * from reviving released bytes; absent targets create no unbounded state unless
 * the caller first proved that a legacy manifest exists.
 */
export async function releaseSnapshotPinState(
  ctx: DurableObjectState,
  commitSha: string,
  options?: { legacySnapshotExists?: boolean; qualificationOwned?: boolean }
): Promise<ReleaseSnapshotPinResult> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) {
      return { released: false, reason: "repository-deleting" };
    }
    const refs = (await store.get("refs")) ?? [];
    if (refs.some((ref) => ref.oid === commitSha)) {
      return { released: false, reason: "ref-referenced" };
    }
    const pin = await store.get(snapshotPinKey(commitSha));
    if (!pin) {
      const released = await store.get(releasedSnapshotKey(commitSha));
      if (!released && options?.legacySnapshotExists) {
        await store.put(releasedSnapshotKey(commitSha), {
          commitSha,
          releasedAt: Date.now(),
          qualificationOwned: options.qualificationOwned,
        });
      }
      return { released: true };
    }
    // A current pointer is monotonic projection history, not content
    // authority. Only an authoritative ref target prevents release.
    await store.delete(snapshotPinKey(commitSha));
    await store.delete(materializedSnapshotKey(commitSha));
    await store.put(releasedSnapshotKey(commitSha), {
      commitSha,
      releasedAt: Date.now(),
      qualificationOwned: pin.qualificationOwned || options?.qualificationOwned,
    });
    return { released: true };
  });
}

export async function listAcceptedWriteJournalState(
  ctx: DurableObjectState
): Promise<AcceptedWriteJournalEntry[]> {
  const entries = await ctx.storage.list<AcceptedWriteJournalEntry>({ prefix: "acceptedWrite:" });
  return [...entries.values()];
}

export async function getSnapshotProjectionState(
  ctx: DurableObjectState,
  ref: string
): Promise<SnapshotProjectionState> {
  const snapshots = await ctx.storage.list({ prefix: "materializedSnapshot:" });
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  return {
    snapshotCount: snapshots.size,
    current: await store.get(snapshotCurrentKey(ref)),
  };
}

/**
 * Removes snapshot projections owned by disposable qualification refs. The
 * accepted-write journal is the canonical link between a run ref and every
 * commit projection it may have created, including after the current pointer
 * has advanced to the deletion OID.
 */
export async function clearQualificationSnapshotProjectionState(
  storage: DurableObjectTransaction,
  authoritativeRefs: Array<{ name: string; oid: string }>
): Promise<number> {
  const qualificationPrefix = "refs/heads/qual-";
  const journal = await storage.list<AcceptedWriteJournalEntry>({ prefix: "acceptedWrite:" });
  const disposableOids = new Set(
    [...journal.values()]
      .filter((entry) => entry.fact.ref.startsWith(qualificationPrefix))
      .map((entry) => entry.fact.afterSha)
      .filter((oid) => oid !== zeroOid())
  );
  const currents = await storage.list<SnapshotCurrent>({ prefix: "snapshotCurrent:" });
  const pins = await storage.list<SnapshotPin>({ prefix: "snapshotPin:" });
  const released = await storage.list<{ qualificationOwned?: boolean }>({
    prefix: "releasedSnapshot:",
  });
  const deletedCurrentKeys: string[] = [];
  const retainedOids = new Set(authoritativeRefs.map((ref) => ref.oid));
  const qualificationPinOids = new Set(
    [...pins.values()]
      .filter((pin) => pin.qualificationOwned || pin.ref.startsWith(qualificationPrefix))
      .map((pin) => pin.commitSha)
  );
  for (const [key, current] of currents) {
    if (
      current.ref.startsWith(qualificationPrefix) ||
      (qualificationPinOids.has(current.commitSha) && !retainedOids.has(current.commitSha))
    ) {
      deletedCurrentKeys.push(key);
      if (current.commitSha !== zeroOid()) disposableOids.add(current.commitSha);
    } else if (current.commitSha !== zeroOid()) {
      retainedOids.add(current.commitSha);
    }
  }
  for (const oid of qualificationPinOids) {
    if (!retainedOids.has(oid)) disposableOids.add(oid);
  }
  const snapshotKeys = [...disposableOids]
    .filter((oid) => !retainedOids.has(oid))
    .map((oid) => materializedSnapshotKey(oid));
  const pinKeys = [...pins.entries()]
    .filter(
      ([, pin]) =>
        (pin.qualificationOwned || pin.ref.startsWith(qualificationPrefix)) &&
        !retainedOids.has(pin.commitSha)
    )
    .map(([key]) => key);
  const releasedKeys = [...released.entries()]
    .filter(([, tombstone]) => tombstone.qualificationOwned)
    .map(([key]) => key);
  const keys = [...deletedCurrentKeys, ...snapshotKeys, ...pinKeys, ...releasedKeys];
  if (keys.length > 0) await storage.delete(keys);
  return keys.length;
}

export async function projectAcceptedWriteState(
  ctx: DurableObjectState,
  args: {
    entryId: string;
    commitSha: string;
    treeSha: string;
    materializedAt: number;
    readerToken?: string;
  }
): Promise<AcceptedWriteProjectionResult> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return { status: "repository-deleting" };
    const entryKey = acceptedWriteJournalKey(args.entryId);
    const entry = await store.get(entryKey);
    if (!entry || entry.fact.afterSha !== args.commitSha) {
      throw new Error("accepted-write journal entry does not match snapshot");
    }
    const isDeletion = args.commitSha === zeroOid();
    if (!isDeletion && !isValidPinnedObjectSha(args.treeSha)) {
      return { status: "invalid-snapshot" };
    }
    const existingPin = isDeletion ? undefined : await store.get(snapshotPinKey(args.commitSha));
    if (!isDeletion && !existingPin && !(await hasActiveProjectionLease(store, args.readerToken))) {
      return { status: "projection-lease-expired" };
    }
    const snapshotKey = materializedSnapshotKey(args.commitSha);
    const existingSnapshot = isDeletion ? undefined : await store.get(snapshotKey);
    if (!isDeletion) {
      const retained = await retainSnapshotPin(
        store,
        entry.sequence,
        {
          ref: entry.fact.ref,
          beforeSha: entry.fact.beforeSha,
          commitSha: entry.fact.afterSha,
          actor: entry.fact.actor,
          sourceSurface: entry.fact.sourceSurface,
          idempotencyKey: entry.fact.idempotencyKey,
        },
        {
          commitSha: args.commitSha,
          treeSha: args.treeSha,
          materializedAt: args.materializedAt,
        }
      );
      if (!retained) return { status: "invalid-snapshot" };
    }
    if (!entry.materializedAt) {
      await store.put(entryKey, { ...entry, materializedAt: args.materializedAt });
    }

    const currentKey = snapshotCurrentKey(entry.fact.ref);
    const current = await store.get(currentKey);
    let nextCurrent = current;
    let pointerAdvanced = false;
    if (!current || entry.sequence > current.sequence) {
      nextCurrent = {
        ref: entry.fact.ref,
        commitSha: entry.fact.afterSha,
        sequence: entry.sequence,
        updatedAt: args.materializedAt,
      };
      await store.put(currentKey, nextCurrent);
      pointerAdvanced = true;
    } else if (entry.sequence === current.sequence && entry.fact.afterSha !== current.commitSha) {
      throw new Error("snapshot current-pointer sequence conflict");
    }
    return {
      snapshotCreated: !isDeletion && !existingSnapshot,
      pointerAdvanced,
      current: nextCurrent!,
    };
  });
}

export type ReconciledHeadProjectionResult =
  | ({ status: "projected" } & SnapshotProjectionResult)
  | {
      status: "stale" | "repository-deleting" | "invalid-snapshot" | "projection-lease-expired";
    };

export async function projectReconciledHeadState(
  ctx: DurableObjectState,
  args: {
    ref: string;
    commitSha: string;
    treeSha: string;
    sequence: number;
    materializedAt: number;
    readerToken?: string;
  }
): Promise<ReconciledHeadProjectionResult> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return { status: "repository-deleting" };
    const acceptedHead = await store.get(acceptedWriteHeadKey(args.ref));
    const refs = (await store.get("refs")) ?? [];
    const authoritative = refs.find((ref) => ref.name === args.ref)?.oid ?? zeroOid();
    const refsVersion = (await store.get("refsVersion")) ?? 0;
    const sequenceMatches =
      acceptedHead?.afterSha === authoritative
        ? acceptedHead.sequence === args.sequence
        : refsVersion === args.sequence;
    if (authoritative !== args.commitSha || !sequenceMatches) {
      return { status: "stale" };
    }
    const isDeletion = args.commitSha === zeroOid();
    if (!isDeletion && !isValidPinnedObjectSha(args.treeSha)) {
      return { status: "invalid-snapshot" };
    }
    const existingPin = isDeletion ? undefined : await store.get(snapshotPinKey(args.commitSha));
    if (!isDeletion && !existingPin && !(await hasActiveProjectionLease(store, args.readerToken))) {
      return { status: "projection-lease-expired" };
    }
    const snapshotKey = materializedSnapshotKey(args.commitSha);
    const existingSnapshot = isDeletion ? undefined : await store.get(snapshotKey);
    if (!isDeletion) {
      const retained = await retainSnapshotPin(
        store,
        args.sequence,
        {
          ref: args.ref,
          beforeSha: zeroOid(),
          commitSha: args.commitSha,
          actor: "reconcile",
          sourceSurface: "reconcile",
          idempotencyKey: null,
        },
        {
          commitSha: args.commitSha,
          treeSha: args.treeSha,
          materializedAt: args.materializedAt,
        }
      );
      if (!retained) return { status: "invalid-snapshot" };
    }
    const currentKey = snapshotCurrentKey(args.ref);
    const current = await store.get(currentKey);
    let nextCurrent = current;
    let pointerAdvanced = false;
    if (!current || args.sequence > current.sequence) {
      nextCurrent = {
        ref: args.ref,
        commitSha: args.commitSha,
        sequence: args.sequence,
        updatedAt: args.materializedAt,
      };
      await store.put(currentKey, nextCurrent);
      pointerAdvanced = true;
    } else if (args.sequence === current.sequence && args.commitSha !== current.commitSha) {
      throw new Error("snapshot current-pointer sequence conflict");
    }
    return {
      status: "projected",
      snapshotCreated: !isDeletion && !existingSnapshot,
      pointerAdvanced,
      current: nextCurrent!,
    };
  });
}

export async function getSnapshotReconcilePlanState(
  ctx: DurableObjectState,
  ref: string
): Promise<SnapshotReconcilePlan> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const acceptedHead = await store.get(acceptedWriteHeadKey(ref));
  const refs = (await store.get("refs")) ?? [];
  const afterSha = refs.find((candidate) => candidate.name === ref)?.oid ?? zeroOid();
  if (!acceptedHead && afterSha === zeroOid()) return { status: "unborn", ref };
  const acceptedHeadIsCurrent = acceptedHead?.afterSha === afterSha;
  const sequence = acceptedHeadIsCurrent
    ? acceptedHead.sequence
    : ((await store.get("refsVersion")) ?? 0);
  const current = await store.get(snapshotCurrentKey(ref));
  if (current?.commitSha === afterSha && current.sequence === sequence) {
    return { status: "up_to_date", current };
  }
  const journal = await listAcceptedWriteJournalState(ctx);
  const entry = [...journal]
    .reverse()
    .find(
      (candidate) =>
        candidate.sequence === sequence &&
        candidate.fact.ref === ref &&
        candidate.fact.afterSha === afterSha
    );
  if (entry) return { status: "deliver", entry };
  return {
    status: "head_only",
    ref,
    afterSha,
    sequence,
    beforeSha: current?.commitSha ?? zeroOid(),
  };
}

export async function dropAcceptedWriteJournalEntry(
  ctx: DurableObjectState,
  entryId: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    return Boolean(await store.delete(acceptedWriteJournalKey(entryId)));
  });
}

export const __test = {
  async dropJournalEntry(ctx: DurableObjectState, entryId: string): Promise<void> {
    await dropAcceptedWriteJournalEntry(ctx, entryId);
  },
};
