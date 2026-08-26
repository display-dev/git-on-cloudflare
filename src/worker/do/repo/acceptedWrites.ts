import { zeroOid } from "@/worker/common";
import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type { ReceiveCommand } from "@/worker/git/operations/validation";
import {
  acceptedWriteJournalKey,
  acceptedWriteHeadKey,
  asTypedStorage,
  materializedSnapshotKey,
  snapshotCurrentKey,
  type AcceptedWriteJournalEntry,
  type RepoStateSchema,
  type SnapshotCurrent,
  type TypedStorage,
} from "./repoState";

export type SnapshotProjectionResult = {
  snapshotCreated: boolean;
  pointerAdvanced: boolean;
  current: SnapshotCurrent;
};

export type AcceptedWriteProjectionResult =
  | SnapshotProjectionResult
  | { status: "repository-deleting" };

export type SnapshotProjectionState = {
  snapshotCount: number;
  current?: SnapshotCurrent;
};

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
  journal: boolean
): Promise<void> {
  if (facts.length === 0) return;
  for (const fact of facts) {
    await store.put(acceptedWriteHeadKey(fact.ref), {
      ref: fact.ref,
      beforeSha: fact.beforeSha,
      afterSha: fact.afterSha,
      sequence,
    });
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
  const deletedCurrentKeys: string[] = [];
  const retainedOids = new Set(authoritativeRefs.map((ref) => ref.oid));
  for (const [key, current] of currents) {
    if (current.ref.startsWith(qualificationPrefix)) {
      deletedCurrentKeys.push(key);
      if (current.commitSha !== zeroOid()) disposableOids.add(current.commitSha);
    } else if (current.commitSha !== zeroOid()) {
      retainedOids.add(current.commitSha);
    }
  }
  const snapshotKeys = [...disposableOids]
    .filter((oid) => !retainedOids.has(oid))
    .map((oid) => materializedSnapshotKey(oid));
  const keys = [...deletedCurrentKeys, ...snapshotKeys];
  if (keys.length > 0) await storage.delete(keys);
  return keys.length;
}

export async function projectAcceptedWriteState(
  ctx: DurableObjectState,
  args: { entryId: string; commitSha: string; materializedAt: number }
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
    const snapshotKey = materializedSnapshotKey(args.commitSha);
    const existingSnapshot = isDeletion ? undefined : await store.get(snapshotKey);
    if (!isDeletion && !existingSnapshot) {
      await store.put(snapshotKey, {
        commitSha: args.commitSha,
        firstSequence: entry.sequence,
        materializedAt: args.materializedAt,
      });
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
  | { status: "stale" | "repository-deleting" };

export async function projectReconciledHeadState(
  ctx: DurableObjectState,
  args: { ref: string; commitSha: string; sequence: number; materializedAt: number }
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
    const snapshotKey = materializedSnapshotKey(args.commitSha);
    const existingSnapshot = isDeletion ? undefined : await store.get(snapshotKey);
    if (!isDeletion && !existingSnapshot) {
      await store.put(snapshotKey, {
        commitSha: args.commitSha,
        firstSequence: args.sequence,
        materializedAt: args.materializedAt,
      });
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
