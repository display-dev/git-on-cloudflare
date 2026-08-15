import type { Logger } from "@/worker/common/logger";
import type { IngestionReceipt, ReceiveCommitOutcome, RepoStateSchema } from "../repoState";

import { asTypedStorage, ingestionReceiptKey, receiveOutcomeKey } from "../repoState";
import {
  applyReceiveCommands,
  isValidRefName,
  type ReceiveCommand,
  type ReceiveStatus,
  validateReceiveCommands,
} from "@/worker/git/operations/validation";
import { getDb, listActivePackCatalog, upsertPackCatalogRow } from "../db";
import {
  DEFAULT_HEAD,
  RECEIVE_LEASE_TTL_MS,
  bumpPacksetVersion,
  ensureRepoMetadataDefaults,
} from "./shared";
import { catalogNeedsCompaction, scheduleCompactionWake } from "./compaction/plan";
import { acceptedWritesMatchCommands, recordAcceptedWrites } from "../acceptedWrites";
import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import { snapshotEventProbeEnabled } from "@/worker/git/snapshot/config";

export type FinalizeReceiveResult =
  | {
      status: "committed";
      statuses: ReceiveStatus[];
      changed: boolean;
      empty: boolean;
      shouldQueueCompaction: boolean;
    }
  | {
      status: "ref_conflict";
      statuses: ReceiveStatus[];
      message: string;
    }
  | {
      status: "lease_mismatch";
      message: string;
    };

export type ReconcileReceiveResult =
  | { status: "committed"; result: Extract<FinalizeReceiveResult, { status: "committed" }> }
  | { status: "aborted" }
  | { status: "unknown" };

const MAX_INGESTION_RECEIPTS = 128;
const MAX_RECEIVE_OUTCOMES = 128;
let skipNextReceiptStoreForTesting = false;
let failNextAfterOutcomeStoreForTesting = false;
let failNextOutcomeIndexStoreForTesting = false;

export const __test = {
  skipNextReceiptStore(): void {
    skipNextReceiptStoreForTesting = true;
  },
  failNextAfterOutcomeStore(): void {
    failNextAfterOutcomeStoreForTesting = true;
  },
  failNextOutcomeIndexStore(): void {
    failNextOutcomeIndexStoreForTesting = true;
  },
  reset(): void {
    skipNextReceiptStoreForTesting = false;
    failNextAfterOutcomeStoreForTesting = false;
    failNextOutcomeIndexStoreForTesting = false;
  },
};

async function storeReceiveOutcome(
  storage: DurableObjectStorage,
  outcome: ReceiveCommitOutcome
): Promise<void> {
  await storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const key = receiveOutcomeKey(outcome.token);
    await store.put(key, outcome);
    if (failNextOutcomeIndexStoreForTesting) {
      failNextOutcomeIndexStoreForTesting = false;
      throw new Error("injected receive outcome index failure");
    }
    const index = (await store.get("receiveOutcomeIndex")) ?? [];
    const nextIndex = [...index.filter((token) => token !== outcome.token), outcome.token];
    while (nextIndex.length > MAX_RECEIVE_OUTCOMES) {
      const removedToken = nextIndex.shift();
      if (removedToken) await store.delete(receiveOutcomeKey(removedToken));
    }
    await store.put("receiveOutcomeIndex", nextIndex);
  });
}

async function storeIngestionReceipt(
  storage: DurableObjectStorage,
  receipt: IngestionReceipt
): Promise<void> {
  if (skipNextReceiptStoreForTesting) {
    skipNextReceiptStoreForTesting = false;
    return;
  }
  await storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const key = ingestionReceiptKey(receipt.keyHash);
    const existing = await store.get(key);
    if (existing && existing.fingerprint !== receipt.fingerprint) {
      throw new Error("ingestion receipt fingerprint conflict");
    }
    await store.put(key, existing ?? receipt);
    const index = (await store.get("ingestionReceiptIndex")) ?? [];
    const nextIndex = [...index.filter((keyHash) => keyHash !== receipt.keyHash), receipt.keyHash];
    while (nextIndex.length > MAX_INGESTION_RECEIPTS) {
      const removedHash = nextIndex.shift();
      if (removedHash) await store.delete(ingestionReceiptKey(removedHash));
    }
    await store.put("ingestionReceiptIndex", nextIndex);
  });
}

async function clearMatchingReceiveLease(
  store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
  token: string
): Promise<void> {
  const lease = await store.get("receiveLease");
  if (lease?.token === token) await store.delete("receiveLease");
}

async function holdReceiveCompletionFence(
  ctx: DurableObjectState,
  token: string,
  allowRecovery: boolean
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    const now = Date.now();
    const lease = await store.get("receiveLease");
    if (lease && lease.token !== token && lease.expiresAt > now) return false;
    if (lease?.token === token && lease.expiresAt <= now && !allowRecovery) return false;
    if ((!lease || lease.token !== token) && !allowRecovery) return false;
    await store.put("receiveLease", {
      token,
      createdAt: lease?.token === token ? lease.createdAt : now,
      expiresAt: now + RECEIVE_LEASE_TTL_MS,
    });
    return true;
  });
}

export async function getIngestionReceiptState(
  ctx: DurableObjectState,
  keyHash: string
): Promise<IngestionReceipt | null> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  return (await store.get(ingestionReceiptKey(keyHash))) ?? null;
}

export async function reconcileReceiveState(
  ctx: DurableObjectState,
  args: {
    token: string;
    commands: ReceiveCommand[];
    stagedPackKey?: string | undefined;
    ingestionReceipt?: IngestionReceipt | undefined;
  }
): Promise<ReconcileReceiveResult> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const outcome = await store.get(receiveOutcomeKey(args.token));
  if (!(await holdReceiveCompletionFence(ctx, args.token, Boolean(outcome)))) {
    return { status: "unknown" };
  }
  if (outcome) {
    await storeReceiveOutcome(ctx.storage, outcome);
    if (args.ingestionReceipt) await storeIngestionReceipt(ctx.storage, args.ingestionReceipt);
    await clearMatchingReceiveLease(store, args.token);
    return {
      status: "committed",
      result: {
        status: "committed",
        statuses: outcome.statuses,
        changed: outcome.changed,
        empty: outcome.empty,
        shouldQueueCompaction: outcome.shouldQueueCompaction,
      },
    };
  }

  const lease = await store.get("receiveLease");
  if (lease?.token !== args.token) return { status: "unknown" };

  const refs = (await store.get("refs")) ?? [];
  const refsMatch = args.commands.every((command) => {
    const current = refs.find((ref) => ref.name === command.ref)?.oid;
    return /^0{40}$/i.test(command.newOid) ? current === undefined : current === command.newOid;
  });
  const activeCatalog = await listActivePackCatalog(getDb(ctx.storage));
  const stagedPackActive = args.stagedPackKey
    ? activeCatalog.some((pack) => pack.packKey === args.stagedPackKey)
    : true;

  if (refsMatch && stagedPackActive) {
    const result: Extract<FinalizeReceiveResult, { status: "committed" }> = {
      status: "committed",
      statuses: args.commands.map((command) => ({ ref: command.ref, ok: true })),
      changed: args.commands.length > 0,
      empty: refs.length === 0,
      shouldQueueCompaction: catalogNeedsCompaction(activeCatalog),
    };
    await storeReceiveOutcome(ctx.storage, {
      token: args.token,
      statuses: result.statuses,
      changed: result.changed,
      empty: result.empty,
      shouldQueueCompaction: result.shouldQueueCompaction,
    });
    if (args.ingestionReceipt) await storeIngestionReceipt(ctx.storage, args.ingestionReceipt);
    await store.delete("receiveLease");
    return { status: "committed", result };
  }

  // A catalog row without matching refs is a partial/unknown finalize. Keep
  // both the lease and staged data for operator reconciliation.
  if (args.stagedPackKey && stagedPackActive) return { status: "unknown" };
  await store.delete("receiveLease");
  return { status: "aborted" };
}

function resolveHeadAfterReceive(args: {
  storedHead:
    | {
        target: string;
        oid?: string;
        unborn?: boolean;
      }
    | undefined;
  refs: Array<{ name: string; oid: string }>;
}) {
  const target = args.storedHead?.target || DEFAULT_HEAD.target;
  const match = args.refs.find((ref) => ref.name === target);
  if (match) {
    return { target, oid: match.oid } as const;
  }
  return { target, unborn: true } as const;
}

export async function finalizeReceiveState(args: {
  ctx: DurableObjectState;
  env: Env;
  token: string;
  commands: ReceiveCommand[];
  stagedPack?:
    | {
        packKey: string;
        packBytes: number;
        idxBytes: number;
        objectCount: number;
      }
    | undefined;
  ingestionReceipt?: IngestionReceipt | undefined;
  acceptedWrites?: AcceptedWriteFact[] | undefined;
  logger?: Logger;
}): Promise<FinalizeReceiveResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  if (!(await holdReceiveCompletionFence(args.ctx, args.token, false))) {
    return {
      status: "lease_mismatch",
      message: "Receive lease is no longer active for this request.",
    };
  }
  await ensureRepoMetadataDefaults(store);

  const priorOutcome = await store.get(receiveOutcomeKey(args.token));
  if (priorOutcome) {
    await storeReceiveOutcome(args.ctx.storage, priorOutcome);
    if (args.ingestionReceipt) {
      await storeIngestionReceipt(args.ctx.storage, args.ingestionReceipt);
    }
    await clearMatchingReceiveLease(store, args.token);
    return {
      status: "committed",
      statuses: priorOutcome.statuses,
      changed: priorOutcome.changed,
      empty: priorOutcome.empty,
      shouldQueueCompaction: priorOutcome.shouldQueueCompaction,
    };
  }

  const lease = await store.get("receiveLease");
  if (!lease || lease.token !== args.token || lease.expiresAt <= Date.now()) {
    if (lease?.token === args.token) await store.delete("receiveLease");
    return {
      status: "lease_mismatch",
      message: "Receive lease is no longer active for this request.",
    };
  }

  const currentRefs = (await store.get("refs")) || [];
  const invalidStatuses = args.commands
    .filter((command) => !isValidRefName(command.ref))
    .map((command) => ({ ref: command.ref, ok: false, msg: "invalid" satisfies string }));
  if (invalidStatuses.length > 0) {
    await store.delete("receiveLease");
    args.logger?.warn("receive:finalize-invalid-ref", {
      invalidCount: invalidStatuses.length,
    });
    return {
      status: "ref_conflict",
      statuses: invalidStatuses,
      message: "Receive finalization rejected invalid refs.",
    };
  }

  const statuses = validateReceiveCommands(currentRefs, args.commands);
  if (!statuses.every((status) => status.ok)) {
    await store.delete("receiveLease");
    args.logger?.warn("receive:finalize-ref-conflict", {
      conflictCount: statuses.filter((status) => !status.ok).length,
    });
    return {
      status: "ref_conflict",
      statuses,
      message: "Ref expectations changed before the receive could be committed.",
    };
  }

  const nextRefs = applyReceiveCommands(currentRefs, args.commands);
  const storedHead = await store.get("head");
  const nextHead = resolveHeadAfterReceive({ storedHead, refs: nextRefs });
  const nextRefsVersion = ((await store.get("refsVersion")) || 0) + 1;
  if (args.acceptedWrites && !acceptedWritesMatchCommands(args.commands, args.acceptedWrites)) {
    throw new Error("accepted-write facts do not match receive commands");
  }

  let shouldQueueCompaction = false;
  if (args.stagedPack) {
    const nextPackSeq = (await store.get("nextPackSeq")) || 1;
    const db = getDb(args.ctx.storage);
    await upsertPackCatalogRow(db, {
      packKey: args.stagedPack.packKey,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: nextPackSeq,
      seqHi: nextPackSeq,
      objectCount: args.stagedPack.objectCount,
      packBytes: args.stagedPack.packBytes,
      idxBytes: args.stagedPack.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    });
    await store.put("nextPackSeq", nextPackSeq + 1);
    const activeCatalog = await listActivePackCatalog(db);
    await bumpPacksetVersion(store);
    shouldQueueCompaction = catalogNeedsCompaction(activeCatalog);
    if (shouldQueueCompaction) {
      await store.put("compactionWantedAt", Date.now());
      await scheduleCompactionWake(args.ctx, args.env);
    }
  }

  const committed: ReceiveCommitOutcome = {
    token: args.token,
    statuses,
    changed: args.commands.length > 0,
    empty: nextRefs.length === 0,
    shouldQueueCompaction,
  };

  await args.ctx.storage.transaction(async (transaction) => {
    const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
    await transactionStore.put("refs", nextRefs);
    await transactionStore.put("head", nextHead);
    await transactionStore.put("refsVersion", nextRefsVersion);
    if (args.acceptedWrites) {
      await recordAcceptedWrites(
        transactionStore,
        nextRefsVersion,
        args.acceptedWrites,
        Date.now(),
        snapshotEventProbeEnabled(args.env)
      );
    }
  });
  await storeReceiveOutcome(args.ctx.storage, committed);
  if (failNextAfterOutcomeStoreForTesting) {
    failNextAfterOutcomeStoreForTesting = false;
    throw new Error("injected post-outcome finalize failure");
  }
  if (args.ingestionReceipt) await storeIngestionReceipt(args.ctx.storage, args.ingestionReceipt);
  await store.delete("receiveLease");

  args.logger?.info("receive:finalize-committed", {
    commandCount: args.commands.length,
    refCount: nextRefs.length,
    empty: nextRefs.length === 0,
    stagedPackKey: args.stagedPack?.packKey,
    shouldQueueCompaction,
  });

  return {
    status: "committed",
    statuses,
    changed: committed.changed,
    empty: committed.empty,
    shouldQueueCompaction,
  };
}
