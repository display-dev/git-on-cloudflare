import type { Logger } from "@/worker/common/logger";
import type {
  IngestionReceipt,
  ReceiveCommitOutcome,
  ReceiveFinalizeIntent,
  RepoStateSchema,
  TypedStorage,
} from "../repoState";

import {
  asTypedStorage,
  ingestionReceiptKey,
  receiveFinalizeIntentKey,
  receiveOutcomeKey,
} from "../repoState";
import {
  applyReceiveCommands,
  isValidRefName,
  type ReceiveCommand,
  type ReceiveStatus,
  validateReceiveCommands,
} from "@/worker/git/operations/validation";
import { getDb, listActivePackCatalog, upsertPackCatalogRow } from "../db";
import { DEFAULT_HEAD, RECEIVE_LEASE_TTL_MS, ensureRepoMetadataDefaults } from "./shared";
import { catalogNeedsCompaction, scheduleCompactionWake } from "./compaction/plan";
import { acceptedWritesMatchCommands, recordAcceptedWrites } from "../acceptedWrites";
import {
  acceptedWriteFactsForCommands,
  type AcceptedWriteContext,
  type AcceptedWriteFact,
} from "@/worker/git/acceptedWrite";
import { snapshotEventProbeEnabled } from "@/worker/git/snapshot/config";
import { packIndexKey, packRefsKey } from "@/worker/keys";
import { SubrequestLimiter } from "@/worker/git/operations/limits";
import { bytesToHex, createDigestStream } from "@/worker/common";
import {
  RECOVERY_ESCALATION_ATTEMPTS,
  recoveryRetryDelayMs,
  scheduleAlarmIfSooner,
} from "../scheduler";

export type FinalizeReceiveResult =
  | {
      status: "committed";
      statuses: ReceiveStatus[];
      changed: boolean;
      empty: boolean;
      shouldQueueCompaction: boolean;
      outputValidationBytes: number;
      outputValidationRequests: number;
      outputEtags?: { pack: string; idx: string; refs: string };
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

export class ReceiveOutputIntegrityError extends Error {
  readonly bytes: number;
  readonly requests: number;
  readonly role: "pack" | "index" | "references";

  constructor(
    message: string,
    details: {
      bytes: number;
      requests: number;
      role: "pack" | "index" | "references";
    }
  ) {
    super(message);
    this.name = "ReceiveOutputIntegrityError";
    this.bytes = details.bytes;
    this.requests = details.requests;
    this.role = details.role;
  }
}

export type ReceiveFinalizeMilestone = {
  phase: "output-integrity-verified" | "wal-put-complete" | "authoritative-ref-cas";
  durable?: boolean | undefined;
  bytes?: number | undefined;
};

async function verifyReceiveOutputIntegrity(
  env: Env,
  stagedPack: ReceiveFinalizeIntent["stagedPack"],
  logger?: Logger
): Promise<{
  bytes: number;
  requests: number;
  etags?: { pack: string; idx: string; refs: string };
}> {
  if (!stagedPack?.integrity) return { bytes: 0, requests: 0 };
  const limiter = new SubrequestLimiter(3);
  const artifacts = [
    {
      role: "pack",
      key: stagedPack.packKey,
      bytes: stagedPack.packBytes,
      sha256: stagedPack.integrity.packSha256,
      expectedEtag: stagedPack.integrity.packEtag,
    },
    {
      role: "index",
      key: packIndexKey(stagedPack.packKey),
      bytes: stagedPack.idxBytes,
      sha256: stagedPack.integrity.idxSha256,
      expectedEtag: stagedPack.integrity.idxEtag,
    },
    {
      role: "references",
      key: packRefsKey(stagedPack.packKey),
      bytes: stagedPack.integrity.refsBytes,
      sha256: stagedPack.integrity.refsSha256,
      expectedEtag: stagedPack.integrity.refsEtag,
    },
  ] as const;
  const etags: string[] = [];
  let validatedBytes = 0;
  let validationRequests = 0;
  for (const artifact of artifacts) {
    const reject = (message: string): ReceiveOutputIntegrityError =>
      new ReceiveOutputIntegrityError(message, {
        bytes: validatedBytes,
        requests: validationRequests,
        role: artifact.role,
      });
    if (artifact.bytes <= 0 || artifact.bytes > 32 * 1024 * 1024) {
      throw reject(`receive ${artifact.role} output exceeds integrity verification bound`);
    }
    validationRequests++;
    const object = await limiter.run(`r2:verify-receive-${artifact.role}`, () =>
      env.REPO_BUCKET.get(artifact.key)
    );
    if (
      !object ||
      object.size !== artifact.bytes ||
      object.customMetadata?.sha256 !== artifact.sha256 ||
      (artifact.expectedEtag !== undefined && object.etag !== artifact.expectedEtag)
    ) {
      throw reject(`receive ${artifact.role} output integrity mismatch`);
    }
    etags.push(object.etag);
    const digest = createDigestStream("SHA-256");
    const digestWriter = digest.getWriter();
    const reader = object.body.getReader();
    let readBytes = 0;
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      validatedBytes += next.value.byteLength;
      if (next.value.byteLength > artifact.bytes - readBytes) {
        await reader.cancel("receive output exceeded declared size").catch(() => {});
        await digestWriter.abort("receive output exceeded declared size").catch(() => {});
        throw reject(`receive ${artifact.role} output exceeded declared size`);
      }
      readBytes += next.value.byteLength;
      await digestWriter.write(next.value);
    }
    if (readBytes !== artifact.bytes) {
      await digestWriter.abort("receive output was truncated").catch(() => {});
      throw reject(`receive ${artifact.role} output was truncated`);
    }
    await digestWriter.close();
    const actualSha256 = bytesToHex(new Uint8Array(await digest.digest));
    if (actualSha256 !== artifact.sha256) {
      throw reject(`receive ${artifact.role} output digest mismatch`);
    }
    logger?.debug("receive:output-integrity-read", {
      role: artifact.role,
      bytes: artifact.bytes,
    });
  }
  logger?.info("receive:output-integrity-verified", { artifactCount: artifacts.length });
  return {
    bytes: validatedBytes,
    requests: validationRequests,
    etags: {
      pack: etags[0]!,
      idx: etags[1]!,
      refs: etags[2]!,
    },
  };
}

const MAX_INGESTION_RECEIPTS = 128;
const MAX_RECEIVE_OUTCOMES = 128;
let skipNextReceiptStoreForTesting = false;
let failNextAfterOutcomeStoreForTesting = false;
let failNextOutcomeIndexStoreForTesting = false;
let failNextAfterCatalogActivationForTesting = false;
let failNextAfterCatalogUpsertForTesting = false;
let catalogUpsertFailureCountForTesting = 0;
let catalogActivationFailureCountForTesting = 0;
let outcomeStoreFailureCountForTesting = 0;
let suppressCompactionSchedulingForTesting = false;

function acceptedWriteContextForFacts(
  facts: AcceptedWriteFact[] | undefined
): AcceptedWriteContext | undefined {
  const first = facts?.[0];
  if (!first) return undefined;
  const context: AcceptedWriteContext = {
    repositoryId: first.repositoryId,
    actor: first.actor,
    sourceSurface: first.sourceSurface,
    idempotencyKey: first.idempotencyKey,
  };
  if (
    !facts.every(
      (fact) =>
        fact.repositoryId === context.repositoryId &&
        fact.actor === context.actor &&
        fact.sourceSurface === context.sourceSurface &&
        fact.idempotencyKey === context.idempotencyKey
    )
  ) {
    throw new Error("accepted-write facts do not share one receive context");
  }
  return context;
}

function acceptedWriteFactsForIntent(
  intent: Pick<ReceiveFinalizeIntent, "commands" | "acceptedWriteContext">
): AcceptedWriteFact[] | undefined {
  if (!intent.acceptedWriteContext) return undefined;
  return acceptedWriteFactsForCommands({
    ...intent.acceptedWriteContext,
    commands: intent.commands,
  });
}

export const __test = {
  suppressCompactionScheduling(): void {
    suppressCompactionSchedulingForTesting = true;
  },
  compactionSchedulingSuppressed(): boolean {
    return suppressCompactionSchedulingForTesting;
  },
  skipNextReceiptStore(): void {
    skipNextReceiptStoreForTesting = true;
  },
  failNextAfterOutcomeStore(): void {
    failNextAfterOutcomeStoreForTesting = true;
  },
  failNextOutcomeIndexStore(): void {
    failNextOutcomeIndexStoreForTesting = true;
  },
  failNextAfterCatalogActivation(): void {
    failNextAfterCatalogActivationForTesting = true;
  },
  failNextAfterCatalogUpsert(): void {
    failNextAfterCatalogUpsertForTesting = true;
  },
  consumedFailureCounts(): {
    catalogUpsert: number;
    catalogActivation: number;
    outcomeStore: number;
  } {
    return {
      catalogUpsert: catalogUpsertFailureCountForTesting,
      catalogActivation: catalogActivationFailureCountForTesting,
      outcomeStore: outcomeStoreFailureCountForTesting,
    };
  },
  reset(): void {
    skipNextReceiptStoreForTesting = false;
    failNextAfterOutcomeStoreForTesting = false;
    failNextOutcomeIndexStoreForTesting = false;
    failNextAfterCatalogActivationForTesting = false;
    failNextAfterCatalogUpsertForTesting = false;
    catalogUpsertFailureCountForTesting = 0;
    catalogActivationFailureCountForTesting = 0;
    outcomeStoreFailureCountForTesting = 0;
    suppressCompactionSchedulingForTesting = false;
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
  store: TypedStorage<RepoStateSchema>,
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
  env: Env,
  args: {
    token: string;
    commands: ReceiveCommand[];
    stagedPackKey?: string | undefined;
    ingestionReceipt?: IngestionReceipt | undefined;
  }
): Promise<ReconcileReceiveResult> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const outcome = await store.get(receiveOutcomeKey(args.token));
  const intent = await store.get(receiveFinalizeIntentKey(args.token));
  if (!(await holdReceiveCompletionFence(ctx, args.token, Boolean(intent || outcome)))) {
    return { status: "unknown" };
  }
  if (outcome) {
    await storeReceiveOutcome(ctx.storage, outcome);
    if (args.ingestionReceipt) await storeIngestionReceipt(ctx.storage, args.ingestionReceipt);
    if (outcome.shouldQueueCompaction && !suppressCompactionSchedulingForTesting) {
      await store.put("compactionWantedAt", Date.now());
      await scheduleCompactionWake(ctx, env);
    }
    await clearMatchingReceiveLease(store, args.token);
    return {
      status: "committed",
      result: {
        status: "committed",
        statuses: outcome.statuses,
        changed: outcome.changed,
        empty: outcome.empty,
        shouldQueueCompaction: outcome.shouldQueueCompaction,
        outputValidationBytes: outcome.outputValidationBytes ?? 0,
        outputValidationRequests: outcome.outputValidationRequests ?? 0,
        outputEtags: outcome.outputEtags,
      },
    };
  }

  if (intent) {
    const finalized = await finalizeReceiveState({
      ctx,
      env,
      token: intent.token,
      commands: intent.commands,
      stagedPack: intent.stagedPack,
      ingestionReceipt: intent.ingestionReceipt,
      acceptedWrites: acceptedWriteFactsForIntent(intent),
    });
    if (finalized.status === "committed") {
      return { status: "committed", result: finalized };
    }
    return finalized.status === "ref_conflict" ? { status: "aborted" } : { status: "unknown" };
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
      outputValidationBytes: 0,
      outputValidationRequests: 0,
    };
    await storeReceiveOutcome(ctx.storage, {
      token: args.token,
      statuses: result.statuses,
      changed: result.changed,
      empty: result.empty,
      shouldQueueCompaction: result.shouldQueueCompaction,
      outputValidationBytes: result.outputValidationBytes,
      outputValidationRequests: result.outputValidationRequests,
      outputEtags: result.outputEtags,
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

export async function resumeReceiveFinalizeFromAlarm(args: {
  ctx: DurableObjectState;
  env: Env;
  logger: Logger;
}): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const lease = await store.get("receiveLease");
  if (!lease) return false;
  const intent = await store.get(receiveFinalizeIntentKey(lease.token));
  if (!intent) return false;
  try {
    await finalizeReceiveState({
      ctx: args.ctx,
      env: args.env,
      token: intent.token,
      commands: intent.commands,
      stagedPack: intent.stagedPack,
      ingestionReceipt: intent.ingestionReceipt,
      acceptedWrites: acceptedWriteFactsForIntent(intent),
      logger: args.logger,
    });
  } catch {
    const recovery = await args.ctx.storage.transaction(async (transaction) => {
      const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
      const current = await transactionStore.get(receiveFinalizeIntentKey(intent.token));
      if (!current) return null;
      const attempts = (current.recoveryAttempts ?? 0) + 1;
      const escalate = attempts >= RECOVERY_ESCALATION_ATTEMPTS && !current.recoveryEscalated;
      await transactionStore.put(receiveFinalizeIntentKey(intent.token), {
        ...current,
        recoveryAttempts: attempts,
        recoveryEscalated: current.recoveryEscalated || escalate,
      });
      return { attempts, escalate };
    });
    if (!recovery) return true;
    const fields = { attempts: recovery.attempts, retryable: true };
    if (recovery.escalate) {
      args.logger.error("receive:finalize-recovery-escalated", fields);
    } else {
      args.logger.warn("receive:finalize-recovery-failed", fields);
    }
    await scheduleAlarmIfSooner(
      args.ctx,
      args.env,
      Date.now() + recoveryRetryDelayMs(recovery.attempts)
    );
  }
  return true;
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

function stagedPackMatchesRequest(
  durable: ReceiveFinalizeIntent["stagedPack"],
  requested: ReceiveFinalizeIntent["stagedPack"]
): boolean {
  if (!durable || !requested) return durable === requested;
  const durableIntegrity = durable.integrity
    ? {
        packSha256: durable.integrity.packSha256,
        idxSha256: durable.integrity.idxSha256,
        refsSha256: durable.integrity.refsSha256,
        refsBytes: durable.integrity.refsBytes,
      }
    : undefined;
  return (
    durable.packKey === requested.packKey &&
    durable.packBytes === requested.packBytes &&
    durable.idxBytes === requested.idxBytes &&
    durable.objectCount === requested.objectCount &&
    JSON.stringify(durableIntegrity) === JSON.stringify(requested.integrity)
  );
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
        integrity?: {
          packSha256: string;
          idxSha256: string;
          refsSha256: string;
          refsBytes: number;
          packEtag?: string;
          idxEtag?: string;
          refsEtag?: string;
        };
      }
    | undefined;
  ingestionReceipt?: IngestionReceipt | undefined;
  acceptedWrites?: AcceptedWriteFact[] | undefined;
  logger?: Logger;
  onMilestone?: ((milestone: ReceiveFinalizeMilestone) => Promise<void>) | undefined;
}): Promise<FinalizeReceiveResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const intentKey = receiveFinalizeIntentKey(args.token);
  let intent = await store.get(intentKey);
  const priorOutcome = await store.get(receiveOutcomeKey(args.token));
  if (!(await holdReceiveCompletionFence(args.ctx, args.token, Boolean(intent || priorOutcome)))) {
    return {
      status: "lease_mismatch",
      message: "Receive lease is no longer active for this request.",
    };
  }
  await ensureRepoMetadataDefaults(store);

  if (priorOutcome) {
    await args.onMilestone?.({
      phase: "output-integrity-verified",
      bytes: priorOutcome.outputValidationBytes ?? 0,
    });
    await args.onMilestone?.({ phase: "wal-put-complete", durable: true });
    await args.onMilestone?.({ phase: "authoritative-ref-cas", durable: true });
    await storeReceiveOutcome(args.ctx.storage, priorOutcome);
    if (args.ingestionReceipt) {
      await storeIngestionReceipt(args.ctx.storage, args.ingestionReceipt);
    }
    if (priorOutcome.shouldQueueCompaction && !suppressCompactionSchedulingForTesting) {
      await store.put("compactionWantedAt", Date.now());
      await scheduleCompactionWake(args.ctx, args.env);
    }
    await store.delete(intentKey);
    await clearMatchingReceiveLease(store, args.token);
    return {
      status: "committed",
      statuses: priorOutcome.statuses,
      changed: priorOutcome.changed,
      empty: priorOutcome.empty,
      shouldQueueCompaction: priorOutcome.shouldQueueCompaction,
      outputValidationBytes: priorOutcome.outputValidationBytes ?? 0,
      outputValidationRequests: priorOutcome.outputValidationRequests ?? 0,
      outputEtags: priorOutcome.outputEtags,
    };
  }

  let outputValidationBytes = 0;
  let outputValidationRequests = 0;
  let outputEtags: { pack: string; idx: string; refs: string } | undefined;

  if (!intent) {
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
    if (args.acceptedWrites && !acceptedWritesMatchCommands(args.commands, args.acceptedWrites)) {
      throw new Error("accepted-write facts do not match receive commands");
    }
    const nextRefs = applyReceiveCommands(currentRefs, args.commands);
    const expectedRefsVersion = (await store.get("refsVersion")) || 0;
    const outputVerification = await verifyReceiveOutputIntegrity(
      args.env,
      args.stagedPack,
      args.logger
    );
    outputValidationBytes = outputVerification.bytes;
    outputValidationRequests = outputVerification.requests;
    outputEtags = outputVerification.etags;
    await args.onMilestone?.({
      phase: "output-integrity-verified",
      bytes: outputVerification.bytes,
    });
    const stagedPack =
      args.stagedPack?.integrity && outputEtags
        ? {
            ...args.stagedPack,
            integrity: {
              ...args.stagedPack.integrity,
              packEtag: outputEtags.pack,
              idxEtag: outputEtags.idx,
              refsEtag: outputEtags.refs,
            },
          }
        : args.stagedPack;
    const nextIntent: ReceiveFinalizeIntent = {
      token: args.token,
      commands: args.commands,
      expectedRefsVersion,
      nextHead: resolveHeadAfterReceive({ storedHead: await store.get("head"), refs: nextRefs }),
      nextRefsVersion: expectedRefsVersion + 1,
      stagedPack,
      packSequence: args.stagedPack ? (await store.get("nextPackSeq")) || 1 : undefined,
      nextPacksetVersion: args.stagedPack
        ? ((await store.get("packsetVersion")) || 0) + 1
        : undefined,
      ingestionReceipt: args.ingestionReceipt,
      acceptedWriteContext: acceptedWriteContextForFacts(args.acceptedWrites),
      createdAt: Date.now(),
    };
    intent = nextIntent;
    await args.ctx.storage.transaction(async (transaction) => {
      const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
      await transactionStore.put(intentKey, nextIntent);
      const recoveryAt = Date.now() + 1_000;
      const currentAlarm = await transaction.getAlarm();
      if (currentAlarm === null || currentAlarm > recoveryAt) {
        await transaction.setAlarm(recoveryAt);
      }
    });
    args.logger?.info("receive:wal-put-complete", {
      stockIntegrity: Boolean(nextIntent.stagedPack?.integrity),
    });
    await args.onMilestone?.({ phase: "wal-put-complete", durable: true });
  } else if (
    JSON.stringify(intent.commands) !== JSON.stringify(args.commands) ||
    !stagedPackMatchesRequest(intent.stagedPack, args.stagedPack) ||
    JSON.stringify(intent.acceptedWriteContext) !==
      JSON.stringify(acceptedWriteContextForFacts(args.acceptedWrites))
  ) {
    throw new Error("FUBAR: receive finalize retry does not match durable intent");
  } else {
    const outputVerification = await verifyReceiveOutputIntegrity(
      args.env,
      intent.stagedPack,
      args.logger
    );
    outputValidationBytes = outputVerification.bytes;
    outputValidationRequests = outputVerification.requests;
    outputEtags = outputVerification.etags;
    await args.onMilestone?.({
      phase: "output-integrity-verified",
      bytes: outputVerification.bytes,
    });
    await args.onMilestone?.({ phase: "wal-put-complete", durable: true });
  }

  let shouldQueueCompaction = false;
  if (intent.stagedPack) {
    const db = getDb(args.ctx.storage);
    let activeCatalog = await listActivePackCatalog(db);
    const existingPack = activeCatalog.find((pack) => pack.packKey === intent.stagedPack?.packKey);
    if (
      existingPack &&
      (existingPack.packBytes !== intent.stagedPack.packBytes ||
        existingPack.idxBytes !== intent.stagedPack.idxBytes ||
        existingPack.objectCount !== intent.stagedPack.objectCount ||
        existingPack.seqLo !== intent.packSequence ||
        existingPack.seqHi !== intent.packSequence)
    ) {
      throw new Error("FUBAR: active receive pack metadata does not match finalize intent");
    }
    const alreadyActive = Boolean(existingPack);
    if (!alreadyActive) {
      if (intent.packSequence === undefined || intent.nextPacksetVersion === undefined) {
        throw new Error("FUBAR: receive finalize intent is missing pack sequence state");
      }
      await upsertPackCatalogRow(db, {
        packKey: intent.stagedPack.packKey,
        kind: "receive",
        state: "active",
        tier: 0,
        seqLo: intent.packSequence,
        seqHi: intent.packSequence,
        objectCount: intent.stagedPack.objectCount,
        packBytes: intent.stagedPack.packBytes,
        idxBytes: intent.stagedPack.idxBytes,
        createdAt: Date.now(),
        supersededBy: null,
      });
      if (failNextAfterCatalogUpsertForTesting) {
        failNextAfterCatalogUpsertForTesting = false;
        catalogUpsertFailureCountForTesting++;
        throw new Error("injected post-catalog-upsert receive failure");
      }
      activeCatalog = await listActivePackCatalog(db);
    }
    if (intent.packSequence === undefined || intent.nextPacksetVersion === undefined) {
      throw new Error("FUBAR: receive finalize intent is missing pack sequence state");
    }
    const currentNextPackSeq = (await store.get("nextPackSeq")) || 1;
    if (currentNextPackSeq <= intent.packSequence) {
      await store.put("nextPackSeq", intent.packSequence + 1);
    }
    const currentPacksetVersion = (await store.get("packsetVersion")) || 0;
    if (currentPacksetVersion < intent.nextPacksetVersion) {
      await store.put("packsetVersion", intent.nextPacksetVersion);
    }
    if (failNextAfterCatalogActivationForTesting) {
      failNextAfterCatalogActivationForTesting = false;
      catalogActivationFailureCountForTesting++;
      throw new Error("injected post-catalog receive failure");
    }
    shouldQueueCompaction = catalogNeedsCompaction(activeCatalog);
  }

  let committedRefs: Array<{ name: string; oid: string }> = [];
  const acceptedWrites = acceptedWriteFactsForIntent(intent);
  const statuses = intent.commands.map((command) => ({ ref: command.ref, ok: true }));
  await args.ctx.storage.transaction(async (transaction) => {
    const transactionStore = asTypedStorage<RepoStateSchema>(transaction);
    const currentRefs = (await transactionStore.get("refs")) ?? [];
    const currentRefsVersion = (await transactionStore.get("refsVersion")) || 0;
    const appliedRefs = applyReceiveCommands(currentRefs, intent.commands);
    if (currentRefsVersion === intent.expectedRefsVersion) {
      committedRefs = appliedRefs;
    } else if (
      currentRefsVersion === intent.nextRefsVersion &&
      JSON.stringify(appliedRefs) === JSON.stringify(currentRefs)
    ) {
      committedRefs = currentRefs;
    } else {
      throw new Error("FUBAR: receive refs diverged from durable finalize intent");
    }
    await transactionStore.put("refs", committedRefs);
    await transactionStore.put("head", intent.nextHead);
    await transactionStore.put("refsVersion", intent.nextRefsVersion);
    if (acceptedWrites) {
      await recordAcceptedWrites(
        transactionStore,
        intent.nextRefsVersion,
        acceptedWrites,
        intent.createdAt,
        snapshotEventProbeEnabled(args.env)
      );
    }
  });
  await args.onMilestone?.({ phase: "authoritative-ref-cas", durable: true });
  const committed: ReceiveCommitOutcome = {
    token: args.token,
    statuses,
    changed: intent.commands.length > 0,
    empty: committedRefs.length === 0,
    shouldQueueCompaction,
    outputValidationBytes,
    outputValidationRequests,
    outputEtags,
  };
  await storeReceiveOutcome(args.ctx.storage, committed);
  if (failNextAfterOutcomeStoreForTesting) {
    failNextAfterOutcomeStoreForTesting = false;
    outcomeStoreFailureCountForTesting++;
    throw new Error("injected post-outcome finalize failure");
  }
  if (intent.ingestionReceipt) {
    await storeIngestionReceipt(args.ctx.storage, intent.ingestionReceipt);
  }
  if (shouldQueueCompaction && !suppressCompactionSchedulingForTesting) {
    await store.put("compactionWantedAt", Date.now());
    await scheduleCompactionWake(args.ctx, args.env);
  }
  await store.delete(intentKey);
  await store.delete("receiveLease");

  args.logger?.info("receive:finalize-committed", {
    commandCount: intent.commands.length,
    refCount: committedRefs.length,
    empty: committedRefs.length === 0,
    stagedPack: intent.stagedPack !== undefined,
    shouldQueueCompaction,
  });

  return {
    status: "committed",
    statuses,
    changed: committed.changed,
    empty: committed.empty,
    shouldQueueCompaction,
    outputValidationBytes,
    outputValidationRequests,
    outputEtags,
  };
}
