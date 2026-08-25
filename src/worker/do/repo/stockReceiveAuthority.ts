import type { Logger } from "@/worker/common/logger";
import type {
  AdmitStockReceiveResult,
  CompleteStockReceiveCleanupResult,
  ConfirmStockReceivePublicationResult,
  FinalizeStockReceiveResult,
  NativeReceiveAuthorityPublication,
  NativeReceiveCleanupDescriptor,
  NativeReceiveEvidenceEvent,
  NativeReceiveExecutionRejection,
  NativeReceiveOperation,
  NativeReceiveOperationMetrics,
  NativeReceivePrepared,
  RejectStockReceiveExecutionResult,
} from "@/worker/git/nativeReceive/types";

import { recordAcceptedWrites, acceptedWritesMatchCommands } from "./acceptedWrites";
import { getDb, listActivePackCatalog, upsertPackCatalogRow } from "./db";
import {
  asTypedStorage,
  nativeReceiveOperationKey,
  receiveFinalizeIntentKey,
  receiveOutcomeKey,
  type Head,
  type ReceiveFinalizeIntent,
  type RepoStateSchema,
  type TypedStorage,
} from "./repoState";
import {
  applyReceiveCommands,
  isValidRefName,
  validateReceiveCommands,
} from "@/worker/git/operations/validation";
import {
  authorityPublicationMatchesPlan,
  buildNativeReceiveAuthorityPublicationPlan,
} from "@/worker/git/nativeReceive/authorityPlan";
import {
  isNativeReceiveTerminal,
  nativeReceiveOperationView,
} from "@/worker/git/nativeReceive/types";
import { validateStockReceivePreparedProof } from "@/worker/git/nativeReceive/stockProof";
import { nativeReceiveClaimOutputPackKey, packIndexKey, packRefsKey } from "@/worker/keys";

const MAX_RETAINED_OPERATIONS = 128;
const MAX_EVIDENCE_EVENTS = 128;
const COMPACTION_FAN_IN = 4;
const ZERO_OID = "0".repeat(40);
const STOCK_EXECUTION_CLAIM_MS = 15 * 60_000;
const STOCK_TRACE_PHASES = new Map<string, string>([
  ["receive_pack_invoked", "receive-pack-start"],
  ["pre_receive_started", "pre-receive-start"],
  ["pre_receive_quarantine_nonempty", "quarantine-visible"],
  ["logical_closure_started_ref_still_old", "replacement-closure-start"],
  ["logical_closure_completed", "replacement-closure-complete"],
  ["pre_receive_succeeded", "pre-receive-complete"],
  ["disposable_ref_update_observed", "disposable-ref-updated"],
]);

function cleanupDescriptor(operation: NativeReceiveOperation): NativeReceiveCleanupDescriptor {
  const inputRequestSha256 = operation.stockReceive?.inputRequestSha256;
  if (!inputRequestSha256) throw new Error("FUBAR: stock cleanup descriptor lacks stock input");
  return {
    operationId: operation.id,
    fingerprint: operation.fingerprint,
    inputPackKey: operation.inputPackKey,
    inputRequestSha256,
    outputPackKey: operation.outputPackKey,
    outputIdxKey: operation.outputIdxKey,
    outputRefsKey: operation.outputRefsKey,
  };
}

function withEvidence(
  operation: NativeReceiveOperation,
  additions: Array<Omit<NativeReceiveEvidenceEvent, "sequence">>
): NativeReceiveOperation {
  let events = operation.events ?? [];
  for (const addition of additions) {
    if (events.some((event) => event.phase === addition.phase)) continue;
    if (events.length >= MAX_EVIDENCE_EVENTS) {
      throw new Error("FUBAR: stock receive evidence bound exceeded");
    }
    events = [...events, { ...addition, sequence: (events.at(-1)?.sequence ?? 0) + 1 }];
  }
  return { ...operation, events };
}

async function indexedOperations(
  store: TypedStorage<RepoStateSchema>
): Promise<NativeReceiveOperation[]> {
  const ids = (await store.get("nativeReceiveOperationIndex")) ?? [];
  const operations: NativeReceiveOperation[] = [];
  for (const id of ids) {
    const operation = await store.get(nativeReceiveOperationKey(id));
    if (operation) operations.push(operation);
  }
  return operations;
}

async function operationByExecutionToken(
  store: TypedStorage<RepoStateSchema>,
  executionToken: string
): Promise<NativeReceiveOperation | undefined> {
  const operations = await indexedOperations(store);
  return operations.find((operation) => operation.claimId === executionToken);
}

async function operationByPublicationToken(
  store: TypedStorage<RepoStateSchema>,
  publicationToken: string
): Promise<NativeReceiveOperation | undefined> {
  const operations = await indexedOperations(store);
  return operations.find((operation) => operation.publicationPlan?.token === publicationToken);
}

async function retainOperation(
  store: TypedStorage<RepoStateSchema>,
  operation: NativeReceiveOperation
): Promise<boolean> {
  const index = (await store.get("nativeReceiveOperationIndex")) ?? [];
  const next = [...index.filter((id) => id !== operation.id), operation.id];
  while (next.length > MAX_RETAINED_OPERATIONS) {
    let removed = false;
    for (let candidateIndex = 0; candidateIndex < next.length; candidateIndex++) {
      const candidateId = next[candidateIndex];
      if (!candidateId || candidateId === operation.id) continue;
      const candidate = await store.get(nativeReceiveOperationKey(candidateId));
      if (candidate && isNativeReceiveTerminal(candidate.state) && !candidate.cleanupPending) {
        next.splice(candidateIndex, 1);
        await store.delete(nativeReceiveOperationKey(candidateId));
        removed = true;
        break;
      }
    }
    if (!removed) return false;
  }
  await store.put(nativeReceiveOperationKey(operation.id), operation);
  await store.put("nativeReceiveOperationIndex", next);
  return true;
}

function retryableFailure(code: string | undefined): boolean {
  return code === "r2-transient" || code === "execution-claim-expired";
}

function finalizedDisposition(
  operation: NativeReceiveOperation | undefined
): FinalizeStockReceiveResult | undefined {
  if (!operation) return { status: "rejected", code: "operation-not-found" };
  if (operation.state === "finalizing" && operation.publicationPlan) {
    return {
      status: "publication_pending",
      publicationToken: operation.publicationPlan.token,
      publication: operation.publicationPlan,
      cleanup: cleanupDescriptor(operation),
    };
  }
  if (operation.state === "committed") {
    return {
      status: "replayed",
      operation: nativeReceiveOperationView(operation),
      cleanup: cleanupDescriptor(operation),
    };
  }
  if (operation.state === "aborted" && operation.errorCode === "exact-old-ref-conflict") {
    return {
      status: "ref_conflict",
      code: "exact-old-ref-conflict",
      cleanup: cleanupDescriptor(operation),
    };
  }
  if (operation.state === "aborted" || operation.state === "failed") {
    return {
      status: "rejected",
      code: operation.errorCode ?? `finalize-state-${operation.state}`,
      cleanup: cleanupDescriptor(operation),
    };
  }
  return undefined;
}

async function rejectReadyStockAuthority(args: {
  ctx: DurableObjectState;
  operation: NativeReceiveOperation;
  code: string;
  phase: string;
  expectedState: "ready" | "processing";
  expectedClaimId?: string | undefined;
}): Promise<FinalizeStockReceiveResult> {
  const transitioned = await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const durable = await store.get(nativeReceiveOperationKey(args.operation.id));
    if (
      durable?.fingerprint !== args.operation.fingerprint ||
      durable.state !== args.expectedState ||
      (args.expectedClaimId !== undefined && durable.claimId !== args.expectedClaimId)
    ) {
      return false;
    }
    const rejected = withEvidence(
      {
        ...durable,
        state: "aborted",
        errorCode: args.code,
        cleanupPending: true,
        updatedAt: Date.now(),
      },
      [{ phase: args.phase, durable: true, detailCode: args.code }]
    );
    await store.put(nativeReceiveOperationKey(durable.id), rejected);
    const lease = await store.get("receiveLease");
    if (lease?.token === durable.leaseToken) await store.delete("receiveLease");
    return true;
  });
  if (!transitioned) {
    const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
    const current = await store.get(nativeReceiveOperationKey(args.operation.id));
    if (current?.state === "finalizing" && current.publicationPlan) {
      return {
        status: "publication_pending",
        publicationToken: current.publicationPlan.token,
        publication: current.publicationPlan,
        cleanup: cleanupDescriptor(current),
      };
    }
    if (current?.state === "committed") {
      return {
        status: "replayed",
        operation: nativeReceiveOperationView(current),
        cleanup: cleanupDescriptor(current),
      };
    }
    return { status: "rejected", code: "authority-transition-stale" };
  }
  return { status: "rejected", code: args.code, cleanup: cleanupDescriptor(args.operation) };
}

/** State-only admission. No Env, R2 binding, Container handle, or callback is accepted. */
export async function admitStockReceiveState(args: {
  ctx: DurableObjectState;
  operation: NativeReceiveOperation;
  logger: Logger;
}): Promise<AdmitStockReceiveResult> {
  return await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) {
      return { status: "rejected", code: "repository-deleting" };
    }
    const existing = await store.get(nativeReceiveOperationKey(args.operation.id));
    if (existing) {
      if (existing.fingerprint !== args.operation.fingerprint) {
        return { status: "conflict", code: "operation-id-conflict" };
      }
      if (existing.state === "committed") {
        const currentLease = await store.get("receiveLease");
        if (currentLease?.token === args.operation.leaseToken) await store.delete("receiveLease");
        return {
          status: "replayed",
          operation: nativeReceiveOperationView(existing),
          cleanup: cleanupDescriptor(existing),
        };
      }
      if (existing.publicationPlan && existing.state === "finalizing") {
        const recovery = await store.get("stockReceiveRecoveryLease");
        if (
          recovery?.operationId !== existing.id ||
          recovery.token !== args.operation.leaseToken ||
          recovery.expiresAt <= Date.now()
        ) {
          return { status: "rejected", code: "stock-recovery-lease-mismatch" };
        }
        return {
          status: "publication_pending",
          publicationToken: existing.publicationPlan.token,
          publication: existing.publicationPlan,
          cleanup: cleanupDescriptor(existing),
        };
      }
      if (existing.publicationPlan && existing.processorResult && existing.state === "ready") {
        const recovery = await store.get("stockReceiveRecoveryLease");
        if (
          recovery?.operationId !== existing.id ||
          recovery.token !== args.operation.leaseToken ||
          recovery.expiresAt <= Date.now()
        ) {
          return { status: "rejected", code: "stock-recovery-lease-mismatch" };
        }
        if (!existing.claimId) return { status: "rejected", code: "finalize-claim-missing" };
        return { status: "finalize_pending", executionToken: existing.claimId };
      }
      if (isNativeReceiveTerminal(existing.state) && existing.cleanupPending) {
        return {
          status: "cleanup_pending",
          operation: nativeReceiveOperationView(existing),
          cleanup: cleanupDescriptor(existing),
          includeOutputs: true,
        };
      }
      if (!retryableFailure(existing.errorCode)) {
        return { status: "rejected", code: existing.errorCode ?? "operation-in-progress" };
      }
    }
    for (const current of await indexedOperations(store)) {
      if (
        current.id !== args.operation.id &&
        (current.state === "ready" || current.state === "finalizing")
      ) {
        return { status: "rejected", code: "authority-recovery-pending" };
      }
    }
    const lease = await store.get("receiveLease");
    if (!lease || lease.token !== args.operation.leaseToken || lease.expiresAt <= Date.now()) {
      return { status: "rejected", code: "lease-mismatch" };
    }
    const claimId = crypto.randomUUID();
    const outputPackKey = nativeReceiveClaimOutputPackKey(args.operation.outputPackKey, claimId);
    const admitted = withEvidence(
      {
        ...args.operation,
        outputPackKey,
        outputIdxKey: packIndexKey(outputPackKey),
        outputRefsKey: packRefsKey(outputPackKey),
        state: "processing",
        attempts: (existing?.attempts ?? 0) + 1,
        errorCode: undefined,
        cleanupPending: false,
        claimId,
        claimExpiresAt: Date.now() + STOCK_EXECUTION_CLAIM_MS,
      },
      [{ phase: "repo-do-operation-staged", durable: true }]
    );
    if (!(await retainOperation(store, admitted))) {
      return { status: "rejected", code: "operation-ledger-full" };
    }
    const currentAlarm = await transaction.getAlarm();
    if (currentAlarm === null || currentAlarm > admitted.claimExpiresAt!) {
      await transaction.setAlarm(admitted.claimExpiresAt!);
    }
    args.logger.info("stock-receive:admitted", {
      operationId: admitted.id,
      inputBytes: admitted.inputBytes,
    });
    return {
      status: "admitted",
      executionToken: admitted.claimId!,
      operation: admitted,
    };
  });
}

async function preparedProofValid(
  operation: NativeReceiveOperation,
  prepared: NativeReceivePrepared
): Promise<boolean> {
  const result = prepared.processorResult;
  return (
    prepared.operationId === operation.id &&
    prepared.fingerprint === operation.fingerprint &&
    result.operationId === operation.id &&
    result.packBytes > 0 &&
    result.idxBytes > 0 &&
    result.refsBytes > 0 &&
    result.objectCount > 0 &&
    /^[0-9a-f]{64}$/.test(result.packSha256 ?? "") &&
    /^[0-9a-f]{64}$/.test(result.idxSha256 ?? "") &&
    /^[0-9a-f]{64}$/.test(result.refsSha256 ?? "") &&
    /^[0-9a-f]{64}$/.test(result.planSha256 ?? "") &&
    result.outputValidationRequests === 3 &&
    result.outputValidationBytes === result.packBytes + result.idxBytes + result.refsBytes &&
    Boolean(result.outputPackEtag && result.outputIdxEtag && result.outputRefsEtag) &&
    result.outputPackEtag!.length <= 256 &&
    result.outputIdxEtag!.length <= 256 &&
    result.outputRefsEtag!.length <= 256 &&
    (await validateStockReceivePreparedProof(operation, result))
  );
}

function acceptedWriteContext(operation: NativeReceiveOperation) {
  const first = operation.acceptedWrites[0];
  if (!first) return undefined;
  if (
    !operation.acceptedWrites.every(
      (fact) =>
        fact.repositoryId === first.repositoryId &&
        fact.actor === first.actor &&
        fact.sourceSurface === first.sourceSurface &&
        fact.idempotencyKey === first.idempotencyKey
    )
  ) {
    throw new Error("FUBAR: accepted-write facts do not share one context");
  }
  return {
    repositoryId: first.repositoryId,
    actor: first.actor,
    sourceSurface: first.sourceSurface,
    idempotencyKey: first.idempotencyKey,
  };
}

function nextHead(stored: Head | undefined, refs: Array<{ name: string; oid: string }>): Head {
  const target = stored?.target ?? "refs/heads/main";
  const current = refs.find((ref) => ref.name === target);
  return current ? { target, oid: current.oid } : { target, unborn: true };
}

function catalogNeedsCompaction(rows: Array<{ tier: number }>): boolean {
  const counts = new Map<number, number>();
  for (const row of rows) counts.set(row.tier, (counts.get(row.tier) ?? 0) + 1);
  return [...counts.values()].some((count) => count >= COMPACTION_FAN_IN);
}

function stagedPackFor(
  operation: NativeReceiveOperation,
  prepared: NativeReceivePrepared
): NonNullable<ReceiveFinalizeIntent["stagedPack"]> {
  const result = prepared.processorResult;
  return {
    packKey: operation.outputPackKey,
    packBytes: result.packBytes,
    idxBytes: result.idxBytes,
    objectCount: result.objectCount,
    integrity: {
      packSha256: result.packSha256!,
      idxSha256: result.idxSha256!,
      refsSha256: result.refsSha256!,
      refsBytes: result.refsBytes,
      packEtag: result.outputPackEtag!,
      idxEtag: result.outputIdxEtag!,
      refsEtag: result.outputRefsEtag!,
    },
  };
}

/**
 * State-only exact-old CAS. The Worker has already read and hashed the three
 * immutable R2 artifacts; RepoDO receives only their bounded proof.
 */
export async function finalizeStockReceiveState(args: {
  ctx: DurableObjectState;
  executionToken: string;
  prepared?: NativeReceivePrepared | undefined;
  logger: Logger;
}): Promise<FinalizeStockReceiveResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  let operation = await operationByExecutionToken(store, args.executionToken);
  if (!operation) return { status: "rejected", code: "operation-not-found" };
  if (operation.state === "committed") {
    return {
      status: "replayed",
      operation: nativeReceiveOperationView(operation),
      cleanup: cleanupDescriptor(operation),
    };
  }
  if (operation.publicationPlan && operation.state === "finalizing") {
    return {
      status: "publication_pending",
      publicationToken: operation.publicationPlan.token,
      publication: operation.publicationPlan,
      cleanup: cleanupDescriptor(operation),
    };
  }
  let prepared: NativeReceivePrepared;
  if (
    operation.commands.length !== 1 ||
    operation.commands.some((command) => !isValidRefName(command.ref)) ||
    !acceptedWritesMatchCommands(operation.commands, operation.acceptedWrites)
  ) {
    return { status: "rejected", code: "receive-authority-invalid" };
  }
  let currentRefs = (await store.get("refs")) ?? [];
  let statuses = validateReceiveCommands(currentRefs, operation.commands);
  if (operation.state === "ready") {
    const retainedIntent = await store.get(receiveFinalizeIntentKey(operation.leaseToken));
    if (!operation.processorResult || !operation.publicationPlan || !retainedIntent?.stagedPack) {
      return { status: "rejected", code: "finalize-state-invalid" };
    }
    prepared = {
      operationId: operation.id,
      fingerprint: operation.fingerprint,
      processorResult: operation.processorResult,
    };
    if (!(await preparedProofValid(operation, prepared))) {
      return { status: "rejected", code: "prepared-output-invalid" };
    }
  } else {
    if (
      operation.state !== "processing" ||
      !args.prepared ||
      !(await preparedProofValid(operation, args.prepared))
    ) {
      return { status: "rejected", code: "prepared-output-invalid" };
    }
    prepared = args.prepared;
  }
  if (!statuses.every((status) => status.ok)) {
    const rejected = withEvidence(
      {
        ...operation,
        state: "aborted",
        errorCode: "exact-old-ref-conflict",
        cleanupPending: true,
        updatedAt: Date.now(),
      },
      [{ phase: "authoritative-ref-cas-rejected", durable: true }]
    );
    const transitioned = await args.ctx.storage.transaction(async (transaction) => {
      const tx = asTypedStorage<RepoStateSchema>(transaction);
      const durable = await tx.get(nativeReceiveOperationKey(operation!.id));
      if (
        durable?.state !== operation!.state ||
        durable.fingerprint !== operation!.fingerprint ||
        durable.claimId !== operation!.claimId
      ) {
        return false;
      }
      await tx.put(nativeReceiveOperationKey(operation!.id), rejected);
      const lease = await tx.get("receiveLease");
      if (lease?.token === operation!.leaseToken) await tx.delete("receiveLease");
      return true;
    });
    if (!transitioned) {
      const current = await store.get(nativeReceiveOperationKey(operation.id));
      return (
        finalizedDisposition(current) ?? {
          status: "rejected",
          code: "execution-claim-stale",
        }
      );
    }
    return {
      status: "ref_conflict",
      code: "exact-old-ref-conflict",
      cleanup: cleanupDescriptor(operation),
    };
  }

  const refsVersion = (await store.get("refsVersion")) ?? 0;
  const packSequence = (await store.get("nextPackSeq")) ?? 1;
  const packsetVersion = (await store.get("packsetVersion")) ?? 0;
  const nextRefs = applyReceiveCommands(currentRefs, operation.commands);
  const stagedPack = stagedPackFor(operation, prepared);
  let intent = await store.get(receiveFinalizeIntentKey(operation.leaseToken));
  let publicationPlan = operation.publicationPlan;
  if (operation.state === "processing") {
    intent = {
      token: operation.leaseToken,
      commands: operation.commands,
      expectedRefsVersion: refsVersion,
      nextHead: nextHead(await store.get("head"), nextRefs),
      nextRefsVersion: refsVersion + 1,
      stagedPack,
      packSequence,
      nextPacksetVersion: packsetVersion + 1,
      acceptedWriteContext: acceptedWriteContext(operation),
      createdAt: Date.now(),
    };
    publicationPlan = await buildNativeReceiveAuthorityPublicationPlan({
      operation,
      processorResult: prepared.processorResult,
    });
    const hostTraceEvents = prepared.processorResult
      .stockTrace!.map((entry) => STOCK_TRACE_PHASES.get(entry.event))
      .filter((phase): phase is string => phase !== undefined)
      .map((phase) => ({ phase }));
    operation = withEvidence(
      {
        ...operation,
        state: "ready",
        processorResult: prepared.processorResult,
        publicationPlan,
        cleanupPending: true,
        updatedAt: Date.now(),
      },
      [
        { phase: "go-processor-start" },
        ...hostTraceEvents,
        {
          phase: "output-integrity-verified",
          bytes: prepared.processorResult.outputValidationBytes,
        },
        { phase: "wal-put-complete", durable: true },
      ]
    );
    const walPublished = await args.ctx.storage.transaction(async (transaction) => {
      const tx = asTypedStorage<RepoStateSchema>(transaction);
      const durable = await tx.get(nativeReceiveOperationKey(operation!.id));
      if (
        durable?.state !== "processing" ||
        durable.fingerprint !== operation!.fingerprint ||
        durable.claimId !== operation!.claimId ||
        ((await tx.get("refsVersion")) ?? 0) !== refsVersion
      ) {
        return false;
      }
      await tx.put(receiveFinalizeIntentKey(operation!.leaseToken), intent!);
      await tx.put(nativeReceiveOperationKey(operation!.id), operation!);
      const now = Date.now();
      const currentAlarm = await transaction.getAlarm();
      if (currentAlarm === null || currentAlarm > now + 1) await transaction.setAlarm(now + 1);
      return true;
    });
    if (!walPublished) {
      const current = await store.get(nativeReceiveOperationKey(operation.id));
      if (current?.claimId !== operation.claimId) {
        return { status: "rejected", code: "execution-claim-stale" };
      }
      const disposition = finalizedDisposition(current);
      if (disposition) return disposition;
      if (current?.state !== "ready" || !current.processorResult || !current.publicationPlan) {
        return { status: "rejected", code: "authority-transition-stale" };
      }
      operation = current;
      prepared = {
        operationId: current.id,
        fingerprint: current.fingerprint,
        processorResult: current.processorResult,
      };
      intent = await store.get(receiveFinalizeIntentKey(current.leaseToken));
      publicationPlan = current.publicationPlan;
      currentRefs = (await store.get("refs")) ?? [];
      statuses = validateReceiveCommands(currentRefs, current.commands);
    }
  }
  if (!intent?.stagedPack || !publicationPlan) {
    return { status: "rejected", code: "finalize-state-invalid" };
  }
  if (operation.state === "ready" && intent.expectedRefsVersion !== refsVersion) {
    return await rejectReadyStockAuthority({
      ctx: args.ctx,
      operation,
      code: "exact-old-ref-conflict",
      phase: "authoritative-version-drift-rejected",
      expectedState: "ready",
      expectedClaimId: operation.claimId,
    });
  }
  if (intent.packSequence !== packSequence || intent.nextPacksetVersion !== packsetVersion + 1) {
    return await rejectReadyStockAuthority({
      ctx: args.ctx,
      operation,
      code: "wal-authority-conflict",
      phase: "authoritative-wal-conflict-rejected",
      expectedState: "ready",
      expectedClaimId: operation.claimId,
    });
  }

  const db = getDb(args.ctx.storage);
  const activeBefore = await listActivePackCatalog(db);
  const existingPack = activeBefore.find((row) => row.packKey === stagedPack.packKey);
  if (
    existingPack &&
    (existingPack.packBytes !== stagedPack.packBytes ||
      existingPack.idxBytes !== stagedPack.idxBytes ||
      existingPack.objectCount !== stagedPack.objectCount ||
      existingPack.seqLo !== packSequence ||
      existingPack.seqHi !== packSequence)
  ) {
    return await rejectReadyStockAuthority({
      ctx: args.ctx,
      operation,
      code: "catalog-authority-conflict",
      phase: "authoritative-catalog-conflict-rejected",
      expectedState: "ready",
      expectedClaimId: operation.claimId,
    });
  }
  if (!existingPack) {
    await upsertPackCatalogRow(db, {
      packKey: stagedPack.packKey,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: packSequence,
      seqHi: packSequence,
      objectCount: stagedPack.objectCount,
      packBytes: stagedPack.packBytes,
      idxBytes: stagedPack.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    });
  }
  const activeAfter = await listActivePackCatalog(db);
  const shouldQueueCompaction = catalogNeedsCompaction(activeAfter);
  const committedAt = Date.now();
  const finalizing = withEvidence(
    {
      ...operation,
      state: "finalizing",
      updatedAt: committedAt,
      processorResult: prepared.processorResult,
      publicationPlan,
      cleanupPending: true,
      result: {
        statuses,
        changed: true,
        empty: nextRefs.length === 0,
        packKey: operation.outputPackKey,
        packBytes: stagedPack.packBytes,
        receivePackResponse: prepared.processorResult.receivePackResponse,
        stockTrace: prepared.processorResult.stockTrace,
      },
    },
    [{ phase: "authoritative-ref-cas", durable: true }]
  );
  const committed = await args.ctx.storage.transaction(async (transaction) => {
    const tx = asTypedStorage<RepoStateSchema>(transaction);
    const durable = await tx.get(nativeReceiveOperationKey(operation!.id));
    const durableIntent = await tx.get(receiveFinalizeIntentKey(operation!.leaseToken));
    const durableRefs = (await tx.get("refs")) ?? [];
    const durableVersion = (await tx.get("refsVersion")) ?? 0;
    if (
      !durable ||
      durable.state !== "ready" ||
      durable.fingerprint !== operation!.fingerprint ||
      durable.claimId !== operation!.claimId ||
      !durableIntent ||
      durableIntent.createdAt !== intent.createdAt ||
      durableVersion !== refsVersion ||
      !validateReceiveCommands(durableRefs, operation!.commands).every((status) => status.ok)
    ) {
      return false;
    }
    await tx.put("refs", nextRefs);
    await tx.put("head", intent.nextHead);
    await tx.put("refsVersion", intent.nextRefsVersion);
    await tx.put("nextPackSeq", packSequence + 1);
    await tx.put("packsetVersion", packsetVersion + 1);
    await recordAcceptedWrites(
      tx,
      intent.nextRefsVersion,
      operation!.acceptedWrites,
      intent.createdAt,
      true
    );
    await tx.put(receiveOutcomeKey(operation!.leaseToken), {
      token: operation!.leaseToken,
      statuses,
      changed: true,
      empty: nextRefs.length === 0,
      shouldQueueCompaction,
      outputValidationBytes: prepared.processorResult.outputValidationBytes,
      outputValidationRequests: prepared.processorResult.outputValidationRequests,
      outputEtags: {
        pack: prepared.processorResult.outputPackEtag!,
        idx: prepared.processorResult.outputIdxEtag!,
        refs: prepared.processorResult.outputRefsEtag!,
      },
    });
    await tx.put(nativeReceiveOperationKey(operation!.id), finalizing);
    const lease = await tx.get("receiveLease");
    if (lease?.token === operation!.leaseToken) await tx.delete("receiveLease");
    if (shouldQueueCompaction) await tx.put("compactionWantedAt", committedAt);
    const currentAlarm = await transaction.getAlarm();
    if (currentAlarm === null || currentAlarm > committedAt + 1) {
      await transaction.setAlarm(committedAt + 1);
    }
    return true;
  });
  if (!committed) {
    const current = await store.get(nativeReceiveOperationKey(operation.id));
    if (current?.state === "finalizing" && current.publicationPlan) {
      return {
        status: "publication_pending",
        publicationToken: current.publicationPlan.token,
        publication: current.publicationPlan,
        cleanup: cleanupDescriptor(current),
      };
    }
    if (current?.state === "committed") {
      return {
        status: "replayed",
        operation: nativeReceiveOperationView(current),
        cleanup: cleanupDescriptor(current),
      };
    }
    return await rejectReadyStockAuthority({
      ctx: args.ctx,
      operation,
      code: "authority-cas-conflict",
      phase: "authoritative-cas-conflict-rejected",
      expectedState: "ready",
      expectedClaimId: operation.claimId,
    });
  }
  args.logger.info("stock-receive:finalized", {
    operationId: operation.id,
    refCount: nextRefs.length,
    shouldQueueCompaction,
  });
  return {
    status: "publication_pending",
    publicationToken: publicationPlan.token,
    publication: publicationPlan,
    cleanup: cleanupDescriptor(operation),
  };
}

/** Resume only the state/CAS phase; this path can never invoke Git or R2. */
export async function resumeStockReceiveAuthorityFromAlarm(args: {
  ctx: DurableObjectState;
  logger: Logger;
}): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const operations = await indexedOperations(store);
  const processing = operations.find(
    (operation) => operation.state === "processing" && operation.stockReceive !== undefined
  );
  if (processing) {
    const expiresAt = processing.claimExpiresAt ?? processing.updatedAt + STOCK_EXECUTION_CLAIM_MS;
    if (expiresAt > Date.now()) {
      const currentAlarm = await args.ctx.storage.getAlarm();
      if (currentAlarm === null || currentAlarm > expiresAt) {
        await args.ctx.storage.setAlarm(expiresAt);
      }
      return true;
    }
    await rejectReadyStockAuthority({
      ctx: args.ctx,
      operation: processing,
      code: "execution-claim-expired",
      phase: "worker-execution-claim-expired",
      expectedState: "processing",
      expectedClaimId: processing.claimId,
    });
    return true;
  }
  const ready = operations.find(
    (operation) => operation.state === "ready" && operation.stockReceive !== undefined
  );
  if (!ready) return false;
  const resumed = await finalizeStockReceiveState({
    ...args,
    executionToken: ready.claimId ?? "",
  });
  if (resumed.status === "rejected") {
    const current = await store.get(nativeReceiveOperationKey(ready.id));
    if (current?.state === "ready") {
      await rejectReadyStockAuthority({
        ctx: args.ctx,
        operation: current,
        code: "authority-recovery-invalid",
        phase: "authority-recovery-rejected",
        expectedState: "ready",
        expectedClaimId: current.claimId,
      });
    }
  }
  return true;
}

/** State-only confirmation of Worker-observed immutable publication. */
export async function confirmStockReceivePublicationState(args: {
  ctx: DurableObjectState;
  publicationToken: string;
  proof: NativeReceiveAuthorityPublication;
  logger: Logger;
}): Promise<ConfirmStockReceivePublicationResult> {
  return await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const operation = await operationByPublicationToken(store, args.publicationToken);
    if (!operation || !operation.publicationPlan) {
      return { status: "rejected", code: "publication-not-found" };
    }
    if (operation.state === "committed") {
      return {
        status: "replayed",
        operation: nativeReceiveOperationView(operation),
        cleanup: cleanupDescriptor(operation),
      };
    }
    if (
      operation.state !== "finalizing" ||
      !authorityPublicationMatchesPlan(operation.publicationPlan, args.proof)
    ) {
      return { status: "rejected", code: "publication-proof-invalid" };
    }
    const committed = withEvidence(
      {
        ...operation,
        state: "committed",
        updatedAt: Date.now(),
        result: operation.result
          ? { ...operation.result, authorityPublication: args.proof }
          : undefined,
      },
      [
        {
          phase: "receipt-committed",
          durable: true,
          digest: args.proof.receipt.digest,
        },
      ]
    );
    await store.put(nativeReceiveOperationKey(operation.id), committed);
    args.logger.info("stock-receive:publication-confirmed", {
      operationId: operation.id,
      receiptDigest: args.proof.receipt.digest,
    });
    return {
      status: "committed",
      operation: nativeReceiveOperationView(committed),
      cleanup: cleanupDescriptor(committed),
    };
  });
}

function validPlannerRejectionMetrics(
  operation: NativeReceiveOperation,
  code: "r2-transient" | "replacement-closure-invalid",
  metrics: NativeReceiveOperationMetrics | undefined
): metrics is NativeReceiveOperationMetrics {
  if (!metrics || new TextEncoder().encode(JSON.stringify(metrics)).byteLength > 64 * 1024) {
    return false;
  }
  const values = [
    metrics.elapsedMs,
    metrics.scratchBytes,
    metrics.hydratedBytes,
    metrics.downloadedBytes,
    metrics.cacheHitBytes,
    metrics.metadataBytes,
    metrics.metadataRequests,
    metrics.inputBytesRead,
    metrics.inputRequests,
    metrics.rangeBytes,
    metrics.rangeRequests,
    metrics.packsTouched,
    metrics.activePackTrailerBytes,
    metrics.activePackTrailerRequests,
    metrics.activePackRangeBytes,
    metrics.activePackRangeRequests,
    metrics.activePackWholeBytes,
    metrics.activePackWholeRequests,
    metrics.activePackUnattributedBytes,
    metrics.activePackUnattributedRequests,
    metrics.selectedPackBytes,
    metrics.activePackCount,
    metrics.outputValidationBytes,
    metrics.outputValidationRequests,
    metrics.outputBytesWritten,
    metrics.outputRequests,
  ];
  if (values.some((value) => !Number.isSafeInteger(value) || value! < 0)) return false;
  if (
    metrics.scratchBytes !== 0 ||
    metrics.hydratedBytes !== 0 ||
    metrics.cacheHitBytes !== 0 ||
    metrics.outputValidationBytes !== 0 ||
    metrics.outputValidationRequests !== 0 ||
    metrics.outputBytesWritten !== 0 ||
    metrics.outputRequests !== 0 ||
    metrics.activePackWholeBytes !== 0 ||
    metrics.activePackWholeRequests !== 0 ||
    metrics.activePackUnattributedBytes !== 0 ||
    metrics.activePackUnattributedRequests !== 0 ||
    metrics.activePackCount !== operation.activeCatalog.length ||
    metrics.selectedPackBytes! >
      operation.activeCatalog.reduce((total, pack) => total + pack.packBytes, 0) ||
    metrics.downloadedBytes !==
      metrics.metadataBytes! + metrics.inputBytesRead! + metrics.rangeBytes! ||
    !Array.isArray(metrics.ranges) ||
    metrics.ranges.length > 256 ||
    !Array.isArray(metrics.activePackReads) ||
    metrics.activePackReads.length > 320
  ) {
    return false;
  }
  for (const range of metrics.ranges) {
    if (
      !/^[0-9a-f]{40}$/.test(range.requiredOid) ||
      !/^[0-9a-f]{40}$/.test(range.packChecksum) ||
      !Number.isSafeInteger(range.start) ||
      !Number.isSafeInteger(range.end) ||
      range.start < 0 ||
      range.end <= range.start ||
      range.reason !== "required-object"
    ) {
      return false;
    }
  }
  const trailerReads = metrics.activePackReads.filter((read) => read.kind === "trailer");
  const requiredReads = metrics.activePackReads.filter((read) => read.kind === "required-object");
  if (
    trailerReads.some(
      (read) =>
        !/^[0-9a-f]{40}$/.test(read.packChecksum) ||
        !Number.isSafeInteger(read.start) ||
        !Number.isSafeInteger(read.end) ||
        read.start < 0 ||
        read.end <= read.start ||
        read.returnedBytes !== read.end - read.start
    ) ||
    requiredReads.some((read, index) => {
      const range = metrics.ranges![index];
      return (
        !range ||
        read.packChecksum !== range.packChecksum ||
        read.start !== range.start ||
        read.end !== range.end ||
        read.returnedBytes !== range.end - range.start ||
        read.requiredOid !== range.requiredOid
      );
    }) ||
    requiredReads.length !== metrics.ranges.length ||
    metrics.activePackTrailerBytes !==
      trailerReads.reduce((total, read) => total + read.returnedBytes, 0) ||
    metrics.activePackTrailerRequests !== trailerReads.length ||
    metrics.activePackRangeBytes !==
      requiredReads.reduce((total, read) => total + read.returnedBytes, 0) ||
    metrics.activePackRangeRequests !== requiredReads.length ||
    metrics.rangeBytes !== metrics.activePackRangeBytes ||
    metrics.rangeRequests !== metrics.activePackRangeRequests ||
    metrics.packsTouched !== new Set(requiredReads.map((read) => read.packChecksum)).size
  ) {
    return false;
  }
  if (code === "r2-transient") {
    return (
      metrics.metadataBytes === 0 &&
      metrics.metadataRequests === 0 &&
      metrics.inputBytesRead === 0 &&
      metrics.inputRequests === 0 &&
      metrics.rangeBytes === 0 &&
      metrics.rangeRequests === 0 &&
      metrics.ranges.length === 0 &&
      metrics.activePackReads.length === 0 &&
      metrics.packsTouched === 0 &&
      metrics.selectedPackBytes === 0
    );
  }
  return metrics.rangeRequests! > 0 && metrics.packsTouched! > 0;
}

export async function rejectStockReceiveExecutionState(args: {
  ctx: DurableObjectState;
  executionToken: string;
  rejection: NativeReceiveExecutionRejection | string;
  logger: Logger;
}): Promise<RejectStockReceiveExecutionResult> {
  const supplied: {
    code: string;
    processorResult?: NativeReceiveExecutionRejection["processorResult"];
    metrics?: NativeReceiveExecutionRejection["metrics"];
  } = typeof args.rejection === "string" ? { code: args.rejection } : args.rejection;
  return await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const operation = await operationByExecutionToken(store, args.executionToken);
    if (!operation) return { status: "rejected", code: "operation-not-found" };
    if (isNativeReceiveTerminal(operation.state)) {
      return { status: "replayed", operation: nativeReceiveOperationView(operation) };
    }
    if (operation.state !== "processing" || operation.claimId !== args.executionToken) {
      return { status: "rejected", code: "execution-claim-stale" };
    }
    const acceptedCodes = new Set([
      "r2-transient",
      "replacement-closure-invalid",
      "output-integrity-invalid",
      "native-data-plane-failed",
      "finalize-rejected",
    ]);
    if (!acceptedCodes.has(supplied.code)) {
      return { status: "rejected", code: "execution-rejection-invalid" };
    }
    const processorResult = supplied.processorResult;
    const rejectionMetrics = supplied.metrics;
    if (supplied.code === "output-integrity-invalid") {
      if (
        !processorResult ||
        processorResult.operationId !== operation.id ||
        !Number.isSafeInteger(processorResult.outputValidationBytes) ||
        processorResult.outputValidationBytes! <= 0 ||
        processorResult.outputValidationBytes! >
          processorResult.packBytes + processorResult.idxBytes + processorResult.refsBytes ||
        !Number.isSafeInteger(processorResult.outputValidationRequests) ||
        processorResult.outputValidationRequests! <= 0 ||
        processorResult.outputValidationRequests! > 3 ||
        !processorResult.outputIntegrityRejectedRole ||
        !new Set(["body", "head"]).has(processorResult.outputIntegrityRejectedAt ?? "") ||
        new TextEncoder().encode(JSON.stringify(processorResult)).byteLength > 256 * 1024 ||
        !(await validateStockReceivePreparedProof(operation, processorResult))
      ) {
        return { status: "rejected", code: "execution-rejection-proof-invalid" };
      }
    } else if (processorResult !== undefined) {
      return { status: "rejected", code: "execution-rejection-proof-unexpected" };
    }
    if (supplied.code === "r2-transient" || supplied.code === "replacement-closure-invalid") {
      if (!validPlannerRejectionMetrics(operation, supplied.code, rejectionMetrics)) {
        return { status: "rejected", code: "execution-rejection-metrics-invalid" };
      }
    } else if (rejectionMetrics !== undefined) {
      return { status: "rejected", code: "execution-rejection-metrics-unexpected" };
    }
    const event = {
      "r2-transient": "r2-read-retryable",
      "replacement-closure-invalid": "replacement-closure-rejected",
      "output-integrity-invalid": "output-integrity-rejected",
      "native-data-plane-failed": "worker-data-plane-rejected",
      "finalize-rejected": "worker-data-plane-rejected",
    }[supplied.code]!;
    const failed = withEvidence(
      {
        ...operation,
        state: "failed",
        errorCode: supplied.code,
        processorResult,
        rejectionMetrics,
        cleanupPending: true,
        updatedAt: Date.now(),
      },
      [{ phase: event, detailCode: supplied.code }]
    );
    await store.put(nativeReceiveOperationKey(operation.id), failed);
    const lease = await store.get("receiveLease");
    if (lease?.token === operation.leaseToken) await store.delete("receiveLease");
    args.logger.warn("stock-receive:execution-rejected", {
      operationId: operation.id,
      code: supplied.code,
    });
    return { status: "failed", operation: nativeReceiveOperationView(failed) };
  });
}

export async function completeStockReceiveCleanupState(args: {
  ctx: DurableObjectState;
  operationId: string;
  fingerprint: string;
}): Promise<CompleteStockReceiveCleanupResult> {
  return await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const operation = await store.get(nativeReceiveOperationKey(args.operationId));
    if (!operation) return { status: "rejected", code: "cleanup-operation-not-found" };
    if (operation.fingerprint !== args.fingerprint) {
      return { status: "rejected", code: "cleanup-fingerprint-mismatch" };
    }
    if (!isNativeReceiveTerminal(operation.state)) {
      return { status: "rejected", code: `cleanup-state-${operation.state}` };
    }
    const cleaned = { ...operation, cleanupPending: false, updatedAt: Date.now() };
    await store.put(nativeReceiveOperationKey(operation.id), cleaned);
    return { status: "complete", operation: nativeReceiveOperationView(cleaned) };
  });
}

export const __test = { ZERO_OID };
