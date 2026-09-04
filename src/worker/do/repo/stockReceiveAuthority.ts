import type { Logger } from "@/worker/common/logger";
import { advanceGcReceiveVersions, gcOwnsSource } from "./catalog/gcCoordination";
import { GC_OPERATION_KEY, type GcOperation } from "@/worker/git/maintenance/gcOperation";
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
  RecoverStockReceivePublicationResult,
  RejectStockReceiveExecutionResult,
} from "@/worker/git/nativeReceive/types";

import { recordAcceptedWrites, acceptedWritesMatchCommands } from "./acceptedWrites";
import { getDb, listActivePackCatalog, upsertPackCatalogRow, type PackCatalogRow } from "./db";
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
  STOCK_PLANNER_REJECTION_METRICS_MAX_BYTES,
  STOCK_PROCESSOR_RESULT_MAX_BYTES,
} from "@/worker/git/nativeReceive/types";
import { validateStockReceivePreparedProof } from "@/worker/git/nativeReceive/stockProof";
import {
  catalogMetadataBundleKey,
  catalogMetadataFingerprint,
} from "@/worker/git/nativeReceive/catalogMetadataBundle";
import { nativeReceiveClaimOutputPackKey, packIndexKey, packRefsKey } from "@/worker/keys";
import {
  LEASE_RETRY_AFTER_SECONDS,
  RECEIVE_LEASE_TTL_MS,
  markCompactionActivity,
} from "./catalog/shared";
import {
  STOCK_RECEIVE_EXECUTION_CLAIM_MS,
  removeStockReceivePreparationLease,
} from "./nativeReceiveActivity";
import { catalogNeedsCompaction } from "./catalog/compaction/plan";

const MAX_RETAINED_OPERATIONS = 128;
const MAX_EVIDENCE_EVENTS = 128;
const ZERO_OID = "0".repeat(40);
const STOCK_PUBLICATION_LEASE_MS = 30_000;
const STOCK_TRACE_PHASES = new Map<string, string>([
  ["receive_pack_invoked", "receive-pack-start"],
  ["pre_receive_started", "pre-receive-start"],
  ["pre_receive_quarantine_nonempty", "quarantine-visible"],
  ["logical_closure_started_ref_still_old", "replacement-closure-start"],
  ["logical_closure_completed", "replacement-closure-complete"],
  ["pre_receive_succeeded", "pre-receive-complete"],
  ["disposable_ref_update_observed", "disposable-ref-updated"],
  ["worker_direct_closure_validated", "direct-closure-validated"],
  ["worker_direct_artifacts_published", "direct-artifacts-published"],
]);

let afterPublicationLeaseForTesting: (() => Promise<void>) | undefined;

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
    events = [
      ...events,
      {
        ...addition,
        sequence: (events.at(-1)?.sequence ?? 0) + 1,
      },
    ];
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
        await removeStockReceivePreparationLease(store, args.operation.leaseToken);
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
      if (existing.processorResult && existing.state === "ready") {
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
    const preparationLeases = await store.get("stockReceivePreparationLeases");
    const preparationLease = preparationLeases?.find(
      (candidate) =>
        candidate.token === args.operation.leaseToken && candidate.expiresAt > Date.now()
    );
    if (
      (!lease || lease.token !== args.operation.leaseToken || lease.expiresAt <= Date.now()) &&
      !preparationLease
    ) {
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
        claimExpiresAt: Date.now() + STOCK_RECEIVE_EXECUTION_CLAIM_MS,
      },
      [{ phase: "repo-do-operation-staged", durable: true }]
    );
    if (!(await retainOperation(store, admitted))) {
      return { status: "rejected", code: "operation-ledger-full" };
    }
    await removeStockReceivePreparationLease(store, admitted.leaseToken);
    // The bounded preparation reservation owns staging until the operation
    // record is durable. Ordinary stock work can prepare concurrently; RepoDO
    // still serializes exact-old publication. GC-coordinated receives retain
    // the exclusive lease because their source-protection proof is bound to it.
    if (!gcOwnsSource(await transaction.get<GcOperation>(GC_OPERATION_KEY))) {
      await store.delete("receiveLease");
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
  const resultKind = result.resultKind ?? "artifacts";
  const outputProofValid =
    resultKind === "ref-only"
      ? result.packBytes === 0 &&
        result.idxBytes === 0 &&
        result.refsBytes === 0 &&
        result.objectCount === 0 &&
        result.outputValidationRequests === 0 &&
        result.outputValidationBytes === 0 &&
        result.outputBytesWritten === 0 &&
        result.outputRequests === 0 &&
        !result.packSha256 &&
        !result.idxSha256 &&
        !result.refsSha256 &&
        !result.outputPackEtag &&
        !result.outputIdxEtag &&
        !result.outputRefsEtag
      : result.packBytes > 0 &&
        result.idxBytes > 0 &&
        result.refsBytes > 0 &&
        result.objectCount > 0 &&
        /^[0-9a-f]{64}$/.test(result.packSha256 ?? "") &&
        /^[0-9a-f]{64}$/.test(result.idxSha256 ?? "") &&
        /^[0-9a-f]{64}$/.test(result.refsSha256 ?? "") &&
        result.outputValidationRequests === 3 &&
        result.outputValidationBytes === result.packBytes + result.idxBytes + result.refsBytes &&
        Boolean(result.outputPackEtag && result.outputIdxEtag && result.outputRefsEtag) &&
        result.outputPackEtag!.length <= 256 &&
        result.outputIdxEtag!.length <= 256 &&
        result.outputRefsEtag!.length <= 256;
  return (
    prepared.operationId === operation.id &&
    prepared.fingerprint === operation.fingerprint &&
    result.operationId === operation.id &&
    outputProofValid &&
    /^[0-9a-f]{64}$/.test(result.planSha256 ?? "") &&
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

function stagedPackFor(
  operation: NativeReceiveOperation,
  prepared: NativeReceivePrepared
): ReceiveFinalizeIntent["stagedPack"] {
  const result = prepared.processorResult;
  if (result.resultKind === "ref-only") return undefined;
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

function shouldQueueStockCompaction(
  hasStagedPack: boolean,
  activeCatalog: PackCatalogRow[]
): boolean {
  return hasStagedPack && catalogNeedsCompaction(activeCatalog);
}

/**
 * State-only exact-old CAS. The Worker supplies either bounded proof of three
 * verified immutable R2 artifacts or an explicitly artifact-free ref-only
 * result whose complete target closure is already authoritative.
 */
async function finalizeStockReceiveWithPublicationLease(args: {
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
    if (!operation.processorResult) {
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
  const refOnly = prepared.processorResult.resultKind === "ref-only";
  let intent = await store.get(receiveFinalizeIntentKey(operation.leaseToken));
  let publicationPlan = operation.publicationPlan;
  if (operation.state === "processing" || !intent || !publicationPlan) {
    const expectedState = operation.state;
    intent = {
      token: operation.leaseToken,
      commands: operation.commands,
      expectedRefsVersion: refsVersion,
      nextHead: nextHead(await store.get("head"), nextRefs),
      nextRefsVersion: refsVersion + 1,
      stagedPack,
      packSequence: refOnly ? undefined : packSequence,
      nextPacksetVersion: refOnly ? undefined : packsetVersion + 1,
      acceptedWriteContext: acceptedWriteContext(operation),
      createdAt: Date.now(),
    };
    const preparedObservedAt = Date.now();
    publicationPlan = await buildNativeReceiveAuthorityPublicationPlan({
      operation,
      processorResult: prepared.processorResult,
    });
    const hostTraceEvents = prepared.processorResult
      .stockTrace!.map((entry) => STOCK_TRACE_PHASES.get(entry.event))
      .filter((phase): phase is string => phase !== undefined)
      .map((phase) => ({ phase }));
    const processorStartedAt = prepared.processorResult.processorStartedAt;
    const processorStartPhase =
      prepared.processorResult.executionMode === "direct-pack"
        ? "worker-processor-start"
        : "go-processor-start";
    const correctedEvents = operation.events?.map((event) =>
      event.phase === "go-processor-start" && processorStartedAt !== undefined
        ? { ...event, phase: processorStartPhase, at: processorStartedAt }
        : event
    );
    operation = withEvidence(
      {
        ...operation,
        events: correctedEvents,
        state: "ready",
        processorResult: prepared.processorResult,
        publicationPlan,
        cleanupPending: true,
        updatedAt: Date.now(),
      },
      [
        {
          phase: processorStartPhase,
          at: processorStartedAt,
        },
        ...hostTraceEvents,
        {
          phase: "output-integrity-verified",
          at: preparedObservedAt,
          bytes: prepared.processorResult.outputValidationBytes,
        },
        { phase: "wal-put-complete", durable: true },
      ]
    );
    const walPublished = await args.ctx.storage.transaction(async (transaction) => {
      const tx = asTypedStorage<RepoStateSchema>(transaction);
      const durable = await tx.get(nativeReceiveOperationKey(operation!.id));
      if (
        durable?.state !== expectedState ||
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
  if (!intent || !publicationPlan) {
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
  if (
    !refOnly &&
    (intent.packSequence !== packSequence || intent.nextPacksetVersion !== packsetVersion + 1)
  ) {
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
  const existingPack = stagedPack
    ? activeBefore.find((row) => row.packKey === stagedPack.packKey)
    : undefined;
  if (
    stagedPack &&
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
  if (stagedPack && !existingPack) {
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
  const shouldQueueCompaction = shouldQueueStockCompaction(stagedPack !== undefined, activeAfter);
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
        packKey: stagedPack ? operation.outputPackKey : undefined,
        packBytes: stagedPack?.packBytes,
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
    if (stagedPack) {
      await tx.put("nextPackSeq", packSequence + 1);
      await tx.put("packsetVersion", packsetVersion + 1);
    }
    // The stock planner/proof restricts every semantic external edge to the
    // advertised closure, which is already covered by the GC snapshot or a
    // previous protected receive. Container output includes its encoding bases;
    // direct-pack output instead binds any external encoding base to that same
    // advertised and protected closure.
    await advanceGcReceiveVersions(
      transaction,
      refsVersion,
      intent.nextRefsVersion,
      stagedPack ? packsetVersion + 1 : undefined
    );
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
      outputEtags: stagedPack
        ? {
            pack: prepared.processorResult.outputPackEtag!,
            idx: prepared.processorResult.outputIdxEtag!,
            refs: prepared.processorResult.outputRefsEtag!,
          }
        : undefined,
    });
    await tx.put(nativeReceiveOperationKey(operation!.id), finalizing);
    const lease = await tx.get("receiveLease");
    if (lease?.token === operation!.leaseToken) await tx.delete("receiveLease");
    if (shouldQueueCompaction) await markCompactionActivity(tx, committedAt);
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

/**
 * Retain a validated Container result before waiting for the short publication
 * lane. If that lane's owner disappears, alarm or identical-request recovery
 * can resume this operation without re-running disposable preparation.
 */
async function retainPreparedStockReceiveState(args: {
  ctx: DurableObjectState;
  executionToken: string;
  prepared?: NativeReceivePrepared | undefined;
  logger: Logger;
}): Promise<void> {
  if (!args.prepared) return;
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const operation = await operationByExecutionToken(store, args.executionToken);
  if (operation?.state !== "processing" || !(await preparedProofValid(operation, args.prepared))) {
    return;
  }
  const retained = await args.ctx.storage.transaction(async (transaction) => {
    const tx = asTypedStorage<RepoStateSchema>(transaction);
    const durable = await tx.get(nativeReceiveOperationKey(operation.id));
    if (
      durable?.state !== "processing" ||
      durable.fingerprint !== operation.fingerprint ||
      durable.claimId !== operation.claimId
    ) {
      return false;
    }
    await tx.put(nativeReceiveOperationKey(operation.id), {
      ...durable,
      state: "ready",
      processorResult: args.prepared!.processorResult,
      cleanupPending: true,
      updatedAt: Date.now(),
    });
    const now = Date.now();
    const currentAlarm = await transaction.getAlarm();
    if (currentAlarm === null || currentAlarm > now + 1) await transaction.setAlarm(now + 1);
    return true;
  });
  if (retained) {
    args.logger.info("stock-receive:prepared-retained", { operationId: operation.id });
  }
}

/**
 * Serialize only RepoDO's proof/WAL/ref/catalog publication section. Immutable
 * Worker and Container preparation remains parallel; a second finalizer waits
 * outside RepoDO and re-reads current refs after the first CAS completes.
 */
export async function finalizeStockReceiveState(args: {
  ctx: DurableObjectState;
  executionToken: string;
  prepared?: NativeReceivePrepared | undefined;
  logger: Logger;
}): Promise<FinalizeStockReceiveResult> {
  await retainPreparedStockReceiveState(args);
  const now = Date.now();
  const acquisition = await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const operation = await operationByExecutionToken(store, args.executionToken);
    if (!operation) return "not-required" as const;
    if (operation.state === "committed" || operation.state === "finalizing") {
      return "not-required" as const;
    }
    const publicationLease = await store.get("stockReceivePublicationLease");
    if (publicationLease && publicationLease.expiresAt > now) {
      return { status: "busy" as const, reason: "publication-lease-active" as const };
    }
    const existing = await store.get("receiveLease");
    if (
      existing &&
      existing.token !== operation.leaseToken &&
      (existing.expiresAt > now ||
        (await store.get(receiveFinalizeIntentKey(existing.token))) !== undefined)
    ) {
      return { status: "busy" as const, reason: "receive-lease-active" as const };
    }
    await store.put("receiveLease", {
      token: operation.leaseToken,
      operation: "receive",
      createdAt: now,
      expiresAt: now + RECEIVE_LEASE_TTL_MS,
    });
    await store.put("stockReceivePublicationLease", {
      token: args.executionToken,
      operation: "receive",
      createdAt: now,
      expiresAt: now + STOCK_PUBLICATION_LEASE_MS,
    });
    return {
      status: "acquired" as const,
      receiveToken: operation.leaseToken,
      publicationToken: args.executionToken,
    };
  });
  if (typeof acquisition === "object" && acquisition.status === "busy") {
    args.logger.debug("stock-receive:publication-busy", {
      reason: acquisition.reason,
    });
    return { status: "busy", retryAfter: LEASE_RETRY_AFTER_SECONDS };
  }
  if (acquisition === "not-required") {
    return await finalizeStockReceiveWithPublicationLease(args);
  }
  try {
    const afterPublicationLease = afterPublicationLeaseForTesting;
    afterPublicationLeaseForTesting = undefined;
    await afterPublicationLease?.();
    return await finalizeStockReceiveWithPublicationLease(args);
  } finally {
    await args.ctx.storage.transaction(async (transaction) => {
      const store = asTypedStorage<RepoStateSchema>(transaction);
      const publication = await store.get("stockReceivePublicationLease");
      if (publication?.token === acquisition.publicationToken) {
        await store.delete("stockReceivePublicationLease");
      }
      const current = await store.get("receiveLease");
      if (current?.token === acquisition.receiveToken) await store.delete("receiveLease");
    });
  }
}

/** Resume only the state/CAS phase; this path can never invoke Git or R2. */
export async function resumeStockReceiveAuthorityFromAlarm(args: {
  ctx: DurableObjectState;
  logger: Logger;
}): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const operations = await indexedOperations(store);
  const readyOperations = operations.filter(
    (operation) => operation.state === "ready" && operation.stockReceive !== undefined
  );
  const receiveLease = await store.get("receiveLease");
  // A disappeared publisher leaves its receive lease as the durable owner.
  // Resume that operation before unrelated ready contenders, which must wait
  // for the serialized owner to finish or its short publication lease to age.
  const ready =
    readyOperations.find((operation) => operation.leaseToken === receiveLease?.token) ??
    readyOperations[0];
  if (ready) {
    const resumed = await finalizeStockReceiveState({
      ...args,
      executionToken: ready.claimId ?? "",
    });
    if (resumed.status === "busy") {
      await args.ctx.storage.setAlarm(Date.now() + 1_000);
      return true;
    }
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
    await args.ctx.storage.setAlarm(Date.now() + 1);
    return true;
  }
  const processing = operations.find(
    (operation) => operation.state === "processing" && operation.stockReceive !== undefined
  );
  if (processing) {
    const expiresAt =
      processing.claimExpiresAt ?? processing.updatedAt + STOCK_RECEIVE_EXECUTION_CLAIM_MS;
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
    await args.ctx.storage.setAlarm(Date.now() + 1);
    return true;
  }
  return false;
}

export async function stockReceiveWorkerRecoveryOperationId(
  ctx: DurableObjectState
): Promise<string | undefined> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const operations = await indexedOperations(store);
  return [...operations]
    .reverse()
    .find(
      (operation) =>
        operation.stockReceive !== undefined &&
        ((operation.state === "finalizing" && operation.publicationPlan !== undefined) ||
          (isNativeReceiveTerminal(operation.state) && operation.cleanupPending))
    )?.id;
}

/** State-only confirmation of Worker-observed immutable publication. */
export async function confirmStockReceivePublicationState(args: {
  ctx: DurableObjectState;
  publicationToken: string;
  proof: NativeReceiveAuthorityPublication;
  authorizeClientAck?: boolean | undefined;
  logger: Logger;
}): Promise<ConfirmStockReceivePublicationResult> {
  return await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const operation = await operationByPublicationToken(store, args.publicationToken);
    if (!operation || !operation.publicationPlan) {
      return { status: "rejected", code: "publication-not-found" };
    }
    if (operation.state === "committed") {
      if (
        args.authorizeClientAck &&
        operation.result?.receivePackResponse !== undefined &&
        operation.clientAckReadyAt === undefined
      ) {
        const acknowledgedAt = Date.now();
        const acknowledged = withEvidence(
          {
            ...operation,
            clientAckReadyAt: acknowledgedAt,
            updatedAt: acknowledgedAt,
          },
          [{ phase: "worker-response-ack", at: acknowledgedAt }]
        );
        await store.put(nativeReceiveOperationKey(operation.id), acknowledged);
        return {
          status: "replayed",
          operation: nativeReceiveOperationView(acknowledged),
          cleanup: cleanupDescriptor(acknowledged),
        };
      }
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
    const committedAt = Date.now();
    const authorizeClientAck =
      args.authorizeClientAck && operation.result?.receivePackResponse !== undefined;
    const committed = withEvidence(
      {
        ...operation,
        state: "committed",
        updatedAt: committedAt,
        clientAckReadyAt: authorizeClientAck ? committedAt : undefined,
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
        ...(authorizeClientAck ? [{ phase: "worker-response-ack", at: committedAt }] : []),
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

/** Exposes only the Worker-owned recovery work for one exact operation. */
export async function recoverStockReceivePublicationState(
  ctx: DurableObjectState,
  operationId: string
): Promise<RecoverStockReceivePublicationResult> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const operation = await store.get(nativeReceiveOperationKey(operationId));
  if (!operation?.stockReceive) return { status: "none" };
  if (operation.state === "finalizing" && operation.publicationPlan) {
    return {
      status: "publication_pending",
      publicationToken: operation.publicationPlan.token,
      publication: operation.publicationPlan,
      cleanup: cleanupDescriptor(operation),
    };
  }
  if (isNativeReceiveTerminal(operation.state) && operation.cleanupPending) {
    return {
      status: "cleanup_pending",
      cleanup: cleanupDescriptor(operation),
      includeOutputs: operation.state !== "committed",
    };
  }
  return { status: "none" };
}

async function validPlannerRejectionMetrics(
  operation: NativeReceiveOperation,
  code: "r2-transient" | "replacement-closure-invalid",
  metrics: NativeReceiveOperationMetrics | undefined
): Promise<boolean> {
  if (
    !metrics ||
    new TextEncoder().encode(JSON.stringify(metrics)).byteLength >
      STOCK_PLANNER_REJECTION_METRICS_MAX_BYTES
  ) {
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
  const bundle = metrics.activeMetadataBundle;
  if (bundle) {
    if (operation.activeCatalog.length === 0) return false;
    const fingerprint = await catalogMetadataFingerprint(operation.activeCatalog);
    if (
      bundle.catalogFingerprint !== fingerprint ||
      bundle.key !== (await catalogMetadataBundleKey(operation.activeCatalog)) ||
      metrics.metadataRequests! < 1 ||
      metrics.metadataBytes! < bundle.bytes
    ) {
      return false;
    }
  }
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
    diagnosticCode?: string;
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
    const diagnosticCode = supplied.diagnosticCode;
    if (
      diagnosticCode !== undefined &&
      (supplied.code !== "native-data-plane-failed" ||
        !/^(?:stock-plan|stock-physical-plan|stock-data-plane):[a-z0-9-]{1,80}$/.test(
          diagnosticCode
        ))
    ) {
      args.logger.warn("stock-receive:execution-rejection-diagnostic-invalid", {
        operationId: operation.id,
        code: supplied.code,
      });
      return { status: "rejected", code: "execution-rejection-diagnostic-invalid" };
    }
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
        new TextEncoder().encode(JSON.stringify(processorResult)).byteLength >
          STOCK_PROCESSOR_RESULT_MAX_BYTES ||
        !(await validateStockReceivePreparedProof(operation, processorResult))
      ) {
        return { status: "rejected", code: "execution-rejection-proof-invalid" };
      }
    } else if (processorResult !== undefined) {
      return { status: "rejected", code: "execution-rejection-proof-unexpected" };
    }
    if (supplied.code === "r2-transient" || supplied.code === "replacement-closure-invalid") {
      if (!(await validPlannerRejectionMetrics(operation, supplied.code, rejectionMetrics))) {
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
      [
        { phase: event, detailCode: diagnosticCode ?? supplied.code },
        ...(diagnosticCode
          ? [
              {
                phase: `${event}-attempt-${operation.attempts}`,
                detailCode: diagnosticCode,
              },
            ]
          : []),
      ]
    );
    await store.put(nativeReceiveOperationKey(operation.id), failed);
    const lease = await store.get("receiveLease");
    if (lease?.token === operation.leaseToken) await store.delete("receiveLease");
    args.logger.warn("stock-receive:execution-rejected", {
      operationId: operation.id,
      code: supplied.code,
      diagnosticCode: diagnosticCode ?? supplied.code,
    });
    return { status: "failed", operation: nativeReceiveOperationView(failed) };
  });
}

export async function completeStockReceiveCleanupState(args: {
  ctx: DurableObjectState;
  operationId: string;
  fingerprint: string;
  logger: Logger;
}): Promise<CompleteStockReceiveCleanupResult> {
  const result = await args.ctx.storage.transaction<CompleteStockReceiveCleanupResult>(
    async (transaction) => {
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
      // Give the just-completed Queue delivery time to acknowledge, then let the
      // shared alarm pipeline resume stock recovery, GC, compaction, or idle work.
      const wakeAt = Date.now() + 1_000;
      const currentAlarm = await transaction.getAlarm();
      if (currentAlarm === null || currentAlarm > wakeAt) await transaction.setAlarm(wakeAt);
      return { status: "complete", operation: nativeReceiveOperationView(cleaned) };
    }
  );
  if (result.status === "complete") {
    args.logger.info("stock-receive:cleanup-followup-scheduled", {
      operationId: args.operationId,
    });
  }
  return result;
}

export const __test = {
  ZERO_OID,
  shouldQueueStockCompaction,
  afterPublicationLeaseOnce(callback: () => Promise<void>): void {
    afterPublicationLeaseForTesting = callback;
  },
  reset(): void {
    afterPublicationLeaseForTesting = undefined;
  },
};
