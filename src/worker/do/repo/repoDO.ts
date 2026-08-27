import type { Head, IngestionReceipt } from "./repoState";
import type {
  GcOperation,
  GcProgress,
  GcQualificationOptions,
  GcFault,
} from "@/worker/git/maintenance/gcOperation";
import {
  consumeGcFault,
  gcReaderLatch,
  releaseGcReader,
  observeGcReaderProtection,
} from "./gcQualification";
import {
  GC_OPERATION_KEY,
  registerGcOperation,
  claimGcOperation,
  recordGcProgress,
  commitGcOperation,
  gcDiscardKeys,
  resumeGcFromAlarm,
} from "./catalog/gcOperation";
import { runGcNative } from "./gcNative";
import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type { RepoActivity } from "@/worker/common";
import type { PackCatalogRow } from "./db/schema";
import {
  nativeReceiveOperationView,
  type EnqueueNativeReceiveResult,
  type MatchNativeReceiveOperationResult,
  type NativeReceiveOperation,
  type NativeReceiveOperationView,
  type NativeReceivePrepared,
  type NativeReceiveAuthorityPublication,
} from "@/worker/git/nativeReceive/types";

import { DurableObject } from "cloudflare:workers";

import { doPrefix } from "@/worker/keys";
import { text, createLogger } from "@/worker/common";
import { clearRepositoryStorage, removePack, type RemovePackResult } from "./packOperations";
import {
  getQualificationRepositoryInventory,
  resetQualificationRepositoryState,
  beginQualificationStorageRecovery,
  type QualificationRepositoryInventory,
  type QualificationResetResult,
} from "./qualification";
import {
  abortCompactionLease,
  abortReceiveLease,
  beginStockReceiveRecoveryLease,
  completeStockReceiveRecoveryLease,
  type BeginCompactionResult,
  beginCompactionState,
  beginReceiveLease,
  type BeginReachabilityGcResult,
  beginReachabilityGcState,
  type ClearCompactionRequestResult,
  clearCompactionRequestState,
  clearExpiredLeases,
  type CommitCompactionResult,
  commitCompactionState,
  type CommitReachabilityGcResult,
  commitReachabilityGcState,
  type RecordReachabilityGcPendingResult,
  recordReachabilityGcPendingState,
  type ReconcileReachabilityGcPendingResult,
  reconcileReachabilityGcPendingState,
  completeReachabilityGcPendingCleanupState,
  listSupersededGcPacksState,
  claimSupersededGcPacksState,
  type RemoveSupersededGcPacksResult,
  removeSupersededGcPacksState,
  finalizeReceiveState,
  getIngestionReceiptState,
  reconcileReceiveState,
  resumeReceiveFinalizeFromAlarm,
  type PreviewCompactionResult,
  previewCompactionState,
  type RequestCompactionResult,
  requestCompactionState,
  rearmCompactionQueueFromAlarm,
  getActivePackCatalogSnapshot,
  getRepoActivitySnapshot,
  getPendingGenerationPublicationState,
  ensureGenerationPublicationPendingState,
  completeGenerationPublicationState,
} from "./catalog";
import { getRefs, setRefs, resolveHead, setHead, getHeadAndRefs } from "./refs";
import { handleIdleAndMaintenance } from "./maintenance";
import {
  debugState,
  debugCheckCommit,
  debugCheckOid,
  type DebugCommitCheck,
  type DebugOidCheck,
  type DebugStateSnapshot,
} from "./debug";
import { migrate } from "drizzle-orm/durable-sqlite/migrator";
import { getDb } from "./db";
import migrations from "../../../../drizzle/repo-do/migrations.js";
import {
  ensureAccessAndAlarm,
  touchAndMaybeSchedule,
  type RepoDOAccessContext,
} from "./repoDO/access";
import { seedMinimalRepoState } from "./repoDO/seeding";
import {
  dropAcceptedWriteJournalEntry,
  getSnapshotReconcilePlanState,
  getSnapshotProjectionState,
  listAcceptedWriteJournalState,
  projectAcceptedWriteState,
  projectReconciledHeadState,
} from "./acceptedWrites";
import {
  beginRepositoryDeletionState,
  beginRepositoryMaintenanceState,
  beginRepositoryReadState,
  beginSnapshotMaterializationState,
  finishSnapshotMaterializationState,
  finishRepositoryMaintenanceState,
  finishRepositoryReadState,
  renewRepositoryMaintenanceState,
  renewRepositoryReadState,
  renewSnapshotMaterializationState,
  type BeginRepositoryDeletionResult,
  type BeginRepositoryMaintenanceResult,
  type BeginRepositoryReadResult,
  type BeginSnapshotMaterializationResult,
} from "./repositoryLifecycle";
import {
  canDeleteSupersededGenerationState,
  enqueueNativeReceiveState,
  getNativeReceiveOperationState,
  matchNativeReceiveOperationState,
  recordNativeReceiveClientAckState,
  resumeNativeReceiveFromAlarm,
  runNativeReceiveOperationState,
  stopNativeReceiveContainerState,
} from "./nativeReceive";
import {
  admitStockReceiveState,
  completeStockReceiveCleanupState,
  confirmStockReceivePublicationState,
  finalizeStockReceiveState,
  rejectStockReceiveExecutionState,
  resumeStockReceiveAuthorityFromAlarm,
} from "./stockReceiveAuthority";

/**
 * Repository Durable Object (per-repo authority)
 *
 * Responsibilities
 * - Acts as the strongly consistent source of truth for repository metadata.
 * - Stores refs, HEAD, and pack catalog state in DO storage/SQLite.
 * - All operations are provided as typed RPC methods on the class.
 *
 * Read Path (RPC)
 * - Correctness reads live in worker-local pack-first helpers under `src/worker/git/object-store/`.
 * - There is no public HTTP endpoint for object reads; this keeps internal state access typed
 *   and easy to audit.
 *
 * Write Path
 * - Streaming receive: the Worker writes staged `.pack` and `.idx` data to R2, then
 *   commits refs and pack-catalog metadata through typed DO RPCs.
 *
 * Maintenance & Background Work
 * - `alarm()` handles: lease cleanup, compaction queue re-arm, idle cleanup.
 * - The DO is the metadata authority; the data plane lives in R2 packs.
 */
export class RepoDurableObject extends DurableObject {
  declare env: Env;
  private lastAccessMemMs: number | undefined;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.lastAccessMemMs = await ctx.storage.get("lastAccessMs");
      const db = getDb(ctx.storage);
      await migrate(db, migrations);
      // The constructor also runs before `alarm()`. Do not touch
      // `lastAccessMs` here, or an alarm wakeup would make an idle object look
      // freshly accessed and would keep cleanup from ever seeing it as idle.
    });
  }

  async fetch(request: Request): Promise<Response> {
    try {
      await this.touchAndMaybeSchedule();
    } catch {}
    this.logger.debug("fetch", { path: new URL(request.url).pathname, method: request.method });
    return text("Not found\n", 404);
  }

  async alarm(): Promise<void> {
    this.logger.debug("alarm:start", {});

    if (await this.ctx.storage.get<boolean>("repositoryDeleting")) {
      await this.ctx.storage.deleteAlarm();
      this.logger.info("alarm:repository-deleted-tombstone-preserved", {});
      return;
    }

    if (
      await resumeStockReceiveAuthorityFromAlarm({
        ctx: this.ctx,
        logger: this.logger,
      })
    ) {
      return;
    }

    if (
      await resumeNativeReceiveFromAlarm({
        ctx: this.ctx,
        env: this.env,
        logger: this.logger,
      })
    ) {
      return;
    }

    if (
      await resumeReceiveFinalizeFromAlarm({
        ctx: this.ctx,
        env: this.env,
        logger: this.logger,
      })
    ) {
      return;
    }

    if (await resumeGcFromAlarm({ ctx: this.ctx, env: this.env, logger: this.logger })) return;

    await clearExpiredLeases(this.ctx, this.env, this.logger);

    if (
      await rearmCompactionQueueFromAlarm({ ctx: this.ctx, env: this.env, logger: this.logger })
    ) {
      return;
    }

    await handleIdleAndMaintenance(this.ctx, this.env, this.logger);
    this.logger.debug("alarm:end", {});
  }

  private async touchAndMaybeSchedule(): Promise<void> {
    await touchAndMaybeSchedule(this.accessContext());
  }

  private async ensureAccessAndAlarm(): Promise<void> {
    await ensureAccessAndAlarm(this.accessContext());
  }

  private accessContext(): RepoDOAccessContext {
    return {
      ctx: this.ctx,
      env: this.env,
      logger: this.logger,
      getLastAccessMemMs: () => this.lastAccessMemMs,
      setLastAccessMemMs: (value) => {
        this.lastAccessMemMs = value;
      },
    };
  }

  public async listRefs(): Promise<{ name: string; oid: string }[]> {
    await this.ensureAccessAndAlarm();
    return await getRefs(this.ctx);
  }

  public async setRefs(refs: { name: string; oid: string }[]): Promise<boolean> {
    await this.ensureAccessAndAlarm();
    return await setRefs(this.ctx, refs);
  }

  public async getHead(): Promise<Head> {
    await this.ensureAccessAndAlarm();
    return await resolveHead(this.ctx);
  }

  public async setHead(head: Head): Promise<boolean> {
    await this.ensureAccessAndAlarm();
    return await setHead(this.ctx, head);
  }

  public async getHeadAndRefs(): Promise<{ head: Head; refs: { name: string; oid: string }[] }> {
    await this.ensureAccessAndAlarm();
    return await getHeadAndRefs(this.ctx);
  }

  public async getActivePackCatalog(): Promise<PackCatalogRow[]> {
    await this.ensureAccessAndAlarm();
    return await getActivePackCatalogSnapshot(this.ctx);
  }

  public async getRepoActivity(): Promise<RepoActivity | null> {
    await this.ensureAccessAndAlarm();
    const snapshot = await getRepoActivitySnapshot(this.ctx);
    if (snapshot.state === "idle") return null;
    return {
      state: snapshot.state,
      startedAt: snapshot.lease.createdAt,
      expiresAt: snapshot.lease.expiresAt,
    };
  }

  public async getQualificationInventory(): Promise<QualificationRepositoryInventory> {
    return await getQualificationRepositoryInventory(this.ctx);
  }

  public async resetQualificationState(
    expectedRefStateDigest: string
  ): Promise<QualificationResetResult> {
    return await resetQualificationRepositoryState(this.ctx, expectedRefStateDigest);
  }

  public async beginQualificationStorageRecovery(expectedRefStateDigest: string) {
    return await beginQualificationStorageRecovery(this.ctx, expectedRefStateDigest);
  }

  public async beginReceive() {
    return await beginReceiveLease(this.ctx, this.logger);
  }

  public async beginStockReceiveRecovery(operationId: string) {
    return await beginStockReceiveRecoveryLease(this.ctx, operationId);
  }

  public async completeStockReceiveRecovery(operationId: string, token: string): Promise<boolean> {
    return await completeStockReceiveRecoveryLease(this.ctx, operationId, token);
  }

  public async abortReceive(token: string): Promise<boolean> {
    return await abortReceiveLease(this.ctx, token);
  }

  public async enqueueNativeReceive(
    operation: NativeReceiveOperation
  ): Promise<EnqueueNativeReceiveResult> {
    return await enqueueNativeReceiveState({
      ctx: this.ctx,
      env: this.env,
      operation,
      logger: this.logger,
    });
  }

  /** Tagged, state-only admission for the stock Smart HTTP qualification path. */
  public async admitStockReceive(operation: NativeReceiveOperation) {
    return await admitStockReceiveState({
      ctx: this.ctx,
      operation,
      logger: this.logger,
    });
  }

  /** Exact-old authority CAS over Worker-verified immutable output proof. */
  public async finalizeStockReceive(
    executionToken: string,
    prepared?: NativeReceivePrepared | undefined
  ) {
    return await finalizeStockReceiveState({
      ctx: this.ctx,
      executionToken,
      prepared,
      logger: this.logger,
    });
  }

  /** Confirm the Worker-observed R2 publication without crossing into R2. */
  public async confirmStockReceivePublication(
    publicationToken: string,
    proof: NativeReceiveAuthorityPublication
  ) {
    return await confirmStockReceivePublicationState({
      ctx: this.ctx,
      publicationToken,
      proof,
      logger: this.logger,
    });
  }

  public async rejectStockReceiveExecution(
    executionToken: string,
    rejection: import("@/worker/git/nativeReceive/types").NativeReceiveExecutionRejection | string
  ) {
    return await rejectStockReceiveExecutionState({
      ctx: this.ctx,
      executionToken,
      rejection,
      logger: this.logger,
    });
  }

  public async completeStockReceiveCleanup(operationId: string, fingerprint: string) {
    return await completeStockReceiveCleanupState({
      ctx: this.ctx,
      operationId,
      fingerprint,
    });
  }

  public async getNativeReceiveOperation(
    operationId: string
  ): Promise<NativeReceiveOperationView | null> {
    await this.ensureAccessAndAlarm();
    const operation = await getNativeReceiveOperationState(this.ctx, operationId);
    return operation ? nativeReceiveOperationView(operation) : null;
  }

  public async matchNativeReceiveOperation(
    operationId: string,
    fingerprint: string
  ): Promise<MatchNativeReceiveOperationResult> {
    await this.ensureAccessAndAlarm();
    return await matchNativeReceiveOperationState(this.ctx, operationId, fingerprint);
  }

  public async runNativeReceiveOperation(
    operationId: string
  ): Promise<NativeReceiveOperationView | null> {
    return await runNativeReceiveOperationState({
      ctx: this.ctx,
      env: this.env,
      operationId,
      logger: this.logger,
    });
  }

  public async recordNativeReceiveClientAck(operationId: string): Promise<boolean> {
    await this.ensureAccessAndAlarm();
    return await recordNativeReceiveClientAckState(this.ctx, operationId);
  }

  public async canDeleteSupersededGeneration(
    generation?: number
  ): Promise<{ safe: boolean; retryAfterSeconds?: number }> {
    const result = await canDeleteSupersededGenerationState(this.ctx, generation);
    if (!result.safe) await observeGcReaderProtection(this.ctx, this.env, generation);
    return result;
  }

  public async getPendingGenerationPublication() {
    return await getPendingGenerationPublicationState(this.ctx);
  }

  public async ensureGenerationPublicationPending() {
    return await ensureGenerationPublicationPendingState(this.ctx);
  }

  public async completeGenerationPublication(generation: number): Promise<boolean> {
    return await completeGenerationPublicationState(this.ctx, generation);
  }

  public async getIngestionReceipt(keyHash: string): Promise<IngestionReceipt | null> {
    await this.ensureAccessAndAlarm();
    return await getIngestionReceiptState(this.ctx, keyHash);
  }

  public async listAcceptedWrites() {
    await this.ensureAccessAndAlarm();
    return await listAcceptedWriteJournalState(this.ctx);
  }

  public async projectAcceptedWrite(args: {
    entryId: string;
    commitSha: string;
    materializedAt: number;
  }) {
    await this.ensureAccessAndAlarm();
    return await projectAcceptedWriteState(this.ctx, args);
  }

  public async getSnapshotReconcilePlan(ref: string) {
    await this.ensureAccessAndAlarm();
    return await getSnapshotReconcilePlanState(this.ctx, ref);
  }

  public async getSnapshotProjection(ref: string) {
    await this.ensureAccessAndAlarm();
    return await getSnapshotProjectionState(this.ctx, ref);
  }

  public async dropAcceptedWriteForProbe(entryId: string) {
    await this.ensureAccessAndAlarm();
    return await dropAcceptedWriteJournalEntry(this.ctx, entryId);
  }

  public async projectReconciledHead(args: {
    ref: string;
    commitSha: string;
    sequence: number;
    materializedAt: number;
  }) {
    await this.ensureAccessAndAlarm();
    return await projectReconciledHeadState(this.ctx, args);
  }

  public async reconcileReceive(args: {
    token: string;
    commands: Array<{ oldOid: string; newOid: string; ref: string }>;
    stagedPackKey?: string | undefined;
    ingestionReceipt?: IngestionReceipt | undefined;
  }) {
    return await reconcileReceiveState(this.ctx, this.env, args);
  }

  public async finalizeReceive(args: {
    token: string;
    commands: Array<{ oldOid: string; newOid: string; ref: string }>;
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
  }) {
    return await finalizeReceiveState({
      ctx: this.ctx,
      env: this.env,
      token: args.token,
      commands: args.commands,
      stagedPack: args.stagedPack,
      ingestionReceipt: args.ingestionReceipt,
      acceptedWrites: args.acceptedWrites,
      logger: this.logger,
    });
  }

  public async beginCompaction(): Promise<BeginCompactionResult> {
    return await beginCompactionState({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      logger: this.logger,
    });
  }

  public async abortCompaction(token: string): Promise<boolean> {
    return await abortCompactionLease(this.ctx, token);
  }

  public async beginReachabilityGc(): Promise<BeginReachabilityGcResult> {
    return await beginReachabilityGcState({ ctx: this.ctx, logger: this.logger });
  }

  public async registerGcOperation(repositoryId: string, operationId: string) {
    return registerGcOperation({ ctx: this.ctx, repositoryId, operationId, logger: this.logger });
  }

  public async registerQualificationGc(
    repositoryId: string,
    operationId: string,
    qualification: GcQualificationOptions
  ) {
    if (this.env.QUALIFICATION_MODE !== "1" || !this.env.QUALIFICATION_SECRET)
      return { status: "rejected" as const };
    return registerGcOperation({
      ctx: this.ctx,
      repositoryId,
      operationId,
      qualification,
      logger: this.logger,
    });
  }

  public async consumeGcFault(operationId: string, fault: GcFault) {
    return consumeGcFault(this.ctx, this.env, operationId, fault);
  }

  public async gcReaderLatch(token: string, packKeys: string[]) {
    return gcReaderLatch(this.ctx, this.env, token, packKeys);
  }

  public async releaseGcReader(operationId: string) {
    return releaseGcReader(this.ctx, this.env, operationId);
  }

  public async getGcOperation(): Promise<GcOperation | undefined> {
    return this.ctx.storage.get<GcOperation>(GC_OPERATION_KEY);
  }

  public async claimGcOperation(operationId: string) {
    return claimGcOperation({ ctx: this.ctx, operationId, logger: this.logger });
  }

  public async recordGcProgress(operationId: string, claimId: string, progress: GcProgress) {
    return recordGcProgress({ ctx: this.ctx, operationId, claimId, progress, logger: this.logger });
  }

  public async runGcNative(operationId: string, claimId: string) {
    return runGcNative({ ctx: this.ctx, env: this.env, operationId, claimId, logger: this.logger });
  }

  public async commitGcOperation(operationId: string, claimId: string) {
    if (await consumeGcFault(this.ctx, this.env, operationId, "before-publication"))
      throw new Error("qualification interrupted before GC publication");
    const result = await commitGcOperation({
      ctx: this.ctx,
      operationId,
      claimId,
      logger: this.logger,
    });
    if (
      result.status === "committed" &&
      (await consumeGcFault(this.ctx, this.env, operationId, "after-publication"))
    )
      throw new Error("qualification interrupted after GC publication");
    return result;
  }

  public async gcDiscardKeys(operationId: string, claimId: string) {
    return gcDiscardKeys(this.ctx, operationId, claimId);
  }

  public async commitReachabilityGc(args: {
    token: string;
    refsVersion: number;
    packsetVersion: number;
    sourcePacks: PackCatalogRow[];
    retainedPackKey?: string;
    stagedPack?: {
      packKey: string;
      packBytes: number;
      idxBytes: number;
      objectCount: number;
    };
  }): Promise<CommitReachabilityGcResult> {
    return await commitReachabilityGcState({
      ctx: this.ctx,
      token: args.token,
      refsVersion: args.refsVersion,
      packsetVersion: args.packsetVersion,
      sourcePacks: args.sourcePacks,
      retainedPackKey: args.retainedPackKey,
      stagedPack: args.stagedPack,
      logger: this.logger,
    });
  }

  public async recordReachabilityGcPending(args: {
    token: string;
    packKey: string;
  }): Promise<RecordReachabilityGcPendingResult> {
    const result = await recordReachabilityGcPendingState({ ctx: this.ctx, ...args });
    this.logger.info("reachability-gc:pending-record", {
      status: result.status,
      reason: result.status === "retry" ? result.reason : undefined,
    });
    return result;
  }

  public async reconcileReachabilityGcPending(): Promise<ReconcileReachabilityGcPendingResult> {
    const result = await reconcileReachabilityGcPendingState({ ctx: this.ctx });
    this.logger.info("reachability-gc:pending-reconcile", {
      status: result.status,
      retryAfter: result.status === "wait" ? result.retryAfter : undefined,
    });
    return result;
  }

  public async completeReachabilityGcPendingCleanup(args: {
    token: string;
    packKey: string;
  }): Promise<boolean> {
    return await completeReachabilityGcPendingCleanupState({ ctx: this.ctx, ...args });
  }

  public async listSupersededGcPacks(): Promise<PackCatalogRow[]> {
    await this.ensureAccessAndAlarm();
    const rows = await listSupersededGcPacksState(this.ctx);
    this.logger.debug("reachability-gc:list-superseded", { packCount: rows.length });
    return rows;
  }

  public async claimSupersededGcPacks(packKeys: string[]) {
    await this.ensureAccessAndAlarm();
    const result = await claimSupersededGcPacksState({ ctx: this.ctx, packKeys });
    this.logger.info("reachability-gc:claim-superseded", {
      status: result.status,
      packCount: result.status === "claimed" ? result.packKeys.length : packKeys.length,
      reason: result.status === "retry" ? result.reason : undefined,
    });
    return result;
  }

  public async removeSupersededGcPacks(packKeys: string[]): Promise<RemoveSupersededGcPacksResult> {
    await this.ensureAccessAndAlarm();
    const result = await removeSupersededGcPacksState({ ctx: this.ctx, packKeys });
    this.logger.info("reachability-gc:remove-superseded", {
      status: result.status,
      packCount: result.status === "removed" ? result.packKeys.length : packKeys.length,
      reason: result.status === "retry" ? result.reason : undefined,
    });
    return result;
  }

  public async commitCompaction(args: {
    token: string;
    sourcePacks: PackCatalogRow[];
    targetTier: number;
    packsetVersion: number;
    stagedPack: {
      packKey: string;
      packBytes: number;
      idxBytes: number;
      objectCount: number;
    };
  }): Promise<CommitCompactionResult> {
    return await commitCompactionState({
      ctx: this.ctx,
      env: this.env,
      token: args.token,
      sourcePacks: args.sourcePacks,
      targetTier: args.targetTier,
      packsetVersion: args.packsetVersion,
      stagedPack: args.stagedPack,
      logger: this.logger,
    });
  }

  public async previewCompaction(): Promise<PreviewCompactionResult> {
    await this.ensureAccessAndAlarm();
    return await previewCompactionState({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      logger: this.logger,
    });
  }

  public async requestCompaction(): Promise<RequestCompactionResult> {
    await this.ensureAccessAndAlarm();
    return await requestCompactionState({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      logger: this.logger,
    });
  }

  public async clearCompactionRequest(): Promise<ClearCompactionRequestResult> {
    await this.ensureAccessAndAlarm();
    return await clearCompactionRequestState({
      ctx: this.ctx,
      logger: this.logger,
    });
  }

  public async debugState(): Promise<DebugStateSnapshot> {
    await this.ensureAccessAndAlarm();
    return await debugState(this.ctx, this.env);
  }

  public async debugCheckCommit(commit: string): Promise<DebugCommitCheck> {
    await this.ensureAccessAndAlarm();
    return await debugCheckCommit(this.ctx, this.env, commit);
  }

  public async debugCheckOid(oid: string): Promise<DebugOidCheck> {
    await this.ensureAccessAndAlarm();
    return await debugCheckOid(this.ctx, this.env, oid);
  }

  private prefix() {
    return doPrefix(this.ctx.id.toString());
  }

  private get logger() {
    return createLogger(this.env.LOG_LEVEL, {
      service: "RepoDO",
      doId: this.ctx.id.toString(),
    });
  }

  public async seedMinimalRepo(
    withPack: boolean = true
  ): Promise<{ commitOid: string; treeOid: string }> {
    await this.ensureAccessAndAlarm();
    return await seedMinimalRepoState({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      withPack,
    });
  }

  // DO-only storage clear used by the `repository-delete` queue consumer
  // after R2 cleanup. Callers must NOT chain this from a Worker handler that
  // also enumerates R2 - that would be the forbidden Worker -> DO -> R2 hop.
  public async clearRepositoryStorage(): Promise<{ deletedDO: boolean }> {
    return await clearRepositoryStorage(this.ctx, this.env);
  }

  public async beginSnapshotMaterialization(
    prefix: string
  ): Promise<BeginSnapshotMaterializationResult> {
    return await beginSnapshotMaterializationState(this.ctx, prefix);
  }

  public async renewSnapshotMaterialization(token: string): Promise<boolean> {
    return await renewSnapshotMaterializationState(this.ctx, token);
  }

  public async finishSnapshotMaterialization(token: string): Promise<boolean> {
    return await finishSnapshotMaterializationState(this.ctx, token);
  }

  public async beginRepositoryDeletion(): Promise<BeginRepositoryDeletionResult> {
    const result = await beginRepositoryDeletionState(this.ctx);
    try {
      // The tombstone is already durable, so no new native operation can
      // start. Stop any current process before the deletion consumer sweeps R2;
      // the receive lease keeps deletion non-ready until the writer drains.
      await stopNativeReceiveContainerState(this.ctx);
      this.logger.info("repository-delete:container-stopped", {});
      return result;
    } catch (error) {
      this.logger.warn("repository-delete:container-stop-failed", { error: String(error) });
      return { ...result, ready: false };
    }
  }

  public async beginRepositoryMaintenance(
    operation?: "pack-ref-backfill" | "generation-publication"
  ): Promise<BeginRepositoryMaintenanceResult> {
    return await beginRepositoryMaintenanceState(this.ctx, operation);
  }

  public async renewRepositoryMaintenance(token: string): Promise<boolean> {
    return await renewRepositoryMaintenanceState(this.ctx, token);
  }

  public async finishRepositoryMaintenance(token: string): Promise<boolean> {
    return await finishRepositoryMaintenanceState(this.ctx, token);
  }

  public async beginRepositoryRead(): Promise<BeginRepositoryReadResult> {
    return await beginRepositoryReadState(this.ctx);
  }

  public async renewRepositoryRead(token: string): Promise<boolean> {
    return await renewRepositoryReadState(this.ctx, token);
  }

  public async finishRepositoryRead(token: string): Promise<void> {
    await finishRepositoryReadState(this.ctx, token);
  }

  public async removePack(packKey: string): Promise<RemovePackResult> {
    await this.ensureAccessAndAlarm();
    return await removePack(this.ctx, this.env, packKey);
  }
}
