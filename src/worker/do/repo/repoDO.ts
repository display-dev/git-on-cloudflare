import type { Head, IngestionReceipt } from "./repoState";
import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type { RepoActivity } from "@/worker/common";
import type { PackCatalogRow } from "./db/schema";

import { DurableObject } from "cloudflare:workers";

import { doPrefix } from "@/worker/keys";
import { text, createLogger } from "@/worker/common";
import { clearRepositoryStorage, removePack, type RemovePackResult } from "./packOperations";
import {
  abortCompactionLease,
  abortReceiveLease,
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
  type PreviewCompactionResult,
  previewCompactionState,
  type RequestCompactionResult,
  requestCompactionState,
  rearmCompactionQueueFromAlarm,
  getActivePackCatalogSnapshot,
  getRepoActivitySnapshot,
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
  beginSnapshotMaterializationState,
  finishSnapshotMaterializationState,
  finishRepositoryMaintenanceState,
  renewRepositoryMaintenanceState,
  renewSnapshotMaterializationState,
  type BeginRepositoryDeletionResult,
  type BeginRepositoryMaintenanceResult,
  type BeginSnapshotMaterializationResult,
} from "./repositoryLifecycle";

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

    await clearExpiredLeases(this.ctx, this.logger);

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

  public async setHead(head: Head): Promise<void> {
    await this.ensureAccessAndAlarm();
    await setHead(this.ctx, head);
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

  public async beginReceive() {
    return await beginReceiveLease(this.ctx, this.logger);
  }

  public async abortReceive(token: string): Promise<boolean> {
    return await abortReceiveLease(this.ctx, token);
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
    return await reconcileReceiveState(this.ctx, args);
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

  public async commitReachabilityGc(args: {
    token: string;
    refsVersion: number;
    packsetVersion: number;
    sourcePacks: PackCatalogRow[];
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
    return await beginRepositoryDeletionState(this.ctx);
  }

  public async beginRepositoryMaintenance(): Promise<BeginRepositoryMaintenanceResult> {
    return await beginRepositoryMaintenanceState(this.ctx);
  }

  public async renewRepositoryMaintenance(token: string): Promise<boolean> {
    return await renewRepositoryMaintenanceState(this.ctx, token);
  }

  public async finishRepositoryMaintenance(token: string): Promise<boolean> {
    return await finishRepositoryMaintenanceState(this.ctx, token);
  }

  public async removePack(packKey: string): Promise<RemovePackResult> {
    await this.ensureAccessAndAlarm();
    return await removePack(this.ctx, this.env, packKey);
  }
}
