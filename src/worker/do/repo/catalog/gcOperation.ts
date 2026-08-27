import type { Logger } from "@/worker/common/logger";
import {
  GC_OPERATION_KEY,
  GC_UNPUBLISHED_LIFETIME_MS,
  GC_WAKE_DELAY_MS,
  type GcOperation,
  type GcOperationResult,
  type GcProgress,
  type GcQualificationOptions,
} from "@/worker/git/maintenance/gcOperation";
import type { RepoLease } from "../repoState";
import { doPrefix, r2PackKey } from "@/worker/keys";
import { COMPACT_LEASE_TTL_MS } from "./shared";
import { EXPIRED_WRITER_DRAIN_MS } from "../repositoryLifecycle";
import { commitReachabilityGcState, recordReachabilityGcPendingState } from "./reachabilityGc";
import type { CommitReachabilityGcResult } from "./reachabilityGc";
import { getDb, getPackCatalogRow } from "../db";
import { packIndexKey, packRefsKey } from "@/worker/keys";

export { GC_OPERATION_KEY, GC_WAKE_DELAY_MS } from "@/worker/git/maintenance/gcOperation";

export function isGcTerminal(operation: GcOperation): boolean {
  return operation.phase === "complete" || operation.phase === "blocked";
}

async function armWake(transaction: DurableObjectTransaction, at: number): Promise<void> {
  const alarm = await transaction.getAlarm();
  if (alarm === null || alarm > at) await transaction.setAlarm(at);
}

/** Admission and its first wakeup share a storage transaction. Queue delivery
 * is an acceleration, never the only durable record of requested work. */
export async function registerGcOperation(args: {
  ctx: DurableObjectState;
  repositoryId: string;
  operationId: string;
  logger: Logger;
  qualification?: GcQualificationOptions;
}): Promise<GcOperationResult> {
  if (!/^[A-Za-z0-9_-]{1,100}$/.test(args.operationId) || !args.repositoryId) {
    return { status: "rejected", reason: "invalid-input" };
  }
  if (
    args.qualification &&
    (!Number.isSafeInteger(args.qualification.deadlineAt) ||
      args.qualification.deadlineAt <= Date.now() ||
      args.qualification.deadlineAt > Date.now() + GC_UNPUBLISHED_LIFETIME_MS)
  )
    return { status: "rejected", reason: "invalid-input" };
  const result = await args.ctx.storage.transaction<GcOperationResult>(async (transaction) => {
    if (await transaction.get<boolean>("repositoryDeleting")) {
      return { status: "rejected", reason: "repository-deleting" };
    }
    const current = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    if (current?.id === args.operationId) {
      if (current.repositoryId !== args.repositoryId)
        return { status: "rejected", reason: "operation-mismatch" };
      if (
        args.qualification &&
        (current.qualification?.deadlineAt !== args.qualification.deadlineAt ||
          Object.keys(current.qualification?.faults ?? {})
            .sort()
            .join(",") !== [...args.qualification.faults].sort().join(",") ||
          Boolean(current.qualification?.reader) !== args.qualification.holdReader)
      )
        return { status: "rejected", reason: "operation-mismatch" };
      if (!isGcTerminal(current)) await armWake(transaction, Date.now() + GC_WAKE_DELAY_MS);
      return { status: "ready", operation: current };
    }
    // A delayed delivery from a completed operation must not become a new
    // collection after the bounded current-operation view has advanced.
    if (await transaction.get<boolean>(`gcAdmission:${args.operationId}`))
      return { status: "rejected", reason: "operation-mismatch" };
    if (current && !isGcTerminal(current))
      return { status: "busy", retryAt: Date.now() + GC_WAKE_DELAY_MS };
    const now = Date.now();
    const prefix = doPrefix(args.ctx.id.toString());
    const operation: GcOperation = {
      schemaVersion: 1,
      id: args.operationId,
      repositoryId: args.repositoryId,
      phase: "queued",
      createdAt: now,
      updatedAt: now,
      inputPackKey: r2PackKey(prefix, `gc-${args.operationId}-input.pack`),
      outputPackKey: r2PackKey(prefix, `gc-${args.operationId}-output.pack`),
      outputResultKey: r2PackKey(prefix, `gc-${args.operationId}-result.json`),
      measurements: {},
      qualification: args.qualification
        ? {
            deadlineAt: args.qualification.deadlineAt,
            faults: Object.fromEntries(args.qualification.faults.map((fault) => [fault, {}])),
            reader: args.qualification.holdReader ? {} : undefined,
          }
        : undefined,
    };
    await transaction.put(GC_OPERATION_KEY, operation);
    await transaction.put(`gcAdmission:${args.operationId}`, true);
    await armWake(transaction, now + GC_WAKE_DELAY_MS);
    return { status: "ready", operation };
  });
  args.logger.info("reachability-gc:operation-admission", { status: result.status });
  return result;
}

export async function claimGcOperation(args: {
  ctx: DurableObjectState;
  operationId: string;
  logger: Logger;
}): Promise<GcOperationResult> {
  const result = await args.ctx.storage.transaction<GcOperationResult>(async (transaction) => {
    if (await transaction.get<boolean>("repositoryDeleting"))
      return { status: "rejected", reason: "repository-deleting" };
    const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    if (!operation || operation.id !== args.operationId)
      return { status: "rejected", reason: "operation-mismatch" };
    if (isGcTerminal(operation)) return { status: "ready", operation };
    const now = Date.now();
    if (operation.discardAfter && operation.discardAfter > now) {
      await armWake(transaction, operation.discardAfter);
      return { status: "busy", retryAt: operation.discardAfter };
    }
    if (operation.claim && operation.claim.safeRetryAt > now) {
      await armWake(transaction, operation.claim.safeRetryAt);
      return { status: "busy", retryAt: operation.claim.safeRetryAt };
    }
    // Admission time is immutable across duplicate deliveries and wakeups.
    // Ordinary work gets the same bounded recovery as qualification work;
    // a retryable failure must not renew its source write fence indefinitely.
    // Wait for the previous writer's complete drain first. Publication still
    // reconciles authority; discard retains its existing committed-output guard.
    const deadlineAt =
      operation.qualification?.deadlineAt ?? operation.createdAt + GC_UNPUBLISHED_LIFETIME_MS;
    if (
      deadlineAt <= now &&
      !operation.commit &&
      ["queued", "rewrite", "index"].includes(operation.phase)
    ) {
      operation.phase = "discard";
      operation.blockedReason = operation.qualification
        ? "qualification-deadline"
        : "operation-deadline";
      operation.discardAfter = now;
    }
    if (operation.snapshot && ["rewrite", "index", "publish"].includes(operation.phase)) {
      const compact = await transaction.get<RepoLease>("compactLease");
      const receive = await transaction.get<RepoLease>("receiveLease");
      const otherWriter = [
        receive,
        compact?.token !== operation.snapshot.token ? compact : undefined,
      ].find((lease) => lease && lease.expiresAt + EXPIRED_WRITER_DRAIN_MS > now);
      if (otherWriter && otherWriter.expiresAt + EXPIRED_WRITER_DRAIN_MS > now) {
        await armWake(transaction, now + GC_WAKE_DELAY_MS);
        return { status: "busy", retryAt: otherWriter.expiresAt + EXPIRED_WRITER_DRAIN_MS };
      }
      // A conclusive failed catalog transaction may have released its lease.
      // Reclaim the same operation's lease only while its versions still
      // match; a changed source must go through publication reconciliation.
      if (
        ((await transaction.get<number>("refsVersion")) ?? 0) === operation.snapshot.refsVersion &&
        ((await transaction.get<number>("packsetVersion")) ?? 0) ===
          operation.snapshot.packsetVersion
      ) {
        await transaction.put<RepoLease>("compactLease", {
          token: operation.snapshot.token,
          operation: "reachability-gc",
          createdAt: compact?.createdAt ?? now,
          expiresAt: now + COMPACT_LEASE_TTL_MS,
        });
      } else if (operation.phase !== "publish") {
        // No catalog commit can have started in rewrite/index. All earlier
        // execution claims and other writers drained above, so their owned
        // artifacts may enter the ordinary authoritative discard path now.
        // Publication keeps its existing committed-outcome reconciliation.
        operation.phase = "discard";
        operation.blockedReason = "source-changed";
        operation.discardAfter = now;
      }
    }
    // Wait through the existing writer drain before issuing another writer.
    // The alarm may renew the source lease, but never the execution claim.
    operation.claim = {
      id: crypto.randomUUID(),
      expiresAt: now + COMPACT_LEASE_TTL_MS,
      safeRetryAt: now + COMPACT_LEASE_TTL_MS + EXPIRED_WRITER_DRAIN_MS,
    };
    operation.updatedAt = now;
    const measurement = operation.measurements[operation.phase];
    operation.measurements[operation.phase] = {
      ...measurement,
      attempts: (measurement?.attempts ?? 0) + 1,
      startedAt: measurement?.startedAt ?? now,
    };
    await transaction.put(GC_OPERATION_KEY, operation);
    await armWake(transaction, now + GC_WAKE_DELAY_MS);
    return { status: "ready", operation };
  });
  args.logger.info("reachability-gc:operation-claim", { status: result.status });
  return result;
}

export async function recordGcProgress(args: {
  ctx: DurableObjectState;
  operationId: string;
  claimId: string;
  progress: GcProgress;
  logger: Logger;
}): Promise<GcOperationResult> {
  const result = await args.ctx.storage.transaction<GcOperationResult>(async (transaction) => {
    if (await transaction.get<boolean>("repositoryDeleting"))
      return { status: "rejected", reason: "repository-deleting" };
    const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    if (!operation || operation.id !== args.operationId)
      return { status: "rejected", reason: "operation-mismatch" };
    const now = Date.now();
    if (operation.claim?.id !== args.claimId || operation.claim.expiresAt <= now)
      return { status: "rejected", reason: "claim-mismatch" };
    const previousPhase = operation.phase;
    const progress = args.progress;
    switch (progress.kind) {
      case "step": {
        const phases = {
          "closure-planning": "rewrite",
          "rewrite-selection": "rewrite",
          "rewrite-upload": "rewrite",
          "output-validation": "index",
          "generation-publication": "reclaim",
        };
        if (
          phases[progress.step] !== operation.phase ||
          !Number.isSafeInteger(progress.elapsedMs) ||
          progress.elapsedMs < 0 ||
          ![progress.observedRequests, progress.writtenBytes].every(
            (value) => value === null || (Number.isSafeInteger(value) && value >= 0)
          )
        )
          return { status: "rejected", reason: "invalid-input" };
        operation.stepMeasurements ??= {};
        const previous = operation.stepMeasurements[progress.step];
        operation.stepMeasurements[progress.step] = {
          completedAttempts: (previous?.completedAttempts ?? 0) + 1,
          elapsedMs: (previous?.elapsedMs ?? 0) + progress.elapsedMs,
          observedRequests:
            progress.observedRequests === null || previous?.observedRequests === null
              ? null
              : (previous?.observedRequests ?? 0) + progress.observedRequests,
          writtenBytes:
            progress.writtenBytes === null || previous?.writtenBytes === null
              ? null
              : (previous?.writtenBytes ?? 0) + progress.writtenBytes,
        };
        break;
      }
      case "yield":
        delete operation.claim;
        break;
      case "snapshot": {
        if (operation.phase !== "queued") return { status: "rejected", reason: "phase-mismatch" };
        const lease = await transaction.get<RepoLease>("compactLease");
        if (
          lease?.token !== progress.snapshot.token ||
          lease.operation !== "reachability-gc" ||
          lease.expiresAt <= now ||
          ((await transaction.get<number>("refsVersion")) ?? 0) !== progress.snapshot.refsVersion ||
          ((await transaction.get<number>("packsetVersion")) ?? 0) !==
            progress.snapshot.packsetVersion
        ) {
          return { status: "rejected", reason: "source-changed" };
        }
        operation.snapshot = progress.snapshot;
        operation.phase = "rewrite";
        break;
      }
      case "plan":
        if (operation.phase !== "rewrite" || operation.closure)
          return { status: "rejected", reason: "phase-mismatch" };
        if (
          !Number.isSafeInteger(progress.closure.objectCount) ||
          progress.closure.objectCount < 0 ||
          !/^[a-f0-9]{64}$/.test(progress.closure.objectSetSha256) ||
          (progress.retainedPackKey &&
            !operation.snapshot?.sourcePacks.some(
              (row) => row.packKey === progress.retainedPackKey
            ))
        )
          return { status: "rejected", reason: "invalid-input" };
        operation.closure = progress.closure;
        operation.retainedPackKey = progress.retainedPackKey;
        if (progress.retainedPackKey || progress.closure.objectCount === 0)
          operation.phase = "publish";
        break;
      case "rewrite-intent":
        if (operation.phase !== "rewrite" || !operation.closure)
          return { status: "rejected", reason: "phase-mismatch" };
        if (
          !Number.isSafeInteger(progress.identity.packBytes) ||
          progress.identity.packBytes < 32 ||
          !/^[a-f0-9]{40}$/.test(progress.identity.packSha1)
        )
          return { status: "rejected", reason: "invalid-input" };
        if (
          operation.rewriteIntent &&
          (operation.rewriteIntent.packBytes !== progress.identity.packBytes ||
            operation.rewriteIntent.packSha1 !== progress.identity.packSha1)
        )
          return { status: "rejected", reason: "invalid-input" };
        operation.rewriteIntent = progress.identity;
        break;
      case "rewrite-complete":
        if (operation.phase !== "rewrite" || !operation.rewriteIntent)
          return { status: "rejected", reason: "phase-mismatch" };
        if (
          operation.rewriteIntent.packBytes !== progress.identity.packBytes ||
          operation.rewriteIntent.packSha1 !== progress.identity.packSha1 ||
          !progress.etag
        )
          return { status: "rejected", reason: "invalid-input" };
        operation.rewrite = { ...progress.identity, etag: progress.etag };
        operation.phase = "index";
        break;
      case "native-complete":
        if (operation.phase !== "index" || !operation.rewrite || !operation.closure)
          return { status: "rejected", reason: "phase-mismatch" };
        if (
          progress.result.operationId !== operation.id ||
          progress.result.packSha1 !== operation.rewrite.packSha1 ||
          progress.result.packBytes !== operation.rewrite.packBytes ||
          progress.result.objectCount !== operation.closure.objectCount ||
          progress.result.maintenance?.objectSetSha256 !== operation.closure.objectSetSha256
        )
          return { status: "rejected", reason: "invalid-input" };
        operation.nativeResult = progress.result;
        operation.phase = "publish";
        break;
      case "reclaimed":
        if (operation.phase !== "reclaim" || !operation.commit)
          return { status: "rejected", reason: "phase-mismatch" };
        operation.phase = "complete";
        break;
      case "discard":
        if (!["rewrite", "index", "publish"].includes(operation.phase) || operation.commit)
          return { status: "rejected", reason: "phase-mismatch" };
        // Do not shorten the writer drain even after a conclusive failure.
        // Every earlier claim drained before this claim could be issued.
        operation.discardAfter = operation.claim.safeRetryAt;
        operation.phase = "discard";
        operation.blockedReason = progress.reason;
        break;
      case "discarded":
        if (
          operation.phase !== "discard" ||
          operation.commit ||
          (operation.discardAfter ?? Infinity) > now
        )
          return { status: "rejected", reason: "phase-mismatch" };
        operation.phase = "blocked";
        break;
      case "blocked":
        // A blocked operation still owns its artifacts. Only an operation
        // with no output writes may become terminal through this transition.
        if (
          operation.rewriteIntent ||
          operation.rewrite ||
          operation.nativeResult ||
          operation.commit
        )
          return { status: "rejected", reason: "phase-mismatch" };
        operation.phase = "blocked";
        operation.blockedReason = progress.reason;
        break;
    }
    operation.updatedAt = now;
    if (operation.phase === "blocked") {
      const lease = await transaction.get<RepoLease>("compactLease");
      if (lease?.token === operation.snapshot?.token) await transaction.delete("compactLease");
      const pending = await transaction.get<{ token: string }>("reachabilityGcPending");
      if (pending?.token === operation.snapshot?.token)
        await transaction.delete("reachabilityGcPending");
    }
    if (operation.phase !== previousPhase) {
      const measurement = operation.measurements[previousPhase];
      if (measurement)
        operation.measurements[previousPhase] = {
          ...measurement,
          completedAt: now,
          elapsedMs: now - measurement.startedAt,
        };
      // Snapshotting is part of the first rewrite invocation. Every other
      // stage is dispatched only after this durable handoff has committed.
      if (progress.kind !== "snapshot") delete operation.claim;
      if (progress.kind === "snapshot")
        operation.measurements.rewrite = { attempts: 1, startedAt: now };
    }
    await transaction.put(GC_OPERATION_KEY, operation);
    if (!isGcTerminal(operation)) await armWake(transaction, now + GC_WAKE_DELAY_MS);
    return { status: "ready", operation };
  });
  args.logger.info("reachability-gc:operation-progress", {
    transition: args.progress.kind,
    status: result.status,
  });
  return result;
}

/** The existing catalog commit remains the authority. The durable operation
 * supplies its original operands and retains the committed receipt before any
 * superseded object is eligible for this operation's cleanup dispatch. */
export async function commitGcOperation(args: {
  ctx: DurableObjectState;
  operationId: string;
  claimId: string;
  logger: Logger;
}): Promise<CommitReachabilityGcResult> {
  const operation = await args.ctx.storage.get<GcOperation>(GC_OPERATION_KEY);
  if (!operation || operation.id !== args.operationId)
    return { status: "retry", reason: "lease-mismatch" };
  if (operation.commit) return operation.commit;
  if (
    operation.phase !== "publish" ||
    !operation.snapshot ||
    !operation.closure ||
    operation.claim?.id !== args.claimId ||
    operation.claim.expiresAt <= Date.now()
  )
    return { status: "retry", reason: "lease-mismatch" };
  if (operation.nativeResult) {
    const pending = await recordReachabilityGcPendingState({
      ctx: args.ctx,
      token: operation.snapshot.token,
      packKey: operation.outputPackKey,
    });
    // A lost commit response may have cleared the lease already. In that case
    // still call the catalog's existing authoritative reconciliation path.
    if (pending.status !== "recorded" && pending.reason === "repository-deleting")
      return { status: "retry", reason: "repository-deleting" };
  }
  const result = await commitReachabilityGcState({
    ctx: args.ctx,
    logger: args.logger,
    gcOperationId: operation.id,
    gcClaimId: args.claimId,
    token: operation.snapshot.token,
    refsVersion: operation.snapshot.refsVersion,
    packsetVersion: operation.snapshot.packsetVersion,
    sourcePacks: operation.snapshot.sourcePacks,
    retainedPackKey: operation.retainedPackKey,
    stagedPack: operation.nativeResult
      ? {
          packKey: operation.outputPackKey,
          packBytes: operation.nativeResult.packBytes,
          idxBytes: operation.nativeResult.idxBytes,
          objectCount: operation.nativeResult.objectCount,
        }
      : undefined,
  });
  return result;
}

/** Discard only never-published, run-owned outputs after every possible writer
 * has drained. A catalog row is authoritative even when the receipt was lost. */
export async function gcDiscardKeys(
  ctx: DurableObjectState,
  operationId: string,
  claimId: string
): Promise<string[] | null> {
  const operation = await ctx.storage.get<GcOperation>(GC_OPERATION_KEY);
  if (
    (await ctx.storage.get<boolean>("repositoryDeleting")) ||
    !operation ||
    operation.id !== operationId ||
    operation.phase !== "discard" ||
    operation.commit ||
    operation.claim?.id !== claimId ||
    operation.claim.expiresAt <= Date.now() ||
    (operation.discardAfter ?? Infinity) > Date.now()
  )
    return null;
  if (await getPackCatalogRow(getDb(ctx.storage), operation.outputPackKey)) return null;
  const pending = await ctx.storage.get<{ token: string; state: string; safeCleanupAt?: number }>(
    "reachabilityGcPending"
  );
  if (
    pending &&
    pending.token === operation.snapshot?.token &&
    pending.state === "committing" &&
    (pending.safeCleanupAt ?? Infinity) > Date.now()
  )
    return null;
  return [
    operation.inputPackKey,
    operation.outputPackKey,
    packIndexKey(operation.outputPackKey),
    packRefsKey(operation.outputPackKey),
    operation.outputResultKey,
  ];
}

/** Rearm before enqueue: queue retry exhaustion or a lost send response cannot
 * consume the only wakeup. No R2 or Container work runs inside this alarm. */
export async function resumeGcFromAlarm(args: {
  ctx: DurableObjectState;
  env: Env;
  logger: Logger;
}): Promise<boolean> {
  const operation = await args.ctx.storage.transaction(async (transaction) => {
    if (await transaction.get<boolean>("repositoryDeleting")) return undefined;
    const current = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    if (!current || isGcTerminal(current)) return undefined;
    const now = Date.now();
    const lease = await transaction.get<RepoLease>("compactLease");
    if (
      current.snapshot &&
      lease?.token === current.snapshot.token &&
      lease.operation === "reachability-gc"
    ) {
      await transaction.put("compactLease", { ...lease, expiresAt: now + COMPACT_LEASE_TTL_MS });
    }
    await transaction.setAlarm(now + GC_WAKE_DELAY_MS);
    return current;
  });
  if (!operation) return false;
  if (operation.claim && operation.claim.safeRetryAt > Date.now()) return true;
  try {
    await args.env.REPO_TASKS_QUEUE.send({
      kind: "reachability-gc",
      doId: args.ctx.id.toString(),
      repoId: operation.repositoryId,
      operationId: operation.id,
    });
    args.logger.info("reachability-gc:durable-wake-enqueued", { phase: operation.phase });
  } catch {
    args.logger.warn("reachability-gc:durable-wake-deferred", { phase: operation.phase });
  }
  return true;
}
