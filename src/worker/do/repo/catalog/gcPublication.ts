import {
  GC_OPERATION_KEY,
  GC_WAKE_DELAY_MS,
  type GcOperation,
  type GcCommit,
} from "@/worker/git/maintenance/gcOperation";
import { isNativeReceiveTerminal } from "@/worker/git/nativeReceive/types";
import {
  asTypedStorage,
  nativeReceiveOperationKey,
  receiveFinalizeIntentKey,
  type RepoStateSchema,
} from "../repoState";
import { getDb, getPackCatalogRow, listActivePackCatalog, replaceActivePackCatalog } from "../db";
import type { PackCatalogRow } from "../db/schema";
import type { CommitReachabilityGcResult } from "./reachabilityGc";
import { rowsMatchForCommit } from "./compaction/plan";
import { EXPIRED_WRITER_DRAIN_MS } from "../repositoryLifecycle";
import { createLogger } from "@/worker/common";
import { activeStockReceivePreparationLeases } from "../nativeReceiveActivity";

let failAfterPublicationWritesForTesting = false;
let publicationWriteFailuresForTesting = 0;
export const __test = {
  failAfterPublicationWrites() {
    failAfterPublicationWritesForTesting = true;
  },
  reset() {
    failAfterPublicationWritesForTesting = false;
    publicationWriteFailuresForTesting = 0;
  },
  failureCount: () => publicationWriteFailuresForTesting,
};

/** A prepared publication gets a turn before the next receive. This does no
 * R2 work, scans, native processing or drain waiting. A lost acknowledgement
 * replays the receipt; an expired claim simply leaves admission open. */
export async function helpPreparedGcPublication(ctx: DurableObjectState): Promise<void> {
  const operation = await ctx.storage.get<GcOperation>(GC_OPERATION_KEY);
  const claimId = operation?.coordination?.publicationClaimId;
  if (
    operation?.phase === "publish" &&
    claimId &&
    operation.claim?.id === claimId &&
    operation.claim.expiresAt > Date.now()
  )
    await commitCoordinatedGc(ctx, operation.id, claimId);
}

/** SQLite catalog mutation, accounted versions, generation intent and receipt
 * share one DO storage transaction. Current refs/HEAD are never replaced.
 * The fixed source rows are merged with all later receive packs. */
export async function commitCoordinatedGc(
  ctx: DurableObjectState,
  operationId: string,
  claimId: string
): Promise<CommitReachabilityGcResult> {
  const log = createLogger(undefined, { service: "GcPublication" });
  try {
    return await ctx.storage.transaction<CommitReachabilityGcResult>(async (transaction) => {
      const store = asTypedStorage<RepoStateSchema>(transaction);
      const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
      if (!operation || operation.id !== operationId)
        return { status: "retry", reason: "lease-mismatch" };
      if (operation.commit) return operation.commit;
      if (await store.get("repositoryDeleting"))
        return { status: "retry", reason: "repository-deleting" };
      const now = Date.now();
      const source = operation.snapshot;
      const coordination = operation.coordination;
      const lease = await store.get("compactLease");
      if (
        !source ||
        !coordination ||
        !operation.closure ||
        operation.phase !== "publish" ||
        operation.claim?.id !== claimId ||
        operation.claim.expiresAt <= now ||
        coordination.publicationClaimId !== claimId ||
        lease?.token !== source.token ||
        lease.operation !== "reachability-gc" ||
        lease.expiresAt <= now
      )
        return { status: "retry", reason: "lease-mismatch" };
      const receive = await store.get("receiveLease");
      if (
        receive &&
        (receive.expiresAt + EXPIRED_WRITER_DRAIN_MS > now ||
          (await store.get(receiveFinalizeIntentKey(receive.token))))
      )
        return { status: "retry", reason: "receive-active" };
      if (
        activeStockReceivePreparationLeases(await store.get("stockReceivePreparationLeases"), now)
          .length > 0
      )
        return { status: "retry", reason: "receive-active" };
      for (const id of (await store.get("nativeReceiveOperationIndex")) ?? []) {
        const native = await store.get(nativeReceiveOperationKey(id));
        if (native && !isNativeReceiveTerminal(native.state))
          return { status: "retry", reason: "receive-active" };
      }
      const activeReaders = ((await store.get("repositoryReadLeases")) ?? []).filter(
        (reader) => reader.operation === "snapshot-projection" && reader.expiresAt > now
      );
      if (activeReaders.length > 0) return { status: "retry", reason: "receive-active" };
      if (((await store.get("refsVersion")) ?? 0) !== coordination.refsVersion)
        return { status: "retry", reason: "refs-changed" };
      if (((await store.get("snapshotPinVersion")) ?? 0) !== coordination.snapshotPinVersion)
        return { status: "retry", reason: "pins-changed" };
      if (((await store.get("packsetVersion")) ?? 0) !== coordination.packsetVersion)
        return { status: "retry", reason: "packset-changed" };
      const db = getDb(ctx.storage);
      const currentRows: PackCatalogRow[] = [];
      for (const row of source.sourcePacks) {
        const current = await getPackCatalogRow(db, row.packKey);
        if (current) currentRows.push(current);
      }
      if (!rowsMatchForCommit(source.sourcePacks, currentRows))
        return { status: "retry", reason: "source-changed" };
      let target = source.sourcePacks.find((row) => row.packKey === operation.retainedPackKey);
      if (operation.nativeResult) {
        const pending = await store.get("reachabilityGcPending");
        if (pending?.token !== source.token || pending.packKey !== operation.outputPackKey)
          return { status: "retry", reason: "pending-mismatch" };
        target = {
          packKey: operation.outputPackKey,
          kind: "compact",
          state: "active",
          tier: Math.max(0, ...source.sourcePacks.map((row) => row.tier)) + 1,
          seqLo: Math.min(...source.sourcePacks.map((row) => row.seqLo)),
          seqHi: Math.max(...source.sourcePacks.map((row) => row.seqHi)),
          objectCount: operation.nativeResult.objectCount,
          packBytes: operation.nativeResult.packBytes,
          idxBytes: operation.nativeResult.idxBytes,
          createdAt: now,
          supersededBy: null,
        };
      }
      if (!target && operation.closure.objectCount !== 0)
        return { status: "retry", reason: "source-changed" };
      const retained = new Set(coordination.retainedSourcePackKeys);
      const supersededPackKeys = source.sourcePacks
        .map((row) => row.packKey)
        .filter((key) => key !== target?.packKey && !retained.has(key));
      const changed = supersededPackKeys.length > 0 || Boolean(operation.nativeResult);
      if (changed)
        replaceActivePackCatalog({ db, sourcePackKeys: supersededPackKeys, targetPack: target });
      const generation = coordination.packsetVersion + (changed ? 1 : 0);
      if (changed) {
        await store.put("packsetVersion", generation);
        await store.put("generationPublicationPending", {
          generation,
          activePackKeys: (await listActivePackCatalog(db)).map((row) => row.packKey),
        });
      }
      const committed: GcCommit = {
        status: "committed",
        packCatalogVersion: generation,
        supersededPackKeys,
        targetPackKey: target?.packKey,
      };
      operation.commit = committed;
      operation.phase = "reclaim";
      operation.updatedAt = now;
      const measurement = operation.measurements.publish;
      if (measurement)
        operation.measurements.publish = {
          ...measurement,
          completedAt: now,
          elapsedMs: now - measurement.startedAt,
        };
      delete operation.claim;
      delete coordination.publicationClaimId;
      await transaction.put(GC_OPERATION_KEY, operation);
      await store.delete("compactLease");
      await store.delete("reachabilityGcPending");
      if (failAfterPublicationWritesForTesting) {
        failAfterPublicationWritesForTesting = false;
        publicationWriteFailuresForTesting++;
        throw new Error("injected failure after GC catalog and publication writes");
      }
      // New receives may have requested ordinary compaction. Preserve it.
      const alarm = await transaction.getAlarm();
      if (alarm === null || alarm > now + GC_WAKE_DELAY_MS)
        await transaction.setAlarm(now + GC_WAKE_DELAY_MS);
      log.info("gc:catalog-merged", {
        generation,
        supersededPackCount: supersededPackKeys.length,
        protectedSourcePackCount: retained.size,
        acceptedReceives: coordination.acceptedReceives,
      });
      return committed;
    });
  } catch {
    // Both SQL and KV roll back. Never release ownership or delete output on
    // an uncertain transaction; its same-operation retry remains authoritative.
    log.warn("gc:catalog-transaction-retry");
    return { status: "retry", reason: "catalog-replacement-failed" };
  }
}
