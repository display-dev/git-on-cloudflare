import type { GcOperation } from "./gcOperation";

/** Deliberately excludes object keys, execution claims and reader tokens. */
export function gcOperationStatus(operation: GcOperation) {
  const reader = operation.qualification?.reader;
  return {
    schemaVersion: 1,
    operationId: operation.id,
    phase: operation.phase,
    createdAt: operation.createdAt,
    updatedAt: operation.updatedAt,
    source: operation.snapshot
      ? {
          refsVersion: operation.snapshot.refsVersion,
          packsetVersion: operation.snapshot.packsetVersion,
          packCount: operation.snapshot.sourcePacks.length,
          packBytes: operation.snapshot.sourcePacks.reduce((sum, pack) => sum + pack.packBytes, 0),
        }
      : null,
    closure: operation.closure ?? null,
    rewrite: operation.rewrite
      ? { packBytes: operation.rewrite.packBytes, packSha1: operation.rewrite.packSha1 }
      : null,
    exactPackReused: Boolean(operation.retainedPackKey),
    native: operation.nativeResult
      ? {
          packBytes: operation.nativeResult.packBytes,
          idxBytes: operation.nativeResult.idxBytes,
          refsBytes: operation.nativeResult.refsBytes,
          objectCount: operation.nativeResult.objectCount,
          packSha1: operation.nativeResult.packSha1,
          elapsedMs: operation.nativeResult.elapsedMs,
          scratchBytes: operation.nativeResult.scratchBytes,
          downloadedBytes: operation.nativeResult.downloadedBytes,
          cacheHitBytes: operation.nativeResult.cacheHitBytes,
          maintenance: operation.nativeResult.maintenance,
        }
      : null,
    nativeReadyAt: operation.nativeReadyAt ?? null,
    nativeWasRunning: operation.nativeWasRunning ?? null,
    publication: operation.commit
      ? {
          generation: operation.commit.packCatalogVersion,
          supersededPackCount: operation.commit.supersededPackKeys.length,
        }
      : null,
    blockedReason: operation.blockedReason ?? null,
    measurements: operation.measurements,
    stepMeasurements: operation.stepMeasurements ?? {},
    qualification: operation.qualification
      ? {
          deadlineAt: operation.qualification.deadlineAt,
          faults: operation.qualification.faults,
          reader: reader
            ? {
                startedAt: reader.startedAt ?? null,
                releasedAt: reader.releasedAt ?? null,
                deletionAttemptAt: reader.deletionAttemptAt ?? null,
                generation: reader.generation ?? null,
                expired: reader.expired ?? false,
              }
            : null,
        }
      : null,
  };
}
