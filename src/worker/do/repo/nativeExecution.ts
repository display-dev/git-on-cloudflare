import type {
  RepositoryContainerBridgeProps,
  NativeReceiveOperation,
} from "@/worker/git/nativeReceive/types";
import {
  nativeBridgeGrantDigest,
  NATIVE_EXECUTION_TIMEOUT_MS,
  sameNativeExecution,
  type NativeExecutionIdentity,
  type NativeExecutionLane,
  type NativeExecutionRecord,
} from "@/worker/git/nativeReceive/execution";
import { GC_OPERATION_KEY, type GcOperation } from "@/worker/git/maintenance/gcOperation";
import { nativeReceiveOperationKey } from "./repoState";
import { EXPIRED_WRITER_DRAIN_MS } from "./repositoryLifecycle";
import { createLogger } from "@/worker/common/logger";

const log = createLogger("info", { service: "NativeExecution" });

export function nativeExecutionKey(lane: NativeExecutionLane): string {
  return `nativeExecution:${lane}`;
}

async function ownsClaim(
  store: DurableObjectStorage | DurableObjectTransaction,
  lane: NativeExecutionLane,
  operationId: string,
  claimId: string
): Promise<boolean> {
  if (await store.get<boolean>("repositoryDeleting")) return false;
  if (lane === "maintenance") {
    const operation = await store.get<GcOperation>(GC_OPERATION_KEY);
    return (
      operation?.id === operationId &&
      operation.phase === "index" &&
      operation.claim?.id === claimId &&
      operation.claim.expiresAt > Date.now()
    );
  }
  const operation = await store.get<NativeReceiveOperation>(nativeReceiveOperationKey(operationId));
  return (
    operation?.state === "processing" &&
    operation.claimId === claimId &&
    (operation.claimExpiresAt ?? 0) > Date.now()
  );
}

/** Only the repository coordinator issues execution generations. The host's
 * copy fences compute; it never owns domain claims or publication. */
export async function beginNativeExecution(
  ctx: DurableObjectState,
  lane: NativeExecutionLane,
  operationId: string,
  claimId: string,
  props: RepositoryContainerBridgeProps
): Promise<NativeExecutionIdentity | null> {
  if (props.operationId !== operationId) return null;
  const grantSha256 = await nativeBridgeGrantDigest(props);
  return ctx.storage.transaction(async (store) => {
    if (!(await ownsClaim(store, lane, operationId, claimId))) return null;
    const previous = await store.get<NativeExecutionRecord>(nativeExecutionKey(lane));
    if (
      previous?.state === "active" ||
      previous?.stopPending ||
      (previous?.drainUntil ?? 0) > Date.now()
    )
      return null;
    const generation = ((await store.get<number>("nativeExecutionGeneration")) ?? 0) + 1;
    const identity: NativeExecutionIdentity = {
      repositoryId: ctx.id.toString(),
      lane,
      generation,
      operationId,
      claimId,
      expiresAt: Date.now() + NATIVE_EXECUTION_TIMEOUT_MS,
      grantSha256,
    };
    await store.put("nativeExecutionGeneration", generation);
    await store.put<NativeExecutionRecord>(nativeExecutionKey(lane), {
      identity,
      state: "active",
      drainUntil: 0,
      dispatchedAt: Date.now(),
    });
    log.info("native-execution:admitted", { lane, generation });
    return identity;
  });
}

export async function authorizeNativeExecution(
  ctx: DurableObjectState,
  identity: NativeExecutionIdentity
): Promise<boolean> {
  return ctx.storage.transaction(async (store) => {
    if (identity.repositoryId !== ctx.id.toString() || identity.expiresAt <= Date.now())
      return false;
    const record = await store.get<NativeExecutionRecord>(nativeExecutionKey(identity.lane));
    return (
      !!record &&
      record.state === "active" &&
      sameNativeExecution(record.identity, identity) &&
      (await ownsClaim(store, identity.lane, identity.operationId, identity.claimId))
    );
  });
}

export async function finishNativeExecution(
  ctx: DurableObjectState,
  identity: NativeExecutionIdentity,
  outcome: "completed" | "revoked"
): Promise<boolean> {
  return ctx.storage.transaction(async (store) => {
    const record = await store.get<NativeExecutionRecord>(nativeExecutionKey(identity.lane));
    if (!record || !sameNativeExecution(record.identity, identity)) return false;
    if (record.state !== "active") return false;
    if (
      outcome === "completed" &&
      (identity.repositoryId !== ctx.id.toString() ||
        identity.expiresAt <= Date.now() ||
        !(await ownsClaim(store, identity.lane, identity.operationId, identity.claimId)))
    )
      return false;
    record.state = outcome;
    record.stopPending = outcome === "revoked";
    record.completedAt = Date.now();
    // Revocation prevents new bridge requests, not completion of an already
    // started R2 write. Keep its ordinary writer drain before key reuse/sweep.
    record.drainUntil = outcome === "revoked" ? Date.now() + EXPIRED_WRITER_DRAIN_MS : 0;
    await store.put(nativeExecutionKey(identity.lane), record);
    log.info("native-execution:settled", {
      lane: identity.lane,
      generation: identity.generation,
      outcome,
    });
    return true;
  });
}

export async function noteNativeBridgeIO(
  ctx: DurableObjectState,
  identity: NativeExecutionIdentity,
  kind: "read" | "write",
  bytes: number
): Promise<void> {
  if (!Number.isSafeInteger(bytes) || bytes < 0) return;
  await ctx.storage.transaction(async (store) => {
    const record = await store.get<NativeExecutionRecord>(nativeExecutionKey(identity.lane));
    if (!record || !sameNativeExecution(record.identity, identity)) return;
    if (kind === "read") {
      record.inputReadStartedAt ??= Date.now();
      record.readRequests = (record.readRequests ?? 0) + 1;
      record.declaredReadBytes = (record.declaredReadBytes ?? 0) + bytes;
    } else {
      record.writeRequests = (record.writeRequests ?? 0) + 1;
      record.completedWriteBytes = (record.completedWriteBytes ?? 0) + bytes;
    }
    await store.put(nativeExecutionKey(identity.lane), record);
  });
}

export async function acknowledgeNativeStop(
  ctx: DurableObjectState,
  identity: NativeExecutionIdentity
): Promise<void> {
  await ctx.storage.transaction(async (store) => {
    const record = await store.get<NativeExecutionRecord>(nativeExecutionKey(identity.lane));
    if (record?.state === "revoked" && sameNativeExecution(record.identity, identity)) {
      record.stopPending = false;
      await store.put(nativeExecutionKey(identity.lane), record);
    }
  });
}
