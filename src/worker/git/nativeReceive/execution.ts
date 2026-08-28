import type { RepositoryContainerBridgeProps } from "./types";
import { bytesToHex } from "@/worker/common";

export type NativeExecutionLane = "foreground" | "maintenance";
/** A compute capability, not a ref/catalog publication capability. */
export type NativeExecutionIdentity = {
  repositoryId: string;
  lane: NativeExecutionLane;
  generation: number;
  operationId: string;
  claimId: string;
  expiresAt: number;
  grantSha256: string;
};
export type NativeExecutionRecord = {
  identity: NativeExecutionIdentity;
  state: "active" | "completed" | "revoked";
  drainUntil: number;
  stopPending?: boolean;
  dispatchedAt?: number;
  completedAt?: number;
  inputReadStartedAt?: number;
  readRequests?: number;
  declaredReadBytes?: number;
  writeRequests?: number;
  completedWriteBytes?: number;
};
export const NATIVE_EXECUTION_TIMEOUT_MS = 14 * 60_000;

export function sameNativeExecution(
  a: NativeExecutionIdentity,
  b: NativeExecutionIdentity
): boolean {
  return (
    a.repositoryId === b.repositoryId &&
    a.lane === b.lane &&
    a.generation === b.generation &&
    a.operationId === b.operationId &&
    a.claimId === b.claimId &&
    a.expiresAt === b.expiresAt &&
    a.grantSha256 === b.grantSha256
  );
}

export async function nativeBridgeGrantDigest(
  props: RepositoryContainerBridgeProps
): Promise<string> {
  const descriptor = {
    operationId: props.operationId,
    readKeys: props.readKeys,
    writeKeys: props.writeKeys,
    requireWriteSha256: props.requireWriteSha256 ?? false,
    durableOutputOwner: props.durableOutputOwner ?? false,
  };
  return bytesToHex(
    new Uint8Array(
      await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(descriptor)))
    )
  );
}
