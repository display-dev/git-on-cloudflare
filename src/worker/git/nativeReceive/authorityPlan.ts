import type {
  NativeReceiveAuthorityPublication,
  NativeReceiveAuthorityPublicationObjectPlan,
  NativeReceiveAuthorityPublicationPlan,
  NativeReceiveOperation,
  NativeReceiveProcessResult,
} from "./types";

import { asBufferSource, bytesToHex } from "@/worker/common";
import {
  nativeReceiveAuthorityReceiptKey,
  nativeReceiveAuthorityRefKey,
} from "@/worker/keys";

async function sha256Bytes(bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

async function jsonObjectPlan(
  key: string,
  value: unknown
): Promise<NativeReceiveAuthorityPublicationObjectPlan> {
  const json = JSON.stringify(value);
  const bytes = new TextEncoder().encode(json);
  return { key, bytes: bytes.byteLength, sha256: await sha256Bytes(bytes), json };
}

/** Pure authority plan: no Env, R2, Container, Worker RPC, or callback. */
export async function buildNativeReceiveAuthorityPublicationPlan(args: {
  operation: NativeReceiveOperation;
  processorResult: NativeReceiveProcessResult;
}): Promise<NativeReceiveAuthorityPublicationPlan> {
  if (
    args.operation.commands.length !== 1 ||
    args.operation.commands[0]!.newOid === "0".repeat(40)
  ) {
    throw new Error("FUBAR: stock authority publication requires one non-delete transition");
  }
  const command = args.operation.commands[0]!;
  const result = args.processorResult;
  if (
    !result.packSha256 ||
    !result.idxSha256 ||
    !result.refsSha256 ||
    !result.planSha256 ||
    result.outputValidationBytes === undefined ||
    result.outputValidationRequests !== 3 ||
    !result.outputPackEtag ||
    !result.outputIdxEtag ||
    !result.outputRefsEtag
  ) {
    throw new Error("FUBAR: stock publication lacks verified immutable output proof");
  }
  const refKey = nativeReceiveAuthorityRefKey(
    args.operation.outputPackKey,
    args.operation.id,
    args.operation.fingerprint,
    0
  );
  const ref = {
    ...(await jsonObjectPlan(refKey, {
      schemaVersion: 1,
      kind: "authoritative-ref",
      name: command.ref,
      oid: command.newOid,
    })),
    name: command.ref,
    oid: command.newOid,
  };
  const receiptDigest = await sha256Bytes(
    new TextEncoder().encode(
      JSON.stringify({
        operationId: args.operation.id,
        fingerprint: args.operation.fingerprint,
        refName: command.ref,
        oldOid: command.oldOid,
        newOid: command.newOid,
        packSha256: result.packSha256,
        idxSha256: result.idxSha256,
        refsSha256: result.refsSha256,
        planSha256: result.planSha256,
        outputValidationBytes: result.outputValidationBytes,
        outputValidationRequests: result.outputValidationRequests,
        outputPackEtag: result.outputPackEtag,
        outputIdxEtag: result.outputIdxEtag,
        outputRefsEtag: result.outputRefsEtag,
      })
    )
  );
  const receiptKey = nativeReceiveAuthorityReceiptKey(
    args.operation.outputPackKey,
    args.operation.id,
    args.operation.fingerprint
  );
  const receipt = {
    ...(await jsonObjectPlan(receiptKey, {
      schemaVersion: 1,
      kind: "operation-receipt",
      disposition: "committed",
      refName: command.ref,
      newOid: command.newOid,
      digest: receiptDigest,
    })),
    disposition: "committed" as const,
    refName: command.ref,
    newOid: command.newOid,
    digest: receiptDigest,
  };
  const token = await sha256Bytes(
    new TextEncoder().encode(
      `stock-authority-publication-v1\0${args.operation.id}\0${args.operation.fingerprint}\0${ref.sha256}\0${receipt.sha256}`
    )
  );
  return {
    token,
    operationId: args.operation.id,
    fingerprint: args.operation.fingerprint,
    refs: [ref],
    receipt,
  };
}

function boundedEtag(value: string): boolean {
  return value.length > 0 && value.length <= 256 && !/[\0\r\n]/.test(value);
}

export function authorityPublicationMatchesPlan(
  plan: NativeReceiveAuthorityPublicationPlan,
  proof: NativeReceiveAuthorityPublication
): boolean {
  return (
    proof.refs.length === plan.refs.length &&
    proof.refs.length <= 16 &&
    proof.refs.every((candidate, index) => {
      const expected = plan.refs[index];
      return (
        expected !== undefined &&
        candidate.name === expected.name &&
        candidate.oid === expected.oid &&
        candidate.key === expected.key &&
        candidate.bytes === expected.bytes &&
        candidate.sha256 === expected.sha256 &&
        boundedEtag(candidate.etag)
      );
    }) &&
    proof.receipt.disposition === plan.receipt.disposition &&
    proof.receipt.refName === plan.receipt.refName &&
    proof.receipt.newOid === plan.receipt.newOid &&
    proof.receipt.digest === plan.receipt.digest &&
    proof.receipt.key === plan.receipt.key &&
    proof.receipt.bytes === plan.receipt.bytes &&
    proof.receipt.sha256 === plan.receipt.sha256 &&
    boundedEtag(proof.receipt.etag)
  );
}
