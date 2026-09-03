import type { Logger } from "@/worker/common/logger";
import type {
  NativeReceiveAuthorityPublication,
  NativeReceiveAuthorityPublicationObjectPlan,
  NativeReceiveAuthorityPublicationPlan,
} from "./types";
import type { Limiter } from "@/worker/git/operations/limits";

import { asBufferSource, bytesToHex } from "@/worker/common";

async function sha256Bytes(bytes: Uint8Array): Promise<string> {
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

async function putImmutablePublicationObject(args: {
  env: Env;
  limiter: Limiter;
  plan: NativeReceiveAuthorityPublicationObjectPlan;
  countSubrequest(op: string, n?: number): void;
}): Promise<string> {
  const bytes = new TextEncoder().encode(args.plan.json);
  if (bytes.byteLength !== args.plan.bytes || (await sha256Bytes(bytes)) !== args.plan.sha256) {
    throw new Error("authority publication plan bytes do not match their digest");
  }
  args.countSubrequest("r2:put-native-authority");
  const created = await args.limiter.run("r2:put-native-authority", () =>
    args.env.REPO_BUCKET.put(args.plan.key, bytes, {
      onlyIf: { etagDoesNotMatch: "*" },
      httpMetadata: { contentType: "application/json" },
      customMetadata: { sha256: args.plan.sha256 },
    })
  );
  if (created) return created.etag;
  args.countSubrequest("r2:get-native-authority");
  const existing = await args.limiter.run("r2:get-native-authority", () =>
    args.env.REPO_BUCKET.get(args.plan.key)
  );
  if (!existing || existing.size !== args.plan.bytes) {
    throw new Error("immutable authority publication conflicts with existing R2 state");
  }
  const actual = new Uint8Array(await existing.arrayBuffer());
  const actualSha256 = await sha256Bytes(actual);
  if (actualSha256 !== args.plan.sha256) {
    const kind = args.plan.key.endsWith("/receipt.json") ? "receipt" : "ref";
    throw new Error(
      `immutable authority ${kind} digest mismatch: expected ${args.plan.sha256}, got ${actualSha256}`
    );
  }
  return existing.etag;
}

/** Worker-owned R2 publication; RepoDO receives only the resulting proof. */
export async function publishNativeReceiveAuthorityPlan(args: {
  env: Env;
  limiter: Limiter;
  plan: NativeReceiveAuthorityPublicationPlan;
  countSubrequest(op: string, n?: number): void;
  logger: Logger;
}): Promise<NativeReceiveAuthorityPublication> {
  const [refs, receiptEtag] = await Promise.all([
    Promise.all(
      args.plan.refs.map(async (ref) => ({
        name: ref.name,
        oid: ref.oid,
        key: ref.key,
        bytes: ref.bytes,
        sha256: ref.sha256,
        etag: await putImmutablePublicationObject({ ...args, plan: ref }),
      }))
    ),
    putImmutablePublicationObject({ ...args, plan: args.plan.receipt }),
  ]);
  args.logger.info("stock-receive:authority-published", {
    operationId: args.plan.operationId,
    refCount: refs.length,
  });
  return {
    refs,
    receipt: {
      disposition: "committed",
      refName: args.plan.receipt.refName,
      newOid: args.plan.receipt.newOid,
      digest: args.plan.receipt.digest,
      key: args.plan.receipt.key,
      bytes: args.plan.receipt.bytes,
      sha256: args.plan.receipt.sha256,
      etag: receiptEtag,
    },
  };
}
