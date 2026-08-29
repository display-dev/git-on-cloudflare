import type { RepoDurableObject } from "@/worker/do/repo/repoDO";
import type { Limiter } from "@/worker/git/operations/limits";
import { doPrefix, packIndexKey, packRefsKey } from "@/worker/keys";
import { readPublishedRepositoryGenerationState } from "@/worker/git/generation/publish";
import { EXPIRED_WRITER_DRAIN_MS } from "@/worker/do/repo/repositoryLifecycle";
import { isValidRefName } from "@/worker/git/operations/validation";
import { isValidNativeReceiveOperationId } from "@/worker/git/nativeReceive/types";

type AuthorityOwner = {
  operationId: string;
  fingerprint: string;
  ref?: { object: R2Object; name: string; oid: string };
  receipt?: { object: R2Object; name: string; oid: string };
};

function authorityIdentity(relative: string) {
  const matched = relative.match(
    /^native-receive\/authority\/(.+)-([a-f0-9]{64})\/(ref-0|receipt)\.json$/
  );
  if (!matched || !isValidNativeReceiveOperationId(matched[1]!)) return null;
  return {
    operationId: matched[1]!,
    fingerprint: matched[2]!,
    role: matched[3]! as "ref-0" | "receipt",
  };
}

function nativeOutputOwner(relative: string) {
  const matched = relative.match(
    /^objects\/pack\/pack-native-(.+)-([a-f0-9]{64})-claim-[a-f0-9-]{36}\.(pack|idx|refs)$/
  );
  if (!matched || !isValidNativeReceiveOperationId(matched[1]!)) return null;
  return `${matched[1]}\0${matched[2]}`;
}

/** Synthetic qualification only. Never accepts caller-selected object keys. */
export async function recoverQualificationStorage(args: {
  env: Env;
  stub: DurableObjectStub<RepoDurableObject>;
  limiter: Limiter;
  expectedRefStateDigest: string;
  expectedObjectCount: number;
}) {
  const held = await args.stub.beginQualificationStorageRecovery(args.expectedRefStateDigest);
  if (held.status !== "held")
    return { status: "conflict", reason: "repository_active_or_changed" } as const;
  const prefix = doPrefix(args.stub.id.toString());
  try {
    const published = await readPublishedRepositoryGenerationState({
      env: args.env,
      doId: args.stub.id.toString(),
      limiter: args.limiter,
      countSubrequest: () => {},
    });
    if (!published || published.generation !== held.packsetVersion) {
      return { status: "conflict", reason: "generation_not_published" } as const;
    }
    const protectedKeys = new Set<string>();
    for (const packKey of [
      ...held.catalog.map((row) => row.packKey),
      ...published.activePackKeys,
    ]) {
      protectedKeys.add(packKey);
      protectedKeys.add(packIndexKey(packKey));
      protectedKeys.add(packRefsKey(packKey));
    }
    // This recovery seam is deliberately bounded, not a general bucket sweep.
    const page = await args.env.REPO_BUCKET.list({ prefix: `${prefix}/`, limit: 1000 });
    if (page.truncated || page.objects.length !== args.expectedObjectCount) {
      return { status: "conflict", reason: "storage_state_mismatch" } as const;
    }
    const candidates: R2Object[] = [];
    const unprotected: Array<{ object: R2Object; relative: string }> = [];
    // Admission already waits for each writer lease's expiry PLUS its drain
    // period. Do not charge the full writer lease again from object upload:
    // that timestamp can be near the end of an already-expired invocation.
    const oldestAllowed = Date.now() - EXPIRED_WRITER_DRAIN_MS;
    for (const object of page.objects) {
      if (
        protectedKeys.has(object.key) ||
        object.key === `${prefix}/generation-index.json` ||
        new RegExp(`^${prefix}/generations/[0-9]+\\.json$`).test(object.key)
      )
        continue;
      if (object.uploaded.getTime() > oldestAllowed) {
        return { status: "conflict", reason: "orphan_writer_drain" } as const;
      }
      unprotected.push({ object, relative: object.key.slice(prefix.length + 1) });
    }
    if (unprotected.length > 100) {
      return { status: "conflict", reason: "recovery_budget" } as const;
    }

    // Authority records are the ownership root for native receive output. A
    // filename or run marker alone can never make a pack eligible.
    const owners = new Map<string, AuthorityOwner>();
    for (const { object, relative } of unprotected) {
      const identity = authorityIdentity(relative);
      if (identity && object.size <= 4096) {
        const body = await args.env.REPO_BUCKET.get(object.key);
        if (!body || body.etag !== object.etag)
          return { status: "conflict", reason: "orphan_changed" } as const;
        const value = await body.json<Record<string, unknown>>();
        const ref = value.kind === "authoritative-ref" ? value.name : value.refName;
        const oid = value.kind === "authoritative-ref" ? value.oid : value.newOid;
        const recognized =
          value.schemaVersion === 1 &&
          (value.kind === "authoritative-ref" ||
            (value.kind === "operation-receipt" &&
              value.disposition === "committed" &&
              typeof value.digest === "string" &&
              /^[a-f0-9]{64}$/.test(value.digest)));
        if (
          !recognized ||
          typeof ref !== "string" ||
          !isValidRefName(ref) ||
          typeof oid !== "string" ||
          !/^[a-f0-9]{40}$/.test(oid) ||
          held.refs.some((r) => r.name === ref)
        ) {
          return { status: "conflict", reason: "unrecognized_authority" } as const;
        }
        const ownerKey = `${identity.operationId}\0${identity.fingerprint}`;
        const owner = owners.get(ownerKey) ?? {
          operationId: identity.operationId,
          fingerprint: identity.fingerprint,
        };
        const proof = { object, name: ref, oid };
        if (identity.role === "ref-0") owner.ref = proof;
        else owner.receipt = proof;
        owners.set(ownerKey, owner);
      }
    }
    if (
      [...owners.values()].some(
        (owner) =>
          !owner.ref ||
          !owner.receipt ||
          owner.ref.name !== owner.receipt.name ||
          owner.ref.oid !== owner.receipt.oid
      )
    ) {
      return { status: "conflict", reason: "unrecognized_authority" } as const;
    }
    for (const { object, relative } of unprotected) {
      const identity = authorityIdentity(relative);
      if (identity) {
        if (!owners.has(`${identity.operationId}\0${identity.fingerprint}`)) {
          return { status: "conflict", reason: "unrecognized_authority" } as const;
        }
        candidates.push(object);
        continue;
      }
      const ownerKey = nativeOutputOwner(relative);
      if (ownerKey && owners.has(ownerKey)) {
        candidates.push(object);
        continue;
      }
      return { status: "conflict", reason: "unrecognized_orphan" } as const;
    }
    if (candidates.length > 100 || Date.now() + 60_000 >= held.lease.expiresAt) {
      return { status: "conflict", reason: "recovery_budget" } as const;
    }
    // The complete candidate plan is checked before the first deletion. An
    // interrupted batch is safe to repeat from a new authoritative inventory.
    if (candidates.length) await args.env.REPO_BUCKET.delete(candidates.map((o) => o.key));
    const after = await args.env.REPO_BUCKET.list({ prefix: `${prefix}/`, limit: 1000 });
    const removed = new Set(candidates.map((o) => o.key));
    if (after.truncated || after.objects.some((o) => removed.has(o.key))) {
      return { status: "inconclusive", reason: "deletion_not_proven" } as const;
    }
    return {
      status: "recovered",
      deletedObjectCount: candidates.length,
      deletedObjectBytes: candidates.reduce((sum, o) => sum + o.size, 0),
      remainingObjectCount: after.objects.length,
    } as const;
  } finally {
    await args.stub.abortCompaction(held.lease.token);
  }
}
