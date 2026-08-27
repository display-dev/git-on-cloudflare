import type { RepoDurableObject } from "@/worker/do/repo/repoDO";
import type { Limiter } from "@/worker/git/operations/limits";
import { doPrefix, packIndexKey, packRefsKey } from "@/worker/keys";
import { readPublishedRepositoryGenerationState } from "@/worker/git/generation/publish";
import { COMPACT_LEASE_TTL_MS } from "@/worker/do/repo/catalog/shared";
import { EXPIRED_WRITER_DRAIN_MS } from "@/worker/do/repo/repositoryLifecycle";

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
    const oldestAllowed = Date.now() - COMPACT_LEASE_TTL_MS - EXPIRED_WRITER_DRAIN_MS;
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
      const relative = object.key.slice(prefix.length + 1);
      if (/^objects\/pack\/pack-(cmp|gc)-[a-f0-9-]{36}\.(pack|idx|refs)$/.test(relative)) {
        candidates.push(object);
        continue;
      }
      // Reset has removed all operation records and all run refs have already
      // been reconciled. Only proofs for absent synthetic run refs are eligible.
      if (
        /^native-receive\/authority\/[^/]+\/(ref-0|receipt)\.json$/.test(relative) &&
        object.size <= 4096
      ) {
        const body = await args.env.REPO_BUCKET.get(object.key);
        if (!body || body.etag !== object.etag)
          return { status: "conflict", reason: "orphan_changed" } as const;
        const value = await body.json<Record<string, unknown>>();
        const ref = value.kind === "authoritative-ref" ? value.name : value.refName;
        const oid = value.kind === "authoritative-ref" ? value.oid : value.newOid;
        const recognized =
          value.schemaVersion === 1 &&
          (value.kind === "authoritative-ref" ||
            (value.kind === "operation-receipt" && value.disposition === "committed"));
        if (
          !recognized ||
          typeof ref !== "string" ||
          !/^refs\/heads\/qual-[A-Za-z0-9_-]+$/.test(ref) ||
          typeof oid !== "string" ||
          !/^[a-f0-9]{40}$/.test(oid) ||
          held.refs.some((r) => r.name === ref)
        ) {
          return { status: "conflict", reason: "unrecognized_authority" } as const;
        }
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
