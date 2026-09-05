import type { CacheContext } from "@/worker/cache";
import type { Logger } from "@/worker/common/logger";
import type { Limiter } from "@/worker/git/operations/limits";
import type { GcOperation, GcProgress, GcOperationResult, GcSnapshot } from "./gcOperation";
import type {
  BeginReachabilityGcResult,
  CommitReachabilityGcResult,
} from "@/worker/do/repo/catalog/reachabilityGc";
import type { GcNativeResult } from "@/worker/do/repo/gcNative";
import type { NativeReceiveProcessResult } from "@/worker/git/nativeReceive/types";
import { bytesToHex, getRepoStub } from "@/worker/common";
import { nativeReceiveProcessResultSchema } from "@/worker/do/repo/nativeReceive";
import { computeNeededFromPackRefs } from "@/worker/git/operations/fetch/refClosure";
import { loadPackRefSnapshot } from "@/worker/git/operations/fetch/plan";
import { loadOrderedPackSnapshot } from "@/worker/git/pack/snapshot";
import { rewritePackResult } from "@/worker/git/pack/rewrite";
import { stagePackToR2 } from "@/worker/git/receive/r2Upload";
import { parseIdxView, getOidHexAt } from "@/worker/git/object-store";
import { parsePackRefView } from "@/worker/git/pack/refIndex";
import { packIndexKey, packRefsKey } from "@/worker/keys";
import {
  publishPendingGeneration,
  reconcilePendingGc,
  reconcilePriorCleanup,
  scheduleSupersededPackCleanup,
  type ReachabilityGcResult,
} from "./reachabilityGc";

export type DurableGcArguments = {
  env: Env;
  repoId: string;
  operationId: string;
  cacheCtx: CacheContext;
  limiter: Limiter;
  log: Logger;
  countSubrequest(op: string, n?: number): void;
};

let beforeSnapshotRegistrationObserver:
  | ((begin: Extract<BeginReachabilityGcResult, { ok: true }>) => Promise<void>)
  | undefined;

// This bounds each artifact's bytes, not total validation memory: both sidecars
// and derived object sets coexist. Large-object-count memory capacity remains
// unqualified. A rejected immutable artifact cannot improve through retries.
const MAX_GC_SIDECAR_BYTES = 64 * 1024 * 1024;
class InvalidGcArtifact extends Error {}

function snapshotWants(snapshot: GcSnapshot): string[] {
  return Array.from(
    new Set([...snapshot.refs.map((ref) => ref.oid), ...(snapshot.snapshotPinOids ?? [])])
  );
}

export async function gcObjectSetDigest(oids: string[]): Promise<string> {
  const bytes = new TextEncoder().encode(
    [...oids]
      .sort()
      .map((oid) => `${oid}\n`)
      .join("")
  );
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)));
}

async function readGcObject(
  args: DurableGcArguments,
  key: string,
  expectedBytes: number
): Promise<Uint8Array> {
  args.countSubrequest("r2:gc-read-artifact");
  const object = await args.limiter.run("r2:gc-read-artifact", () => args.env.REPO_BUCKET.get(key));
  if (!object || object.size !== expectedBytes) {
    await object?.body.cancel();
    throw new InvalidGcArtifact("GC artifact size mismatch");
  }
  const bytes = new Uint8Array(await object.arrayBuffer());
  const sha256 = bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)));
  if (object.customMetadata?.sha256 !== sha256)
    throw new InvalidGcArtifact("GC artifact digest mismatch");
  return bytes;
}

/** A receipt is written by the native processor only after all three output
 * uploads have passed its bridge validation. Recover it before dispatching a
 * processor, including after a timeout or lost RPC response. */
async function readNativeReceipt(
  args: DurableGcArguments,
  operation: GcOperation
): Promise<NativeReceiveProcessResult | undefined> {
  args.countSubrequest("r2:gc-read-native-receipt");
  const object = await args.limiter.run("r2:gc-read-native-receipt", () =>
    args.env.REPO_BUCKET.get(operation.outputResultKey)
  );
  if (!object) return undefined;
  if (object.size > 64 * 1024) {
    await object.body.cancel();
    throw new InvalidGcArtifact("GC native receipt too large");
  }
  const bytes = new Uint8Array(await object.arrayBuffer());
  const sha256 = bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)));
  if (sha256 !== object.customMetadata?.sha256)
    throw new InvalidGcArtifact("GC native receipt digest mismatch");
  let decoded: unknown;
  try {
    decoded = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new InvalidGcArtifact("GC native receipt JSON invalid");
  }
  const parsed = nativeReceiveProcessResultSchema.safeParse(decoded);
  if (!parsed.success) throw new InvalidGcArtifact("GC native receipt schema invalid");
  const result = parsed.data;
  if (
    result.operationId !== operation.id ||
    result.maintenance?.objectSetSha256 !== operation.closure?.objectSetSha256 ||
    result.packSha1 !== operation.rewrite?.packSha1 ||
    result.packBytes !== operation.rewrite?.packBytes ||
    result.objectCount !== operation.closure?.objectCount
  )
    throw new InvalidGcArtifact("GC native receipt identity mismatch");
  return result;
}

async function validateNativeArtifacts(
  args: DurableGcArguments,
  operation: GcOperation,
  result: NativeReceiveProcessResult
): Promise<void> {
  if (result.idxBytes > MAX_GC_SIDECAR_BYTES || result.refsBytes > MAX_GC_SIDECAR_BYTES)
    throw new InvalidGcArtifact("GC sidecar exceeds artifact byte guard");
  const indexBytes = await readGcObject(
    args,
    packIndexKey(operation.outputPackKey),
    result.idxBytes
  );
  const idx = parseIdxView(operation.outputPackKey, indexBytes, result.packBytes);
  if (!idx || bytesToHex(idx.packChecksum) !== result.packSha1 || idx.count !== result.objectCount)
    throw new InvalidGcArtifact("GC output index identity mismatch");
  const oids = Array.from({ length: idx.count }, (_, index) => getOidHexAt(idx, index));
  if ((await gcObjectSetDigest(oids)) !== operation.closure?.objectSetSha256)
    throw new InvalidGcArtifact("GC output object set mismatch");
  const refBytes = await readGcObject(args, packRefsKey(operation.outputPackKey), result.refsBytes);
  const refs = parsePackRefView(operation.outputPackKey, refBytes, idx);
  if (refs.type !== "Ready") throw new InvalidGcArtifact("GC output reference sidecar invalid");
  const closure = await computeNeededFromPackRefs({
    logLevel: args.env.LOG_LEVEL,
    repoId: args.repoId,
    packs: [
      { packKey: operation.outputPackKey, packBytes: result.packBytes, idx, refs: refs.view },
    ],
    wants: snapshotWants(operation.snapshot!),
    haves: [],
  });
  if (
    closure.type !== "Ready" ||
    (await gcObjectSetDigest(closure.neededOids)) !== operation.closure?.objectSetSha256
  )
    throw new InvalidGcArtifact("GC replacement closure invalid");
  args.countSubrequest("r2:gc-output-head");
  const head = await args.limiter.run("r2:gc-output-head", () =>
    args.env.REPO_BUCKET.head(operation.outputPackKey)
  );
  if (!head || head.size !== result.packBytes || !head.customMetadata?.sha256)
    throw new InvalidGcArtifact("GC output pack unavailable");
}

/** One invocation executes at most one expensive stage. The operation ledger
 * and its alarm, not this invocation or a queue retry count, own continuation. */
export async function runDurableReachabilityGc(
  args: DurableGcArguments
): Promise<ReachabilityGcResult> {
  const stub = getRepoStub(args.env, args.repoId);
  args.cacheCtx.memo = { ...args.cacheCtx.memo, limiter: args.limiter };
  args.countSubrequest("do:claim-gc-operation");
  const claimed = await args.limiter.run<GcOperationResult>(
    "do:claim-gc-operation",
    async () => await stub.claimGcOperation(args.operationId)
  );
  if (claimed.status === "rejected") return { status: "blocked", reason: claimed.reason };
  if (claimed.status === "busy") return { status: "retry", reason: "gc-claim-active" };
  let operation = claimed.operation;
  if (operation.phase === "complete")
    return {
      status: "completed",
      reachableObjects: operation.closure?.objectCount ?? 0,
      sourcePacks: operation.snapshot?.sourcePacks.length ?? 0,
      scheduledArtifacts: (operation.commit?.supersededPackKeys.length ?? 0) * 3,
      packCatalogVersion:
        operation.commit?.packCatalogVersion ?? operation.snapshot?.packsetVersion ?? 0,
    };
  if (operation.phase === "blocked")
    return { status: "blocked", reason: operation.blockedReason ?? "gc-blocked" };
  const claimId = operation.claim!.id;
  const progress = async (change: GcProgress): Promise<void> => {
    args.countSubrequest("do:gc-progress");
    const result = await args.limiter.run<GcOperationResult>(
      "do:gc-progress",
      async () => await stub.recordGcProgress(operation.id, claimId, change)
    );
    if (result.status !== "ready") throw new Error("GC progress claim rejected");
    operation = result.operation;
  };
  const yieldOperation = async (reason: string): Promise<ReachabilityGcResult> => {
    await progress({ kind: "yield" });
    return { status: "retry", reason };
  };
  const afterRewrite = async (): Promise<void> => {
    if (args.env.QUALIFICATION_MODE !== "1") return;
    args.countSubrequest("do:gc-after-rewrite-control");
    if (
      await args.limiter.run("do:gc-after-rewrite-control", () =>
        stub.consumeGcFault(operation.id, "after-rewrite")
      )
    )
      throw new Error("qualification interrupted after durable GC rewrite");
  };
  args.log.info("reachability-gc:durable-stage", { phase: operation.phase });
  if (operation.phase === "queued") {
    if (!(await publishPendingGeneration({ ...args, stub })))
      return yieldOperation("generation-publication-pending");
    const legacy = await reconcilePendingGc({ ...args, stub });
    if (legacy)
      return yieldOperation(legacy.status === "retry" ? legacy.reason : "legacy-gc-pending");
    const cleanup = await reconcilePriorCleanup({ ...args, stub });
    if (cleanup) return yieldOperation("prior-cleanup-pending");
    args.countSubrequest("do:begin-reachability-gc");
    const begin = await args.limiter.run<BeginReachabilityGcResult>(
      "do:begin-reachability-gc",
      async () => await stub.beginReachabilityGc()
    );
    if (!begin.ok) return yieldOperation(begin.reason);
    await beforeSnapshotRegistrationObserver?.(begin);
    const snapshotProgress: GcProgress = {
      kind: "snapshot",
      snapshot: {
        token: begin.lease.token,
        refs: begin.refs,
        snapshotPinOids: begin.snapshotPinOids,
        snapshotPinVersion: begin.snapshotPinVersion,
        refsVersion: begin.refsVersion,
        packsetVersion: begin.packsetVersion,
        sourcePacks: begin.activeCatalog,
      },
    };
    args.countSubrequest("do:gc-progress");
    const registered = await args.limiter.run<GcOperationResult>(
      "do:gc-progress",
      async () => await stub.recordGcProgress(operation.id, claimId, snapshotProgress)
    );
    if (registered.status !== "ready") {
      args.countSubrequest("do:abort-reachability-gc");
      await args.limiter.run("do:abort-reachability-gc", () =>
        stub.abortCompaction(begin.lease.token)
      );
      return await yieldOperation("source-changed");
    }
    operation = registered.operation;
  }
  if (operation.phase === "rewrite") {
    args.countSubrequest("r2:gc-input-head");
    const existing = await args.limiter.run("r2:gc-input-head", () =>
      args.env.REPO_BUCKET.head(operation.inputPackKey)
    );
    if (existing) {
      if (!operation.rewriteIntent || existing.size !== operation.rewriteIntent.packBytes)
        throw new Error("GC rewrite output identity unresolved");
      args.countSubrequest("r2:gc-input-trailer");
      const trailer = await args.limiter.run("r2:gc-input-trailer", () =>
        args.env.REPO_BUCKET.get(operation.inputPackKey, {
          range: { offset: existing.size - 20, length: 20 },
        })
      );
      if (
        !trailer ||
        bytesToHex(new Uint8Array(await trailer.arrayBuffer())) !== operation.rewriteIntent.packSha1
      )
        throw new Error("GC rewrite checksum unresolved");
      await progress({
        kind: "rewrite-complete",
        identity: operation.rewriteIntent,
        etag: existing.etag,
      });
      await afterRewrite();
      return { status: "retry", reason: "native-indexing-pending" };
    }
    const source = operation.snapshot!;
    const planningStarted = Date.now();
    const wants = snapshotWants(source);
    if (source.sourcePacks.length === 0) {
      if (wants.length > 0) {
        await progress({ kind: "blocked", reason: "refs-without-active-packs" });
        return { status: "blocked", reason: "refs-without-active-packs" };
      }
      if (!operation.closure)
        await progress({
          kind: "plan",
          closure: { objectCount: 0, objectSetSha256: await gcObjectSetDigest([]) },
        });
      return { status: "retry", reason: "catalog-publication-pending" };
    }
    if (source.sourcePacks.length > 250) {
      await progress({ kind: "blocked", reason: "source-pack-limit" });
      return { status: "blocked", reason: "source-pack-limit" };
    }
    args.countSubrequest("r2:load-gc-pack-metadata", source.sourcePacks.length * 2);
    args.cacheCtx.memo.packCatalog = source.sourcePacks;
    const loaded = await loadOrderedPackSnapshot(args.env, args.repoId, args.cacheCtx, args.log);
    if (loaded.type !== "Ready") return yieldOperation(loaded.reason);
    const refSnapshot = await loadPackRefSnapshot(
      args.env,
      args.repoId,
      loaded.snapshot,
      args.cacheCtx,
      { scheduleMissingBackfill: false }
    );
    if (refSnapshot.type !== "Ready") {
      for (const missing of refSnapshot.packs) {
        args.countSubrequest("queue:gc-ref-backfill");
        await args.limiter.run("queue:gc-ref-backfill", () =>
          args.env.REPO_TASKS_QUEUE.send({
            kind: "pack-ref-backfill",
            doId: stub.id.toString(),
            repoId: args.repoId,
            packKey: missing.packKey,
          })
        );
      }
      return yieldOperation("missing-ref-index");
    }
    const closure = await computeNeededFromPackRefs({
      logLevel: args.env.LOG_LEVEL,
      repoId: args.repoId,
      packs: refSnapshot.packs,
      wants,
      haves: [],
    });
    if (closure.type !== "Ready") return yieldOperation(closure.reason);
    const digest = await gcObjectSetDigest(closure.neededOids);
    if (
      operation.closure &&
      (operation.closure.objectSetSha256 !== digest ||
        operation.closure.objectCount !== closure.neededOids.length)
    )
      throw new Error("GC source closure changed");
    let retainedPackKey: string | undefined;
    for (const pack of loaded.snapshot.packs) {
      if (pack.idx.count !== closure.neededOids.length) continue;
      const oids = Array.from({ length: pack.idx.count }, (_, index) =>
        getOidHexAt(pack.idx, index)
      );
      if ((await gcObjectSetDigest(oids)) === digest) {
        retainedPackKey = pack.packKey;
        break;
      }
    }
    await progress({
      kind: "step",
      step: "closure-planning",
      elapsedMs: Date.now() - planningStarted,
      observedRequests: null,
      writtenBytes: 0,
    });
    if (!operation.closure)
      await progress({
        kind: "plan",
        closure: { objectCount: closure.neededOids.length, objectSetSha256: digest },
        retainedPackKey,
      });
    if (retainedPackKey || closure.neededOids.length === 0)
      return { status: "retry", reason: "catalog-publication-pending" };
    const selectionStarted = Date.now();
    let rewriteRequests = 0;
    const rewrite = await rewritePackResult(args.env, loaded.snapshot, closure.neededOids, {
      limiter: args.limiter,
      countSubrequest: (n = 1) => {
        rewriteRequests += n;
        args.countSubrequest("r2:rewrite-gc-pack", n);
      },
    });
    if (rewrite.status !== "ok") {
      if (rewrite.failure.retryable) return yieldOperation(rewrite.failure.reason);
      args.log.error("reachability-gc:rewrite-rejected", { reason: rewrite.failure.reason });
      if (operation.rewriteIntent) {
        await progress({ kind: "discard", reason: "rewrite-rejected" });
        return { status: "retry", reason: "unpublished-output-draining" };
      }
      await progress({ kind: "blocked", reason: rewrite.failure.reason });
      return { status: "blocked", reason: rewrite.failure.reason };
    }
    if (rewrite.addedDeltaBases > 0) {
      await rewrite.stream.cancel();
      await progress({ kind: "blocked", reason: "delta-base-outside-reachability-closure" });
      return { status: "blocked", reason: "delta-base-outside-reachability-closure" };
    }
    await progress({
      kind: "step",
      step: "rewrite-selection",
      elapsedMs: Date.now() - selectionStarted,
      observedRequests: rewriteRequests,
      writtenBytes: 0,
    });
    rewriteRequests = 0;
    const uploadStarted = Date.now();
    await stagePackToR2({
      ...args,
      request: new Request("https://maintenance.internal/gc", {
        headers: { "Content-Length": String(rewrite.packBytes) },
      }),
      packStream: rewrite.stream,
      packKey: operation.inputPackKey,
      bytesConsumed: 0,
      durableOwner: {
        beforeComplete: async (identity) => progress({ kind: "rewrite-intent", identity }),
      },
    });
    await progress({
      kind: "step",
      step: "rewrite-upload",
      elapsedMs: Date.now() - uploadStarted,
      observedRequests: rewriteRequests + 1,
      writtenBytes: rewrite.packBytes,
    });
    args.countSubrequest("r2:gc-input-head");
    const head = await args.limiter.run("r2:gc-input-head", () =>
      args.env.REPO_BUCKET.head(operation.inputPackKey)
    );
    if (!head || head.size !== operation.rewriteIntent?.packBytes)
      throw new Error("GC rewrite upload unresolved");
    await progress({
      kind: "rewrite-complete",
      identity: operation.rewriteIntent,
      etag: head.etag,
    });
    await afterRewrite();
    return { status: "retry", reason: "native-indexing-pending" };
  }
  if (operation.phase === "index") {
    try {
      let result = await readNativeReceipt(args, operation);
      if (!result) {
        args.countSubrequest("do:gc-native-process");
        const processed = await args.limiter.run<GcNativeResult>(
          "do:gc-native-process",
          async () => await stub.runGcNative(operation.id, claimId)
        );
        result = await readNativeReceipt(args, operation);
        if (!result && processed.status === "invalid") {
          await progress({ kind: "discard", reason: "native-rejected" });
          return { status: "retry", reason: "unpublished-output-draining" };
        }
      }
      if (!result) return { status: "retry", reason: "native-outcome-unresolved" };
      const validationStarted = Date.now();
      await validateNativeArtifacts(args, operation, result);
      await progress({
        kind: "step",
        step: "output-validation",
        elapsedMs: Date.now() - validationStarted,
        observedRequests: 3,
        writtenBytes: 0,
      });
      await progress({ kind: "native-complete", result });
      return { status: "retry", reason: "catalog-publication-pending" };
    } catch (error) {
      if (!(error instanceof InvalidGcArtifact)) throw error;
      args.log.error("reachability-gc:native-artifact-rejected");
      await progress({ kind: "discard", reason: "native-rejected" });
      return { status: "retry", reason: "unpublished-output-draining" };
    }
  }
  if (operation.phase === "publish") {
    args.countSubrequest("do:gc-catalog-commit");
    const commit = await args.limiter.run<CommitReachabilityGcResult>(
      "do:gc-catalog-commit",
      async () => await stub.commitGcOperation(operation.id, claimId)
    );
    if (commit.status !== "committed") {
      if (
        ["refs-changed", "pins-changed", "packset-changed", "source-changed"].includes(
          commit.reason
        )
      )
        await progress({ kind: "discard", reason: "source-changed" });
      return { status: "retry", reason: commit.reason };
    }
    return { status: "retry", reason: "generation-publication-pending" };
  }
  if (operation.phase === "reclaim") {
    const publicationStarted = Date.now();
    if (!(await publishPendingGeneration({ ...args, stub })))
      return yieldOperation("generation-publication-pending");
    if (!operation.stepMeasurements?.["generation-publication"])
      await progress({
        kind: "step",
        step: "generation-publication",
        elapsedMs: Date.now() - publicationStarted,
        observedRequests: null,
        writtenBytes: null,
      });
    const commit = operation.commit!;
    const keys = commit.supersededPackKeys.flatMap((key) => [
      key,
      packIndexKey(key),
      packRefsKey(key),
    ]);
    let remaining = 0;
    for (const key of keys) {
      args.countSubrequest("r2:gc-reclamation-head");
      if (await args.limiter.run("r2:gc-reclamation-head", () => args.env.REPO_BUCKET.head(key)))
        remaining++;
    }
    args.countSubrequest("do:gc-reclamation-rows");
    const remainingRows = await args.limiter.run(
      "do:gc-reclamation-rows",
      async () => await stub.listSupersededGcPacks()
    );
    if (
      remaining > 0 ||
      remainingRows.some((row) => commit.supersededPackKeys.includes(row.packKey))
    ) {
      await scheduleSupersededPackCleanup({
        ...args,
        packKeys: commit.supersededPackKeys,
        supersededAtGeneration: commit.packCatalogVersion,
      });
      return yieldOperation("reader-safe-reclamation-pending");
    }
    // The input and receipt are never published or exposed to readers. Their
    // writers have completed before publication; immutable bridge writes do
    // not delete output, even after a late failed response.
    args.countSubrequest("r2:gc-delete-staging");
    await args.limiter.run("r2:gc-delete-staging", () =>
      args.env.REPO_BUCKET.delete([operation.inputPackKey, operation.outputResultKey])
    );
    for (const key of [operation.inputPackKey, operation.outputResultKey]) {
      args.countSubrequest("r2:gc-staging-absence");
      if (await args.limiter.run("r2:gc-staging-absence", () => args.env.REPO_BUCKET.head(key)))
        return yieldOperation("staging-cleanup-pending");
    }
    await progress({ kind: "reclaimed" });
    return {
      status: "completed",
      reachableObjects: operation.closure!.objectCount,
      sourcePacks: operation.snapshot!.sourcePacks.length,
      scheduledArtifacts: keys.length,
      packCatalogVersion: commit.packCatalogVersion,
    };
  }
  if (operation.phase === "discard") {
    args.countSubrequest("do:gc-discard-keys");
    const keys = await args.limiter.run(
      "do:gc-discard-keys",
      async () => await stub.gcDiscardKeys(operation.id, claimId)
    );
    if (!keys) return yieldOperation("discard-authority-unresolved");
    args.countSubrequest("r2:gc-discard");
    await args.limiter.run("r2:gc-discard", () => args.env.REPO_BUCKET.delete(keys));
    for (const key of keys) {
      args.countSubrequest("r2:gc-discard-head");
      if (await args.limiter.run("r2:gc-discard-head", () => args.env.REPO_BUCKET.head(key)))
        return yieldOperation("discard-cleanup-pending");
    }
    await progress({ kind: "discarded" });
    return { status: "blocked", reason: operation.blockedReason ?? "gc-blocked" };
  }
  return { status: "retry", reason: "gc-stage-pending" };
}

export const __test = {
  setBeforeSnapshotRegistrationObserver(
    observer:
      | ((begin: Extract<BeginReachabilityGcResult, { ok: true }>) => Promise<void>)
      | undefined
  ): void {
    beforeSnapshotRegistrationObserver = observer;
  },
};
