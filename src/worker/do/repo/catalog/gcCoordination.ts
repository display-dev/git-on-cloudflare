import type { PackCatalogRow } from "../db/schema";
import type { Ref, RepoLease } from "../repoState";
import type { ReceiveCommand } from "@/worker/git/operations/validation";
import type { PackRefSnapshotEntry } from "@/worker/git/pack/refIndex";
import { GC_OPERATION_KEY, type GcOperation } from "@/worker/git/maintenance/gcOperation";
import { getDb, listActivePackCatalog } from "../db";
import { getOidHexAt, loadIdxView } from "@/worker/git/object-store";
import { loadPackRefView } from "@/worker/git/pack/refIndex";
import { computeNeededFromPackRefs } from "@/worker/git/operations/fetch/refClosure";
import { DEFAULT_SUBREQUEST_BUDGET, SubrequestLimiter } from "@/worker/git/operations/limits";
import { readPackHeaderEx } from "@/worker/git/pack/packMeta";
import { createLogger, isZeroOid } from "@/worker/common";

/** Input ownership survives expired processing/maintenance leases. It is not
 * a foreground writer lock. Compaction must not replace these source rows. */
export function gcOwnsSource(operation: GcOperation | undefined): boolean {
  return Boolean(
    operation?.snapshot && !operation.commit && !["complete", "blocked"].includes(operation.phase)
  );
}

/** Compaction resumes after GC has proved reclamation, so it cannot replace
 * the published GC output while its deletion/reader evidence is unresolved. */
export function gcOwnsMaintenance(operation: GcOperation | undefined): boolean {
  return Boolean(operation?.snapshot && !["complete", "blocked"].includes(operation.phase));
}

export type GcReceiveProtection = {
  operationId: string;
  refsVersion: number;
  reason?: "new-source-reachability" | "metadata-unavailable";
};

/** Compare new roots with the already-protected current closure, using only
 * indexes and logical-reference sidecars. Normal checkpoints add no source
 * protection. Resurrection conservatively pins the complete source pack set,
 * including encoding bases: this is explicit retained garbage, not reclamation.
 * Missing metadata has the same safe fallback; it cannot admit unsafe deletion.
 */
export async function prepareGcReceiveProtection(args: {
  ctx: DurableObjectState;
  env: Env;
  refs: Ref[];
  refsVersion: number;
  commands: ReceiveCommand[];
  stagedPack?: Pick<PackCatalogRow, "packKey" | "packBytes">;
}): Promise<GcReceiveProtection | undefined> {
  const operation = await args.ctx.storage.get<GcOperation>(GC_OPERATION_KEY);
  if (!gcOwnsSource(operation) || !operation?.coordination) return undefined;
  const proof: GcReceiveProtection = {
    operationId: operation.id,
    refsVersion: args.refsVersion,
  };
  const startedAt = Date.now();
  const log = createLogger(args.env.LOG_LEVEL, { service: "GcReceiveProtection" });
  let requestReservations = 0;
  const reserve = () => {
    requestReservations++;
    // Leave room for the surrounding receive finalization. Reservations are
    // an upper bound (cached metadata makes no request), not billed requests.
    if (requestReservations > DEFAULT_SUBREQUEST_BUDGET - 16)
      throw new Error("GC receive metadata guard exhausted");
  };
  const finish = (reason?: GcReceiveProtection["reason"]): GcReceiveProtection => {
    log.info("gc:receive-protection", {
      elapsedMs: Date.now() - startedAt,
      requestReservations,
      retention: reason ?? "already-protected",
    });
    return { ...proof, reason };
  };
  if (operation.coordination.retainedSourcePackKeys.length) return proof;
  const wants = args.commands.map((command) => command.newOid).filter((oid) => !isZeroOid(oid));
  if (!wants.length && !args.stagedPack) return finish();
  const limiter = new SubrequestLimiter(4);
  const sourceKeys = new Set(operation.snapshot!.sourcePacks.map((row) => row.packKey));
  const rows: Array<Pick<PackCatalogRow, "packKey" | "packBytes">> = await listActivePackCatalog(
    getDb(args.ctx.storage)
  );
  if (args.stagedPack && !rows.some((row) => row.packKey === args.stagedPack!.packKey))
    rows.unshift(args.stagedPack);
  const packs: PackRefSnapshotEntry[] = [];
  const sourceOids = new Set<string>();
  try {
    for (const row of rows) {
      reserve();
      const idx = await limiter.run("r2:gc-receive-index", () =>
        loadIdxView(args.env, row.packKey, undefined, row.packBytes)
      );
      if (!idx) return finish("metadata-unavailable");
      reserve();
      const refs = await limiter.run("r2:gc-receive-references", () =>
        loadPackRefView(args.env, row.packKey, idx)
      );
      if (refs.type !== "Ready") return finish("metadata-unavailable");
      packs.push({ packKey: row.packKey, packBytes: row.packBytes, idx, refs: refs.view });
      if (row.packKey === args.stagedPack?.packKey) {
        const incoming = new Set(Array.from({ length: idx.count }, (_, i) => getOidHexAt(idx, i)));
        for (let i = 0; i < idx.count; i++) {
          const header = await readPackHeaderEx(args.env, row.packKey, idx.offsets[i]!, {
            limiter,
            countSubrequest: reserve,
          });
          if (!header) return finish("metadata-unavailable");
          // OFS bases are inside the same retained new pack. Thin REF bases
          // may be outside it and need the same protection as logical roots.
          if (header.baseOid && !incoming.has(header.baseOid)) wants.push(header.baseOid);
        }
      }
      if (sourceKeys.has(row.packKey))
        for (let i = 0; i < idx.count; i++) sourceOids.add(getOidHexAt(idx, i));
    }
    const missing = await computeNeededFromPackRefs({
      repoId: operation.repositoryId,
      logLevel: args.env.LOG_LEVEL,
      packs,
      wants,
      haves: args.refs.map((ref) => ref.oid),
    });
    if (missing.type !== "Ready") return finish("metadata-unavailable");
    if (missing.neededOids.some((oid) => sourceOids.has(oid)))
      return finish("new-source-reachability");
    return finish();
  } catch {
    return finish("metadata-unavailable");
  }
}

/** Persist protection with the receive WAL, before any ref acknowledgement.
 * The receive lease prevents publication while metadata is being inspected. */
export async function retainGcReceiveProtection(
  transaction: DurableObjectTransaction,
  token: string,
  proof: GcReceiveProtection | undefined
): Promise<void> {
  if (!proof) return;
  const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
  const lease = await transaction.get<RepoLease>("receiveLease");
  if (
    !operation?.coordination ||
    operation.id !== proof.operationId ||
    operation.commit ||
    lease?.token !== token ||
    ((await transaction.get<number>("refsVersion")) ?? 0) !== proof.refsVersion
  )
    throw new Error("GC receive protection changed before its durable intent");
  if (proof.reason) {
    operation.coordination.retainedSourcePackKeys = operation.snapshot!.sourcePacks.map(
      (p) => p.packKey
    );
    operation.coordination.conservativeRetentionReason = proof.reason;
    await transaction.put(GC_OPERATION_KEY, operation);
  }
}

/** The existing receive ref CAS also advances the GC's accounted versions.
 * An unaccounted catalog/ref mutation still fails GC publication. Replay is
 * idempotent and never counts the same acknowledged write twice. */
export async function advanceGcReceiveVersions(
  transaction: DurableObjectTransaction,
  expectedRefsVersion: number,
  nextRefsVersion: number,
  nextPacksetVersion: number | undefined,
  expectedSnapshotPinVersion: number,
  nextSnapshotPinVersion: number
): Promise<void> {
  const operation = await transaction.get<GcOperation>(GC_OPERATION_KEY);
  if (!gcOwnsSource(operation) || !operation?.coordination) return;
  const coordination = operation.coordination;
  if (coordination.refsVersion === nextRefsVersion) return;
  if (
    coordination.refsVersion !== expectedRefsVersion ||
    (nextPacksetVersion !== undefined && coordination.packsetVersion + 1 !== nextPacksetVersion)
  )
    throw new Error("GC receive versions diverged from the protected source");
  coordination.refsVersion = nextRefsVersion;
  if (nextPacksetVersion !== undefined) coordination.packsetVersion = nextPacksetVersion;
  if (coordination.snapshotPinVersion === expectedSnapshotPinVersion) {
    coordination.snapshotPinVersion = nextSnapshotPinVersion;
  }
  coordination.acceptedReceives++;
  await transaction.put(GC_OPERATION_KEY, operation);
}
