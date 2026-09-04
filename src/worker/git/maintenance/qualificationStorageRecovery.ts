import { z } from "zod";

import { constantTimeEquals } from "@/worker/auth/pat";
import { loadSessionConfig } from "@/worker/auth/session";
import { bytesToHex, type Logger } from "@/worker/common";
import type { RepoDurableObject } from "@/worker/do/repo/repoDO";
import { EXPIRED_WRITER_DRAIN_MS } from "@/worker/do/repo/repositoryLifecycle";
import { readPublishedRepositoryGenerationState } from "@/worker/git/generation/publish";
import { isValidNativeReceiveOperationId } from "@/worker/git/nativeReceive/types";
import type { Limiter } from "@/worker/git/operations/limits";
import { isValidRefName } from "@/worker/git/operations/validation";
import { doPrefix, packIndexKey, packRefsKey } from "@/worker/keys";

type AuthorityOwner = {
  operationId: string;
  fingerprint: string;
  ref?: { object: R2Object; name: string; oid: string };
  receipt?: { object: R2Object; name: string; oid: string };
};

type AuthorityIdentity = {
  operationId: string;
  fingerprint: string;
  role: "ref-0" | "receipt";
};

type CompactionOrphanIdentity = {
  leaseToken: string;
};

const MAX_AUTHORITY_RECORDS_PER_RECOVERY = 800;
const MAX_DELETED_OBJECTS_PER_RECOVERY = 100;
const RECOVERY_JOURNAL_MAX_BYTES = 256 * 1024;
const RECOVERY_SUBREQUEST_OVERHEAD = 16;

export function qualificationStorageRecoverySubrequestReservation(
  expectedObjectCount: number
): number {
  return (
    Math.min(expectedObjectCount, MAX_AUTHORITY_RECORDS_PER_RECOVERY) + RECOVERY_SUBREQUEST_OVERHEAD
  );
}

const recoveryObjectProofSchema = z
  .object({
    key: z.string().min(1),
    etag: z.string().min(1),
    size: z.number().int().nonnegative(),
  })
  .strict();

const recoveryJournalPayloadSchema = z
  .object({
    schemaVersion: z.literal(1),
    kind: z.literal("qualification-storage-recovery-plan"),
    expectedRefStateDigest: z.string().regex(/^[a-f0-9]{64}$/),
    packsetVersion: z.number().int().nonnegative(),
    unchangedInventoryDigest: z.string().regex(/^[a-f0-9]{64}$/),
    ownerGroups: z
      .array(
        z
          .object({
            ownerKey: z.string().min(1),
            objects: z
              .array(recoveryObjectProofSchema)
              .min(1)
              .max(MAX_DELETED_OBJECTS_PER_RECOVERY),
          })
          .strict()
      )
      .min(1)
      .max(MAX_DELETED_OBJECTS_PER_RECOVERY),
  })
  .strict();

const recoveryJournalSchema = recoveryJournalPayloadSchema
  .extend({ signature: z.string().regex(/^[a-f0-9]{64}$/) })
  .strict();

type RecoveryJournal = z.infer<typeof recoveryJournalSchema>;
type RecoveryJournalPayload = z.infer<typeof recoveryJournalPayloadSchema>;
type RecoveryObjectProof = z.infer<typeof recoveryObjectProofSchema>;

function authorityIdentity(relative: string): AuthorityIdentity | null {
  const matched = relative.match(
    /^native-receive\/authority\/(.+)-([a-f0-9]{64})\/(ref-0|receipt)\.json$/
  );
  if (!matched || !isValidNativeReceiveOperationId(matched[1]!)) return null;
  return {
    operationId: matched[1]!,
    fingerprint: matched[2]!,
    role: matched[3]! as AuthorityIdentity["role"],
  };
}

function compactionOrphanIdentity(relative: string): CompactionOrphanIdentity | null {
  const matched = relative.match(
    /^objects\/pack\/pack-cmp-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.pack$/
  );
  return matched ? { leaseToken: matched[1]! } : null;
}

function isCatalogMetadataCache(relative: string): boolean {
  return /^catalog-metadata\/[0-9a-f]{64}\.bin$/.test(relative);
}

function objectProof(object: R2Object): RecoveryObjectProof {
  return { key: object.key, etag: object.etag, size: object.size };
}

function objectMatchesProof(object: R2Object, proof: RecoveryObjectProof): boolean {
  return object.key === proof.key && object.etag === proof.etag && object.size === proof.size;
}

function isProtectedRecoveryObject(args: {
  prefix: string;
  journalKey: string;
  protectedKeys: Set<string>;
  key: string;
}): boolean {
  return (
    args.protectedKeys.has(args.key) ||
    args.key === args.journalKey ||
    args.key === `${args.prefix}/generation-index.json` ||
    new RegExp(`^${args.prefix}/generations/[0-9]+\\.json$`).test(args.key)
  );
}

async function inventoryDigest(objects: R2Object[]): Promise<string> {
  const canonical = objects
    .map(objectProof)
    .sort((left, right) => left.key.localeCompare(right.key));
  const bytes = new TextEncoder().encode(JSON.stringify(canonical));
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)));
}

function recoveryJournalPayload(journal: RecoveryJournal): RecoveryJournalPayload {
  return {
    schemaVersion: journal.schemaVersion,
    kind: journal.kind,
    expectedRefStateDigest: journal.expectedRefStateDigest,
    packsetVersion: journal.packsetVersion,
    unchangedInventoryDigest: journal.unchangedInventoryDigest,
    ownerGroups: journal.ownerGroups,
  };
}

async function signRecoveryJournal(
  serviceSigningSecret: string,
  payload: RecoveryJournalPayload
): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(serviceSigningSecret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    new TextEncoder().encode(`qualification-storage-recovery\0${JSON.stringify(payload)}`)
  );
  return bytesToHex(new Uint8Array(signature));
}

async function executeRecoveryJournal(args: {
  env: Env;
  log: Logger;
  limiter: Limiter;
  countSubrequest(op: string, count?: number): void;
  prefix: string;
  journalKey: string;
  protectedKeys: Set<string>;
  journal: RecoveryJournal;
  currentInventory: R2Object[];
}) {
  const planned = new Map<string, RecoveryObjectProof>();
  let plannedObjectCount = 0;
  let plannedObjectBytes = 0;
  for (const group of args.journal.ownerGroups) {
    for (const proof of group.objects) {
      const prefixMatches = proof.key.startsWith(`${args.prefix}/`);
      const relative = prefixMatches ? proof.key.slice(args.prefix.length + 1) : "";
      const recognizedRecoveryObject =
        prefixMatches &&
        (authorityIdentity(relative) !== null ||
          compactionOrphanIdentity(relative) !== null ||
          isCatalogMetadataCache(relative));
      const protectedObject = isProtectedRecoveryObject({
        prefix: args.prefix,
        journalKey: args.journalKey,
        protectedKeys: args.protectedKeys,
        key: proof.key,
      });
      if (
        !prefixMatches ||
        !recognizedRecoveryObject ||
        planned.has(proof.key) ||
        protectedObject
      ) {
        args.log.warn("qualification-storage-recovery:journal-key-refused", {
          prefixMatches,
          recognizedRecoveryObject,
          duplicate: planned.has(proof.key),
          protectedObject,
        });
        return { status: "conflict", reason: "unrecognized_authority" } as const;
      }
      planned.set(proof.key, proof);
      plannedObjectCount++;
      plannedObjectBytes += proof.size;
    }
  }
  if (plannedObjectCount > MAX_DELETED_OBJECTS_PER_RECOVERY) {
    return { status: "conflict", reason: "recovery_budget" } as const;
  }

  const selectedPresent: R2Object[] = [];
  const unchanged: R2Object[] = [];
  for (const object of args.currentInventory) {
    const proof = planned.get(object.key);
    if (!proof) {
      unchanged.push(object);
      continue;
    }
    if (!objectMatchesProof(object, proof)) {
      return { status: "conflict", reason: "orphan_changed" } as const;
    }
    selectedPresent.push(object);
  }
  if ((await inventoryDigest(unchanged)) !== args.journal.unchangedInventoryDigest) {
    return { status: "conflict", reason: "storage_state_mismatch" } as const;
  }

  // The fixed-key journal is written before deletion. It makes a partially
  // applied or acknowledgement-lost R2 multi-delete safe to reconcile without
  // accepting keys from the caller or relaxing complete-owner validation.
  if (selectedPresent.length) {
    try {
      args.countSubrequest("r2:qualification-recovery-delete");
      await args.limiter.run("r2:qualification-recovery-delete", () =>
        args.env.REPO_BUCKET.delete(selectedPresent.map((object) => object.key))
      );
    } catch {
      // Reconcile through the strongly consistent inventory below.
      args.log.warn("qualification-storage-recovery:delete-unacknowledged", {
        keyCount: selectedPresent.length,
      });
    }
  }
  args.countSubrequest("r2:qualification-recovery-list-after-delete");
  const afterDelete = await args.limiter.run("r2:qualification-recovery-list-after-delete", () =>
    args.env.REPO_BUCKET.list({ prefix: `${args.prefix}/`, limit: 1000 })
  );
  if (afterDelete.truncated) {
    return { status: "inconclusive", reason: "deletion_not_proven" } as const;
  }
  const afterDeleteWithoutJournal = afterDelete.objects.filter(
    (object) => object.key !== args.journalKey
  );
  const unchangedAfterDelete = afterDeleteWithoutJournal.filter(
    (object) => !planned.has(object.key)
  );
  if ((await inventoryDigest(unchangedAfterDelete)) !== args.journal.unchangedInventoryDigest) {
    return { status: "conflict", reason: "storage_state_mismatch" } as const;
  }
  if (afterDeleteWithoutJournal.some((object) => planned.has(object.key))) {
    return { status: "inconclusive", reason: "deletion_not_proven" } as const;
  }

  try {
    args.countSubrequest("r2:qualification-recovery-delete-journal");
    await args.limiter.run("r2:qualification-recovery-delete-journal", () =>
      args.env.REPO_BUCKET.delete(args.journalKey)
    );
  } catch {
    // Reconcile journal deletion through the final strongly consistent list.
    args.log.warn("qualification-storage-recovery:journal-delete-unacknowledged", {
      journalKey: args.journalKey,
    });
  }
  args.countSubrequest("r2:qualification-recovery-list-final");
  const final = await args.limiter.run("r2:qualification-recovery-list-final", () =>
    args.env.REPO_BUCKET.list({ prefix: `${args.prefix}/`, limit: 1000 })
  );
  if (final.truncated || final.objects.some((object) => object.key === args.journalKey)) {
    return { status: "inconclusive", reason: "deletion_not_proven" } as const;
  }
  if ((await inventoryDigest(final.objects)) !== args.journal.unchangedInventoryDigest) {
    return { status: "conflict", reason: "storage_state_mismatch" } as const;
  }
  return {
    status: "recovered",
    deletedObjectCount: plannedObjectCount,
    deletedObjectBytes: plannedObjectBytes,
    remainingObjectCount: final.objects.length,
  } as const;
}

/** Synthetic qualification only. Never accepts caller-selected object keys. */
export async function recoverQualificationStorage(args: {
  env: Env;
  log: Logger;
  stub: DurableObjectStub<RepoDurableObject>;
  limiter: Limiter;
  countSubrequest(op: string, count?: number): void;
  expectedRefStateDigest: string;
  expectedObjectCount: number;
}) {
  const signingConfig = loadSessionConfig(args.env);
  if (!signingConfig.ok) {
    return { status: "conflict", reason: "recovery_signing_unavailable" } as const;
  }
  args.countSubrequest("do:qualification-storage-recovery-begin");
  const held = await args.limiter.run(
    "do:qualification-storage-recovery-begin",
    async () => await args.stub.beginQualificationStorageRecovery(args.expectedRefStateDigest)
  );
  if (held.status !== "held") {
    return { status: "conflict", reason: "repository_active_or_changed" } as const;
  }
  const prefix = doPrefix(args.stub.id.toString());
  const journalKey = `${prefix}/qualification/storage-recovery-plan.json`;
  try {
    const published = await readPublishedRepositoryGenerationState({
      env: args.env,
      doId: args.stub.id.toString(),
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
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
    args.countSubrequest("r2:qualification-recovery-list");
    const page = await args.limiter.run("r2:qualification-recovery-list", () =>
      args.env.REPO_BUCKET.list({ prefix: `${prefix}/`, limit: 1000 })
    );
    if (page.truncated || page.objects.length !== args.expectedObjectCount) {
      return { status: "conflict", reason: "storage_state_mismatch" } as const;
    }
    const journalObject = page.objects.find((object) => object.key === journalKey);
    const unprotected: Array<{ object: R2Object; relative: string }> = [];
    // Admission already waits for each writer lease's expiry PLUS its drain
    // period. Do not charge the full writer lease again from object upload:
    // that timestamp can be near the end of an already-expired invocation.
    const oldestAllowed = Date.now() - EXPIRED_WRITER_DRAIN_MS;
    for (const object of page.objects) {
      if (isProtectedRecoveryObject({ prefix, journalKey, protectedKeys, key: object.key })) {
        continue;
      }
      if (object.uploaded.getTime() > oldestAllowed) {
        return { status: "conflict", reason: "orphan_writer_drain" } as const;
      }
      unprotected.push({ object, relative: object.key.slice(prefix.length + 1) });
    }

    if (journalObject) {
      if (journalObject.size > RECOVERY_JOURNAL_MAX_BYTES) {
        args.log.warn("qualification-storage-recovery:journal-unreadable", {
          oversize: true,
          etagChanged: false,
          jsonValid: null,
        });
        return { status: "conflict", reason: "unrecognized_authority" } as const;
      }
      args.countSubrequest("r2:qualification-recovery-get-journal");
      const body = await args.limiter.run("r2:qualification-recovery-get-journal", () =>
        args.env.REPO_BUCKET.get(journalKey)
      );
      if (!body || body.etag !== journalObject.etag) {
        args.log.warn("qualification-storage-recovery:journal-unreadable", {
          oversize: false,
          etagChanged: true,
          jsonValid: null,
        });
        return { status: "conflict", reason: "orphan_changed" } as const;
      }
      let journalValue: unknown;
      try {
        journalValue = await body.json<unknown>();
      } catch {
        args.log.warn("qualification-storage-recovery:journal-unreadable", {
          oversize: false,
          etagChanged: false,
          jsonValid: false,
        });
        return { status: "conflict", reason: "unrecognized_authority" } as const;
      }
      const parsed = recoveryJournalSchema.safeParse(journalValue);
      const expectedSignature = parsed.success
        ? await signRecoveryJournal(signingConfig.secret, recoveryJournalPayload(parsed.data))
        : null;
      const signatureValid =
        parsed.success &&
        expectedSignature !== null &&
        (await constantTimeEquals(expectedSignature, parsed.data.signature));
      const refDigestMatches =
        parsed.success && parsed.data.expectedRefStateDigest === args.expectedRefStateDigest;
      const packsetVersionMatches =
        parsed.success && parsed.data.packsetVersion === held.packsetVersion;
      if (!parsed.success || !signatureValid || !refDigestMatches || !packsetVersionMatches) {
        args.log.warn("qualification-storage-recovery:journal-rejected", {
          schemaValid: parsed.success,
          signatureValid,
          refDigestMatches,
          packsetVersionMatches,
          journalPacksetVersion: parsed.success ? parsed.data.packsetVersion : null,
          heldPacksetVersion: held.packsetVersion,
        });
        return { status: "conflict", reason: "unrecognized_authority" } as const;
      }
      if (Date.now() + 60_000 >= held.lease.expiresAt) {
        return { status: "conflict", reason: "recovery_budget" } as const;
      }
      return await executeRecoveryJournal({
        env: args.env,
        log: args.log,
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
        prefix,
        journalKey,
        protectedKeys,
        journal: parsed.data,
        currentInventory: page.objects.filter((object) => object.key !== journalKey),
      });
    }

    const authorityRecords: Array<{ object: R2Object; identity: AuthorityIdentity }> = [];
    for (const { object, relative } of unprotected) {
      const identity = authorityIdentity(relative);
      if (identity) authorityRecords.push({ object, identity });
    }
    if (authorityRecords.length > MAX_AUTHORITY_RECORDS_PER_RECOVERY) {
      return { status: "conflict", reason: "recovery_budget" } as const;
    }

    const owners = new Map<string, AuthorityOwner>();
    const authorityProofs = await Promise.all(
      authorityRecords.map(async ({ object, identity }) => {
        if (object.size > 4096) return { status: "unrecognized" } as const;
        args.countSubrequest("r2:qualification-recovery-authority");
        const body = await args.limiter.run("r2:qualification-recovery-authority", () =>
          args.env.REPO_BUCKET.get(object.key)
        );
        if (!body || body.etag !== object.etag) return { status: "changed" } as const;
        let value: unknown;
        try {
          value = await body.json<unknown>();
        } catch {
          return { status: "unrecognized" } as const;
        }
        return {
          status: "read",
          object,
          identity,
          value,
        } as const;
      })
    );
    for (const proofBody of authorityProofs) {
      if (proofBody.status === "changed") {
        return { status: "conflict", reason: "orphan_changed" } as const;
      }
      if (proofBody.status === "unrecognized") {
        return { status: "conflict", reason: "unrecognized_authority" } as const;
      }
      const { object, identity } = proofBody;
      if (
        !proofBody.value ||
        typeof proofBody.value !== "object" ||
        Array.isArray(proofBody.value)
      ) {
        return { status: "conflict", reason: "unrecognized_authority" } as const;
      }
      const value = proofBody.value as Record<string, unknown>;
      const ref = value.kind === "authoritative-ref" ? value.name : value.refName;
      const oid = value.kind === "authoritative-ref" ? value.oid : value.newOid;
      const recognized =
        value.schemaVersion === 1 &&
        (identity.role === "ref-0"
          ? value.kind === "authoritative-ref"
          : value.kind === "operation-receipt" &&
            value.disposition === "committed" &&
            typeof value.digest === "string" &&
            /^[a-f0-9]{64}$/.test(value.digest));
      if (
        !recognized ||
        typeof ref !== "string" ||
        !isValidRefName(ref) ||
        typeof oid !== "string" ||
        !/^[a-f0-9]{40}$/.test(oid)
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

    const candidatesByOwner = new Map<string, R2Object[]>();
    const inventoryKeys = new Set(page.objects.map((candidate) => candidate.key));
    const addCandidate = (ownerKey: string, object: R2Object) => {
      const owned = candidatesByOwner.get(ownerKey) ?? [];
      owned.push(object);
      candidatesByOwner.set(ownerKey, owned);
    };
    for (const { object, relative } of unprotected) {
      const identity = authorityIdentity(relative);
      if (identity) {
        const ownerKey = `${identity.operationId}\0${identity.fingerprint}`;
        if (!owners.has(ownerKey)) {
          return { status: "conflict", reason: "unrecognized_authority" } as const;
        }
        addCandidate(ownerKey, object);
        continue;
      }
      const compaction = compactionOrphanIdentity(relative);
      if (compaction) {
        // Catalogued and currently published packs were removed from this
        // classification above. A pack-only compaction artifact can remain
        // after an interrupted write or partially applied reclamation. The
        // repository-wide recovery lease proves the expired writer cannot
        // still complete it. Index keys sort before pack keys and are already
        // rejected as unknown artifacts; a reference sibling sorts after the
        // pack and therefore needs this explicit fail-closed check.
        if (inventoryKeys.has(packRefsKey(object.key))) {
          return { status: "conflict", reason: "unrecognized_orphan" } as const;
        }
        addCandidate(`compaction\0${compaction.leaseToken}`, object);
        continue;
      }
      if (isCatalogMetadataCache(relative)) {
        addCandidate(`catalog-metadata\0${object.key}`, object);
        continue;
      }
      // Legacy authority records are not authenticated against a server-owned
      // secret. They may therefore prove only their own paired-record shape;
      // they never authorize deletion of pack, index, reference, or other R2
      // artifacts, even when an artifact key repeats their operation identity.
      return { status: "conflict", reason: "unrecognized_orphan" } as const;
    }

    const selectedGroups: Array<{ ownerKey: string; objects: R2Object[] }> = [];
    let selectedCount = 0;
    for (const ownerKey of [...candidatesByOwner.keys()].sort()) {
      const group = candidatesByOwner.get(ownerKey)!;
      if (selectedCount + group.length > MAX_DELETED_OBJECTS_PER_RECOVERY) {
        if (selectedCount === 0) {
          return { status: "conflict", reason: "recovery_budget" } as const;
        }
        break;
      }
      selectedGroups.push({ ownerKey, objects: group });
      selectedCount += group.length;
    }
    if (selectedGroups.length === 0) {
      return {
        status: "recovered",
        deletedObjectCount: 0,
        deletedObjectBytes: 0,
        remainingObjectCount: page.objects.length,
      } as const;
    }
    if (Date.now() + 60_000 >= held.lease.expiresAt) {
      return { status: "conflict", reason: "recovery_budget" } as const;
    }

    const selectedKeys = new Set(
      selectedGroups.flatMap((group) => group.objects.map((object) => object.key))
    );
    const journalPayload: RecoveryJournalPayload = {
      schemaVersion: 1,
      kind: "qualification-storage-recovery-plan",
      expectedRefStateDigest: args.expectedRefStateDigest,
      packsetVersion: held.packsetVersion,
      unchangedInventoryDigest: await inventoryDigest(
        page.objects.filter((object) => !selectedKeys.has(object.key))
      ),
      ownerGroups: selectedGroups.map((group) => ({
        ownerKey: group.ownerKey,
        objects: group.objects.map(objectProof),
      })),
    };
    const journal: RecoveryJournal = {
      ...journalPayload,
      signature: await signRecoveryJournal(signingConfig.secret, journalPayload),
    };
    const journalBytes = new TextEncoder().encode(JSON.stringify(journal));
    if (journalBytes.byteLength > RECOVERY_JOURNAL_MAX_BYTES) {
      return { status: "conflict", reason: "recovery_budget" } as const;
    }
    args.countSubrequest("r2:qualification-recovery-put-journal");
    const written = await args.limiter.run("r2:qualification-recovery-put-journal", () =>
      args.env.REPO_BUCKET.put(journalKey, journalBytes, {
        onlyIf: { etagDoesNotMatch: "*" },
        httpMetadata: { contentType: "application/json" },
      })
    );
    if (!written) {
      return { status: "inconclusive", reason: "deletion_not_proven" } as const;
    }
    return await executeRecoveryJournal({
      env: args.env,
      log: args.log,
      limiter: args.limiter,
      countSubrequest: args.countSubrequest,
      prefix,
      journalKey,
      protectedKeys,
      journal,
      currentInventory: page.objects,
    });
  } finally {
    args.countSubrequest("do:qualification-storage-recovery-abort");
    await args.limiter.run(
      "do:qualification-storage-recovery-abort",
      async () => await args.stub.abortCompaction(held.lease.token)
    );
  }
}
