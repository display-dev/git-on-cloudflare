import { asBufferSource, bytesToHex } from "@/worker/common";
import {
  buildIngestionCommit,
  type BuiltIngestionCommit,
  type IngestionFile,
} from "@/worker/git/ingestion/pack";

export type WorkspaceStrategy =
  | "same-repository-ref"
  | "full-copy"
  | "shared-catalog"
  | "copy-on-write"
  | "operation-backed";

export type WorkspaceOperation = {
  id: string;
  actor: string;
  conversationId?: string;
  messageId?: string;
  path: string;
  bytes: Uint8Array | null;
};

export type WorkspaceCheckpoint = {
  commitOid: string;
  treeOid: string;
  packId: string;
  packBytes: number;
  fileCount: number;
};

export type WorkspaceMetrics = {
  strategy: WorkspaceStrategy;
  storageShape:
    | "shared-mutable-repository"
    | "owned-full-copy"
    | "immutable-pack-catalog"
    | "pinned-base-with-local-deltas"
    | "operation-log-with-git-checkpoints";
  independentAuthorization: boolean;
  independentDeletion: boolean;
  initialBytes: number;
  editBytes: number;
  metadataWrites: number;
  basePins: number;
  catalogReferences: number;
  copiedBasePacks: number;
  operationRecords: number;
  localCheckpointPacks: number;
};

export type PromotionResult =
  | { state: "committed"; transferredBytes: number; commitOid: string }
  | { state: "interrupted"; transferredBytes: number };

type StoredOperation = WorkspaceOperation & {
  fingerprint: string;
  revision: number;
  generation: number;
};

type WorkspaceGeneration = {
  repositoryId: string;
  baseCommitOid: string;
  files: Map<string, Uint8Array>;
};

type PackRecord = {
  id: string;
  bytes: number;
  owners: Set<string>;
  pins: Set<string>;
};

type CommitRecord = {
  oid: string;
  parentOid: string | null;
  files: Map<string, Uint8Array>;
  requiredPackIds: Set<string>;
};

type RepositoryRecord = {
  id: string;
  namespace: string;
  name: string;
  packIds: Set<string>;
  ref: string | null;
  commits: Map<string, CommitRecord>;
  reviewHolds: Set<string>;
  deleted: boolean;
};

type WorkspaceRecord = {
  id: string;
  lineageId: string;
  repositoryId: string;
  sourceRepositoryId: string;
  strategy: WorkspaceStrategy;
  baseGeneration: number;
  generations: Map<number, WorkspaceGeneration>;
  baseCommitOid: string;
  basePackIds: Set<string>;
  baseFiles: Map<string, Uint8Array>;
  files: Map<string, Uint8Array>;
  operations: StoredOperation[];
  operationById: Map<string, StoredOperation>;
  revision: number;
  checkpoint: WorkspaceCheckpoint | null;
  checkpointFingerprint: string | null;
  checkpointPackIds: Set<string>;
  deleted: boolean;
  metrics: WorkspaceMetrics;
};

type TokenRecord = {
  id: string;
  repositoryId: string;
  capabilities: Set<"read" | "write">;
  expiresAt: number;
  revoked: boolean;
};

type PromotionRecord = {
  id: string;
  workspaceId: string;
  targetRepositoryId: string;
  expectedRef: string | null;
  checkpoint: WorkspaceCheckpoint;
  commits: Map<string, CommitRecord>;
  requiredPackIds: string[];
  nextPackIndex: number;
  transferredBytes: number;
  transferredPackIds: Set<string>;
};

type BranchRequestRecord = {
  fingerprint: string;
  workspaceId: string;
  sourceWorkspaceId: string;
  lineageId: string;
};

type CloneRecord = {
  tokenId: string;
  repositoryId: string;
};

const encoder = new TextEncoder();

function cloneBytes(bytes: Uint8Array): Uint8Array {
  return bytes.slice();
}

function equalBytes(left: Uint8Array | undefined, right: Uint8Array | undefined): boolean {
  if (left === undefined || right === undefined) return left === right;
  if (left.byteLength !== right.byteLength) return false;
  return left.every((byte, index) => byte === right[index]);
}

function cloneFiles(files: ReadonlyMap<string, Uint8Array>): Map<string, Uint8Array> {
  return new Map(Array.from(files, ([path, bytes]) => [path, cloneBytes(bytes)]));
}

function cloneCommit(commit: CommitRecord): CommitRecord {
  return {
    oid: commit.oid,
    parentOid: commit.parentOid,
    files: cloneFiles(commit.files),
    requiredPackIds: new Set(commit.requiredPackIds),
  };
}

async function sha256(value: string | Uint8Array): Promise<string> {
  const bytes = typeof value === "string" ? encoder.encode(value) : value;
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

async function operationFingerprint(operation: WorkspaceOperation): Promise<string> {
  const bytesDigest = operation.bytes === null ? "delete" : await sha256(operation.bytes);
  return await sha256(
    JSON.stringify({
      actor: operation.actor,
      conversationId: operation.conversationId ?? null,
      messageId: operation.messageId ?? null,
      path: operation.path,
      bytesDigest,
    })
  );
}

function assertLiveRepository(repository: RepositoryRecord | undefined): RepositoryRecord {
  if (!repository || repository.deleted) throw new Error("Repository is unavailable");
  return repository;
}

function assertLiveWorkspace(workspace: WorkspaceRecord | undefined): WorkspaceRecord {
  if (!workspace || workspace.deleted) throw new Error("Workspace is unavailable");
  return workspace;
}

/**
 * Deterministic, process-local proof model for Investigation 7. It deliberately
 * models authority and object ownership separately: labels route humans, opaque
 * repository IDs authorize tokens and identify every binding, event, and pack.
 * It is not wired into the Worker and cannot mutate deployed repositories.
 */
export class WorkspaceProbeModel {
  private nextId = 1;
  private readonly repositories = new Map<string, RepositoryRecord>();
  private readonly workspaces = new Map<string, WorkspaceRecord>();
  private readonly packs = new Map<string, PackRecord>();
  private readonly tokens = new Map<string, TokenRecord>();
  private readonly promotions = new Map<string, PromotionRecord>();
  private readonly branchRequests = new Map<string, BranchRequestRecord>();
  private readonly clones = new Map<string, CloneRecord>();

  constructor(private readonly now: () => number = Date.now) {}

  private id(prefix: string): string {
    const id = `${prefix}_${this.nextId.toString(16).padStart(8, "0")}`;
    this.nextId += 1;
    return id;
  }

  createRepository(args: {
    namespace: string;
    name: string;
    basePackBytes?: number;
    baseFiles?: ReadonlyMap<string, Uint8Array>;
    baseCommitOid?: string;
  }): { repositoryId: string; packId: string | null; files: Map<string, Uint8Array> } {
    const repositoryId = this.id("repo");
    const repository: RepositoryRecord = {
      id: repositoryId,
      namespace: args.namespace,
      name: args.name,
      packIds: new Set(),
      ref: args.baseCommitOid ?? null,
      commits: new Map(),
      reviewHolds: new Set(),
      deleted: false,
    };
    this.repositories.set(repositoryId, repository);
    let packId: string | null = null;
    if (args.basePackBytes !== undefined) {
      if (!args.baseCommitOid) throw new Error("A base pack requires an exact base commit");
      packId = this.id("pack");
      repository.packIds.add(packId);
      this.packs.set(packId, {
        id: packId,
        bytes: args.basePackBytes,
        owners: new Set([repositoryId]),
        pins: new Set(),
      });
      repository.commits.set(args.baseCommitOid, {
        oid: args.baseCommitOid,
        parentOid: null,
        files: cloneFiles(args.baseFiles ?? new Map()),
        requiredPackIds: new Set([packId]),
      });
    }
    return {
      repositoryId,
      packId,
      files: cloneFiles(args.baseFiles ?? new Map()),
    };
  }

  renameRepository(repositoryId: string, namespace: string, name: string): void {
    const repository = assertLiveRepository(this.repositories.get(repositoryId));
    repository.namespace = namespace;
    repository.name = name;
  }

  issueToken(args: {
    repositoryId: string;
    capabilities: ReadonlySet<"read" | "write">;
    expiresAt: number;
    issuerCanAdminister: boolean;
  }): string {
    if (!args.issuerCanAdminister) throw new Error("Token issuer is unauthorized");
    assertLiveRepository(this.repositories.get(args.repositoryId));
    const id = this.id("token");
    this.tokens.set(id, {
      id,
      repositoryId: args.repositoryId,
      capabilities: new Set(args.capabilities),
      expiresAt: args.expiresAt,
      revoked: false,
    });
    return id;
  }

  authorize(tokenId: string, repositoryId: string, capability: "read" | "write"): boolean {
    const token = this.tokens.get(tokenId);
    const repository = this.repositories.get(repositoryId);
    return Boolean(
      token &&
      !token.revoked &&
      token.expiresAt > this.now() &&
      token.capabilities.has(capability) &&
      token.repositoryId === repositoryId &&
      !repository?.deleted
    );
  }

  revokeToken(tokenId: string): void {
    const token = this.tokens.get(tokenId);
    if (token) token.revoked = true;
  }

  beginClone(tokenId: string, repositoryId: string): string {
    if (!this.authorize(tokenId, repositoryId, "read")) throw new Error("Clone is unauthorized");
    const cloneId = this.id("clone");
    this.clones.set(cloneId, { tokenId, repositoryId });
    return cloneId;
  }

  readCloneChunk(cloneId: string): void {
    const clone = this.clones.get(cloneId);
    if (!clone || !this.authorize(clone.tokenId, clone.repositoryId, "read")) {
      throw new Error("Clone authorization expired");
    }
  }

  compareAndSetRef(repositoryId: string, expected: string | null, next: string | null): boolean {
    const repository = assertLiveRepository(this.repositories.get(repositoryId));
    if (repository.ref !== expected) return false;
    if (next !== null && !repository.commits.has(next)) {
      throw new Error("Target commit is unavailable");
    }
    repository.ref = next;
    this.pruneRepositoryLinks(repository);
    this.collectUnreachablePacks();
    return true;
  }

  retainForReview(repositoryId: string, commitOid: string): void {
    const repository = assertLiveRepository(this.repositories.get(repositoryId));
    if (!repository.commits.has(commitOid)) throw new Error("Review commit is unavailable");
    repository.reviewHolds.add(commitOid);
  }

  releaseReview(repositoryId: string, commitOid: string): void {
    const repository = assertLiveRepository(this.repositories.get(repositoryId));
    repository.reviewHolds.delete(commitOid);
    this.pruneRepositoryLinks(repository);
    this.collectUnreachablePacks();
  }

  createWorkspace(args: {
    sourceRepositoryId: string;
    sourceCommitOid: string;
    strategy: WorkspaceStrategy;
  }): { workspaceId: string; repositoryId: string; metrics: WorkspaceMetrics } {
    const source = assertLiveRepository(this.repositories.get(args.sourceRepositoryId));
    const sourceCommit = source.commits.get(args.sourceCommitOid);
    if (!sourceCommit) throw new Error("Exact source commit is unavailable");
    const workspaceId = this.id("workspace");
    const independent = args.strategy !== "same-repository-ref";
    const repositoryId = independent
      ? this.createRepository({ namespace: "workspace", name: workspaceId }).repositoryId
      : source.id;
    const repository = assertLiveRepository(this.repositories.get(repositoryId));
    const sourcePackIds = new Set(sourceCommit.requiredPackIds);
    const basePackIds = new Set<string>();
    let initialBytes = 0;
    let basePins = 0;

    for (const sourcePackId of sourcePackIds) {
      const sourcePack = this.packs.get(sourcePackId);
      if (!sourcePack) throw new Error("Source pack is unavailable");
      if (args.strategy === "full-copy") {
        const copyId = this.id("pack");
        this.packs.set(copyId, {
          id: copyId,
          bytes: sourcePack.bytes,
          owners: new Set([repositoryId]),
          pins: new Set(),
        });
        repository.packIds.add(copyId);
        basePackIds.add(copyId);
        initialBytes += sourcePack.bytes;
      } else if (independent) {
        sourcePack.pins.add(workspaceId);
        repository.packIds.add(sourcePackId);
        basePackIds.add(sourcePackId);
        basePins += 1;
      } else {
        basePackIds.add(sourcePackId);
      }
    }
    if (independent) {
      repository.commits.set(sourceCommit.oid, {
        oid: sourceCommit.oid,
        parentOid: sourceCommit.parentOid,
        files: cloneFiles(sourceCommit.files),
        requiredPackIds: new Set(basePackIds),
      });
      repository.ref = sourceCommit.oid;
    }

    const metrics: WorkspaceMetrics = {
      strategy: args.strategy,
      storageShape:
        args.strategy === "same-repository-ref"
          ? "shared-mutable-repository"
          : args.strategy === "full-copy"
            ? "owned-full-copy"
            : args.strategy === "shared-catalog"
              ? "immutable-pack-catalog"
              : args.strategy === "copy-on-write"
                ? "pinned-base-with-local-deltas"
                : "operation-log-with-git-checkpoints",
      independentAuthorization: independent,
      independentDeletion: independent,
      initialBytes,
      editBytes: 0,
      metadataWrites: 1,
      basePins,
      catalogReferences: args.strategy === "shared-catalog" ? basePackIds.size : 0,
      copiedBasePacks: args.strategy === "full-copy" ? basePackIds.size : 0,
      operationRecords: 0,
      localCheckpointPacks: 0,
    };
    this.workspaces.set(workspaceId, {
      id: workspaceId,
      lineageId: workspaceId,
      repositoryId,
      sourceRepositoryId: source.id,
      strategy: args.strategy,
      baseGeneration: 1,
      generations: new Map([
        [
          1,
          {
            repositoryId,
            baseCommitOid: sourceCommit.oid,
            files: cloneFiles(sourceCommit.files),
          },
        ],
      ]),
      baseCommitOid: args.sourceCommitOid,
      basePackIds,
      baseFiles: cloneFiles(sourceCommit.files),
      files: cloneFiles(sourceCommit.files),
      operations: [],
      operationById: new Map(),
      revision: 0,
      checkpoint: null,
      checkpointFingerprint: null,
      checkpointPackIds: new Set(),
      deleted: false,
      metrics,
    });
    return { workspaceId, repositoryId, metrics: { ...metrics } };
  }

  async appendOperation(args: {
    workspaceId: string;
    expectedRevision: number;
    operation: WorkspaceOperation;
    interruptAfterAppend?: boolean;
  }): Promise<{ revision: number; replayed: boolean }> {
    const workspace = assertLiveWorkspace(this.workspaces.get(args.workspaceId));
    if (workspace.strategy !== "operation-backed") {
      throw new Error("Operations require an operation-backed workspace");
    }
    const fingerprint = await operationFingerprint(args.operation);
    const existing = workspace.operationById.get(args.operation.id);
    if (existing) {
      if (existing.fingerprint !== fingerprint) throw new Error("Operation ID was reused");
      return { revision: existing.revision, replayed: true };
    }
    if (workspace.revision !== args.expectedRevision) throw new Error("Stale workspace revision");
    const stored: StoredOperation = {
      ...args.operation,
      bytes: args.operation.bytes === null ? null : cloneBytes(args.operation.bytes),
      fingerprint,
      revision: workspace.revision + 1,
      generation: workspace.baseGeneration,
    };
    workspace.operations.push(stored);
    workspace.operationById.set(stored.id, stored);
    workspace.revision = stored.revision;
    if (stored.bytes === null) workspace.files.delete(stored.path);
    else workspace.files.set(stored.path, cloneBytes(stored.bytes));
    workspace.metrics.metadataWrites += 1;
    workspace.metrics.operationRecords += 1;
    if (args.interruptAfterAppend) throw new Error("Synthetic append interruption");
    return { revision: stored.revision, replayed: false };
  }

  replaceFile(args: {
    workspaceId: string;
    expectedRevision: number;
    path: string;
    bytes: Uint8Array;
  }): number {
    const workspace = assertLiveWorkspace(this.workspaces.get(args.workspaceId));
    if (workspace.strategy === "same-repository-ref" || workspace.strategy === "operation-backed") {
      throw new Error("Strategy requires its own write boundary");
    }
    if (workspace.revision !== args.expectedRevision) throw new Error("Stale workspace revision");
    workspace.files.set(args.path, cloneBytes(args.bytes));
    workspace.revision += 1;
    workspace.metrics.editBytes += args.bytes.byteLength;
    workspace.metrics.metadataWrites += 1;
    return workspace.revision;
  }

  branchFromOperation(args: {
    workspaceId: string;
    operationId: string;
    requestId: string;
    interruptAfterCreate?: boolean;
  }): string {
    const source = assertLiveWorkspace(this.workspaces.get(args.workspaceId));
    const boundary = source.operationById.get(args.operationId);
    if (!boundary) throw new Error("Branch operation is unavailable");
    const generation = source.generations.get(boundary.generation);
    if (!generation) throw new Error("Branch generation is unavailable");
    const fingerprint = `${source.id}:${boundary.generation}:${boundary.id}:${boundary.fingerprint}`;
    const existing = this.branchRequests.get(args.requestId);
    if (existing) {
      if (existing.fingerprint !== fingerprint) throw new Error("Branch request ID was reused");
      assertLiveWorkspace(this.workspaces.get(existing.workspaceId));
      return existing.workspaceId;
    }
    const created = this.createWorkspace({
      sourceRepositoryId: generation.repositoryId,
      sourceCommitOid: generation.baseCommitOid,
      strategy: "operation-backed",
    });
    const branch = assertLiveWorkspace(this.workspaces.get(created.workspaceId));
    branch.lineageId = source.lineageId;
    branch.baseGeneration = boundary.generation;
    branch.baseCommitOid = generation.baseCommitOid;
    branch.baseFiles = cloneFiles(generation.files);
    branch.generations = new Map([
      [
        boundary.generation,
        {
          repositoryId: branch.repositoryId,
          baseCommitOid: generation.baseCommitOid,
          files: cloneFiles(generation.files),
        },
      ],
    ]);
    branch.files = cloneFiles(this.replayFiles(source, boundary.revision));
    branch.operations = source.operations
      .filter(
        (operation) =>
          operation.generation === boundary.generation && operation.revision <= boundary.revision
      )
      .map((operation) => ({
        ...operation,
        bytes: operation.bytes === null ? null : cloneBytes(operation.bytes),
      }));
    branch.operationById = new Map(branch.operations.map((operation) => [operation.id, operation]));
    branch.revision = boundary.revision;
    this.branchRequests.set(args.requestId, {
      fingerprint,
      workspaceId: branch.id,
      sourceWorkspaceId: source.id,
      lineageId: source.lineageId,
    });
    if (args.interruptAfterCreate) throw new Error(`Synthetic branch interruption: ${branch.id}`);
    return branch.id;
  }

  private replayFiles(workspace: WorkspaceRecord, revision: number): Map<string, Uint8Array> {
    const boundary = workspace.operations.find((operation) => operation.revision === revision);
    if (!boundary) throw new Error("Replay boundary is unavailable");
    const generation = workspace.generations.get(boundary.generation);
    if (!generation) throw new Error("Replay generation is unavailable");
    const files = cloneFiles(generation.files);
    for (const operation of workspace.operations) {
      if (operation.generation !== boundary.generation) continue;
      if (operation.revision > revision) break;
      if (operation.bytes === null) files.delete(operation.path);
      else files.set(operation.path, cloneBytes(operation.bytes));
    }
    return files;
  }

  replayAtOperation(args: {
    workspaceId: string;
    operationId: string;
    interruptAfterProjection?: boolean;
  }): Map<string, Uint8Array> {
    const workspace = assertLiveWorkspace(this.workspaces.get(args.workspaceId));
    const boundary = workspace.operationById.get(args.operationId);
    if (!boundary) throw new Error("Replay operation is unavailable");
    const files = this.replayFiles(workspace, boundary.revision);
    if (args.interruptAfterProjection) throw new Error("Synthetic replay interruption");
    return cloneFiles(files);
  }

  projectFiles(workspaceId: string): Map<string, Uint8Array> {
    return cloneFiles(assertLiveWorkspace(this.workspaces.get(workspaceId)).files);
  }

  async checkpoint(args: {
    workspaceId: string;
    expectedRevision: number;
    committedAtSeconds: number;
    message: string;
    interruptAfterPack?: boolean;
  }): Promise<WorkspaceCheckpoint> {
    const workspace = assertLiveWorkspace(this.workspaces.get(args.workspaceId));
    if (workspace.strategy === "same-repository-ref") {
      throw new Error("Same-repository refs do not provide workspace isolation");
    }
    if (workspace.revision !== args.expectedRevision) throw new Error("Stale workspace revision");
    const revisionAtStart = workspace.revision;
    const generationAtStart = workspace.baseGeneration;
    const checkpointAtStart = workspace.checkpoint;
    const parentOid = checkpointAtStart?.commitOid ?? workspace.baseCommitOid;
    const repository = assertLiveRepository(this.repositories.get(workspace.repositoryId));
    const filesAtStart = cloneFiles(workspace.files);
    const fileIdentities: Array<{ path: string; sha256: string }> = [];
    for (const [path, bytes] of Array.from(filesAtStart).sort(([left], [right]) =>
      left.localeCompare(right)
    )) {
      fileIdentities.push({ path, sha256: await sha256(bytes) });
    }
    const checkpointFingerprint = await sha256(
      JSON.stringify({
        committedAtSeconds: args.committedAtSeconds,
        message: args.message,
        revision: revisionAtStart,
        baseGeneration: generationAtStart,
        fileIdentities,
      })
    );
    if (
      workspace.checkpoint !== null &&
      workspace.checkpointFingerprint === checkpointFingerprint
    ) {
      if (repository.ref !== workspace.checkpoint.commitOid) {
        throw new Error("Workspace ref changed");
      }
      return { ...workspace.checkpoint };
    }
    if (repository.ref !== parentOid) throw new Error("Workspace ref changed");
    const files: IngestionFile[] = Array.from(filesAtStart, ([path, bytes]) => ({
      path,
      bytes,
    }));
    const built: BuiltIngestionCommit = await buildIngestionCommit({
      files,
      parentOid,
      committedAtSeconds: args.committedAtSeconds,
      message: args.message,
    });
    if (
      workspace.revision !== revisionAtStart ||
      workspace.baseGeneration !== generationAtStart ||
      workspace.checkpoint !== checkpointAtStart ||
      repository.ref !== parentOid
    ) {
      throw new Error("Workspace changed during checkpoint");
    }
    const packId = `pack_${built.commitOid}`;
    const existingPack = this.packs.get(packId);
    if (!existingPack) {
      this.packs.set(packId, {
        id: packId,
        bytes: built.pack.byteLength,
        owners: new Set([workspace.repositoryId]),
        pins: new Set(),
      });
    } else {
      existingPack.owners.add(workspace.repositoryId);
    }
    repository.packIds.add(packId);
    if (args.interruptAfterPack) throw new Error("Synthetic checkpoint interruption");
    const checkpoint: WorkspaceCheckpoint = {
      commitOid: built.commitOid,
      treeOid: built.treeOid,
      packId,
      packBytes: built.pack.byteLength,
      fileCount: files.length,
    };
    workspace.checkpointPackIds.add(packId);
    const requiredPackIds = new Set([...workspace.basePackIds, ...workspace.checkpointPackIds]);
    repository.commits.set(checkpoint.commitOid, {
      oid: checkpoint.commitOid,
      parentOid,
      files: cloneFiles(filesAtStart),
      requiredPackIds,
    });
    repository.ref = checkpoint.commitOid;
    workspace.checkpoint = checkpoint;
    workspace.checkpointFingerprint = checkpointFingerprint;
    workspace.metrics.editBytes = built.pack.byteLength;
    workspace.metrics.metadataWrites += 1;
    workspace.metrics.localCheckpointPacks += 1;
    return checkpoint;
  }

  beginPromotion(args: {
    workspaceId: string;
    targetRepositoryId: string;
    expectedTargetRef: string | null;
    checkpointOid: string;
  }): string {
    const workspace = assertLiveWorkspace(this.workspaces.get(args.workspaceId));
    if (!workspace.checkpoint) throw new Error("Workspace has no checkpoint");
    if (workspace.checkpoint.commitOid !== args.checkpointOid) {
      throw new Error("Workspace checkpoint changed");
    }
    const workspaceRepository = assertLiveRepository(this.repositories.get(workspace.repositoryId));
    const commit = workspaceRepository.commits.get(workspace.checkpoint.commitOid);
    if (!commit) throw new Error("Workspace checkpoint is unavailable");
    const target = assertLiveRepository(this.repositories.get(args.targetRepositoryId));
    if (target.ref !== args.expectedTargetRef) throw new Error("Target ref changed");
    if (
      Array.from(this.promotions.values()).some(
        (promotion) => promotion.targetRepositoryId === target.id
      )
    ) {
      throw new Error("Target already has a promotion in flight");
    }
    const id = this.id("promotion");
    const promotionPin = `promotion:${id}`;
    for (const packId of commit.requiredPackIds) {
      const pack = this.packs.get(packId);
      if (!pack) throw new Error("Promotion pack is unavailable");
      pack.pins.add(promotionPin);
    }
    const commits = new Map<string, CommitRecord>();
    let commitCursor: CommitRecord | undefined = commit;
    while (commitCursor) {
      commits.set(commitCursor.oid, cloneCommit(commitCursor));
      commitCursor = commitCursor.parentOid
        ? workspaceRepository.commits.get(commitCursor.parentOid)
        : undefined;
    }
    this.promotions.set(id, {
      id,
      workspaceId: workspace.id,
      targetRepositoryId: target.id,
      expectedRef: args.expectedTargetRef,
      checkpoint: { ...workspace.checkpoint },
      commits,
      requiredPackIds: Array.from(commit.requiredPackIds),
      nextPackIndex: 0,
      transferredBytes: 0,
      transferredPackIds: new Set(),
    });
    return id;
  }

  resumePromotion(promotionId: string, interruptAfterPack = false): PromotionResult {
    const promotion = this.promotions.get(promotionId);
    if (!promotion) throw new Error("Promotion is unavailable");
    const target = assertLiveRepository(this.repositories.get(promotion.targetRepositoryId));
    if (target.ref !== promotion.expectedRef) throw new Error("Target ref changed");

    while (promotion.nextPackIndex < promotion.requiredPackIds.length) {
      const packId = promotion.requiredPackIds[promotion.nextPackIndex]!;
      const pack = this.packs.get(packId);
      if (!pack) throw new Error("Promotion pack is unavailable");
      if (!pack.owners.has(target.id)) {
        pack.owners.add(target.id);
        target.packIds.add(pack.id);
        promotion.transferredPackIds.add(pack.id);
        promotion.transferredBytes += pack.bytes;
      }
      promotion.nextPackIndex += 1;
      if (interruptAfterPack) {
        return { state: "interrupted", transferredBytes: promotion.transferredBytes };
      }
    }
    for (const commit of promotion.commits.values()) {
      target.commits.set(commit.oid, cloneCommit(commit));
    }
    target.ref = promotion.checkpoint.commitOid;
    this.pruneRepositoryLinks(target);
    this.releasePromotionPins(promotion);
    this.promotions.delete(promotionId);
    this.collectUnreachablePacks();
    return {
      state: "committed",
      transferredBytes: promotion.transferredBytes,
      commitOid: promotion.checkpoint.commitOid,
    };
  }

  abortPromotion(promotionId: string): void {
    const promotion = this.promotions.get(promotionId);
    if (!promotion) return;
    const target = this.repositories.get(promotion.targetRepositoryId);
    if (target) {
      for (const packId of promotion.transferredPackIds) {
        target.packIds.delete(packId);
        this.packs.get(packId)?.owners.delete(target.id);
      }
    }
    this.releasePromotionPins(promotion);
    this.promotions.delete(promotionId);
    this.collectUnreachablePacks();
  }

  private releasePromotionPins(promotion: PromotionRecord): void {
    const promotionPin = `promotion:${promotion.id}`;
    for (const packId of promotion.requiredPackIds) {
      this.packs.get(packId)?.pins.delete(promotionPin);
    }
  }

  rebaseWorkspace(args: {
    workspaceId: string;
    sourceRepositoryId: string;
    sourceCommitOid: string;
    expectedRevision: number;
    expectedCheckpointOid: string | null;
  }): void {
    const workspace = assertLiveWorkspace(this.workspaces.get(args.workspaceId));
    if (workspace.strategy === "same-repository-ref") {
      throw new Error("Same-repository refs cannot be rebased independently");
    }
    if (workspace.revision !== args.expectedRevision) throw new Error("Stale workspace revision");
    if ((workspace.checkpoint?.commitOid ?? null) !== args.expectedCheckpointOid) {
      throw new Error("Workspace checkpoint changed");
    }
    const source = assertLiveRepository(this.repositories.get(args.sourceRepositoryId));
    const sourceCommit = source.commits.get(args.sourceCommitOid);
    if (!sourceCommit) throw new Error("Exact source commit is unavailable");
    const rebasedFiles = cloneFiles(sourceCommit.files);
    const proposalPaths = new Set([...workspace.baseFiles.keys(), ...workspace.files.keys()]);
    for (const path of proposalPaths) {
      const oldBase = workspace.baseFiles.get(path);
      const proposal = workspace.files.get(path);
      if (equalBytes(oldBase, proposal)) continue;
      const newBase = sourceCommit.files.get(path);
      if (!equalBytes(newBase, oldBase) && !equalBytes(newBase, proposal)) {
        throw new Error(`Rebase conflict: ${path}`);
      }
      if (proposal === undefined) rebasedFiles.delete(path);
      else rebasedFiles.set(path, cloneBytes(proposal));
    }
    for (const packId of workspace.basePackIds) this.packs.get(packId)?.pins.delete(workspace.id);
    const repository = assertLiveRepository(this.repositories.get(workspace.repositoryId));
    workspace.sourceRepositoryId = source.id;
    workspace.basePackIds = new Set();
    for (const sourcePackId of sourceCommit.requiredPackIds) {
      const sourcePack = this.packs.get(sourcePackId);
      if (!sourcePack) throw new Error("Source pack is unavailable");
      if (workspace.strategy === "full-copy") {
        const copyId = this.id("pack");
        this.packs.set(copyId, {
          id: copyId,
          bytes: sourcePack.bytes,
          owners: new Set([repository.id]),
          pins: new Set(),
        });
        repository.packIds.add(copyId);
        workspace.basePackIds.add(copyId);
      } else {
        sourcePack.pins.add(workspace.id);
        repository.packIds.add(sourcePackId);
        workspace.basePackIds.add(sourcePackId);
      }
    }
    workspace.baseGeneration += 1;
    workspace.baseCommitOid = sourceCommit.oid;
    workspace.baseFiles = cloneFiles(sourceCommit.files);
    workspace.files = rebasedFiles;
    workspace.generations.set(workspace.baseGeneration, {
      repositoryId: workspace.repositoryId,
      baseCommitOid: sourceCommit.oid,
      files: cloneFiles(rebasedFiles),
    });
    workspace.checkpoint = null;
    workspace.checkpointFingerprint = null;
    workspace.checkpointPackIds = new Set();
    repository.commits.set(sourceCommit.oid, {
      oid: sourceCommit.oid,
      parentOid: sourceCommit.parentOid,
      files: cloneFiles(sourceCommit.files),
      requiredPackIds: new Set(workspace.basePackIds),
    });
    repository.ref = sourceCommit.oid;
    this.pruneRepositoryLinks(repository);
    this.collectUnreachablePacks();
  }

  purgeOperationHistory(workspaceId: string): void {
    const workspace = assertLiveWorkspace(this.workspaces.get(workspaceId));
    const lineage = Array.from(this.workspaces.values()).filter(
      (candidate) =>
        !candidate.deleted &&
        candidate.lineageId === workspace.lineageId &&
        candidate.operations.length > 0
    );
    for (const candidate of lineage) {
      if (!candidate.checkpoint) {
        throw new Error("Every linked workspace requires a checkpoint before provenance purge");
      }
    }
    for (const candidate of lineage) {
      const repository = assertLiveRepository(this.repositories.get(candidate.repositoryId));
      const checkpoint = candidate.checkpoint!;
      const checkpointCommit = repository.commits.get(checkpoint.commitOid);
      if (!checkpointCommit) throw new Error("Checkpoint commit is unavailable");
      for (const packId of candidate.basePackIds) {
        this.packs.get(packId)?.pins.delete(candidate.id);
      }
      candidate.baseGeneration += 1;
      candidate.sourceRepositoryId = candidate.repositoryId;
      candidate.baseCommitOid = checkpointCommit.oid;
      candidate.basePackIds = new Set(checkpointCommit.requiredPackIds);
      for (const packId of candidate.basePackIds) this.packs.get(packId)?.pins.add(candidate.id);
      candidate.baseFiles = cloneFiles(checkpointCommit.files);
      candidate.files = cloneFiles(checkpointCommit.files);
      candidate.generations = new Map([
        [
          candidate.baseGeneration,
          {
            repositoryId: candidate.repositoryId,
            baseCommitOid: checkpointCommit.oid,
            files: cloneFiles(checkpointCommit.files),
          },
        ],
      ]);
      candidate.operations = [];
      candidate.operationById.clear();
      candidate.revision = 0;
    }
    for (const [requestId, request] of this.branchRequests) {
      if (request.lineageId === workspace.lineageId) this.branchRequests.delete(requestId);
    }
  }

  operationCount(workspaceId: string): number {
    return assertLiveWorkspace(this.workspaces.get(workspaceId)).operations.length;
  }

  deleteWorkspace(workspaceId: string): void {
    const workspace = assertLiveWorkspace(this.workspaces.get(workspaceId));
    for (const packId of workspace.basePackIds) this.packs.get(packId)?.pins.delete(workspace.id);
    for (const [requestId, request] of this.branchRequests) {
      if (request.workspaceId === workspace.id || request.sourceWorkspaceId === workspace.id) {
        this.branchRequests.delete(requestId);
      }
    }
    if (workspace.strategy !== "same-repository-ref") {
      this.deleteRepository(workspace.repositoryId);
    }
    workspace.operations.length = 0;
    workspace.operationById.clear();
    workspace.files.clear();
    workspace.baseFiles.clear();
    workspace.generations.clear();
    workspace.checkpointPackIds.clear();
    workspace.basePackIds.clear();
    workspace.deleted = true;
    this.workspaces.delete(workspace.id);
    this.collectUnreachablePacks();
  }

  deleteRepository(repositoryId: string): void {
    const repository = assertLiveRepository(this.repositories.get(repositoryId));
    repository.deleted = true;
    for (const token of this.tokens.values()) {
      if (token.repositoryId === repositoryId) token.revoked = true;
    }
    for (const packId of repository.packIds) this.packs.get(packId)?.owners.delete(repositoryId);
    this.collectUnreachablePacks();
  }

  private collectUnreachablePacks(): void {
    const reachablePackIds = new Set<string>();
    for (const pack of this.packs.values()) {
      if (pack.pins.size > 0) reachablePackIds.add(pack.id);
    }
    for (const repository of this.repositories.values()) {
      if (repository.deleted) continue;
      const roots = new Set(repository.reviewHolds);
      if (repository.ref) roots.add(repository.ref);
      for (const commitOid of roots) {
        const commit = repository.commits.get(commitOid);
        if (!commit) continue;
        for (const packId of commit.requiredPackIds) reachablePackIds.add(packId);
      }
    }
    for (const packId of this.packs.keys()) {
      if (reachablePackIds.has(packId)) continue;
      this.packs.delete(packId);
      for (const repository of this.repositories.values()) repository.packIds.delete(packId);
    }
  }

  private pruneRepositoryLinks(repository: RepositoryRecord): void {
    const reachableCommitIds = new Set<string>();
    const pending = Array.from(repository.reviewHolds);
    if (repository.ref) pending.push(repository.ref);
    for (const workspace of this.workspaces.values()) {
      if (workspace.deleted || workspace.repositoryId !== repository.id) continue;
      const operationGenerations = new Set(
        workspace.operations.map((operation) => operation.generation)
      );
      for (const generationId of operationGenerations) {
        const generation = workspace.generations.get(generationId);
        if (generation) pending.push(generation.baseCommitOid);
      }
    }
    while (pending.length > 0) {
      const commitOid = pending.pop()!;
      if (reachableCommitIds.has(commitOid)) continue;
      const commit = repository.commits.get(commitOid);
      if (!commit) continue;
      reachableCommitIds.add(commitOid);
      if (commit.parentOid) pending.push(commit.parentOid);
    }
    const reachablePackIds = new Set<string>();
    for (const commitOid of reachableCommitIds) {
      const commit = repository.commits.get(commitOid)!;
      for (const packId of commit.requiredPackIds) reachablePackIds.add(packId);
    }
    for (const commitOid of repository.commits.keys()) {
      if (!reachableCommitIds.has(commitOid)) repository.commits.delete(commitOid);
    }
    for (const packId of repository.packIds) {
      if (reachablePackIds.has(packId)) continue;
      repository.packIds.delete(packId);
      this.packs.get(packId)?.owners.delete(repository.id);
    }
  }

  metrics(workspaceId: string): WorkspaceMetrics {
    return { ...assertLiveWorkspace(this.workspaces.get(workspaceId)).metrics };
  }

  repositoryReadable(repositoryId: string, commitOid: string): boolean {
    const repository = this.repositories.get(repositoryId);
    if (!repository || repository.deleted || repository.ref !== commitOid) return false;
    const commit = repository.commits.get(commitOid);
    if (!commit) return false;
    return Array.from(commit.requiredPackIds).every(
      (packId) => repository.packIds.has(packId) && this.packs.has(packId)
    );
  }

  repositoryCommitReadable(repositoryId: string, commitOid: string): boolean {
    const repository = this.repositories.get(repositoryId);
    if (!repository || repository.deleted) return false;
    const commit = repository.commits.get(commitOid);
    if (!commit) return false;
    return Array.from(commit.requiredPackIds).every(
      (packId) => repository.packIds.has(packId) && this.packs.has(packId)
    );
  }

  packState(): { packs: number; bytes: number; pins: number } {
    let bytes = 0;
    let pins = 0;
    for (const pack of this.packs.values()) {
      bytes += pack.bytes;
      pins += pack.pins.size;
    }
    return { packs: this.packs.size, bytes, pins };
  }

  workspaceStorageState(): {
    workspaces: number;
    operations: number;
    operationBytes: number;
    branchRequests: number;
  } {
    let operations = 0;
    let operationBytes = 0;
    for (const workspace of this.workspaces.values()) {
      operations += workspace.operations.length;
      for (const operation of workspace.operations)
        operationBytes += operation.bytes?.byteLength ?? 0;
    }
    return {
      workspaces: this.workspaces.size,
      operations,
      operationBytes,
      branchRequests: this.branchRequests.size,
    };
  }
}
