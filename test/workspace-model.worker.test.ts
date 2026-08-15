import { describe, expect, it } from "vitest";
import { env, exports as workerExports } from "cloudflare:workers";

import { zeroOid } from "@/worker/common";
import { setupRepoForTests } from "./util/repoSeed";
import { uniqueRepoId } from "./util/test-helpers";
import { WorkspaceProbeModel, type WorkspaceStrategy } from "./util/workspace-model";

const encoder = new TextEncoder();
const BASE_OID = "1".repeat(40);
const SECOND_BASE_OID = "2".repeat(40);

function files(...entries: Array<[string, string]>): Map<string, Uint8Array> {
  return new Map(entries.map(([path, value]) => [path, encoder.encode(value)]));
}

describe("Investigation 7 workspace model", () => {
  it("checkpoints an operation projection through the real ingestion and snapshot path", async () => {
    const model = new WorkspaceProbeModel();
    const source = model.createRepository({
      namespace: "team",
      name: "source",
      basePackBytes: 100,
      baseFiles: files(["index.html", "base"], ["notes.txt", "keep"]),
      baseCommitOid: BASE_OID,
    });
    const workspace = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      operation: {
        id: "runtime-edit",
        actor: "agent-runtime",
        conversationId: "conversation-runtime",
        path: "index.html",
        bytes: encoder.encode("<h1>projected</h1>"),
      },
    });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 1,
      operation: {
        id: "runtime-add",
        actor: "agent-runtime",
        path: "nested/new.txt",
        bytes: encoder.encode("new"),
      },
    });

    const owner = "workspace";
    const repo = uniqueRepoId("runtime-checkpoint");
    await setupRepoForTests(env, owner, repo, { doName: `repo:${owner}-${repo}` });
    const form = new FormData();
    form.set("expectedOid", zeroOid());
    form.set("actor", "agent-runtime");
    form.set("idempotencyKey", "workspace-runtime-checkpoint");
    form.set("committedAtSeconds", "1786742400");
    form.set("message", "Workspace checkpoint");
    const projected = model.projectFiles(workspace.workspaceId);
    for (const [path, bytes] of projected) {
      form.append("files", new Blob([Uint8Array.from(bytes)]), path);
    }

    const response = await workerExports.default.fetch(
      `https://example.com/_internal/ingestion/${owner}/${repo}`,
      {
        method: "POST",
        headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` },
        body: form,
      }
    );
    expect(response.status).toBe(201);
    const body = (await response.json()) as { acceptedWrite: { afterSha: string } };
    const snapshotBase = `https://example.com/_internal/snapshots/${owner}/${repo}/${body.acceptedWrite.afterSha}`;
    const headers = { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` };
    const manifestResponse = await workerExports.default.fetch(`${snapshotBase}/manifest`, {
      headers,
    });
    expect(manifestResponse.status).toBe(200);
    const manifest = (await manifestResponse.json()) as { files: Array<{ path: string }> };
    expect(manifest.files.map((file) => file.path)).toEqual([
      "index.html",
      "nested/new.txt",
      "notes.txt",
    ]);
    for (const [path, expected] of projected) {
      const fileResponse = await workerExports.default.fetch(
        `${snapshotBase}/file?path=${encodeURIComponent(path)}`,
        { headers }
      );
      expect(fileResponse.status).toBe(200);
      expect(new Uint8Array(await fileResponse.arrayBuffer())).toEqual(expected);
    }
  });

  it("uses opaque repository IDs for tokens across label changes and deletion", () => {
    let now = 1_000;
    const model = new WorkspaceProbeModel(() => now);
    const first = model.createRepository({ namespace: "team", name: "site" });
    const second = model.createRepository({ namespace: "team", name: "other" });
    const token = model.issueToken({
      repositoryId: first.repositoryId,
      capabilities: new Set(["read"]),
      expiresAt: 2_000,
      issuerCanAdminister: true,
    });

    expect(model.authorize(token, first.repositoryId, "read")).toBe(true);
    expect(model.authorize(token, first.repositoryId, "write")).toBe(false);
    expect(model.authorize(token, second.repositoryId, "read")).toBe(false);
    model.renameRepository(first.repositoryId, "renamed", "site-two");
    expect(model.authorize(token, first.repositoryId, "read")).toBe(true);
    const clone = model.beginClone(token, first.repositoryId);
    expect(() => model.readCloneChunk(clone)).not.toThrow();
    now = 2_000;
    expect(model.authorize(token, first.repositoryId, "read")).toBe(false);
    expect(() => model.readCloneChunk(clone)).toThrow("Clone authorization expired");
    now = 1_000;
    model.revokeToken(token);
    expect(model.authorize(token, first.repositoryId, "read")).toBe(false);
    const replacement = model.issueToken({
      repositoryId: first.repositoryId,
      capabilities: new Set(["read", "write"]),
      expiresAt: 2_000,
      issuerCanAdminister: true,
    });
    const replacementClone = model.beginClone(replacement, first.repositoryId);
    model.revokeToken(replacement);
    expect(() => model.readCloneChunk(replacementClone)).toThrow("Clone authorization expired");
    const deletionToken = model.issueToken({
      repositoryId: first.repositoryId,
      capabilities: new Set(["write"]),
      expiresAt: 2_000,
      issuerCanAdminister: true,
    });
    model.deleteRepository(first.repositoryId);
    expect(model.authorize(deletionToken, first.repositoryId, "write")).toBe(false);
    expect(() =>
      model.issueToken({
        repositoryId: second.repositoryId,
        capabilities: new Set(["read"]),
        expiresAt: 2_000,
        issuerCanAdminister: false,
      })
    ).toThrow("Token issuer is unauthorized");
  });

  it("rejects same-repository refs and preserves every independent model through deletion", async () => {
    const strategies: WorkspaceStrategy[] = [
      "same-repository-ref",
      "full-copy",
      "shared-catalog",
      "copy-on-write",
      "operation-backed",
    ];
    const storageShapes = new Set<string>();
    for (const strategy of strategies) {
      const model = new WorkspaceProbeModel();
      const source = model.createRepository({
        namespace: "team",
        name: "source",
        basePackBytes: 100_000_000,
        baseCommitOid: BASE_OID,
      });
      const workspace = model.createWorkspace({
        sourceRepositoryId: source.repositoryId,
        sourceCommitOid: BASE_OID,
        strategy,
      });
      expect(workspace.metrics.independentAuthorization).toBe(strategy !== "same-repository-ref");
      expect(workspace.metrics.independentDeletion).toBe(strategy !== "same-repository-ref");
      expect(workspace.metrics.initialBytes).toBe(strategy === "full-copy" ? 100_000_000 : 0);
      storageShapes.add(workspace.metrics.storageShape);
      if (strategy === "same-repository-ref") {
        await expect(
          model.checkpoint({
            workspaceId: workspace.workspaceId,
            expectedRevision: 0,
            committedAtSeconds: 1_786_742_400,
            message: "rejected",
          })
        ).rejects.toThrow("Same-repository refs do not provide workspace isolation");
        continue;
      }
      if (strategy === "operation-backed") {
        await model.appendOperation({
          workspaceId: workspace.workspaceId,
          expectedRevision: 0,
          operation: {
            id: "edit",
            actor: "agent",
            path: "index.html",
            bytes: encoder.encode("edited"),
          },
        });
      } else {
        expect(
          model.replaceFile({
            workspaceId: workspace.workspaceId,
            expectedRevision: 0,
            path: "index.html",
            bytes: encoder.encode("edited"),
          })
        ).toBe(1);
      }
      const checkpoint = await model.checkpoint({
        workspaceId: workspace.workspaceId,
        expectedRevision: 1,
        committedAtSeconds: 1_786_742_400,
        message: strategy,
      });
      const target = model.createRepository({ namespace: "team", name: "target" });
      model.deleteRepository(source.repositoryId);
      const promotion = model.beginPromotion({
        workspaceId: workspace.workspaceId,
        targetRepositoryId: target.repositoryId,
        expectedTargetRef: null,
        checkpointOid: checkpoint.commitOid,
      });
      expect(model.resumePromotion(promotion)).toMatchObject({ state: "committed" });
      model.deleteWorkspace(workspace.workspaceId);
      expect(model.repositoryReadable(target.repositoryId, checkpoint.commitOid)).toBe(true);
    }
    expect(storageShapes.size).toBe(5);
  });

  it("deletes the workspace first, reclaims its storage, and preserves the source", async () => {
    const strategies: WorkspaceStrategy[] = [
      "full-copy",
      "shared-catalog",
      "copy-on-write",
      "operation-backed",
    ];
    for (const strategy of strategies) {
      const model = new WorkspaceProbeModel();
      const source = model.createRepository({
        namespace: "team",
        name: "source",
        basePackBytes: 100,
        baseFiles: files(["index.html", "base"]),
        baseCommitOid: BASE_OID,
      });
      const workspace = model.createWorkspace({
        sourceRepositoryId: source.repositoryId,
        sourceCommitOid: BASE_OID,
        strategy,
      });
      if (strategy === "operation-backed") {
        await model.appendOperation({
          workspaceId: workspace.workspaceId,
          expectedRevision: 0,
          operation: {
            id: "workspace-first",
            actor: "agent",
            path: "index.html",
            bytes: encoder.encode("edited"),
          },
        });
      } else {
        model.replaceFile({
          workspaceId: workspace.workspaceId,
          expectedRevision: 0,
          path: "index.html",
          bytes: encoder.encode("edited"),
        });
      }
      const checkpoint = await model.checkpoint({
        workspaceId: workspace.workspaceId,
        expectedRevision: 1,
        committedAtSeconds: 1_786_742_400,
        message: `workspace-first-${strategy}`,
      });

      model.deleteWorkspace(workspace.workspaceId);
      expect(model.repositoryReadable(source.repositoryId, BASE_OID)).toBe(true);
      expect(model.repositoryCommitReadable(workspace.repositoryId, checkpoint.commitOid)).toBe(
        false
      );
      expect(model.packState()).toEqual({ packs: 1, bytes: 100, pins: 0 });
      expect(model.workspaceStorageState()).toEqual({
        workspaces: 0,
        operations: 0,
        operationBytes: 0,
        branchRequests: 0,
      });
    }
  });

  it("replays operations deterministically, branches at an operation, and survives interruptions", async () => {
    const model = new WorkspaceProbeModel();
    const sourceFiles = files(["index.html", "base"], ["untouched.txt", "keep"]);
    const source = model.createRepository({
      namespace: "team",
      name: "source",
      basePackBytes: 100_000_000,
      baseFiles: sourceFiles,
      baseCommitOid: BASE_OID,
    });
    const workspace = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    const first = {
      id: "op-1",
      actor: "agent-1",
      conversationId: "conversation-1",
      messageId: "message-1",
      path: "index.html",
      bytes: encoder.encode("first"),
    };
    await expect(
      model.appendOperation({
        workspaceId: workspace.workspaceId,
        expectedRevision: 0,
        operation: first,
        interruptAfterAppend: true,
      })
    ).rejects.toThrow("Synthetic append interruption");
    expect(
      await model.appendOperation({
        workspaceId: workspace.workspaceId,
        expectedRevision: 0,
        operation: first,
      })
    ).toEqual({ revision: 1, replayed: true });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 1,
      operation: {
        id: "op-2",
        actor: "agent-2",
        path: "notes.md",
        bytes: encoder.encode("second"),
      },
    });

    expect(() =>
      model.replayAtOperation({
        workspaceId: workspace.workspaceId,
        operationId: "op-1",
        interruptAfterProjection: true,
      })
    ).toThrow("Synthetic replay interruption");
    const replayed = model.replayAtOperation({
      workspaceId: workspace.workspaceId,
      operationId: "op-1",
    });
    expect(new TextDecoder().decode(replayed.get("index.html"))).toBe("first");
    expect(replayed.has("notes.md")).toBe(false);

    model.deleteRepository(source.repositoryId);
    expect(() =>
      model.branchFromOperation({
        workspaceId: workspace.workspaceId,
        operationId: "op-1",
        requestId: "branch-1",
        interruptAfterCreate: true,
      })
    ).toThrow("Synthetic branch interruption");
    const branchId = model.branchFromOperation({
      workspaceId: workspace.workspaceId,
      operationId: "op-1",
      requestId: "branch-1",
    });
    expect(Array.from(model.projectFiles(branchId).keys()).sort()).toEqual([
      "index.html",
      "untouched.txt",
    ]);
    expect(new TextDecoder().decode(model.projectFiles(branchId).get("index.html"))).toBe("first");
    const nestedBranchId = model.branchFromOperation({
      workspaceId: branchId,
      operationId: "op-1",
      requestId: "branch-2",
    });
    expect(new TextDecoder().decode(model.projectFiles(nestedBranchId).get("untouched.txt"))).toBe(
      "keep"
    );
    expect(() =>
      model.branchFromOperation({
        workspaceId: workspace.workspaceId,
        operationId: "op-2",
        requestId: "branch-1",
      })
    ).toThrow("Branch request ID was reused");
  });

  it("checkpoints through real Git objects and recovers an interrupted checkpoint idempotently", async () => {
    const model = new WorkspaceProbeModel();
    const source = model.createRepository({
      namespace: "team",
      name: "source",
      basePackBytes: 100_000_000,
      baseCommitOid: BASE_OID,
    });
    const workspace = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    const args = {
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      committedAtSeconds: 1_786_742_400,
      message: "workspace checkpoint",
    };
    await expect(model.checkpoint({ ...args, interruptAfterPack: true })).rejects.toThrow(
      "Synthetic checkpoint interruption"
    );
    const first = await model.checkpoint(args);
    const replay = await model.checkpoint(args);
    expect(replay).toEqual(first);
    expect(model.repositoryReadable(workspace.repositoryId, first.commitOid)).toBe(true);
    expect(first.commitOid).toMatch(/^[0-9a-f]{40}$/);
    expect(first.treeOid).toMatch(/^[0-9a-f]{40}$/);
    const otherSource = model.createRepository({
      namespace: "team",
      name: "other-source",
      basePackBytes: 100_000_000,
      baseCommitOid: SECOND_BASE_OID,
    });
    const otherWorkspace = model.createWorkspace({
      sourceRepositoryId: otherSource.repositoryId,
      sourceCommitOid: SECOND_BASE_OID,
      strategy: "operation-backed",
    });
    const otherCheckpoint = await model.checkpoint({
      ...args,
      workspaceId: otherWorkspace.workspaceId,
    });
    expect(otherCheckpoint.treeOid).toBe(first.treeOid);
    expect(otherCheckpoint.commitOid).not.toBe(first.commitOid);
    expect(model.compareAndSetRef(workspace.repositoryId, first.commitOid, BASE_OID)).toBe(true);
    await expect(model.checkpoint(args)).rejects.toThrow("Workspace ref changed");
  });

  it("serializes overlapping checkpoints with one ref-CAS winner", async () => {
    const model = new WorkspaceProbeModel();
    const source = model.createRepository({
      namespace: "team",
      name: "source",
      basePackBytes: 100,
      baseFiles: files(["index.html", "same"]),
      baseCommitOid: BASE_OID,
    });
    const workspace = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    const checkpoint = {
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      committedAtSeconds: 1_786_742_400,
      message: "concurrent",
    };
    const outcomes = await Promise.allSettled([
      model.checkpoint(checkpoint),
      model.checkpoint(checkpoint),
    ]);
    expect(outcomes.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1);
    expect(outcomes.filter((outcome) => outcome.status === "rejected")).toHaveLength(1);
  });

  it("pins a source generation, repairs promotion, and leaves the target independent", async () => {
    const model = new WorkspaceProbeModel();
    const sourceFiles = files(["index.html", "base"]);
    const source = model.createRepository({
      namespace: "team",
      name: "source",
      basePackBytes: 100_000_000,
      baseFiles: sourceFiles,
      baseCommitOid: BASE_OID,
    });
    const target = model.createRepository({ namespace: "team", name: "target" });
    const workspace = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      operation: {
        id: "op-1",
        actor: "agent",
        path: "index.html",
        bytes: encoder.encode("edited"),
      },
    });
    const firstCheckpoint = await model.checkpoint({
      workspaceId: workspace.workspaceId,
      expectedRevision: 1,
      committedAtSeconds: 1_786_742_400,
      message: "first checkpoint",
    });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 1,
      operation: {
        id: "op-2",
        actor: "agent",
        path: "notes.md",
        bytes: encoder.encode("second checkpoint"),
      },
    });
    await expect(
      model.checkpoint({
        workspaceId: workspace.workspaceId,
        expectedRevision: 1,
        committedAtSeconds: 1_786_742_401,
        message: "stale checkpoint",
      })
    ).rejects.toThrow("Stale workspace revision");
    const checkpoint = await model.checkpoint({
      workspaceId: workspace.workspaceId,
      expectedRevision: 2,
      committedAtSeconds: 1_786_742_401,
      message: "second checkpoint",
    });
    expect(() =>
      model.beginPromotion({
        workspaceId: workspace.workspaceId,
        targetRepositoryId: target.repositoryId,
        expectedTargetRef: null,
        checkpointOid: firstCheckpoint.commitOid,
      })
    ).toThrow("Workspace checkpoint changed");

    model.deleteRepository(source.repositoryId);
    expect(model.packState().pins).toBeGreaterThan(0);
    const promotion = model.beginPromotion({
      workspaceId: workspace.workspaceId,
      targetRepositoryId: target.repositoryId,
      expectedTargetRef: null,
      checkpointOid: checkpoint.commitOid,
    });
    expect(model.resumePromotion(promotion, true).state).toBe("interrupted");
    model.deleteWorkspace(workspace.workspaceId);
    const completed = model.resumePromotion(promotion);
    expect(completed).toMatchObject({ state: "committed", commitOid: checkpoint.commitOid });
    expect(model.repositoryReadable(target.repositoryId, checkpoint.commitOid)).toBe(true);
    expect(model.packState().pins).toBe(0);
  });

  it("uses a retained checkpoint as the replay base after operation provenance is purged", async () => {
    const model = new WorkspaceProbeModel();
    const source = model.createRepository({
      namespace: "team",
      name: "source",
      basePackBytes: 100,
      baseFiles: files(["base.txt", "base"]),
      baseCommitOid: BASE_OID,
    });
    const workspace = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      operation: { id: "op-1", actor: "agent", path: "edit.txt", bytes: encoder.encode("edit") },
    });
    const prePurgeBranch = model.branchFromOperation({
      workspaceId: workspace.workspaceId,
      operationId: "op-1",
      requestId: "pre-purge",
    });
    const branchCheckpoint = await model.checkpoint({
      workspaceId: prePurgeBranch,
      expectedRevision: 1,
      committedAtSeconds: 1_786_742_399,
      message: "branch retention base",
    });
    const unrelated = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    await model.appendOperation({
      workspaceId: unrelated.workspaceId,
      expectedRevision: 0,
      operation: { id: "op-1", actor: "other", path: "other.txt", bytes: encoder.encode("other") },
    });
    await model.checkpoint({
      workspaceId: workspace.workspaceId,
      expectedRevision: 1,
      committedAtSeconds: 1_786_742_400,
      message: "retained base",
    });
    expect(model.workspaceStorageState().branchRequests).toBe(1);
    model.purgeOperationHistory(workspace.workspaceId);
    expect(model.operationCount(workspace.workspaceId)).toBe(0);
    expect(model.operationCount(prePurgeBranch)).toBe(0);
    expect(model.operationCount(unrelated.workspaceId)).toBe(1);
    expect(model.workspaceStorageState().branchRequests).toBe(0);
    expect(() =>
      model.branchFromOperation({
        workspaceId: workspace.workspaceId,
        operationId: "op-1",
        requestId: "pre-purge",
      })
    ).toThrow("Branch operation is unavailable");
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      operation: { id: "op-2", actor: "agent", path: "tail.txt", bytes: encoder.encode("tail") },
    });
    const branch = model.branchFromOperation({
      workspaceId: workspace.workspaceId,
      operationId: "op-2",
      requestId: "post-purge-branch",
    });
    expect(Array.from(model.projectFiles(branch).keys()).sort()).toEqual([
      "base.txt",
      "edit.txt",
      "tail.txt",
    ]);
    const target = model.createRepository({ namespace: "team", name: "retained-target" });
    model.deleteWorkspace(workspace.workspaceId);
    const promotion = model.beginPromotion({
      workspaceId: prePurgeBranch,
      targetRepositoryId: target.repositoryId,
      expectedTargetRef: null,
      checkpointOid: branchCheckpoint.commitOid,
    });
    model.resumePromotion(promotion);
    model.deleteWorkspace(prePurgeBranch);
    expect(model.repositoryReadable(target.repositoryId, branchCheckpoint.commitOid)).toBe(true);
  });

  it("keeps a reused checkpoint pack owned by every independent workspace", async () => {
    const model = new WorkspaceProbeModel();
    const source = model.createRepository({
      namespace: "team",
      name: "source",
      basePackBytes: 100,
      baseFiles: files(["index.html", "same"]),
      baseCommitOid: BASE_OID,
    });
    const first = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    const second = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    const checkpointArgs = { committedAtSeconds: 1_786_742_400, message: "same" };
    const firstCheckpoint = await model.checkpoint({
      workspaceId: first.workspaceId,
      expectedRevision: 0,
      ...checkpointArgs,
    });
    const secondCheckpoint = await model.checkpoint({
      workspaceId: second.workspaceId,
      expectedRevision: 0,
      ...checkpointArgs,
    });
    expect(secondCheckpoint.packId).toBe(firstCheckpoint.packId);
    const otherSource = model.createRepository({
      namespace: "team",
      name: "other-source",
      basePackBytes: 120,
      baseFiles: files(["other", "other"]),
      baseCommitOid: SECOND_BASE_OID,
    });
    model.retainForReview(first.repositoryId, firstCheckpoint.commitOid);
    model.rebaseWorkspace({
      workspaceId: first.workspaceId,
      sourceRepositoryId: otherSource.repositoryId,
      sourceCommitOid: SECOND_BASE_OID,
      expectedRevision: 0,
      expectedCheckpointOid: firstCheckpoint.commitOid,
    });
    model.releaseReview(first.repositoryId, firstCheckpoint.commitOid);
    expect(model.repositoryCommitReadable(first.repositoryId, firstCheckpoint.commitOid)).toBe(
      false
    );
    expect(model.repositoryCommitReadable(second.repositoryId, secondCheckpoint.commitOid)).toBe(
      true
    );
    model.deleteWorkspace(first.workspaceId);
    const target = model.createRepository({ namespace: "team", name: "target" });
    const promotion = model.beginPromotion({
      workspaceId: second.workspaceId,
      targetRepositoryId: target.repositoryId,
      expectedTargetRef: null,
      checkpointOid: secondCheckpoint.commitOid,
    });
    expect(model.resumePromotion(promotion)).toMatchObject({ state: "committed" });
    model.deleteWorkspace(second.workspaceId);
    expect(model.repositoryReadable(target.repositoryId, secondCheckpoint.commitOid)).toBe(true);
  });

  it("releases promotion holds and partial ownership after a target CAS conflict", async () => {
    const model = new WorkspaceProbeModel();
    const source = model.createRepository({
      namespace: "team",
      name: "source",
      basePackBytes: 100,
      baseFiles: files(["index.html", "source"]),
      baseCommitOid: BASE_OID,
    });
    const target = model.createRepository({
      namespace: "team",
      name: "target",
      basePackBytes: 80,
      baseFiles: files(["target.html", "target"]),
      baseCommitOid: SECOND_BASE_OID,
    });
    const workspace = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      operation: { id: "op", actor: "agent", path: "edit", bytes: encoder.encode("edit") },
    });
    const conflictCheckpoint = await model.checkpoint({
      workspaceId: workspace.workspaceId,
      expectedRevision: 1,
      committedAtSeconds: 1_786_742_400,
      message: "conflict",
    });
    const pinsBefore = model.packState().pins;
    const promotion = model.beginPromotion({
      workspaceId: workspace.workspaceId,
      targetRepositoryId: target.repositoryId,
      expectedTargetRef: SECOND_BASE_OID,
      checkpointOid: conflictCheckpoint.commitOid,
    });
    expect(() =>
      model.beginPromotion({
        workspaceId: workspace.workspaceId,
        targetRepositoryId: target.repositoryId,
        expectedTargetRef: SECOND_BASE_OID,
        checkpointOid: conflictCheckpoint.commitOid,
      })
    ).toThrow("Target already has a promotion in flight");
    expect(model.resumePromotion(promotion, true).state).toBe("interrupted");
    expect(model.compareAndSetRef(target.repositoryId, SECOND_BASE_OID, null)).toBe(true);
    expect(() => model.resumePromotion(promotion)).toThrow("Target ref changed");
    model.abortPromotion(promotion);
    expect(model.packState().pins).toBe(pinsBefore);
  });

  it("promotion prunes superseded target links without collecting a shared survivor", async () => {
    const model = new WorkspaceProbeModel();
    const source = model.createRepository({
      namespace: "team",
      name: "source",
      basePackBytes: 100,
      baseFiles: files(["source", "source"]),
      baseCommitOid: BASE_OID,
    });
    const target = model.createRepository({
      namespace: "team",
      name: "target",
      basePackBytes: 120,
      baseFiles: files(["target", "target"]),
      baseCommitOid: SECOND_BASE_OID,
    });
    const survivor = model.createWorkspace({
      sourceRepositoryId: target.repositoryId,
      sourceCommitOid: SECOND_BASE_OID,
      strategy: "shared-catalog",
    });
    const workspace = model.createWorkspace({
      sourceRepositoryId: source.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      operation: { id: "op", actor: "agent", path: "edit", bytes: encoder.encode("edit") },
    });
    const checkpoint = await model.checkpoint({
      workspaceId: workspace.workspaceId,
      expectedRevision: 1,
      committedAtSeconds: 1_786_742_400,
      message: "replace target",
    });
    const promotion = model.beginPromotion({
      workspaceId: workspace.workspaceId,
      targetRepositoryId: target.repositoryId,
      expectedTargetRef: SECOND_BASE_OID,
      checkpointOid: checkpoint.commitOid,
    });
    model.resumePromotion(promotion);
    expect(model.repositoryCommitReadable(target.repositoryId, SECOND_BASE_OID)).toBe(false);
    expect(model.repositoryReadable(survivor.repositoryId, SECOND_BASE_OID)).toBe(true);
  });

  it("starts a new base generation on rebase instead of fabricating operation provenance", async () => {
    const model = new WorkspaceProbeModel();
    const first = model.createRepository({
      namespace: "team",
      name: "first",
      basePackBytes: 100,
      baseFiles: files(["one", "1"]),
      baseCommitOid: BASE_OID,
    });
    const second = model.createRepository({
      namespace: "team",
      name: "second",
      basePackBytes: 120,
      baseFiles: files(["two", "2"]),
      baseCommitOid: SECOND_BASE_OID,
    });
    const workspace = model.createWorkspace({
      sourceRepositoryId: first.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      operation: { id: "old", actor: "agent", path: "old", bytes: encoder.encode("old") },
    });
    const reviewedCheckpoint = await model.checkpoint({
      workspaceId: workspace.workspaceId,
      expectedRevision: 1,
      committedAtSeconds: 1_786_742_399,
      message: "reviewed",
    });
    model.retainForReview(workspace.repositoryId, reviewedCheckpoint.commitOid);
    expect(() =>
      model.rebaseWorkspace({
        workspaceId: workspace.workspaceId,
        sourceRepositoryId: second.repositoryId,
        sourceCommitOid: SECOND_BASE_OID,
        expectedRevision: 0,
        expectedCheckpointOid: reviewedCheckpoint.commitOid,
      })
    ).toThrow("Stale workspace revision");
    expect(() =>
      model.rebaseWorkspace({
        workspaceId: workspace.workspaceId,
        sourceRepositoryId: second.repositoryId,
        sourceCommitOid: SECOND_BASE_OID,
        expectedRevision: 1,
        expectedCheckpointOid: null,
      })
    ).toThrow("Workspace checkpoint changed");
    model.rebaseWorkspace({
      workspaceId: workspace.workspaceId,
      sourceRepositoryId: second.repositoryId,
      sourceCommitOid: SECOND_BASE_OID,
      expectedRevision: 1,
      expectedCheckpointOid: reviewedCheckpoint.commitOid,
    });
    expect(
      model.repositoryCommitReadable(workspace.repositoryId, reviewedCheckpoint.commitOid)
    ).toBe(true);
    await expect(
      model.appendOperation({
        workspaceId: workspace.workspaceId,
        expectedRevision: 1,
        operation: { id: "new", actor: "agent", path: "new", bytes: encoder.encode("new") },
      })
    ).resolves.toEqual({ revision: 2, replayed: false });
    expect(Array.from(model.projectFiles(workspace.workspaceId).keys()).sort()).toEqual([
      "new",
      "old",
      "two",
    ]);
    model.releaseReview(workspace.repositoryId, reviewedCheckpoint.commitOid);
    expect(
      model.repositoryCommitReadable(workspace.repositoryId, reviewedCheckpoint.commitOid)
    ).toBe(false);
  });

  it("reports a three-way rebase conflict without mutating proposal state", async () => {
    const model = new WorkspaceProbeModel();
    const first = model.createRepository({
      namespace: "team",
      name: "first",
      basePackBytes: 100,
      baseFiles: files(["shared.txt", "base"]),
      baseCommitOid: BASE_OID,
    });
    const second = model.createRepository({
      namespace: "team",
      name: "second",
      basePackBytes: 120,
      baseFiles: files(["shared.txt", "target"]),
      baseCommitOid: SECOND_BASE_OID,
    });
    const workspace = model.createWorkspace({
      sourceRepositoryId: first.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "copy-on-write",
    });
    model.replaceFile({
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      path: "shared.txt",
      bytes: encoder.encode("proposal"),
    });
    expect(() =>
      model.rebaseWorkspace({
        workspaceId: workspace.workspaceId,
        sourceRepositoryId: second.repositoryId,
        sourceCommitOid: SECOND_BASE_OID,
        expectedRevision: 1,
        expectedCheckpointOid: null,
      })
    ).toThrow("Rebase conflict: shared.txt");
    expect(
      new TextDecoder().decode(model.projectFiles(workspace.workspaceId).get("shared.txt"))
    ).toBe("proposal");
  });

  it("keeps operation replay and branching pinned to their base generation across rebase", async () => {
    const model = new WorkspaceProbeModel();
    const first = model.createRepository({
      namespace: "team",
      name: "first",
      basePackBytes: 100,
      baseFiles: files(["x", "A"]),
      baseCommitOid: BASE_OID,
    });
    const second = model.createRepository({
      namespace: "team",
      name: "second",
      basePackBytes: 120,
      baseFiles: files(["x", "C"], ["target-only", "target"]),
      baseCommitOid: SECOND_BASE_OID,
    });
    const workspace = model.createWorkspace({
      sourceRepositoryId: first.repositoryId,
      sourceCommitOid: BASE_OID,
      strategy: "operation-backed",
    });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 0,
      operation: { id: "to-b", actor: "agent", path: "x", bytes: encoder.encode("B") },
    });
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 1,
      operation: { id: "back-to-a", actor: "agent", path: "x", bytes: encoder.encode("A") },
    });
    const reviewed = await model.checkpoint({
      workspaceId: workspace.workspaceId,
      expectedRevision: 2,
      committedAtSeconds: 1_786_742_400,
      message: "before rebase",
    });
    let interruptedBranchId = "";
    try {
      model.branchFromOperation({
        workspaceId: workspace.workspaceId,
        operationId: "back-to-a",
        requestId: "historical-generation",
        interruptAfterCreate: true,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "";
      interruptedBranchId = message.replace("Synthetic branch interruption: ", "");
    }
    expect(interruptedBranchId).toMatch(/^workspace_/);
    model.retainForReview(workspace.repositoryId, reviewed.commitOid);
    model.rebaseWorkspace({
      workspaceId: workspace.workspaceId,
      sourceRepositoryId: second.repositoryId,
      sourceCommitOid: SECOND_BASE_OID,
      expectedRevision: 2,
      expectedCheckpointOid: reviewed.commitOid,
    });
    expect(new TextDecoder().decode(model.projectFiles(workspace.workspaceId).get("x"))).toBe("C");
    const historical = model.branchFromOperation({
      workspaceId: workspace.workspaceId,
      operationId: "back-to-a",
      requestId: "historical-generation",
    });
    expect(historical).toBe(interruptedBranchId);
    expect(new TextDecoder().decode(model.projectFiles(historical).get("x"))).toBe("A");
    expect(model.projectFiles(historical).has("target-only")).toBe(false);
    await model.appendOperation({
      workspaceId: workspace.workspaceId,
      expectedRevision: 2,
      operation: { id: "new-tail", actor: "agent", path: "tail", bytes: encoder.encode("tail") },
    });
    const current = model.branchFromOperation({
      workspaceId: workspace.workspaceId,
      operationId: "new-tail",
      requestId: "current-generation",
    });
    expect(new TextDecoder().decode(model.projectFiles(current).get("x"))).toBe("C");
    expect(model.projectFiles(current).has("target-only")).toBe(true);
  });

  it("rebases every independent physical model onto an exact imported generation", async () => {
    const strategies: WorkspaceStrategy[] = [
      "full-copy",
      "shared-catalog",
      "copy-on-write",
      "operation-backed",
    ];
    for (const strategy of strategies) {
      const model = new WorkspaceProbeModel();
      const first = model.createRepository({
        namespace: "team",
        name: "first",
        basePackBytes: 100,
        baseFiles: files(["old.txt", "old"]),
        baseCommitOid: BASE_OID,
      });
      const second = model.createRepository({
        namespace: "team",
        name: "second",
        basePackBytes: 120,
        baseFiles: files(["new.txt", "new"]),
        baseCommitOid: SECOND_BASE_OID,
      });
      const workspace = model.createWorkspace({
        sourceRepositoryId: first.repositoryId,
        sourceCommitOid: BASE_OID,
        strategy,
      });
      model.rebaseWorkspace({
        workspaceId: workspace.workspaceId,
        sourceRepositoryId: second.repositoryId,
        sourceCommitOid: SECOND_BASE_OID,
        expectedRevision: 0,
        expectedCheckpointOid: null,
      });
      if (strategy === "operation-backed") {
        await model.appendOperation({
          workspaceId: workspace.workspaceId,
          expectedRevision: 0,
          operation: {
            id: "post-rebase",
            actor: "agent",
            path: "edit.txt",
            bytes: encoder.encode("edit"),
          },
        });
      } else {
        model.replaceFile({
          workspaceId: workspace.workspaceId,
          expectedRevision: 0,
          path: "edit.txt",
          bytes: encoder.encode("edit"),
        });
      }
      const checkpoint = await model.checkpoint({
        workspaceId: workspace.workspaceId,
        expectedRevision: 1,
        committedAtSeconds: 1_786_742_400,
        message: `rebase-${strategy}`,
      });
      const target = model.createRepository({ namespace: "team", name: "target" });
      model.deleteRepository(second.repositoryId);
      const promotion = model.beginPromotion({
        workspaceId: workspace.workspaceId,
        targetRepositoryId: target.repositoryId,
        expectedTargetRef: null,
        checkpointOid: checkpoint.commitOid,
      });
      model.resumePromotion(promotion);
      model.deleteWorkspace(workspace.workspaceId);
      expect(model.repositoryReadable(target.repositoryId, checkpoint.commitOid)).toBe(true);
    }
  });
});
