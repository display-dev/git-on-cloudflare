import { afterEach, describe, expect, it } from "vitest";
import { createExecutionContext } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";

import { getRepoStub, zeroOid } from "@/worker/common";
import { __test as nativeReceiveTest } from "@/worker/do/repo/nativeReceive";
import { __test as receiveCatalogTest } from "@/worker/do/repo/catalog/receive";
import { clearExpiredLeases } from "@/worker/do/repo/catalog/leases";
import { RECOVERY_ESCALATION_ATTEMPTS, recoveryRetryDelayMs } from "@/worker/do/repo/scheduler";
import type { RepoDurableObject } from "@/worker/do/repo/repoDO";
import { getDb, upsertPackCatalogRow } from "@/worker/do/repo/db";
import {
  isNativeReceiveTerminal,
  type NativeReceiveOperationView,
} from "@/worker/git/nativeReceive/types";
import { concatChunks, flushPkt, pktLine } from "@/worker/git/core";
import { encodeGitObject } from "@/worker/git/core/objects";
import { SubrequestLimiter } from "@/worker/git/operations/limits";
import { findOidIndex, parseIdxView } from "@/worker/git/object-store/idxView";
import {
  getPackRefObjectType,
  getPackRefRefsAt,
  parsePackRefView,
} from "@/worker/git/pack/refIndex";
import { handleStreamingReceivePackPOST } from "@/worker/git/receive/streamReceivePack";
import {
  doPrefix,
  nativeReceiveInputPackKey,
  packIndexKey,
  packRefsKey,
  repositoryImportPackKey,
  r2PackKey,
} from "@/worker/keys";
import { buildPack } from "./util/git-pack";
import { buildTreePayload } from "./util/packed-repo";
import { setupRepoForTests } from "./util/repoSeed";
import { decodeReportStatus } from "./util/streaming-helpers";
import {
  runAlarmWithRetry,
  runDOWithRetry,
  toRequestBody,
  uniqueRepoId,
  withEnvOverrides,
} from "./util/test-helpers";

const targetOID = "1".repeat(40);
const nativeFixture = {
  blobOid: "26ae93e9c4fbd1d87ff9bc19448a4049f699a08f",
  commitOid: "d3a543e82192d58c0dc595fcd32fe8193d0fac4a",
  pack: "UEFDSwAAAAIAAAADkgt4nJ3LQQrCMBAAwHtekbsgiZu0G5DiwSf4gU2y4kLSlnYV/b2+wcvcRjdmG4aE8Y7IGcKQT7kETqViBMACFVxJ1flYyNBTH8tmr7KvjT72xrvas/688Jv62vgo84ua1Mn6EUeP4DzYgwPnTFl6F1X+sxuZRYWa+QJ4mDdepQJ4nDM0MDAzMVEIcnV08XXVy01hUFs3+eWR3xdv1P/cI+nS5eD5beaCfgD18hAXO3icy0ssySxLVUjPLOECABnsA/ZKwmTJal8VK3r4dPvK8+dNZP2MTQ==",
  idx: "/3RPYwAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAABAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAgAAAAIAAAACAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADAAAAAwAAAAMAAAADJq6T6cT70dh/+bwZRIpASfaZoI9GmF+I6zRrK8TpzYUzjD0wydAVytOlQ+ghktWMDcWV/NMv6Bk9D6xKkpnGQn2KghJTAAnvAAAAugAAAIoAAAAMSsJkyWpfFSt6+HT7yvPnTWT9jE1jNPVbnc7P+2Xiz98/A+MrBbZf9Q==",
  refs: "UFJFRgAAAAEAAAADAAAAAAAAAOJKwmTJal8VK3r4dPvK8+dNZP2MTWM09Vudzs/7ZeLP3z8D4ysFtl/1AwIBAAAAAAAAAAAAAAABAAAAAiauk+nE+9HYf/m8GUSKQEn2maCPRphfiOs0ayvE6c2FM4w9MMnQFco=",
} as const;

function decodeBase64(value: string): Uint8Array {
  return Uint8Array.from(atob(value), (character) => character.charCodeAt(0));
}

function bridgeUrl(key: string): string {
  const encoded = btoa(String.fromCharCode(...new TextEncoder().encode(key)))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
  return `https://bridge.invalid/r2/${encoded}`;
}

function streamedBody(bytes: Uint8Array): ReadableStream<Uint8Array> {
  const split = Math.max(1, Math.floor(bytes.byteLength / 2));
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes.slice(0, split));
      if (split < bytes.byteLength) controller.enqueue(bytes.slice(split));
      controller.close();
    },
  });
}

afterEach(() => {
  nativeReceiveTest.reset();
  receiveCatalogTest.reset();
});

async function postImport(args: {
  owner: string;
  repo: string;
  operationId: string;
  inputBytes: number;
  actor?: string;
  inputEtag: string;
  oldOid?: string;
  newOid?: string;
  idempotencyKey?: string;
}): Promise<Response> {
  const body = JSON.stringify({
    inputBytes: args.inputBytes,
    inputEtag: args.inputEtag,
    commands: [
      {
        oldOid: args.oldOid ?? zeroOid(),
        newOid: args.newOid ?? targetOID,
        ref: "refs/heads/main",
      },
    ],
    actor: args.actor ?? "native-import-test",
    idempotencyKey: args.idempotencyKey ?? "import-request-1",
  });
  return await workerExports.default.fetch(
    `https://example.com/_internal/imports/${args.owner}/${args.repo}/${args.operationId}`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}`,
        "Content-Type": "application/json",
        "Content-Length": String(new TextEncoder().encode(body).byteLength),
      },
      body,
    }
  );
}

async function runOperationToTerminal(
  stub: DurableObjectStub<RepoDurableObject>,
  operationId: string
): Promise<NativeReceiveOperationView> {
  for (let attempt = 0; attempt < 100; attempt++) {
    const operation = await stub.runNativeReceiveOperation(operationId);
    if (operation && isNativeReceiveTerminal(operation.state)) return operation;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error("native receive did not reach a terminal state");
}

describe("durable native receive and import", () => {
  it("backs repeated finalization recovery away from a one-hertz retry loop", () => {
    expect(
      Array.from({ length: RECOVERY_ESCALATION_ATTEMPTS + 2 }, (_, index) =>
        recoveryRetryDelayMs(index + 1)
      )
    ).toEqual([1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]);
  });

  it("streams exact Container output through the workerd R2 bridge", async () => {
    const allowedKey = "do/test/objects/pack/output.pack";
    const bridge = workerExports.RepositoryContainerBridge({
      props: {
        operationId: "bridge-fixed-length",
        readKeys: [],
        writeKeys: [{ key: allowedKey, maxBytes: 8 }],
      },
    });
    const exact = new Uint8Array([1, 2, 3, 4]);
    const written = await bridge.fetch(
      new Request(bridgeUrl(allowedKey), {
        method: "PUT",
        headers: { "Content-Length": String(exact.byteLength) },
        body: streamedBody(exact),
      })
    );
    expect(written.status).toBe(204);
    expect(new Uint8Array(await (await env.REPO_BUCKET.get(allowedKey))!.arrayBuffer())).toEqual(
      exact
    );

    const denied = await bridge.fetch(
      new Request(bridgeUrl("do/test/objects/pack/denied.pack"), {
        method: "PUT",
        headers: { "Content-Length": "1" },
        body: streamedBody(new Uint8Array([1])),
      })
    );
    expect(denied.status).toBe(403);

    const short = await bridge.fetch(
      new Request(bridgeUrl(allowedKey), {
        method: "PUT",
        headers: { "Content-Length": "5" },
        body: streamedBody(exact),
      })
    );
    expect(short.status).toBe(400);
    expect(await env.REPO_BUCKET.head(allowedKey)).toBeNull();

    const oversized = await bridge.fetch(
      new Request(bridgeUrl(allowedKey), {
        method: "PUT",
        headers: { "Content-Length": "3" },
        body: streamedBody(exact),
      })
    );
    expect(oversized.status).toBe(400);
    expect(await env.REPO_BUCKET.head(allowedKey)).toBeNull();
  });

  it("reads native Git pack, index, and PREF artifacts through the Worker object path", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-cross-runtime");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    const pack = decodeBase64(nativeFixture.pack);
    const idx = decodeBase64(nativeFixture.idx);
    const refs = decodeBase64(nativeFixture.refs);
    const packKey = r2PackKey(doPrefix(stub.id.toString()), "pack-go-native-fixture.pack");
    const idxView = parseIdxView(packKey, idx, pack.byteLength);
    if (!idxView) throw new Error("native Git fixture index did not parse");
    const refsResult = parsePackRefView(packKey, refs, idxView);
    expect(refsResult.type).toBe("Ready");
    if (refsResult.type !== "Ready") throw new Error("native Git PREF sidecar did not parse");
    const commitIndex = findOidIndex(idxView, nativeFixture.commitOid);
    const blobIndex = findOidIndex(idxView, nativeFixture.blobOid);
    expect(commitIndex).toBeGreaterThanOrEqual(0);
    expect(blobIndex).toBeGreaterThanOrEqual(0);
    expect(getPackRefObjectType(refsResult.view, commitIndex)).toBe("commit");
    expect(getPackRefObjectType(refsResult.view, blobIndex)).toBe("blob");
    const commitRefs = getPackRefRefsAt(refsResult.view, commitIndex);
    expect(commitRefs).toHaveLength(1);
    const treeOid = commitRefs[0];
    if (!treeOid) throw new Error("native Git commit did not reference a tree");
    const treeIndex = findOidIndex(idxView, treeOid);
    expect(treeIndex).toBeGreaterThanOrEqual(0);
    expect(getPackRefObjectType(refsResult.view, treeIndex)).toBe("tree");
    expect(getPackRefRefsAt(refsResult.view, treeIndex)).toEqual([nativeFixture.blobOid]);
    expect(getPackRefRefsAt(refsResult.view, blobIndex)).toEqual([]);
    await env.REPO_BUCKET.put(packKey, pack);
    await env.REPO_BUCKET.put(packIndexKey(packKey), idx);
    await env.REPO_BUCKET.put(packRefsKey(packKey), refs);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await upsertPackCatalogRow(getDb(state.storage), {
          packKey,
          kind: "receive",
          state: "active",
          tier: 0,
          seqLo: 1,
          seqHi: 1,
          objectCount: 3,
          packBytes: pack.byteLength,
          idxBytes: idx.byteLength,
          createdAt: 1,
          supersededBy: null,
        });
        await state.storage.put("packsetVersion", 1);
        await state.storage.put("nextPackSeq", 2);
      }
    );
    expect(await stub.setRefs([{ name: "refs/heads/main", oid: nativeFixture.commitOid }])).toBe(
      true
    );
    expect(await stub.setHead({ target: "refs/heads/main", oid: nativeFixture.commitOid })).toBe(
      true
    );

    const response = await workerExports.default.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${nativeFixture.blobOid}&name=README.md`
    );
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("native git\n");
  });

  it("routes a gated Smart HTTP push through the durable native operation", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-smart-http");
    const seeded = await setupRepoForTests(env, owner, repo);
    const blobPayload = new TextEncoder().encode("native smart HTTP\n");
    const blob = await encodeGitObject("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "README.md", oid: blob.oid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const author = "Display <display@example.invalid> 0 +0000";
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\nauthor ${author}\ncommitter ${author}\n\nnative\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: commitPayload },
    ]);
    const body = concatChunks([
      pktLine(`${zeroOid()} ${commit.oid} refs/heads/main\0 report-status ofs-delta agent=test\n`),
      flushPkt(),
      pack,
    ]);
    nativeReceiveTest.setNativeProcessor(async ({ request }) => {
      await env.REPO_BUCKET.put(request.outputPackKey, pack);
      await env.REPO_BUCKET.put(request.outputIdxKey, new Uint8Array([2]));
      await env.REPO_BUCKET.put(request.outputRefsKey, new Uint8Array([3]));
      return {
        operationId: request.operationId,
        packBytes: pack.byteLength,
        idxBytes: 1,
        refsBytes: 1,
        objectCount: 3,
        packSha1: "c".repeat(40),
        elapsedMs: 2,
        scratchBytes: pack.byteLength + 2,
      };
    });

    const response = await handleStreamingReceivePackPOST(
      { ...env, NATIVE_RECEIVE_CONTAINER: "1" },
      seeded.doName,
      new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
        method: "POST",
        headers: { "Content-Type": "application/x-git-receive-pack-request" },
        body: toRequestBody(body),
      }),
      createExecutionContext(),
      { limiter: new SubrequestLimiter(900) }
    );
    expect(response.status).toBe(200);
    expect(decodeReportStatus(new Uint8Array(await response.arrayBuffer()))).toContain(
      "ok refs/heads/main"
    );
    expect((await getRepoStub(env, seeded.doName).listRefs())[0]?.oid).toBe(commit.oid);
  });

  it("preserves a staged Smart HTTP input when the enqueue response is lost", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-lost-enqueue");
    const seeded = await setupRepoForTests(env, owner, repo);
    const blobPayload = new TextEncoder().encode("lost enqueue response\n");
    const blob = await encodeGitObject("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "README.md", oid: blob.oid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const author = "Display <display@example.invalid> 0 +0000";
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\nauthor ${author}\ncommitter ${author}\n\nlost enqueue\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: commitPayload },
    ]);
    const body = concatChunks([
      pktLine(`${zeroOid()} ${commit.oid} refs/heads/main\0 report-status agent=test\n`),
      flushPkt(),
      pack,
    ]);
    nativeReceiveTest.useManualWakeups();
    nativeReceiveTest.failNextAfterEnqueueStore();
    nativeReceiveTest.setNativeProcessor(async ({ request }) => {
      await env.REPO_BUCKET.put(request.outputPackKey, pack);
      await env.REPO_BUCKET.put(request.outputIdxKey, new Uint8Array([2]));
      await env.REPO_BUCKET.put(request.outputRefsKey, new Uint8Array([3]));
      return {
        operationId: request.operationId,
        packBytes: pack.byteLength,
        idxBytes: 1,
        refsBytes: 1,
        objectCount: 3,
        packSha1: "f".repeat(40),
        elapsedMs: 1,
        scratchBytes: pack.byteLength + 2,
      };
    });

    const response = await handleStreamingReceivePackPOST(
      { ...env, NATIVE_RECEIVE_CONTAINER: "1" },
      seeded.doName,
      new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
        method: "POST",
        headers: { "Content-Type": "application/x-git-receive-pack-request" },
        body: toRequestBody(body),
      }),
      createExecutionContext(),
      { limiter: new SubrequestLimiter(900) }
    );
    expect(response.status).toBe(503);

    const stub = getRepoStub(env, seeded.doName);
    const operationIds = await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        return (await state.storage.get<string[]>("nativeReceiveOperationIndex")) ?? [];
      }
    );
    expect(operationIds).toHaveLength(1);
    const operationId = operationIds[0];
    if (!operationId) throw new Error("expected durable native operation");
    expect((await runOperationToTerminal(stub, operationId)).state).toBe("committed");
    expect((await stub.listRefs()).find((ref) => ref.name === "refs/heads/main")?.oid).toBe(
      commit.oid
    );
  });

  it("does not let an expired ordinary receive lease strand admin ref mutation", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-expired-admin-lease");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.put("receiveLease", {
          token: "expired-ordinary-receive",
          createdAt: 1,
          expiresAt: 2,
        });
      }
    );

    expect(await stub.setRefs([{ name: "refs/heads/main", oid: targetOID }])).toBe(true);
    expect(await stub.setHead({ target: "refs/heads/main", oid: targetOID })).toBe(true);
    expect(await stub.getRepoActivity()).toBeNull();
  });

  it("removes an unowned native input when an ambiguous receive lease expires", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-expired-orphan-input");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    const token = "orphaned-native-input";
    const inputKey = nativeReceiveInputPackKey(doPrefix(stub.id.toString()), token);
    await env.REPO_BUCKET.put(inputKey, new Uint8Array([1, 2, 3]));
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        await state.storage.put("receiveLease", {
          token,
          createdAt: 1,
          expiresAt: 2,
        });
        await clearExpiredLeases(state, env, undefined, 3);
        expect(await state.storage.get("receiveLease")).toBeUndefined();
      }
    );
    expect(await env.REPO_BUCKET.head(inputKey)).toBeNull();
  });

  it("stops blocked native processing before repository deletion can become ready", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-delete-fence");
    const seeded = await setupRepoForTests(env, owner, repo);
    const operationId = "operation-delete-fence";
    const input = new Uint8Array([1, 2, 3]);
    const inputObject = await env.REPO_BUCKET.put(
      repositoryImportPackKey(seeded.doName, operationId),
      input
    );
    if (!inputObject) throw new Error("failed to stage deletion-fence input");
    nativeReceiveTest.useManualWakeups();
    let attemptedOutputPackKey: string | undefined;
    expect(
      (
        await postImport({
          owner,
          repo,
          operationId,
          inputBytes: input.byteLength,
          inputEtag: inputObject.etag,
        })
      ).status
    ).toBe(202);
    const stub = getRepoStub(env, seeded.doName);
    await runDOWithRetry(
      () => stub,
      async (instance, state) => {
        let markProcessorStarted: (() => void) | undefined;
        const processorStarted = new Promise<void>((resolve) => {
          markProcessorStarted = resolve;
        });
        nativeReceiveTest.setNativeProcessor(async ({ request, signal }) => {
          attemptedOutputPackKey = request.outputPackKey;
          markProcessorStarted?.();
          await new Promise<void>((_resolve, reject) => {
            if (signal.aborted) {
              reject(new Error("repository deletion stopped native processing"));
              return;
            }
            signal.addEventListener(
              "abort",
              () => reject(new Error("repository deletion stopped native processing")),
              { once: true }
            );
          });
          await env.REPO_BUCKET.put(request.outputPackKey, new Uint8Array([9]));
          throw new Error("blocked processor unexpectedly resumed");
        });
        const processing = instance.runNativeReceiveOperation(operationId);
        await processorStarted;
        expect((await instance.beginRepositoryDeletion()).ready).toBe(false);
        expect((await processing)?.state).toBe("staged");
        const lease = await state.storage.get<{
          createdAt: number;
          expiresAt: number;
          token: string;
        }>("receiveLease");
        if (!lease) throw new Error("expected receive lease after processor cancellation");
        await state.storage.put("receiveLease", { ...lease, expiresAt: 1 });
        expect((await instance.beginRepositoryDeletion()).ready).toBe(true);
        expect((await instance.getNativeReceiveOperation(operationId))?.state).toBe("staged");
      }
    );
    if (!attemptedOutputPackKey) throw new Error("expected native output key");
    expect(await env.REPO_BUCKET.head(attemptedOutputPackKey)).toBeNull();
  });

  it("rolls a generic finalize intent forward from the alarm after caller loss", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-generic-finalize-intent");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    const begun = await stub.beginReceive();
    if (!begun.ok) throw new Error("expected receive lease");
    const commands = [{ oldOid: zeroOid(), newOid: targetOID, ref: "refs/heads/main" }];
    const stagedPack = {
      packKey: r2PackKey(doPrefix(stub.id.toString()), "pack-generic-intent.pack"),
      packBytes: 3,
      idxBytes: 2,
      objectCount: 1,
    };
    receiveCatalogTest.failNextAfterCatalogUpsert();
    await runDOWithRetry(
      () => stub,
      async (instance) => {
        await expect(
          instance.finalizeReceive({ token: begun.lease.token, commands, stagedPack })
        ).rejects.toThrow("injected post-catalog-upsert receive failure");
      }
    );
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => {
        const lease = await state.storage.get<{
          createdAt: number;
          expiresAt: number;
          token: string;
        }>("receiveLease");
        if (!lease) throw new Error("expected recovery lease");
        await state.storage.put("receiveLease", { ...lease, expiresAt: 1 });
      }
    );

    expect(await stub.setRefs([{ name: "refs/heads/other", oid: "2".repeat(40) }])).toBe(false);
    expect(await stub.setHead({ target: "refs/heads/other", oid: "2".repeat(40) })).toBe(false);
    expect((await stub.beginReceive()).ok).toBe(false);
    expect(await runAlarmWithRetry(() => stub)).toBe(true);
    expect((await stub.listRefs()).find((ref) => ref.name === "refs/heads/main")?.oid).toBe(
      targetOID
    );
  });

  it("authenticates prepare before lookup and returns only the exact staged key", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-import-prepare");
    const seeded = await setupRepoForTests(env, owner, repo);
    const path = `https://example.com/_internal/imports/${owner}/${repo}/operation-prepare/prepare`;

    const unauthorized = await workerExports.default.fetch(path, { method: "POST" });
    expect(unauthorized.status).toBe(401);

    const prepared = await workerExports.default.fetch(path, {
      method: "POST",
      headers: { Authorization: `Bearer ${env.INGESTION_RPC_TOKEN}` },
    });
    expect(prepared.status).toBe(200);
    expect(await prepared.json()).toEqual({
      operationId: "operation-prepare",
      uploadKey: repositoryImportPackKey(seeded.doName, "operation-prepare"),
      maximumBytes: 5_000_000_000,
    });
  });

  it("commits a staged import once and returns the same durable outcome on replay", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-import-commit");
    const seeded = await setupRepoForTests(env, owner, repo);
    const operationId = "operation-commit";
    const input = new Uint8Array([1, 2, 3, 4]);
    const inputKey = repositoryImportPackKey(seeded.doName, operationId);
    const inputObject = await env.REPO_BUCKET.put(inputKey, input);
    if (!inputObject) throw new Error("failed to stage test import");

    nativeReceiveTest.setNativeProcessor(async ({ request }) => {
      const pack = new Uint8Array([9, 8, 7]);
      const idx = new Uint8Array([6, 5]);
      const refs = new Uint8Array([4]);
      await env.REPO_BUCKET.put(request.outputPackKey, pack);
      await env.REPO_BUCKET.put(request.outputIdxKey, idx);
      await env.REPO_BUCKET.put(request.outputRefsKey, refs);
      return {
        operationId: request.operationId,
        packBytes: pack.byteLength,
        idxBytes: idx.byteLength,
        refsBytes: refs.byteLength,
        objectCount: 1,
        packSha1: "a".repeat(40),
        elapsedMs: 5,
        scratchBytes: 6,
      };
    });

    // Suppress Queue/alarm delivery so each injected crash boundary is driven
    // and asserted by the calls below rather than consumed in the background.
    nativeReceiveTest.useManualWakeups();
    const accepted = await postImport({
      owner,
      repo,
      operationId,
      inputBytes: input.byteLength,
      inputEtag: inputObject.etag,
    });
    expect(accepted.status).toBe(202);

    const stub = getRepoStub(env, seeded.doName);
    // Simulate resets before finalization, after catalog activation, and
    // after the authoritative outcome. Every partial state must roll forward.
    nativeReceiveTest.pauseNextBeforeFinalization();
    const terminal: NativeReceiveOperationView | null = await withEnvOverrides(
      env,
      { SNAPSHOT_EVENT_PROBE: "1" },
      async () => {
        const ready = await stub.runNativeReceiveOperation(operationId);
        expect(ready?.state).toBe("ready");
        expect(await stub.setRefs([{ name: "refs/heads/other", oid: "2".repeat(40) }])).toBe(false);
        expect(await stub.setHead({ target: "refs/heads/other", oid: "2".repeat(40) })).toBe(false);

        receiveCatalogTest.failNextAfterCatalogUpsert();
        const afterUpsert = await stub.runNativeReceiveOperation(operationId);
        expect(afterUpsert?.state).toBe("finalizing");
        expect(receiveCatalogTest.consumedFailureCounts().catalogUpsert).toBe(1);

        receiveCatalogTest.failNextAfterCatalogActivation();
        const finalizing = await stub.runNativeReceiveOperation(operationId);
        expect(finalizing?.state).toBe("finalizing");
        expect(receiveCatalogTest.consumedFailureCounts().catalogActivation).toBe(1);

        // Simulate a reset after lease expiry/cleanup while the durable
        // finalize intent remains. Recovery must recreate the completion
        // fence from the intent rather than strand the operation.
        await runDOWithRetry(
          () => stub,
          async (_instance, state) => await state.storage.delete("receiveLease")
        );

        receiveCatalogTest.failNextAfterOutcomeStore();
        const afterOutcome = await stub.runNativeReceiveOperation(operationId);
        expect(afterOutcome?.state).toBe("finalizing");
        expect(receiveCatalogTest.consumedFailureCounts().outcomeStore).toBe(1);
        return await stub.runNativeReceiveOperation(operationId);
      }
    );
    expect(terminal?.state).toBe("committed");
    expect((await stub.listRefs()).find((ref) => ref.name === "refs/heads/main")?.oid).toBe(
      targetOID
    );
    expect(await stub.getActivePackCatalog()).toMatchObject([{ seqLo: 1, seqHi: 1 }]);
    expect(await env.REPO_BUCKET.head(inputKey)).toBeNull();
    expect(await stub.getRepoActivity()).toBeNull();
    expect(await stub.listAcceptedWrites()).toMatchObject([
      {
        fact: {
          repositoryId: seeded.repositoryId,
          sourceSurface: "import",
          ref: "refs/heads/main",
          beforeSha: zeroOid(),
          afterSha: targetOID,
        },
      },
    ]);

    const replay = await postImport({
      owner,
      repo,
      operationId,
      inputBytes: input.byteLength,
      inputEtag: inputObject.etag,
    });
    expect(replay.status).toBe(200);
    expect(await replay.json()).toMatchObject({ state: "committed" });

    const conflict = await postImport({
      owner,
      repo,
      operationId,
      inputBytes: input.byteLength,
      inputEtag: inputObject.etag,
      actor: "different-actor",
    });
    expect(conflict.status).toBe(409);
  });

  it("retries a transient native processor failure without losing the staged input", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-import-retry");
    const seeded = await setupRepoForTests(env, owner, repo);
    const operationId = "operation-retry";
    const input = new Uint8Array([1, 2, 3, 4]);
    const inputKey = repositoryImportPackKey(seeded.doName, operationId);
    const inputObject = await env.REPO_BUCKET.put(inputKey, input);
    if (!inputObject) throw new Error("failed to stage test import");
    let calls = 0;
    nativeReceiveTest.setNativeProcessor(async ({ request }) => {
      calls++;
      if (calls === 1) throw new Error("synthetic container restart");
      await env.REPO_BUCKET.put(request.outputPackKey, new Uint8Array([1]));
      await env.REPO_BUCKET.put(request.outputIdxKey, new Uint8Array([2]));
      await env.REPO_BUCKET.put(request.outputRefsKey, new Uint8Array([3]));
      return {
        operationId: request.operationId,
        packBytes: 1,
        idxBytes: 1,
        refsBytes: 1,
        objectCount: 1,
        packSha1: "b".repeat(40),
        elapsedMs: 1,
        scratchBytes: 3,
      };
    });

    expect(
      (
        await postImport({
          owner,
          repo,
          operationId,
          inputBytes: input.byteLength,
          inputEtag: inputObject.etag,
        })
      ).status
    ).toBe(202);
    const stub = getRepoStub(env, seeded.doName);
    const first = await stub.runNativeReceiveOperation(operationId);
    expect(first?.state).toBe("staged");
    expect(await env.REPO_BUCKET.head(inputKey)).not.toBeNull();
    const second = await stub.runNativeReceiveOperation(operationId);
    expect(second?.state).toBe("committed");
    expect(second?.attempts).toBe(2);
    expect(await env.REPO_BUCKET.head(inputKey)).toBeNull();
  });

  it("rejects a same-size staged import overwrite by immutable etag", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-import-etag");
    const seeded = await setupRepoForTests(env, owner, repo);
    const operationId = "operation-etag";
    const inputKey = repositoryImportPackKey(seeded.doName, operationId);
    const original = await env.REPO_BUCKET.put(inputKey, new Uint8Array([1, 2, 3, 4]));
    if (!original) throw new Error("failed to stage original import");
    await env.REPO_BUCKET.put(inputKey, new Uint8Array([4, 3, 2, 1]));

    const response = await postImport({
      owner,
      repo,
      operationId,
      inputBytes: 4,
      inputEtag: original.etag,
    });
    expect(response.status).toBe(409);
    expect(await getRepoStub(env, seeded.doName).getRepoActivity()).toBeNull();
  });

  it("claims a durable operation once when duplicate wakeups race", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-import-claim");
    const seeded = await setupRepoForTests(env, owner, repo);
    const operationId = "operation-claim";
    const input = new Uint8Array([1, 2, 3, 4]);
    const inputKey = repositoryImportPackKey(seeded.doName, operationId);
    const inputObject = await env.REPO_BUCKET.put(inputKey, input);
    if (!inputObject) throw new Error("failed to stage test import");
    let calls = 0;
    let releaseProcessor: (() => void) | undefined;
    const processorReleased = new Promise<void>((resolve) => {
      releaseProcessor = resolve;
    });
    nativeReceiveTest.setNativeProcessor(async ({ request }) => {
      calls++;
      await processorReleased;
      await env.REPO_BUCKET.put(request.outputPackKey, new Uint8Array([1]));
      await env.REPO_BUCKET.put(request.outputIdxKey, new Uint8Array([2]));
      await env.REPO_BUCKET.put(request.outputRefsKey, new Uint8Array([3]));
      return {
        operationId: request.operationId,
        packBytes: 1,
        idxBytes: 1,
        refsBytes: 1,
        objectCount: 1,
        packSha1: "d".repeat(40),
        elapsedMs: 1,
        scratchBytes: 3,
      };
    });
    expect(
      (
        await postImport({
          owner,
          repo,
          operationId,
          inputBytes: input.byteLength,
          inputEtag: inputObject.etag,
        })
      ).status
    ).toBe(202);

    const stub = getRepoStub(env, seeded.doName);
    const first = stub.runNativeReceiveOperation(operationId);
    await new Promise((resolve) => setTimeout(resolve, 0));
    await runDOWithRetry(
      () => stub,
      async (_instance, state) => await state.storage.deleteAlarm()
    );
    const duplicate = await stub.runNativeReceiveOperation(operationId);
    expect(duplicate?.state).toBe("processing");
    expect(calls).toBe(1);
    expect(
      await runDOWithRetry(
        () => stub,
        async (_instance, state) => await state.storage.getAlarm()
      )
    ).not.toBeNull();
    releaseProcessor?.();
    expect((await first)?.state).toBe("committed");
    expect(calls).toBe(1);
  });

  it("commits 130 sequential checkpoint receives without losing CAS or operation liveness", async () => {
    const owner = "o";
    const repo = uniqueRepoId("native-checkpoint-churn");
    const seeded = await setupRepoForTests(env, owner, repo);
    const stub = getRepoStub(env, seeded.doName);
    nativeReceiveTest.setNativeProcessor(async ({ request }) => {
      await env.REPO_BUCKET.put(request.outputPackKey, new Uint8Array([1]));
      await env.REPO_BUCKET.put(request.outputIdxKey, new Uint8Array([2]));
      await env.REPO_BUCKET.put(request.outputRefsKey, new Uint8Array([3]));
      return {
        operationId: request.operationId,
        packBytes: 1,
        idxBytes: 1,
        refsBytes: 1,
        objectCount: 1,
        packSha1: "e".repeat(40),
        elapsedMs: 1,
        scratchBytes: 3,
      };
    });

    let previous = zeroOid();
    for (let checkpoint = 1; checkpoint <= 130; checkpoint++) {
      const operationId = `checkpoint-${checkpoint}`;
      const next = checkpoint.toString(16).padStart(40, "0");
      const input = new Uint8Array([checkpoint % 251]);
      const inputObject = await env.REPO_BUCKET.put(
        repositoryImportPackKey(seeded.doName, operationId),
        input
      );
      if (!inputObject) throw new Error("failed to stage checkpoint import");
      const accepted = await postImport({
        owner,
        repo,
        operationId,
        inputBytes: input.byteLength,
        inputEtag: inputObject.etag,
        oldOid: previous,
        newOid: next,
        idempotencyKey: operationId,
      });
      expect(accepted.status).toBe(202);
      expect((await runOperationToTerminal(stub, operationId)).state).toBe("committed");
      previous = next;
    }

    expect((await stub.listRefs()).find((ref) => ref.name === "refs/heads/main")?.oid).toBe(
      previous
    );
    expect(await stub.getNativeReceiveOperation("checkpoint-1")).toBeNull();
    expect((await stub.getNativeReceiveOperation("checkpoint-130"))?.state).toBe("committed");
    const catalogBeforeReuse = await stub.getActivePackCatalog();
    expect(catalogBeforeReuse).toHaveLength(130);
    const originalPack = catalogBeforeReuse.find((pack) => pack.packKey.includes("checkpoint-1-"));
    if (!originalPack) throw new Error("expected original checkpoint pack");

    const reusedInput = new Uint8Array([252]);
    const reusedInputObject = await env.REPO_BUCKET.put(
      repositoryImportPackKey(seeded.doName, "checkpoint-1"),
      reusedInput
    );
    if (!reusedInputObject) throw new Error("failed to stage reused operation id");
    const reusedHead = "131".padStart(40, "0");
    expect(
      (
        await postImport({
          owner,
          repo,
          operationId: "checkpoint-1",
          inputBytes: reusedInput.byteLength,
          inputEtag: reusedInputObject.etag,
          oldOid: previous,
          newOid: reusedHead,
          idempotencyKey: "checkpoint-1-reused",
        })
      ).status
    ).toBe(202);
    expect((await runOperationToTerminal(stub, "checkpoint-1")).state).toBe("committed");
    const catalogAfterReuse = await stub.getActivePackCatalog();
    expect(catalogAfterReuse).toHaveLength(131);
    expect(catalogAfterReuse.some((pack) => pack.packKey === originalPack.packKey)).toBe(true);
    expect(
      catalogAfterReuse
        .filter((pack) => pack.packKey.includes("checkpoint-1-"))
        .map((pack) => pack.packKey)
    ).toHaveLength(2);
  }, 30_000);
});
