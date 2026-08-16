import { describe, expect, it } from "vitest";
import { createExecutionContext, env, waitOnExecutionContext } from "cloudflare:test";

import { getRepoStub } from "@/worker/common";
import { concatChunks, flushPkt, pktLine } from "@/worker/git/core";
import { SubrequestLimiter } from "@/worker/git/operations/limits";
import { getHeadAndRefs, readPath } from "@/worker/git/operations/read";
import {
  handleStreamingReceivePackPOST,
  type AcceptedWriteContext,
} from "@/worker/git/receive/streamReceivePack";
import { resolveRepositoryRoute } from "@/worker/repositories/route";
import { handleUploadPackPOST } from "@/worker/routes/git";

import { createTestCacheContext, seedPackFirstRepo } from "./util/pack-first";
import { decodePktLinePayloads } from "./util/fetch-protocol";
import { setupRepoForTests } from "./util/repoSeed";
import { buildStreamingReceiveBody, decodeReportStatus } from "./util/streaming-helpers";
import { toRequestBody, uniqueRepoId } from "./util/test-helpers";

function withUnavailableRepoDO(sourceEnv: Env): Env {
  const namespace = new Proxy(sourceEnv.REPO_DO, {
    get(target, property) {
      if (property === "get") {
        return () =>
          new Proxy(
            {},
            {
              get() {
                return async () => {
                  throw new Error("synthetic repository DO outage");
                };
              },
            }
          );
      }
      const value = Reflect.get(target, property);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return { ...sourceEnv, REPO_DO: namespace };
}

function buildLsRefsBody(): Uint8Array {
  return concatChunks([
    pktLine("command=ls-refs\n"),
    new TextEncoder().encode("0001"),
    pktLine("ref-prefix refs/heads/\n"),
    flushPkt(),
  ]);
}

describe("repository component outage recovery", () => {
  it("does not begin a receive while the repository DO is unavailable and accepts the exact retry", async () => {
    const owner = "o";
    const repo = uniqueRepoId("receive-do-outage");
    const repoId = `${owner}/${repo}`;
    await setupRepoForTests(env, owner, repo);
    const seeded = await seedPackFirstRepo(repoId);
    const stub = getRepoStub(env, repoId);
    const refsBefore = await stub.listRefs();
    const catalogBefore = await stub.getActivePackCatalog();
    const built = await buildStreamingReceiveBody({
      parentOid: seeded.nextCommit.oid,
      nextText: "DO recovery\n",
      commitMessage: "DO outage recovery",
      capabilities: "report-status ofs-delta agent=test",
    });
    const acceptedWriteContext: AcceptedWriteContext = {
      repositoryId: repoId,
      actor: "synthetic-test-actor",
      sourceSurface: "git-push",
      idempotencyKey: null,
    };
    const makeRequest = () =>
      new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
        method: "POST",
        headers: { "Content-Type": "application/x-git-receive-pack-request" },
        body: toRequestBody(built.body),
      });

    const failedContext = createExecutionContext();
    await expect(
      handleStreamingReceivePackPOST(
        withUnavailableRepoDO(env),
        repoId,
        makeRequest(),
        failedContext,
        {
          acceptedWriteContext,
          limiter: new SubrequestLimiter(900),
        }
      )
    ).rejects.toThrow("synthetic repository DO outage");
    await waitOnExecutionContext(failedContext);
    expect(await stub.listRefs()).toEqual(refsBefore);
    expect(await stub.getActivePackCatalog()).toEqual(catalogBefore);
    expect(await stub.getRepoActivity()).toBeNull();

    const retryContext = createExecutionContext();
    const retry = await handleStreamingReceivePackPOST(env, repoId, makeRequest(), retryContext, {
      acceptedWriteContext,
      limiter: new SubrequestLimiter(900),
    });
    await waitOnExecutionContext(retryContext);
    expect(retry.status).toBe(200);
    expect(decodeReportStatus(new Uint8Array(await retry.arrayBuffer()))).toContain(
      "ok refs/heads/main"
    );
    expect((await stub.listRefs()).find((ref) => ref.name === "refs/heads/main")?.oid).toBe(
      built.commit.oid
    );
  });

  it("propagates DO failure from ref/path reads and returns exact state after recovery", async () => {
    const owner = "o";
    const repo = uniqueRepoId("read-do-outage");
    const repoId = `${owner}/${repo}`;
    const repository = await setupRepoForTests(env, owner, repo);
    const seeded = await seedPackFirstRepo(repoId);
    const refsBefore = await seeded.getStub().listRefs();
    const unavailableEnv = withUnavailableRepoDO(env);

    await expect(
      getHeadAndRefs(
        unavailableEnv,
        repoId,
        createTestCacheContext("https://example.com/refs-do-outage")
      )
    ).rejects.toThrow("synthetic repository DO outage");
    await expect(
      readPath(
        unavailableEnv,
        repoId,
        "main",
        "README.md",
        createTestCacheContext("https://example.com/path-do-outage")
      )
    ).rejects.toThrow("synthetic repository DO outage");
    expect(await seeded.getStub().listRefs()).toEqual(refsBefore);

    const route = await resolveRepositoryRoute(env, owner, repo);
    if (!route) throw new Error(`expected seeded route for ${repository.repositoryId}`);
    const outageRequest = new Request(`https://example.com/${owner}/${repo}/git-upload-pack`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: toRequestBody(buildLsRefsBody()),
    });
    const outageCache = createTestCacheContext(outageRequest.url);
    outageCache.memo = {
      ...outageCache.memo,
      flags: new Set(["no-cache-read", "no-cache-write"]),
    };
    const outageResponse = await handleUploadPackPOST(
      unavailableEnv,
      route,
      outageRequest,
      outageCache
    );
    expect(outageResponse.status).toBe(503);
    expect(outageResponse.headers.get("Cache-Control")).toBe("no-store");
    expect(outageResponse.headers.get("Retry-After")).toBe("5");
    expect(await outageResponse.text()).toBe("Repository metadata unavailable\n");

    const recoveredRefs = await getHeadAndRefs(
      env,
      repoId,
      createTestCacheContext("https://example.com/refs-recovered")
    );
    expect(recoveredRefs.refs).toEqual(refsBefore);
    const recoveredPath = await readPath(
      env,
      repoId,
      "main",
      "README.md",
      createTestCacheContext("https://example.com/path-recovered")
    );
    expect(recoveredPath.type).toBe("blob");
    if (recoveredPath.type === "blob") {
      expect(new TextDecoder().decode(recoveredPath.content)).toBe("version two\n");
    }
    const recoveryRequest = new Request(`https://example.com/${owner}/${repo}/git-upload-pack`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: toRequestBody(buildLsRefsBody()),
    });
    const recoveryCache = createTestCacheContext(recoveryRequest.url);
    recoveryCache.memo = {
      ...recoveryCache.memo,
      flags: new Set(["no-cache-read", "no-cache-write"]),
    };
    const recoveryResponse = await handleUploadPackPOST(env, route, recoveryRequest, recoveryCache);
    expect(recoveryResponse.status).toBe(200);
    const recoveryLines = decodePktLinePayloads(
      new Uint8Array(await recoveryResponse.arrayBuffer())
    );
    expect(recoveryLines).toContain(`${seeded.nextCommit.oid} refs/heads/main\n`);
    expect(await seeded.getStub().listRefs()).toEqual(refsBefore);
  });
});
