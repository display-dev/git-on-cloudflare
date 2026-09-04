import { describe, expect, it } from "vitest";
import { env } from "cloudflare:workers";

import { bytesToHex } from "@/worker/common";
import { encodeGitObject } from "@/worker/git/core/objects";
import { buildPackV2Artifacts } from "@/worker/git/pack/build";
import { resolveDeltasAndWriteIdx, scanPack } from "@/worker/git/pack/indexer";
import { packIndexKey, packRefsKey } from "@/worker/keys";

import { makeLimiter, packIndexerLog as log } from "./util/pack-indexer.helpers";
import { buildPack, buildAppendOnlyDelta } from "./util/git-pack";
import { buildTreePayload } from "./util/packed-repo";
import { uniqueRepoId } from "./util/test-helpers";

describe("buildPackV2Artifacts", () => {
  it("scans and resolves an in-memory OFS delta across one-byte chunks", async () => {
    const basePayload = new TextEncoder().encode("base payload\n");
    const suffix = new TextEncoder().encode("cross-chunk suffix\n");
    const resolvedPayload = new Uint8Array(basePayload.byteLength + suffix.byteLength);
    resolvedPayload.set(basePayload);
    resolvedPayload.set(suffix, basePayload.byteLength);
    const resolvedObject = await encodeGitObject("blob", resolvedPayload);
    const pack = await buildPack([
      { type: "blob", payload: basePayload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(basePayload, suffix) },
    ]);
    const packKey = `test/memory-chunks-${uniqueRepoId()}.pack`;
    const scan = await scanPack({
      env,
      packKey,
      packSize: pack.byteLength,
      packData: pack,
      chunkSize: 1,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const resolved = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: pack.byteLength,
      packData: pack,
      persistArtifacts: false,
      chunkSize: 1,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: scan,
      repoId: uniqueRepoId(),
    });

    expect(bytesToHex(scan.table.oids.subarray(20, 40))).toBe(resolvedObject.oid);
    expect(resolved.idxData).toBeDefined();
    expect(resolved.refIndexData).toBeDefined();
    expect(await env.REPO_BUCKET.head(packKey)).toBeNull();
  });

  it("rejects an in-memory pack whose declared length does not match", async () => {
    const pack = await buildPack([{ type: "blob", payload: new Uint8Array([1, 2, 3]) }]);
    const packKey = `test/memory-length-${uniqueRepoId()}.pack`;
    await expect(
      scanPack({
        env,
        packKey,
        packSize: pack.byteLength + 1,
        packData: pack,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow("scan: in-memory pack length mismatch");

    const scan = await scanPack({
      env,
      packKey,
      packSize: pack.byteLength,
      packData: pack,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    await expect(
      resolveDeltasAndWriteIdx({
        env,
        packKey,
        packSize: pack.byteLength + 1,
        packData: pack,
        persistArtifacts: false,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
        scanResult: scan,
        repoId: uniqueRepoId(),
      })
    ).rejects.toThrow("resolve: in-memory pack length mismatch");
  });

  it("stops an in-memory scan when its request is cancelled", async () => {
    const pack = await buildPack([{ type: "blob", payload: new Uint8Array([1, 2, 3]) }]);
    const abortController = new AbortController();
    abortController.abort();

    await expect(
      scanPack({
        env,
        packKey: `test/memory-abort-${uniqueRepoId()}.pack`,
        packSize: pack.byteLength,
        packData: pack,
        signal: abortController.signal,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow("scan: aborted during reader:ensure");
  });

  it("matches independently scanned IDX and PREF bytes", async () => {
    const blobPayload = new TextEncoder().encode("canonical artifact proof\n");
    const blob = await encodeGitObject("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "proof.txt", oid: blob.oid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const identity = "Proof <proof@example.invalid> 0 +0000";
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\nauthor ${identity}\ncommitter ${identity}\n\ncanonical\n`
    );

    const artifacts = await buildPackV2Artifacts(
      [
        { type: "blob", payload: blobPayload },
        { type: "tree", payload: treePayload },
        { type: "commit", payload: commitPayload },
      ],
      { compressionLevel: 1 }
    );
    const packKey = `test/canonical-artifacts-${uniqueRepoId()}.pack`;
    const memoryScan = await scanPack({
      env,
      packKey,
      packSize: artifacts.pack.byteLength,
      packData: artifacts.pack,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const memoryResolved = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: artifacts.pack.byteLength,
      packData: artifacts.pack,
      persistArtifacts: false,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: memoryScan,
      repoId: uniqueRepoId(),
    });
    expect(memoryResolved.idxData).toEqual(artifacts.idx);
    expect(memoryResolved.refIndexData).toEqual(artifacts.refs);
    expect(await env.REPO_BUCKET.head(packKey)).toBeNull();
    expect(await env.REPO_BUCKET.head(packIndexKey(packKey))).toBeNull();
    expect(await env.REPO_BUCKET.head(packRefsKey(packKey))).toBeNull();

    await env.REPO_BUCKET.put(packKey, artifacts.pack);
    const scan = await scanPack({
      env,
      packKey,
      packSize: artifacts.pack.byteLength,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const resolved = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: artifacts.pack.byteLength,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: scan,
      repoId: uniqueRepoId(),
    });
    const independentlyBuiltIdx = await env.REPO_BUCKET.get(packIndexKey(packKey));
    const independentlyBuiltRefs = await env.REPO_BUCKET.get(packRefsKey(packKey));
    expect(independentlyBuiltIdx).not.toBeNull();
    expect(independentlyBuiltRefs).not.toBeNull();
    expect(new Uint8Array(await independentlyBuiltIdx!.arrayBuffer())).toEqual(artifacts.idx);
    expect(new Uint8Array(await independentlyBuiltRefs!.arrayBuffer())).toEqual(artifacts.refs);
    expect(bytesToHex(resolved.idxView.packChecksum)).toBe(
      bytesToHex(artifacts.pack.subarray(-20))
    );
  });
});
