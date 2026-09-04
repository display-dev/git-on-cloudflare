import { describe, expect, it } from "vitest";
import { env } from "cloudflare:workers";

import { bytesToHex } from "@/worker/common";
import { encodeGitObject } from "@/worker/git/core/objects";
import { buildPackV2Artifacts } from "@/worker/git/pack/build";
import { resolveDeltasAndWriteIdx, scanPack } from "@/worker/git/pack/indexer";
import { packIndexKey, packRefsKey } from "@/worker/keys";

import { makeLimiter, packIndexerLog as log } from "./util/pack-indexer.helpers";
import { buildTreePayload } from "./util/packed-repo";
import { uniqueRepoId } from "./util/test-helpers";

describe("buildPackV2Artifacts", () => {
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
