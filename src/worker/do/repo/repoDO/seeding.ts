import { buildPackV2 } from "@/worker/git/pack/build";
import { encodeGitObject } from "@/worker/git/core";
import { scanPack, resolveDeltasAndWriteIdx } from "@/worker/git/pack/indexer";
import { createLogger } from "@/worker/common/logger";
import { r2PackKey } from "@/worker/keys";
import type { Limiter } from "@/worker/git/operations/limits";

import { asTypedStorage, objKey } from "../repoState";
import type { RepoStateSchema } from "../repoState";
import { getDb, upsertPackCatalogRow } from "../db";

/** No-op limiter for test-only seeding path (no real concurrency limits). */
const seedLimiter: Limiter = { run: (_label, fn) => fn() };
const seedLog = createLogger(undefined, { service: "seed" });

export async function seedMinimalRepoState(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  withPack: boolean;
}): Promise<{ commitOid: string; treeOid: string }> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const db = getDb(args.ctx.storage);

  const treeContent = new Uint8Array(0);
  const { oid: treeOid, zdata: treeZ } = await encodeGitObject("tree", treeContent);

  const author = `You <you@example.com> 0 +0000`;
  const message = "initial\n";
  const commitPayload =
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${author}\n` + `\n${message}`;
  const { oid: commitOid, zdata: commitZ } = await encodeGitObject(
    "commit",
    new TextEncoder().encode(commitPayload)
  );

  if (args.withPack) {
    const packData = await buildPackV2([
      { type: "tree", payload: treeContent },
      { type: "commit", payload: new TextEncoder().encode(commitPayload) },
    ]);

    const packKey = r2PackKey(args.prefix, `pack-test-${Date.now()}.pack`);
    await args.env.REPO_BUCKET.put(packKey, packData);

    // Index the pack to produce .idx in R2
    const scanResult = await scanPack({
      env: args.env,
      packKey,
      packSize: packData.byteLength,
      limiter: seedLimiter,
      countSubrequest: () => {},
      log: seedLog,
    });
    const resolveResult = await resolveDeltasAndWriteIdx({
      env: args.env,
      packKey,
      packSize: packData.byteLength,
      limiter: seedLimiter,
      countSubrequest: () => {},
      log: seedLog,
      scanResult,
      repoId: "test",
    });

    // Register in pack catalog
    const seq = (await store.get("nextPackSeq")) || 1;
    await upsertPackCatalogRow(db, {
      packKey,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: seq,
      seqHi: seq,
      objectCount: resolveResult.objectCount,
      packBytes: packData.byteLength,
      idxBytes: resolveResult.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    });

    const packsetVersion = ((await store.get("packsetVersion")) || 0) + 1;
    await store.put("packsetVersion", packsetVersion);
    await store.put("nextPackSeq", seq + 1);
  } else {
    await store.put(objKey(treeOid), treeZ);
    await store.put(objKey(commitOid), commitZ);
  }

  await store.put("refs", [{ name: "refs/heads/main", oid: commitOid }]);
  await store.put("head", { target: "refs/heads/main" });

  return { treeOid, commitOid };
}
