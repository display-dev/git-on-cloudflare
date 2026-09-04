import { env } from "cloudflare:test";
import { packIndexKey, packRefsKey } from "@/worker/keys";
import { pushStreamingUpdate } from "./streaming-helpers";
import { runQueueMessage, type QueueRunResult } from "./queue";
import { getRepoStub } from "@/worker/common";
import {
  COMPACTION_ACTIVITY_QUIET_MS,
  COMPACTION_MAX_DEFERRAL_MS,
} from "@/worker/do/repo/catalog/shared";
import { runDOWithRetry } from "./test-helpers";

export async function compactOnce(repoId: string): Promise<QueueRunResult> {
  const doId = env.REPO_DO.idFromName(repoId).toString();
  await expireCompactionQuietPeriod(repoId);
  return await runQueueMessage({
    kind: "compaction",
    doId,
    repoId,
  });
}

export async function expireCompactionQuietPeriod(repoId: string): Promise<void> {
  const stub = getRepoStub(env, repoId);
  await runDOWithRetry(
    () => stub,
    async (_instance, state) => {
      const wantedAt = await state.storage.get<number>("compactionWantedAt");
      if (typeof wantedAt === "number") {
        await state.storage.put(
          "compactionWantedAt",
          Date.now() - COMPACTION_ACTIVITY_QUIET_MS - 1
        );
        await state.storage.put(
          "compactionPendingSince",
          Date.now() - COMPACTION_MAX_DEFERRAL_MS - 1
        );
      }
    }
  );
}

export async function deleteSupersededOnce(
  repoId: string,
  packKeys: string[],
  removeCatalogRows = false,
  supersededAtGeneration?: number
): Promise<QueueRunResult> {
  const doId = env.REPO_DO.idFromName(repoId).toString();
  return await runQueueMessage({
    kind: "compaction-delete",
    doId,
    repoId,
    packKeys,
    removeCatalogRows,
    ...(typeof supersededAtGeneration === "number" ? { supersededAtGeneration } : {}),
  });
}

export async function collectPackObjects(
  packKeys: string[]
): Promise<Array<{ packKey: string; exists: boolean; idxExists: boolean; refsExists: boolean }>> {
  const checks: Array<{
    packKey: string;
    exists: boolean;
    idxExists: boolean;
    refsExists: boolean;
  }> = [];
  for (const packKey of packKeys) {
    checks.push({
      packKey,
      exists: (await env.REPO_BUCKET.head(packKey)) !== null,
      idxExists: (await env.REPO_BUCKET.head(packIndexKey(packKey))) !== null,
      refsExists: (await env.REPO_BUCKET.head(packRefsKey(packKey))) !== null,
    });
  }
  return checks;
}

export async function pushOverflowingStreamingHistory(args: {
  owner: string;
  repo: string;
  repoId: string;
  startingCommitOid: string;
  updates: number;
}): Promise<{ currentCommitOid: string; objectOids: string[] }> {
  let currentCommitOid = args.startingCommitOid;
  const objectOids: string[] = [];

  for (let index = 0; index < args.updates; index++) {
    const pushed = await pushStreamingUpdate(
      args.owner,
      args.repo,
      currentCommitOid,
      `streaming update ${index}\n`
    );
    currentCommitOid = pushed.commitOid;
    objectOids.push(pushed.blob.oid, pushed.tree.oid, pushed.commit.oid);
  }

  return { currentCommitOid, objectOids };
}
