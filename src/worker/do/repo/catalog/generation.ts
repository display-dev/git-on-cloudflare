import type { RepoStateSchema } from "../repoState";

import { asTypedStorage } from "../repoState";
import { getDb, listActivePackCatalog } from "../db";

export type PendingGenerationPublication = {
  generation: number;
  activePackKeys: string[];
};

export async function getPendingGenerationPublicationState(
  ctx: DurableObjectState
): Promise<PendingGenerationPublication | null> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const pending = await store.get("generationPublicationPending");
  return pending ?? null;
}

export async function ensureGenerationPublicationPendingState(
  ctx: DurableObjectState
): Promise<PendingGenerationPublication> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const existing = await store.get("generationPublicationPending");
  if (existing) return existing;

  const generation = (await store.get("packsetVersion")) || 0;
  const activeCatalog = await listActivePackCatalog(getDb(ctx.storage));
  const pending = {
    generation,
    activePackKeys: activeCatalog.map((row) => row.packKey),
  };
  await store.put("generationPublicationPending", pending);
  return pending;
}

export async function completeGenerationPublicationState(
  ctx: DurableObjectState,
  generation: number
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const pending = await store.get("generationPublicationPending");
    if (pending?.generation !== generation) return false;
    await store.delete("generationPublicationPending");
    return true;
  });
}
