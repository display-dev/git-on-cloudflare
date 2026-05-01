import type { CacheContext } from "@/worker/cache";
import type { IdxView } from "@/worker/git/object-store/types";

export type OrderedPackSnapshotEntry = {
  packKey: string;
  packBytes: number;
  idx: IdxView;
};

export type OrderedPackSnapshot = {
  packs: OrderedPackSnapshotEntry[];
};

export type ServeUploadPackPlan = {
  type: "Serve";
  repoId: string;
  snapshot: OrderedPackSnapshot;
  neededOids: string[];
  ackOids: string[];
  signal?: AbortSignal;
  cacheCtx?: CacheContext;
};

export type UploadPackPlan =
  | ServeUploadPackPlan
  | {
      type: "RepositoryNotReady";
    };
