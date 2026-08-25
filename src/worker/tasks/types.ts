import { z } from "zod";

export { z };

/** Reader drain window before immutable artifacts from a catalog swap are removed. */
export const SUPERSEDED_PACK_DELETE_DELAY_SECONDS = 60;

export type RepoQueueMessageHandle<Body> = MessageBatch<Body>["messages"][number];

export const CompactionQueueMessageSchema = z.object({
  kind: z.literal("compaction"),
  doId: z.string(),
  repoId: z.string().optional(),
});

export type CompactionQueueMessage = z.infer<typeof CompactionQueueMessageSchema>;

export const CompactionDeleteQueueMessageSchema = z.object({
  kind: z.literal("compaction-delete"),
  doId: z.string(),
  repoId: z.string().optional(),
  packKeys: z.array(z.string()),
  supersededAtGeneration: z.number().int().nonnegative().optional(),
  removeCatalogRows: z.boolean().optional(),
});

export type CompactionDeleteQueueMessage = z.infer<typeof CompactionDeleteQueueMessageSchema>;

export const ReachabilityGcQueueMessageSchema = z.object({
  kind: z.literal("reachability-gc"),
  doId: z.string(),
  repoId: z.string(),
});

export type ReachabilityGcQueueMessage = z.infer<typeof ReachabilityGcQueueMessageSchema>;

export const PackRefBackfillQueueMessageSchema = z.object({
  kind: z.literal("pack-ref-backfill"),
  doId: z.string(),
  repoId: z.string().optional(),
  packKey: z.string(),
});

export type PackRefBackfillQueueMessage = z.infer<typeof PackRefBackfillQueueMessageSchema>;

export const NativeReceiveQueueMessageSchema = z.object({
  kind: z.literal("native-receive"),
  doId: z.string(),
  operationId: z.string(),
});

export type NativeReceiveQueueMessage = z.infer<typeof NativeReceiveQueueMessageSchema>;

export const SnapshotMaterializeQueueMessageSchema = z
  .object({
    kind: z.literal("snapshot-materialize"),
    doName: z.string().min(1).max(512),
    repositoryId: z.string().min(1).max(128),
    ref: z.string().min(1).max(1024),
    beforeSha: z.string().regex(/^[0-9a-f]{40}$/),
    afterSha: z.string().regex(/^[0-9a-f]{40}$/),
    actor: z.string().min(1).max(256),
    sourceSurface: z.enum(["git-push", "ingestion", "import"]),
    idempotencyKey: z.string().min(1).max(256).nullable(),
  })
  .strict();

export type SnapshotMaterializeQueueMessage = z.infer<typeof SnapshotMaterializeQueueMessageSchema>;

export const RouteCacheSyncMessageSchema = z.object({
  kind: z.literal("route-cache-sync"),
  repositoryId: z.string(),
  namespaceSlug: z.string(),
  repoSlug: z.string(),
  enqueuedAt: z.number(),
});

export type RouteCacheSyncMessage = z.infer<typeof RouteCacheSyncMessageSchema>;

export const RepositoryDeleteMessageSchema = z.object({
  kind: z.literal("repository-delete"),
  repositoryId: z.string(),
  namespaceId: z.string(),
  namespaceSlug: z.string(),
  repoSlug: z.string(),
  doName: z.string(),
  actor: z.string(),
  requestedAt: z.number(),
});

export type RepositoryDeleteMessage = z.infer<typeof RepositoryDeleteMessageSchema>;

export const RepoTaskQueueMessageSchema = z.discriminatedUnion("kind", [
  CompactionQueueMessageSchema,
  CompactionDeleteQueueMessageSchema,
  ReachabilityGcQueueMessageSchema,
  PackRefBackfillQueueMessageSchema,
  NativeReceiveQueueMessageSchema,
  SnapshotMaterializeQueueMessageSchema,
  RouteCacheSyncMessageSchema,
  RepositoryDeleteMessageSchema,
]);

export type RepoTaskQueueMessage = z.infer<typeof RepoTaskQueueMessageSchema>;
