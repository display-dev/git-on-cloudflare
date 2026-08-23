import type { Logger } from "@/worker/common/logger";
import type { Limiter } from "@/worker/git/operations/limits";

import { z } from "zod";
import {
  doPrefix,
  repositoryGenerationIndexKey,
  repositoryGenerationManifestKey,
} from "@/worker/keys";

const GENERATION_INDEX_MAX_BYTES = 64 * 1024;
const GENERATION_MANIFEST_MAX_BYTES = 4 * 1024 * 1024;
const GENERATION_PUBLISH_ATTEMPTS = 4;

const generationIndexSchema = z.object({
  schemaVersion: z.literal(1),
  generation: z.number().int().nonnegative(),
  manifestKey: z.string().min(1),
  updatedAt: z.number().int().nonnegative(),
});

type GenerationIndex = z.infer<typeof generationIndexSchema>;

const generationManifestSchema = z.object({
  schemaVersion: z.literal(1),
  generation: z.number().int().nonnegative(),
  packs: z.array(z.object({ packKey: z.string().min(1) })),
});

function encodeJson(value: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index++) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

async function readIndexObject(object: R2ObjectBody): Promise<GenerationIndex> {
  if (object.size > GENERATION_INDEX_MAX_BYTES) {
    throw new Error("repository generation index exceeds its size bound");
  }
  const bytes = new Uint8Array(await object.arrayBuffer());
  return generationIndexSchema.parse(JSON.parse(new TextDecoder().decode(bytes)));
}

export async function readPublishedRepositoryGeneration(args: {
  env: Env;
  doId: string;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
}): Promise<number | null> {
  return (await readPublishedRepositoryGenerationState(args))?.generation ?? null;
}

export async function readPublishedRepositoryGenerationState(args: {
  env: Env;
  doId: string;
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
}): Promise<{ generation: number; activePackKeys: Set<string> } | null> {
  const prefix = doPrefix(args.doId);
  args.countSubrequest("r2:get-generation-index");
  const object = await args.limiter.run("r2:get-generation-index", () =>
    args.env.REPO_BUCKET.get(repositoryGenerationIndexKey(prefix))
  );
  if (!object) return null;
  const index = await readIndexObject(object);
  if (index.manifestKey !== repositoryGenerationManifestKey(prefix, index.generation)) {
    throw new Error("repository generation index references an unexpected manifest");
  }
  args.countSubrequest("r2:get-generation-manifest");
  const manifestObject = await args.limiter.run("r2:get-generation-manifest", () =>
    args.env.REPO_BUCKET.get(index.manifestKey)
  );
  if (!manifestObject || manifestObject.size > GENERATION_MANIFEST_MAX_BYTES) {
    throw new Error("repository generation manifest is missing or exceeds its size bound");
  }
  const manifest = generationManifestSchema.parse(await manifestObject.json());
  if (manifest.generation !== index.generation) {
    throw new Error("repository generation manifest does not match its index");
  }
  return {
    generation: index.generation,
    activePackKeys: new Set(manifest.packs.map((pack) => pack.packKey)),
  };
}

export async function publishRepositoryGeneration(args: {
  env: Env;
  doId: string;
  generation: number;
  activePackKeys: string[];
  limiter: Limiter;
  countSubrequest(op: string, n?: number): void;
  log: Logger;
}): Promise<"published" | "superseded"> {
  const prefix = doPrefix(args.doId);
  const manifestKey = repositoryGenerationManifestKey(prefix, args.generation);
  const indexKey = repositoryGenerationIndexKey(prefix);
  const manifestBytes = encodeJson({
    schemaVersion: 1,
    generation: args.generation,
    packs: args.activePackKeys.map((packKey) => ({ packKey })),
  });

  args.countSubrequest("r2:put-generation-manifest");
  const manifest = await args.limiter.run("r2:put-generation-manifest", () =>
    args.env.REPO_BUCKET.put(manifestKey, manifestBytes, {
      onlyIf: { etagDoesNotMatch: "*" },
      httpMetadata: { contentType: "application/json" },
    })
  );
  if (!manifest) {
    args.countSubrequest("r2:get-generation-manifest");
    const existing = await args.limiter.run("r2:get-generation-manifest", () =>
      args.env.REPO_BUCKET.get(manifestKey)
    );
    if (!existing || existing.size !== manifestBytes.byteLength) {
      throw new Error("repository generation manifest conflicts with existing bytes");
    }
    const existingBytes = new Uint8Array(await existing.arrayBuffer());
    if (!bytesEqual(existingBytes, manifestBytes)) {
      throw new Error("repository generation manifest conflicts with existing content");
    }
  }

  for (let attempt = 1; attempt <= GENERATION_PUBLISH_ATTEMPTS; attempt++) {
    args.countSubrequest("r2:get-generation-index");
    const currentObject = await args.limiter.run("r2:get-generation-index", () =>
      args.env.REPO_BUCKET.get(indexKey)
    );
    const current = currentObject ? await readIndexObject(currentObject) : null;
    if (current && current.generation > args.generation) return "superseded";
    if (current?.generation === args.generation && current.manifestKey === manifestKey) {
      return "published";
    }

    const nextBytes = encodeJson({
      schemaVersion: 1,
      generation: args.generation,
      manifestKey,
      updatedAt: Date.now(),
    });
    args.countSubrequest("r2:cas-generation-index");
    const written = await args.limiter.run("r2:cas-generation-index", () =>
      args.env.REPO_BUCKET.put(indexKey, nextBytes, {
        onlyIf: currentObject ? { etagMatches: currentObject.etag } : { etagDoesNotMatch: "*" },
        httpMetadata: { contentType: "application/json" },
      })
    );
    if (written) {
      args.log.info("generation:published", {
        generation: args.generation,
        packCount: args.activePackKeys.length,
        attempt,
      });
      return "published";
    }
  }
  throw new Error("repository generation index CAS did not converge");
}
