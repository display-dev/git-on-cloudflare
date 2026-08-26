import { constantTimeEquals } from "@/worker/auth/pat";
import { getRepoStub, json } from "@/worker/common";
import { doPrefix } from "@/worker/keys";
import { resolveRepositoryRoute } from "@/worker/repositories/route";
import type { QualificationResetResult } from "@/worker/do/repo/qualification";
import type { AppContext, AppRouter } from "./hono";

const QUALIFICATION_SCHEMA_VERSION = 2;
const SYNTHETIC_NAMESPACE = /^qual-[a-f0-9]{32,64}$/;
const MAX_INVENTORY_OBJECTS = 10_000;

type StorageInventory = {
  objectCount: number;
  objectBytes: number;
  repositoryObjects: {
    objectCount: number;
    objectBytes: number;
  };
  durableGenerationMetadata: {
    objectCount: number;
    objectBytes: number;
  };
  complete: boolean;
};

function isDurableGenerationMetadata(prefix: string, key: string): boolean {
  return (
    key === `${prefix}/generation-index.json` ||
    new RegExp(`^${prefix}/generations/[0-9]+\\.json$`).test(key)
  );
}

async function authorizeQualification(c: AppContext): Promise<Response | null> {
  if (c.env.QUALIFICATION_MODE !== "1" || !c.env.QUALIFICATION_SECRET) {
    return new Response("Not found\n", { status: 404, headers: { "Cache-Control": "no-store" } });
  }
  const match = /^Bearer (.+)$/.exec(c.req.header("Authorization") ?? "");
  if (!match || !(await constantTimeEquals(c.env.QUALIFICATION_SECRET, match[1]!))) {
    return json({ schemaVersion: 1, status: "denied", reason: "unauthorized" }, 401, {
      "Cache-Control": "no-store",
    });
  }
  return null;
}

function validSyntheticTarget(c: AppContext): boolean {
  const owner = c.req.param("owner") ?? "";
  const repo = c.req.param("repo") ?? "";
  return (
    SYNTHETIC_NAMESPACE.test(owner) &&
    owner === c.env.QUALIFICATION_NAMESPACE &&
    /^repo-[a-f0-9]{16,64}$/.test(repo) &&
    repo === c.env.QUALIFICATION_REPOSITORY
  );
}

async function resolveQualificationTarget(c: AppContext) {
  if (!validSyntheticTarget(c)) return null;
  return await resolveRepositoryRoute(c.env, c.req.param("owner")!, c.req.param("repo")!, {
    mode: "allow-d1-fallback",
    db: c.var.db,
    log: c.var.logFor({ service: "QualificationRoute" }),
  });
}

async function storageInventory(c: AppContext, doId: string): Promise<StorageInventory> {
  const prefix = doPrefix(doId);
  let cursor: string | undefined;
  let objectCount = 0;
  let objectBytes = 0;
  let repositoryObjectCount = 0;
  let repositoryObjectBytes = 0;
  let durableGenerationObjectCount = 0;
  let durableGenerationObjectBytes = 0;
  do {
    const page = await c.var.limiter.run("r2:qualification-inventory", () =>
      c.env.REPO_BUCKET.list({ prefix: `${prefix}/`, cursor, limit: 1000 })
    );
    for (const object of page.objects) {
      if (objectCount === MAX_INVENTORY_OBJECTS) {
        return {
          objectCount,
          objectBytes,
          repositoryObjects: {
            objectCount: repositoryObjectCount,
            objectBytes: repositoryObjectBytes,
          },
          durableGenerationMetadata: {
            objectCount: durableGenerationObjectCount,
            objectBytes: durableGenerationObjectBytes,
          },
          complete: false,
        };
      }
      objectCount += 1;
      objectBytes += object.size;
      if (isDurableGenerationMetadata(prefix, object.key)) {
        durableGenerationObjectCount += 1;
        durableGenerationObjectBytes += object.size;
      } else {
        repositoryObjectCount += 1;
        repositoryObjectBytes += object.size;
      }
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  return {
    objectCount,
    objectBytes,
    repositoryObjects: { objectCount: repositoryObjectCount, objectBytes: repositoryObjectBytes },
    durableGenerationMetadata: {
      objectCount: durableGenerationObjectCount,
      objectBytes: durableGenerationObjectBytes,
    },
    complete: true,
  };
}

async function inventoryResponse(
  c: AppContext,
  route: NonNullable<Awaited<ReturnType<typeof resolveQualificationTarget>>>
) {
  const stub = getRepoStub(c.env, route.doName);
  const [repository, storage] = await Promise.all([
    c.var.limiter.run("do:qualification-inventory", () => stub.getQualificationInventory()),
    storageInventory(c, stub.id.toString()),
  ]);
  return {
    schemaVersion: QUALIFICATION_SCHEMA_VERSION,
    status: storage.complete ? "ready" : "over_budget",
    targetRevision: c.env.QUALIFICATION_TARGET_REVISION,
    containerImageDigest: c.env.QUALIFICATION_CONTAINER_IMAGE_DIGEST,
    repository,
    storage,
  } as const;
}

async function readResetRequest(request: Request): Promise<{
  schemaVersion: 1;
  expectedRefStateDigest: string;
  expectedObjectCount: number;
} | null> {
  if ((request.headers.get("Content-Type") ?? "").split(";", 1)[0] !== "application/json")
    return null;
  const length = Number(request.headers.get("Content-Length"));
  if (!Number.isSafeInteger(length) || length < 1 || length > 1024) return null;
  let value: unknown;
  try {
    value = await request.json();
  } catch {
    return null;
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const body = value as Record<string, unknown>;
  if (
    Object.keys(body).sort().join(",") !==
    "expectedObjectCount,expectedRefStateDigest,schemaVersion"
  )
    return null;
  if (
    body.schemaVersion !== 1 ||
    typeof body.expectedRefStateDigest !== "string" ||
    !/^[a-f0-9]{64}$/.test(body.expectedRefStateDigest) ||
    !Number.isSafeInteger(body.expectedObjectCount) ||
    (body.expectedObjectCount as number) < 0 ||
    (body.expectedObjectCount as number) > MAX_INVENTORY_OBJECTS
  )
    return null;
  return body as { schemaVersion: 1; expectedRefStateDigest: string; expectedObjectCount: number };
}

export function registerQualificationRoutes(router: AppRouter): void {
  router.get("/_internal/qualification/:owner/:repo", async (c) => {
    const denied = await authorizeQualification(c);
    if (denied) return denied;
    const route = await resolveQualificationTarget(c);
    if (!route)
      return new Response("Not found\n", { status: 404, headers: { "Cache-Control": "no-store" } });
    if (
      !/^[a-f0-9]{40}$/.test(c.env.QUALIFICATION_TARGET_REVISION) ||
      !/^sha256:[a-f0-9]{64}$/.test(c.env.QUALIFICATION_CONTAINER_IMAGE_DIGEST)
    ) {
      return json(
        { schemaVersion: 1, status: "inconclusive", reason: "invalid_deployment_identity" },
        503,
        { "Cache-Control": "no-store" }
      );
    }
    return json(await inventoryResponse(c, route), 200, { "Cache-Control": "no-store" });
  });

  router.post("/_internal/qualification/:owner/:repo/reset", async (c) => {
    const denied = await authorizeQualification(c);
    if (denied) return denied;
    const request = await readResetRequest(c.req.raw);
    if (!request)
      return json({ schemaVersion: 1, status: "rejected", reason: "invalid_request" }, 400, {
        "Cache-Control": "no-store",
      });
    const route = await resolveQualificationTarget(c);
    if (!route)
      return new Response("Not found\n", { status: 404, headers: { "Cache-Control": "no-store" } });
    if (
      !/^[a-f0-9]{40}$/.test(c.env.QUALIFICATION_TARGET_REVISION) ||
      !/^sha256:[a-f0-9]{64}$/.test(c.env.QUALIFICATION_CONTAINER_IMAGE_DIGEST)
    ) {
      return json(
        { schemaVersion: 1, status: "inconclusive", reason: "invalid_deployment_identity" },
        503,
        { "Cache-Control": "no-store" }
      );
    }
    const before = await inventoryResponse(c, route);
    if (!before.storage.complete || before.storage.objectCount !== request.expectedObjectCount) {
      return json({ schemaVersion: 1, status: "conflict", reason: "storage_state_mismatch" }, 409, {
        "Cache-Control": "no-store",
      });
    }
    const stub = getRepoStub(c.env, route.doName);
    const result = await c.var.limiter.run<QualificationResetResult>(
      "do:qualification-reset",
      async () => stub.resetQualificationState(request.expectedRefStateDigest)
    );
    if (result.status === "conflict") {
      return json(result, 409, { "Cache-Control": "no-store" });
    }
    try {
      await c.var.limiter.run("queue:qualification-reachability-gc", () =>
        c.env.REPO_TASKS_QUEUE.send({
          kind: "reachability-gc",
          doId: stub.id.toString(),
          repoId: route.doName,
        })
      );
    } catch (error) {
      c.var
        .logFor({ service: "QualificationReset", repoId: route.doName })
        .warn("qualification-reset:gc-enqueue-failed", { error: String(error) });
      return json({ schemaVersion: 1, status: "inconclusive", reason: "gc_enqueue_failed" }, 503, {
        "Cache-Control": "no-store",
      });
    }
    return json({ ...result, reachabilityGc: "queued" }, 202, { "Cache-Control": "no-store" });
  });
}
