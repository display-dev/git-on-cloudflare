import { z } from "zod";

import { asBufferSource, bytesToHex, getRepoStub } from "@/worker/common";
import type { BeginReceiveResult } from "@/worker/do/repo/catalog/shared";
import { acceptedWriteFactsForCommands } from "@/worker/git/acceptedWrite";
import { fingerprintNativeReceive } from "@/worker/git/nativeReceive/fingerprint";
import {
  isValidNativeReceiveOperationId,
  isNativeReceiveTerminal,
  type EnqueueNativeReceiveResult,
  type MatchNativeReceiveOperationResult,
  type NativeReceiveOperation,
  type NativeReceiveOperationView,
} from "@/worker/git/nativeReceive/types";
import { countSubrequest } from "@/worker/git/operations/limits";
import { isValidRefName, validateReceiveCommands } from "@/worker/git/operations/validation";
import {
  doPrefix,
  nativeReceiveOutputPackKey,
  packIndexKey,
  packRefsKey,
  repositoryImportPackKey,
} from "@/worker/keys";
import { resolveRepositoryRoute } from "@/worker/repositories/route";
import { isValidOwnerRepo } from "@/shared/web";
import { authorizeInternalRequest } from "./internalAuth";
import { type AppContext, type AppRouter } from "./hono";

const MAX_IMPORT_CONTROL_BYTES = 64 * 1024;
const MAX_IMPORT_PACK_BYTES = 5_000_000_000;

const receiveCommandSchema = z.object({
  oldOid: z.string().regex(/^[0-9a-f]{40}$/),
  newOid: z.string().regex(/^[0-9a-f]{40}$/),
  ref: z.string().min(1).max(1024),
});

const commitImportSchema = z.object({
  inputBytes: z.number().int().positive().max(MAX_IMPORT_PACK_BYTES),
  inputEtag: z
    .string()
    .min(1)
    .max(128)
    .regex(/^[A-Za-z0-9_-]+$/),
  commands: z.array(receiveCommandSchema).min(1).max(4096),
  actor: z
    .string()
    .min(1)
    .max(200)
    .regex(/^[^\0\r\n]+$/),
  idempotencyKey: z
    .string()
    .min(1)
    .max(200)
    .regex(/^[^\0\r\n]+$/),
});

function noStoreJson(body: unknown, status: number): Response {
  return Response.json(body, { status, headers: { "Cache-Control": "no-store" } });
}

async function sha256(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", asBufferSource(bytes));
  return bytesToHex(new Uint8Array(digest));
}

async function resolveImportContext(c: AppContext) {
  const owner = c.req.param("owner") ?? "";
  const repo = c.req.param("repo") ?? "";
  const operationId = c.req.param("operationId") ?? "";
  if (
    !isValidOwnerRepo(owner) ||
    !isValidOwnerRepo(repo) ||
    !isValidNativeReceiveOperationId(operationId)
  ) {
    return null;
  }
  const route = await resolveRepositoryRoute(c.env, owner, repo, {
    mode: "allow-d1-fallback",
    db: c.var.db,
    log: c.var.logFor({ service: "NativeImport" }),
  });
  return route ? { operationId, route } : null;
}

async function handlePrepareImport(c: AppContext): Promise<Response> {
  const auth = await authorizeInternalRequest(c);
  if (auth) return auth;
  const resolved = await resolveImportContext(c);
  if (!resolved) return noStoreJson({ error: "Not found" }, 404);
  return noStoreJson(
    {
      operationId: resolved.operationId,
      uploadKey: repositoryImportPackKey(resolved.route.doName, resolved.operationId),
      maximumBytes: MAX_IMPORT_PACK_BYTES,
    },
    200
  );
}

async function handleCommitImport(c: AppContext): Promise<Response> {
  const auth = await authorizeInternalRequest(c);
  if (auth) return auth;
  const resolved = await resolveImportContext(c);
  if (!resolved) return noStoreJson({ error: "Not found" }, 404);

  const contentLength = Number(c.req.header("Content-Length"));
  if (!Number.isSafeInteger(contentLength) || contentLength <= 0) {
    return noStoreJson({ error: "A bounded Content-Length is required" }, 411);
  }
  if (contentLength > MAX_IMPORT_CONTROL_BYTES) {
    return noStoreJson({ error: "Import control request is too large" }, 413);
  }
  const parsed = commitImportSchema.safeParse(await c.req.json().catch(() => null));
  if (!parsed.success || parsed.data.commands.some((command) => !isValidRefName(command.ref))) {
    return noStoreJson({ error: "Invalid import request" }, 400);
  }

  const { route, operationId } = resolved;
  const stub = getRepoStub(c.env, route.doName);
  const limiter = c.var.limiter;
  const inputPackKey = repositoryImportPackKey(route.doName, operationId);
  const keyHash = await sha256(parsed.data.idempotencyKey);
  const acceptedWrites = acceptedWriteFactsForCommands({
    repositoryId: route.repositoryId,
    commands: parsed.data.commands,
    actor: parsed.data.actor,
    sourceSurface: "import",
    idempotencyKey: keyHash,
  });
  const fingerprint = await fingerprintNativeReceive({
    repositoryId: route.doName,
    commands: parsed.data.commands,
    acceptedWrites,
    inputPackKey,
    inputBytes: parsed.data.inputBytes,
    inputEtag: parsed.data.inputEtag,
  });

  const count = (label: string) => {
    if (!countSubrequest(c.var.cacheCtx, 1)) {
      throw new Error(`Import subrequest budget exhausted at ${label}`);
    }
  };

  count("do:match-native-import");
  const prior = await limiter.run<MatchNativeReceiveOperationResult>(
    "do:match-native-import",
    async () => await stub.matchNativeReceiveOperation(operationId, fingerprint)
  );
  if (prior.status === "conflict") {
    return noStoreJson({ error: "Operation id conflict" }, 409);
  }
  if (prior.status === "match") {
    return noStoreJson(prior.operation, isNativeReceiveTerminal(prior.operation.state) ? 200 : 202);
  }

  count("r2:head-import-pack");
  const input = await limiter.run("r2:head-import-pack", () =>
    c.env.REPO_BUCKET.head(inputPackKey)
  );
  if (!input || input.size !== parsed.data.inputBytes || input.etag !== parsed.data.inputEtag) {
    return noStoreJson({ error: "Staged import pack is missing or has the wrong size" }, 409);
  }

  count("do:begin-native-import");
  const begin = await limiter.run<BeginReceiveResult>(
    "do:begin-native-import",
    async () => await stub.beginReceive()
  );
  if (!begin.ok) {
    return noStoreJson({ error: "Repository is busy", retryAfter: begin.retryAfter }, 503);
  }

  const statuses = validateReceiveCommands(begin.refs, parsed.data.commands);
  if (!statuses.every((status) => status.ok)) {
    count("do:abort-native-import");
    await limiter.run("do:abort-native-import", () => stub.abortReceive(begin.lease.token));
    return noStoreJson({ error: "Ref conflict", statuses }, 409);
  }

  const outputPackKey = nativeReceiveOutputPackKey(
    doPrefix(stub.id.toString()),
    operationId,
    fingerprint
  );
  const now = Date.now();
  const operation: NativeReceiveOperation = {
    id: operationId,
    fingerprint,
    leaseToken: begin.lease.token,
    repositoryId: route.doName,
    state: "staged",
    inputPackKey,
    inputBytes: parsed.data.inputBytes,
    inputEtag: parsed.data.inputEtag,
    outputPackKey,
    outputIdxKey: packIndexKey(outputPackKey),
    outputRefsKey: packRefsKey(outputPackKey),
    commands: parsed.data.commands,
    acceptedWrites,
    activeCatalog: begin.activeCatalog,
    createdAt: now,
    updatedAt: now,
    attempts: 0,
    cleanupPending: false,
  };
  count("do:enqueue-native-import");
  const enqueued = await limiter.run<EnqueueNativeReceiveResult>(
    "do:enqueue-native-import",
    async () => await stub.enqueueNativeReceive(operation)
  );
  if (enqueued.status !== "queued" && enqueued.status !== "replayed") {
    await limiter.run("do:abort-native-import", () => stub.abortReceive(begin.lease.token));
    return noStoreJson({ error: enqueued.message }, 409);
  }

  // The immediate Durable Object alarm owns processing after this response.
  // No request lifetime or client connection is part of the operation's
  // correctness boundary.
  return noStoreJson(enqueued.operation, 202);
}

async function handleGetNativeOperation(c: AppContext): Promise<Response> {
  const auth = await authorizeInternalRequest(c);
  if (auth) return auth;
  const resolved = await resolveImportContext(c);
  if (!resolved) return noStoreJson({ error: "Not found" }, 404);
  const stub = getRepoStub(c.env, resolved.route.doName);
  const log = c.var.logFor({ service: "NativeReceiveQuery" });
  if (!countSubrequest(c.var.cacheCtx, 1)) {
    log.warn("native-receive-query:budget-exhausted");
    return noStoreJson({ error: "Native receive query is temporarily unavailable" }, 503);
  }
  let operation: NativeReceiveOperationView | null | undefined;
  try {
    operation = await c.var.limiter.run("do:get-native-receive", () =>
      stub.getNativeReceiveOperation(resolved.operationId)
    );
  } catch (error) {
    log.error("native-receive-query:failed", { error: String(error) });
    return noStoreJson({ error: "Native receive query is temporarily unavailable" }, 503);
  }
  if (!operation) return noStoreJson({ error: "Not found" }, 404);
  log.info("native-receive-query:resolved", {
    state: operation.state,
    terminal: isNativeReceiveTerminal(operation.state),
  });
  return noStoreJson(operation, isNativeReceiveTerminal(operation.state) ? 200 : 202);
}

export function registerNativeImportRoutes(router: AppRouter): void {
  const path = "/_internal/imports/:owner/:repo/:operationId";
  router.post(`${path}/prepare`, handlePrepareImport);
  router.post(path, handleCommitImport);
  router.get(path, handleGetNativeOperation);
  // Smart HTTP clients can supply the same operation id through the
  // X-Display-Operation-ID header, then reconcile a lost response here.
  router.get("/_internal/receives/:owner/:repo/:operationId", handleGetNativeOperation);
}
