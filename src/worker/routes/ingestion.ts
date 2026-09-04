import { asBodyInit, asBufferSource, bytesToHex, getRepoStub, zeroOid } from "@/worker/common";
import type { Logger } from "@/worker/common/logger";
import { touchRepositoryUpdatedAt } from "@/worker/db/d1/dal/repositories";
import type { BeginReceiveResult } from "@/worker/do/repo/catalog/shared";
import { acceptedWriteFactsForCommands, emitAcceptedWriteFacts } from "@/worker/git/acceptedWrite";
import { buildIngestionCommit, type IngestionFile } from "@/worker/git/ingestion/pack";
import { countSubrequest } from "@/worker/git/operations/limits";
import type { ReceiveCommand } from "@/worker/git/operations/validation";
import { executeReceivePipeline } from "@/worker/git/receive/pipeline";
import {
  materializeAcceptedWrite,
  publishStagedSnapshot,
  releaseStagedSnapshot,
  stageSnapshotBundle,
  type StagedSnapshotBundle,
} from "@/worker/git/snapshot/materialize";
import { snapshotEventProbeEnabled } from "@/worker/git/snapshot/config";
import { resolveRepositoryRoute } from "@/worker/repositories/route";
import { isValidOwnerRepo, MAX_PATH_LEN } from "@/shared/web";
import { workerExecutionContext, type AppContext, type AppRouter } from "./hono";
import { authorizeInternalRequest } from "./internalAuth";

const INGESTION_REF = "refs/heads/main";
const MAX_FILES = 100;
const MAX_TOTAL_BYTES = 10 * 1024 * 1024;
// Bounded multipart framing/field overhead above the accepted file bytes.
const MAX_MULTIPART_BYTES = MAX_TOTAL_BYTES + 1024 * 1024;
const MAX_ACTOR_LENGTH = 200;
const MAX_IDEMPOTENCY_KEY_LENGTH = 200;
const MAX_MESSAGE_LENGTH = 500;
const MAX_PATH_SEGMENTS = 128;

type ParsedIngestionRequest = {
  files: IngestionFile[];
  expectedOid: string;
  actor: string;
  idempotencyKey: string;
  committedAtSeconds: number;
  message: string;
  historyMode: "append" | "epoch";
};

type StagedSnapshotOutcome =
  | { ok: true; staged: StagedSnapshotBundle | null }
  | { ok: false; error: unknown };

class IngestionRequestError extends Error {
  readonly status: number;
  readonly reason: string;

  constructor(status: number, reason: string, message: string) {
    super(message);
    this.status = status;
    this.reason = reason;
  }
}

function jsonResponse(body: unknown, status: number): Response {
  return Response.json(body, {
    status,
    headers: { "Cache-Control": "no-store" },
  });
}

function validatePath(path: string): void {
  if (
    !path ||
    new TextEncoder().encode(path).byteLength > MAX_PATH_LEN ||
    path.startsWith("/") ||
    path.includes("\\") ||
    path.includes("\0")
  ) {
    throw new IngestionRequestError(400, "invalid_path", "Invalid file path");
  }
  const segments = path.split("/");
  if (
    segments.length > MAX_PATH_SEGMENTS ||
    segments.some(
      (segment) =>
        !segment ||
        segment === "." ||
        segment === ".." ||
        segment === "_v" ||
        /[\r\n\p{Cf}]/u.test(segment) ||
        segment.endsWith(".") ||
        segment.endsWith(" ") ||
        [".git", "git~1"].includes(segment.normalize("NFKC").toLowerCase())
    ) ||
    segments[0] === "_"
  ) {
    throw new IngestionRequestError(400, "invalid_path", "Invalid file path");
  }
}

function requiredText(form: FormData, name: string, maxLength: number): string {
  const value = form.get(name);
  if (typeof value !== "string" || !value || value.length > maxLength || /[\0\r\n]/.test(value)) {
    throw new IngestionRequestError(400, `invalid_${name}`, `Invalid ${name}`);
  }
  return value;
}

async function parseIngestionRequest(request: Request): Promise<ParsedIngestionRequest> {
  if (!request.headers.get("Content-Type")?.toLowerCase().startsWith("multipart/form-data")) {
    throw new IngestionRequestError(415, "invalid_content_type", "Expected multipart/form-data");
  }
  const contentLength = Number(request.headers.get("Content-Length"));
  if (!Number.isSafeInteger(contentLength) || contentLength <= 0) {
    throw new IngestionRequestError(
      411,
      "content_length_required",
      "A bounded Content-Length is required"
    );
  }
  if (contentLength > MAX_MULTIPART_BYTES) {
    throw new IngestionRequestError(
      413,
      "multipart_too_large",
      `Multipart body exceeds ${MAX_MULTIPART_BYTES} bytes`
    );
  }

  let form: FormData;
  try {
    form = await request.formData();
  } catch {
    throw new IngestionRequestError(400, "malformed_multipart", "Malformed multipart body");
  }

  const expectedOid = requiredText(form, "expectedOid", 40).toLowerCase();
  if (!/^[0-9a-f]{40}$/.test(expectedOid)) {
    throw new IngestionRequestError(400, "invalid_expected_oid", "Invalid expectedOid");
  }
  const actor = requiredText(form, "actor", MAX_ACTOR_LENGTH);
  const idempotencyKey = requiredText(form, "idempotencyKey", MAX_IDEMPOTENCY_KEY_LENGTH);
  const messageValue = form.get("message");
  const message =
    messageValue === null ? "Publish folder" : requiredText(form, "message", MAX_MESSAGE_LENGTH);
  const committedAtRaw = requiredText(form, "committedAtSeconds", 16);
  const committedAtSeconds = Number(committedAtRaw);
  if (!Number.isSafeInteger(committedAtSeconds) || committedAtSeconds < 0) {
    throw new IngestionRequestError(400, "invalid_commit_timestamp", "Invalid committedAtSeconds");
  }
  const historyModeValue = form.get("historyMode");
  if (
    historyModeValue !== null &&
    (typeof historyModeValue !== "string" ||
      (historyModeValue !== "append" && historyModeValue !== "epoch"))
  ) {
    throw new IngestionRequestError(400, "invalid_history_mode", "Invalid historyMode");
  }
  const historyMode = historyModeValue ?? "append";

  const fileParts = form.getAll("files");
  if (fileParts.length === 0 || fileParts.length > MAX_FILES) {
    throw new IngestionRequestError(400, "invalid_file_count", `Expected 1-${MAX_FILES} files`);
  }
  const seenPaths = new Set<string>();
  const files: IngestionFile[] = [];
  let totalBytes = 0;
  for (const part of fileParts) {
    if (!(part instanceof File)) {
      throw new IngestionRequestError(400, "invalid_file_part", "Every files part must be a file");
    }
    validatePath(part.name);
    if (seenPaths.has(part.name)) {
      throw new IngestionRequestError(400, "duplicate_path", "Duplicate file path");
    }
    seenPaths.add(part.name);
    totalBytes += part.size;
    if (totalBytes > MAX_TOTAL_BYTES) {
      throw new IngestionRequestError(
        413,
        "folder_too_large",
        `Folder exceeds ${MAX_TOTAL_BYTES} bytes`
      );
    }
    files.push({ path: part.name, bytes: new Uint8Array(await part.arrayBuffer()) });
  }
  for (const path of seenPaths) {
    const segments = path.split("/");
    for (let index = 1; index < segments.length; index++) {
      if (seenPaths.has(segments.slice(0, index).join("/"))) {
        throw new IngestionRequestError(400, "path_conflict", "File and directory paths conflict");
      }
    }
  }

  return {
    files,
    expectedOid,
    actor,
    idempotencyKey,
    committedAtSeconds,
    message,
    historyMode,
  };
}

async function sha256(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asBufferSource(bytes))));
}

async function releaseStagedSnapshotSafely(
  staged: StagedSnapshotBundle | null,
  log: Logger
): Promise<void> {
  if (!staged) return;
  try {
    await releaseStagedSnapshot(staged);
  } catch (error) {
    // Snapshot state is a disposable projection and must not replace the
    // authoritative Git outcome or its original error.
    log.warn("ingestion:staged-snapshot-release-failed", { error: String(error) });
  }
}

async function handleIngestion(c: AppContext): Promise<Response> {
  const authResult = await authorizeInternalRequest(c);
  if (authResult) return authResult;

  const owner = c.req.param("owner") ?? "";
  const repo = c.req.param("repo") ?? "";
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return new Response("Not found\n", { status: 404 });
  }

  const log = c.var.logFor({ service: "Ingestion" });
  try {
    const input = await parseIngestionRequest(c.req.raw);
    const route = await resolveRepositoryRoute(c.env, owner, repo, {
      mode: "allow-d1-fallback",
      db: c.var.db,
      log,
    });
    if (!route) return new Response("Not found\n", { status: 404 });

    const ctx = workerExecutionContext(c);
    const stub = getRepoStub(c.env, route.doName);
    const limiter = c.var.limiter;
    const count = (op: string, amount = 1) => {
      if (countSubrequest(c.var.cacheCtx, amount)) return;
      log.warn("ingestion:soft-budget-exhausted", { op });
    };

    // Epoch ingestion still CASes the current ref through expectedOid, but
    // intentionally omits the parent so the accepted commit starts a new
    // history boundary that reachability GC can make physically effective.
    const parentOid =
      input.historyMode === "epoch" || input.expectedOid === zeroOid() ? null : input.expectedOid;
    const built = await buildIngestionCommit({
      files: input.files,
      parentOid,
      committedAtSeconds: input.committedAtSeconds,
      message: input.message,
    });
    const keyHash = await sha256(input.idempotencyKey);
    // Preserve the original append-mode fingerprint exactly so a retry of a
    // request accepted before historyMode existed still returns its durable
    // receipt. Epoch is a distinct operation and includes its mode marker.
    const fingerprintFields = [
      input.expectedOid,
      input.actor,
      input.committedAtSeconds,
      input.message,
      built.commitOid,
      built.treeOid,
    ];
    if (input.historyMode === "epoch") fingerprintFields.splice(4, 0, "epoch");
    const fingerprint = await sha256(JSON.stringify(fingerprintFields));

    count("do:get-ingestion-receipt");
    const priorReceipt = await limiter.run("do:get-ingestion-receipt", () =>
      stub.getIngestionReceipt(keyHash)
    );
    if (priorReceipt) {
      if (priorReceipt.fingerprint !== fingerprint) {
        return jsonResponse({ error: "Idempotency key conflict" }, 409);
      }
      if (!snapshotEventProbeEnabled(c.env)) {
        await materializeAcceptedWrite({
          env: c.env,
          repoId: route.doName,
          fact: priorReceipt.acceptedWrite,
          request: c.req.raw,
          ctx,
          limiter,
          log,
          source: { treeSha: built.treeOid, files: input.files },
        });
      }
      return jsonResponse(
        {
          acceptedWrite: priorReceipt.acceptedWrite,
          treeSha: priorReceipt.treeSha,
          replayed: true,
        },
        200
      );
    }

    count("do:begin-receive");
    const begin = await limiter.run<BeginReceiveResult>("do:begin-receive", async () => {
      return await stub.beginReceive();
    });
    if (!begin.ok) {
      return jsonResponse({ error: "Repository is busy", retryAfter: begin.retryAfter }, 503);
    }

    let pipelineStarted = false;
    let stagedSnapshotOutcome: Promise<StagedSnapshotOutcome> | undefined;
    try {
      const currentOid = begin.refs.find((ref) => ref.name === INGESTION_REF)?.oid ?? zeroOid();

      if (currentOid === built.commitOid) {
        count("do:abort-receive");
        await limiter.run("do:abort-receive", () => stub.abortReceive(begin.lease.token));
        return jsonResponse({ error: "Commit already accepted under another request" }, 409);
      }

      if (currentOid !== input.expectedOid) {
        count("do:abort-receive");
        await limiter.run("do:abort-receive", () => stub.abortReceive(begin.lease.token));
        return jsonResponse({ error: "Ref conflict", currentOid }, 409);
      }

      const commands: ReceiveCommand[] = [
        { oldOid: input.expectedOid, newOid: built.commitOid, ref: INGESTION_REF },
      ];
      const acceptedWrite = acceptedWriteFactsForCommands({
        repositoryId: route.repositoryId,
        commands,
        actor: input.actor,
        sourceSurface: "ingestion",
        idempotencyKey: input.idempotencyKey,
      })[0]!;
      const packRequest = new Request(c.req.url, {
        method: "POST",
        headers: { "Content-Length": String(built.pack.byteLength) },
        body: asBodyInit(built.pack),
        signal: c.req.raw.signal,
      });
      if (!packRequest.body) throw new Error("Failed to create ingestion pack stream");

      if (!snapshotEventProbeEnabled(c.env)) {
        // Start the independent immutable bundle upload before the accepted
        // pack pipeline; only the manifest is held behind authoritative commit.
        stagedSnapshotOutcome = stageSnapshotBundle({
          env: c.env,
          repoId: route.doName,
          fact: acceptedWrite,
          request: c.req.raw,
          ctx,
          limiter,
          log,
          source: { treeSha: built.treeOid, files: input.files },
        }).then(
          (staged) => ({ ok: true as const, staged }),
          (error: unknown) => ({ ok: false as const, error })
        );
      }
      pipelineStarted = true;
      const result = await executeReceivePipeline({
        env: c.env,
        repoId: route.doName,
        request: packRequest,
        ctx,
        packStream: packRequest.body,
        bytesConsumed: 0,
        stub,
        leaseToken: begin.lease.token,
        activeCatalog: begin.activeCatalog,
        commands,
        ingestionReceipt: {
          keyHash,
          fingerprint,
          acceptedWrite,
          treeSha: built.treeOid,
          createdAt: Date.now(),
        },
        acceptedWrites: [acceptedWrite],
        prebuiltPackArtifacts: {
          idx: built.idx,
          refs: built.refs,
          objectCount: built.objectCount,
        },
        log,
        cacheCtx: c.var.cacheCtx,
        limiter,
        countSubrequest: count,
      });
      if (!result.changed) {
        const staged = await stagedSnapshotOutcome;
        if (staged?.ok) await releaseStagedSnapshotSafely(staged.staged, log);
        if (staged && !staged.ok) {
          log.warn("ingestion:staged-snapshot-failed", { error: String(staged.error) });
        }
        return jsonResponse({ error: "Ref conflict" }, 409);
      }

      emitAcceptedWriteFacts(log, [acceptedWrite]);
      // The DO ref commit is authoritative. Activity metadata is a
      // best-effort visibility signal, so keep its D1 round trip outside the
      // request-to-serveable critical path. The response's D1 bookmark
      // intentionally does not cover this write, so updatedAt has no
      // read-your-writes guarantee.
      ctx.waitUntil(
        touchRepositoryUpdatedAt(c.var.db, route.repositoryId, Date.now()).catch((error) => {
          log.warn("ingestion:repo-updated-at-failed", {
            repositoryId: route.repositoryId,
            error: String(error),
          });
        })
      );
      log.info("ingestion:committed", {
        repositoryId: route.repositoryId,
        commitOid: built.commitOid,
        treeOid: built.treeOid,
        fileCount: input.files.length,
        objectCount: built.objectCount,
        packBytes: built.pack.byteLength,
      });
      const staged = await stagedSnapshotOutcome;
      if (staged && !staged.ok) {
        log.warn("ingestion:staged-snapshot-failed", {
          committed: true,
          error: String(staged.error),
        });
        throw staged.error;
      }
      if (staged?.staged) {
        // executeReceivePipeline has verified the pack graph and committed the
        // ref CAS to built.commitOid, which cryptographically binds treeOid.
        try {
          await publishStagedSnapshot(staged.staged);
        } catch (error) {
          log.warn("ingestion:staged-snapshot-failed", {
            committed: true,
            stage: "publish",
            error: String(error),
          });
          throw error;
        }
      }
      return jsonResponse({ acceptedWrite, treeSha: built.treeOid, replayed: false }, 201);
    } catch (error) {
      const staged = await stagedSnapshotOutcome;
      if (staged?.ok) await releaseStagedSnapshotSafely(staged.staged, log);
      if (staged && !staged.ok && staged.error !== error) {
        log.warn("ingestion:staged-snapshot-failed", { error: String(staged.error) });
      }
      if (!pipelineStarted) {
        count("do:abort-receive");
        await limiter
          .run("do:abort-receive", () => stub.abortReceive(begin.lease.token))
          .catch(() => {});
      }
      throw error;
    }
  } catch (error) {
    if (error instanceof IngestionRequestError) {
      log.warn("ingestion:rejected", { status: error.status, reason: error.reason });
      return jsonResponse({ error: error.message }, error.status);
    }
    log.error("ingestion:failed", { error: String(error) });
    return jsonResponse({ error: "Internal Server Error" }, 500);
  }
}

export function registerIngestionRoutes(router: AppRouter): void {
  router.post("/_internal/ingestion/:owner/:repo", handleIngestion);
}
