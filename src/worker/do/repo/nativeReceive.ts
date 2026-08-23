import type { Logger } from "@/worker/common/logger";
import type {
  EnqueueNativeReceiveResult,
  MatchNativeReceiveOperationResult,
  NativeReceiveOperation,
  NativeReceiveOperationView,
  NativeReceiveProcessRequest,
  NativeReceiveProcessResult,
  RepositoryContainerBridgeProps,
} from "@/worker/git/nativeReceive/types";
import { z } from "zod";
import {
  isNativeReceiveTerminal,
  nativeReceiveOperationView,
} from "@/worker/git/nativeReceive/types";
import { packIndexKey } from "@/worker/keys";
import { MAX_SIMULTANEOUS_CONNECTIONS, SubrequestLimiter } from "@/worker/git/operations/limits";

import { finalizeReceiveState } from "./catalog/receive";
import { RECEIVE_LEASE_TTL_MS } from "./catalog/shared";
import { abortReceiveLease } from "./catalog/leases";
import {
  asTypedStorage,
  nativeReceiveOperationKey,
  receiveFinalizeIntentKey,
  type RepoStateSchema,
  type TypedStorage,
} from "./repoState";
import {
  RECOVERY_ESCALATION_ATTEMPTS,
  recoveryRetryDelayMs,
  scheduleAlarmIfSooner,
} from "./scheduler";

const MAX_RETAINED_OPERATIONS = 128;
const MAX_PROCESS_ATTEMPTS = 5;
const CONTAINER_PORT = 8080;
const CONTAINER_READY_ATTEMPTS = 120;
const CONTAINER_READY_INTERVAL_MS = 250;
const CONTAINER_RESPONSE_MAX_BYTES = 64 * 1024;
const OUTPUT_SIDECAR_MAX_BYTES = 1_000_000_000;
const LEASE_HEARTBEAT_MS = 30_000;
const NATIVE_READER_LEASE_TTL_MS = 2 * 60_000;
const PROCESSING_CLAIM_TTL_MS = 3 * 60_000;
const CONTAINER_PROCESS_TIMEOUT_MS = 20 * 60_000;

const nativeReceiveProcessResultSchema = z.object({
  operationId: z.string().min(1),
  packBytes: z.number().int().positive(),
  idxBytes: z.number().int().positive(),
  refsBytes: z.number().int().positive(),
  objectCount: z.number().int().nonnegative(),
  packSha1: z.string().regex(/^[0-9a-f]{40}$/),
  elapsedMs: z.number().int().nonnegative(),
  scratchBytes: z.number().int().nonnegative(),
  hydratedBytes: z.number().int().nonnegative().default(0),
  downloadedBytes: z.number().int().nonnegative().default(0),
  cacheHitBytes: z.number().int().nonnegative().default(0),
});

type NativeProcessor = (args: {
  ctx: DurableObjectState;
  request: NativeReceiveProcessRequest;
  bridgeProps: RepositoryContainerBridgeProps;
  signal: AbortSignal;
}) => Promise<NativeReceiveProcessResult>;

let nativeProcessorForTesting: NativeProcessor | undefined;
let pauseNextBeforeFinalizationForTesting = false;
let manualWakeupsForTesting = false;
let failNextAfterEnqueueStoreForTesting = false;
const activeProcessorAborts = new Map<string, AbortController>();

export const __test = {
  setNativeProcessor(processor: NativeProcessor): void {
    nativeProcessorForTesting = processor;
  },
  pauseNextBeforeFinalization(): void {
    pauseNextBeforeFinalizationForTesting = true;
  },
  useManualWakeups(): void {
    manualWakeupsForTesting = true;
  },
  failNextAfterEnqueueStore(): void {
    failNextAfterEnqueueStoreForTesting = true;
  },
  reset(): void {
    nativeProcessorForTesting = undefined;
    pauseNextBeforeFinalizationForTesting = false;
    manualWakeupsForTesting = false;
    failNextAfterEnqueueStoreForTesting = false;
  },
};

class NativeProcessorError extends Error {
  readonly retryable: boolean;
  readonly code: string;

  constructor(code: string, message: string, retryable: boolean) {
    super(message);
    this.name = "NativeProcessorError";
    this.retryable = retryable;
    this.code = code;
  }
}

function repositoryContainer(ctx: DurableObjectState): Container {
  if (!ctx.container) {
    throw new NativeProcessorError(
      "container_unavailable",
      "Repository Container binding is unavailable.",
      true
    );
  }
  return ctx.container;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function scheduleNativeWake(
  ctx: DurableObjectState,
  env: Env,
  scheduledTimeMs: number
): Promise<void> {
  if (manualWakeupsForTesting) return;
  await scheduleAlarmIfSooner(ctx, env, scheduledTimeMs);
}

function operationFingerprintMatches(
  existing: NativeReceiveOperation,
  input: NativeReceiveOperation
): boolean {
  return existing.fingerprint === input.fingerprint;
}

async function storeOperation(
  storage: DurableObjectStorage,
  operation: NativeReceiveOperation
): Promise<void> {
  await storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    await store.put(nativeReceiveOperationKey(operation.id), operation);
    const currentIndex = (await store.get("nativeReceiveOperationIndex")) ?? [];
    const nextIndex = [...currentIndex.filter((id) => id !== operation.id), operation.id];

    while (nextIndex.length > MAX_RETAINED_OPERATIONS) {
      const removableIndex = await findOldestTerminalOperationIndex(store, nextIndex);
      if (removableIndex < 0) {
        throw new Error("native receive operation ledger has no terminal entry to prune");
      }
      const [removedId] = nextIndex.splice(removableIndex, 1);
      if (removedId) await store.delete(nativeReceiveOperationKey(removedId));
    }
    await store.put("nativeReceiveOperationIndex", nextIndex);
  });
}

async function findOldestTerminalOperationIndex(
  store: TypedStorage<RepoStateSchema>,
  operationIds: string[]
): Promise<number> {
  for (let index = 0; index < operationIds.length; index++) {
    const operationId = operationIds[index];
    if (!operationId) continue;
    const operation = await store.get(nativeReceiveOperationKey(operationId));
    if (operation && isNativeReceiveTerminal(operation.state) && !operation.cleanupPending) {
      return index;
    }
  }
  return -1;
}

export async function getNativeReceiveOperationState(
  ctx: DurableObjectState,
  operationId: string
): Promise<NativeReceiveOperation | null> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  return (await store.get(nativeReceiveOperationKey(operationId))) ?? null;
}

export async function matchNativeReceiveOperationState(
  ctx: DurableObjectState,
  operationId: string,
  fingerprint: string
): Promise<MatchNativeReceiveOperationResult> {
  const operation = await getNativeReceiveOperationState(ctx, operationId);
  if (!operation) return { status: "not_found" };
  if (operation.fingerprint !== fingerprint) return { status: "conflict" };
  return { status: "match", operation: nativeReceiveOperationView(operation) };
}

export async function enqueueNativeReceiveState(args: {
  ctx: DurableObjectState;
  env: Env;
  operation: NativeReceiveOperation;
  logger: Logger;
}): Promise<EnqueueNativeReceiveResult> {
  const result = await args.ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) {
      return {
        status: "repository_deleting",
        message: "Repository deletion is in progress.",
      } satisfies EnqueueNativeReceiveResult;
    }

    const existing = await store.get(nativeReceiveOperationKey(args.operation.id));
    if (existing) {
      if (!operationFingerprintMatches(existing, args.operation)) {
        return {
          status: "conflict",
          message: "Operation id is already bound to a different receive.",
        } satisfies EnqueueNativeReceiveResult;
      }
      return {
        status: "replayed",
        operation: nativeReceiveOperationView(existing),
      } satisfies EnqueueNativeReceiveResult;
    }

    const lease = await store.get("receiveLease");
    if (!lease || lease.token !== args.operation.leaseToken || lease.expiresAt <= Date.now()) {
      return {
        status: "lease_mismatch",
        message: "Receive lease is no longer active.",
      } satisfies EnqueueNativeReceiveResult;
    }

    const index = (await store.get("nativeReceiveOperationIndex")) ?? [];
    const nextIndex = [...index.filter((id) => id !== args.operation.id), args.operation.id];
    while (nextIndex.length > MAX_RETAINED_OPERATIONS) {
      const removableIndex = await findOldestTerminalOperationIndex(store, nextIndex);
      if (removableIndex < 0) {
        return {
          status: "conflict",
          message: "Native receive operation ledger is full.",
        } satisfies EnqueueNativeReceiveResult;
      }
      const [removedId] = nextIndex.splice(removableIndex, 1);
      if (removedId) await store.delete(nativeReceiveOperationKey(removedId));
    }
    await store.put(nativeReceiveOperationKey(args.operation.id), args.operation);
    await store.put("nativeReceiveOperationIndex", nextIndex);
    return {
      status: "queued",
      operation: nativeReceiveOperationView(args.operation),
    } satisfies EnqueueNativeReceiveResult;
  });

  if (failNextAfterEnqueueStoreForTesting) {
    failNextAfterEnqueueStoreForTesting = false;
    throw new Error("injected lost enqueue response");
  }

  if (manualWakeupsForTesting) {
    return result;
  }
  if (
    (result.status === "queued" || result.status === "replayed") &&
    !isNativeReceiveTerminal(result.operation.state)
  ) {
    const dispatchAt = Date.now();
    const alarm = await scheduleAlarmIfSooner(args.ctx, args.env, dispatchAt);
    const alarmViable =
      alarm.scheduled || (alarm.prev !== null && alarm.prev <= dispatchAt + 1_000);
    let queueViable = false;
    try {
      await args.env.REPO_TASKS_QUEUE.send({
        kind: "native-receive",
        doId: args.ctx.id.toString(),
        operationId: args.operation.id,
      });
      queueViable = true;
    } catch (error) {
      args.logger.warn("native-receive:queue-dispatch-failed", {
        operationId: args.operation.id,
        error: String(error),
      });
    }
    if (!alarmViable && !queueViable) {
      const durableOperation = await args.ctx.storage.transaction(async (transaction) => {
        const store = asTypedStorage<RepoStateSchema>(transaction);
        const current = await store.get(nativeReceiveOperationKey(args.operation.id));
        if (
          current?.fingerprint === args.operation.fingerprint &&
          current.state === "staged" &&
          !current.claimId
        ) {
          await store.delete(nativeReceiveOperationKey(args.operation.id));
          const index = (await store.get("nativeReceiveOperationIndex")) ?? [];
          await store.put(
            "nativeReceiveOperationIndex",
            index.filter((id) => id !== args.operation.id)
          );
          return null;
        }
        return current ? nativeReceiveOperationView(current) : null;
      });
      if (durableOperation) {
        return { status: "replayed", operation: durableOperation };
      }
      return {
        status: "dispatch_failed",
        message: "Native receive was staged but durable dispatch could not be scheduled.",
      };
    }
    args.logger.info("native-receive:queued", {
      operationId: args.operation.id,
      inputBytes: args.operation.inputBytes,
      activePackCount: args.operation.activeCatalog.length,
    });
  }
  return result;
}

type ClaimOperationAttemptResult =
  | { status: "claimed"; operation: NativeReceiveOperation; claimId: string }
  | { status: "current"; operation: NativeReceiveOperation }
  | { status: "missing" };

async function claimOperationAttempt(
  ctx: DurableObjectState,
  operationId: string
): Promise<ClaimOperationAttemptResult> {
  const claimId = crypto.randomUUID();
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return { status: "missing" };
    const operation = await store.get(nativeReceiveOperationKey(operationId));
    if (!operation) return { status: "missing" };
    if (isNativeReceiveTerminal(operation.state)) return { status: "current", operation };
    const now = Date.now();
    if (
      operation.claimId &&
      operation.claimExpiresAt !== undefined &&
      operation.claimExpiresAt > now
    ) {
      return { status: "current", operation };
    }
    const lease = await store.get("receiveLease");
    if (!lease || lease.token !== operation.leaseToken) {
      const intent = await store.get(receiveFinalizeIntentKey(operation.leaseToken));
      if (operation.state !== "ready" && operation.state !== "finalizing" && !intent) {
        return { status: "current", operation };
      }
    }

    const claimed: NativeReceiveOperation = {
      ...operation,
      state:
        operation.state === "ready" || operation.state === "finalizing"
          ? "finalizing"
          : "processing",
      attempts:
        operation.state === "ready" || operation.state === "finalizing"
          ? operation.attempts
          : operation.attempts + 1,
      updatedAt: now,
      errorCode: undefined,
      claimId,
      claimExpiresAt: now + PROCESSING_CLAIM_TTL_MS,
    };
    await store.put("receiveLease", {
      token: operation.leaseToken,
      createdAt: lease?.token === operation.leaseToken ? lease.createdAt : now,
      expiresAt: now + RECEIVE_LEASE_TTL_MS,
    });
    await store.put(nativeReceiveOperationKey(operationId), claimed);
    return { status: "claimed", operation: claimed, claimId };
  });
}

async function renewOperationLease(
  ctx: DurableObjectState,
  operation: NativeReceiveOperation,
  claimId: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    const lease = await store.get("receiveLease");
    if (!lease || lease.token !== operation.leaseToken) return false;
    const current = await store.get(nativeReceiveOperationKey(operation.id));
    if (!current || current.claimId !== claimId) return false;
    const now = Date.now();
    await store.put("receiveLease", {
      ...lease,
      expiresAt: now + RECEIVE_LEASE_TTL_MS,
    });
    await store.put(nativeReceiveOperationKey(operation.id), {
      ...current,
      claimExpiresAt: now + PROCESSING_CLAIM_TTL_MS,
    });
    return true;
  });
}

async function storeClaimedOperation(
  ctx: DurableObjectState,
  operation: NativeReceiveOperation,
  claimId: string
): Promise<boolean> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const current = await store.get(nativeReceiveOperationKey(operation.id));
    if (!current || current.claimId !== claimId) return false;
    await store.put(nativeReceiveOperationKey(operation.id), operation);
    return true;
  });
}

async function releaseOperationClaim(
  ctx: DurableObjectState,
  operation: NativeReceiveOperation,
  claimId: string
): Promise<NativeReceiveOperation> {
  const released: NativeReceiveOperation = {
    ...operation,
    claimId: undefined,
    claimExpiresAt: undefined,
    updatedAt: Date.now(),
  };
  if (await storeClaimedOperation(ctx, released, claimId)) return released;
  return (await getNativeReceiveOperationState(ctx, operation.id)) ?? released;
}

async function acquireNativeCatalogReaderLease(
  ctx: DurableObjectState,
  operation: NativeReceiveOperation
): Promise<"acquired" | "catalog_superseded" | "reader_busy" | "repository_deleting"> {
  const now = Date.now();
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return "repository_deleting";
    const generationFloor = await store.get("nativeCatalogReaderGenerationFloor");
    if (typeof generationFloor === "number" && operation.catalogGeneration < generationFloor) {
      return "catalog_superseded";
    }
    const existing = await store.get("nativeCatalogReaderLease");
    if (existing && existing.expiresAt > now && existing.token !== operation.id) {
      return "reader_busy";
    }
    await store.put("nativeCatalogReaderLease", {
      token: operation.id,
      createdAt: existing?.token === operation.id ? existing.createdAt : now,
      expiresAt: now + NATIVE_READER_LEASE_TTL_MS,
      operation: "native-reader",
      generation: operation.catalogGeneration,
    });
    return "acquired";
  });
}

async function renewNativeCatalogReaderLease(
  ctx: DurableObjectState,
  operation: NativeReceiveOperation
): Promise<boolean> {
  const now = Date.now();
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    if (await store.get("repositoryDeleting")) return false;
    const existing = await store.get("nativeCatalogReaderLease");
    if (
      !existing ||
      existing.token !== operation.id ||
      existing.generation !== operation.catalogGeneration ||
      existing.expiresAt <= now
    ) {
      return false;
    }
    await store.put("nativeCatalogReaderLease", {
      ...existing,
      expiresAt: now + NATIVE_READER_LEASE_TTL_MS,
    });
    return true;
  });
}

async function releaseNativeCatalogReaderLease(
  ctx: DurableObjectState,
  operationId: string
): Promise<void> {
  await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const existing = await store.get("nativeCatalogReaderLease");
    if (existing?.token === operationId) await store.delete("nativeCatalogReaderLease");
  });
}

export async function canDeleteSupersededGenerationState(
  ctx: DurableObjectState,
  generation?: number
): Promise<{ safe: boolean; retryAfterSeconds?: number }> {
  return await ctx.storage.transaction(async (transaction) => {
    const store = asTypedStorage<RepoStateSchema>(transaction);
    const lease = await store.get("nativeCatalogReaderLease");
    const now = Date.now();
    const repositoryReaders = ((await store.get("repositoryReadLeases")) ?? []).filter(
      (reader) => reader.expiresAt > now
    );
    if (repositoryReaders.length > 0) {
      await store.put("repositoryReadLeases", repositoryReaders);
      return {
        safe: false,
        retryAfterSeconds: Math.max(
          1,
          Math.ceil((Math.max(...repositoryReaders.map((reader) => reader.expiresAt)) - now) / 1000)
        ),
      };
    }
    await store.delete("repositoryReadLeases");
    if (
      lease &&
      lease.expiresAt > now &&
      (typeof generation !== "number" || lease.generation < generation)
    ) {
      return {
        safe: false,
        retryAfterSeconds: Math.max(1, Math.ceil((lease.expiresAt - now) / 1000)),
      };
    }
    if (typeof generation === "number") {
      const floor = await store.get("nativeCatalogReaderGenerationFloor");
      if (floor === undefined || floor < generation) {
        await store.put("nativeCatalogReaderGenerationFloor", generation);
      }
    }
    return { safe: true };
  });
}

function bridgeProps(operation: NativeReceiveOperation): RepositoryContainerBridgeProps {
  const activeBytes = operation.activeCatalog.reduce((total, pack) => total + pack.packBytes, 0);
  const maximumOutputPackBytes = operation.inputBytes + activeBytes;
  return {
    operationId: operation.id,
    readKeys: [
      {
        key: operation.inputPackKey,
        expectedBytes: operation.inputBytes,
        expectedEtag: operation.inputEtag,
      },
      ...operation.activeCatalog.flatMap((pack) => [
        { key: pack.packKey, expectedBytes: pack.packBytes },
        { key: packIndexKey(pack.packKey), expectedBytes: pack.idxBytes },
      ]),
    ],
    writeKeys: [
      { key: operation.outputPackKey, maxBytes: maximumOutputPackBytes },
      { key: operation.outputIdxKey, maxBytes: OUTPUT_SIDECAR_MAX_BYTES },
      { key: operation.outputRefsKey, maxBytes: OUTPUT_SIDECAR_MAX_BYTES },
    ],
  };
}

function processRequest(operation: NativeReceiveOperation): NativeReceiveProcessRequest {
  return {
    operationId: operation.id,
    inputPackKey: operation.inputPackKey,
    inputBytes: operation.inputBytes,
    activePacks: operation.activeCatalog.map((pack) => ({
      packKey: pack.packKey,
      packBytes: pack.packBytes,
      idxBytes: pack.idxBytes,
    })),
    commands: operation.commands,
    outputPackKey: operation.outputPackKey,
    outputIdxKey: operation.outputIdxKey,
    outputRefsKey: operation.outputRefsKey,
  };
}

async function waitForContainerReady(container: Container): Promise<void> {
  const port = container.getTcpPort(CONTAINER_PORT);
  for (let attempt = 0; attempt < CONTAINER_READY_ATTEMPTS; attempt++) {
    try {
      const response = await port.fetch("http://container/ready", { method: "GET" });
      if (response.status === 200) {
        await response.body?.cancel();
        return;
      }
      await response.body?.cancel();
    } catch {}
    await delay(CONTAINER_READY_INTERVAL_MS);
  }
  throw new NativeProcessorError(
    "container_not_ready",
    "Repository Container did not become ready.",
    true
  );
}

async function parseBoundedJSON(response: Response): Promise<unknown> {
  const contentLength = Number(response.headers.get("Content-Length"));
  if (Number.isFinite(contentLength) && contentLength > CONTAINER_RESPONSE_MAX_BYTES) {
    throw new NativeProcessorError(
      "container_response_too_large",
      "Repository Container response exceeded its bound.",
      false
    );
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > CONTAINER_RESPONSE_MAX_BYTES) {
    throw new NativeProcessorError(
      "container_response_too_large",
      "Repository Container response exceeded its bound.",
      false
    );
  }
  try {
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new NativeProcessorError(
      "container_invalid_response",
      "Repository Container returned invalid JSON.",
      false
    );
  }
}

async function runContainerProcessor(args: {
  ctx: DurableObjectState;
  request: NativeReceiveProcessRequest;
  bridgeProps: RepositoryContainerBridgeProps;
}): Promise<NativeReceiveProcessResult> {
  const processorAbort = new AbortController();
  const processorKey = args.ctx.id.toString();
  activeProcessorAborts.set(processorKey, processorAbort);
  const signal = AbortSignal.any([
    processorAbort.signal,
    AbortSignal.timeout(CONTAINER_PROCESS_TIMEOUT_MS),
  ]);
  if (nativeProcessorForTesting) {
    try {
      return await nativeProcessorForTesting({ ...args, signal });
    } finally {
      if (activeProcessorAborts.get(processorKey) === processorAbort) {
        activeProcessorAborts.delete(processorKey);
      }
    }
  }

  const container = repositoryContainer(args.ctx);
  const bridge = args.ctx.exports.RepositoryContainerBridge({ props: args.bridgeProps });
  await container.interceptOutboundHttp("repo-r2.internal", bridge);
  if (!container.running) {
    container.start({ enableInternet: false });
  }

  try {
    await waitForContainerReady(container);
    const response = await container.getTcpPort(CONTAINER_PORT).fetch("http://container/process", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args.request),
      signal,
    });
    const payload = await parseBoundedJSON(response);
    if (response.status !== 200) {
      throw new NativeProcessorError(
        response.status >= 500 ? "container_transient_failure" : "native_git_rejected",
        "Repository Container rejected the receive.",
        response.status >= 500
      );
    }
    const parsed = nativeReceiveProcessResultSchema.safeParse(payload);
    if (!parsed.success || parsed.data.operationId !== args.request.operationId) {
      throw new NativeProcessorError(
        "container_invalid_response",
        "Repository Container returned an invalid result.",
        false
      );
    }
    return parsed.data;
  } catch (error) {
    if (error instanceof NativeProcessorError) throw error;
    throw new NativeProcessorError(
      "container_transport_failure",
      "Repository Container request failed.",
      true
    );
  } finally {
    if (activeProcessorAborts.get(processorKey) === processorAbort) {
      activeProcessorAborts.delete(processorKey);
    }
  }
}

export async function stopNativeReceiveContainerState(ctx: DurableObjectState): Promise<void> {
  activeProcessorAborts.get(ctx.id.toString())?.abort("repository deletion fence activated");
  if (ctx.container?.running) {
    await ctx.container.destroy("repository deletion fence activated");
  }
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  await store.delete("nativeCatalogReaderLease");
}

async function deleteOperationObjects(args: {
  env: Env;
  operation: NativeReceiveOperation;
  includeOutputs: boolean;
  logger: Logger;
}): Promise<void> {
  const keys = args.includeOutputs
    ? [
        args.operation.inputPackKey,
        args.operation.outputPackKey,
        args.operation.outputIdxKey,
        args.operation.outputRefsKey,
      ]
    : [args.operation.inputPackKey];
  const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
  await limiter.run("r2:delete-native-receive-objects", () => args.env.REPO_BUCKET.delete(keys));
  args.logger.info("native-receive:cleanup", {
    operationId: args.operation.id,
    inputDeleted: true,
    outputsDeleted: args.includeOutputs,
  });
}

async function markTerminal(args: {
  ctx: DurableObjectState;
  operation: NativeReceiveOperation;
  claimId: string;
  state: "committed" | "aborted" | "failed";
  result?: NativeReceiveOperation["result"];
  errorCode?: string;
}): Promise<NativeReceiveOperation> {
  const terminal: NativeReceiveOperation = {
    ...args.operation,
    state: args.state,
    updatedAt: Date.now(),
    result: args.result,
    errorCode: args.errorCode,
    cleanupPending: true,
    claimId: undefined,
    claimExpiresAt: undefined,
  };
  if (!(await storeClaimedOperation(args.ctx, terminal, args.claimId))) {
    const current = await getNativeReceiveOperationState(args.ctx, args.operation.id);
    if (current && isNativeReceiveTerminal(current.state)) return current;
    throw new Error("FUBAR: native receive operation claim was lost before terminal state");
  }
  return terminal;
}

async function completeOperationCleanup(args: {
  ctx: DurableObjectState;
  env: Env;
  operation: NativeReceiveOperation;
  includeOutputs: boolean;
  logger: Logger;
}): Promise<NativeReceiveOperation> {
  try {
    await deleteOperationObjects(args);
    const cleaned: NativeReceiveOperation = {
      ...args.operation,
      cleanupPending: false,
      updatedAt: Date.now(),
    };
    await storeOperation(args.ctx.storage, cleaned);
    return cleaned;
  } catch (error) {
    args.logger.warn("native-receive:cleanup-deferred", {
      operationId: args.operation.id,
      error: String(error),
    });
    await scheduleNativeWake(args.ctx, args.env, Date.now() + 1_000);
    return args.operation;
  }
}

async function reconcileFinalizingOperation(args: {
  ctx: DurableObjectState;
  env: Env;
  operation: NativeReceiveOperation;
  claimId: string;
  logger: Logger;
}): Promise<NativeReceiveOperation | null> {
  const processorResult = args.operation.processorResult;
  if (!processorResult) return null;
  const finalized = await finalizeReceiveState({
    ctx: args.ctx,
    env: args.env,
    token: args.operation.leaseToken,
    commands: args.operation.commands,
    stagedPack: {
      packKey: args.operation.outputPackKey,
      packBytes: processorResult.packBytes,
      idxBytes: processorResult.idxBytes,
      objectCount: processorResult.objectCount,
    },
    acceptedWrites:
      args.operation.acceptedWrites.length > 0 ? args.operation.acceptedWrites : undefined,
    logger: args.logger,
  });
  if (finalized.status === "committed") {
    const committed = await markTerminal({
      ctx: args.ctx,
      operation: args.operation,
      claimId: args.claimId,
      state: "committed",
      result: {
        statuses: finalized.statuses,
        changed: finalized.changed,
        empty: finalized.empty,
        packKey: args.operation.outputPackKey,
        packBytes: processorResult.packBytes,
      },
    });
    return await completeOperationCleanup({
      ctx: args.ctx,
      env: args.env,
      operation: committed,
      includeOutputs: false,
      logger: args.logger,
    });
  }

  const aborted = await markTerminal({
    ctx: args.ctx,
    operation: args.operation,
    claimId: args.claimId,
    state: "aborted",
    result: {
      statuses: finalized.status === "ref_conflict" ? finalized.statuses : [],
      changed: false,
      empty: false,
    },
    errorCode: finalized.status,
  });
  return await completeOperationCleanup({
    ctx: args.ctx,
    env: args.env,
    operation: aborted,
    includeOutputs: true,
    logger: args.logger,
  });
}

export async function runNativeReceiveOperationState(args: {
  ctx: DurableObjectState;
  env: Env;
  operationId: string;
  logger: Logger;
}): Promise<NativeReceiveOperationView | null> {
  const claim = await claimOperationAttempt(args.ctx, args.operationId);
  if (claim.status === "missing") return null;
  if (claim.status === "current") {
    if (!isNativeReceiveTerminal(claim.operation.state) && claim.operation.claimExpiresAt) {
      await scheduleNativeWake(args.ctx, args.env, claim.operation.claimExpiresAt + 1);
    }
    return nativeReceiveOperationView(claim.operation);
  }
  const { operation, claimId } = claim;

  if (operation.state === "finalizing") {
    try {
      const reconciled = await reconcileFinalizingOperation({ ...args, operation, claimId });
      if (reconciled) return nativeReceiveOperationView(reconciled);
    } catch {
      // The durable operation and finalize intent contain everything needed
      // for recovery. Keep ordinary logs closed over bounded state only.
    }
    const finalizeAttempts = (operation.finalizeAttempts ?? 0) + 1;
    const escalate =
      finalizeAttempts >= RECOVERY_ESCALATION_ATTEMPTS && !operation.finalizeEscalated;
    const released = await releaseOperationClaim(
      args.ctx,
      {
        ...operation,
        finalizeAttempts,
        finalizeEscalated: operation.finalizeEscalated || escalate,
      },
      claimId
    );
    const fields = { operationId: operation.id, attempts: finalizeAttempts, retryable: true };
    if (escalate) {
      args.logger.error("native-receive:finalization-recovery-escalated", fields);
    } else {
      args.logger.warn("native-receive:finalization-recovery-pending", fields);
    }
    await scheduleNativeWake(
      args.ctx,
      args.env,
      Date.now() + recoveryRetryDelayMs(finalizeAttempts)
    );
    return nativeReceiveOperationView(released);
  }

  args.logger.info("native-receive:processing", {
    operationId: operation.id,
    attempt: operation.attempts,
    inputBytes: operation.inputBytes,
    activePackCount: operation.activeCatalog.length,
  });

  let nativeResult: NativeReceiveProcessResult;
  const readerLeaseResult = await acquireNativeCatalogReaderLease(args.ctx, operation);
  if (readerLeaseResult === "catalog_superseded") {
    args.logger.warn("native-receive:catalog-superseded", {
      operationId: operation.id,
      catalogGeneration: operation.catalogGeneration,
      retryable: false,
    });
    const failed = await markTerminal({
      ctx: args.ctx,
      operation,
      claimId,
      state: "failed",
      errorCode: "catalog_superseded",
    });
    await abortReceiveLease(args.ctx, operation.leaseToken);
    const cleaned = await completeOperationCleanup({
      ctx: args.ctx,
      env: args.env,
      operation: failed,
      includeOutputs: true,
      logger: args.logger,
    });
    return nativeReceiveOperationView(cleaned);
  }
  if (readerLeaseResult !== "acquired") {
    args.logger.info("native-receive:reader-pending", {
      operationId: operation.id,
      reason: readerLeaseResult,
      retryable: true,
    });
    const released = await releaseOperationClaim(
      args.ctx,
      {
        ...operation,
        state: "staged",
        attempts: Math.max(0, operation.attempts - 1),
      },
      claimId
    );
    await scheduleNativeWake(args.ctx, args.env, Date.now() + 1_000);
    return nativeReceiveOperationView(released);
  }
  let heartbeatFailure: Error | undefined;
  let heartbeatTail = Promise.resolve();
  const heartbeat = setInterval(() => {
    heartbeatTail = heartbeatTail
      .then(async () => {
        const operationRenewed = await renewOperationLease(args.ctx, operation, claimId);
        const readerRenewed = await renewNativeCatalogReaderLease(args.ctx, operation);
        if (!operationRenewed || !readerRenewed) {
          heartbeatFailure = new Error("native receive lease ownership was lost");
          activeProcessorAborts.get(args.ctx.id.toString())?.abort("native lease renewal failed");
        }
      })
      .catch((error) => {
        heartbeatFailure = error instanceof Error ? error : new Error(String(error));
      });
  }, LEASE_HEARTBEAT_MS);
  try {
    nativeResult = await runContainerProcessor({
      ctx: args.ctx,
      request: processRequest(operation),
      bridgeProps: bridgeProps(operation),
    });
    await heartbeatTail;
    if (heartbeatFailure) throw heartbeatFailure;
  } catch (error) {
    const processorError =
      error instanceof NativeProcessorError
        ? error
        : new NativeProcessorError("native_processor_failed", String(error), true);
    args.logger.warn("native-receive:processing-failed", {
      operationId: operation.id,
      attempt: operation.attempts,
      code: processorError.code,
      retryable: processorError.retryable,
    });
    if (processorError.retryable && operation.attempts < MAX_PROCESS_ATTEMPTS) {
      const retrying: NativeReceiveOperation = {
        ...operation,
        state: "staged",
        updatedAt: Date.now(),
        errorCode: processorError.code,
        claimId: undefined,
        claimExpiresAt: undefined,
      };
      if (!(await storeClaimedOperation(args.ctx, retrying, claimId))) {
        return nativeReceiveOperationView(
          (await getNativeReceiveOperationState(args.ctx, operation.id)) ?? retrying
        );
      }
      const retryDelay = Math.min(30_000, 2 ** (operation.attempts - 1) * 1_000);
      await scheduleNativeWake(args.ctx, args.env, Date.now() + retryDelay);
      return nativeReceiveOperationView(retrying);
    }

    const failed = await markTerminal({
      ctx: args.ctx,
      operation,
      claimId,
      state: "failed",
      errorCode: processorError.code,
    });
    await abortReceiveLease(args.ctx, operation.leaseToken);
    const cleaned = await completeOperationCleanup({
      ctx: args.ctx,
      env: args.env,
      operation: failed,
      includeOutputs: true,
      logger: args.logger,
    });
    return nativeReceiveOperationView(cleaned);
  } finally {
    clearInterval(heartbeat);
    await releaseNativeCatalogReaderLease(args.ctx, operation.id);
  }

  args.logger.info("native-receive:processing-complete", {
    operationId: operation.id,
    elapsedMs: nativeResult.elapsedMs,
    hydratedBytes: nativeResult.hydratedBytes,
    downloadedBytes: nativeResult.downloadedBytes,
    cacheHitBytes: nativeResult.cacheHitBytes,
  });

  const ready: NativeReceiveOperation = {
    ...operation,
    state: "ready",
    updatedAt: Date.now(),
    result: {
      statuses: [],
      changed: false,
      empty: false,
      packKey: operation.outputPackKey,
      packBytes: nativeResult.packBytes,
    },
    processorResult: nativeResult,
    claimId: undefined,
    claimExpiresAt: undefined,
  };
  if (!(await storeClaimedOperation(args.ctx, ready, claimId))) {
    const current = await getNativeReceiveOperationState(args.ctx, operation.id);
    return current ? nativeReceiveOperationView(current) : null;
  }
  await scheduleNativeWake(args.ctx, args.env, Date.now());
  if (pauseNextBeforeFinalizationForTesting) {
    pauseNextBeforeFinalizationForTesting = false;
    return nativeReceiveOperationView(ready);
  }
  return await runNativeReceiveOperationState(args);
}

export async function resumeNativeReceiveFromAlarm(args: {
  ctx: DurableObjectState;
  env: Env;
  logger: Logger;
}): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const operationIds = (await store.get("nativeReceiveOperationIndex")) ?? [];
  for (let index = operationIds.length - 1; index >= 0; index--) {
    const operationId = operationIds[index];
    if (!operationId) continue;
    const operation = await store.get(nativeReceiveOperationKey(operationId));
    if (!operation) continue;
    if (isNativeReceiveTerminal(operation.state)) {
      if (!operation.cleanupPending) continue;
      await completeOperationCleanup({
        ctx: args.ctx,
        env: args.env,
        operation,
        includeOutputs: operation.state !== "committed",
        logger: args.logger,
      });
      return true;
    }
    await runNativeReceiveOperationState({ ...args, operationId });
    return true;
  }
  return false;
}
