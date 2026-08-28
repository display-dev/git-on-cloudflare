import type { Logger } from "@/worker/common/logger";
import type { GcOperation } from "@/worker/git/maintenance/gcOperation";
import type { NativeReceiveProcessResult } from "@/worker/git/nativeReceive/types";
import { packIndexKey, packRefsKey } from "@/worker/keys";
import { GC_OPERATION_KEY } from "./catalog/gcOperation";
import { NativeProcessorError, runNativeExecution, cancelNativeExecution } from "./nativeReceive";
import { nativeExecutionKey } from "./nativeExecution";
import type { NativeExecutionRecord } from "@/worker/git/nativeReceive/execution";
import { consumeGcFault } from "./gcQualification";
import { SubrequestLimiter, MAX_SIMULTANEOUS_CONNECTIONS } from "@/worker/git/operations/limits";

export type GcNativeResult =
  | { status: "processed"; result: NativeReceiveProcessResult }
  | { status: "retry" }
  | { status: "invalid" }
  | { status: "rejected" };

/** Use the maintenance-only execution host with the existing native processor
 * and exact-key bridge. No receive admission, ref transaction, or accepted-write
 * publication is delegated to that host. */
export async function runGcNative(args: {
  ctx: DurableObjectState;
  env: Env;
  operationId: string;
  claimId: string;
  logger: Logger;
}): Promise<GcNativeResult> {
  const operation = await args.ctx.storage.transaction(async (transaction) => {
    if (await transaction.get<boolean>("repositoryDeleting")) return undefined;
    const current = await transaction.get<GcOperation>(GC_OPERATION_KEY);
    if (
      !current ||
      current.id !== args.operationId ||
      current.phase !== "index" ||
      current.claim?.id !== args.claimId ||
      current.claim.expiresAt <= Date.now() ||
      current.nativeStartedClaimId === args.claimId
    )
      return undefined;
    current.nativeStartedClaimId = args.claimId;
    await transaction.put(GC_OPERATION_KEY, current);
    return current;
  });
  if (!operation?.rewrite || !operation.snapshot || !operation.closure)
    return { status: "rejected" };
  args.logger.info("reachability-gc:native-dispatch", { inputBytes: operation.rewrite.packBytes });
  try {
    let settled = false;
    const processing = runNativeExecution({
      ctx: args.ctx,
      env: args.env,
      lane: "maintenance",
      claimId: args.claimId,
      onReady: async (wasRunning) => {
        await args.ctx.storage.transaction(async (transaction) => {
          const current = await transaction.get<GcOperation>(GC_OPERATION_KEY);
          if (
            current?.id !== operation.id ||
            current.claim?.id !== args.claimId ||
            current.claim.expiresAt <= Date.now()
          )
            throw new Error("GC native claim expired before dispatch");
          current.nativeReadyAt = Date.now();
          current.nativeWasRunning = wasRunning;
          await transaction.put(GC_OPERATION_KEY, current);
        });
      },
      request: {
        operationId: operation.id,
        inputPackKey: operation.inputPackKey,
        inputBytes: operation.rewrite.packBytes,
        activePacks: [],
        commands: [],
        outputPackKey: operation.outputPackKey,
        outputIdxKey: packIndexKey(operation.outputPackKey),
        outputRefsKey: packRefsKey(operation.outputPackKey),
        maintenance: {
          roots: operation.snapshot.refs.map((ref) => ref.oid),
          ...operation.closure,
          packSha1: operation.rewrite.packSha1,
          resultKey: operation.outputResultKey,
        },
      },
      bridgeProps: {
        operationId: operation.id,
        readKeys: [
          {
            key: operation.inputPackKey,
            expectedBytes: operation.rewrite.packBytes,
            expectedEtag: operation.rewrite.etag,
          },
        ],
        writeKeys: [
          { key: operation.outputPackKey, maxBytes: operation.rewrite.packBytes },
          { key: packIndexKey(operation.outputPackKey), maxBytes: 64 * 1024 * 1024 },
          { key: packRefsKey(operation.outputPackKey), maxBytes: 64 * 1024 * 1024 },
          { key: operation.outputResultKey, maxBytes: 64 * 1024 },
        ],
        requireWriteSha256: true,
        durableOutputOwner: true,
      },
    }).finally(() => {
      settled = true;
    });
    // Attach a rejection handler before awaiting the observation loop.
    void processing.catch(() => {});
    const fault = operation.qualification?.faults["during-native"];
    if (args.env.QUALIFICATION_MODE === "1" && fault && !fault.triggeredAt) {
      const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
      while (!settled) {
        const observed = await limiter.run(
          "do:observe-maintenance",
          async () =>
            await args.env.MAINTENANCE_CONTAINER_HOST.getByName(args.ctx.id.toString()).observe(
              operation.id
            )
        );
        if (observed && (await consumeGcFault(args.ctx, args.env, operation.id, "during-native"))) {
          const execution = await args.ctx.storage.get<NativeExecutionRecord>(
            nativeExecutionKey("maintenance")
          );
          if (
            execution?.identity.operationId === operation.id &&
            execution.identity.claimId === args.claimId
          )
            await cancelNativeExecution(args.ctx, args.env, execution.identity);
          await args.ctx.storage.transaction(async (transaction) => {
            const current = await transaction.get<GcOperation>(GC_OPERATION_KEY);
            const configured = current?.qualification?.faults["during-native"];
            if (current?.id === operation.id && configured?.triggeredAt) {
              configured.containerStoppedAt = Date.now();
              await transaction.put(GC_OPERATION_KEY, current);
            }
          });
          break;
        }
        if (!settled) await new Promise((resolve) => setTimeout(resolve, 1_000));
      }
    }
    const result = await processing;
    args.logger.info("reachability-gc:native-complete", {
      elapsedMs: result.elapsedMs,
      packBytes: result.packBytes,
    });
    return { status: "processed", result };
  } catch (error) {
    if (error instanceof NativeProcessorError && !error.retryable) return { status: "invalid" };
    args.logger.warn("reachability-gc:native-outcome-unresolved", {});
    return { status: "retry" };
  }
}
