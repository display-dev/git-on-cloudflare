import { createLogger } from "@/worker/common/logger";
import { nativeExecutionKey, authorizeNativeExecution } from "./nativeExecution";
import { cancelNativeExecution } from "./nativeReceive";
import type {
  NativeExecutionIdentity,
  NativeExecutionLane,
  NativeExecutionRecord,
} from "@/worker/git/nativeReceive/execution";

type InputHold = { operationId: string; deadlineAt: number };
const log = createLogger("info", { service: "NativeExecutionQualification" });
function enabled(env: Env): boolean {
  return env.QUALIFICATION_MODE === "1" && !!env.QUALIFICATION_SECRET;
}
function holdKey(lane: NativeExecutionLane): string {
  return `nativeInputHold:${lane}`;
}

export async function holdNativeInput(
  ctx: DurableObjectState,
  env: Env,
  lane: NativeExecutionLane,
  operationId: string,
  deadlineAt: number
): Promise<boolean> {
  if (
    !enabled(env) ||
    !/^[A-Za-z0-9_-]{1,100}$/.test(operationId) ||
    !Number.isSafeInteger(deadlineAt) ||
    deadlineAt <= Date.now() ||
    deadlineAt > Date.now() + 120_000
  )
    return false;
  return ctx.storage.transaction(async (store) => {
    if (await store.get("repositoryDeleting")) return false;
    const current = await store.get<InputHold>(holdKey(lane));
    if (current && current.deadlineAt > Date.now() && current.operationId !== operationId)
      return false;
    await store.put<InputHold>(holdKey(lane), { operationId, deadlineAt });
    log.info("native-probe:input-hold-armed", { lane });
    return true;
  });
}

export async function releaseNativeInput(
  ctx: DurableObjectState,
  env: Env,
  lane: NativeExecutionLane,
  operationId: string
): Promise<boolean> {
  if (!enabled(env)) return false;
  return ctx.storage.transaction(async (store) => {
    const hold = await store.get<InputHold>(holdKey(lane));
    if (hold && hold.operationId !== operationId) return false;
    await store.delete(holdKey(lane));
    log.info("native-probe:input-hold-released", { lane });
    return true;
  });
}

export async function nativeInputHeld(
  ctx: DurableObjectState,
  env: Env,
  identity: NativeExecutionIdentity
): Promise<boolean> {
  if (!enabled(env) || !(await authorizeNativeExecution(ctx, identity))) return false;
  const hold = await ctx.storage.get<InputHold>(holdKey(identity.lane));
  if (!hold || hold.operationId !== identity.operationId) return false;
  if (hold.deadlineAt <= Date.now()) {
    await releaseNativeInput(ctx, env, identity.lane, identity.operationId);
    return false;
  }
  return true;
}

export async function qualificationNativeExecutions(ctx: DurableObjectState, env: Env) {
  if (!enabled(env)) return null;
  const records = await Promise.all(
    (["foreground", "maintenance"] as const).map(async (lane) => {
      const record = await ctx.storage.get<NativeExecutionRecord>(nativeExecutionKey(lane));
      if (!record) return null;
      return {
        lane,
        operationId: record.identity.operationId,
        generation: record.identity.generation,
        grantSha256: record.identity.grantSha256,
        state: record.state,
        dispatchedAt: record.dispatchedAt ?? null,
        completedAt: record.completedAt ?? null,
        inputReadStartedAt: record.inputReadStartedAt ?? null,
        readRequests: record.readRequests ?? 0,
        declaredReadBytes: record.declaredReadBytes ?? 0,
        writeRequests: record.writeRequests ?? 0,
        completedWriteBytes: record.completedWriteBytes ?? 0,
      };
    })
  );
  return { schemaVersion: 1 as const, executions: records.filter((record) => record !== null) };
}

export async function cancelQualificationNativeExecution(
  ctx: DurableObjectState,
  env: Env,
  lane: NativeExecutionLane,
  operationId: string,
  generation: number
): Promise<boolean> {
  if (!enabled(env)) return false;
  const record = await ctx.storage.get<NativeExecutionRecord>(nativeExecutionKey(lane));
  if (
    !record ||
    record.state !== "active" ||
    record.identity.operationId !== operationId ||
    record.identity.generation !== generation ||
    !record.inputReadStartedAt
  )
    return false;
  await cancelNativeExecution(ctx, env, record.identity);
  log.info("native-probe:execution-cancelled", { lane, generation });
  return true;
}
