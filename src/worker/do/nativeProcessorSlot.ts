import {
  sameNativeExecution,
  type NativeExecutionIdentity,
  type NativeExecutionRecord,
} from "@/worker/git/nativeReceive/execution";

const SLOT_KEY = "nativeProcessorSlot";
// This lock serializes only configure/start/stop, never native processing. The
// durable record protects restart/replay; the lock closes await-boundary races
// between outbound-handler installation and Container destruction.
const lifecycleTails = new WeakMap<DurableObjectState, Promise<void>>();
const aborts = new Map<string, AbortController>();
function abortKey(ctx: DurableObjectState, identity: NativeExecutionIdentity): string {
  return `${ctx.id.toString()}:${identity.generation}`;
}
async function lifecycle<T>(ctx: DurableObjectState, action: () => Promise<T>): Promise<T> {
  const previous = lifecycleTails.get(ctx) ?? Promise.resolve();
  const result = previous.then(action);
  const tail = result.then(
    () => {},
    () => {}
  );
  lifecycleTails.set(ctx, tail);
  try {
    return await result;
  } finally {
    if (lifecycleTails.get(ctx) === tail) lifecycleTails.delete(ctx);
  }
}

export async function startNativeProcessorSlot(
  ctx: DurableObjectState,
  identity: NativeExecutionIdentity,
  configure: () => Promise<void>
): Promise<AbortSignal | null> {
  return lifecycle(ctx, async () => {
    const priorSlot = await ctx.storage.get<NativeExecutionRecord>(SLOT_KEY);
    if (priorSlot?.state === "revoked" && ctx.container?.running)
      await ctx.container.destroy("retired native execution cleanup");
    const admitted = await ctx.storage.transaction(async (store) => {
      const previous = await store.get<NativeExecutionRecord>(SLOT_KEY);
      if (
        identity.expiresAt <= Date.now() ||
        (await store.get<boolean>("nativeProcessorDeleting")) ||
        ((await store.get<number>("nativeProcessorRevokedThrough")) ?? 0) >= identity.generation ||
        (previous &&
          (identity.generation <= previous.identity.generation || previous.state === "active"))
      )
        return false;
      await store.put<NativeExecutionRecord>(SLOT_KEY, {
        identity,
        state: "active",
        drainUntil: 0,
      });
      return true;
    });
    if (!admitted) return null;
    // The dedicated host owns its expiry alarm. Install only after admission,
    // inside this lifecycle lock, so a stale dispatch cannot replace it.
    if (identity.lane === "maintenance") await ctx.storage.setAlarm(identity.expiresAt);
    const abort = new AbortController();
    aborts.set(abortKey(ctx, identity), abort);
    try {
      await configure();
    } catch (error) {
      abort.abort();
      // Retain the active record on an uncertain configuration failure. The
      // caller's exact-job stop must settle it before a successor can start.
      throw error;
    }
    return abort.signal;
  });
}

export async function finishNativeProcessorSlot(
  ctx: DurableObjectState,
  identity: NativeExecutionIdentity
): Promise<void> {
  await lifecycle(ctx, async () => {
    await ctx.storage.transaction(async (store) => {
      const record = await store.get<NativeExecutionRecord>(SLOT_KEY);
      if (record?.state === "active" && sameNativeExecution(record.identity, identity)) {
        record.state = "completed";
        await store.put(SLOT_KEY, record);
      }
    });
    aborts.delete(abortKey(ctx, identity));
  });
}

export async function stopNativeProcessorSlot(
  ctx: DurableObjectState,
  identity: NativeExecutionIdentity
): Promise<boolean> {
  return lifecycle(ctx, async () => {
    const stop = await ctx.storage.transaction(async (store) => {
      const current = await store.get<NativeExecutionRecord>(SLOT_KEY);
      // Persist cancel-before-start, but never touch a newer execution.
      if (current && current.identity.generation > identity.generation) return false;
      if (
        current &&
        current.identity.generation === identity.generation &&
        !sameNativeExecution(current.identity, identity)
      )
        return false;
      await store.put(
        "nativeProcessorRevokedThrough",
        Math.max(
          identity.generation,
          (await store.get<number>("nativeProcessorRevokedThrough")) ?? 0
        )
      );
      // A cancellation delivered before its start must retire that generation,
      // not destroy a different, earlier job that is still finishing.
      if (current && current.identity.generation < identity.generation) return false;
      await store.put<NativeExecutionRecord>(SLOT_KEY, {
        identity,
        state: "revoked",
        drainUntil: 0,
      });
      return true;
    });
    if (!stop) return false;
    aborts.get(abortKey(ctx, identity))?.abort("native execution cancelled");
    if (ctx.container?.running) await ctx.container.destroy("native execution cancelled");
    aborts.delete(abortKey(ctx, identity));
    return true;
  });
}

export async function deleteNativeProcessorSlot(ctx: DurableObjectState): Promise<void> {
  await lifecycle(ctx, async () => {
    await ctx.storage.put("nativeProcessorDeleting", true);
    const record = await ctx.storage.get<NativeExecutionRecord>(SLOT_KEY);
    if (record) {
      aborts.get(abortKey(ctx, record.identity))?.abort("repository deletion fence activated");
      await ctx.storage.put<NativeExecutionRecord>(SLOT_KEY, { ...record, state: "revoked" });
    }
    if (ctx.container?.running) await ctx.container.destroy("repository deletion fence activated");
  });
}

export async function currentNativeProcessor(
  ctx: DurableObjectState
): Promise<NativeExecutionRecord | undefined> {
  return ctx.storage.get<NativeExecutionRecord>(SLOT_KEY);
}
