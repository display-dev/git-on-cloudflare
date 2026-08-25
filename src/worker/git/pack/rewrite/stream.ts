import type {
  OrderedPackSnapshot,
  OrderedPackSnapshotEntry,
} from "@/worker/git/operations/fetch/types";
import type { Logger } from "@/worker/common/logger";

import { createDigestStream } from "@/worker/common";
import { isResolveAbortedError } from "@/worker/git/pack/indexer/resolve/errors";
import { encodeOfsDeltaDistance } from "../packMeta";
import { readPackRange } from "../packMeta";
import {
  WHOLE_PACK_MAX_BYTES,
  buildPackHeader,
  countRewriteSubrequest,
  type PackReadState,
  type RewriteOptions,
  type SelectionTable,
} from "./shared";

const PASSTHROUGH_RANGE_BYTES = 8 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Entry header construction
// ---------------------------------------------------------------------------

function buildEntryHeaderBytes(table: SelectionTable, sel: number): Uint8Array | undefined {
  const type = table.typeCodes[sel];
  const svStart = sel * 5;
  const svLen = table.sizeVarLens[sel];
  if (svLen === 0) return undefined;

  if (type === 6) {
    const base = table.baseSlots[sel];
    if (base < 0) return undefined;
    const distance = table.outputOffsets[sel] - table.outputOffsets[base];
    const distBytes = encodeOfsDeltaDistance(distance);
    const out = new Uint8Array(svLen + distBytes.length);
    out.set(table.sizeVarBuf.subarray(svStart, svStart + svLen), 0);
    out.set(distBytes, svLen);
    return out;
  }

  if (type === 7) {
    if (!table.baseOidRaw) return undefined;
    const baseOidBytes = table.baseOidRaw.subarray(sel * 20, sel * 20 + 20);
    const out = new Uint8Array(svLen + 20);
    out.set(table.sizeVarBuf.subarray(svStart, svStart + svLen), 0);
    out.set(baseOidBytes, svLen);
    return out;
  }

  // Non-delta: just the size varint (subarray is safe — table is immutable during streaming)
  return table.sizeVarBuf.subarray(svStart, svStart + svLen);
}

// ---------------------------------------------------------------------------
// Payload emission
// ---------------------------------------------------------------------------

async function emitPackPayload(
  controller: ReadableStreamDefaultController<Uint8Array>,
  writer: WritableStreamDefaultWriter<Uint8Array>,
  table: SelectionTable,
  sel: number,
  state: PackReadState | undefined,
  waitForCapacity: () => Promise<void>
): Promise<void> {
  const syntheticPayload = table.syntheticPayloads[sel];
  if (syntheticPayload) {
    await writer.write(syntheticPayload);
    await waitForCapacity();
    controller.enqueue(syntheticPayload);
    return;
  }

  if (!state) {
    throw new Error(
      `rewrite: missing read state for pack#${table.packSlots[sel]} entry#${table.entryIndices[sel]}`
    );
  }

  const payloadStart = table.offsets[sel] + table.headerLens[sel];
  let bytesLeft = table.payloadLens[sel];
  if (bytesLeft <= 0) return;

  if (state.wholePack) {
    const payload = state.wholePack.subarray(payloadStart, payloadStart + bytesLeft);
    await writer.write(payload);
    await waitForCapacity();
    controller.enqueue(payload);
    return;
  }

  let currentOffset = payloadStart;
  while (bytesLeft > 0) {
    let window = await state.reader.readWindow(currentOffset, bytesLeft);
    if (window.length === 0) {
      window = await state.reader.readRange(currentOffset, Math.min(bytesLeft, 1));
    }
    if (window.length === 0) {
      throw new Error(
        `rewrite: unexpected EOF while streaming pack#${table.packSlots[sel]} entry#${table.entryIndices[sel]}`
      );
    }

    await writer.write(window);
    await waitForCapacity();
    controller.enqueue(window);
    currentOffset += window.length;
    bytesLeft -= window.length;
  }
}

// ---------------------------------------------------------------------------
// Passthrough stream (single pack, all objects selected)
// ---------------------------------------------------------------------------

export async function passthroughSinglePack(
  env: Env,
  snapshotPack: OrderedPackSnapshotEntry,
  readState: PackReadState,
  outputWriter: WritableStreamDefaultWriter<Uint8Array>,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<"completed" | "aborted"> {
  const digestStream = createDigestStream("SHA-1");
  const writer = digestStream.getWriter();

  if (options?.signal?.aborted) {
    await writer.abort();
    return "aborted";
  }

  const emit = async (chunk: Uint8Array) => {
    await writer.write(chunk);
    await outputWriter.write(chunk);
  };

  options?.onProgress?.(`Enumerating objects: ${snapshotPack.idx.count}, from 1 packs\n`);

  if (readState.wholePack) {
    if (readState.wholePack.length < 20) {
      throw new Error("rewrite: passthrough pack read failed");
    }
    if (options?.signal?.aborted) {
      await writer.abort();
      return "aborted";
    }
    await emit(readState.wholePack.subarray(0, readState.wholePack.length - 20));
  } else if (snapshotPack.packBytes <= WHOLE_PACK_MAX_BYTES) {
    throw new Error("rewrite: missing whole-pack preload for passthrough");
  } else {
    const bodyBytes = snapshotPack.packBytes - 20;
    if (bodyBytes <= 0) throw new Error("rewrite: truncated passthrough pack");
    let offset = 0;
    let ranges = 0;
    while (offset < bodyBytes) {
      if (options?.signal?.aborted) {
        await writer.abort();
        throw new Error("rewrite: passthrough aborted");
      }
      const length = Math.min(PASSTHROUGH_RANGE_BYTES, bodyBytes - offset);
      const bytes = await readPackRange(env, snapshotPack.packKey, offset, length, {
        limiter: options!.limiter,
        signal: options?.signal,
        log,
        exactLength: true,
        countSubrequest: (n?: number) =>
          countRewriteSubrequest(
            log,
            warnedFlags,
            options,
            `rewrite-passthrough-range:${snapshotPack.packKey}`,
            { op: "r2:get-range", offset, length },
            n
          ),
      });
      if (!bytes) throw new Error("rewrite: passthrough range unavailable");
      await emit(bytes);
      offset += bytes.byteLength;
      ranges++;
    }
    log.info("rewrite:passthrough-ranges-complete", {
      bytes: bodyBytes,
      ranges,
    });
  }

  await writer.close();
  options?.onProgress?.(
    `Counting objects: 100% (${snapshotPack.idx.count}/${snapshotPack.idx.count}), done.\n`
  );
  await outputWriter.write(new Uint8Array(await digestStream.digest));
  return "completed";
}

export function createPassthroughStream(args: {
  env: Env;
  snapshotPack: OrderedPackSnapshotEntry;
  readState: PackReadState;
  log: Logger;
  warnedFlags: Set<string>;
  options?: RewriteOptions;
  onComplete?: () => void;
}): ReadableStream<Uint8Array> {
  const output = new TransformStream<Uint8Array, Uint8Array>();
  const outputWriter = output.writable.getWriter();
  void (async () => {
    try {
      const status = await passthroughSinglePack(
        args.env,
        args.snapshotPack,
        args.readState,
        outputWriter,
        args.log,
        args.warnedFlags,
        args.options
      );
      if (status === "aborted") {
        args.log.debug("rewrite:passthrough-aborted");
        await outputWriter.close();
        return;
      }
      args.onComplete?.();
      await outputWriter.close();
    } catch (error) {
      if (
        isResolveAbortedError(error) ||
        args.options?.signal?.aborted ||
        (error instanceof Error && error.message === "rewrite: passthrough aborted")
      ) {
        args.log.debug("rewrite:passthrough-aborted");
        await outputWriter.abort(error).catch(() => undefined);
        return;
      }
      args.log.error("rewrite:passthrough-error", { error: String(error) });
      await outputWriter.abort(error).catch(() => undefined);
    }
  })();
  return output.readable;
}

// ---------------------------------------------------------------------------
// Rewrite stream (multi-pack or partial selection)
// ---------------------------------------------------------------------------

export function createRewriteStream(
  table: SelectionTable,
  snapshot: OrderedPackSnapshot,
  readStates: Map<number, PackReadState>,
  log: Logger,
  options?: RewriteOptions,
  onComplete?: () => void
): ReadableStream<Uint8Array> {
  let resumeOnPull: (() => void) | undefined;
  return new ReadableStream<Uint8Array>({
    start(controller) {
      void (async () => {
        let writer: WritableStreamDefaultWriter<Uint8Array> | undefined;
        try {
          const digestStream = createDigestStream("SHA-1");
          const digestWriter = digestStream.getWriter();
          writer = digestWriter;

          const waitForCapacity = async (): Promise<void> => {
            while ((controller.desiredSize ?? 1) <= 0) {
              if (options?.signal?.aborted) {
                throw options.signal.reason ?? new Error("rewrite stream aborted");
              }
              await new Promise<void>((resolve) => {
                let settled = false;
                const resume = (): void => {
                  if (settled) return;
                  settled = true;
                  options?.signal?.removeEventListener("abort", resume);
                  if (resumeOnPull === resume) resumeOnPull = undefined;
                  resolve();
                };
                resumeOnPull = resume;
                options?.signal?.addEventListener("abort", resume, { once: true });
                if (options?.signal?.aborted) resume();
              });
            }
          };

          const emit = async (chunk: Uint8Array) => {
            await digestWriter.write(chunk);
            await waitForCapacity();
            controller.enqueue(chunk);
          };

          await emit(buildPackHeader(table.count));
          options?.onProgress?.(
            `Enumerating objects: ${table.count}, from ${readStates.size} packs\n`
          );

          const progressInterval = Math.max(1, Math.floor(table.count / 10));
          let streamed = 0;

          for (let i = 0; i < table.count; i++) {
            if (options?.signal?.aborted) {
              log.debug("rewrite:stream-aborted");
              await digestWriter.abort();
              controller.close();
              return;
            }

            const sel = table.outputOrder[i];
            const packSlot = table.packSlots[sel];
            const readState = readStates.get(packSlot);
            const syntheticPayload = table.syntheticPayloads[sel];
            const headerBytes = buildEntryHeaderBytes(table, sel);
            if (!readState && !syntheticPayload) {
              const pack = snapshot.packs[packSlot];
              log.error("rewrite:missing-read-state", {
                sel,
                packSlot,
                entryIndex: table.entryIndices[sel],
                packKey: pack?.packKey,
                typeCode: table.typeCodes[sel],
                baseSel: table.baseSlots[sel],
              });
              throw new Error(
                `rewrite: missing read state for ${pack?.packKey}#${table.entryIndices[sel]}`
              );
            }

            if (!headerBytes) {
              const pack = snapshot.packs[packSlot];
              const svLen = table.sizeVarLens[sel];
              const typeCode = table.typeCodes[sel];
              const baseSel = table.baseSlots[sel];
              const basePackSlot = baseSel >= 0 ? table.packSlots[baseSel] : undefined;
              const baseEntryIndex = baseSel >= 0 ? table.entryIndices[baseSel] : undefined;
              log.error("rewrite:invalid-header-state", {
                sel,
                packSlot,
                entryIndex: table.entryIndices[sel],
                packKey: pack?.packKey,
                typeCode,
                sizeVarLen: svLen,
                hasBaseOidRaw: !!table.baseOidRaw,
                baseSel,
                basePackSlot,
                baseEntryIndex,
              });
              throw new Error(
                `rewrite: invalid header state for ${pack?.packKey}#${table.entryIndices[sel]}`
              );
            }

            await emit(headerBytes);
            await emitPackPayload(controller, writer, table, sel, readState, waitForCapacity);

            streamed++;
            if (streamed % progressInterval === 0 || streamed === table.count) {
              const percent = Math.round((streamed / table.count) * 100);
              if (streamed === table.count) {
                options?.onProgress?.(
                  `Counting objects: 100% (${table.count}/${table.count}), done.\n`
                );
              } else {
                options?.onProgress?.(
                  `Counting objects: ${percent}% (${streamed}/${table.count})\r`
                );
              }
            }
          }

          await digestWriter.close();
          await waitForCapacity();
          controller.enqueue(new Uint8Array(await digestStream.digest));
          onComplete?.();
          controller.close();
        } catch (error) {
          if (isResolveAbortedError(error) || options?.signal?.aborted) {
            log.debug("rewrite:stream-aborted");
            try {
              await writer?.abort();
            } catch {}
            controller.close();
            return;
          }
          log.error("rewrite:stream-error", { error: String(error) });
          controller.error(error);
        }
      })();
    },
    pull() {
      resumeOnPull?.();
      resumeOnPull = undefined;
    },
    cancel() {
      resumeOnPull?.();
      resumeOnPull = undefined;
    },
  });
}
