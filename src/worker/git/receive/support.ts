import type { Logger } from "@/worker/common/logger";
import type { ReceiveCommand, ReceiveStatus } from "@/worker/git/operations/validation";

import { concatChunks, flushPkt, pktLine } from "@/worker/git/core/pktline";
import { isResolveAbortedError } from "@/worker/git/pack/indexer";

export function throwIfReceiveAborted(request: Request, log: Logger, stage: string): void {
  if (!request.signal.aborted) return;
  log.debug("receive:aborted", { stage });
  const error = new Error(`receive: aborted during ${stage}`);
  error.name = "AbortError";
  throw error;
}

export function isReceiveAbort(request: Request, error: unknown): boolean {
  if (request.signal.aborted) return true;
  if (isResolveAbortedError(error)) return true;
  return error instanceof Error && error.name === "AbortError";
}

export function buildReceiveReportStatus(args: {
  unpackOk: boolean;
  unpackMessage?: string;
  commands: ReceiveCommand[];
  statuses: ReceiveStatus[];
}): Uint8Array {
  const chunks: Uint8Array[] = [];
  chunks.push(
    pktLine(args.unpackOk ? "unpack ok\n" : `unpack error ${args.unpackMessage || "failed"}\n`)
  );
  for (let index = 0; index < args.commands.length; index++) {
    const command = args.commands[index];
    const status = args.statuses[index];
    if (status?.ok) {
      chunks.push(pktLine(`ok ${command.ref}\n`));
      continue;
    }
    chunks.push(pktLine(`ng ${command.ref} ${status?.msg || "rejected"}\n`));
  }
  chunks.push(flushPkt());
  return concatChunks(chunks);
}

export function buildReceiveUnpackFailureReport(
  commands: ReceiveCommand[],
  unpackMessage: string,
  statusMessage: string = "unpack-failed"
): Uint8Array {
  const statuses: ReceiveStatus[] = commands.map((command) => ({
    ref: command.ref,
    ok: false,
    msg: statusMessage,
  }));
  return buildReceiveReportStatus({
    unpackOk: false,
    unpackMessage,
    commands,
    statuses,
  });
}
