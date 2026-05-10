import { asBodyInit } from "@/worker/common";
import { concatChunks, flushPkt } from "@/worker/git/core/pktline";
import {
  createSidebandPacketChunks,
  enqueueSidebandPayload,
} from "@/worker/git/operations/fetch/sideband";

export type ReceiveResponseMode = "plain" | "side-band-64k";

export function buildSidebandReceiveBody(reportStatusBody: Uint8Array): Uint8Array {
  return concatChunks([...createSidebandPacketChunks(1, reportStatusBody), flushPkt()]);
}

export function buildReceiveResultResponse(args: {
  mode: ReceiveResponseMode;
  reportStatusBody: Uint8Array;
  changed: boolean;
  empty: boolean;
}): Response {
  const body =
    args.mode === "side-band-64k"
      ? buildSidebandReceiveBody(args.reportStatusBody)
      : args.reportStatusBody;

  return new Response(asBodyInit(body), {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-receive-pack-result",
      // Receive-pack is credentialed and mutating; never sit in shared
      // caches regardless of repo visibility.
      "Cache-Control": "no-store",
      "X-Repo-Changed": args.changed ? "1" : "0",
      "X-Repo-Empty": args.empty ? "1" : "0",
    },
  });
}

export class ReceiveSidebandWriter {
  private readonly controller: ReadableStreamDefaultController<Uint8Array>;

  constructor(controller: ReadableStreamDefaultController<Uint8Array>) {
    this.controller = controller;
  }

  progress(message: string): void {
    enqueueSidebandPayload(this.controller, 2, new TextEncoder().encode(message));
  }

  fatal(message: string): void {
    enqueueSidebandPayload(this.controller, 3, new TextEncoder().encode(`fatal: ${message}\n`));
  }

  reportStatus(reportStatusBody: Uint8Array): void {
    enqueueSidebandPayload(this.controller, 1, reportStatusBody);
    this.controller.enqueue(flushPkt());
  }
}
