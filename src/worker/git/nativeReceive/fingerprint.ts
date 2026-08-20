import type { AcceptedWriteFact } from "@/worker/git/acceptedWrite";
import type { ReceiveCommand } from "@/worker/git/operations/validation";

import { asBufferSource, bytesToHex } from "@/worker/common";

export async function fingerprintNativeReceive(args: {
  repositoryId: string;
  commands: ReceiveCommand[];
  acceptedWrites: AcceptedWriteFact[];
  inputPackKey: string;
  inputBytes: number;
  inputEtag: string;
}): Promise<string> {
  const canonical = JSON.stringify({
    repositoryId: args.repositoryId,
    commands: args.commands,
    acceptedWrites: args.acceptedWrites,
    inputPackKey: args.inputPackKey,
    inputBytes: args.inputBytes,
    inputEtag: args.inputEtag,
  });
  const bytes = new TextEncoder().encode(canonical);
  const digest = await crypto.subtle.digest("SHA-256", asBufferSource(bytes));
  return bytesToHex(new Uint8Array(digest));
}
