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
  stockReceive?: {
    inputRequestSha256: string;
    packOffset: number;
    advertisedRefs: Array<{ name: string; oid: string }>;
  };
}): Promise<string> {
  const canonical = JSON.stringify(
    args.stockReceive
      ? {
          repositoryId: args.repositoryId,
          commands: args.commands,
          acceptedWrites: args.acceptedWrites,
          stockReceive: {
            inputRequestSha256: args.stockReceive.inputRequestSha256,
            packOffset: args.stockReceive.packOffset,
          },
        }
      : {
          repositoryId: args.repositoryId,
          commands: args.commands,
          acceptedWrites: args.acceptedWrites,
          inputPackKey: args.inputPackKey,
          inputBytes: args.inputBytes,
          inputEtag: args.inputEtag,
        }
  );
  const bytes = new TextEncoder().encode(canonical);
  const digest = await crypto.subtle.digest("SHA-256", asBufferSource(bytes));
  return bytesToHex(new Uint8Array(digest));
}
