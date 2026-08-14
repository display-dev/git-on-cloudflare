import type { Logger } from "@/worker/common/logger";
import type { ReceiveCommand } from "@/worker/git/operations/validation";

export type AcceptedWriteSource = "git-push" | "ingestion";

export type AcceptedWriteFact = {
  repositoryId: string;
  ref: string;
  beforeSha: string;
  afterSha: string;
  actor: string;
  sourceSurface: AcceptedWriteSource;
  idempotencyKey: string | null;
};

export function acceptedWriteFactsForCommands(args: {
  repositoryId: string;
  commands: ReceiveCommand[];
  actor: string;
  sourceSurface: AcceptedWriteSource;
  idempotencyKey: string | null;
}): AcceptedWriteFact[] {
  const factsByRef = new Map<string, AcceptedWriteFact>();
  for (const command of args.commands) {
    const existing = factsByRef.get(command.ref);
    if (existing) {
      // Receive finalization applies the last command for a repeated ref. Keep
      // the first observed base and replace only the authoritative outcome.
      existing.afterSha = command.newOid;
      continue;
    }
    factsByRef.set(command.ref, {
      repositoryId: args.repositoryId,
      ref: command.ref,
      beforeSha: command.oldOid,
      afterSha: command.newOid,
      actor: args.actor,
      sourceSurface: args.sourceSurface,
      idempotencyKey: args.idempotencyKey,
    });
  }
  return Array.from(factsByRef.values()).filter((fact) => fact.beforeSha !== fact.afterSha);
}

/**
 * Investigation 4's candidate-owned emission boundary. Durable delivery and
 * reconciliation are intentionally left to Investigation 6; this structured
 * fact is emitted only after the repository Durable Object commits ref CAS.
 */
export function emitAcceptedWriteFacts(log: Logger, facts: AcceptedWriteFact[]): void {
  for (const fact of facts) {
    log.info("accepted-write:emitted", { acceptedWrite: fact });
  }
}
