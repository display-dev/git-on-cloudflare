import type { Logger } from "@/worker/common/logger";
import type { Limiter } from "@/worker/git/operations/limits";

/**
 * This is the request-scoped ownership seam for stock receive qualification.
 * RepoDO owns only transactionally tagged admission, exact-old publication,
 * and conclusive receipt state. The stateless Worker owns native processing
 * and every R2 call through the data-plane adapter.
 */
export const STOCK_RECEIVE_SINGLE_HOP_SHAPE = {
  exactOldOwner: "repo-do",
  workerTargets: ["repo-do", "native-data-plane", "r2"],
  repoDoTargets: [] as string[],
  taggedAdmission: true,
  taggedFinalize: true,
} as const;

export type StockReceiveAdmission<Work, Publication, Committed> =
  | { status: "admitted"; executionToken: string; work: Work }
  | { status: "publication_pending"; publicationToken: string; publication: Publication }
  | { status: "replayed"; committed: Committed }
  | { status: "conflict"; code: string }
  | { status: "rejected"; code: string };

export type StockReceiveFinalize<Publication, Committed> =
  | { status: "publication_pending"; publicationToken: string; publication: Publication }
  | { status: "replayed"; committed: Committed }
  | { status: "ref_conflict"; code: string }
  | { status: "rejected"; code: string };

export type StockReceiveConfirmation<Committed> =
  | { status: "committed"; committed: Committed }
  | { status: "replayed"; committed: Committed }
  | { status: "rejected"; code: string };

export type StockReceiveSingleHopAuthority<Request, Work, Prepared, Publication, Proof, Committed> = {
  admit(request: Request): Promise<StockReceiveAdmission<Work, Publication, Committed>>;
  finalize(
    executionToken: string,
    prepared: Prepared
  ): Promise<StockReceiveFinalize<Publication, Committed>>;
  confirmPublication(
    publicationToken: string,
    proof: Proof
  ): Promise<StockReceiveConfirmation<Committed>>;
};

export type StockReceiveSingleHopDataPlane<Work, Prepared, Publication, Proof> = {
  execute(work: Work): Promise<Prepared>;
  publish(publication: Publication): Promise<Proof>;
  cleanup(): Promise<void>;
};

export type StockReceiveSingleHopResult<Committed> =
  | { status: "committed"; committed: Committed }
  | { status: "conflict"; code: string }
  | { status: "rejected"; code: string }
  | { status: "indeterminate"; phase: "execute" | "finalize" | "publish" | "confirm" };

export type ExecuteStockReceiveSingleHopArgs<
  Request,
  Work,
  Prepared,
  Publication,
  Proof,
  Committed,
> = {
  request: Request;
  authority: StockReceiveSingleHopAuthority<
    Request,
    Work,
    Prepared,
    Publication,
    Proof,
    Committed
  >;
  dataPlane: StockReceiveSingleHopDataPlane<Work, Prepared, Publication, Proof>;
  limiter: Limiter;
  logger: Logger;
};

function rejected<Committed>(code: string): StockReceiveSingleHopResult<Committed> {
  return { status: "rejected", code };
}

/**
 * Executes only direct calls from the Worker. In particular, none of the
 * authority methods receives the data-plane adapter, an R2 binding, or a
 * Container handle, so RepoDO cannot cross either boundary transitively.
 */
export async function executeStockReceiveSingleHop<
  Request,
  Work,
  Prepared,
  Publication,
  Proof,
  Committed,
>(
  args: ExecuteStockReceiveSingleHopArgs<
    Request,
    Work,
    Prepared,
    Publication,
    Proof,
    Committed
  >
): Promise<StockReceiveSingleHopResult<Committed>> {
  const admission = await args.limiter.run("do:stock-receive-admit", () =>
    args.authority.admit(args.request)
  );
  if (admission.status === "conflict") return { status: "conflict", code: admission.code };
  if (admission.status === "rejected") return rejected(admission.code);
  if (admission.status === "replayed") {
    return { status: "committed", committed: admission.committed };
  }

  let publicationToken: string;
  let publication: Publication;
  if (admission.status === "publication_pending") {
    publicationToken = admission.publicationToken;
    publication = admission.publication;
    args.logger.info("stock-receive:publication-resumed", {});
  } else {
    let prepared: Prepared;
    try {
      prepared = await args.dataPlane.execute(admission.work);
    } catch {
      args.logger.warn("stock-receive:data-plane-failed", { phase: "execute" });
      return { status: "indeterminate", phase: "execute" };
    }
    const finalized = await args.limiter.run("do:stock-receive-finalize", () =>
      args.authority.finalize(admission.executionToken, prepared)
    );
    if (finalized.status === "ref_conflict") {
      await args.dataPlane.cleanup();
      return { status: "conflict", code: finalized.code };
    }
    if (finalized.status === "rejected") {
      await args.dataPlane.cleanup();
      return rejected(finalized.code);
    }
    if (finalized.status === "replayed") {
      await args.dataPlane.cleanup();
      return { status: "committed", committed: finalized.committed };
    }
    publicationToken = finalized.publicationToken;
    publication = finalized.publication;
  }

  let proof: Proof;
  try {
    proof = await args.dataPlane.publish(publication);
  } catch {
    // Exact-old authority has already committed. Preserve immutable output and
    // let the same operation resume from publication_pending on retry.
    args.logger.warn("stock-receive:data-plane-failed", { phase: "publish" });
    return { status: "indeterminate", phase: "publish" };
  }
  const confirmation = await args.limiter.run("do:stock-receive-confirm-publication", () =>
    args.authority.confirmPublication(publicationToken, proof)
  );
  if (confirmation.status === "rejected") {
    args.logger.error("stock-receive:publication-confirm-rejected", {
      code: confirmation.code,
    });
    return { status: "indeterminate", phase: "confirm" };
  }
  await args.dataPlane.cleanup();
  args.logger.info("stock-receive:committed", {});
  return { status: "committed", committed: confirmation.committed };
}
