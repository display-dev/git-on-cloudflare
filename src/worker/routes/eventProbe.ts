import { getRepoStub, zeroOid } from "@/worker/common";
import type {
  AcceptedWriteProjectionResult,
  ReconciledHeadProjectionResult,
  SnapshotReconcilePlan,
} from "@/worker/do/repo/acceptedWrites";
import { countSubrequest } from "@/worker/git/operations/limits";
import { isValidRefName } from "@/worker/git/operations/validation";
import { materializeAcceptedWrite } from "@/worker/git/snapshot/materialize";
import { snapshotEventProbeEnabled } from "@/worker/git/snapshot/config";
import { resolveRepositoryRoute, type RepositoryRoute } from "@/worker/repositories/route";
import { isValidOwnerRepo } from "@/shared/web";
import { authorizeInternalRequest } from "./internalAuth";
import { workerExecutionContext, type AppContext, type AppRouter } from "./hono";
import { z } from "zod";

const MAX_BODY_BYTES = 1024;

const EventProbeRequestSchema = z.object({
  action: z.enum(["deliver", "reconcile", "drop"]),
  entryId: z.string().min(1).max(512).optional(),
  ref: z.string().min(1).max(512).optional(),
  crash: z.enum(["before_snapshot", "after_snapshot"]).optional(),
});
type EventProbeRequest = z.infer<typeof EventProbeRequestSchema>;

async function resolveEventProbeRoute(c: AppContext): Promise<RepositoryRoute | Response> {
  if (!snapshotEventProbeEnabled(c.env)) return new Response("Not found\n", { status: 404 });
  const denied = await authorizeInternalRequest(c);
  if (denied) return denied;
  const owner = c.req.param("owner") ?? "";
  const repo = c.req.param("repo") ?? "";
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return new Response("Not found\n", { status: 404 });
  }
  const route = await resolveRepositoryRoute(c.env, owner, repo, {
    mode: "allow-d1-fallback",
    db: c.var.db,
    log: c.var.logFor({ service: "SnapshotEventProbe" }),
  });
  return route ?? new Response("Not found\n", { status: 404 });
}

function count(c: AppContext, op: string): void {
  if (!countSubrequest(c.var.cacheCtx)) {
    c.var.logFor({ service: "SnapshotEventProbe" }).warn("event-probe:soft-budget-exhausted", {
      op,
    });
  }
}

async function listState(c: AppContext, route: RepositoryRoute): Promise<Response> {
  const stub = getRepoStub(c.env, route.doName);
  count(c, "do:list-accepted-writes");
  const entries = await c.var.limiter.run("do:list-accepted-writes", () =>
    stub.listAcceptedWrites()
  );
  count(c, "do:get-snapshot-reconcile-plan");
  const plan = await c.var.limiter.run<SnapshotReconcilePlan>(
    "do:get-snapshot-reconcile-plan",
    () => stub.getSnapshotReconcilePlan("refs/heads/main")
  );
  count(c, "do:get-snapshot-projection");
  const projection = await c.var.limiter.run("do:get-snapshot-projection", () =>
    stub.getSnapshotProjection("refs/heads/main")
  );
  c.var.logFor({ service: "SnapshotEventProbe" }).info("event-probe:state", {
    journalEntries: entries.length,
    reconcileStatus: plan.status,
    snapshotCount: projection.snapshotCount,
  });
  return Response.json(
    { entries, plan, projection },
    { headers: { "Cache-Control": "private, no-store" } }
  );
}

async function parseRequest(c: AppContext): Promise<EventProbeRequest | Response> {
  const length = Number(c.req.header("Content-Length"));
  if (!Number.isSafeInteger(length) || length < 1 || length > MAX_BODY_BYTES) {
    return Response.json({ error: "Invalid body length" }, { status: 413 });
  }
  let body: unknown;
  try {
    body = await c.req.json();
  } catch {
    return Response.json({ error: "Invalid JSON" }, { status: 400 });
  }
  const parsed = EventProbeRequestSchema.safeParse(body);
  return parsed.success
    ? parsed.data
    : Response.json({ error: "Invalid event probe request" }, { status: 400 });
}

async function materializeEntry(
  c: AppContext,
  route: RepositoryRoute,
  entryId: string,
  crash?: EventProbeRequest["crash"]
): Promise<Response> {
  const log = c.var.logFor({ service: "SnapshotEventProbe" });
  const stub = getRepoStub(c.env, route.doName);
  count(c, "do:list-accepted-writes");
  const entries = await c.var.limiter.run("do:list-accepted-writes", () =>
    stub.listAcceptedWrites()
  );
  const entry = entries.find((candidate) => candidate.id === entryId);
  if (!entry) return Response.json({ error: "Accepted write not found" }, { status: 404 });
  if (crash === "before_snapshot") {
    log.warn("event-probe:crash-before-snapshot", { entryId });
    return Response.json({ error: "Injected crash before snapshot" }, { status: 503 });
  }
  const startedAt = performance.now();
  if (entry.fact.afterSha !== zeroOid()) {
    await materializeAcceptedWrite({
      env: c.env,
      repoId: route.doName,
      fact: entry.fact,
      request: c.req.raw,
      ctx: workerExecutionContext(c),
      limiter: c.var.limiter,
      log,
    });
  }
  if (crash === "after_snapshot") {
    log.warn("event-probe:crash-after-snapshot", { entryId });
    return Response.json({ error: "Injected crash after snapshot" }, { status: 503 });
  }
  count(c, "do:project-accepted-write");
  const projection = await c.var.limiter.run<AcceptedWriteProjectionResult>(
    "do:project-accepted-write",
    () =>
      stub.projectAcceptedWrite({
        entryId,
        commitSha: entry.fact.afterSha,
        materializedAt: Date.now(),
      })
  );
  if ("status" in projection) {
    return Response.json({ error: "Repository is being deleted" }, { status: 409 });
  }
  const elapsedMs = performance.now() - startedAt;
  log.info("event-probe:delivered", {
    entryId,
    commitSha: entry.fact.afterSha,
    elapsedMs,
    snapshotCreated: projection.snapshotCreated,
    pointerAdvanced: projection.pointerAdvanced,
  });
  return Response.json({ entry, projection, elapsedMs });
}

async function reconcile(c: AppContext, route: RepositoryRoute, ref: string): Promise<Response> {
  const log = c.var.logFor({ service: "SnapshotEventProbe" });
  const stub = getRepoStub(c.env, route.doName);
  count(c, "do:get-snapshot-reconcile-plan");
  const plan = await c.var.limiter.run<SnapshotReconcilePlan>(
    "do:get-snapshot-reconcile-plan",
    () => stub.getSnapshotReconcilePlan(ref)
  );
  if (plan.status === "deliver") return await materializeEntry(c, route, plan.entry.id);
  if (plan.status !== "head_only") return Response.json({ plan });
  if (plan.afterSha !== zeroOid()) {
    await materializeAcceptedWrite({
      env: c.env,
      repoId: route.doName,
      fact: {
        repositoryId: route.repositoryId,
        afterSha: plan.afterSha,
        sourceSurface: "reconcile",
      },
      request: c.req.raw,
      ctx: workerExecutionContext(c),
      limiter: c.var.limiter,
      log,
    });
  }
  count(c, "do:project-reconciled-head");
  const projection = await c.var.limiter.run<ReconciledHeadProjectionResult>(
    "do:project-reconciled-head",
    () =>
      stub.projectReconciledHead({
        ref: plan.ref,
        commitSha: plan.afterSha,
        sequence: plan.sequence,
        materializedAt: Date.now(),
      })
  );
  if (projection.status === "stale" || projection.status === "repository-deleting") {
    log.info("event-probe:reconcile-stale", { ref: plan.ref, sequence: plan.sequence });
    return Response.json({ plan, status: "stale" }, { status: 409 });
  }
  log.warn("event-probe:head-only-reconciled", {
    ref: plan.ref,
    commitSha: plan.afterSha,
    historyComplete: false,
  });
  return Response.json({ plan, projection, historyComplete: false });
}

async function handleGet(c: AppContext): Promise<Response> {
  const resolved = await resolveEventProbeRoute(c);
  return resolved instanceof Response ? resolved : await listState(c, resolved);
}

async function handlePost(c: AppContext): Promise<Response> {
  const resolved = await resolveEventProbeRoute(c);
  if (resolved instanceof Response) return resolved;
  const parsed = await parseRequest(c);
  if (parsed instanceof Response) return parsed;
  if (parsed.action === "deliver") {
    if (!parsed.entryId) return Response.json({ error: "Missing entryId" }, { status: 400 });
    return await materializeEntry(c, resolved, parsed.entryId, parsed.crash);
  }
  if (parsed.action === "drop") {
    if (!parsed.entryId) return Response.json({ error: "Missing entryId" }, { status: 400 });
    const stub = getRepoStub(c.env, resolved.doName);
    count(c, "do:drop-accepted-write");
    const dropped = await c.var.limiter.run("do:drop-accepted-write", () =>
      stub.dropAcceptedWriteForProbe(parsed.entryId!)
    );
    c.var.logFor({ service: "SnapshotEventProbe" }).warn("event-probe:fact-dropped", {
      dropped,
    });
    return Response.json({ dropped });
  }
  const ref = parsed.ref ?? "refs/heads/main";
  if (!isValidRefName(ref)) return Response.json({ error: "Invalid ref" }, { status: 400 });
  return await reconcile(c, resolved, ref);
}

export function registerEventProbeRoutes(router: AppRouter): void {
  router.get("/_internal/event-probe/:owner/:repo", handleGet);
  router.post("/_internal/event-probe/:owner/:repo", handlePost);
}
