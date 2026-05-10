import {
  createLogger,
  getRepoStub,
  isValidOid,
  json,
  unauthorizedAdminBasic,
} from "@/worker/common";
import { repoKey } from "@/worker/keys";
import { verifyAuth } from "@/worker/auth";
import { resolveRepositoryRoute, type RepositoryRoute } from "@/worker/repositories/route";
import { loadSessionMembership } from "@/worker/auth/sessionMembership";
import { getLimiter } from "@/worker/git/operations/limits";
import { listReposForOwner, addRepoToOwner, removeRepoFromOwner } from "@/worker/registry";
import { isJsonObject, safeParseJsonRequest, type JsonValue } from "@/shared/web";
import { requestCacheContext } from "./ui/helpers";
import type { AppContext, AppRouter } from "./hono";

type RefPayload = {
  name: string;
  oid: string;
};

type HeadPayload = {
  target: string;
  oid?: string;
  unborn?: boolean;
};

function isRefPayload(value: JsonValue): value is RefPayload {
  return isJsonObject(value) && typeof value.name === "string" && typeof value.oid === "string";
}

function isHeadPayload(value: JsonValue | null): value is HeadPayload {
  return (
    isJsonObject(value) &&
    typeof value.target === "string" &&
    (value.oid === undefined || typeof value.oid === "string") &&
    (value.unborn === undefined || typeof value.unborn === "boolean")
  );
}

type RepoAdminGate =
  | { kind: "ok"; route: RepositoryRoute; limiter: ReturnType<typeof getLimiter> }
  | { kind: "response"; response: Response };

async function requireRepoAdmin(c: AppContext): Promise<RepoAdminGate> {
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  if (!owner || !repo) {
    return { kind: "response", response: json({ error: "Not found" }, 404) };
  }
  const route = await resolveRepositoryRoute(c.env, owner, repo);
  if (!route) {
    return { kind: "response", response: json({ error: "Not found" }, 404) };
  }
  const membership = await loadSessionMembership(c, route.namespaceId);
  if (membership.kind === "anonymous") {
    return { kind: "response", response: json({ error: "Unauthorized" }, 401) };
  }
  if (membership.kind === "signed-in-non-member") {
    if (route.visibility === "private") {
      return { kind: "response", response: json({ error: "Not found" }, 404) };
    }
    return { kind: "response", response: json({ error: "Forbidden" }, 403) };
  }
  return { kind: "ok", route, limiter: getLimiter(requestCacheContext(c)) };
}

export function registerAdminRoutes(router: AppRouter) {
  async function handleCompactionPost(c: AppContext<"/:owner/:repo/admin/compact">) {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const env = c.env;
    const body = await safeParseJsonRequest(c.req.raw);
    const dryRun = !isJsonObject(body) || body.dryRun !== false;
    const stub = getRepoStub(env, route.doName);
    const log = createLogger(env.LOG_LEVEL, {
      service: "AdminRoutes",
      repoId: route.doName,
    });
    try {
      const res = dryRun
        ? await limiter.run("do:admin-preview-compaction", () => stub.previewCompaction())
        : await limiter.run("do:admin-request-compaction", () => stub.requestCompaction());
      if (!dryRun && res.status === "queued" && res.shouldEnqueue) {
        const queueTask = env.REPO_MAINT_QUEUE.send({
          kind: "compaction",
          doId: stub.id.toString(),
          repoId: route.doName,
        })
          .then(() => {
            log.info("admin:compaction-enqueue-requested", {
              doId: stub.id.toString(),
            });
          })
          .catch((error) => {
            log.warn("admin:compaction-enqueue-failed", {
              doId: stub.id.toString(),
              error: String(error),
            });
          });
        c.executionCtx.waitUntil(queueTask);
      }

      const status = dryRun || res.status !== "queued" ? 200 : 202;
      return json(res, status, { "Cache-Control": "no-cache" });
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  }

  async function handleCompactionDelete(c: AppContext<"/:owner/:repo/admin/compact">) {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const stub = getRepoStub(c.env, route.doName);
    try {
      const res = await limiter.run("do:admin-clear-compaction", () =>
        stub.clearCompactionRequest()
      );
      return json({ ok: true, ...res }, 200, { "Cache-Control": "no-cache" });
    } catch (e) {
      return json({ ok: false, error: String(e) }, 500);
    }
  }

  router.get(`/:owner/admin/registry`, async (c) => {
    const request = c.req.raw;
    const env = c.env;
    const owner = c.req.param("owner");
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const repos = await listReposForOwner(env, owner);
    return json({ owner, repos });
  });

  router.delete(`/:owner/:repo/admin/compact`, handleCompactionDelete);
  router.post(`/:owner/:repo/admin/compact`, handleCompactionPost);

  router.post(`/:owner/admin/registry/sync`, async (c) => {
    const request = c.req.raw;
    const env = c.env;
    const owner = c.req.param("owner");
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const input = await safeParseJsonRequest(request);
    let targets =
      isJsonObject(input) && Array.isArray(input.repos)
        ? input.repos.filter((repo): repo is string => typeof repo === "string" && repo.length > 0)
        : [];
    if (targets.length === 0) {
      targets = await listReposForOwner(env, owner);
    }
    const updated: { added: string[]; removed: string[]; unchanged: string[] } = {
      added: [],
      removed: [],
      unchanged: [],
    };
    const cacheCtx = requestCacheContext(c);
    const limiter = getLimiter(cacheCtx);
    for (const repo of targets) {
      const stub = getRepoStub(env, repoKey(owner, repo));
      let present = false;
      try {
        const refs = await limiter.run("do:legacy-registry-list-refs", () => stub.listRefs());
        present = Array.isArray(refs) && refs.length > 0;
      } catch {}
      if (present) {
        await addRepoToOwner(env, owner, repo);
        updated.added.push(repo);
      } else {
        await removeRepoFromOwner(env, owner, repo);
        updated.removed.push(repo);
      }
    }
    return json({ owner, ...updated });
  });

  // -------------------------------------------------------------------------
  // Repo-scoped admin endpoints. Auth model: tessera session + namespace
  // membership. PATs and AuthDO Basic must NOT authorize these routes.

  router.get(`/:owner/:repo/admin/refs`, async (c) => {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const stub = getRepoStub(c.env, route.doName);
    try {
      const refs = await limiter.run("do:admin-list-refs", () => stub.listRefs());
      return json(refs);
    } catch {
      return json([]);
    }
  });

  router.put(`/:owner/:repo/admin/refs`, async (c) => {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const stub = getRepoStub(c.env, route.doName);
    const body = await safeParseJsonRequest(c.req.raw);
    if (!Array.isArray(body)) {
      return new Response("Invalid refs payload\n", { status: 400 });
    }
    const refs = body.filter(isRefPayload);
    if (refs.length !== body.length) {
      return new Response("Invalid refs payload\n", { status: 400 });
    }
    await limiter.run("do:admin-set-refs", () => stub.setRefs(refs));
    return new Response("OK\n");
  });

  router.get(`/:owner/:repo/admin/head`, async (c) => {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const stub = getRepoStub(c.env, route.doName);
    try {
      const head = await limiter.run("do:admin-get-head", () => stub.getHead());
      return json(head);
    } catch {
      return new Response("Not found\n", { status: 404 });
    }
  });

  router.put(`/:owner/:repo/admin/head`, async (c) => {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const stub = getRepoStub(c.env, route.doName);
    const body = await safeParseJsonRequest(c.req.raw);
    if (!isHeadPayload(body)) {
      return new Response("Invalid head payload\n", { status: 400 });
    }
    await limiter.run("do:admin-set-head", () => stub.setHead(body));
    return new Response("OK\n");
  });

  router.get(`/:owner/:repo/admin/debug-state`, async (c) => {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const stub = getRepoStub(c.env, route.doName);
    try {
      const state = await limiter.run("do:admin-debug-state", () => stub.debugState());
      return json(state);
    } catch {
      return json({});
    }
  });

  router.get(`/:owner/:repo/admin/debug-commit/:commit`, async (c) => {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const commit = c.req.param("commit");
    if (!isValidOid(commit)) {
      return new Response("Invalid commit\n", { status: 400 });
    }
    const stub = getRepoStub(c.env, route.doName);
    try {
      const result = await limiter.run("do:admin-debug-commit", () =>
        stub.debugCheckCommit(commit)
      );
      return json(result);
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  });

  router.get(`/:owner/:repo/admin/debug-oid/:oid`, async (c) => {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const oid = c.req.param("oid");
    if (!isValidOid(oid)) {
      return new Response("Invalid OID\n", { status: 400 });
    }
    const stub = getRepoStub(c.env, route.doName);
    try {
      const result = await limiter.run("do:admin-debug-oid", () => stub.debugCheckOid(oid));
      return json(result);
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  });

  router.delete(`/:owner/:repo/admin/pack/:packKey`, async (c) => {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const packKey = c.req.param("packKey");
    if (!packKey) {
      return json({ error: "Pack key is required" }, 400);
    }
    const stub = getRepoStub(c.env, route.doName);
    try {
      const result = await limiter.run("do:admin-remove-pack", () => stub.removePack(packKey));
      if (result.rejected) {
        const error =
          result.rejected === "active-pack"
            ? "Active packs cannot be deleted until they are superseded"
            : "Only superseded packs can be deleted through this endpoint";
        return json(
          {
            ok: false,
            error,
            ...result,
          },
          409
        );
      }
      return json({ ok: result.removed, ...result });
    } catch (e) {
      return json({ ok: false, error: String(e) }, 500);
    }
  });

  // DANGEROUS: completely purge repo (all R2 objects + DO storage).
  router.delete(`/:owner/:repo/admin/purge`, async (c) => {
    const gate = await requireRepoAdmin(c);
    if (gate.kind === "response") return gate.response;
    const { route, limiter } = gate;
    const owner = c.req.param("owner");
    const repo = c.req.param("repo");
    const body = await safeParseJsonRequest(c.req.raw);
    const confirm = isJsonObject(body) && typeof body.confirm === "string" ? body.confirm : "";
    if (confirm !== `purge-${owner}/${repo}`) {
      return json(
        {
          error: "Confirmation required",
          hint: `Set confirm to "purge-${owner}/${repo}"`,
        },
        400
      );
    }

    const stub = getRepoStub(c.env, route.doName);
    try {
      const result = await limiter.run("do:admin-purge", () => stub.purgeRepo());
      await removeRepoFromOwner(c.env, route.routeNamespaceSlug, route.routeRepoSlug);
      return json({ ok: true, ...result });
    } catch (e) {
      return json({ ok: false, error: String(e) }, 500);
    }
  });
}
