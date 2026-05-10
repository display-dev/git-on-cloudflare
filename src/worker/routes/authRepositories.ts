import { safeParseJsonRequest } from "@/shared/web";
import { validateSlugForRoute } from "@/shared/slugs";
import { json, newPrefixedId } from "@/worker/common";
import {
  findNamespaceById,
  findNamespaceBySlug,
  findRepositoryById,
  insertRepositoryIfNew,
  listRepositoriesForUser,
  updateRepositoryVisibility,
} from "@/worker/db/d1/dal";
import { sameOriginViolation } from "@/worker/auth/origin";
import { loadViewer } from "@/worker/auth/session";
import { viewerIsNamespaceMember } from "@/worker/auth/pat";
import type { AppRouter } from "./hono";
import { enqueueRouteCacheSync } from "./authShared";
import { RepositoryCreateRequestSchema, RepositoryVisibilityRequestSchema } from "./requestSchemas";

export function registerAuthRepositoryRoutes(router: AppRouter) {
  router.get(`/auth/api/repositories`, async (c) => {
    const viewer = await loadViewer(c);
    if (!viewer) return json({ error: "Unauthorized" }, 401);
    const rows = await listRepositoriesForUser(c.var.db, viewer.userId);
    return json({
      repositories: rows.map((row) => ({
        id: row.repository.id,
        slug: row.repository.slug,
        namespaceSlug: row.namespace.slug,
        visibility: row.repository.visibility,
        updatedAt: row.repository.updatedAt,
      })),
    });
  });

  router.post(`/auth/api/repositories`, async (c) => {
    const log = c.var.logFor({ service: "RepoCreate" });
    const violation = sameOriginViolation(c);
    if (violation) {
      log.warn("repo-create:same-origin-violation");
      return violation;
    }
    const viewer = await loadViewer(c);
    if (!viewer) {
      log.info("repo-create:not-authenticated");
      return json({ error: "Unauthorized" }, 401);
    }
    const rawBody = await safeParseJsonRequest(c.req.raw);
    const parsedBody = RepositoryCreateRequestSchema.safeParse(rawBody);
    const body = parsedBody.success
      ? parsedBody.data
      : { namespaceSlug: "", slug: "", visibility: null };
    if (body.visibility === null) {
      log.warn("repo-create:invalid-visibility");
      return json({ ok: false, reason: "invalid-visibility" } as const, 400);
    }
    const visibility = body.visibility;
    const namespaceValidation = validateSlugForRoute(body.namespaceSlug);
    if (!namespaceValidation.ok) {
      log.warn("repo-create:invalid-slug", { field: "namespaceSlug" });
      return json({ ok: false, reason: "invalid-slug" } as const, 400);
    }
    const slugValidation = validateSlugForRoute(body.slug);
    if (!slugValidation.ok) {
      log.warn("repo-create:invalid-slug", { field: "slug" });
      return json({ ok: false, reason: "invalid-slug" } as const, 400);
    }
    const db = c.var.db;
    const namespace = await findNamespaceBySlug(db, namespaceValidation.slug);
    if (!namespace) {
      log.warn("repo-create:namespace-not-found", { namespaceSlug: namespaceValidation.slug });
      return json({ ok: false, reason: "namespace-not-found" } as const, 404);
    }
    if (!(await viewerIsNamespaceMember(db, viewer.userId, namespace.id))) {
      log.warn("repo-create:not-member", {
        userId: viewer.userId,
        namespaceId: namespace.id,
      });
      return json({ ok: false, reason: "not-member" } as const, 403);
    }
    const now = Date.now();
    const repositoryId = newPrefixedId("repo");
    // Fresh repositories use an opaque RepoDO storage identity. Existing D1
    // rows may store a slash-shaped `doName`, so the `repo:` prefix keeps new
    // identities unambiguous without inspecting URL slugs.
    const doName = `repo:${repositoryId.slice("repo_".length)}`;
    const inserted = await insertRepositoryIfNew(db, {
      id: repositoryId,
      namespaceId: namespace.id,
      createdBy: viewer.userId,
      slug: slugValidation.slug,
      doName,
      visibility,
      createdAt: now,
      updatedAt: now,
    });
    if (!inserted) {
      // Race lost: another writer committed `(namespaceId, slug)` between
      // our membership check and the insert. The user sees this as
      // slug-taken and can pick another name.
      log.warn("repo-create:slug-taken", {
        namespaceId: namespace.id,
        slug: slugValidation.slug,
      });
      return json({ ok: false, reason: "slug-taken" } as const, 409);
    }
    enqueueRouteCacheSync(c, log, {
      repositoryId: inserted.id,
      namespaceSlug: namespaceValidation.slug,
      repoSlug: slugValidation.slug,
    });
    log.info("repo-create:ok", {
      userId: viewer.userId,
      repositoryId: inserted.id,
      namespaceSlug: namespaceValidation.slug,
      slug: slugValidation.slug,
      visibility,
    });
    return json({
      ok: true,
      id: inserted.id,
      namespaceSlug: namespaceValidation.slug,
      slug: slugValidation.slug,
      visibility,
      updatedAt: now,
    } as const);
  });

  router.patch(`/auth/api/repositories/:repositoryId`, async (c) => {
    const log = c.var.logFor({ service: "RepoVisibility" });
    const violation = sameOriginViolation(c);
    if (violation) {
      log.warn("repo-visibility:same-origin-violation");
      return violation;
    }
    const viewer = await loadViewer(c);
    if (!viewer) return json({ error: "Unauthorized" }, 401);
    const repositoryId = c.req.param("repositoryId");
    const rawBody = await safeParseJsonRequest(c.req.raw);
    const parsedBody = RepositoryVisibilityRequestSchema.safeParse(rawBody);
    const body = parsedBody.success ? parsedBody.data : { visibility: null };
    if (body.visibility === null) {
      log.warn("repo-visibility:invalid-payload", { repositoryId });
      return json({ ok: false, reason: "invalid-payload" } as const, 400);
    }
    const visibility = body.visibility;
    const db = c.var.db;
    const repo = await findRepositoryById(db, repositoryId);
    if (!repo) {
      log.warn("repo-visibility:not-found", { repositoryId });
      return json({ ok: false, reason: "not-found" } as const, 404);
    }
    if (!(await viewerIsNamespaceMember(db, viewer.userId, repo.namespaceId))) {
      log.warn("repo-visibility:not-member", {
        userId: viewer.userId,
        repositoryId,
        namespaceId: repo.namespaceId,
      });
      return json({ ok: false, reason: "not-member" } as const, 403);
    }
    const result = await updateRepositoryVisibility(db, repositoryId, visibility, Date.now());
    if (!result.ok) {
      log.warn("repo-visibility:not-found", { repositoryId });
      return json({ ok: false, reason: "not-found" } as const, 404);
    }
    if (result.previous !== result.current) {
      // Reconcile ROUTES KV via the queue. The consumer reads D1 at
      // execution time and converges KV to canonical state, so a
      // public->private flip drops the route candidate and a
      // private->public flip puts it. We capture the slugs from the
      // current row so the consumer can drop any stale captured key as
      // well as set the canonical key.
      const namespace = await findNamespaceById(db, repo.namespaceId);
      if (namespace) {
        enqueueRouteCacheSync(c, log, {
          repositoryId,
          namespaceSlug: namespace.slug,
          repoSlug: repo.slug,
        });
      } else {
        log.warn("repo-visibility:namespace-missing-for-sync", {
          repositoryId,
          namespaceId: repo.namespaceId,
        });
      }
    }
    log.info("repo-visibility:ok", {
      userId: viewer.userId,
      repositoryId,
      previous: result.previous,
      current: result.current,
    });
    return json({
      ok: true,
      id: repositoryId,
      visibility: result.current,
      previous: result.previous,
    } as const);
  });
}
