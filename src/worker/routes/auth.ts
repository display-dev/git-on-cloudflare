import {
  createLogger,
  getAuthStub,
  getBearerToken,
  json,
  newPrefixedId,
  tooManyAttempts,
  unauthorizedBearer,
} from "@/worker/common";
import { isJsonObject, safeParseJsonRequest } from "@/shared/web";
import { validateSlugForRoute } from "@/shared/slugs";
import { createDb, type Db } from "@/worker/db/d1/client";
import {
  claimNamespace,
  findNamespaceById,
  findNamespaceBySlug,
  findRepositoryById,
  findRepositoryByNamespaceAndSlug,
  findUserByTesseraSub,
  insertMembershipIfMissing,
  insertPatWithGrants,
  insertRepositoryIfNew,
  insertUserIfNew,
  listNamespacesForUser,
  listPatGrantsByIds,
  listPatsForUser,
  listRepositoriesForUser,
  revokePatById,
  updateRepositoryVisibility,
  type PatGrantLevel,
  type PersonalAccessTokenRow,
} from "@/worker/db/d1/dal";
import {
  clearOidcTransactionCookie,
  getOidcTransactionCookie,
  setOidcTransactionCookie,
} from "@/worker/auth/cookies";
import {
  buildAuthorizeUrl,
  buildCallbackUrl,
  discoverOidcProvider,
  exchangeAuthorizationCode,
  generateNonce,
  generatePkcePair,
  generateState,
  loadOidcConfig,
  sealTransaction,
  unsealTransaction,
  verifyIdTokenClaims,
} from "@/worker/auth/oidc";
import { sameOriginViolation } from "@/worker/auth/origin";
import {
  createSessionForUser,
  endSession,
  generateNamespaceId,
  generateUserId,
  loadSessionConfig,
  loadViewer,
} from "@/worker/auth/session";
import {
  generatePatPlaintext,
  hashPatPlaintext,
  validatePatName,
  viewerIsNamespaceMember,
} from "@/worker/auth/pat";
import type { AppContext, AppRouter } from "./hono";
import { renderUiDocumentResponse } from "./uiResponse";
import type { LegacyBackfillMessage, RouteCacheSyncMessage } from "@/worker/tasks/queue";
import type { TokensIslandSummary } from "@/client/islands/tokens";

// Best-effort enqueue of a `route-cache-sync` task after a D1 mutation that
// changes ROUTES KV state (repo create, visibility flip, future rename).
// D1 is canonical and the resolver D1 fallback covers the gap until the
// queue drains, so a send failure is logged but does not fail the request.
function enqueueRouteCacheSync(
  c: AppContext,
  log: ReturnType<typeof createLogger>,
  payload: { repositoryId: string; namespaceSlug: string; repoSlug: string }
): void {
  const message: RouteCacheSyncMessage = {
    kind: "route-cache-sync",
    repositoryId: payload.repositoryId,
    namespaceSlug: payload.namespaceSlug,
    repoSlug: payload.repoSlug,
    enqueuedAt: Date.now(),
  };
  c.executionCtx.waitUntil(
    c.env.REPO_TASKS_QUEUE.send(message).catch((error) => {
      log.warn("route-cache-sync:enqueue-failed", {
        repositoryId: payload.repositoryId,
        namespaceSlug: payload.namespaceSlug,
        repoSlug: payload.repoSlug,
        error: String(error),
      });
    })
  );
}

// Build the wire-shape summary that the management UI consumes. Grants are
// fetched in two batched queries (one per grant table) and grouped by PAT
// id, so an arbitrary number of tokens still costs the same fixed number
// of round trips.
async function summarizeTokens(
  db: Db,
  tokens: PersonalAccessTokenRow[]
): Promise<TokensIslandSummary[]> {
  if (tokens.length === 0) return [];
  const ids = tokens.map((row) => row.id);
  const grants = await listPatGrantsByIds(db, ids);
  const namespaceByPatId = new Map<string, TokensIslandSummary["namespaceGrants"]>();
  for (const grant of grants.namespaceGrants) {
    const list = namespaceByPatId.get(grant.patId) ?? [];
    list.push({
      namespaceSlug: grant.namespaceSlug,
      level: grant.level,
    });
    namespaceByPatId.set(grant.patId, list);
  }
  const repoByPatId = new Map<string, TokensIslandSummary["repoGrants"]>();
  for (const grant of grants.repoGrants) {
    const list = repoByPatId.get(grant.patId) ?? [];
    list.push({
      namespaceSlug: grant.namespaceSlug,
      repoSlug: grant.repoSlug,
      level: grant.level,
    });
    repoByPatId.set(grant.patId, list);
  }
  return tokens.map((token) => ({
    id: token.id,
    name: token.name,
    prefix: token.prefix,
    createdAt: token.createdAt,
    expiresAt: token.expiresAt ?? undefined,
    revokedAt: token.revokedAt ?? undefined,
    lastUsedAt: token.lastUsedAt ?? undefined,
    namespaceGrants: namespaceByPatId.get(token.id) ?? [],
    repoGrants: repoByPatId.get(token.id) ?? [],
  }));
}

function safeRedirect(c: AppContext, url: string, status: 302 | 303 = 302): Response {
  // Hono's c.redirect uses 302 by default. Sign-out uses 303 to force a GET on
  // the target after a same-origin POST.
  return c.redirect(url, status);
}

function errorRedirect(c: AppContext, code: string): Response {
  return safeRedirect(c, `/auth?error=${encodeURIComponent(code)}`);
}

export function registerAuthRoutes(router: AppRouter) {
  // -------------------------------------------------------------------------
  // Tessera OIDC RP surface

  // GET /auth: anonymous sees the sign-in page; signed-in goes to account.
  router.get(`/auth`, async (c) => {
    try {
      const viewer = await loadViewer(c);
      if (viewer) return safeRedirect(c, "/auth/account");
      const errorCode = c.req.query("error") ?? undefined;
      return await renderUiDocumentResponse(
        c.env,
        "auth-signin",
        { errorCode },
        { failureBody: "Failed to render page\n", viewer: null }
      );
    } catch {
      return new Response("Failed to render page\n", { status: 500 });
    }
  });

  router.get(`/auth/start`, async (c) => {
    const log = createLogger(c.env.LOG_LEVEL, { service: "AuthOidc" });
    const result = loadOidcConfig(c.env);
    if (!result.ok) {
      log.warn("oidc:start-config-missing", { reason: result.reason });
      return errorRedirect(c, "oidc_unavailable");
    }
    const config = result.config;
    const providerResult = await discoverOidcProvider(config);
    if (!providerResult.ok) {
      log.warn("oidc:start-discovery-failed", { reason: providerResult.reason });
      return errorRedirect(c, "oidc_unavailable");
    }
    const state = generateState();
    const nonce = generateNonce();
    const pkce = await generatePkcePair();
    const redirectUri = buildCallbackUrl(c.req.url);
    let sealed: string;
    try {
      sealed = await sealTransaction(config.clientSecret, {
        state,
        nonce,
        codeVerifier: pkce.verifier,
        redirectUri,
        createdAt: Date.now(),
      });
    } catch (error) {
      log.error("oidc:start-seal-failed", { error: String(error) });
      return errorRedirect(c, "oidc_unavailable");
    }
    setOidcTransactionCookie(c, sealed);
    const authorizeUrl = buildAuthorizeUrl(providerResult.provider, {
      redirectUri,
      state,
      nonce,
      codeChallenge: pkce.challenge,
    });
    return safeRedirect(c, authorizeUrl);
  });

  router.get(`/auth/callback`, async (c) => {
    const log = createLogger(c.env.LOG_LEVEL, { service: "AuthOidc" });
    const configResult = loadOidcConfig(c.env);
    if (!configResult.ok) {
      // Always clear the sealed transaction cookie when bailing out of the
      // callback. A missing/changed config must not leave a stale sealed
      // payload in the browser that could be replayed if config recovers.
      clearOidcTransactionCookie(c);
      log.warn("oidc:callback-config-missing", { reason: configResult.reason });
      return errorRedirect(c, "oidc_unavailable");
    }
    const config = configResult.config;
    const code = c.req.query("code") ?? "";
    const state = c.req.query("state") ?? "";
    if (!code || !state) {
      clearOidcTransactionCookie(c);
      log.warn("oidc:callback-missing-params");
      return errorRedirect(c, "invalid_request");
    }

    const sealed = getOidcTransactionCookie(c);
    if (!sealed) {
      log.warn("oidc:callback-missing-cookie");
      return errorRedirect(c, "missing_state");
    }
    const unsealed = await unsealTransaction(config.clientSecret, sealed);
    if (!unsealed.ok) {
      log.warn("oidc:callback-unseal-failed", { reason: unsealed.reason });
      clearOidcTransactionCookie(c);
      return errorRedirect(c, "invalid_state");
    }
    if (unsealed.payload.state !== state) {
      log.warn("oidc:callback-state-mismatch");
      clearOidcTransactionCookie(c);
      return errorRedirect(c, "invalid_state");
    }
    if (unsealed.payload.redirectUri !== buildCallbackUrl(c.req.url)) {
      log.warn("oidc:callback-redirect-mismatch");
      clearOidcTransactionCookie(c);
      return errorRedirect(c, "invalid_state");
    }
    const providerResult = await discoverOidcProvider(config);
    if (!providerResult.ok) {
      log.warn("oidc:callback-discovery-failed", { reason: providerResult.reason });
      clearOidcTransactionCookie(c);
      return errorRedirect(c, "oidc_unavailable");
    }
    const tokenResult = await exchangeAuthorizationCode(providerResult.provider, {
      callbackUrl: c.req.url,
      codeVerifier: unsealed.payload.codeVerifier,
      nonce: unsealed.payload.nonce,
      state,
    });
    if (!tokenResult.ok) {
      log.warn("oidc:callback-token-exchange-failed", { reason: tokenResult.reason });
      clearOidcTransactionCookie(c);
      return errorRedirect(
        c,
        tokenResult.reason === "invalid_id_token" ? "invalid_id_token" : "token_exchange_failed"
      );
    }
    const verified = verifyIdTokenClaims(tokenResult.tokens.claims);
    if (!verified.ok) {
      log.warn("oidc:callback-verify-failed", { reason: verified.reason });
      clearOidcTransactionCookie(c);
      return errorRedirect(c, "invalid_id_token");
    }
    const sessionConfig = loadSessionConfig(c.env);
    if (!sessionConfig.ok) {
      log.warn("oidc:callback-session-config-missing", { reason: sessionConfig.reason });
      clearOidcTransactionCookie(c);
      return errorRedirect(c, "session_create_failed");
    }

    // First-login race-safe write model:
    //   1. Insert user; ON CONFLICT means existing row.
    //   2. If new + valid pref-name, attempt namespace claim; can lose race.
    //   3. If claim won, create membership.
    //   4. Always create a sealed local session cookie.
    //   5. If claim won, enqueue legacy backfill (best effort via waitUntil).
    //
    // We deliberately run these as three sequential D1 writes instead of one
    // `db.batch()`. The branches are mutually dependent on prior results:
    //   - the user insert may no-op (existing tessera_sub) and we then look up
    //     the canonical row id;
    //   - the namespace claim only runs when the user is genuinely new and
    //     the pref-name passes the slug policy;
    //   - the membership only runs when the namespace claim won the slug
    //     race.
    // Folding the three writes into a single batch would either roll back
    // the user insert on a namespace FK violation (wrong) or require the
    // same conditional branching after-the-fact (no win).
    const db = createDb(c.env.DB);
    const now = Date.now();
    const newUserId = generateUserId();
    const inserted = await insertUserIfNew(db, {
      id: newUserId,
      tesseraSub: verified.verified.sub,
      createdAt: now,
    });
    let userId: string;
    let wasNewUser = false;
    if (inserted) {
      userId = inserted.id;
      wasNewUser = true;
    } else {
      const existing = await findUserByTesseraSub(db, verified.verified.sub);
      if (!existing) {
        log.error("oidc:callback-user-upsert-failed");
        clearOidcTransactionCookie(c);
        return errorRedirect(c, "session_create_failed");
      }
      userId = existing.id;
    }

    let claimedNamespaceSlug: string | null = null;
    if (wasNewUser && verified.verified.preferredUsername) {
      const candidate = validateSlugForRoute(verified.verified.preferredUsername.toLowerCase());
      if (candidate.ok) {
        const claimed = await claimNamespace(db, {
          id: generateNamespaceId(),
          slug: candidate.slug,
          createdBy: userId,
          createdAt: now,
        });
        if (claimed) {
          await insertMembershipIfMissing(db, {
            namespaceId: claimed.id,
            userId,
            createdAt: now,
          });
          claimedNamespaceSlug = claimed.slug;
        } else {
          log.info("oidc:callback-namespace-taken", { slug: candidate.slug });
        }
      }
    }

    try {
      await createSessionForUser(c.env, c, userId, now);
    } catch (error) {
      log.error("oidc:callback-session-create-failed", { error: String(error) });
      clearOidcTransactionCookie(c);
      return errorRedirect(c, "session_create_failed");
    }
    clearOidcTransactionCookie(c);

    if (claimedNamespaceSlug) {
      const message: LegacyBackfillMessage = {
        kind: "legacy-backfill",
        userId,
        namespaceSlug: claimedNamespaceSlug,
      };
      // Best-effort: queue failure must not fail login. Replay via the
      // dashboard if the operator notices missing imports.
      c.executionCtx.waitUntil(
        c.env.REPO_TASKS_QUEUE.send(message).catch((error) =>
          log.warn("oidc:callback-backfill-enqueue-failed", {
            userId,
            namespaceSlug: claimedNamespaceSlug,
            error: String(error),
          })
        )
      );
    }

    log.info("oidc:callback-success", { userId, claimedNamespaceSlug });
    return safeRedirect(c, "/auth/account");
  });

  router.post(`/auth/sign-out`, async (c) => {
    const violation = sameOriginViolation(c);
    if (violation) return violation;
    await endSession(c);
    return safeRedirect(c, "/", 303);
  });

  router.get(`/auth/account`, async (c) => {
    const viewer = await loadViewer(c);
    if (!viewer) return safeRedirect(c, "/auth");
    const db = createDb(c.env.DB);
    const namespaces = await listNamespacesForUser(db, viewer.userId);
    const repositoryRows = await listRepositoriesForUser(db, viewer.userId);
    const tokenRows = await listPatsForUser(db, viewer.userId);
    const tokens = await summarizeTokens(db, tokenRows);
    return renderUiDocumentResponse(
      c.env,
      "account",
      {
        userId: viewer.userId,
        primaryNamespaceSlug: viewer.primaryNamespaceSlug,
        namespaces: namespaces.map((ns) => ({ id: ns.id, slug: ns.slug })),
        repositories: repositoryRows.map((row) => ({
          id: row.repository.id,
          slug: row.repository.slug,
          namespaceSlug: row.namespace.slug,
          visibility: row.repository.visibility as "public" | "private",
          updatedAt: row.repository.updatedAt,
        })),
        tokens,
      },
      { failureBody: "Failed to render page\n", viewer }
    );
  });

  router.get(`/auth/api/tokens`, async (c) => {
    const viewer = await loadViewer(c);
    if (!viewer) return json({ error: "Unauthorized" }, 401);
    const db = createDb(c.env.DB);
    const tokens = await listPatsForUser(db, viewer.userId);
    const summaries = await summarizeTokens(db, tokens);
    return json({ tokens: summaries });
  });

  router.post(`/auth/api/tokens`, async (c) => {
    const log = createLogger(c.env.LOG_LEVEL, { service: "AuthPat" });
    const violation = sameOriginViolation(c);
    if (violation) {
      log.warn("pat:create-same-origin-violation");
      return violation;
    }
    const viewer = await loadViewer(c);
    if (!viewer) {
      log.info("pat:create-not-authenticated");
      return json({ error: "Unauthorized" }, 401);
    }

    // Required tagged-union body shape:
    //   { scope: "namespace", name, namespaceSlug, level }
    //   { scope: "repo", name, namespaceSlug, repoSlug, level }
    // `scope` and `level` are both required so contract drift surfaces as
    // a 400 instead of silently coercing. `level === "push"` includes pull
    // access by construction (see `pat_*_grants.level` CHECK).
    const body = await safeParseJsonRequest(c.req.raw);
    const scope =
      isJsonObject(body) && (body.scope === "namespace" || body.scope === "repo")
        ? body.scope
        : null;
    if (scope === null) {
      log.warn("pat:create-invalid-scope");
      return json({ error: "Body must include scope: 'namespace' or 'repo'" }, 400);
    }
    const name = isJsonObject(body) && typeof body.name === "string" ? body.name.trim() : "";
    const namespaceSlug =
      isJsonObject(body) && typeof body.namespaceSlug === "string"
        ? body.namespaceSlug.trim().toLowerCase()
        : "";
    const repoSlugRaw =
      scope === "repo" && isJsonObject(body) && typeof body.repoSlug === "string"
        ? body.repoSlug.trim().toLowerCase()
        : "";
    const level: PatGrantLevel | null =
      isJsonObject(body) && (body.level === "pull" || body.level === "push") ? body.level : null;

    const nameValidation = validatePatName(name);
    if (!nameValidation.ok) {
      log.warn("pat:create-invalid-name", { reason: nameValidation.reason });
      return json({ error: "Invalid token name" }, 400);
    }
    if (level === null) {
      log.warn("pat:create-invalid-level");
      return json({ error: "Body must include level: 'pull' or 'push'" }, 400);
    }
    const slugValidation = validateSlugForRoute(namespaceSlug);
    if (!slugValidation.ok) {
      log.warn("pat:create-invalid-namespace-slug", { reason: slugValidation.reason });
      return json({ error: "Invalid namespace slug" }, 400);
    }
    let repoSlug: string | null = null;
    if (scope === "repo") {
      const repoSlugValidation = validateSlugForRoute(repoSlugRaw);
      if (!repoSlugValidation.ok) {
        log.warn("pat:create-invalid-repo-slug", { reason: repoSlugValidation.reason });
        return json({ error: "Invalid repo slug" }, 400);
      }
      repoSlug = repoSlugValidation.slug;
    }
    const db = createDb(c.env.DB);
    const namespace = await findNamespaceBySlug(db, slugValidation.slug);
    if (!namespace) {
      log.warn("pat:create-namespace-not-found", { namespaceSlug: slugValidation.slug });
      return json({ error: "Namespace not found" }, 404);
    }
    if (!(await viewerIsNamespaceMember(db, viewer.userId, namespace.id))) {
      log.warn("pat:create-not-member", {
        userId: viewer.userId,
        namespaceId: namespace.id,
      });
      return json({ error: "Forbidden" }, 403);
    }

    let repoId: string | null = null;
    if (scope === "repo" && repoSlug !== null) {
      const repository = await findRepositoryByNamespaceAndSlug(db, namespace.id, repoSlug);
      if (!repository) {
        log.warn("pat:create-repo-not-found", {
          namespaceId: namespace.id,
          repoSlug,
        });
        return json({ error: "Repo not found" }, 404);
      }
      repoId = repository.id;
    }

    const generated = generatePatPlaintext();
    const hash = await hashPatPlaintext(generated.plaintext);
    const now = Date.now();
    const patId = newPrefixedId("pat");
    await insertPatWithGrants(db, {
      pat: {
        id: patId,
        userId: viewer.userId,
        name: nameValidation.name,
        prefix: generated.publicPrefix,
        hash,
        createdAt: now,
        expiresAt: null,
        revokedAt: null,
        lastUsedAt: null,
      },
      namespaceGrants: scope === "namespace" ? [{ patId, namespaceId: namespace.id, level }] : [],
      repoGrants: scope === "repo" && repoId !== null ? [{ patId, repoId, level }] : [],
    });
    log.info("pat:create-ok", {
      userId: viewer.userId,
      patId,
      prefix: generated.publicPrefix,
      scope,
      namespaceSlug: slugValidation.slug,
      repoSlug,
      level,
    });
    return json({ id: patId, plaintext: generated.plaintext, prefix: generated.publicPrefix });
  });

  router.get(`/auth/api/repositories`, async (c) => {
    const viewer = await loadViewer(c);
    if (!viewer) return json({ error: "Unauthorized" }, 401);
    const db = createDb(c.env.DB);
    const rows = await listRepositoriesForUser(db, viewer.userId);
    return json({
      repositories: rows.map((row) => ({
        id: row.repository.id,
        slug: row.repository.slug,
        namespaceSlug: row.namespace.slug,
        visibility: row.repository.visibility as "public" | "private",
        updatedAt: row.repository.updatedAt,
      })),
    });
  });

  router.post(`/auth/api/repositories`, async (c) => {
    const log = createLogger(c.env.LOG_LEVEL, { service: "RepoCreate" });
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
    const body = await safeParseJsonRequest(c.req.raw);
    const namespaceSlugRaw =
      isJsonObject(body) && typeof body.namespaceSlug === "string"
        ? body.namespaceSlug.trim().toLowerCase()
        : "";
    const slugRaw =
      isJsonObject(body) && typeof body.slug === "string" ? body.slug.trim().toLowerCase() : "";
    const visibility =
      isJsonObject(body) && (body.visibility === "public" || body.visibility === "private")
        ? body.visibility
        : null;
    if (visibility === null) {
      log.warn("repo-create:invalid-visibility");
      return json({ ok: false, reason: "invalid-visibility" } as const, 400);
    }
    const namespaceValidation = validateSlugForRoute(namespaceSlugRaw);
    if (!namespaceValidation.ok) {
      log.warn("repo-create:invalid-slug", { field: "namespaceSlug" });
      return json({ ok: false, reason: "invalid-slug" } as const, 400);
    }
    const slugValidation = validateSlugForRoute(slugRaw);
    if (!slugValidation.ok) {
      log.warn("repo-create:invalid-slug", { field: "slug" });
      return json({ ok: false, reason: "invalid-slug" } as const, 400);
    }
    const db = createDb(c.env.DB);
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
    // `doName` for fresh repos uses `repo:<id-suffix>`; legacy backfilled
    // rows keep the historical `<owner>/<repo>` form. The colon namespace
    // prevents future collisions if a namespace ever uses dashes that look
    // like `<owner>/<repo>`.
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
    const log = createLogger(c.env.LOG_LEVEL, { service: "RepoVisibility" });
    const violation = sameOriginViolation(c);
    if (violation) {
      log.warn("repo-visibility:same-origin-violation");
      return violation;
    }
    const viewer = await loadViewer(c);
    if (!viewer) return json({ error: "Unauthorized" }, 401);
    const repositoryId = c.req.param("repositoryId");
    const body = await safeParseJsonRequest(c.req.raw);
    const visibility =
      isJsonObject(body) && (body.visibility === "public" || body.visibility === "private")
        ? body.visibility
        : null;
    if (visibility === null) {
      log.warn("repo-visibility:invalid-payload", { repositoryId });
      return json({ ok: false, reason: "invalid-payload" } as const, 400);
    }
    const db = createDb(c.env.DB);
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

  router.delete(`/auth/api/tokens/:patId`, async (c) => {
    const log = createLogger(c.env.LOG_LEVEL, { service: "AuthPat" });
    const violation = sameOriginViolation(c);
    if (violation) {
      log.warn("pat:revoke-same-origin-violation");
      return violation;
    }
    const viewer = await loadViewer(c);
    if (!viewer) return json({ error: "Unauthorized" }, 401);
    const patId = c.req.param("patId");
    const db = createDb(c.env.DB);
    const result = await revokePatById(db, patId, viewer.userId, Date.now());
    if (result.ok) {
      log.info("pat:revoke-ok", { userId: viewer.userId, patId });
      return json({ ok: true });
    }
    if (result.reason === "not-owner") {
      log.warn("pat:revoke-not-owner", { userId: viewer.userId, patId });
      return json({ error: "Forbidden" }, 403);
    }
    if (result.reason === "not-found") {
      log.warn("pat:revoke-not-found", { userId: viewer.userId, patId });
      return json({ error: "Not found" }, 404);
    }
    // Re-revoke is idempotent; surface as a 200 but record it for visibility.
    log.debug("pat:revoke-already-revoked", { userId: viewer.userId, patId });
    return json({ ok: true });
  });

  // Render the legacy AuthDO admin island under its new path so /auth can
  // host the tessera sign-in page. The JSON management API at
  // /auth/api/users keeps its current path for operator scripts/tests.
  router.get(`/auth/legacy`, async (c) => {
    try {
      return await renderUiDocumentResponse(
        c.env,
        "auth-legacy",
        {},
        {
          failureBody: "Failed to render page\n",
          viewer: await loadViewer(c),
        }
      );
    } catch {
      return new Response("Failed to render page\n", { status: 500 });
    }
  });

  // -------------------------------------------------------------------------
  // Legacy AuthDO admin JSON. Tests still exercise these endpoints; do not
  // change their paths or behavior here; the legacy admin UI lives at
  // /auth/legacy.

  // List users
  router.get(`/auth/api/users`, async (c) => {
    const request = c.req.raw;
    const stub = getAuthStub(c.env);
    if (!stub) return new Response("Not configured\n", { status: 501 });
    const provided = getBearerToken(request);
    const clientIp = request.headers.get("CF-Connecting-IP") || "unknown";
    const auth = await stub.adminAuthorizeOrRateLimit(provided, clientIp);
    if (!auth.ok) {
      if (auth.status === 401) return unauthorizedBearer();
      if (auth.status === 429) return tooManyAttempts(auth.retryAfter);
      return unauthorizedBearer();
    }
    try {
      const users = await stub.getUsers();
      return json({ users });
    } catch {
      return json({ users: [] });
    }
  });

  // Create user
  router.post(`/auth/api/users`, async (c) => {
    const request = c.req.raw;
    const stub = getAuthStub(c.env);
    if (!stub) return new Response("Not configured\n", { status: 501 });
    const provided = getBearerToken(request);
    const clientIp = request.headers.get("CF-Connecting-IP") || "unknown";
    const auth = await stub.adminAuthorizeOrRateLimit(provided, clientIp);
    if (!auth.ok) {
      if (auth.status === 401) return unauthorizedBearer();
      if (auth.status === 429) return tooManyAttempts(auth.retryAfter);
      return unauthorizedBearer();
    }
    const input = await safeParseJsonRequest(request);
    const owner = isJsonObject(input) && typeof input.owner === "string" ? input.owner.trim() : "";
    const token =
      isJsonObject(input) && typeof input.token === "string" && input.token
        ? input.token
        : undefined;
    const tokens =
      isJsonObject(input) && Array.isArray(input.tokens)
        ? input.tokens.filter((value): value is string => typeof value === "string")
        : undefined;
    if (!owner || (!token && !tokens)) {
      return json({ error: "owner and token(s) required" }, 400);
    }
    const toAdd: string[] = [];
    if (token) toAdd.push(token);
    if (tokens) toAdd.push(...tokens);
    const res = await stub.addTokens(owner, toAdd);
    return json(res);
  });

  // Delete user
  router.delete(`/auth/api/users`, async (c) => {
    const request = c.req.raw;
    const stub = getAuthStub(c.env);
    if (!stub) return new Response("Not configured\n", { status: 501 });
    const provided = getBearerToken(request);
    const clientIp = request.headers.get("CF-Connecting-IP") || "unknown";
    const auth = await stub.adminAuthorizeOrRateLimit(provided, clientIp);
    if (!auth.ok) {
      if (auth.status === 401) return unauthorizedBearer();
      if (auth.status === 429) return tooManyAttempts(auth.retryAfter);
      return unauthorizedBearer();
    }
    const input = await safeParseJsonRequest(request);
    const owner = isJsonObject(input) && typeof input.owner === "string" ? input.owner.trim() : "";
    const token =
      isJsonObject(input) && typeof input.token === "string" && input.token
        ? input.token
        : undefined;
    const tokenHash =
      isJsonObject(input) && typeof input.tokenHash === "string" && input.tokenHash
        ? input.tokenHash
        : undefined;
    if (!owner) {
      return json({ error: "owner required" }, 400);
    }
    if (!token && !tokenHash) {
      await stub.deleteOwner(owner);
      return json({ ok: true });
    }
    if (tokenHash) {
      await stub.deleteTokenByHash(owner, tokenHash);
      return json({ ok: true });
    }
    if (token) {
      await stub.deleteToken(owner, token);
      return json({ ok: true });
    }
    return json({ ok: true });
  });
}
