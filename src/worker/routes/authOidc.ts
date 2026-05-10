import { validateSlugForRoute } from "@/shared/slugs";
import {
  claimNamespace,
  findUserByTesseraSub,
  insertMembershipIfMissing,
  insertUserIfNew,
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
import type { AppRouter } from "./hono";
import { renderUiDocumentResponse } from "./uiResponse";
import { errorRedirect, safeRedirect } from "./authShared";

export function registerAuthOidcRoutes(router: AppRouter) {
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
    const log = c.var.logFor({ service: "AuthOidc" });
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
    const log = c.var.logFor({ service: "AuthOidc" });
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
    //
    // These writes stay sequential because each branch depends on prior
    // results. Batching would either roll back the valid user insert on a
    // namespace FK conflict or require the same branching after the batch.
    const db = c.var.db;
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

    log.info("oidc:callback-success", { userId, claimedNamespaceSlug });
    return safeRedirect(c, "/auth/account");
  });

  router.post(`/auth/sign-out`, async (c) => {
    const violation = sameOriginViolation(c);
    if (violation) return violation;
    await endSession(c);
    return safeRedirect(c, "/", 303);
  });
}
