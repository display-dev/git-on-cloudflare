import { AutoRouter } from "itty-router";
import { renderUiView } from "@/client/server/render";
import {
  getAuthStub,
  getBearerToken,
  unauthorizedBearer,
  tooManyAttempts,
  json,
} from "@/worker/common";
import { isJsonObject, safeParseJsonRequest } from "@/shared/web";

export function registerAuthRoutes(router: ReturnType<typeof AutoRouter>) {
  // Auth UI page
  router.get(`/auth`, async (_request, env: Env) => {
    try {
      const html = await renderUiView(env, "auth", {});
      if (!html) {
        return new Response("Failed to render page\n", { status: 500 });
      }
      return new Response(html, {
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store, no-cache, must-revalidate",
          "X-Page-Renderer": "react-ssr",
        },
      });
    } catch {
      return new Response("Failed to render page\n", { status: 500 });
    }
  });

  // List users
  router.get(`/auth/api/users`, async (request, env: Env) => {
    const stub = getAuthStub(env);
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
  router.post(`/auth/api/users`, async (request, env: Env) => {
    const stub = getAuthStub(env);
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
  router.delete(`/auth/api/users`, async (request, env: Env) => {
    const stub = getAuthStub(env);
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
