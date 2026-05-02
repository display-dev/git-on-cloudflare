import { deleteCookie, getCookie, setCookie } from "hono/cookie";
import type { CookieOptions } from "hono/utils/cookie";

import type { AppContext } from "@/worker/routes/hono";

// __Host- prefix requires Secure + Path=/ + no Domain attribute. Cloudflare
// custom domains serve this app over HTTPS, and the test pool runs over a
// Secure-origin URL, so the prefix is safe to use everywhere.
export const SESSION_COOKIE_NAME = "__Host-goc_session";
export const OIDC_TX_COOKIE_NAME = "__Host-goc_oidc";

const SESSION_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 30; // 30 days
const OIDC_TX_COOKIE_MAX_AGE_SECONDS = 5 * 60; // matches sealed-state TTL

const SHARED_COOKIE_OPTIONS = {
  httpOnly: true,
  secure: true,
  sameSite: "Lax",
  path: "/",
} as const satisfies CookieOptions;

export function setSessionCookie(c: AppContext, opaqueToken: string): void {
  setCookie(c, SESSION_COOKIE_NAME, opaqueToken, {
    ...SHARED_COOKIE_OPTIONS,
    maxAge: SESSION_COOKIE_MAX_AGE_SECONDS,
  });
}

export function getSessionCookie(c: AppContext): string | undefined {
  return getCookie(c, SESSION_COOKIE_NAME);
}

export function clearSessionCookie(c: AppContext): void {
  deleteCookie(c, SESSION_COOKIE_NAME, SHARED_COOKIE_OPTIONS);
}

export function setOidcTransactionCookie(c: AppContext, sealed: string): void {
  setCookie(c, OIDC_TX_COOKIE_NAME, sealed, {
    ...SHARED_COOKIE_OPTIONS,
    maxAge: OIDC_TX_COOKIE_MAX_AGE_SECONDS,
  });
}

export function getOidcTransactionCookie(c: AppContext): string | undefined {
  return getCookie(c, OIDC_TX_COOKIE_NAME);
}

export function clearOidcTransactionCookie(c: AppContext): void {
  deleteCookie(c, OIDC_TX_COOKIE_NAME, SHARED_COOKIE_OPTIONS);
}
