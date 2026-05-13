import { deleteCookie, getCookie, getSignedCookie, setCookie, setSignedCookie } from "hono/cookie";
import type { CookieOptions } from "hono/utils/cookie";

import type { AppContext } from "@/worker/routes/hono";

// Hono's `host` prefix option serializes these logical cookie names with
// the `__Host-` prefix, forcing Secure + Path=/ + no Domain on the wire.
// Cloudflare custom domains serve this app over HTTPS, and the test pool
// runs over a Secure-origin URL, so the prefix is safe to use everywhere.
export const AUTH_COOKIE_PREFIX = "host";
export const SESSION_COOKIE_NAME = "goc_session";
export const OIDC_TX_COOKIE_NAME = "goc_oidc";
export const SESSION_COOKIE_HEADER_NAME = `__Host-${SESSION_COOKIE_NAME}`;
export const OIDC_TX_COOKIE_HEADER_NAME = `__Host-${OIDC_TX_COOKIE_NAME}`;

const SESSION_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 30; // 30 days
const OIDC_TX_COOKIE_MAX_AGE_SECONDS = 5 * 60; // matches transaction payload TTL

const SHARED_COOKIE_OPTIONS = {
  httpOnly: true,
  sameSite: "Lax",
  prefix: AUTH_COOKIE_PREFIX,
} as const satisfies CookieOptions;

export type SignedCookieReadResult =
  | { kind: "ok"; value: string }
  | { kind: "missing" }
  | { kind: "invalid_signature" };
export type CookieSigningSecret = BufferSource;

export function setSessionCookie(c: AppContext, opaqueToken: string): void {
  setCookie(c, SESSION_COOKIE_NAME, opaqueToken, {
    ...SHARED_COOKIE_OPTIONS,
    maxAge: SESSION_COOKIE_MAX_AGE_SECONDS,
  });
}

export function getSessionCookie(c: AppContext): string | undefined {
  return getCookie(c, SESSION_COOKIE_NAME, AUTH_COOKIE_PREFIX);
}

export function clearSessionCookie(c: AppContext): void {
  deleteCookie(c, SESSION_COOKIE_NAME, SHARED_COOKIE_OPTIONS);
}

export async function setOidcTransactionCookie(
  c: AppContext,
  value: string,
  secret: CookieSigningSecret
): Promise<void> {
  await setSignedCookie(c, OIDC_TX_COOKIE_NAME, value, secret, {
    ...SHARED_COOKIE_OPTIONS,
    maxAge: OIDC_TX_COOKIE_MAX_AGE_SECONDS,
  });
}

export async function getOidcTransactionCookie(
  c: AppContext,
  secret: CookieSigningSecret
): Promise<SignedCookieReadResult> {
  const rawValue = getCookie(c, OIDC_TX_COOKIE_NAME, AUTH_COOKIE_PREFIX);
  if (rawValue === undefined) return { kind: "missing" };
  const value = await getSignedCookie(c, secret, OIDC_TX_COOKIE_NAME, AUTH_COOKIE_PREFIX);
  if (value === undefined || value === false) return { kind: "invalid_signature" };
  return { kind: "ok", value };
}

export function clearOidcTransactionCookie(c: AppContext): void {
  deleteCookie(c, OIDC_TX_COOKIE_NAME, SHARED_COOKIE_OPTIONS);
}
