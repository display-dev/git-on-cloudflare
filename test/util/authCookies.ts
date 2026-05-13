import { generateSignedCookie } from "hono/cookie";

import {
  AUTH_COOKIE_PREFIX,
  OIDC_TX_COOKIE_NAME,
  SESSION_COOKIE_HEADER_NAME,
} from "@/worker/auth/cookies";
import {
  deriveTransactionCookieSecret,
  encodeTransactionPayload,
  type TransactionPayload,
} from "@/worker/auth/oidc";

export function sessionCookieHeader(token: string): string {
  return `${SESSION_COOKIE_HEADER_NAME}=${token}`;
}

export function extractSessionToken(setCookieHeader: string | null): string | null {
  if (!setCookieHeader) return null;
  const pattern = new RegExp(`${SESSION_COOKIE_HEADER_NAME}=(goc_sess_[^;]+)`);
  return pattern.exec(setCookieHeader)?.[1] ?? null;
}

export async function oidcTransactionCookieHeader(
  clientSecret: string,
  payload: TransactionPayload
): Promise<string> {
  const signingSecret = await deriveTransactionCookieSecret(clientSecret);
  const setCookie = await generateSignedCookie(
    OIDC_TX_COOKIE_NAME,
    encodeTransactionPayload(payload),
    signingSecret,
    { prefix: AUTH_COOKIE_PREFIX }
  );
  return setCookie.split(";")[0] ?? "";
}
