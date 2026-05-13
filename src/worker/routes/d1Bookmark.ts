import { getCookie, setCookie } from "hono/cookie";
import type { CookieOptions } from "hono/utils/cookie";
import type { AppContext } from "./hono";

// D1 Sessions API transport contract. Hono middleware opens exactly one
// `D1DatabaseSession` per request and uses these helpers to carry the
// advanced bookmark into the next request. Hono's `host` prefix option
// serializes this as a `__Host-` cookie so browsers enforce `Secure` +
// `Path=/` + no `Domain`; `HttpOnly` because the bookmark is consistency
// metadata, not something the SSR UI needs to read from JS.
export const D1_BOOKMARK_HEADER = "x-goc-d1-bookmark";
export const D1_BOOKMARK_COOKIE_NAME = "goc-d1-bm";
export const D1_BOOKMARK_COOKIE_MAX_AGE_SECONDS = 300;

// D1 bookmarks are short Lamport-style logical-clock tokens. The current
// format is four hyphen-separated hex groups (`xxxxxxxx-xxxxxxxx-xxxxxxxx-`
// `xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`, 59 chars total) and they are
// lexicographically sortable; see
// https://developers.cloudflare.com/d1/reference/time-travel/#bookmarks.
// We still treat the value as opaque per the API contract, but cap the
// accepted length well below the cookie/header size envelope so a
// malformed inbound value cannot inflate downstream serialization. The
// control-character check keeps stray bytes from poisoning `Set-Cookie`
// or response-header emission.
const D1_BOOKMARK_MAX_LENGTH = 256;
const CONTROL_CHARACTER_PATTERN = /[\x00-\x1F\x7F]/;
const D1_BOOKMARK_COOKIE_PREFIX = "host";
const D1_BOOKMARK_COOKIE_OPTIONS = {
  httpOnly: true,
  sameSite: "Lax",
  maxAge: D1_BOOKMARK_COOKIE_MAX_AGE_SECONDS,
  prefix: D1_BOOKMARK_COOKIE_PREFIX,
} as const satisfies CookieOptions;

function sanitizeBookmark(raw: string | null): string | null {
  if (raw === null) return null;
  const trimmed = raw.trim();
  if (trimmed.length === 0) return null;
  if (trimmed.length > D1_BOOKMARK_MAX_LENGTH) return null;
  if (CONTROL_CHARACTER_PATTERN.test(trimmed)) return null;
  return trimmed;
}

export function readInboundD1Bookmark(c: AppContext): string | null {
  // Header takes precedence over cookie so a programmatic client can
  // override whatever the browser happens to be carrying.
  const headerValue = sanitizeBookmark(c.req.raw.headers.get(D1_BOOKMARK_HEADER));
  if (headerValue) return headerValue;
  return sanitizeBookmark(getCookie(c, D1_BOOKMARK_COOKIE_NAME, D1_BOOKMARK_COOKIE_PREFIX) ?? null);
}

export function emitD1Bookmark(
  c: AppContext,
  session: D1DatabaseSession,
  inboundBookmark: string | null
): void {
  const bookmark = session.getBookmark();
  // No queries ran on the session (or none advanced state).
  if (bookmark === null) return;
  // Avoid header/cookie churn when the bookmark hasn't moved past the
  // value we already observed on the request. This also covers
  // primary-anchored requests that didn't actually perform a write.
  if (bookmark === inboundBookmark) return;
  c.header(D1_BOOKMARK_HEADER, bookmark);
  setCookie(c, D1_BOOKMARK_COOKIE_NAME, bookmark, D1_BOOKMARK_COOKIE_OPTIONS);
}
