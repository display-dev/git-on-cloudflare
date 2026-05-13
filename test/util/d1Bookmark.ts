import {
  D1_BOOKMARK_COOKIE_MAX_AGE_SECONDS,
  D1_BOOKMARK_COOKIE_NAME,
  D1_BOOKMARK_HEADER,
} from "@/worker/routes/d1Bookmark";

export { D1_BOOKMARK_HEADER, D1_BOOKMARK_COOKIE_NAME, D1_BOOKMARK_COOKIE_MAX_AGE_SECONDS };
export const D1_BOOKMARK_COOKIE_HEADER_NAME = `__Host-${D1_BOOKMARK_COOKIE_NAME}`;

// Parses `Set-Cookie` headers and pulls out the bookmark cookie. Tests
// drive the worker through `workerExports.default.fetch`, which surfaces
// a single combined `Set-Cookie` header even when the worker appended
// several cookies; we split on the conventional comma-newline pattern
// and look for our entry by name. Attribute values are the raw strings
// the worker emitted (after URL-decoding the cookie value itself).
export type ParsedBookmarkCookie = {
  value: string;
  attributes: Record<string, string | true>;
};

export function parseBookmarkCookie(setCookieHeader: string | null): ParsedBookmarkCookie | null {
  if (!setCookieHeader) return null;
  // Multiple cookies in a combined header are separated by `, ` in
  // workerd's surface; entries can themselves contain commas inside
  // `Expires=...`, but the bookmark cookie only carries `Max-Age`, so
  // splitting on `,` followed by a space is safe here.
  const entries = setCookieHeader.split(/, (?=[^ ]+=)/);
  for (const entry of entries) {
    const [head, ...rest] = entry.split(";");
    if (!head) continue;
    const eq = head.indexOf("=");
    if (eq === -1) continue;
    const name = head.slice(0, eq).trim();
    if (name !== D1_BOOKMARK_COOKIE_HEADER_NAME) continue;
    const rawValue = head.slice(eq + 1).trim();
    const attributes: Record<string, string | true> = {};
    for (const attr of rest) {
      const trimmed = attr.trim();
      if (!trimmed) continue;
      const aeq = trimmed.indexOf("=");
      if (aeq === -1) {
        attributes[trimmed] = true;
      } else {
        attributes[trimmed.slice(0, aeq)] = trimmed.slice(aeq + 1);
      }
    }
    return { value: decodeURIComponent(rawValue), attributes };
  }
  return null;
}
