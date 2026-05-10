import type { CacheContext } from "./cache";

// Cache-policy discriminator used by visibility-aware request paths to
// decide whether the shared Workers Cache may be read or written. Public
// repositories cache freely; private repositories must bypass shared
// cache reads/writes regardless of upstream success.
export type SharedCachePolicy = "allow-shared-cache" | "bypass-shared-cache";

// CacheContext memo flag tags. `no-cache-read`/`no-cache-write` are honored
// by the shared cache helpers; route handlers that mark a request private
// must set both before calling any cache-aware loader.
const NO_CACHE_READ = "no-cache-read";
const NO_CACHE_WRITE = "no-cache-write";

// Marks a request as private so any downstream shared-cache lookup or
// write is skipped. Idempotent: callers can mark private once at gate
// time and trust that all subsequent helpers honor it.
export function markRequestPrivate(cacheCtx: CacheContext): void {
  cacheCtx.memo = cacheCtx.memo || {};
  cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
  cacheCtx.memo.flags.add(NO_CACHE_READ);
  cacheCtx.memo.flags.add(NO_CACHE_WRITE);
}

// Returns true if the request was marked private (private repo, sensitive
// data path, etc.). Used to gate shared-cache reads/writes and to pick the
// `Cache-Control` response header.
export function isRequestPrivate(cacheCtx: CacheContext | undefined): boolean {
  return cacheCtx?.memo?.flags?.has(NO_CACHE_READ) === true;
}

// HTTP `Cache-Control` value for responses serving repo data over Git or
// data APIs:
// - `mutating: true` -> always "no-store" (credentialed mutating paths
//   such as receive-pack must never sit in shared caches).
// - private (membership-derived) request -> "no-store".
// - otherwise -> "no-cache" so downstream caches re-validate but may
//   reuse bodies briefly.
export function responseCacheControl(
  cacheCtx: CacheContext | undefined,
  options?: { mutating?: boolean }
): "no-store" | "no-cache" {
  if (options?.mutating) return "no-store";
  return isRequestPrivate(cacheCtx) ? "no-store" : "no-cache";
}
