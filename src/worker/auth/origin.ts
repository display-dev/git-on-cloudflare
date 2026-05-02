import type { AppContext } from "@/worker/routes/hono";

const SAFE_METHODS: ReadonlySet<string> = new Set(["GET", "HEAD", "OPTIONS"]);

// Returns a 403 Response if a non-safe method lacks an Origin header that
// matches the request origin. SameSite=Lax already keeps cookies off most
// cross-site mutations; this is the explicit defense for cookie-mutating
// POST routes (sign-out, PAT mutations) without resorting to a custom CSRF
// header. Returns null when the request is allowed to proceed.
export function sameOriginViolation(c: AppContext): Response | null {
  if (SAFE_METHODS.has(c.req.method)) return null;
  const origin = c.req.header("origin");
  if (!origin) {
    return new Response("Forbidden\n", { status: 403 });
  }
  let originUrl: URL;
  try {
    originUrl = new URL(origin);
  } catch {
    return new Response("Forbidden\n", { status: 403 });
  }
  if (originUrl.origin !== new URL(c.req.url).origin) {
    return new Response("Forbidden\n", { status: 403 });
  }
  return null;
}
