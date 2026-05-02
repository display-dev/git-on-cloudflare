import { renderUiView } from "@/client/server/render";
import type { Viewer } from "@/client/server/viewer";

const HTML_HEADERS = {
  "Content-Type": "text/html; charset=utf-8",
  "X-Page-Renderer": "react-ssr",
};

type RenderUiDocumentOptions = {
  status?: number;
  cacheControl?: string;
  failureBody: string;
  failureStatus?: number;
  headers?: HeadersInit;
  // Optional signed-in viewer threaded into the SSR shell. Route handlers
  // that want the signed-in header must call `loadViewer(c)` and pass the
  // result here. The renderer itself never reads cookies or D1.
  viewer?: Viewer | null;
};

/**
 * Renders a registered React SSR document with the headers used by route pages.
 * Callers still choose the cache policy because owner listings are cacheable
 * while repository views intentionally remain no-store.
 */
export async function renderUiDocumentResponse(
  env: Env,
  view: string,
  data: Record<string, unknown>,
  options: RenderUiDocumentOptions
): Promise<Response> {
  const body = await renderUiView(env, view, data, { viewer: options.viewer ?? null });
  if (!body) {
    return new Response(options.failureBody, { status: options.failureStatus ?? 500 });
  }

  const headers = new Headers(options.headers);
  for (const [name, value] of Object.entries(HTML_HEADERS)) {
    if (!headers.has(name)) headers.set(name, value);
  }
  if (!headers.has("Cache-Control")) {
    headers.set("Cache-Control", options.cacheControl ?? "no-store, no-cache, must-revalidate");
  }

  return new Response(body, {
    status: options.status ?? 200,
    headers,
  });
}
