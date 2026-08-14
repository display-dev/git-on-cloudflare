import { constantTimeEquals } from "@/worker/auth/pat";
import type { AppContext } from "./hono";

export async function authorizeInternalRequest(c: AppContext): Promise<Response | null> {
  const configuredToken = c.env.INGESTION_RPC_TOKEN;
  if (!configuredToken) return new Response("Not found\n", { status: 404 });
  const match = /^Bearer (.+)$/.exec(c.req.header("Authorization") ?? "");
  if (!match || !(await constantTimeEquals(configuredToken, match[1]!))) {
    return Response.json(
      { error: "Unauthorized" },
      { status: 401, headers: { "Cache-Control": "no-store" } }
    );
  }
  return null;
}
