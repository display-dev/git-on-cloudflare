import type { Ref } from "@/worker/git";
import { classifyRef, formatRefOption } from "@/shared/git/ref-display";
import { isRequestPrivate, loadHeadAndRefsCached, resolveUiRepoAccess } from "./helpers";
import type { AppContext } from "../hono";

export async function handleRefsApi(c: AppContext<"/:owner/:repo/api/refs">) {
  const env = c.env;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  const access = await resolveUiRepoAccess(c, owner, repo, { responseShape: "json" });
  if (access.kind === "response") return access.response;
  const { route, cacheCtx } = access;
  try {
    const refsData = await loadHeadAndRefsCached(env, cacheCtx, route.doName);
    const refs: Ref[] = refsData?.refs || [];
    const branches = refs.filter((ref) => classifyRef(ref.name) === "branch").map(formatRefOption);
    const tags = refs.filter((ref) => classifyRef(ref.name) === "tag").map(formatRefOption);
    const isPrivate = isRequestPrivate(cacheCtx);
    return new Response(JSON.stringify({ branches, tags }), {
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": isPrivate ? "no-store" : "public, max-age=60",
      },
    });
  } catch (e) {
    return new Response(
      JSON.stringify({ branches: [], tags: [], error: e instanceof Error ? e.message : String(e) }),
      {
        status: 500,
        headers: { "Content-Type": "application/json" },
      }
    );
  }
}
