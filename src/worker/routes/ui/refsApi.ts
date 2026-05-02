import type { Ref } from "@/worker/git";
import { classifyRef, formatRefOption } from "@/shared/git/ref-display";
import { isValidOwnerRepo } from "@/shared/web";
import { repoKey } from "@/worker/keys";
import { loadHeadAndRefsCached } from "./helpers";
import type { AppContext } from "../hono";

export async function handleRefsApi(c: AppContext<"/:owner/:repo/api/refs">) {
  const request = c.req.raw;
  const env = c.env;
  const ctx = c.executionCtx;
  const owner = c.req.param("owner");
  const repo = c.req.param("repo");
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return new Response(JSON.stringify({ branches: [], tags: [] }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }
  const repoId = repoKey(owner, repo);
  try {
    const refsData = await loadHeadAndRefsCached(env, request, ctx, repoId);
    const refs: Ref[] = refsData?.refs || [];
    const branches = refs.filter((ref) => classifyRef(ref.name) === "branch").map(formatRefOption);
    const tags = refs.filter((ref) => classifyRef(ref.name) === "tag").map(formatRefOption);
    return new Response(JSON.stringify({ branches, tags }), {
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=60",
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
