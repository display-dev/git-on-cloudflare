import type { Ref } from "@/worker/git";
import { classifyRef, formatRefOption } from "@/shared/git/ref-display";
import { isValidOwnerRepo } from "@/shared/web";
import { repoKey } from "@/worker/keys";
import { loadHeadAndRefsCached } from "./helpers";
import type { RepoParams, RouteArgs } from "../hono";

export async function handleRefsApi({ request, env, ctx, params }: RouteArgs<RepoParams>) {
  const { owner, repo } = params;
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
  } catch (e: any) {
    return new Response(
      JSON.stringify({ branches: [], tags: [], error: String(e?.message || e) }),
      {
        status: 500,
        headers: { "Content-Type": "application/json" },
      }
    );
  }
}
