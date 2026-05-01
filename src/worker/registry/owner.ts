// Owner repository registry backed by Workers KV.
// Uses per-repo keys to avoid read-modify-write races on a single JSON array.
// Key format: owner:<owner>:<repo> -> "1"

function key(owner: string, repo: string) {
  return `owner:${owner}:${repo}`;
}

function prefix(owner: string) {
  return `owner:${owner}:`;
}

// Minimal shape for KV list results (avoids bringing in external types)
type KVListResult = { keys: Array<{ name: string }>; list_complete: boolean; cursor?: string };

export async function addRepoToOwner(env: Env, owner: string, repo: string) {
  await env.OWNER_REGISTRY.put(key(owner, repo), "1");
}

export async function removeRepoFromOwner(env: Env, owner: string, repo: string) {
  await env.OWNER_REGISTRY.delete(key(owner, repo));
}

export async function listReposForOwner(env: Env, owner: string): Promise<string[]> {
  const pfx = prefix(owner);
  const repos: string[] = [];
  let cursor: string | undefined = undefined;
  do {
    const res = (await env.OWNER_REGISTRY.list({ prefix: pfx, cursor })) as KVListResult;
    for (const k of res.keys || []) {
      const name = k.name || "";
      const repo = name.slice(pfx.length);
      if (repo) repos.push(repo);
    }
    cursor = res.list_complete ? undefined : res.cursor;
  } while (cursor);
  return repos.sort((a, b) => a.localeCompare(b));
}
