/// <reference lib="dom" />

import { useMemo, useState } from "react";
import { FolderGit2, Globe, Lock, Plus } from "lucide-react";

import { hydrateIsland } from "@/client/hydrate";
import { Button, Card, EmptyState, ErrorBanner, Input } from "@/client/components/ui";

// SSR-side repository summary mirrored to keep the island bundle decoupled
// from the page-side `AccountRepository` shape. Browser islands intentionally
// re-declare wire types so SSR rename ripples don't reach the bundle.
export type RepositoriesIslandSummary = {
  id: string;
  slug: string;
  namespaceSlug: string;
  visibility: "public" | "private";
  updatedAt: number;
};

export type RepositoriesIslandProps = {
  primaryNamespaceSlug?: string;
  repositories: RepositoriesIslandSummary[];
};

// Cross-island event used by the tokens island (and any future repo-aware
// island) to refresh its cached repo list when the user creates or toggles
// a repo. Exported as a constant so consumers don't typo the name.
export const REPOSITORIES_CHANGED_EVENT = "goc:repositories-changed";

export type RepositoriesChangedDetail = {
  repositories: RepositoriesIslandSummary[];
};

function formatRelativeTime(epochMs: number): string {
  const delta = Date.now() - epochMs;
  if (delta < 60_000) return "just now";
  const minutes = Math.floor(delta / 60_000);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  return new Date(epochMs).toISOString().slice(0, 10);
}

type CreateResult =
  | {
      ok: true;
      id: string;
      namespaceSlug: string;
      slug: string;
      visibility: "public" | "private";
      updatedAt: number;
    }
  | {
      ok: false;
      reason:
        | "invalid-slug"
        | "invalid-visibility"
        | "namespace-not-found"
        | "not-member"
        | "slug-taken";
    };

type ToggleResult =
  | { ok: true; id: string; visibility: "public" | "private"; previous: "public" | "private" }
  | { ok: false; reason: "invalid-payload" | "not-found" | "not-member" };

function compareRepositoriesByName(
  left: RepositoriesIslandSummary,
  right: RepositoriesIslandSummary
): number {
  const namespaceOrder = left.namespaceSlug.localeCompare(right.namespaceSlug);
  if (namespaceOrder !== 0) return namespaceOrder;
  return left.slug.localeCompare(right.slug);
}

export function RepositoriesIsland({
  primaryNamespaceSlug,
  repositories: initialRepos,
}: RepositoriesIslandProps) {
  const [repositories, setRepositories] = useState(initialRepos);
  const [showForm, setShowForm] = useState(false);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const broadcast = (next: RepositoriesIslandSummary[]) => {
    setRepositories(next);
    if (typeof window !== "undefined") {
      const detail: RepositoriesChangedDetail = { repositories: next };
      window.dispatchEvent(new CustomEvent(REPOSITORIES_CHANGED_EVENT, { detail }));
    }
  };

  async function handleCreate(payload: {
    namespaceSlug: string;
    slug: string;
    visibility: "public" | "private";
  }): Promise<boolean> {
    setError(null);
    try {
      const res = await fetch("/auth/api/repositories", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const body = (await res.json().catch(() => null)) as CreateResult | null;
      if (!body) {
        setError("Could not create repository");
        return false;
      }
      if (!body.ok) {
        setError(describeCreateReason(body.reason));
        return false;
      }
      const next = [
        ...repositories,
        {
          id: body.id,
          slug: body.slug,
          namespaceSlug: body.namespaceSlug,
          visibility: body.visibility,
          updatedAt: body.updatedAt,
        },
      ].sort(compareRepositoriesByName);
      broadcast(next);
      return true;
    } catch (err) {
      setError(String(err));
      return false;
    }
  }

  async function handleToggle(repo: RepositoriesIslandSummary) {
    if (pendingId) return;
    const next = repo.visibility === "private" ? "public" : "private";
    if (next === "public") {
      const confirmed = confirm(
        `Make ${repo.namespaceSlug}/${repo.slug} public? Anyone will be able to read it.`
      );
      if (!confirmed) return;
    }
    setPendingId(repo.id);
    setError(null);
    try {
      const res = await fetch(`/auth/api/repositories/${encodeURIComponent(repo.id)}`, {
        method: "PATCH",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ visibility: next }),
      });
      const body = (await res.json().catch(() => null)) as ToggleResult | null;
      if (!body || !body.ok) {
        const reason = body && !body.ok ? body.reason : "unknown";
        setError(`Could not toggle visibility (${reason})`);
        return;
      }
      const updated = repositories.map((r) =>
        r.id === repo.id ? { ...r, visibility: body.visibility } : r
      );
      broadcast(updated);
    } catch (err) {
      setError(String(err));
    } finally {
      setPendingId(null);
    }
  }

  return (
    <div className="grid gap-3">
      {error ? <ErrorBanner>{error}</ErrorBanner> : null}
      {repositories.length === 0 ? (
        <div className="flex flex-col items-center gap-3">
          <EmptyState
            icon={<FolderGit2 className="h-5 w-5 text-zinc-400" aria-hidden="true" />}
            title="Repositories you own will appear here."
            detail="Create one below; once it exists, you can use a PAT to clone or push."
          />
          {!showForm ? (
            <Button type="button" variant="primary" size="md" onClick={() => setShowForm(true)}>
              <Plus className="h-4 w-4" aria-hidden="true" />
              New repository
            </Button>
          ) : null}
        </div>
      ) : (
        <div className="overflow-hidden rounded-2xl border border-zinc-300 dark:border-zinc-800/60">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Visibility</th>
                <th className="text-right">Updated</th>
              </tr>
            </thead>
            <tbody>
              {repositories.map((repo) => (
                <tr key={repo.id}>
                  <td>
                    <a
                      href={`/${repo.namespaceSlug}/${repo.slug}`}
                      className="font-mono text-sm text-zinc-900 no-underline hover:text-accent-600 dark:text-zinc-100 dark:hover:text-accent-400"
                    >
                      {repo.namespaceSlug}/{repo.slug}
                    </a>
                  </td>
                  <td>
                    <button
                      type="button"
                      onClick={() => void handleToggle(repo)}
                      disabled={pendingId === repo.id}
                      className={`inline-flex items-center transition-opacity hover:opacity-80 disabled:opacity-50 ${
                        repo.visibility === "private"
                          ? "text-amber-700 dark:text-amber-400"
                          : "text-emerald-700 dark:text-emerald-400"
                      }`}
                      title={`${repo.visibility === "private" ? "Private" : "Public"} - click to toggle`}
                      aria-label={`${repo.visibility === "private" ? "Private" : "Public"} repository - click to toggle`}
                    >
                      {repo.visibility === "private" ? (
                        <Lock className="h-3.5 w-3.5" aria-hidden="true" />
                      ) : (
                        <Globe className="h-3.5 w-3.5" aria-hidden="true" />
                      )}
                    </button>
                  </td>
                  <td className="text-right text-xs text-zinc-500">
                    {formatRelativeTime(repo.updatedAt)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {showForm ? (
        <CreateForm
          primaryNamespaceSlug={primaryNamespaceSlug}
          onCancel={() => setShowForm(false)}
          onSubmit={async (payload) => {
            const ok = await handleCreate(payload);
            if (ok) setShowForm(false);
            return ok;
          }}
        />
      ) : repositories.length > 0 ? (
        <div className="flex justify-end">
          <Button type="button" variant="ghost" size="sm" onClick={() => setShowForm(true)}>
            <Plus className="h-4 w-4" aria-hidden="true" />
            New repository
          </Button>
        </div>
      ) : null}
    </div>
  );
}

function describeCreateReason(reason: Exclude<CreateResult, { ok: true }>["reason"]): string {
  switch (reason) {
    case "invalid-slug":
      return "Slug must be lowercase letters, digits, and dashes (1-40 chars).";
    case "invalid-visibility":
      return "Pick a visibility option.";
    case "namespace-not-found":
      return "Namespace not found.";
    case "not-member":
      return "You are not a member of that namespace.";
    case "slug-taken":
      return "A repository with that slug already exists.";
  }
}

type CreateFormProps = {
  primaryNamespaceSlug?: string;
  onCancel: () => void;
  onSubmit: (payload: {
    namespaceSlug: string;
    slug: string;
    visibility: "public" | "private";
  }) => Promise<boolean>;
};

function CreateForm({ primaryNamespaceSlug, onCancel, onSubmit }: CreateFormProps) {
  const [namespaceSlug, setNamespaceSlug] = useState(primaryNamespaceSlug ?? "");
  const [slug, setSlug] = useState("");
  const [visibility, setVisibility] = useState<"public" | "private">("private");
  const [submitting, setSubmitting] = useState(false);

  const canSubmit = useMemo(() => {
    if (submitting) return false;
    return namespaceSlug.trim().length > 0 && slug.trim().length > 0;
  }, [submitting, namespaceSlug, slug]);

  return (
    <Card>
      <form
        className="grid gap-5"
        onSubmit={async (event) => {
          event.preventDefault();
          if (!canSubmit) return;
          setSubmitting(true);
          try {
            await onSubmit({
              namespaceSlug: namespaceSlug.trim().toLowerCase(),
              slug: slug.trim().toLowerCase(),
              visibility,
            });
          } finally {
            setSubmitting(false);
          }
        }}
      >
        <h3 className="m-0 text-xs font-semibold uppercase tracking-wider text-zinc-500">
          New repository
        </h3>
        <Input
          label="Namespace"
          type="text"
          value={namespaceSlug}
          onChange={(event) => setNamespaceSlug(event.target.value)}
          placeholder={primaryNamespaceSlug ?? "rachel"}
          helperText="The namespace to create under (your handle by default)."
          required
        />
        <Input
          label="Slug"
          type="text"
          value={slug}
          onChange={(event) => setSlug(event.target.value)}
          placeholder="site"
          helperText="Lowercase ASCII, dashes between letters/digits. 1-40 chars."
          required
        />
        <VisibilityTiles visibility={visibility} onChange={setVisibility} />
        <div className="flex items-center justify-end gap-2">
          <Button type="button" variant="ghost" size="sm" onClick={onCancel}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" size="md" disabled={!canSubmit}>
            <Plus className="h-4 w-4" aria-hidden="true" />
            Create repository
          </Button>
        </div>
      </form>
    </Card>
  );
}

type VisibilityTilesProps = {
  visibility: "public" | "private";
  onChange: (next: "public" | "private") => void;
};

// Mirrors the PermissionLevelTiles shape from `tokens.tsx` so the account
// page reads as one cohesive control language.
function VisibilityTiles({ visibility, onChange }: VisibilityTilesProps) {
  const tile =
    "group relative flex cursor-pointer flex-col gap-1 rounded-xl border border-zinc-300 p-4 transition-colors hover:border-zinc-400 has-[:checked]:border-accent-500 has-[:checked]:bg-accent-500/5 has-[:focus-visible]:ring-2 has-[:focus-visible]:ring-accent-500/40 dark:border-zinc-800/60 dark:hover:border-zinc-700";
  return (
    <fieldset className="m-0 grid gap-3 border-0 p-0 sm:grid-cols-2">
      <legend className="sr-only">Visibility</legend>
      <label className={tile}>
        <input
          type="radio"
          name="repo-visibility"
          value="private"
          className="sr-only"
          checked={visibility === "private"}
          onChange={() => onChange("private")}
        />
        <span className="flex items-center gap-2 font-mono text-[11px] font-semibold uppercase tracking-widest text-zinc-700 group-has-[:checked]:text-accent-600 dark:text-zinc-300 dark:group-has-[:checked]:text-accent-400">
          <Lock className="h-3 w-3" aria-hidden="true" />
          Private
        </span>
        <span className="text-sm text-zinc-600 dark:text-zinc-400">
          Only namespace members can read.
        </span>
      </label>
      <label className={tile}>
        <input
          type="radio"
          name="repo-visibility"
          value="public"
          className="sr-only"
          checked={visibility === "public"}
          onChange={() => onChange("public")}
        />
        <span className="flex items-center gap-2 font-mono text-[11px] font-semibold uppercase tracking-widest text-zinc-700 group-has-[:checked]:text-accent-600 dark:text-zinc-300 dark:group-has-[:checked]:text-accent-400">
          <Globe className="h-3 w-3" aria-hidden="true" />
          Public
        </span>
        <span className="text-sm text-zinc-600 dark:text-zinc-400">Anyone can read.</span>
      </label>
    </fieldset>
  );
}

export function initRepositoriesIsland() {
  hydrateIsland("repositories", RepositoriesIsland);
}
