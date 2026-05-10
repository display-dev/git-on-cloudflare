/// <reference lib="dom" />

import { useEffect, useMemo, useState } from "react";

import { hydrateIsland } from "@/client/hydrate";
import { Button, Card, EmptyState, ErrorBanner, Input } from "@/client/components/ui";
import { Copy, FolderGit2, KeyRound, Trash2 } from "lucide-react";
import {
  REPOSITORIES_CHANGED_EVENT,
  type RepositoriesChangedDetail,
} from "@/client/islands/repositories";

// Mirror of `PatGrantLevel` from `@/worker/db/d1/schema/patNamespaceGrants`.
// The island purposely re-declares wire-shape types in its own file (see
// also `TokensIslandSummary`) so the SSR boundary stays one-way.
type Level = "pull" | "push";

export type TokensIslandSummary = {
  id: string;
  name: string;
  prefix: string;
  createdAt: number;
  expiresAt?: number | null;
  revokedAt?: number | null;
  lastUsedAt?: number | null;
  namespaceGrants: Array<{ namespaceSlug: string; level: Level }>;
  repoGrants: Array<{
    namespaceSlug: string;
    repoSlug: string;
    level: Level;
  }>;
};

// SSR-boundary mirror of `AccountRepository`. Re-declared here for the
// same reason as `TokensIslandSummary` above: keep the SSR boundary
// one-way so a page-side type rename doesn't ripple into the bundle.
export type IslandRepository = {
  id: string;
  slug: string;
  namespaceSlug: string;
  visibility: "public" | "private";
  updatedAt: number;
};

export type TokensIslandProps = {
  primaryNamespaceSlug?: string;
  repositories: IslandRepository[];
  tokens: TokensIslandSummary[];
};

// Tagged-union request shape mirroring the server contract. `scope` and
// `level` are both required on the wire.
type CreatePayload =
  | {
      scope: "namespace";
      name: string;
      namespaceSlug: string;
      level: Level;
    }
  | {
      scope: "repo";
      name: string;
      namespaceSlug: string;
      repoSlug: string;
      level: Level;
    };

type Scope = CreatePayload["scope"];

function describeLevel(level: Level): string {
  return level === "push" ? "pull + push" : "pull";
}

function formatDate(epochMs: number): string {
  return new Date(epochMs).toISOString().slice(0, 10);
}

function formatGrants(token: TokensIslandSummary): string {
  if (token.namespaceGrants.length > 0) {
    return token.namespaceGrants
      .map((grant) => `@${grant.namespaceSlug} (${describeLevel(grant.level)})`)
      .join(", ");
  }
  return token.repoGrants
    .map((grant) => `${grant.namespaceSlug}/${grant.repoSlug} (${describeLevel(grant.level)})`)
    .join(", ");
}

export function TokensIsland({
  primaryNamespaceSlug,
  repositories: initialRepositories,
  tokens: initialTokens,
}: TokensIslandProps) {
  const [tokens, setTokens] = useState(initialTokens);
  const [repositories, setRepositories] = useState(initialRepositories);
  const [scope, setScope] = useState<Scope>("namespace");
  const [name, setName] = useState("");
  const [namespaceSlug, setNamespaceSlug] = useState(primaryNamespaceSlug ?? "");
  const [repoSlug, setRepoSlug] = useState("");
  const [level, setLevel] = useState<Level>("pull");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [revealedToken, setRevealedToken] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  // SSR-side `namespaceSlug` is already canonicalized lowercase; the
  // namespace input is user-typed and can carry stray case/whitespace.
  const filteredRepos = useMemo(() => {
    const target = namespaceSlug.trim().toLowerCase();
    if (target.length === 0) return [];
    return repositories.filter((repo) => repo.namespaceSlug === target);
  }, [repositories, namespaceSlug]);

  const canSubmit = useMemo(() => {
    if (submitting) return false;
    if (name.trim().length === 0) return false;
    if (namespaceSlug.trim().length === 0) return false;
    if (scope === "repo" && !filteredRepos.some((repo) => repo.slug === repoSlug)) return false;
    return true;
  }, [submitting, name, namespaceSlug, scope, repoSlug, filteredRepos]);

  useEffect(() => {
    if (!revealedToken) return;
    setCopied(false);
  }, [revealedToken]);

  // Subscribe to repository creates/visibility changes from the
  // `repositories` island so the repo selector here updates without a
  // page reload. The event detail mirrors the SSR shape.
  useEffect(() => {
    if (typeof window === "undefined") return;
    const handler = (event: Event) => {
      const detail = (event as CustomEvent<RepositoriesChangedDetail>).detail;
      if (!detail || !Array.isArray(detail.repositories)) return;
      setRepositories(detail.repositories);
    };
    window.addEventListener(REPOSITORIES_CHANGED_EVENT, handler);
    return () => window.removeEventListener(REPOSITORIES_CHANGED_EVENT, handler);
  }, []);

  useEffect(() => {
    if (scope !== "repo") return;
    if (repoSlug.length === 0) return;
    if (filteredRepos.some((repo) => repo.slug === repoSlug)) return;
    setRepoSlug("");
  }, [scope, repoSlug, filteredRepos]);

  async function refresh() {
    try {
      const res = await fetch("/auth/api/tokens", { credentials: "same-origin" });
      if (!res.ok) return;
      const body = (await res.json()) as { tokens: TokensIslandSummary[] };
      setTokens(body.tokens);
    } catch {
      // Best-effort; the next mutation will retry.
    }
  }

  async function handleCreate(payload: CreatePayload) {
    setSubmitting(true);
    setError(null);
    try {
      const res = await fetch("/auth/api/tokens", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const body = (await res.json().catch(() => ({}))) as { error?: string };
        setError(body.error ?? "Could not create token");
        return;
      }
      const body = (await res.json()) as { plaintext: string };
      setRevealedToken(body.plaintext);
      setName("");
      setRepoSlug("");
      setLevel("pull");
      await refresh();
    } catch (err) {
      setError(String(err));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleRevoke(id: string) {
    if (!confirm("Revoke this token? This cannot be undone.")) return;
    try {
      const res = await fetch(`/auth/api/tokens/${encodeURIComponent(id)}`, {
        method: "DELETE",
        credentials: "same-origin",
      });
      if (!res.ok) {
        const body = (await res.json().catch(() => ({}))) as { error?: string };
        setError(body.error ?? "Could not revoke token");
        return;
      }
      await refresh();
    } catch (err) {
      setError(String(err));
    }
  }

  const activeTokens = tokens.filter((t) => !t.revokedAt);

  return (
    <div className="grid gap-8">
      {error ? <ErrorBanner>{error}</ErrorBanner> : null}

      {revealedToken ? (
        <RevealPanel
          token={revealedToken}
          copied={copied}
          onCopy={() => {
            navigator.clipboard?.writeText(revealedToken).then(
              () => setCopied(true),
              () => setCopied(false)
            );
          }}
          onDismiss={() => setRevealedToken(null)}
        />
      ) : (
        <Card>
          <form
            className="grid gap-5"
            onSubmit={(event) => {
              event.preventDefault();
              if (!canSubmit) return;
              const payload: CreatePayload =
                scope === "namespace"
                  ? {
                      scope: "namespace",
                      name: name.trim(),
                      namespaceSlug: namespaceSlug.trim(),
                      level,
                    }
                  : {
                      scope: "repo",
                      name: name.trim(),
                      namespaceSlug: namespaceSlug.trim(),
                      repoSlug: repoSlug.trim(),
                      level,
                    };
              void handleCreate(payload);
            }}
          >
            <h3 className="m-0 text-xs font-semibold uppercase tracking-wider text-zinc-500">
              Create token
            </h3>

            <ScopeSegmented scope={scope} onChange={setScope} />

            <Input
              label="Name"
              type="text"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="ci-deploy"
              helperText="A label to recognise this token in the list."
              required
            />
            <Input
              label="Namespace"
              type="text"
              value={namespaceSlug}
              onChange={(event) => setNamespaceSlug(event.target.value)}
              placeholder={primaryNamespaceSlug ?? "rachel"}
              helperText="Defaults to your handle."
              required
            />
            {scope === "repo" ? (
              filteredRepos.length === 0 ? (
                <EmptyState
                  icon={<FolderGit2 className="h-5 w-5 text-zinc-400" aria-hidden="true" />}
                  title={`No repositories under @${namespaceSlug.trim() || primaryNamespaceSlug || "namespace"}`}
                  detail="Repo-scoped tokens become available once a repository exists under this namespace. Switch to namespace scope, or create a repo first."
                />
              ) : (
                <RepoTiles
                  repositories={filteredRepos}
                  selectedSlug={repoSlug}
                  onChange={setRepoSlug}
                />
              )
            ) : null}

            <PermissionLevelTiles level={level} onChange={setLevel} />

            <div className="flex justify-end">
              <Button type="submit" variant="primary" size="md" disabled={!canSubmit}>
                <KeyRound className="h-4 w-4" aria-hidden="true" />
                Generate token
              </Button>
            </div>
          </form>
        </Card>
      )}

      <section className="grid gap-3">
        <h3 className="m-0 text-xs font-semibold uppercase tracking-wider text-zinc-500">
          {tokens.length === 0 ? "Your tokens" : `Active tokens · ${activeTokens.length}`}
        </h3>
        {tokens.length === 0 ? (
          <p className="m-0 font-mono text-xs text-zinc-500">
            <span className="select-none">// </span>no tokens yet, create one above
          </p>
        ) : (
          <Card>
            <ul className="-mx-5 divide-y divide-zinc-200 sm:-mx-6 dark:divide-zinc-800/60">
              {tokens.map((token) => (
                <TokenRow key={token.id} token={token} onRevoke={handleRevoke} />
              ))}
            </ul>
          </Card>
        )}
      </section>
    </div>
  );
}

type RevealPanelProps = {
  token: string;
  copied: boolean;
  onCopy: () => void;
  onDismiss: () => void;
};

// `-webkit-text-security: disc` masks the plaintext while keeping it
// selectable; hover/focus-within reveals it for visual verification.
function RevealPanel({ token, copied, onCopy, onDismiss }: RevealPanelProps) {
  return (
    <Card variant="accent">
      <div className="flex flex-col gap-5">
        <p className="m-0 animate-pulse-soft font-mono text-[10px] font-semibold uppercase tracking-[0.2em] text-accent-600 dark:text-accent-400">
          One time only · copy now
        </p>
        <code className="block break-all py-2 text-center font-mono text-base text-zinc-900 select-all [-webkit-text-security:disc] hover:[-webkit-text-security:none] focus-within:[-webkit-text-security:none] sm:text-lg dark:text-zinc-100">
          {token}
        </code>
        <div className="flex items-center justify-between gap-3">
          <Button type="button" variant="primary" size="md" onClick={onCopy}>
            <Copy className="h-4 w-4" aria-hidden="true" />
            {copied ? "Copied" : "Copy token"}
          </Button>
          <Button type="button" variant="ghost" size="sm" onClick={onDismiss}>
            Done
          </Button>
        </div>
      </div>
    </Card>
  );
}

type ScopeSegmentedProps = {
  scope: Scope;
  onChange: (next: Scope) => void;
};

function ScopeSegmented({ scope, onChange }: ScopeSegmentedProps) {
  const baseHalf =
    "flex flex-1 flex-col items-start gap-1 px-4 py-3 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent-500/40";
  const activeHalf = "bg-accent-500/10 text-accent-600 dark:text-accent-400";
  const inactiveHalf =
    "text-zinc-500 hover:bg-zinc-50 hover:text-zinc-700 dark:hover:bg-zinc-800/40 dark:hover:text-zinc-300";
  return (
    <div
      className="flex w-full flex-col overflow-hidden rounded-xl border border-zinc-300 sm:flex-row dark:border-zinc-800/60"
      role="radiogroup"
      aria-label="Token scope"
    >
      <button
        type="button"
        className={`${baseHalf} ${scope === "namespace" ? activeHalf : inactiveHalf}`}
        onClick={() => onChange("namespace")}
        role="radio"
        aria-checked={scope === "namespace"}
      >
        <span className="font-mono text-[11px] font-semibold uppercase tracking-widest">
          Namespace
        </span>
        <span className="font-mono text-[11px] text-zinc-500">@rachel</span>
      </button>
      <button
        type="button"
        className={`${baseHalf} border-t border-zinc-300 sm:border-t-0 sm:border-l ${scope === "repo" ? activeHalf : inactiveHalf} dark:border-zinc-800/60`}
        onClick={() => onChange("repo")}
        role="radio"
        aria-checked={scope === "repo"}
      >
        <span className="font-mono text-[11px] font-semibold uppercase tracking-widest">Repo</span>
        <span className="font-mono text-[11px] text-zinc-500">rachel/site</span>
      </button>
    </div>
  );
}

type PermissionLevelTilesProps = {
  level: Level;
  onChange: (next: Level) => void;
};

// Single-select control over the grant level. Push includes pull by
// construction, so the UI cannot express an inconsistent "push without
// pull" state.
function PermissionLevelTiles({ level, onChange }: PermissionLevelTilesProps) {
  const tile =
    "group relative flex cursor-pointer flex-col gap-1 rounded-xl border border-zinc-300 p-4 transition-colors hover:border-zinc-400 has-[:checked]:border-accent-500 has-[:checked]:bg-accent-500/5 has-[:focus-visible]:ring-2 has-[:focus-visible]:ring-accent-500/40 dark:border-zinc-800/60 dark:hover:border-zinc-700";
  return (
    <fieldset className="m-0 grid gap-3 border-0 p-0 sm:grid-cols-2">
      <legend className="sr-only">Permission level</legend>
      <label className={tile}>
        <input
          type="radio"
          name="pat-level"
          value="pull"
          className="sr-only"
          checked={level === "pull"}
          onChange={() => onChange("pull")}
        />
        <span className="font-mono text-[11px] font-semibold uppercase tracking-widest text-zinc-700 group-has-[:checked]:text-accent-600 dark:text-zinc-300 dark:group-has-[:checked]:text-accent-400">
          Pull only
        </span>
        <span className="text-sm text-zinc-600 dark:text-zinc-400">Clone and fetch refs.</span>
      </label>
      <label className={tile}>
        <input
          type="radio"
          name="pat-level"
          value="push"
          className="sr-only"
          checked={level === "push"}
          onChange={() => onChange("push")}
        />
        <span className="font-mono text-[11px] font-semibold uppercase tracking-widest text-zinc-700 group-has-[:checked]:text-accent-600 dark:text-zinc-300 dark:group-has-[:checked]:text-accent-400">
          Pull + push
        </span>
        <span className="text-sm text-zinc-600 dark:text-zinc-400">
          Clone, fetch, and push commits.
        </span>
      </label>
    </fieldset>
  );
}

type RepoTilesProps = {
  repositories: IslandRepository[];
  selectedSlug: string;
  onChange: (next: string) => void;
};

function RepoTiles({ repositories, selectedSlug, onChange }: RepoTilesProps) {
  const tile =
    "group relative flex cursor-pointer items-center justify-between gap-3 rounded-xl border border-zinc-300 px-4 py-3 transition-colors hover:border-zinc-400 has-[:checked]:border-accent-500 has-[:checked]:bg-accent-500/5 has-[:focus-visible]:ring-2 has-[:focus-visible]:ring-accent-500/40 dark:border-zinc-800/60 dark:hover:border-zinc-700";
  return (
    <fieldset className="m-0 grid max-h-60 gap-2 overflow-y-auto border-0 p-0">
      <legend className="sr-only">Repository</legend>
      {repositories.map((repo) => (
        <label key={repo.id} className={tile}>
          <input
            type="radio"
            name="pat-repo"
            value={repo.slug}
            className="sr-only"
            checked={selectedSlug === repo.slug}
            onChange={() => onChange(repo.slug)}
          />
          <span className="min-w-0 truncate font-mono text-sm text-zinc-800 group-has-[:checked]:text-accent-600 dark:text-zinc-200 dark:group-has-[:checked]:text-accent-400">
            {repo.slug}
          </span>
          <span
            className={`shrink-0 font-mono text-[10px] uppercase tracking-widest ${
              repo.visibility === "private"
                ? "text-amber-700 dark:text-amber-400"
                : "text-emerald-700 dark:text-emerald-400"
            }`}
          >
            {repo.visibility}
          </span>
        </label>
      ))}
    </fieldset>
  );
}

type TokenRowProps = {
  token: TokensIslandSummary;
  onRevoke: (id: string) => void;
};

function TokenRow({ token, onRevoke }: TokenRowProps) {
  const grants = formatGrants(token);
  const revoked = Boolean(token.revokedAt);
  const metaParts = [token.prefix, `created ${formatDate(token.createdAt)}`];
  if (revoked && token.revokedAt) metaParts.push(`revoked ${formatDate(token.revokedAt)}`);
  if (grants) metaParts.push(grants);
  return (
    <li className="group flex flex-col items-start justify-between gap-3 px-5 py-3.5 sm:flex-row sm:items-center sm:gap-4 sm:px-6">
      <div className="flex w-full min-w-0 flex-col gap-0.5">
        <span
          className={`text-sm font-medium ${
            revoked
              ? "text-zinc-500 line-through dark:text-zinc-500"
              : "text-zinc-900 dark:text-zinc-100"
          }`}
        >
          {token.name}
        </span>
        <span className="font-mono text-xs break-words text-zinc-500 sm:truncate">
          {metaParts.join(" · ")}
        </span>
      </div>
      {revoked ? null : (
        <Button
          type="button"
          variant="danger"
          size="sm"
          onClick={() => void onRevoke(token.id)}
          className="self-end transition-opacity sm:opacity-0 sm:group-hover:opacity-100 sm:group-focus-within:opacity-100"
        >
          <Trash2 className="h-4 w-4" aria-hidden="true" />
          Revoke
        </Button>
      )}
    </li>
  );
}

export function initTokensIsland() {
  hydrateIsland("tokens", TokensIsland);
}
