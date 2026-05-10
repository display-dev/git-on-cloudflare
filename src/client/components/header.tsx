import { GitBranch, LogIn, User } from "lucide-react";

import { IslandHost } from "@/client/server/IslandHost";
import { ThemeToggleIsland } from "@/client/islands/theme-toggle";
import type { Viewer } from "@/client/server/viewer";

type HeaderProps = {
  currentView?: string;
  viewer?: Viewer | null;
};

const navLinkClass = (isActive: boolean): string =>
  [
    "flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent-500/50 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas",
    isActive
      ? "bg-accent-500/10 text-accent-500 dark:text-accent-400"
      : "text-zinc-500 hover:bg-zinc-100 hover:text-zinc-800 dark:text-zinc-400 dark:hover:bg-zinc-800 dark:hover:text-zinc-200",
  ].join(" ");

const signOutButtonClass =
  "flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm text-zinc-500 hover:text-zinc-800 dark:text-zinc-500 dark:hover:text-zinc-200 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent-500/50";

function AnonymousNav({ currentView }: { currentView?: string }) {
  // Single primary call-to-action: the impeccable.md "confident restraint"
  // principle says avoid extra nav noise on the anonymous header.
  return (
    <a href="/auth" className={navLinkClass(currentView === "auth-signin")}>
      <LogIn className="h-3.5 w-3.5" aria-hidden="true" />
      Sign in
    </a>
  );
}

function SignedInNav({ currentView, viewer }: { currentView?: string; viewer: Viewer }) {
  return (
    <>
      <a href="/auth/account" className={navLinkClass(currentView === "account")}>
        <User className="h-3.5 w-3.5" aria-hidden="true" />
        {viewer.primaryNamespaceSlug ? `@${viewer.primaryNamespaceSlug}` : "Account"}
      </a>
      {/* Sign-out is a same-origin POST; the form preserves Lax cookies. */}
      <form method="post" action="/auth/sign-out" className="contents">
        <button type="submit" className={signOutButtonClass}>
          Sign out
        </button>
      </form>
    </>
  );
}

export function Header({ currentView, viewer }: HeaderProps) {
  return (
    <header className="sticky top-0 z-50 w-full border-b border-zinc-300 bg-white/80 backdrop-blur-sm dark:border-zinc-800/60 dark:bg-zinc-900/95">
      <div className="mx-auto flex w-full max-w-7xl items-center justify-between gap-4 px-4 py-3 sm:px-6">
        <nav className="flex items-center gap-1.5" aria-label="Primary">
          <a href="/" className="group flex items-center gap-3 transition-opacity hover:opacity-80">
            <span className="inline-grid h-9 w-9 place-items-center">
              <GitBranch
                className="h-6 w-6 text-accent-500 transition-transform duration-200 group-hover:-rotate-6 dark:text-accent-400"
                strokeWidth={2}
                aria-hidden="true"
              />
            </span>
            <span className="hidden sm:block">
              <strong className="block text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                git-on-cloudflare
              </strong>
              <small className="block text-xs text-zinc-400">Git hosting on Cloudflare</small>
            </span>
          </a>
          {viewer ? (
            <SignedInNav currentView={currentView} viewer={viewer} />
          ) : (
            <AnonymousNav currentView={currentView} />
          )}
        </nav>
        <IslandHost name="theme-toggle" props={{}}>
          <ThemeToggleIsland />
        </IslandHost>
      </div>
    </header>
  );
}
