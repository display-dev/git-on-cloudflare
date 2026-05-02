import { ShieldCheck } from "lucide-react";

import { Button, Card, ErrorBanner, PageHeader } from "@/client/components/ui";

export type AuthSignInPageProps = {
  errorCode?: string;
};

const ERROR_MESSAGES: Record<string, string> = {
  invalid_request: "The sign-in callback was missing required parameters. Start a fresh sign-in.",
  missing_state:
    "The sign-in transaction expired before tessera redirected back. Start a fresh sign-in.",
  invalid_state:
    "The tab where you started signing in was closed before tessera redirected back. Start a fresh sign-in.",
  token_exchange_failed:
    "tessera could not be reached during the token exchange. Try again, then check your network if it persists.",
  invalid_id_token: "tessera returned an ID token that did not validate. Contact your operator.",
  oidc_unavailable:
    "tessera sign-in is not configured for this deployment. Use the legacy admin link below if you have an admin token.",
  session_create_failed:
    "Could not create your session after sign-in. Try again, then check the worker logs if it persists.",
};

function describeError(code: string | undefined): string | null {
  if (!code) return null;
  return ERROR_MESSAGES[code] ?? "Sign-in failed. Start a fresh sign-in.";
}

export function AuthSignInPage({ errorCode }: AuthSignInPageProps) {
  const errorText = describeError(errorCode);
  return (
    <div className="mx-auto max-w-md py-10">
      <PageHeader>
        <div>
          <h1 className="font-display text-3xl font-semibold text-zinc-900 dark:text-zinc-100">
            Sign in
          </h1>
        </div>
      </PageHeader>
      {errorText ? <ErrorBanner>{errorText}</ErrorBanner> : null}
      <Card>
        <div className="flex flex-col items-stretch gap-5">
          <pre className="m-0 overflow-x-auto rounded-lg border border-zinc-200 bg-zinc-50/60 p-3 font-mono text-xs leading-relaxed text-zinc-500 dark:border-zinc-800/60 dark:bg-zinc-950/40">
            <code className="bg-transparent p-0 font-mono text-xs">
              <span className="select-none">$ </span>tessera authorize{"\n"}
              <span className="select-none">→ </span>verify id_token{"\n"}
              <span className="select-none">→ </span>claim @namespace
            </code>
          </pre>
          <Button href="/auth/start" variant="primary">
            <ShieldCheck className="h-4 w-4" aria-hidden="true" />
            Continue with tessera
          </Button>
        </div>
      </Card>
      <p className="m-0 mt-4 text-center text-xs text-zinc-500">
        Already have a legacy admin token?{" "}
        <a
          href="/auth/legacy"
          className="text-accent-600 underline decoration-zinc-300 underline-offset-2 hover:text-accent-500 dark:text-accent-400 dark:decoration-zinc-700"
        >
          Open the legacy admin
        </a>
        .
      </p>
    </div>
  );
}
