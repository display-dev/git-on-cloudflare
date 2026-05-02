// Shared stable environment variables for Vitest Workers runs
// These override Wrangler vars via poolOptions.workers.miniflare.bindings
// Miniflare values take precedence over wrangler.jsonc.

export const BASE_TEST_BINDINGS = {
  // Repo DO idle cleanup threshold (minutes)
  REPO_DO_IDLE_MINUTES: "30",

  // Logging level lowered for tests to reduce noise
  LOG_LEVEL: "warn",

  // Hermetic browser session sealing secret for worker tests.
  SESSION_SECRET: "test-goc-session-secret",

  // Hermetic OIDC config. Tests that need a working provider preload one via
  // `__test.setProviderForTesting`; tests that exercise discovery failure
  // rely on this issuer being unreachable. A loopback URL with port 65535
  // keeps the `loadOidcConfig` policy happy (loopback HTTP is allowed)
  // without ever reaching whatever tessera dev instance the developer might
  // have running locally.
  TESSERA_OIDC_ISSUER: "http://127.0.0.1:65535",
  TESSERA_OIDC_CLIENT_ID: "test-goc",
  TESSERA_OIDC_CLIENT_SECRET: "test-goc-secret",
} as const;
