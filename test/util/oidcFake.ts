import * as oidc from "openid-client";

import type { OidcProvider } from "@/worker/auth/oidc";

// Minimal fake provider: openid-client's `Configuration` consults the
// metadata it was built with. We construct a Configuration from a hand-
// rolled metadata object so unit tests do not need a real discovery
// document or live network. This keeps the worker-test seam aligned with
// production: production uses the real `oidc.discovery`; tests preload an
// equivalent shape directly into the cache.
export function fakeProvider(args: {
  authorizationEndpoint: string;
  tokenEndpoint: string;
  jwksUri: string;
  issuer?: string;
}): OidcProvider {
  const issuer = args.issuer ?? "https://fake.local";
  const configuration = new oidc.Configuration(
    {
      issuer,
      authorization_endpoint: args.authorizationEndpoint,
      token_endpoint: args.tokenEndpoint,
      jwks_uri: args.jwksUri,
      response_types_supported: ["code"],
    },
    "test-client-id",
    {
      client_id: "test-client-id",
      client_secret: "test-secret",
    },
    oidc.ClientSecretPost("test-secret")
  );
  return { configuration };
}
