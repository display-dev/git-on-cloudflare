# tessera Ownership Migration Plan

This plan modernizes git-on-cloudflare repository ownership and moves human identity to tessera OIDC while preserving the existing Git storage model: Workers remain stateless, RepoDO remains the per-repository metadata authority for refs and pack catalog state, and R2 remains the pack/object data plane.

The implementation is split into three phases so identity, route resolution, authorization, caching, and legacy removal do not all land in one deploy.

## Current Source Anchors

- Worker route order lives in `src/worker/index.ts`. `registerAuthRoutes(router)` is currently before `registerUiRoutes(router)`, and that must remain true because `/:owner` would otherwise shadow auth routes.
- Git Smart HTTP routes in `src/worker/routes/git.ts` currently derive the storage identity with `repoKey(owner, repo)` and pass that value to upload-pack, receive-pack, and `capabilityAdvertisement()`.
- UI browse routes in `src/worker/routes/ui.ts` and route handlers under `src/worker/routes/ui/` also derive `repoKey(owner, repo)` directly.
- Legacy owner listing lives in `src/worker/registry/owner.ts` over `OWNER_REGISTRY` keys shaped as `owner:<owner>:<repo>`.
- Legacy push auth lives in `src/worker/auth/verify.ts` and `src/worker/do/auth/authDO.ts`.
- Repo metadata SQLite is inside RepoDO under `src/worker/do/repo/db/`; new global ownership tables must not be added there.
- Current cache helpers in `src/worker/cache/cache.ts` read shared Workers Cache before loader execution. Private repo support must add an explicit bypass path before these helpers are called.
- Existing limiter usage is centered on `CacheContext` and `getLimiter()` in `src/worker/git/operations/limits.ts`. New outbound DO RPC and R2 calls in request-scoped code must use that limiter.

## Patterns To Reuse

- Keep a dedicated global D1 folder instead of mixing global metadata with RepoDO SQLite:
  - `src/worker/db/d1/client.ts`
  - `src/worker/db/d1/schema.ts`
  - `src/worker/db/d1/dal/*.ts`
  - `src/worker/db/d1/index.ts`
- Follow the typed Drizzle client shape used in `~/code/anvil/src/worker/db/d1/client.ts`; keep future executor/bookmark support inside the D1 client helper rather than leaking it into DAL call sites.
- Follow goc's own DAL rule by keeping all global D1 app reads and writes behind the D1 DAL. Avoid raw Drizzle queries outside `src/worker/db/d1/dal/`.
- Use the flamemail OIDC pattern for tessera relying-party code:
  - `openid-client` v6 owns discovery, authorization URL construction, code exchange, PKCE, and ID token validation.
  - OIDC transaction state is sealed in an HttpOnly cookie derived from `TESSERA_OIDC_CLIENT_SECRET`.
  - Plaintext issuers are allowed only for loopback local development.
- Use anvil and bland naming conventions for bindings: the D1 binding is `DB`. Do not introduce names such as `GOC_DB`.

## Cloudflare Constraints Checked

- D1 app code should use bindings through `env.DB`. D1 `batch()` is the grouped statement primitive and executes statements sequentially as one batch; do not plan around D1 `transaction()` in Workers app code.
- D1 enforces foreign keys by default, so migration ordering and `ON DELETE` choices matter.
- Workers KV is eventually consistent. Writes and negative lookups can remain stale in other locations for 60 seconds or more, so KV cannot be an authorization source and must not contain `visibility`.
- Workers Cache API data is local to the data center where it was written. `cache.delete()` is not a global purge, so cache deletion cannot be the correctness mechanism for public-to-private transitions.
- Queues support dashboard-sent messages, explicit `ack()` and `retry()`, and retry/DLQ configuration. Backfill messages must be idempotent.
- Durable Object delete migrations use `deleted_classes` and delete stored data for that class. Removing `AuthDurableObject` is a Phase 3 cutover step, not a Phase 1 or 2 cleanup.

Reference docs checked:

- D1 Database API: https://developers.cloudflare.com/d1/worker-api/d1-database/
- D1 foreign keys: https://developers.cloudflare.com/d1/sql-api/foreign-keys/
- Workers KV consistency: https://developers.cloudflare.com/kv/concepts/how-kv-works/
- Workers Cache API: https://developers.cloudflare.com/workers/runtime-apis/cache/
- Queues batching and retries: https://developers.cloudflare.com/queues/configuration/batching-retries/
- Queues dashboard messages: https://developers.cloudflare.com/queues/examples/send-messages-from-dash/
- Durable Object migrations: https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/

## Cloudflare Primitive Map

- Worker routes: existing Hono routes in `src/worker/routes/*`; new auth/session, route resolver, PAT, and repo-admin gates stay in Worker code.
- Durable Object classes and identity keys: `RepoDurableObject` remains addressed with `env.REPO_DO.idFromName(doName)`. Existing legacy rows keep `doName = "<owner>/<repo>"`; new rows use `doName = "repo:<uuid>"`.
- D1 database: new global `DB` binding for users, namespaces, repositories, PATs, and grants. RepoDO SQLite stays dedicated to per-repo pack metadata.
- R2 bucket: existing `REPO_BUCKET` remains the data plane. Do not introduce Worker -> DO -> R2 chains for new request flows.
- KV namespaces: keep `OWNER_REGISTRY` only for legacy listing/backfill until Phase 3. Add `ROUTES` for route read acceleration with keys `repo-route:v1:<namespace>/<repo>`.
- Queues: extend existing `REPO_MAINT_QUEUE` for legacy backfill messages. Do not create a second queue unless queue isolation becomes operationally necessary.
- External systems: tessera OIDC provider at `TESSERA_OIDC_ISSUER`, production `https://auth.limic.dev`.

## Global D1 Model

Add `drizzle-d1.config.ts`, `drizzle/d1/`, and a D1 binding in `wrangler.jsonc`:

```jsonc
"d1_databases": [
  {
    "binding": "DB",
    "database_name": "git-on-cloudflare",
    "database_id": "REPLACE_WITH_DATABASE_ID",
    "migrations_dir": "drizzle/d1"
  }
]
```

Add scripts:

- `db:gen:d1`: `drizzle-kit generate --config drizzle-d1.config.ts`
- `db:gen:repo`: existing repo-DO migration command
- `db:gen`: run both, or keep the existing command and document that D1 changes require `db:gen:d1`

Use integer millisecond timestamps via Drizzle where possible, matching tessera and anvil.

### Tables

`users`

- `id TEXT PRIMARY KEY`
- `tessera_sub TEXT UNIQUE NOT NULL`
- `created_at INTEGER NOT NULL`

`namespaces`

- `id TEXT PRIMARY KEY`
- `slug TEXT UNIQUE NOT NULL`
- `created_by TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT`
- `created_at INTEGER NOT NULL`
- There is no normal user-delete flow. If one is added later, namespace ownership must be transferred or explicitly rejected before user deletion.

`namespace_memberships`

- `namespace_id TEXT NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE`
- `user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE`
- `created_at INTEGER NOT NULL`
- Primary key: `(namespace_id, user_id)`
- Any membership means owner-level access for this migration. Roles are intentionally deferred.

`repositories`

- `id TEXT PRIMARY KEY`
- `namespace_id TEXT NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE`
- `created_by TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT`
- `slug TEXT NOT NULL`
- `do_name TEXT UNIQUE NOT NULL`
- `visibility TEXT NOT NULL`
- `created_at INTEGER NOT NULL`
- `updated_at INTEGER NOT NULL`
- Unique: `(namespace_id, slug)`
- Check: `visibility IN ('public', 'private')`

`personal_access_tokens`

- `id TEXT PRIMARY KEY`
- `user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE`
- `name TEXT NOT NULL`
- `prefix TEXT UNIQUE NOT NULL`
- `hash TEXT NOT NULL`
- `created_at INTEGER NOT NULL`
- `expires_at INTEGER`
- `revoked_at INTEGER`
- `last_used_at INTEGER`
- Plaintext token is shown once as `goc_<short-prefix>_<secret>`. Public token prefixing is allowed; internal DB and binding names stay unprefixed.
- Hash the full plaintext token, not just the secret segment. `last_used_at` updates must be `ctx.waitUntil()` best-effort and coarse-throttled.

`pat_namespace_grants`

- `pat_id TEXT NOT NULL REFERENCES personal_access_tokens(id) ON DELETE CASCADE`
- `namespace_id TEXT NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE`
- `level TEXT NOT NULL` with `CHECK (level IN ('pull','push'))`
- Primary key: `(pat_id, namespace_id)`

`pat_repo_grants`

- `pat_id TEXT NOT NULL REFERENCES personal_access_tokens(id) ON DELETE CASCADE`
- `repo_id TEXT NOT NULL REFERENCES repositories(id) ON DELETE CASCADE`
- `level TEXT NOT NULL` with `CHECK (level IN ('pull','push'))`
- Primary key: `(pat_id, repo_id)`

`level = 'push'` includes pull/fetch/read access by construction; the
absence of a grant row encodes no-access. There is no separate
`can_pull` / `can_push` pair, and no `can_admin`. Web admin uses a local
goc session plus namespace membership.

### Index Audit

The prompt's base indexes are necessary but not sufficient for all planned lookup paths. Add these indexes:

- `users(tessera_sub)` via unique constraint: OIDC callback upsert and lookup.
- `namespaces(slug)` via unique constraint: route resolution by `:owner`.
- `namespace_memberships(namespace_id, user_id)` via primary key: authorization check for a known namespace and user.
- `namespace_memberships(user_id, namespace_id)`: "my namespaces" and "my repositories" pages.
- `repositories(namespace_id, slug)` via unique constraint: route resolution by namespace and repo slug.
- `repositories(do_name)` via unique constraint: reverse lookup and safety checks.
- `repositories(namespace_id, updated_at DESC, slug)`: owner page and namespace repo listing without scanning all repos in a namespace.
- `personal_access_tokens(prefix)` via unique constraint: PAT prefix lookup before hash verification.
- `personal_access_tokens(user_id, created_at DESC)`: PAT management page.
- `pat_namespace_grants(namespace_id)`: admin grant listing and cleanup by namespace.
- `pat_repo_grants(repo_id)`: admin grant listing and cleanup by repo.

Authorization checks by PAT use the composite grant primary keys:

- `pat_namespace_grants(pat_id, namespace_id)`
- `pat_repo_grants(pat_id, repo_id)`

No separate `level` index is needed because the level value is read after the targeted grant row is found by composite primary key.

## Naming Decisions

- Use `DB` for the D1 binding.
- Use `ROUTES` for the new route KV binding.
- Keep existing `OWNER_REGISTRY` only while legacy compatibility exists.
- Use `repositoryId` for `repositories.id`.
- Use `namespaceId` for `namespaces.id`.
- Use `doName` for `env.REPO_DO.idFromName(doName)`.
- Use `routeNamespaceSlug` and `routeRepoSlug` for URL params.
- Do not pass bare `owner`, `repo`, or `repoId` across route, auth, and storage boundaries when a `RepositoryRoute` object is available.
- Keep public prefixes only where they help users/operators identify tokens or cookies: `goc_` PATs, `goc_sess_` opaque session token, `__Host-goc_session`, and `__Host-goc_oidc`.

## Phase 1: Identity, D1 Model, UI Shell, And Automatic Backfill

Goal: add tessera identity and global D1 ownership metadata without moving Git traffic through the new resolver.

### 1. Add D1, Drizzle, And DAL

Implementation tasks:

- Add `drizzle-d1.config.ts`, `src/worker/db/d1/schema.ts`, `src/worker/db/d1/client.ts`, and `src/worker/db/d1/dal/`.
- Add DAL modules:
  - `users.ts`
  - `namespaces.ts`
  - `repositories.ts`
  - `tokens.ts`
  - `route-cache.ts` if KV refresh helpers should sit next to DB mutations
- Export all D1 DAL functions from `src/worker/db/d1/index.ts`.
- Keep application D1 operations in Drizzle. Do not use `env.DB.prepare`, `env.DB.exec`, or handwritten SQL for request handling.
- Use Drizzle D1 `batch()` for grouped writes whose rows can be inserted in one transaction without conditional branching, such as PAT creation with grants (`dal/tokens.ts:insertPatWithGrants`). The first-login user/namespace/membership flow does NOT fit `batch()`: the membership insert depends on whether the namespace claim won its slug race, which is not knowable until the claim insert returns, and a FK violation inside a batch would roll back the unrelated user insert. Run those three writes sequentially instead. Do not use `transaction()` in Worker app code.
- Add named result unions for DAL operations that can conflict:
  - namespace slug taken
  - repository slug taken
  - PAT not found/revoked/expired
- Add structured logging with `createLogger` around D1 writes, route-KV writes, queue sends, and backfill operations.

Acceptance:

- `npm run db:gen:d1` produces a D1 migration under `drizzle/d1/`.
- `npm run typecheck` passes after generated Env types include `DB` and `ROUTES`.

### 2. Add OIDC Relying-Party Support

Implementation tasks:

- Add runtime dependency `openid-client`.
- Add `src/worker/auth/oidc.ts` based on the flamemail pattern:
  - normalize issuer URL
  - allow `http://` only for loopback issuers
  - cache discovery by issuer/client id for a short TTL
  - generate PKCE S256 verifier/challenge, `state`, and `nonce`
  - seal the transaction in `__Host-goc_oidc`
  - exchange callback with `openid-client.authorizationCodeGrant()`
  - read verified ID token claims once
- Add env vars:
  - `TESSERA_OIDC_ISSUER`
  - `TESSERA_OIDC_CLIENT_ID`
  - secret `TESSERA_OIDC_CLIENT_SECRET`
  - secret `SESSION_SECRET` for sealing the local goc browser session cookie
- Register goc in tessera `/admin/clients`; production issuer is `https://auth.limic.dev`.
- Add local session support:
  - `GET /auth` renders the sign-in surface for anonymous users and account links for signed-in users.
  - `GET /auth/start` starts tessera authorization code + PKCE and sets `__Host-goc_oidc`.
  - `POST /auth/sign-out` clears the local session cookie after same-origin validation.
  - `GET /auth/callback` mints a local goc session after OIDC verification.
  - Session cookie `__Host-goc_session` stores a sealed payload with user id, created time, and expiry, using `SESSION_SECRET`.
  - Stateless sealed sessions cannot be individually server-revoked without adding a revocation store; `SESSION_SECRET` rotation is the coarse invalidation mechanism.
  - Cookie mutations use `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`, and same-origin checks for sign-out.
  - Session reads fail closed if `SESSION_SECRET` is missing, cookie unsealing fails, the payload is expired, or the D1 user row is missing.
- OIDC callback user binding:
  - upsert `users(tessera_sub)` by verified `sub`
  - do not use tessera ID tokens as goc sessions
  - treat `preferred_username` as a convenience slug candidate only, not ownership proof

Route decision:

- `/auth` becomes the tessera sign-in/account surface.
- Move legacy AuthDO UI to `/auth/legacy`.
- Keep deprecated legacy JSON aliases under `/auth/api/users` during Phase 1 if needed for operator scripts, but implement them as wrappers to `/auth/legacy/api/users` and mark them for Phase 3 removal.
- Keep `registerAuthRoutes(router)` before `registerUiRoutes(router)`.

### 3. Add Slug Policy

Implementation tasks:

- Add a shared slug validator, for example `src/shared/slugs.ts`.
- Use it for namespace slugs and repository slugs.
- Reserve slugs that conflict with Worker-owned routes or common static paths:
  - `auth`
  - `sign-in`
  - `sign-out`
  - `api`
  - `assets`
  - `_cache`
  - `favicon.ico`
  - `robots.txt`
- Lowercase route slugs and reject ambiguous Unicode. Keep the policy ASCII unless the product intentionally supports more later.

Acceptance:

- `preferred_username` creates a namespace only when it passes the same namespace slug policy and is unused.
- Invalid or taken `preferred_username` still creates the user and session, but no namespace is imported.

### 4. Add Signed-In UI Shell

Implementation tasks:

- Extend SSR shell data so `AppLayout` and `Header` can render anonymous vs signed-in states.
- Header behavior:
  - anonymous users see `Sign in`
  - signed-in users see links to namespaces and PATs, plus sign-out
  - legacy AuthDO is not linked from primary nav
- Add pages through the existing view registry:
  - `namespaces` or `account`
  - `tokens`
  - `repository settings` shell for `/:owner/:repo/admin`
- Add client entrypoints only for interactive PAT creation/revocation and visibility toggles.
- Keep UI rendering through `renderUiView()` and `src/client/server/registry.tsx`.

Phase 1 UI reads D1 for signed-in pages only. Existing owner/repo public UI routes continue to use legacy routing and `repoKey(owner, repo)`.

### 5. Add PAT Management But Do Not Enforce It Yet

Implementation tasks:

- Add PAT create/list/revoke endpoints under session auth.
- PAT creation returns plaintext once:
  - example: `goc_abcd1234_<random-secret>`
  - DB `prefix`: `goc_abcd1234`
  - DB `hash`: hash of full plaintext token
- Create-token request body is `{ scope, name, namespaceSlug, [repoSlug,] level }` where `level` is `'pull'` or `'push'`. Missing or invalid `level` returns 400.
- Store namespace and repo grants in D1 with the supplied `level`.
- Do not add a PAT admin permission.
- Do not wire Git receive-pack or upload-pack to PAT authorization until Phase 2.

### 6. Add Automatic Legacy Backfill

Queue payload:

```ts
type LegacyBackfillMessage = {
  kind: "legacy-backfill";
  userId: string;
  namespaceSlug: string;
  cursor?: string;
};
```

Implementation tasks:

- Extend `RepoMaintenanceQueueMessage` in `src/worker/maintenance/queue.ts`.
- Add `src/worker/maintenance/legacyBackfill.ts`.
- On first login for a new tessera `sub`:
  - if `preferred_username` is valid and unused, create user, namespace, membership, and enqueue a backfill message
  - otherwise create only the user and sealed session cookie
- Queue consumer behavior:
  - list `OWNER_REGISTRY` with prefix `owner:<namespaceSlug>:`
  - for each repo key suffix, upsert:
    - namespace slug = `namespaceSlug`
    - namespace membership = `userId`
    - repository slug = key suffix
    - `doName = "${namespaceSlug}/${repoSlug}"`
    - `visibility = "public"` because legacy reads are public
  - write `ROUTES.put("repo-route:v1:<namespace>/<repo>", { repositoryId, namespaceId, doName, updatedAt })`
  - do not include `visibility` in KV
  - if list is incomplete, re-enqueue with the cursor
  - acknowledge each message only after its D1/KV work is complete or intentionally retried
- Duplicate messages must be idempotent through unique constraints and upserts.
- Queue recovery is an operator workflow through the Cloudflare dashboard by sending the same JSON payload. Do not expose a product UI button for backfill.

KV failure policy:

- D1 is the source of truth. If a route-KV write fails after a successful D1 upsert, log `backfill:route-cache-put-failed` with namespace, repo, and repository id, then retry the queue message unless the failure is clearly permanent.
- KV must never contain `visibility`.

### Phase 1 Tests

- D1 migration generation and fresh apply.
- OIDC start/callback with a mocked provider or fetch boundary mock following tessera's `scripts/test-client/run.ts`.
- Sealed session cookie creation, lookup, tamper rejection, `SESSION_SECRET` failure, and expiry.
- User upsert by `tessera_sub`.
- Valid `preferred_username` creates namespace/membership and enqueues backfill.
- Invalid or taken `preferred_username` creates only the user and sealed session cookie.
- Backfill duplicate messages are idempotent.
- Backfill cursor continuation re-enqueues and resumes.
- Route KV payload omits `visibility`.
- PAT create/list/revoke and prefix-then-hash verification, without Git enforcement yet. Cover both `level = 'pull'` and `level = 'push'` round-trips, plus 400 responses for missing or invalid `level`.

## Phase 2: Resolver, ACLs, PATs, Admin, And Cache Policy

Goal: all repo-serving route handlers use a resolver and D1 authorization, while legacy fallback remains available during compatibility.

### 1. Introduce Repository Route Context

Add a named route context, for example:

```ts
type RepositoryRoute = {
  routeNamespaceSlug: string;
  routeRepoSlug: string;
  namespaceId: string;
  repositoryId: string;
  doName: string;
  visibility: "public" | "private";
  source: "kv" | "d1" | "legacy";
};
```

Implementation tasks:

- Add resolver code under `src/worker/repositories/resolve.ts` or `src/worker/routes/repositories/resolve.ts`.
- Route handlers pass this context, not separate owner/repo strings, across auth and cache decisions.
- `env.REPO_DO.idFromName()` must receive `route.doName`.
- `repoKey(owner, repo)` must disappear from route handlers for resolved D1 routes.
- Keep lower-level Git operation parameters as-is if renaming them would make Phase 2 too broad, but boundary comments must say the value is the RepoDO `doName`.

Resolver behavior in Phase 2 compatibility:

1. KV hit: treat as a candidate only, then read D1 by ids/slugs to verify existence and current visibility.
2. KV miss: read D1 by namespace slug and repo slug.
3. D1 miss: legacy fallback to old `owner/repo` behavior when compatibility is enabled.

Scanner-shield checkpoint:

- Early Phase 2 keeps D1 fallback on anonymous KV miss to reveal backfill misses.
- After backfill metrics are clean, enable anonymous KV-miss 404 for public routes.
- Phase 3 makes anonymous KV-miss 404 mandatory.
- Authenticated session routes and PAT routes always retain D1 fallback on KV miss because private repos may not be discoverable through a public route index.

### 2. Update Git Smart HTTP Routes

Route families:

- `GET /:owner/:repo/info/refs`
- `POST /:owner/:repo/git-upload-pack`
- `POST /:owner/:repo/git-receive-pack`

Implementation tasks:

- Resolve route context before capability advertisement or pack handling.
- For `git-upload-pack` and `info/refs?service=git-upload-pack`:
  - public repo: anonymous allowed
  - private repo: require signed-in session membership or any PAT grant (`level = 'pull'` or `level = 'push'`)
- For `git-receive-pack`:
  - require PAT with `level = 'push'`
  - Phase 2 compatibility may fall back to AuthDO owner token for legacy/imported public repos only
  - do not allow AuthDO fallback to bypass private repo policy
- Git Basic username must equal `route.routeNamespaceSlug`.
- Git Basic password is the PAT plaintext.
- Validate route owner from resolved context before PAT verification.
- Keep `Retry-After: 10` behavior for receive lease conflicts.

Limiter/logging:

- Any new outbound DO RPC in route handlers must use `getLimiter(cacheCtx).run("do:<operation>", fn)`.
- Existing receive path already has a receive-scoped limiter; keep new resolver/auth D1 work outside DO/R2 limiter because D1 is not covered by the goc limiter rule, but log it.

### 3. Update UI Browse And API Routes

Route families:

- `/:owner`
- `/:owner/:repo`
- `/:owner/:repo/tree`
- `/:owner/:repo/blob`
- `/:owner/:repo/raw`
- `/:owner/:repo/rawpath`
- `/:owner/:repo/commits`
- `/:owner/:repo/commits/fragments/:oid`
- `/:owner/:repo/commit/:oid`
- `/:owner/:repo/commit/:oid/diff`
- `/:owner/:repo/api/refs`
- `/:owner/:repo/admin`

Implementation tasks:

- Owner page:
  - anonymous users see public D1 repos plus Phase 2 legacy-compatible public entries
  - namespace members see public and private D1 repos
  - private repos are not disclosed to anonymous users
- Repo data pages:
  - resolve route context first
  - authorize before any shared cache lookup
  - return 404 for anonymous private repo requests to avoid existence disclosure
  - return 403 only when the user is authenticated but lacks membership/grants
- Admin page:
  - require local goc session
  - require namespace membership
  - no PAT admin access
  - no Basic/AuthDO admin access for the new admin page
- Account page repo-create UI:
  - on `/auth/account`, the existing Repositories section gains a "+ New repository" affordance that reveals an inline form with a `slug` input, a private/public toggle (default `private`), and a Create button
  - on success, the new repo appears in the table and the form collapses
  - the Repositories `EmptyState` gains the same affordance so first-time signed-in users have a path off the empty state without legacy push
  - this is the surface that closes the catch-22 documented in `### 6` (a token can only grant against an existing repo, but until this UI lands there is no way for a signed-in user to create the first repo once legacy backfill is removed in Phase 3)
- JSON admin routes in `src/worker/routes/admin.ts` must move to session membership authorization. If a legacy Basic fallback is retained for one compatibility release, scope it to explicitly named legacy endpoints and test it.

### 4. Implement PAT Authorization

Implementation tasks:

- Add `src/worker/auth/pat.ts` with named result unions:
  - malformed
  - username mismatch
  - token not found
  - token revoked
  - token expired
  - grant missing
  - ok
- Parse public prefix from the plaintext token before hashing.
- Hash the full plaintext token and compare to the stored hash with constant-time comparison.
- Check grants in this order:
  - repo grant for exact `repositoryId`
  - namespace grant for `namespaceId`
- Any present grant (`level = 'pull'` or `level = 'push'`) authorizes fetch/clone and private UI read APIs when PAT credentials are used; `level = 'push'` includes pull access by construction.
- `level = 'push'` authorizes receive-pack.
- PATs never authorize web admin operations.
- Update `last_used_at` with `ctx.waitUntil()` only when the previous value is older than a coarse threshold, for example 15 minutes. Do not write D1 on every Git request.

### 5. Define Cache Policy

Rules:

- D1 is the source of truth for visibility and authorization.
- `ROUTES` is only a route candidate cache.
- Shared Workers Cache may be read or written only after D1 confirms the repo currently exists and is public.
- Private repo responses must set `Cache-Control: no-store`.
- Private repo handlers must bypass shared JSON and object cache reads and writes.
- Public-to-private correctness must not depend on KV deletion or `cache.delete()`.

Implementation tasks:

- Add a small cache policy type, for example:

```ts
type SharedCachePolicy = "allow-shared-cache" | "bypass-shared-cache";
```

- Extend `CacheContext.memo.flags` or cache helper signatures so private paths bypass:
  - refs cache
  - README cache
  - tree/blob metadata cache
  - commits/diff cache
  - object cache
- Existing `readLooseObjectRaw()` already honors `no-cache-read`; extend the pattern so object cache writes are also skipped for private routes.
- Update every route family listed above to resolve and authorize before calling cache helpers.
- On public responses, use existing TTLs after D1 visibility check.
- On private responses, do not call `cacheOrLoadJSON`, `cacheOrLoadJSONWithTTL`, or `cacheOrLoadObject` unless they receive an explicit bypass mode.

Cache key decision:

- Use `doName` as the storage cache scope for object reads because it is the RepoDO/R2 storage identity.
- Never use D1 `repositoryId` as a substitute for `doName`.
- The D1 visibility gate must happen before cache key construction and cache lookup.

### 6. Define Mutation And KV Failure Policy

Repo create — endpoint and UI:

The mechanics below need a session-authorized surface. Add it as part of Phase 2 alongside the resolver and ACLs work — without it, the catch-22 of "PAT requires existing repo, push requires PAT, legacy backfill is being removed" has no resolution.

- **Endpoint**: `POST /auth/api/repositories`
- **Auth**: `loadViewer`-required session; same-origin check via `sameOriginViolation`. PATs cannot create repositories — repo creation stays a session-only mutation so slug typos in the token form cannot create ghost repos.
- **Body** (required fields, no server-side defaults):

  ```ts
  {
    namespaceSlug: string;
    slug: string;
    visibility: "public" | "private";
  }
  ```

- **Validation**:
  - `namespaceSlug` and `slug` pass `validateSlugForRoute` (existing helper already used by the tokens route)
  - Namespace exists (`findNamespaceBySlug`)
  - Viewer is a member (`viewerIsNamespaceMember`)
  - `(namespaceId, slug)` is not already taken (relies on `uq_repositories_namespace_slug` unique index)
- **Mechanics** — the existing numbered list below applies, with these specifics:
  1. `id = newPrefixedId("repo")`, `doName = "repo:<id-suffix>"`
  2. D1 insert via existing `insertRepositoryIfNew` (`src/worker/db/d1/dal/repositories.ts`); a returned `undefined` means the slug was taken between validation and insert (race) — return `{ ok: false, reason: "slug-taken" }`.
  3. `ROUTES.put("repo-route:v1:<namespace>/<slug>", record)` is best-effort; failure logs `repo-create:route-cache-put-failed` but the endpoint still returns 200. Anonymous KV-miss serving may briefly 404 a freshly-created public repo until KV converges; authenticated/PAT paths resolve through D1 fallback immediately (Phase 2 §1).
- **Response** — tagged union per the project convention:

  ```ts
  | { ok: true; id: string; namespaceSlug: string; slug: string; visibility: "public" | "private" }
  | { ok: false; reason: "invalid-slug" | "namespace-not-found" | "not-member" | "slug-taken" }
  ```

- **Logging**: `repo-create:*` structured events with `userId`, `namespaceId`, `slug`, `repositoryId`.
- **Visibility default**: the UI defaults the toggle to `"private"`. There is no schema default — `repositories.visibility` is `notNull` with no `.default()` at `src/worker/db/d1/schema/repositories.ts`, so the wire field is required. Legacy backfill continues to import as `"public"` because legacy reads were public (rule unchanged from the legacy backfill section).

Repo create:

1. Session membership authorizes namespace mutation.
2. D1 creates repository row with new `doName = "repo:<uuid>"`.
3. `ROUTES.put()` writes the route candidate.
4. KV put failure logs a warning and returns success with an admin-visible stale-route warning; a repair action can refresh KV from D1.

Repo rename:

1. D1 updates `repositories.slug` and `updated_at`.
2. Delete old route KV key and put new route KV key.
3. If either KV operation fails, log and expose repair state. Correctness still comes from D1.

Repo visibility change:

1. D1 updates visibility first.
2. For `public -> private`, delete route KV as a best-effort privacy optimization.
3. Do not rely on KV deletion or cache deletion for correctness.
4. All serving paths must read D1 visibility before any shared cache read.

Repo delete:

1. D1 deletes the repository row and cascades grants. The schema does not add a tombstone column in this migration.
2. Delete route KV as best effort.
3. Existing RepoDO/R2 physical purge remains a separate operator/admin operation unless a new cleanup workflow is designed. Do not add a Worker -> DO -> R2 chain for route deletion.

### 7. Phase 2 Tests

- Resolver:
  - KV hit then D1 verification
  - KV miss then D1 fallback
  - D1 miss then legacy fallback in compatibility mode
  - anonymous KV miss 404 after scanner-shield flag/checkpoint
  - UUID-style `doName` reaches the correct RepoDO
- Git ACL:
  - public fetch anonymous succeeds
  - private fetch anonymous returns non-disclosing 404
  - private fetch with PAT `level = 'pull'` succeeds
  - private fetch with PAT `level = 'push'` succeeds (push includes pull)
  - push with PAT `level = 'push'` succeeds
  - push with PAT `level = 'pull'` fails
  - Basic username mismatch fails closed
  - AuthDO fallback works only in the scoped compatibility cases
- UI ACL:
  - anonymous private overview/tree/blob/raw/commits/refs API do not disclose existence
  - namespace member can browse private repo
  - `/:owner/:repo/admin` requires session membership
  - PAT-only admin access fails
- Cache:
  - public route uses shared cache only after D1 public check
  - private route bypasses cache read/write
  - public-to-private does not serve stale public cache even when KV/cache deletion is delayed
- KV:
  - route cache writes omit `visibility`
  - create/rename/delete update or delete KV keys
  - KV failure logs and leaves D1 as source of truth
- Repo create endpoint (`POST /auth/api/repositories`):
  - signed-in member POST returns `{ ok: true, ... }` and the repo appears in `listRepositoriesForUser`
  - non-member POST returns `{ ok: false, reason: "not-member" }`
  - unknown namespace returns `{ ok: false, reason: "namespace-not-found" }`
  - duplicate slug returns `{ ok: false, reason: "slug-taken" }`
  - invalid slug (per `validateSlugForRoute`) returns `{ ok: false, reason: "invalid-slug" }`
  - `ROUTES.put()` failure is logged but the endpoint still returns 200 with the new row
  - newly-created repo immediately resolves through `resolveRepositoryRoute` via the D1 path
  - PAT-authenticated POST is rejected (session-only mutation)

## Phase 3: Cutover And Legacy Removal

Goal: require D1 ownership metadata for all repo routes and remove AuthDO, legacy owner registry, legacy route fallback, and automatic backfill.

### 1. Cutover Preconditions

- Backfill metrics show no remaining legacy repos outside explicit operator exceptions.
- Route resolver logs show D1 rows for normal traffic.
- Anonymous KV-miss scanner shield has already been enabled and monitored in Phase 2.
- PAT rollout has replaced legacy owner push tokens for active push users.
- Admin UI uses tessera sessions for repository admin tasks.

Rollback posture:

- Before the Phase 3 deploy, normal code rollback is acceptable.
- After the `deleted_classes` migration for `AuthDurableObject`, AuthDO data is intentionally gone. Treat failures as forward-fix unless an operator has a separate backup.
- Do not invent a strict public/private rollback policy. The practical safety boundary is D1 visibility checks before data serving.

### 2. Remove Legacy AuthDO

Implementation tasks:

- Delete `src/worker/do/auth/`.
- Remove `AuthDurableObject` export from `src/worker/index.ts`.
- Remove `AUTH_DO` binding from `wrangler.jsonc`.
- Add a new ordered Durable Object migration:

```jsonc
{
  "tag": "v3",
  "deleted_classes": ["AuthDurableObject"],
}
```

- Remove `AUTH_ADMIN_TOKEN` usage and tests.
- Remove legacy `/auth/legacy` and `/auth/api/users` compatibility routes.
- Remove `src/worker/auth/verify.ts` or reduce `src/worker/auth/` to session/PAT/OIDC modules only.

### 3. Require D1 Rows For All Repo Routes

Implementation tasks:

- Remove resolver legacy fallback.
- Remove all route-handler calls to `repoKey(owner, repo)`.
- All Git and UI repo routes require a `RepositoryRoute`.
- Existing legacy repositories still work because their D1 row stores `doName = "<legacy-owner>/<legacy-repo>"`.
- New repositories continue to use `doName = "repo:<uuid>"`.
- Public anonymous KV miss returns 404 without D1 fallback.
- Authenticated session and PAT routes may query D1 directly on KV miss.

### 4. Replace Owner Registry Listing

Implementation tasks:

- Replace `listReposForOwner()` call sites with D1 DAL queries.
- Remove `src/worker/registry/owner.ts` if no longer used.
- Remove `OWNER_REGISTRY` binding from `wrangler.jsonc`.
- Keep `ROUTES` for route acceleration only.
- If a public repo listing cache is useful, derive it from D1 into KV with non-sensitive payloads. Do not store `visibility`.

### 5. Remove Backfill And Compatibility Code

Implementation tasks:

- Remove `legacy-backfill` from `RepoMaintenanceQueueMessage`.
- Delete backfill handler/tests.
- Remove manual dashboard backfill instructions from operator docs after cutover.
- Remove AuthDO fallback from PAT/auth logic.
- Remove compatibility feature flags added for Phase 2 scanner-shield rollout once the strict behavior is default.

### 6. Normalize Naming At The Boundary

Implementation tasks:

- Route handlers must use `RepositoryRoute`.
- Storage access helpers should take `doName` when they call `env.REPO_DO.idFromName()`.
- Comments and docs must stop describing `repoId` as `owner/repo`.
- If deeper Git read helpers still use a `repoId` parameter, add or update comments that it is the RepoDO storage identity, not the D1 repository id. Prefer renaming touched call sites to `doName` when local churn is manageable.

### 7. Phase 3 Tests

- No route falls back to `owner/repo` when a D1 row is missing.
- Legacy repositories with D1 rows and `doName = "<owner>/<repo>"` still fetch/push with D1 PATs.
- New repositories with UUID-style `doName` fetch/push and browse correctly.
- Anonymous KV miss returns 404.
- Authenticated/PAT KV miss queries D1.
- AuthDO routes, bindings, exports, and tests are absent.
- `OWNER_REGISTRY` is absent from generated Env types.
- Backfill queue messages are rejected or ignored as unknown after removal.

## Validation Commands

Run the smallest useful set per phase:

- Phase 1 D1/auth work:
  - `npm run db:gen:d1`
  - `npm run typecheck`
  - targeted worker tests for OIDC/session/PAT/backfill
- Phase 2 resolver/auth/cache work:
  - `npm run typecheck`
  - `npm run test:workers -- test/<new-resolver-tests>.worker.test.ts`
  - existing Git worker tests touched by route changes
- Phase 3 removal:
  - `npm run typecheck`
  - `npm run test:workers`
  - `npm run format:check`

Do not run the 42 MiB pack-indexer fixture unless intentionally validating pack-indexer behavior.

## Final Consistency Review

- Internal binding names are unprefixed: `DB`, `ROUTES`.
- Public token/cookie prefixes remain explicit: `goc_`, `goc_sess_`, `__Host-goc_session`, `__Host-goc_oidc`.
- KV never stores `visibility`.
- D1 is the source of truth for repository existence, visibility, memberships, PATs, and grants.
- Public and private serving paths both read D1 before shared Workers Cache access.
- Phase 2 compatibility and Phase 3 scanner-shield behavior are not contradictory: Phase 2 starts with D1 fallback for anonymous KV misses, then enables anonymous KV-miss 404 after backfill is proven; Phase 3 makes that strict behavior mandatory.
- No new plan step requires Worker -> DO -> R2 for route deletion or authorization.
- RepoDO remains the only transactional authority for per-repo refs and pack catalog state.
- The implementation can be assigned phase-by-phase without requiring identity, resolver, admin, cache, and legacy removal to land in one PR.
