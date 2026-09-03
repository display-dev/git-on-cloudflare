# Architecture Overview

This project implements a Git Smart HTTP v2 server on Cloudflare Workers using a hybrid of Durable Objects (DO) and R2.

## Module Structure

The codebase is organized into focused modules with `index.ts` export files:

- **`/git`** - Core Git functionality
  - `operations/` - Git operations (upload-pack, receive-pack)
  - `core/` - Protocol handling, pkt-line, readers
  - `pack/` - Pack assembly, indexing
- **`/do`** - Durable Objects
  - `repo/repoDO.ts` - Repository Durable Object (per-repo authority)
- **`/auth`** - Authentication module
  - Tessera OIDC, sealed browser sessions, and Git PAT verification
- **`/cache`** - Two-tier caching system
  - UI layer caching (JSON responses)
  - Git object caching (immutable objects)
- **`/web`** - Web UI utilities
  - `format.ts` - Content formatting helpers
  - `render.ts` - Page rendering
  - `templates.ts` - React view compatibility shim
- **`/ui`** - React SSR UI layer
  - `server/` - Document shell, manifest resolution, render registry
  - `pages/` - Route-level React page components
  - `components/` - Shared server-rendered UI building blocks
  - `islands/` - Small client-side interactive modules
  - `client/entry.tsx` - Browser entry for CSS and islands
- **`/common`** - Shared utilities
  - `compression.ts`, `hex.ts`, `logger.ts`, `response.ts`, `stub.ts`, `progress.ts`
- **`/routes`** - HTTP route handlers
  - `git.ts` - Git protocol endpoints (upload-pack, receive-pack)
  - `ui.ts` - Web UI routes for browsing repos
  - `auth.ts` - Authentication UI and API endpoints
  - `admin.ts` - Repository admin routes

## Core Components

### Worker Entry (`src/index.ts`)

- Routes for Git endpoints, admin JSON, and the web UI
- Integrates all route handlers via AutoRouter

### Repository DO (`src/do/repo/repoDO.ts`)

- Metadata authority for a single repo. The data plane lives in R2 packs.
- Typed RPC methods (selected):
  - `listRefs()`, `setRefs()`, `getHead()`, `setHead()`, `getHeadAndRefs()`
  - `beginReceive()`, `finalizeReceive()`, `abortReceive()` — receive lease lifecycle
  - `beginCompaction()`, `commitCompaction()` — queue-driven pack compaction
  - `getActivePackCatalog()` — pack catalog snapshot for worker-local reads
- Native processing pins the returned `packsetVersion` with one renewable
  Container reader lease. Superseded-pack cleanup retries while an older
  generation is pinned.
- The qualification execution-isolation candidate keeps foreground native
  processing on RepoDO's Container and GC indexing on `MaintenanceContainerHost`.
  This second DO owns compute lifecycle only. RepoDO issues and revokes exact
  job-scoped R2 bridge grants and remains the sole operation/ref/catalog authority.
  Cancelling maintenance does not cancel foreground processing or its reader
  lease; repository deletion stops both hosts and retains ordinary drain fences.
  See [qualification](qualification.md#foreground-coordination-during-gc) for
  the controls and the still-required live availability proof.
- Smart HTTP fetches hold a bounded renewable repository read lease for the
  lifetime of the response stream, so a long clone cannot lose a snapshotted
  pack to delayed cleanup.
- Successful compaction/GC leaves a durable publication marker in RepoDO until
  the Worker uploads the immutable R2 generation manifest and conditionally
  advances `generation-index.json`. The marker stores only the ordered pack
  keys, and upgraded repositories bootstrap their first marker from the current
  catalog before any old superseded rows are removed.
- Push: bounded ordinary native receives use Worker-owned selective hydration.
  The Worker reads authenticated `.idx` and reference sidecars, materializes only
  the exact semantic prerequisite ranges and required encoding bases, and sends
  the incoming request plus that prerequisite pack to the foreground native
  processor. Independent ranges use fixed concurrency four; shared bases are
  single-flight and the retained plan remains deterministic and base-first.
  Larger or length-unknown requests retain the generic native/streaming path.
  Artifact-producing receives write immutable output to R2, then RepoDO commits
  refs and pack-catalog metadata atomically through typed RPCs. Native Git may
  also return an explicit ref-only result after hook and closure validation when
  every target object is already authoritative. RepoDO then commits the exact-old
  ref transaction, accepted-write fact and receipt without inserting a catalog
  row or fabricating an empty artifact triple. One active receive lease at a
  time; concurrent pushes receive `503 Retry-After: 10`.
- `StockReceiveContainerHost` rechecks the low-level Container process state
  during port readiness, not only before it. A process that exits before any
  receive bytes are forwarded is restarted within the existing bounded
  readiness window. Readiness and forwarding failures remain distinct bounded
  evidence codes; RepoDO still records the one authoritative terminal outcome.
  The same repository-bound host configures Cloudflare's low-level inactivity
  timeout to retain a running process for 120 seconds by default, bounded to
  5–900 seconds through `STOCK_RECEIVE_CONTAINER_IDLE_SECONDS`. The timeout
  applies only after inactivity; it does not stop an in-flight receive. Container
  disk remains disposable and no state on it becomes authoritative.
  Stock-receive operation evidence records one non-overcounted Worker data-plane
  duration plus bounded phase timings for selective planning/R2 reads, Container
  readiness and process state, native execution, output upload/verification,
  and proof validation. Actual route-start, processor-start, prepared, and
  acknowledgement boundaries carry timestamps; replayed native trace and
  publication events remain ordinal rather than receiving post-hoc timestamps.
  These fields are diagnostic only and do not participate in validation or
  publication authority.
- Pack metadata lives in `pack_catalog` (SQLite). Exact pack membership lives in `.idx` files in R2.

### Ownership And Auth

- D1 stores users, namespaces, memberships, repositories, PATs, and grants.
- Tessera OIDC signs users into sealed local browser sessions.
- Git push uses HTTP Basic where the username matches the namespace slug and the password is a PAT with push access.

### Caching Layer (`src/cache/`)

- **UI Cache**: 60s for HEAD/refs, 5min for README, 1hr for tag commits
- **Object Cache**: Immutable Git objects cached for 1 year
- **Pack discovery and memoization**: `src/git/object-store/catalog.ts#loadActivePackCatalog()` loads the active pack catalog through the Repo DO once per request and memoizes the snapshot in `RequestMemo`.
- **Per-request limiter and soft budget**: All DO/R2 calls in read and upload paths use a concurrency limiter and a soft subrequest budget to avoid hitting platform limits.

### Durable Objects SQLite (drizzle-orm)

- The Repository DO maintains a small SQLite database using `drizzle-orm/durable-sqlite` for metadata that benefits from indexed lookups and batch queries.
- Migrations run during DO initialization via `migrate(db, migrations)` and Wrangler `new_sqlite_classes` (see `wrangler.jsonc` and `drizzle.config.ts`).
- Tables:
  - `pack_catalog(pack_key, ...)` — authoritative pack metadata: key, state, tier, sequence range, object count, byte sizes, creation/supersession timestamps. Drives both read-path discovery and compaction planning.
- Access policy: all SQLite operations must go through the DAL (`src/do/repo/db/dal.ts`). Avoid raw drizzle queries outside the DAL.
- Repository listing and authorization come from D1. `ROUTES` KV is only a non-sensitive route candidate cache.

### Static assets and UI rendering (env.ASSETS + React SSR)

- React page components are rendered on the Worker through `renderToReadableStream()` in `src/client/server/render.tsx`.
- Route handlers call `renderUiView()` and the view registry in `src/client/server/registry.tsx` so SSR pages and fragments share one rendering path.
- Client assets are split across `src/client/entries/*.ts`, with `src/client/entries/styles.ts` loading shared UI CSS and route-specific entrypoints mounting only the islands each page needs.
- Production assets are built by Vite and served through the `ASSETS` binding using the generated manifest (`dist/client/manifest.json`).
- Development runs through the Cloudflare Vite plugin so Worker code, TSX, and CSS all participate in the same hot-reload pipeline.
- Assets config uses `html_handling: "none"` so the Worker controls routes like `/auth` without the assets layer intercepting them.

## Background processing and alarms

- The repo DO `alarm()` handles:
  - Lightweight lease cleanup (expired receive/compaction leases)
  - Compaction queue re-arm when `compactionWantedAt` is set
  - Idle cleanup (purge empty repos after idle timeout)
- Helpers:
  - `rearmCompactionQueueFromAlarm()` - Triggers compaction when requested
  - `handleIdleAndMaintenance()` - Manages idle cleanup and alarm scheduling
  - `shouldCleanupIdle()` - Determines if cleanup is needed
  - `performIdleCleanup()` - Executes cleanup
  - `purgeR2Mirror()` - Handles R2 cleanup

## Logging

- Structured JSON logs are emitted with a minimal logger. Set `LOG_LEVEL` to `debug|info|warn|error` to control verbosity.

See also:

- [Storage model](./storage.md)
- [Data flows](./data-flows.md)
- Top-level `README.md` for development and testing commands.
