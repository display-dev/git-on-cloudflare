# Qualification deployment and operation

The repository-service qualification surface is an internal, synthetic-only
test seam. It is not part of the product API and must never share a route,
credential, resource, or data set with production.

## Frozen candidate composition

Schema v1 qualifies this repository's normal Worker entrypoint, repository
route lookup, `RepoDurableObject`, the zero-authority
`StockReceiveContainerHost`, native receive Container image, R2 object
store, D1 route authority, ROUTES KV candidate cache, and repository task
Queue. Git push, clone, fetch, operation receipts, pack publication, and
reachability cleanup use the same modules as ordinary service traffic. The
qualification routes add observation and exact reset operands; they do not add
a benchmark Git implementation.

Every runtime edge remains one hop: Worker to Durable Object, Worker to R2,
Worker to D1/KV, or Worker to Queue. The dedicated zero-authority Durable
Object-owned Container binding is the candidate's native-receive boundary. No
qualification handler chains a Durable Object call through to R2.

## Required deployment inputs

Every push to `main` must pass the repository TypeScript and Worker tests and
the Go tests in the pinned Container build stage before CI publishes
`ghcr.io/display-dev/git-on-cloudflare:<40-character-commit>`. The Container
job emits one `REPOSITORY_SERVICE_IMAGE=<json>` record in its log and run
summary and uploads the same schema-v1 record as
`repository-service-image-<40-character-commit>`. The record binds the source
revision, workflow run/attempt, and pinned BuildKit image to the registry's
immutable `sha256:` digest and `name@sha256:` reference. Step 0 must extend the
strict plan/evidence validators to freeze that complete record before the first
provider mutation. Until then, the emitted identity is build evidence rather
than an accepted plan field. A tag, including the commit tag, is not a
deployment identity.

Use a dedicated Cloudflare account scope or exact qualification resources. The
generated Wrangler configuration must bind one qualification-only Worker, D1
database, KV namespace, R2 bucket, Queue producer/consumer, `RepoDurableObject`,
`StockReceiveContainerHost`, `MaintenanceContainerHost`, and their three Container applications using the
same image built from `container/Dockerfile`.
The stock-receive application sets
`STOCK_RECEIVE_CONTAINER_IDLE_SECONDS` to a finite 120-second qualification
default. The host bounds configured values to 5–900 seconds and delegates idle
shutdown to Cloudflare's inactivity-aware lifecycle, so active receives remain
outside the stop condition and the same repository-bound process can serve a
nearby follow-on operation.

[`qualification/wrangler.template.jsonc`](../qualification/wrangler.template.jsonc)
freezes that composition. The external orchestrator must replace every
`__QUALIFICATION_*__`, `__TARGET_REVISION__`,
`__CONTAINER_IMAGE_REFERENCE__`, and `__CONTAINER_IMAGE_DIGEST__` placeholder
before deployment and must reject a
remaining placeholder. The template deliberately limits the maintained
each Container application to one instance for bounded qualification. Native
receive and maintenance may run simultaneously; approval must budget both.
The zero-authority framed-stream application remains unchanged and is not used
as the maintenance host.
The image reference must include the exact recorded digest; a local Dockerfile
build is not an acceptable qualification deployment identity.

Supply these values outside source control:

- `QUALIFICATION_MODE=1`;
- `QUALIFICATION_NAMESPACE=qual-<32–64 lowercase hex>`;
- `QUALIFICATION_REPOSITORY=repo-<16–64 lowercase hex>`;
- `QUALIFICATION_SECRET` as an independent secret;
- `QUALIFICATION_OBSERVER_SECRET` as a separate read-only operation-observer secret;
- `SESSION_SECRET` as an independent qualification-only server-side signing
  secret, never the production value; recovery journal authentication fails
  closed without it;
- `QUALIFICATION_TARGET_REVISION` as the exact deployed 40-character commit;
- `QUALIFICATION_CONTAINER_IMAGE_DIGEST` as the exact `sha256:` image digest;
- the synthetic Git credential for the exact repository; and
- provider credentials only to the external orchestrator.

After deployment, record the exact single-version Cloudflare deployment ID and
Worker version ID. The orchestrator uses a separate read-only control-plane
token to fetch those objects and verify that the active version's deployed
plain-text revision and image-digest bindings match the frozen plan. Values
echoed by the Worker endpoint alone are not accepted as provenance.

Set the deployed version's `workers/tag` annotation to the exact target commit.
Freeze the returned script ETag and SHA-256 of the complete canonical typed
binding array in the run plan; the maintained orchestrator rejects extra or
changed bindings and verifies the `StockReceiveContainerHost` export names the
expected Container. Cloudflare version detail exposes that Container by
configuration name rather than image digest, so the follow-on deployment spike
must prove the pinned digest reached the running rollout before scale
qualification is accepted.

Scale setup uses the existing native-import endpoint: the orchestrator prepares
an exact operation, uploads one bounded pack directly to the qualification R2
bucket with bucket-scoped S3 credentials, commits the import with the existing
internal bearer, polls its terminal operation view, and verifies the resulting
base ref. The multi-gigabyte pack does not cross the Worker request body.

`QUALIFICATION_SECRET`, `QUALIFICATION_OBSERVER_SECRET`, and `SESSION_SECRET`
must be installed with `wrangler secret put`; none may appear in Wrangler vars,
generated configuration, arguments, or evidence. All three secret bindings must
be present in the frozen canonical typed binding array. Only the observer secret enters
ForgeMark over its bounded JSON stdin request. A deployment lacking the mode
flag or the applicable bearer secret returns 404 for that qualification surface.
A missing or blank `SESSION_SECRET` instead refuses storage recovery with
`recovery_signing_unavailable` before any repository fence is taken.

## Readiness and reset sequence

1. The orchestrator validates the candidate revision and every resource
   binding, then checks the Worker readiness endpoint through the synthetic
   hostname.
2. It reads the schema-v2 qualification inventory and retains only the bounded
   counts and digest. Storage totals are split between ordinary repository
   objects and exact-key durable repository-generation metadata. Historical
   generation manifests and the current generation index are service-owned
   durable metadata; every other object remains in the exact cleanup baseline.
3. ForgeMark performs normal stock-Git operations with one run-scoped ref.
4. The orchestrator removes the run ref using an exact Git delete, verifies the
   expected baseline ref digest, and lets the normal reachability-GC path remove
   unreachable objects.
5. It invokes reset with the freshly observed ref digest and R2 object count.
   A parser failure, incomplete inventory, active repository, or mismatched
   operand blocks reset. Reset also waits for active reachability/compaction
   work, transactionally cancels an idle queued compaction request, and removes
   accepted-write and snapshot projections owned by disposable
   `refs/heads/qual-*` refs without disturbing projections retained by fixed
   refs.
6. It rereads inventory. Cleanup is conclusive only when refs, packs, transient
   state, and ordinary repository object count and bytes match the declared
   baseline. Durable repository-generation metadata remains separately
   accounted and may advance; otherwise the orchestrator retains a mode-0600
   recovery record.

### Interrupted-storage recovery

Exact reachability GC reuses an immutable source pack when its complete indexed
object set equals the authoritative closure. It still fences the entire source
catalog and ref version, publishes the new generation, and delays deletion of
the other packs for readers. A single already-exact pack is a no-op.

After ordinary GC and reset have settled, `POST .../storage-recovery` accepts
the same strict schema-v1 ref-digest/object-count operands as reset. This
default-off, exact-synthetic-repository control never accepts object keys. It
acquires the existing compaction fence only after every activity lease and
writer drain period has ended, requires no transient operation or pending
generation, and protects all catalogued and currently published pack triples.
It removes only complete recognized native authority pairs and the narrower
structurally proven pack-only compaction output described below. Native records
are eligible only after transient operation state has been cleared, including a
completed qualification operation whose ref name remains active. Those records prove run-output
ownership; the authoritative ref remains in RepoDO and its complete object
closure remains in protected catalogued/published pack triples. Legacy authority
records are not authenticated by a server-owned secret, so they prove only
their own paired-record shape and can never authorize deletion of an
uncatalogued pack, index, reference sidecar, or other artifact. The ref and
receipt bodies must also form one complete, content-valid pair. A run
marker, filename, timestamp or size alone is never ownership evidence. One
narrower structural case is recoverable: an aged, uncatalogued
`pack-cmp-<lease>.pack` with neither an index nor reference sidecar. Under the
repository-wide recovery lease and completed writer/reader drain, that object
cannot form a publishable pack triple or still be completed by its expired
compaction lease. The current catalog and published generation must not
reference it, and the fixed-key recovery journal revalidates the unchanged
inventory before deletion. Any sidecar, changed inventory, active writer or
reader, or other uncatalogued compaction/GC output is refused rather than
guessed.
Authoritative active-catalog and published-generation pack, index and reference
artifacts are removed from eligibility before ownership classification. Unknown
objects, any uncatalogued native output artifact, changed inventory, recent objects,
or incomplete publication block the whole deletion plan. Generation metadata
is retained. The response contains aggregate counts and bytes, and an
independent inventory is still required before removing the private recovery
record. The sweep validates the complete supplied inventory before selecting a
deterministic prefix of whole authority-owner or singleton compaction-output
groups totaling at most 100 keys.
Before deletion, the control writes an authenticated fixed-key private journal
binding the validated owner groups and the unchanged full-inventory remainder
digest, including active and protected artifacts. A
partially applied or acknowledgement-lost R2 deletion resumes from that journal
without accepting caller-selected keys or weakening complete-owner validation;
a journal not authenticated by a domain-separated server-only signing authority
separate from bucket-scoped R2 authority is refused, and journal execution
independently revalidates recognized authority or compaction key shape and
rejects currently protected keys. For a compaction output, any sidecar appearing
after planning changes the bound unchanged-inventory digest and refuses replay.
The existing server-only session
signing configuration must be present and nonblank or recovery fails closed
before taking a repository fence. If eligible objects remain after
a completed batch, the operator supplies the new exact object count to the same
maintained control. The sweep reserves its complete Worker subrequest envelope
before acquiring the fence and is bounded to one 1,000-object inventory page and
at most 800 authority-record reads; these are recovery safety caps, not service
storage limits.

Once the control has written a recovery journal, the external orchestrator must
resume that journal before any further receive, reset, GC, or generation
publication on the repository. The journal intentionally binds the original ref
digest, packset version, and complete unchanged inventory so interleaved
repository mutation fails closed rather than silently changing the deletion
proof. The private recovery record remains required until the journal is gone
and exact inventory is proven. If that ordering is violated, stop with the
retained recovery record; do not remove the journal or surviving owner record
outside the maintained control.

Fixed provider-resource teardown is a separate explicit operator action. An
ordinary run never deletes the Worker, D1, KV, R2 bucket, Queue, or Container
configuration.

## Durable reachability GC

GC admission registers one repository-owned operation before writing output.
The operation binds the protected refs, ref/catalog versions, exact source
rows, reachable-object digest, staging key, native output triple, and receipt.
Its phases are `queued`, `rewrite`, `index`, `publish`, `reclaim`, and `complete`.
An unpublished rejected result drains through `discard` to `blocked`.

Rewriting and native indexing run in separate Queue invocations. Completed
rewrite identity is durable before native dispatch. A lost upload reply is
reconciled against that intent and immutable R2 input. Native processing uses
the existing Container bridge, generates its own index/reference sidecar,
validates the exact closure, and uploads the unchanged complete pack again.
The receipt is written last. Retrying indexing first checks that receipt.
This implementation does not optimize the duplicate upload.

The operation's Durable Object alarm owns wakeups independently of Queue
retry exhaustion. Expired execution claims wait for the existing writer drain;
a late worker cannot publish through an expired claim. Publication retains
the existing lease, refsVersion, packsetVersion, exact source-row, generation,
reader, and repository-deletion checks. A committed-but-unacknowledged catalog
change is reconciled before deciding that source versions changed. GC changes
physical storage, not logical refs or user accepted-write events.

Permanent planner rejection without output releases its source fence. Invalid
immutable native artifacts and stale unpublished sources enter drained discard.
Transport failures remain retryable. No unpublished output is deleted until
ownership, absence from the committed catalog, and writer drain permit it.
Ordinary queued/rewrite/index work stops after 24 hours from immutable admission
time and enters that same drained discard path. Qualification can bind a shorter
deadline. Duplicate deliveries do not reset it. This recovery failsafe is not a
repository quota or GC latency target; publication/reclamation still reconcile
authoritative state and reader protection after the deadline.
Completion requires published generation and physical absence of superseded
pack/index/reference objects plus staging and receipt cleanup. Existing reader
leases delay deletion. Per-operation admission tombstones remain to reject
late duplicate deliveries after the terminal status is reset.

Encoding restrictions remain explicit: rewriting that requires a delta base
outside the exact reachable closure is rejected. This is not a delta-support
expansion. The 250-source-pack guard and 64 MiB per-sidecar byte guard are
implementation safety bounds, not repository quotas. The sidecar cap does not
bound total Worker memory: both sidecars and derived object sets coexist.
Large-object-count validation memory remains unqualified.

### Foreground coordination during GC

Once a durable source snapshot exists, its source lease protects inputs and
excludes competing maintenance, not ordinary receives. Rewriting, indexing and
expired-claim/drain waits leave receive admission open. Old operations without
the optional coordination record retain their original exclusive behavior.

Open receive admission is not proof of native execution availability. The prior
shared-processor candidate failed a receive during GC indexing. This candidate
keeps ordinary receive on the repository Container and dispatches GC indexing to
the dedicated `MaintenanceContainerHost`. The original RepoDO remains the only
ref/catalog and operation authority. No new accepted-write event is emitted by
maintenance. The separate stock framed-stream host remains zero-authority.

RepoDO durably issues each native job's lane, monotonically increasing generation,
operation, domain claim, deadline and bridge-grant digest. A host's durable slot
serializes handler installation/start/stop, not processing across the two hosts.
It rejects duplicate, replaced, cancelled and deleted jobs. Each R2 request checks
the exact job and live domain claim with RepoDO before touching storage; a later
job cannot replace another active job's bridge. The Go processor mutex is unchanged.

Cancellation revokes only that job, records an outstanding stop until the host
acknowledges it, and preserves the existing writer drain. Foreground recovery
does not wait for maintenance cancellation. Repository deletion is deliberately
broader: tombstone the authority, stop both hosts, then preserve normal drains
and readers before removal. Terminal lane records and generation/cancel high-water
marks are bounded protected metadata, not run-owned residue. Rollback requires
draining both lanes first; reverting code alone cannot undo new host metadata.
Whole-path responsiveness remains unqualified until the real overlap canary and
dependent foreground runs pass; local concurrency tests do not establish it.

Receive finalization accounts ref/catalog versions in the existing ref CAS.
Before acknowledging a generic receive, metadata-only reachability subtraction
protects any newly referenced source objects and external encoding bases. Normal
append-only checkpoints require no extra source retention. If a permitted write
resurrects old source objects, or metadata cannot establish the subtraction, all
source packs are conservatively retained; status reports this explicitly. Such
retention is safe but is not proof of full reclamation. Bounded ordinary native
receives now use the selective stock path's existing advertised-closure and
materialized thin-base validation. The qualification header remains only an
explicit fail-closed control for exact request-length and durable replay probes;
ordinary eligible stock Git does not need that header. The planner schedules at
most four independent exact range reads, shares physical bases across semantic
roots, and still emits deterministic base-before-dependent evidence.

Publication atomically replaces only the exact snapshotted source rows, merging
all later receive packs and any conservatively retained sources. Current refs and
HEAD are not overwritten. Accounted versions, source-row identity, live claim,
source lease, repository deletion, active receives and durable finalization
intents still fence publication. A prepared publication gets a metadata-only
turn before the next receive, avoiding restart-on-every-write starvation. A lost
reply replays the same durable receipt. Claims and drain periods are unchanged.
Ordinary compaction stays deferred through GC reclamation, with its queued demand
preserved. Repository deletion and real reader leases retain their existing
protection.

Status schema 2 adds bounded coordination versions, accepted-receive count,
conservative-retention disposition, prepared-publication flag and claim/drain
timestamps; it never exposes claim tokens or object keys. The real-reader latch
selects only the request marked `<gc-operation-id>-reader`, requires an actual
active read lease containing every source pack, and can start during indexing
or pre-publication recovery. Other reads are never captured. Its fifteen-minute
expiry is unchanged; start a recovery reader near the end of the ordinary drain
wait rather than extending its lifetime.

### Exact-target GC qualification controls

These controls require `QUALIFICATION_MODE=1`, the qualification secret, and
the configured exact synthetic namespace/repository before object lookup.
They are disabled by default and return `Cache-Control: no-store`.

`GET .../native-executions` returns at most the two lane records: operation,
generation, grant digest, state, dispatch/input-read/completion timestamps and
bounded bridge byte/request counters. Input bytes are declared R2 response
payload, not proof that every byte reached the Container. Completed write bytes
count verified output uploads; native receipt counters remain distinct.
`POST` on that route accepts only schema 1 and one of `hold-input` (lane,
operation, deadline no more than 120 seconds away), `release-input` (lane and
operation), or `cancel` (lane, operation and generation). Cancellation requires
observed real input I/O. The hold pauses that job's actual R2 response, not an
idle process; it neither changes claims/drains nor writes catalog state. Release
holds during cleanup, including after lost control replies. No arbitrary command,
object key, URL, claim credential or repository selector is accepted in the body.

- `POST .../gc`: closed schema-v1 `operationId`, `faults`, `holdReader`, and
  `deadlineAt` admission. Supported one-shot faults are `after-rewrite`,
  `during-native`, `before-publication`, and `after-publication`.
- `GET .../gc/:operationId`: sanitized durable status, completed phase costs,
  native transfer measurements, authoritative publication and R2 source-absence
  checks. Claims, object keys, credentials, and reader tokens are excluded.
- `POST .../gc/:operationId/release-reader`: releases only that operation's
  qualification latch, not the underlying reader/deletion fence.
- `GET .../gc/:operationId/artifacts/{pack,index,references}`: published output
  under an ordinary reader lease, for independent empty-store reconstruction.
- `GET .../gc-source` and `GET .../gc-source/:ordinal/artifacts/:role?generation=N`:
  bounded current source inventory and generation-bound source downloads.

The reader test uses a real Git request. The latch records an actual blocked
deletion attempt before release; it cannot manufacture overlap by merely
registering a reader. The native fault stops only an observed matching native
operation and records successful Container stop separately from fault intent.
Qualification deadlines stop starting new rewrite/index work after normal
claim drain; they do not bypass publication reconciliation or reader fences.

Durable measurements cover completed attempts. Partial work lost to runtime
termination can be unmetered; request/byte counters are not billed-cost proof.
The complete 3 GB lifecycle and interruption/reader cases require live
qualification. Local tests or the earlier isolated native-indexing spike do
not establish those results.

Foreground setup and operator teardown may call the disabled-by-default
`POST /_internal/qualification/:owner/:repo/gc-source/settle` control with schema 1
and the exact current `expectedRefStateDigest`. It cancels only pending ordinary
compaction demand. Active GC or repository deletion rejects the request;
receive/compaction leases, claims, readers, catalogs and refs are unchanged.
The response distinguishes request cancellation from an active writer, which
must still finish or drain normally. This is not automatic GC recovery and must
not be used during measured GC. New foreground receives can request compaction
again; GC preserves that demand through reclamation.
