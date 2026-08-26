# Qualification deployment and operation

The repository-service qualification surface is an internal, synthetic-only
test seam. It is not part of the product API and must never share a route,
credential, resource, or data set with production.

## Frozen candidate composition

Schema v1 qualifies this repository's normal Worker entrypoint, repository
route lookup, `RepoDurableObject`, native receive Container image, R2 object
store, D1 route authority, ROUTES KV candidate cache, and repository task
Queue. Git push, clone, fetch, operation receipts, pack publication, and
reachability cleanup use the same modules as ordinary service traffic. The
qualification routes add observation and exact reset operands; they do not add
a benchmark Git implementation.

Every runtime edge remains one hop: Worker to Durable Object, Worker to R2,
Worker to D1/KV, or Worker to Queue. The Durable Object-owned Container binding
is the candidate's existing native-receive boundary. No qualification handler
chains a Durable Object call through to R2.

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
and the Container image built from `container/Dockerfile`.

[`qualification/wrangler.template.jsonc`](../qualification/wrangler.template.jsonc)
freezes that composition. The external orchestrator must replace every
`__QUALIFICATION_*__`, `__TARGET_REVISION__`,
`__CONTAINER_IMAGE_REFERENCE__`, and `__CONTAINER_IMAGE_DIGEST__` placeholder
before deployment and must reject a
remaining placeholder. The template deliberately limits the maintained
Container pool to one instance for the bounded first qualification slices.
The image reference must include the exact recorded digest; a local Dockerfile
build is not an acceptable qualification deployment identity.

Supply these values outside source control:

- `QUALIFICATION_MODE=1`;
- `QUALIFICATION_NAMESPACE=qual-<32–64 lowercase hex>`;
- `QUALIFICATION_REPOSITORY=repo-<16–64 lowercase hex>`;
- `QUALIFICATION_SECRET` as an independent secret;
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
changed bindings and verifies the `RepoDurableObject` export names the expected
Container. Cloudflare version detail exposes that Container by configuration
name rather than image digest, so the follow-on deployment spike must prove the
pinned digest reached the running rollout before scale qualification is accepted.

Scale setup uses the existing native-import endpoint: the orchestrator prepares
an exact operation, uploads one bounded pack directly to the qualification R2
bucket with bucket-scoped S3 credentials, commits the import with the existing
internal bearer, polls its terminal operation view, and verifies the resulting
base ref. The multi-gigabyte pack does not cross the Worker request body.

`QUALIFICATION_SECRET` must be installed with `wrangler secret put`; it must not
appear in Wrangler vars, generated configuration, arguments, evidence, or
ForgeMark input. A deployment lacking either the mode flag or secret returns
404 for the qualification surface.

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
   operand blocks reset. Reset also waits for reachability/compaction work and
   removes accepted-write and snapshot projections owned by disposable
   `refs/heads/qual-*` refs without disturbing projections retained by fixed
   refs.
6. It rereads inventory. Cleanup is conclusive only when refs, packs, transient
   state, and ordinary repository object count and bytes match the declared
   baseline. Durable repository-generation metadata remains separately
   accounted and may advance; otherwise the orchestrator retains a mode-0600
   recovery record.

Fixed provider-resource teardown is a separate explicit operator action. An
ordinary run never deletes the Worker, D1, KV, R2 bucket, Queue, or Container
configuration.
