# What changed

- `CODEX_SUMMARY.md`: Record the final changes, behavior, and gate results after review.
- `crates/loonfs-core/src/commit_engine_content_tests.rs`: Use the default shard target in existing GC options.
- `crates/loonfs-core/src/commit_engine_inline_layout_tests.rs`: Pin ordered shard listings, exact object deletion, and a shared base in another shard with fixed content ids.
- `crates/loonfs-core/src/gc/charged_set.rs`: Charge string and content-id sets to read working memory and release their reservations.
- `crates/loonfs-core/src/gc/collect.rs`: Sweep listed entries, dispatch content shards, and report shard width and layout-view count.
- `crates/loonfs-core/src/gc/content.rs`: Deduplicate layout views, collect shared bases, and build and release one shard of object keys at a time.
- `crates/loonfs-core/src/gc/fork_pins.rs`: Read source-pin presence without HEAD during retirement.
- `crates/loonfs-core/src/gc/live_set.rs`: Keep tail and session ids, root manifests from listed successor times, skip absent successors, and remove revision scans.
- `crates/loonfs-core/src/gc/mod.rs`: Register charged sets and content sweeping; remove the unused deletion helper export.
- `crates/loonfs-core/src/gc/options.rs`: Default and validate content_shard_rows while preserving GcRequest conversion.
- `crates/loonfs-core/src/gc/reap.rs`: Construct GraceAge from optional listed times and remove delete_if_aged.
- `crates/loonfs-core/src/gc/sweep.rs`: Age candidates from their listing entries and protect content through shard decisions.
- `crates/loonfs-core/src/gc/tests.rs`: Preserve pin-release reclamation, test the listed-age deletion boundary, validate shard targets, and use fixed ids for reclamation fixtures.
- `crates/loonfs-core/src/gc/tests/listings.rs`: Allow only the discovery successor HEAD; pin listed ages and segment reads for covered and distinct layout views.
- `crates/loonfs-core/src/gc/tests/many_pins.rs`: Expect the content-id listing prefix.
- `crates/loonfs-core/src/gc/tests/retirement.rs`: Use the default shard target in retirement options.
- `crates/loonfs-core/src/gc/tests/superseded_roots.rs`: Keep missing-successor reclamation and verify that age selection reads only rooted existing manifests.
- `crates/loonfs-core/src/lib.rs`: Remove the unused delete_if_aged export.
- `crates/loonfs-core/src/limits.rs`: Define GC_CONTENT_SHARD_ROWS as 65,536.
- `crates/loonfs-core/src/manifest/tests/cas_recovery.rs`: Forward listing entries in the test store.
- `crates/loonfs-core/src/manifest/tests/streaming_compaction.rs`: Forward listing entries and use the default shard target.
- `crates/loonfs-core/src/namespace/tests.rs`: Use the default shard target in existing GC options.
- `crates/loonfs-core/src/pin/tests/inventory.rs`: Forward listing entries in the test store.
- `crates/loonfs-core/src/wal/tests.rs`: Use the default shard target in existing GC options.
- `crates/loonfs-grep/src/gc.rs`: Use listing timestamps for candidate deletion; keep grace_age for successor checks without a listing.
- `crates/loonfs-grep/tests/it/grep_step_permits.rs`: Delegate the required entry-listing method.
- `crates/loonfs-objectstore/src/lib.rs`: Export ListedObject.
- `crates/loonfs-objectstore/src/local_fs_store.rs`: Return listed file modification times through the same conversion used by HEAD.
- `crates/loonfs-objectstore/src/metrics.rs`: Instrument entry streams without dropping timestamps.
- `crates/loonfs-objectstore/src/object_store.rs`: Require entry streams, provide key projection, and update Arc, reference, and test implementations.
- `crates/loonfs-objectstore/src/probe.rs`: Forward listing entries in probe test stores.
- `crates/loonfs-objectstore/src/provider_object_store.rs`: Preserve listing timestamps and use raw-prefix pagination; test partial prefixes, continuation pages, and resuming.
- `crates/loonfs-objectstore/tests/it/metrics_instrumented_object_store.rs`: Forward entries through the instrumented-store fixture.
- `crates/loonfs-objectstore/tests/it/objectstore_conformance.rs`: Require partial-prefix listings to resume in order and listed timestamps to agree with HEAD.
- `crates/loonfs-sim/src/fault_store.rs`: Keep timestamps while tracing listings and injecting omitted keys.
- `crates/loonfs-test-support/src/stores/buffer_watch_store.rs`: Forward listing entries.
- `crates/loonfs-test-support/src/stores/concurrency_watch_store.rs`: Forward listing entries.
- `crates/loonfs-test-support/src/stores/delegate.rs`: Generate the required entry-listing method in delegated stores.
- `crates/loonfs-test-support/src/stores/fake_multipart_store.rs`: Forward listing entries.
- `crates/loonfs-test-support/src/stores/intercept_store.rs`: Record and inject faults into entry listings.
- `crates/loonfs-test-support/src/stores/metadata_map_store.rs`: Apply timestamp transforms to listing entries as well as object metadata.
- `crates/loonfs-test-support/src/stores/operation.rs`: Describe entry-listing operation records.
- `crates/loonfs/src/fs/maintenance/tests.rs`: Remove the obsolete revision-scan comment; retain five GETs and one discovery HEAD for idle collection.
- `crates/loonfs/src/lib.rs`: Remove the unused deletion helper from extension exports.
- `crates/loonfs/tests/it/content_ref_import_access.rs`: Use the default shard target in existing GC options.
- `crates/loonfs/tests/it/retained_views.rs`: Use the default shard target in existing GC options.
- `crates/loonfs/tests/it/staged_content_reclamation.rs`: Use the default shard target in existing GC options.
- `docs/design/content-collection.md`: Describe layout roots, shard costs, listed successor presence, and the discovery HEAD exception.
- `docs/specs/format.md`: Specify listed ages, absent-successor exclusion, exact layout roots, and stateless shard sweeping.

Listing implementations changed: **31**: 21 explicit implementations and 10 generated by the shared delegation macro. The trait declaration and macro definition are not counted as additional implementations.

# Findings skipped and why

- `STYLE.md` and `.codex/preamble.md` are absent from this worktree. Their copies in `/Users/conormccarter/Code/loonfs` were read and followed. Neither they nor `CONTRIBUTING.md` were edited.
- Layout compaction still uses its conservative revision filter. Its policy is outside this change. Fixed ids make the affected collection fixtures deterministic.
- Grep successor discovery still uses `grace_age` because that caller has no manifest listing. Its candidate sweep uses listed timestamps. No broader grep root-policy change was made.
- No wire or durable encoding changed. Versions remain at 1. Golden fixtures and OpenAPI documents were not regenerated; their checks were run.

# Behavior changes a reviewer must know about

- `ObjectStore` implementers must provide `list_entries_from_stream`. Key-only methods project entries to keys. Raw-prefix provider pagination preserves partial content-id prefixes, continuation pages, and listed timestamps.
- An older manifest is a grace root only when its immediate successor is listed and is younger than the grace or has no timestamp. An absent successor adds no root. Current and pinned manifests retain their independent protection.
- Pin release can reclaim the old manifest, its segments, and its unreferenced content after its successor has been collected. The original reclamation assertions and `assert_basis_reaped` are restored.
- Discovery uses its original HEAD successor probe. `SuccessorProbe` was removed. The sweep issues no HEAD requests for content, WAL, segments, pins, sessions, or temporary objects. Candidate ages and superseded-manifest ages come from listings.
- `provider_age_reserves_the_clock_margin_before_deletion` now runs the collector and records candidate deletes: none one millisecond before the full grace, and one at the boundary, with no HEAD for the candidate.
- `GcOptions.content_shard_rows` defaults to 65,536 and rejects zero. It is not a wire request field. The current manifest's layout row count selects a hex width from zero through four.
- Layout rows protect exact owned extent keys. Unnamed spans of a live chain can be deleted. Shared bases remain protected across shards; tail and session ids protect whole chains. All three sets use read working memory. Each shard releases its reservations before the next.
- Malformed content keys outside the listed `con_` shard prefixes remain stored but are not included in retained-candidate counts.
- The pass stores no durable progress. No files were staged, committed, or pushed.

# Gates run

The full required gate list ran again in the requested order after the review changes. `XDG_STATE_HOME=/tmp/loonfs-review-gates-8peegb06` kept CLI upload journals in writable temporary storage.

- `cargo fmt --all`: PASS.
- `cargo clippy --all-targets --all-features -- -D warnings`: PASS.
- `cargo test -p loonfs-types`: PASS; 307 passed, 0 ignored.
- `cargo test -p loonfs-core --features test-support`: PASS; 783 passed, 0 ignored.
- `cargo test -p loonfs`: PASS; 423 passed, 1 ignored.
- `cargo +1.99.0 clippy --all-targets --all-features -- -D warnings`: PASS.
- `cargo +1.88 check --workspace --all-targets --all-features --locked`: PASS; four existing dead-code warnings in unchanged `loonfs-http/src/http/extractors.rs`.
- `cargo test -p loonfs-http --features openapi`: BLOCKED (exit 101); 71 passed and 35 failed at listener creation with `PermissionDenied` (OS error 1); later targets did not run after the library failure.
- `cargo test -p loonfs-server`: BLOCKED (exit 101); 112 passed and 8 failed at listener creation with `PermissionDenied` (OS error 1); later targets did not run after the library failure.
- `cargo test -p loonfs-client`: BLOCKED (exit 101); 72 passed and 2 failed at listener creation with `PermissionDenied` (OS error 1); later targets did not run after the library failure.
- `cargo build -p loonfs-server`: PASS.
- `cargo test -p loonfs-cli`: BLOCKED (exit 101); all 100 unit tests passed; 118 embedded or non-network integration tests passed; 23 remote integration tests failed at listener creation with `PermissionDenied` (OS error 1).
- `cargo test -p loonfs-http --features openapi --test it`: PASS; 33 passed, 0 ignored.

Additional checks:

- `cargo test -p loonfs-core --features test-support gc::`: PASS; all 64 selected tests passed before the full gate run.
- `git diff --check`: PASS.

Socket-dependent HTTP, server, client, and CLI remote tests could not complete in this sandbox. The standalone OpenAPI target was run after the full gate list to verify the checked-in documents. No permission escalation was attempted.

Gate logs are `/tmp/content-review-gate-01.log` through `/tmp/content-review-gate-13.log`. The focused collector log is `/tmp/content-review-targeted.log`.
