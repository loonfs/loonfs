# LoonFS pre-release cleanup — PR execution plans

Derived from the July 2026 consistency/structure audit of this branch (pre-`main`). Each section below is a self-contained plan for one PR, sized for a single implementing agent session, ordered so that earlier PRs never rebase onto later ones.

> **Round 1 scope:** waves 1, 2, 4, 5, and 6, executed as a stacked branch series based on the current branch. Waves 3 (observability), 7 (tests), and 8 (CI/docs) are deferred to a later round — skip them entirely for now.
>
> **Progress ledger** (stack base: `conor/pr155-conflict-resolution`):
> - PR-01 `conor/wire-field-names-and-tags` → [#179](https://github.com/loonfs/loonfs/pull/179) ✅ (scope grew per decision 2: also `child_inode`→`child_inode_id`, `new_parent_inode`→`new_parent_inode_id`, 3 straggler error fields; fingerprint pins re-pinned under v0)
> - PR-02 `conor/format-key-table-sync` → [#180](https://github.com/loonfs/loonfs/pull/180) ✅ (7 drifted rows fixed, not 2; sync test covers all 18 families)
> - PR-03 `conor/error-display-messages` → [#181](https://github.com/loonfs/loonfs/pull/181) ✅ (79 variants; `WriterEpoch` doesn't exist — audit ref was stale; FenceToken + InodeKind got Display too)
> - PR-04 `conor/id-newtype-macro` → [#182](https://github.com/loonfs/loonfs/pull/182) ✅ (fixture sample id `up_`→`upl_` fixed rather than worked around)
> - PR-05 `conor/client-listing-order` → [#183](https://github.com/loonfs/loonfs/pull/183) ✅
> - PR-06 `conor/single-op-effect-encoding` → [#184](https://github.com/loonfs/loonfs/pull/184) ✅ (no pre-existing divergence; shared delta-applier, −180 prod lines)
> - PR-07 `conor/one-error-taxonomy` → [#185](https://github.com/loonfs/loonfs/pull/185) ✅ (PreconditionFailed kind removed, Gone added; statuses bit-identical)
> - PR-08 `conor/cli-error-registry` → [#186](https://github.com/loonfs/loonfs/pull/186) ✅ (embedded/remote code parity, pinned by two-binary test)
> - PR-09 `conor/error-style-sweep` → [#187](https://github.com/loonfs/loonfs/pull/187) ✅ (note: ObjectStoreError reshape forced mechanical construction-site fixes in loonfs-sim/fault_store.rs — no sim public API change; flagged in PR body)
> - PR-14 `conor/split-protocol-module` → [#188](https://github.com/loonfs/loonfs/pull/188) ✅ (5 submodules incl. candidates.rs; enter-across-await fixed; `acquired_writer` expects were already gone — stale audit ref)
> - PR-15 `conor/unify-commit-validators` → [#189](https://github.com/loonfs/loonfs/pull/189) ✅ (CommitValidationView trait; 40→19 helpers; −551 prod lines; mutation_guards untouched; includes lockfile catch-up commit from PR-08's dep drop)
> - PR-16 `conor/split-metadata-state` → [#190](https://github.com/loonfs/loonfs/pull/190) ✅ (rows/apply/queries/tests; 2 walk pairs collapsed, 9 left with reasons; wal tests → sibling file)
> - PR-17 `conor/single-visibility-implementation` → [#191](https://github.com/loonfs/loonfs/pull/191) ✅ (**real drift found**: current_view's two visible_child copies lacked the bound-child-visible check — unified on the stricter rule; BindingIdentity + MetadataVisibilityReads trait; 859 duplicated lines deleted)
> - PR-18 `conor/read-context-parameter` → [#192](https://github.com/loonfs/loonfs/pull/192) ✅ (A7 shadow methods were already gone — done by 92e715c0; shipped as the _with_X folding: single begin_upload/list_changes_after per layer + ListChangesOptions)
> - PR-19 `conor/shared-provider-config` → [#193](https://github.com/loonfs/loonfs/pull/193) ✅ (StoreConfig + SecretString in objectstore; caught two live traps — presigner would have signed with `<redacted>`; 17-row flag table replaces 120 reject calls)
> - PR-21 `conor/complete-facade-seam` → [#194](https://github.com/loonfs/loonfs/pull/194) ✅ (loonfs-core fully out of server deps — no dev-dep even needed; Fs::namespace_status returns wire type; one SharedObjectStore)
> - PR-22 `conor/client-dead-code` → [#195](https://github.com/loonfs/loonfs/pull/195) ✅ (−180 lines + walkdir; param-typing note moved into PR-23's Backend remodel)
> - PR-20 `conor/split-http-module` → [#196](https://github.com/loonfs/loonfs/pull/196) ✅ (7-file split; NamespaceIdPath extractor preserves 401-before-400; operationId `_handler` suffixes REMOVED from openapi — overrode the agent's byte-identical pinning since ids are pre-release-free and become SDK names)
> - PR-23 `conor/admin-plane-parity` → [#197](https://github.com/loonfs/loonfs/pull/197) ✅ (Backend trait promoted to loonfs-client w/ BackendError; EmbeddedBackend stays CLI-side; `loon admin checkpoint|retention-advance` + `loon changes`; capability→command conformance table; round-2 note: advance_retention on missing namespace classifies `namespace_corrupt` in both modes — core LoadHead quirk)
> - PR-24 `conor/publisher-into-runtime` → [#198](https://github.com/loonfs/loonfs/pull/198) ✅ (opt-in per amended decision 6; server −1789 lines; core publisher.rs→commit_engine.rs collision fix rode along)
> - PR-25 `conor/naming-canon` → [#199](https://github.com/loonfs/loonfs/pull/199) ✅ (InodeKind::Directory w/ wire pin held; v0/ module consolidation; ls prints "dir"/"file" now — the one human-output change)
>
> **ROUND 1 COMPLETE** — 21 stacked PRs, [#179](https://github.com/loonfs/loonfs/pull/179) → [#199](https://github.com/loonfs/loonfs/pull/199), each green on fmt + clippy `-D warnings` + full `cargo test --all` (560 tests at stack tip). Merge order = PR number order (each PR's base is the previous branch). Round-2 queue: waves 3 (observability), 7 (tests), 8 (CI/docs/CONTRIBUTING+STYLE codification), plus noted quirks (advance_retention→namespace_corrupt classification; namespace-list design).
> - Wave 0 cleanup ✅ (stale local configs deleted; gcs local kept chmod 600; key rotation with Conor)
>
> Stale-audit notes discovered during execution (for future reference): `WriterEpochAcquireError`/`writer_epoch.rs` no longer exist; `metadata/view.rs` became `path/read/{current_view,materialized_view}.rs`; `_base_seq` dead params already removed; `commit_validation_from_core` replaced by `#[from]`.

## Context for the implementing agent

- **There are no users and no deployments. Backwards compatibility is a non-goal.** Break wire formats, rename public APIs, delete dead code without deprecation cycles. Do it the clean way, not the compatible way.
- **North star: the codebase should read as if one very thoughtful engineer wrote it.** When a plan says "canonicalize", pick the stated pattern and eliminate the others everywhere — half-migrations are worse than no migration.
- **Prefer deletion over abstraction.** If a plan can be satisfied by removing code, that beats adding a layer.
- Workspace lints (`Cargo.toml`) and `clippy.toml` disallowed methods (no ambient `SystemTime::now`/`Instant::now`/`sleep`/`rand`) apply to all new code. In tests, use function-scoped `#[allow(clippy::disallowed_methods)]` with a reason — never file-scoped.
- CLI output goes through `crates/loonfs-cli/src/render.rs` (that's how the `print_stdout` lint is satisfied). Don't add bare `println!`.

### Git conventions (mandatory)

- Branch names: `conor/<kebab-description>` (project convention — every PR branch starts with `conor/`). Never include "claude", "ai", or any agent identifier in branch names, commits, or PRs.
- Commit messages: single line, matching the repo convention `type(scope): lowercase summary` (verify with `git log --oneline -15`). No bodies, no bullet changelogs.
- **Never add `Co-Authored-By: Claude`, "Generated with Claude Code", or any AI attribution anywhere.** Check `git log -1` before pushing; amend if tooling injected anything.
- PR descriptions: 1–4 sentences of rationale-first prose (why the change exists). No section templates, no diff restatement.
- Base every PR on `main` (after the current branch merges). If stacking is unavoidable, note the base in the PR description.

### Definition of done (every PR)

1. `cargo fmt --all --check`
2. `cargo clippy --all-targets --all-features -- -D warnings`
3. `cargo test --all` (build `loonfs-server` first; the CLI suite probes its binary)
4. Spec-locked artifacts updated deliberately, never hand-edited to silence tests:
   - OpenAPI: the failing `openapi_static_file_is_current` test prints the regen command — run it.
   - Golden wire fixtures: `crates/loonfs-api/tests/golden/` — updating them **is expected** for wire-format PRs; say so in the PR description.
   - `docs/specs/api.md` / `format.md` tables are test-enforced; change doc and code together.
   - CLI snapshots: `cargo insta review`.
5. If the PR changes a convention, sweep it to 100% — grep for stragglers before finishing.

### Decisions baked into this plan (veto before executing)

1. **All internally-tagged serde enums in wire/durable formats use `tag = "kind"`** — including today's `op`, `delta`, `type`, `row_kind`. Scope: wire + durable + CLI JSON output shapes. NOT in scope: human-authored config TOML (`mode = "embedded"` in CLI profiles stays — semantic keys are right for config), and `loonfs-sim` (frozen, decision 5).
2. **Id-typed fields are suffixed `_id`** in every serialized format (`parent_inode_id`, not `parent_inode`).
3. **HTTP statuses do not change.** `ErrorKind` is reconciled *to* the currently-served statuses, then becomes the single source statuses derive from.
4. **`parse()` is the canonical fallible constructor** for validating string types; `try_new` is deleted.
5. **`loonfs-sim` is frozen — do not modify, adopt, or delete it.** The simulator lives in another repo and consumes these hooks; treat the crate (including its `inspection` feature and its dependency list) as an external contract.
6. **The batching publisher moves from `loonfs-server` into the `loonfs` runtime crate** (PR-24). The one big architectural move; skipping it does not block anything else. **Execution amendment:** it lands as an *opt-in* concurrent-write front-end, NOT wired into embedded `Fs` default paths — the measured pacing constants (100ms coalescing delay, 1s per-namespace CAS interval) would regress every solo embedded/CLI write if always-on. Server behavior unchanged; embedded many-writer hosts construct the registry explicitly.
7. **`loon namespace list` is deferred — do not implement it.** It needs design thought (it rides on list calls). Leave the doc/spec references alone for now; they get reconciled in the docs wave of a later round.
8. Wire types keep **tolerant decoding** (no `deny_unknown_fields`); config-file structs get `deny_unknown_fields`.
9. Rust-side names spell out `Directory`; frozen wire values (e.g. `"dir"`) are untouched.

---

## Wave 0 — operator action (not a PR)

Key rotation for the Cloudflare R2 credential in `configs/loond.cloudflare-r2.host-a.local.toml` is handled by the lead directly. Workspace cleanup (delete the stale untracked local configs — `loond.*`, `loon.host-a/host-c.*` old-schema files, the `loonfs-server.azure-abs.local.toml` example twin — and `chmod 600` remaining `*.local.toml`) is done. Git history was already clean.

---

## Wave 1 — wire-format freeze and verified bugs

These change durable/wire shapes or observable behavior. Land before everything else; every later PR rebases cleanly on top.

### PR-01 · `conor/wire-field-names-and-tags`
**Title:** `refactor(api): unify wire field names and enum tag discriminators`
**Effort:** M · **Depends on:** —
**Why:** The same concept serializes as `parent_inode` in the WAL format but `parent_inode_id` in the manifest format, and internally-tagged enums use five different discriminators (`kind`, `op`, `type`, `delta`, `row_kind`). These freeze at first release.
**Do:**
- Rename `parent_inode` → `parent_inode_id`, `root_inode` → `root_inode_id`, `child_inode` → `child_inode_id`, `new_parent_inode` → `new_parent_inode_id` (decision 2 covers all id-typed fields) in `crates/loonfs-api/src/wal.rs` and `crates/loonfs-api/src/v0.rs`, rippled through core/model/tests.
- Change every wire-format `#[serde(tag = ...)]` to `tag = "kind"`: loonfs-api `wal.rs`/`v0.rs`/`http.rs`/`manifest.rs`, plus core `commit/{operation,materialize,precondition}.rs`, `path/write/planner.rs`, and cli `commands/output.rs`. (loonfs-sim and CLI config-TOML tags stay per decision 1 scope.)
- Update golden fixtures, regen OpenAPI, update `docs/specs/format.md`/`api.md` examples.
- Add one line to `format.md` stating the policy: snake_case values, `kind` discriminator, tolerant decoding (unknown fields ignored).
**Done when:** grep for `tag = "` in loonfs-api shows only `"kind"`; grep for `parent_inode:` / `root_inode:` (without `_id`) is empty workspace-wide; golden + openapi tests green.

### PR-02 · `conor/format-key-table-sync`
**Title:** `docs(specs): fix format.md key table and pin it with a sync test`
**Effort:** S · **Depends on:** —
**Why:** The normative key table says `{manifest_id}.manifest` and `{table_id}.sst`; the code writes `.manifest.json` (zero-padded id) and `.sst.zst` (`crates/loonfs-objectstore/src/layout.rs:183,190`). The test named `key_builders_match_spec_examples` (`keys.rs:108`) asserts the code's values, not the spec's.
**Do:**
- Correct the `format.md` table rows (manifest, metadata SST; note `{manifest_id:020}` padding).
- Add a sync test that parses the `format.md` key-pattern table and asserts each pattern against `ObjectLayout` builders — same mechanism as `error_status_mapping_matches_the_api_spec_table` (`crates/loonfs-server/src/http.rs:~1700`).
- Fix the stale `seg_…` WAL-id example in `keys.rs:119` to match the real `{start_seq:020}-{suffix}` grammar.
**Done when:** the new test fails if either the doc table or the layout code changes unilaterally.

### PR-03 · `conor/error-display-messages`
**Title:** `refactor(core): give commit pipeline errors real display messages`
**Effort:** S–M · **Depends on:** —
**Why:** Five core error enums have no `Display`, so `CoreError` Debug-formats them (`{0:?}`, `crates/loonfs-core/src/error.rs:44-51`) and clients receive raw Rust syntax like `CreateChildNameCollision { parent_inode: InodeId(2), ... }` in HTTP bodies.
**Do:**
- Derive `thiserror::Error` with `#[error(...)]` messages on `CommitValidationError` (`commit/validate_error.rs`), `CommitHeadPublishError` (`commit/publish_error.rs`), `WalBuildError` + `WalReplayError` (`wal/frame.rs:19,192`), `MetadataApplyError` (`metadata/mod.rs:174`). serde derives coexist fine (`ManifestLoadError` is the in-repo precedent).
- Switch the `CoreError` wrappers from `{0:?}` to prefixed `{0}`; same for `WalChainLoadError::Replay` (`frame.rs:175`).
- Add `Display` to numeric newtypes `RevisionNo`, `ChangeSeq`, `WriterEpoch` (`crates/loonfs-api/src/ids.rs:682,690`) and convert message interpolations from `{field:?}` to `{field}` (e.g. `error.rs:64,139`, `frame.rs:148,173`, `checkpoint/error.rs:31`).
- Message style: lowercase, backticked identifiers, no trailing period — match the existing majority.
**Done when:** no `{0:?}`/`{field:?}` remains in any `#[error(...)]` string for types that now have Display; an http_smoke assertion shows a human-readable name-collision message.

### PR-04 · `conor/id-newtype-macro`
**Title:** `refactor(api): macro-generate id newtypes and retire raw string ids`
**Effort:** M–L · **Depends on:** PR-01 (rebase order only)
**Why:** `ids.rs` hand-rolls ~750 lines of near-identical impls with real drift (`NameKey` missing `TryFrom`/`Ord`; only `ManifestId` has `From<u64>`), while upload/WAL-segment/table/pin ids are bare `String`s with free `generate_*`/`validate_*` functions — a second, weaker id system.
**Do:**
- Write `string_id!` / `numeric_id!` macros (precedent: `typed_key!` in `crates/loonfs-objectstore/src/layout.rs:30-51`) generating the full uniform suite: `parse`, `as_str`, `TryFrom<&str>`, `TryFrom<String>`, `AsRef`, `Borrow<str>`, `Display`, `FromStr`, serde, `Ord`, `Hash`.
- Regenerate `NamespaceId`, `ContentStoreId`, `CommitId`, `CheckpointId`, `NameKey` through it; fix the drift (NameKey gets the missing impls and derives; `InodeKind` gains `Copy` + `Hash`).
- Newtype the stragglers: `UploadId`, `WalSegmentId`, `MetadataTableId`, `GcPinId`. Delete the free `generate_upload_id`/`validate_upload_id`/`generate_wal_segment_id`/`generate_checkpoint_id` functions (`ids.rs:165-194`) — generation becomes inherent `::generate()`. Wire representation stays a plain string; this is API-only.
- Canonicalize constructors: keep `parse()`, **delete `try_new()`** everywhere (`ids.rs:112/119` pattern ×5).
- Use `NameKey` (not `String`) in manifest rows (`manifest.rs:132`) and the model.
- Give `AbsolutePath`/`DisplayName`/`PathComponent` (`crates/loonfs-api/src/path.rs`) the same trait suite.
- Ripple signatures through core/server/cli (e.g. `CoreError::UploadNotFound { upload_id: UploadId }`).
**Done when:** `ids.rs` has zero hand-written per-type serde/Borrow/TryFrom impls; grep for `try_new` is empty; golden fixtures byte-identical (string representations unchanged) — if fixtures change, something is wrong.

### PR-05 · `conor/client-listing-order`
**Title:** `fix(client): preserve canonical server listing order`
**Effort:** S · **Depends on:** —
**Why:** The server pages directory entries in canonical casefolded name-key order; the client re-sorts aggregated pages by raw `display_name` (`crates/loonfs-client/src/lib.rs:244-248`), so `loon ls` can print different orders for embedded vs remote profiles.
**Do:**
- Delete the client re-sort; concatenated pages are already canonical.
- State the canonical presentation order in `docs/specs/api.md` (one sentence next to the list endpoint).
- Add a test with names that order differently by display-name vs name-key (e.g. `B.txt` vs `a.txt`) asserting client output matches server page order.
**Done when:** embedded and remote listings agree in the test.

### PR-06 · `conor/single-op-effect-encoding`
**Title:** `refactor(core): derive commit overlay rows from wal materialization`
**Effort:** S–M · **Depends on:** —
**Why:** `ValidatedOp` effects are encoded twice — `apply_validated_op_mut` (`crates/loonfs-core/src/commit/metadata_overlay.rs:27`) maps ops→rows directly while `materialize_validated_op` (`commit/materialize.rs:76`) maps ops→WAL deltas which `apply_committed_wal_delta_mut` (`metadata/mod.rs:350`) maps to rows. Nothing forces the two paths to agree; divergence = validation preview disagreeing with durable replay, a correctness bug.
**Do:**
- Reimplement the overlay application as composition: `materialize_validated_op(op)` then delta-application onto the overlay rows. Delete the hand-maintained direct mapping.
- Keep a regression test asserting overlay rows == materialize-then-apply rows for every `CommitOp` variant (this test is the point of the PR — write it first against the old code).
**Done when:** exactly one op→rows path exists; the per-variant equivalence test passes.

---

## Wave 2 — error architecture

### PR-07 · `conor/one-error-taxonomy`
**Title:** `refactor(api): derive http status from a single error taxonomy`
**Effort:** M · **Depends on:** PR-03
**Why:** The code→category grouping is maintained three times (`ErrorCode::kind()` at `crates/loonfs-api/src/error.rs:129-169`, the 38-arm status match at `crates/loonfs-server/src/http.rs:1636-1682`, and the api.md table). `ErrorKind` has zero production consumers and has already drifted (`StaleRevision` → `PreconditionFailed` kind, but served 409).
**Do:**
- Reconcile kinds to currently-served statuses (statuses are the tested, spec-locked truth): `StaleRevision` joins the Conflict kind; `CommitOutcomeUnknown`'s kind maps to 503; audit all 38.
- Replace the per-code server match with one small `kind → StatusCode` function; keep the spec-table test — it now transitively pins `kind()`.
- Give `ClientError` typed access: `error_code() -> Option<ErrorCode>` and `kind()` (unknown codes fall back by HTTP status class), making the client the reference consumer of the registry's retry semantics. `ApiError::error_code()` (`crates/loonfs-api/src/http.rs:28-31`) finally gets a caller.
- Apply the `#[non_exhaustive]` rule: present on cross-crate public error enums (api, `CoreError`, `RuntimeError`, `ClientError`, `ObjectStoreError`), removed from internal per-op enums.
- Replace the hand-maintained `ErrorCode::ALL` triple-entry ritual (`error.rs:88-127`) with a macro that emits variant + string + list together.
**Done when:** one grouping exists; the spec test still passes with zero api.md status changes; `ErrorCode::ALL` cannot drift from the variants.

### PR-08 · `conor/cli-error-registry`
**Title:** `refactor(cli): map errors through the shared error-code registry`
**Effort:** S–M · **Depends on:** PR-07
**Why:** The CLI re-derives the server's error mappings with raw string literals (`crates/loonfs-cli/src/backend.rs:503-544` vs `crates/loonfs-server/src/http.rs:1554-1615`, including a byte-identical 404 message), and embedded vs remote modes emit *different codes for the same error* (`invalid_input` vs `invalid_commit_id`).
**Do:**
- Add `code() -> ErrorCode` to `BootstrapNamespaceError` (and any core error the CLI matches on), mirroring `CoreError::code()`.
- Collapse both mapping sites to generic code+message conversion; CLI uses `ErrorCode::as_str()`, never string literals for registry codes.
- Remove the embedded-mode rewrite at `backend.rs:503-509` so both modes surface identical codes; add a parity test.
- Keep CLI-only codes (`profile_not_found`, `invalid_config`, …) as a documented, distinct set in `crates/loonfs-cli/src/error.rs`; use the existing `CliError::io()` helper at `backend.rs:210`.
- Drop the now-unneeded `loonfs-core` dev-dependency from loonfs-cli (only consumer is one mapping test, `backend.rs:677`).
**Done when:** `loon --json` emits the same `code` for the same failure in embedded and remote modes (tested); no registry code appears as a string literal in loonfs-cli.

### PR-09 · `conor/error-style-sweep`
**Title:** `refactor(core): standardize error sources, context, and naming`
**Effort:** M · **Depends on:** PR-03, PR-07
**Why:** Remaining error-shape drift: `#[from]` vs manual `From` vs transparent-without-from in three combinations; store/codec wrappers inconsistently carry `object_key`; variant names diverge from the frozen wire codes; two `Result` alias schemes.
**Do:**
- `#[from]` wherever an inner type maps to exactly one variant (e.g. `CoreError::InvalidUploadId` at `error.rs:60`, the manual `From`s at `error.rs:217-239` — most fall out of PR-03; `WalChainLoadError` manual From at `frame.rs:179`); one-line comment where transparent-without-from is deliberate (shared inner types, e.g. `bootstrap.rs:39-46`).
- Every store/codec wrapper variant carries `object_key` (`ControlObjectLoadError::Store` at `control.rs:137`, `ObjectStoreError::NotFound`/`Transport` at `object_store.rs:59-74`, `CoreError::Store`/`WalWrite`). Ban bare `#[error("{0}")]` on String payloads (`error.rs:83`, `content.rs:85`) — always a prefix.
- Rename variants to mirror wire codes: `MissingPath`→`PathNotFound`, `MissingRevision`→`RevisionNotFound`, etc.
- Document the rule that core errors are `Clone + Serialize` so foreign causes become prefixed message strings; in unconstrained crates (client, server config) use `#[source]`.
- Result alias canon: the facade pattern (`pub type Result<T>` + used everywhere) — adopt in core (124 signatures currently spell it out) or delete core's unused alias; pick `pub use X as Error` OR `pub type Error = X`, once.
- Honest errors in upper layers: replace the fabricated `CoreError::Store("publisher task stopped")` (`crates/loonfs-server/src/publisher.rs:192`) and the handler-minted `CoreError::InvalidUploadContent` (`http.rs:979-984`) with proper publisher/`ApiResponseError` variants.
- Messages on every `unreachable!` (`profile_config.rs:128,331`); give `serve()` a real `ServeError` instead of `Result<(), String>` (`http.rs:260`).
- Dedupe near-identical variants where cheap (`MissingEtag`/`MissingHeadEtag` ×3); rename the private `ManifestLoadErrorKind` to avoid colliding with api's `ErrorKind` concept.
**Done when:** grep finds no `#[error("{0}")]` with String payload, no manual `From` impls thiserror could derive, no `MissingX` variants; clippy green.

---

## Wave 3 — observability (DEFERRED — skip in round 1)

### PR-10 · `conor/telemetry-foundation`
**Title:** `feat(server): default-on tracing with startup and shutdown logging`
**Effort:** M · **Depends on:** —
**Why:** The subscriber is opt-in via undocumented `LOONFS_TRACE=json` (`crates/loonfs-server/src/trace.rs:26-31`), so a default production server emits nothing — no "listening on", no request record, no error on a 500 — and the CLI/client can't emit at all.
**Do:**
- Server: compact stderr subscriber at `info` by default; `LOONFS_TRACE=json|pretty|compact` selects format (json currently the only mode, `trace.rs:52`); `RUST_LOG` overrides. Add `loonfs_objectstore` to the default filter (`trace.rs:9`).
- Startup `info!` (bind addr, store kind, writer id, version — the redacted config Debug exists for this) and shutdown `info!`; add SIGINT/SIGTERM graceful shutdown via `axum::serve(...).with_graceful_shutdown` (tokio `signal` feature already enabled).
- CLI: `-v/-vv` global flags → stderr subscriber at info/debug (stdout stays machine-stable). `tracing-subscriber` is already a workspace dep.
- Document `LOONFS_TRACE` and `LOONFS_OBJECT_STORE_METRICS_JSONL` in the server example configs.
- Write the span/field taxonomy doc (span names, `phase` values, `key_class` values, standard fields: `namespace_id`, `seq`, `operation`, `store_kind`, `object_key`, `elapsed_ms`, `error`) as `docs/observability.md`; dedupe the two reused `phase` labels (`project_metadata_state`, `project_manifest`) and rename core's `publisher.*` spans to `commit.*` (`protocol.rs:857+`) so "publisher" means only the server/runtime component.
**Done when:** `loonfs-server --config …` with no env vars prints a startup line and per-request output; `loon -v ls /` shows spans; Ctrl-C shuts down cleanly.

### PR-11 · `conor/durability-failure-events`
**Title:** `feat(core): emit errors and identifiers on durability failure paths`
**Effort:** M · **Depends on:** PR-10; land after PR-14 if both are pending (same file regions)
**Why:** The workspace contains zero `error!`/`warn!` macros. WAL-write and head-CAS failures record `result="error"` on a span and discard the error, key, and namespace (`crates/loonfs-core/src/protocol.rs:1011-1021`); a publisher panic fails all in-flight waiters silently (`PublishAbortGuard`, `crates/loonfs-server/src/publisher.rs:700-732`).
**Do:**
- `error!(namespace_id, object_key, %error)` at the origin of WAL-write, head-CAS, checkpoint-publish, and lease failures; `warn!` on CAS retries/stale-head races and publisher delete failures (`publisher.rs:540-547`); `error!` in the abort guard (namespace, orphaned-waiter count).
- Add `namespace_id` (and `seq`/`commit_id` where in scope) as span/event fields on every per-namespace operation — publisher events have it in scope and omit it (`publisher.rs:361-370`); keep ids out of metric label sets (cardinality stance is correct for metrics only).
- Fix the two `enter()`-held-across-`.await` sites (`protocol.rs:999-1001`, `1058-1061`) → `.instrument()` (correct pattern 20 lines earlier at `:972`).
- Demote the per-candidate `info!` firehose (`publisher.rs:444-453`, `624-633`) to `debug!`; keep one per-batch `info!` summary.
**Done when:** killing the store mid-commit in a test produces an `error!` with namespace + key; steady-state writes emit ≤1 info line per batch.

### PR-12 · `conor/request-and-store-visibility`
**Title:** `feat(server): request middleware, auth layer, and object-store telemetry`
**Effort:** M · **Depends on:** PR-10
**Why:** No HTTP request lifecycle exists — the router has no layers (`http.rs:126-196`), `ApiResponseError::into_response` logs nothing (`http.rs:1684-1688`), and only PUT-class ops get a span. Separately, the bearer check is non-constant-time string equality repeated in all 21 handlers (`http.rs:1430`), and the object-store layer is fully dark (providers have zero tracing; the metrics recorder only writes JSONL to an opt-in file and skips `list_prefix_stream`, `metrics.rs:313-318`).
**Do:**
- Request-span middleware: method, route template, namespace, status, `elapsed_ms`; `error!` on 5xx / `warn!` on 4xx at `ApiResponseError` construction.
- Auth middleware on the router with an explicit `/health` allowlist, constant-time comparison (`subtle`), no per-request `format!` allocation; delete the 21 `authorize(...)` call sites.
- Default tracing-backed `ObjectStoreMetricsRecorder` (debug-level per-op event; `warn!` on transport errors); instrument `list_prefix_stream`; keep the JSONL recorder as an option.
**Done when:** a forced 500 in a test leaves a server-side error event with route + namespace + status; a route added without auth is caught by a test asserting the middleware covers everything except the allowlist.

### PR-13 · `conor/instrument-recovery-paths`
**Title:** `feat(core): instrument recovery, retention, and remaining facade operations`
**Effort:** M · **Depends on:** PR-10
**Why:** The code that runs when something already went wrong — WAL replay, writer-epoch acquire, bootstrap, fork, delete, retention — has zero instrumentation (`wal/replay.rs`, `namespace/*.rs`, `checkpoint/retention.rs`), and the facade instruments only 5 of ~44 public ops (`loon.stat` exists; `loon.get`/`loon.list`/`loon.delete` do not).
**Do:**
- Outcome `info!` events + `loon.phase` spans: replay (segments/records/final seq), writer-epoch acquire (epoch, contended?), namespace create/fork/delete, retention advance (floor before/after, objects deleted — it's irreversible; it must leave a record).
- Extend the existing `loon.<op>` root-span template (copy `stat_path`/`put_file_bytes`, `crates/loonfs/src/fs.rs:402,675`) to all public facade ops.
- Delete the orphaned `#[instrument]` on the `#[cfg(test)]`-only loader (`checkpoint/load.rs:217-225`).
**Done when:** every public `Fs` method carries a `loon.*` span (add the forcing-function test suggested by the existing spec-table pattern); recovery paths emit outcome events in the fault tests.

---

## Wave 4 — core deduplication and structure

Order within this wave matters: 14 → 15 → 16 → 17 (moves before rewrites; validators before visibility).

### PR-14 · `conor/split-protocol-module`
**Title:** `refactor(core): split protocol.rs into focused modules`
**Effort:** M · **Depends on:** —
**Why:** `crates/loonfs-core/src/protocol.rs` (1,572 non-test lines) is five modules in a trenchcoat: upload sessions (:197-488), publish-view loading (:609-822), the 273-line batch publisher (:824-1096), candidate prep/idempotency (:1099-1282), change feed (:1284+), WAL→API delta conversion (:1376+).
**Do:**
- Pure moves into `protocol/{uploads,publish_view,batch,changes}.rs`; `protocol/mod.rs` = doc + re-exports (house style: `checkpoint/mod.rs`).
- Extract `abort_batch(...)` — the identical 3-line failure exit is repeated verbatim at :1026, :1044, :1074.
- `prepare_candidate_request` returns `Result<Option<CandidateCoreRequest>>` instead of writing through three `&mut` out-params; batch dedup state becomes a small struct.
- Publish-view carries a non-optional `AcquiredWriter` (kills the ×4 repeated `.expect("publish view should carry acquired writer")`).
- Delete the pure rename-shim `publish_namespace_mutations_batch` (:555) — one function, one name.
- Wrap the inline `let _span = ….entered(); match …` phase blocks (:912-960) in `#[instrument]`ed helper fns (sets up PR-11).
**Done when:** no file in `protocol/` exceeds ~500 lines; no function exceeds ~120; behavior-neutral (existing tests unchanged and green).

### PR-15 · `conor/unify-commit-validators`
**Title:** `refactor(core): collapse duplicate commit validators`
**Effort:** M · **Depends on:** PR-14 (rebase only)
**Why:** `validate_metadata_preconditions` (310 lines, `commit/validate.rs:290`) and `validate_publish_metadata_preconditions` (277 lines, `:601`) are the same function twice — only view construction and error plumbing differ — as are the two `build_commit_plan` variants (`:94`, `:165`). Twelve helpers differ only in which error variant they return; 19 helpers take an ignored `_base_seq`.
**Do:**
- Keep only the generic `MetadataView`-based validator; the in-memory path constructs an `InMemoryMetadataView` and converts errors once at the boundary via the existing `commit_validation_from_core`. Same collapse for the plan builders.
- One parameterized helper per check shape taking an error-constructor closure (tombstone-coverage ×6, inode-kind ×2, name-absent ×2).
- Delete the 19 `_base_seq` params and their call-site threading.
- Fold the facade's lone pre-flight duplicate (`validate_runtime_mutation_path`, `crates/loonfs/src/fs.rs:1205`) into a named core helper the facade calls, with a comment explaining it avoids orphaned content writes.
**Done when:** `validate.rs` is ~700 lines with one validator; a rule change (add a precondition in one place) demonstrably flows to both entry paths via the existing mutation-guard tests.

### PR-16 · `conor/split-metadata-state`
**Title:** `refactor(core): split metadata state into rows, apply, and queries`
**Effort:** M · **Depends on:** PR-15 (rebase only)
**Why:** `metadata/mod.rs` is a 1,951-line implementation file in a codebase whose house style is curated re-export mod.rs (`checkpoint/mod.rs`); its seq-gated query API repeats the `if base_seq >= indexed_seq { at_head } else { scan }` guard ~12 times with two fully-duplicated 20-line ancestor walks.
**Do:**
- Move `MetadataState` into `metadata/state.rs` split as `rows.rs` (records + accounting), `apply.rs` (WAL application), `queries.rs` (seq-gated reads); `mod.rs` = doc + re-exports. Move the 706 inline test lines to `metadata/tests.rs`; same for `wal/mod.rs`'s 450-line inline test mod.
- Collapse each at-seq/at-head duplicated traversal pair by parameterizing on a lookup closure (`covering_subtree_tombstone` at `:670` vs `:699` is the worst).
**Done when:** `metadata/mod.rs` < 60 lines; no duplicated traversal bodies; pure refactor, tests unchanged.

### PR-17 · `conor/single-visibility-implementation`
**Title:** `refactor(core): single implementation of visibility semantics`
**Effort:** L · **Depends on:** PR-15, PR-16
**Why:** The most safety-critical rule in the system — "is this direntry visible" — exists four times with copies identical down to variable names (`metadata/state` sync version, `view.rs:314`, `view.rs:902` cached session, `path/write/planner.rs:273`), and `resolve_visible_path` three times. A maintainer fixing a visibility bug cannot know they found all copies.
**Do:**
- One binding-identity helper on the record type (`DirentryBindRecord::is_same_binding(&other)`) used by every check including the planner.
- Make `MetadataViewSession` the single composite-visibility implementation; uncached callers construct a session (caching is harmless). The sync `MetadataState` versions may remain for at-head index primitives only — composite rules (visible_child, resolve_visible_path) live once.
- The model's copy (`loonfs-model/src/metadata.rs:404`) is an *intentional* independent implementation — add a doc comment saying so, so nobody "deduplicates" the oracle.
**Done when:** grep for the 5-condition binding comparison finds one implementation + the documented model mirror; full mutation-guard + differential suites green.

### PR-18 · `conor/read-context-parameter`
**Title:** `refactor(core): thread read context as a parameter`
**Effort:** M · **Depends on:** —
**Why:** Every one of `NamespaceEngine`'s 10 read ops exists three times (`X`, `#[doc(hidden)] X_with_runtime_context`, private `X_with_context` — `engine.rs:186-517`, ~30 bodies) purely to thread the facade's caches; the "public" API and the API actually used are parallel surfaces. The facade doubles methods the same way (`begin_upload_with_request`, `list_changes_after_with_limit`).
**Do:**
- One method set taking `ReadContext` (with `ReadContext::latest()` for the plain case), or engine-owned optional cache handles at construction — pick whichever deletes more code; kill the `#[doc(hidden)]` shadow family and the hidden re-exports (`lib.rs:82-83`).
- Facade canon is options structs: fold `limit` into `ListChangesOptions`, the request into `begin_upload(options)`.
**Done when:** `engine.rs` has one public read surface; no `#[doc(hidden)]` items remain in loonfs-core's API.

---

## Wave 5 — cross-crate structure

### PR-19 · `conor/shared-provider-config`
**Title:** `refactor(objectstore): shared provider config with redacted secrets`
**Effort:** M–L · **Depends on:** —
**Why:** Server and CLI each define the same 5-variant `StoreConfig` enum + ~70-line `object_store()` constructor + verbatim `trace_store_kind()` (server `config.rs:42-77,221-303`; cli `config.rs:53-97,239-305`), already drifted on redaction. Adding a provider touches ≥6 sites in 4 crates. Secret handling has three Debug policies and echoing prompts.
**Do:**
- One serde-ready `StoreConfig` in `loonfs-objectstore` next to `ConfiguredObjectStore`: kind-tagged, kebab-case, one fallible constructor, one validation walk; both binaries consume it. Derive the trace label from `ConfiguredObjectStoreKind` (delete `TraceStoreKind` duplication).
- `Secret<String>` newtype (Debug/Display print `<redacted>`; `expose()` for use) for every secret field — provider configs, `content_token_secret`, `auth_token`. Every config type derives Debug (ServerConfig currently can't be logged at all); keep the existing redaction test, now generic.
- Masked input (`prompt_secret_keep_current`, already exists at `crates/loonfs-cli/src/prompt.rs:19`) for **all** secret prompts — AWS/R2 currently echo and even display the current secret as the default (`profile_config.rs:646-673`); Azure alone is masked.
- `#[serde(deny_unknown_fields)]` on config-file structs (test the tagged-enum interaction); add a commented `[runtime_cache]` block to one server example (8 override fields are documented nowhere).
- Env fallbacks for server secrets: `LOONFS_AUTH_TOKEN`, `LOONFS_CONTENT_TOKEN_SECRET`; steer `examples/SKILL.md` away from secrets-as-flags.
- Data-drive the CLI provider-flag rejection matrix — `build_embedded_profile` (:132) and `apply_update_flags` (:413) hand-write 64 `reject_*` calls; replace with one `[(flag, accessor, allowed_kinds)]` table used by both.
**Done when:** adding a hypothetical provider touches loonfs-objectstore + one CLI table row + one server match arm; `{:?}` on any config type shows no secret (test); all secret prompts are masked.

### PR-20 · `conor/split-http-module`
**Title:** `refactor(server): split http.rs by resource and drop handler suffixes`
**Effort:** M · **Depends on:** — (PR-12 is deferred with wave 3: keep the per-handler `authorize(...)` calls as-is; the namespace-parse extractor still applies)
**Why:** One 2,390-line file holds the whole HTTP surface (30% inline tests); half the handlers are named `create_namespace`, half `delete_namespace_handler` — same file, same router.
**Do:**
- Mechanical split: `http/{handlers_namespace,handlers_filesystem,handlers_uploads,error,openapi}.rs`, tests to `http/tests.rs`; router table stays in one place.
- Drop the `_handler` suffix everywhere (~10 fns); an axum extractor for `parse_namespace_id` removes the 18 repeated parse+map lines (auth stays as per-handler `authorize(...)` calls until wave 3 lands).
**Done when:** no `_handler` names; no http file > ~600 lines; route table diff-clean.

### PR-21 · `conor/complete-facade-seam`
**Title:** `refactor(runtime): complete the facade seam so the server stops importing core`
**Effort:** S–M · **Depends on:** —
**Why:** `loonfs::publish` exists as a deliberate server-integration seam (`crates/loonfs/src/lib.rs:43-46`) but is incomplete, so loonfs-server also depends on loonfs-core directly (`publisher.rs:5-6`, `http.rs:30-32` for commit identity + content tokens) — core-internal renames break the server even when the facade is stable. Small response types are also copied field-by-field in two places.
**Do:**
- Widen the seam: re-export (or move) `SemanticMutationIdentity`, `CommitHeadPublishError`, and the content-token mint/verify surface through `loonfs`; drop `loonfs-core` from `loonfs-server/Cargo.toml`.
- `Fs::namespace_status` returns `loonfs_api::NamespaceStatusResponse` directly — delete the six-field copies in `http.rs:505-512` and `backend.rs:277-284` and the redundant intermediate struct; apply the same test to `MaintenanceTickResult`.
- Unify on one `SharedObjectStore` alias (drop the redundant `+ Send + Sync` spelling in `loonfs/src/lib.rs:68`).
- CI guard: a check that `loonfs-server` does not depend on `loonfs-core` (cargo-deny bans or a small script).
**Done when:** `cargo tree -p loonfs-server` shows loonfs-core only via loonfs; the CI guard fails if the dep returns.

### PR-22 · `conor/client-dead-code`
**Title:** `refactor(client): remove dead transfer helpers and take typed ids`
**Effort:** S · **Depends on:** —
**Why:** `Client::get_to_path`/`put_from_path`/`get_directory` (+`GetPathResult`/`PutPathResult`, ~150 lines, `lib.rs:803-908`) have zero callers — the CLI implements those flows itself — and the `walkdir` dependency exists solely for them. Client methods also take raw `&str` where the facade takes typed ids.
**Do:** Delete the helpers and the walkdir dep; switch client method params to `&NamespaceId`/typed ids to match `Fs`.
**Done when:** no unused public API in loonfs-client; `cargo udeps`-style check (or manual) shows no orphan deps.

### PR-23 · `conor/surface-parity`
**Title:** `feat(cli): admin plane parity across surfaces`
**Effort:** L · **Depends on:** PR-22
**Why:** The server exposes `/v0/admin/…` and `/changes`, but loonfs-client has zero checkpoint/retention/changes methods and the CLI no admin commands — advertised profiles are unreachable from two of three surfaces. The embedded-vs-remote `Backend` trait that solves this is trapped `pub(crate)` in the CLI (`backend.rs:20-70`).
**Note:** `loon namespace list` is explicitly OUT of scope (decision 7) — do not add a listing route, command, or capability key.
**Do:**
- Promote `Backend` (trimmed) out of the CLI — into loonfs-client as the "one logical API, two transports" seam.
- Client methods for checkpoint/retention/changes/status; CLI `loon admin checkpoint|retention` and `loon changes` wired through `Backend` so embedded and remote both work.
- Follow the CLI's existing render/exit-code conventions; `--json` support like every other command.
**Done when:** every feature key advertised in the capability document except namespace listing is exercisable from CLI in both modes (add a conformance test iterating the registry, with the deferred listing feature explicitly excluded).

### PR-24 · `conor/publisher-into-runtime`
**Title:** `refactor(runtime): move the batching publisher into the runtime crate`
**Effort:** L · **Depends on:** PR-21; do last in this wave. **Decision-gated — confirm before starting.**
**Why:** `NamespacePublisher`/`PublisherRegistry` (coalescing, commit-id idempotency dedup, delete-as-barrier, CAS pacing — `crates/loonfs-server/src/publisher.rs`, 810 code lines) is generic multi-writer machinery with no HTTP dependency, yet embedded hosts (the README's "many writers" pitch) get none of it — embedded submissions serialize on a mutex with no coalescing or dedup.
**Do:**
- Move publisher + its 960 test lines into `loonfs` (e.g. `loonfs::publisher`); server keeps construction + config wiring. Embedded `Fs` write paths route through it (this is the point — decide whether `Fs` always owns a registry or exposes it opt-in; prefer always-on for one code path).
- Re-home the queue telemetry consistently with the PR-10 taxonomy (`publisher.*` spans now genuinely mean the publisher).
- Revisit the PR-21 seam: re-exports that existed only for the server's publisher can now be deleted.
**Done when:** embedded and server writes share one concurrency front-end; a two-writer embedded test shows coalescing/dedup behavior that previously only the server had.

---

## Wave 6 — naming canon sweep

### PR-25 · `conor/naming-canon`
**Title:** `refactor: apply naming canon across the workspace`
**Effort:** M · **Depends on:** waves 4–5 (renames after moves = clean diffs)
**Why:** Residual pattern-A/pattern-B drift a style guide must settle once.
**Do:**
- Builder setters: bare field names — `NamespaceEngineBuilder::writer` → `writer_id` (`engine.rs:843`); drop the lone `with_metrics_recorder` prefix (`fs.rs:146`).
- `Dir` → `Directory` in Rust names (`InodeKind::Dir`, `create_dir`, `CreateDirOptions` → align with `CommitOp::CreateDirectory`); serialized values (`"dir"`) untouched — assert via golden fixtures.
- Parameter naming: `namespace_id: &NamespaceId` everywhere (166/49/17/12 split today).
- CLI dispatch: one scheme for `run_*` functions (`commands/mod.rs` currently mixes three).
- File renames for grep-ambiguity: `loonfs-core/src/publisher.rs` → `commit_engine.rs` (matches its type), `loonfs-objectstore/src/fs.rs` → `local_fs_store.rs`, disambiguate the three `trace.rs` (sim's is `sim_trace`).
- loonfs-api module homes: all v0 HTTP shapes under `v0` (today split across private `http.rs`, `server.rs`, `v0.rs` with flat re-exports — `CreateNamespaceRequest` vs `v0::CommitRequest`); `//!` docs on each file stating what belongs there. Curated re-export style for loonfs-sim's root (currently double-pathed).
- Boundary types: `lease_duration_ms: u64` → `Duration` at API boundaries; unify `PutFileOptions.behavior` vs core `put_behavior` field naming; rename the reused `FilesystemMoveArgs` for `cp`; fix human render printing Rust Debug of `InodeKind` (`render.rs:155,163`).
**Done when:** each bullet's grep is clean; golden fixtures byte-identical (proves no wire leakage).

---

## Wave 7 — tests (DEFERRED — skip in round 1; PR-26 must NOT touch loonfs-sim, see decision 5)

### PR-26 · `conor/shared-test-support`
**Title:** `test: shared test-support crate for fixtures and fault stores`
**Effort:** L (test-only risk) · **Depends on:** PR-17 (avoid churn)
**Why:** Five big suites hand-roll 13 fault-injection stores — including two same-named-but-diverged `HeadCasFailureStore`s (`runtime.rs:2245` vs `checkpoint/tests.rs:2665`) — three `block_on` implementations, and two `page_limit` helpers with different semantics.
**Note:** `loonfs-sim` is an external contract (decision 5) — the support crate must NOT depend on, modify, or duplicate-into it. Consolidate the ad-hoc wrappers into the support crate on their own terms.
**Do:**
- New `crates/loonfs-test-support` (`publish = false`): one configurable fault-injecting store covering what the 13 wrappers do (lost puts/acks, stale reads/CAS, get counting, access limits); namespace/engine bootstrap builders; the single `block_on`; `page_limit`; the `FsTestExt` blocking trait.
- Delete all 13 ad-hoc wrappers, migrating their suites.
- Async idiom canon: bare `#[tokio::test]` unless multi-thread is semantically required (comment why when it is); fix the mixed idioms inside `http_smoke.rs`.
- Split `mutation_guards.rs` (5,607 lines, helpers at both ends) into `tests/mutation_guards/{main,harness,commit_validation,uploads,query_reads,batch_publish,forks,restore,path_ops}.rs` — one binary, navigable domains.
- Scope the three file-wide `#![allow(clippy::disallowed_methods)]` escapes (`http_smoke.rs:1`, `cli.rs:1`, `direct_put_real_provider.rs:1`) down to the individual polling functions (house pattern: `objectstore_conformance.rs:705`).
- Fix the CLI suite's implicit server-binary dependency (`cli.rs:992` probes `target/debug/loonfs-server`): add loonfs-server as a dev-dependency so `CARGO_BIN_EXE_loonfs-server` works, or document the build prerequisite in CONTRIBUTING.
- Remove sim's unused `loonfs-core` dependency if still unused after adoption.
**Done when:** zero `impl ObjectStore` in test files outside the support crate/sim; `cargo test -p loonfs-cli` works from a clean checkout (or the prerequisite is documented); sim has real consumers.

### PR-27 · `conor/seeded-differential`
**Title:** `test(core): seeded differential exploration against the reference model`
**Effort:** M · **Depends on:** PR-26 · **Optional — highest-leverage use of the sim crate; cut if schedule demands.**
**Why:** The differential suite replays 6 hand-written scenarios; the model oracle has one self-test. Sim's `DeterministicRng`/`ReplaySeed` were built for exactly this and are unused.
**Do:** Seeded generator of random valid op sequences (create/write/move/delete/tombstone/restore mixes) replayed through `loonfs_model::MetadataState` and core after every publish; normalize + compare; print the failing seed; a fixed-seed corpus runs in CI, an env var widens the search locally. Add model self-tests for the semantics the generator exercises.
**Done when:** a deliberately-introduced visibility bug in core (mutation test) is caught by the differential run within the CI seed corpus.

### PR-28 · `conor/client-integration`
**Title:** `test(client): direct integration coverage against the in-process server`
**Effort:** S–M · **Depends on:** PR-23
**Why:** loonfs-client's only tests are six config-parsing cases; request construction, decoding, pagination, and error mapping are covered only as a side effect of other crates' suites.
**Do:** `crates/loonfs-client/tests/` suite driving `Client` against `loonfs_server::app` on an ephemeral port: happy paths per method family, error-code decoding (now typed via PR-07), pagination aggregation, auth failure.
**Done when:** a client-only regression (e.g. wrong path template) fails in the client's own test binary.

---

## Wave 8 — CI, docs, release (DEFERRED — skip in round 1)

### PR-29 · `conor/harden-pipeline`
**Title:** `ci: pin toolchain, verify msrv and macos, gate releases on all targets`
**Effort:** M · **Depends on:** —
**Why:** CI is fmt/clippy/test on floating stable, Linux-only, without `--locked`; new-stable clippy will break unrelated PRs (`-D warnings`); macOS breaks surface at release time; the release workflow publishes even if 3 of 4 target builds fail (`if: always()` + "at least one artifact"); the live-provider conformance suites are never executed anywhere.
**Do:**
- Pin the CI toolchain (from `rust-toolchain.toml`, itself pinned to a specific stable); add `--locked` everywhere; MSRV job (`cargo +1.83 check --all`); macOS test job; `concurrency:` group cancelling superseded runs; `cargo doc --no-deps` with warnings denied; cargo-deny (advisories + licenses + the loonfs-server→core ban from PR-21).
- Release: publish requires `needs.build.result == 'success'` for all four targets; decide and state the `loonfs-server` distribution story (artifact, or an explicit "built from source" README note).
- Weekly scheduled, secret-gated workflow running `cargo test -p loonfs-objectstore -- --ignored` per configured provider (start with S3 + R2), plus `direct_put_real_provider`.
**Done when:** a PR with a lockfile drift, an MSRV break, or a macOS-only break fails CI; a single failed target build blocks release publish.

### PR-30 · `conor/contributing-and-cli-polish`
**Title:** `docs: contributing guide, style guide, and cli reference`
**Effort:** M · **Depends on:** best last (it codifies the canons the earlier PRs establish)
**Why:** CONTRIBUTING.md is two lines; the conventions this plan enforces exist nowhere in writing; `--help` has real text on exactly two flags; neither binary supports `--version`; the CLI README and agent docs have drifted.
**Do:**
- CONTRIBUTING.md: crate map + which crates are spec-locked; test tiers (inline units / sibling `tests.rs` for private-internals suites / `tests/` integration / golden fixtures / insta / `#[ignore]`d live suites + `provider-conformance.env.example`); determinism rules (the five clippy bans, named escape-hatch pattern, function-scoped allows); spec-locked artifact workflow (never hand-edit `openapi.json` — regen command; api.md/format.md tables are test-enforced; golden changes = format break); naming/test-naming conventions; build prerequisites.
- STYLE.md (or a CONTRIBUTING section): the canons — constructors (`parse`/`new -> Result`/builders), error shape (thiserror, `#[from]` rule, context fields, non_exhaustive rule), serde policy (tag `kind`, tolerant wire decoding, strict config decoding), observability policy (from `docs/observability.md`), mod.rs = docs + re-exports, options structs over `_with_X`.
- CLI: `#[command(version, about)]` on both binaries (`args.rs:4`, server main); `///` help on every arg/subcommand; regenerate `crates/loonfs-cli/README.md` from the now-authoritative help (add `namespace delete/fork`, the gcs/azure provider flags) or snapshot-test rendered help.
- One confirmation policy for destructive commands: `--yes` flag + interactive prompt + clear `non_interactive_input_required` error (namespace delete currently panics under `--no-input`, `commands/namespace.rs:80`; profile remove silently proceeds, `profile.rs:143`).
- Doc fixes: README quickstart `--release` copy; `examples/SKILL.md` revisions columns (no timestamp exists); one canonical install URL; workspace keywords (`"r2"` leftover); explicit `publish` set + per-crate descriptions in Cargo.tomls; rename root `examples/` (agent skills, not Rust examples) to `skills/`; PR template + CODEOWNERS stubs.
- `#![warn(missing_docs)]` on `loonfs-api` + `loonfs-objectstore` with backfill (the two vocabulary crates; ~53% and ~11% today — the `ObjectStore` trait itself is undocumented). Core/client backfill can be a follow-up wave; state the policy either way.
**Done when:** a new maintainer can go from clone to green tests to first PR using only in-repo docs; `loon --version` works; help snapshot test pins the reference.

---

## Round 1 execution order (stacked branch series)

One stack, each branch based on the previous, in this order:

**01 → 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09 → 14 → 15 → 16 → 17 → 18 → 19 → 21 → 22 → 20 → 23 → 24 → 25**

(Waves 3, 7, 8 — PRs 10–13 and 26–30 — are deferred to a later round.)

Round 1 total: 21 PRs — 8 S, 10 M, 3 L. The three L items (17 visibility singleton, 23 admin parity, 24 publisher relocation) are the ones to schedule deliberately; everything else is a focused half-day-or-less change for an implementing agent.
