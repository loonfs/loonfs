# Spec 080: repository and delivery plan

This repository is intentionally scaffolded around **large bodies of work** rather than around one giant application crate.

## Why the workspace is split

- `loon-types` isolates shared vocabulary
- `loon-objectstore` isolates provider assumptions
- `loon-core` owns canonical metadata rules
- `loon-model` owns pure semantics for tests
- `loon-queue` isolates rebuildable background coordination
- `loon-sim` owns determinism and failure injection

This split is not about micro-crates for their own sake. It is about review boundaries and test boundaries.

## Expected workflow

A team should be able to pick one workstream at a time:

- provider contract
- core metadata rules
- model/simulation
- server shell
- client shell

Current delivery order:

- implement the semantic core before widening shells and adapters
- treat review boundaries inside crates as equally important as crate boundaries
- prefer deleting or quarantining placeholder surfaces over expanding them
- use `docs/roadmap/020-semantic-core-reset.md` as the current execution-order document

Current real delivery surfaces:

- `loon-core`
- `loon-model`
- `loon-objectstore`
- `loon-ops`
- `loon-server::mutation`
- `loon-server::ops`
- `loon-client`
- `loon-macos` Rust bridge and native interop surface
- `native/macos/LoonFileProviderSample` developer sample shell
- `loon-testkit`
- `xtask`

Current operator-facing recovery shell:

- `xtask conflict-list <namespace_id> --db <path> --store-root <path>`
- `xtask conflict-list <namespace_id> --db <path> --store-root <path> --all`
- `xtask conflict-list <namespace_id> --db <path> --store-root <path> --archived`
- `xtask conflict-show <namespace_id> <conflict_id> --db <path> --store-root <path>`
- `xtask conflict-restore <namespace_id> <conflict_id> --db <path> --store-root <path> --to <path>`
- `xtask conflict-archive <namespace_id> <conflict_id> --db <path> --store-root <path>`
- `xtask conflict-unarchive <namespace_id> <conflict_id> --db <path> --store-root <path>`

Constraints for that shell:

- it requires an existing client SQLite DB path and an existing local-fs object-store root
- it may refresh only the local `conflict_artifacts` and `conflict_artifact_archives` caches
  during discovery
- restore is out-of-band and does not rebind recovered content into sync state
- archive state is canonical in object storage via per-artifact sidecars
- there is still no destructive delete/GC lifecycle

Current local operability shell:

- shared command/config/rendering layer in `loon-ops`
- thin active frontends in `xtask ops ...` and `loon ops ...`
- current commands:
  - `xtask ops bootstrap-namespace --config <path> --namespace <id> [--allow-existing]`
  - `xtask ops show-namespace-state --config <path> --namespace <id>`
  - `xtask ops show-client-state --config <path> --namespace <id>`
  - `xtask ops import-remote-observations --config <path> --namespace <id>`
  - `xtask ops observe-local --config <path> --namespace <id> --path <path>`
  - `xtask ops observe-delete --config <path> --namespace <id> --path <path>`
  - `xtask ops observe-move --config <path> --namespace <id> --from <path> --to <path>`
  - `xtask ops observe-subtree --config <path> --namespace <id> --path <path>`
  - `xtask ops sync-once --config <path> --namespace <id>`
  - `xtask ops sync-until-idle --config <path> --namespace <id> [--max-steps <n>]`
  - `xtask ops smoke --config <path> --namespace <id>`
  - `loon ops bootstrap-namespace --config <path> --namespace <id> [--allow-existing]`
  - `loon ops show-namespace-state --config <path> --namespace <id>`
  - `loon ops show-client-state --config <path> --namespace <id>`
  - `loon ops import-remote-observations --config <path> --namespace <id>`
  - `loon ops observe-local --config <path> --namespace <id> --path <path>`
  - `loon ops observe-delete --config <path> --namespace <id> --path <path>`
  - `loon ops observe-move --config <path> --namespace <id> --from <path> --to <path>`
  - `loon ops observe-subtree --config <path> --namespace <id> --path <path>`
  - `loon ops sync-once --config <path> --namespace <id>`
  - `loon ops sync-until-idle --config <path> --namespace <id> [--max-steps <n>]`
  - `loon ops smoke --config <path> --namespace <id>`
  - `loon config path [--config <path>]`
  - `loon config show [--config <path>]`
  - `loon config validate [--config <path>]`
  - `loon doctor [--config <path>]`
  - `loon completion <bash|zsh|fish|powershell|elvish>`
  - `loon manpages <output-dir>`
  - `loon version`

Constraints for that shell:

- `xtask` stays a thin wrapper; `loon-cli` also stays thin; config loading and command execution
  belong in `loon-ops`
- namespace bootstrap and authoritative-state inspection belong in supported library code,
  primarily `loon-server::ops`
- namespace bootstrap seeds the canonical root inode directly into the authoritative basis at
  `seq = 0`, and frontend code must treat that root as ordinary authoritative metadata rather than
  a synthetic sentinel
- `bootstrap-namespace --allow-existing` remains read-only and idempotent; lease renewal and
  takeover happen only on the authoritative mutation path
- the server mutation path must acquire or renew the namespace lease before authoritative basis
  loading:
  - same-holder unexpired renewal extends `lease_expires_at_ms` without rotating the fence token
  - any reacquire after expiry rotates `head.active_fence_token` before publishing a new lease,
    even for the same writer
  - an expired foreign-writer lease may be taken over by the next real write
- full-namespace authoritative remote observation import remains a supported library path in
  `loon-ops`, and `xtask ops import-remote-observations` must call that API verbatim rather than
  re-implementing it
- authoritative remote observation apply must restart plannable client state in the same client
  transaction for actionable outcomes, so imported remote-only placeholders do not rely on a
  separate shell-exposed planner tick
- `loon-client` owns local filesystem event normalization and path-based local observation routing
- local observation of one existing file remains a supported client/library path first, and
  `xtask ops observe-local` is only a thin adapter over that `loon-client` path
- explicit local delete and move observation remain supported client/library paths first, and
  `xtask ops observe-delete` / `xtask ops observe-move` are thin adapters over those
  `loon-client` paths
- recursive local subtree observation remains a supported client/library path first, and
  `xtask ops observe-subtree` is only a thin adapter over that `loon-client` path
- `observe-subtree` must restore same-path bound-directory reappearance as the existing bound
  inode, not as delete+create replacement
- `observe-subtree` must treat same-path file↔directory changes as one atomic delete+create
  replacement batch
- exact same-path bound replacement dependencies must be represented as planner-visible local-only
  waiting state and must wake once the one unique bound `delete_file`, `delete_subtree`, or
  `rename` really vacates that exact path
- unsupported descendant entries such as symlinks must be skipped and reported instead of aborting
  the whole subtree scan
- tracked paths at or under a skipped unsupported root must be excluded from delete inference in
  that scan
- any future watcher adapter must feed the generic `loon-client` event reducer rather than adding
  a second local observation path
- `xtask ops sync-once` is intentionally single-step and executor-only
- `xtask ops sync-until-idle` is only a thin loop over `sync-once`
- the supported bootstrap path is:
  - bootstrap namespace
  - import remote observations
  - sync once or until idle
  and a freshly bootstrapped namespace root must become a planned `materialize_remote_dir`
  placeholder immediately after import
- `loon ops ...` must preserve the same subcommand grammar and stdout rendering as `xtask ops ...`
- `loon-cli` may add CLI-only affordances such as help text, completions, manpage generation,
  config inspection, `doctor`, and version output, but it must not become a second owner of
  `loon-ops` config semantics, command behavior, or stdout rendering
- the first broader user-facing `loon-cli` surface may widen beyond `loon ops ...` only when the
  behavior still lives in shared library code rather than in the binary shell
- the first user-facing authoritative file slice is read/write:
  - `loon file ls <namespace:/path>`
  - `loon file stat <namespace:/path>`
  - `loon file get <namespace:/path> <local-path>`
  - `loon file get --recursive <namespace:/absolute/path> <local-path>`
  - `loon file cat <namespace:/path>`
  - `loon file put <local-file> <namespace:/absolute/path>`
  - `loon file put --replace <local-file> <namespace:/absolute/path>`
  - `loon file cp <from-namespace:/absolute/path> <to-namespace:/absolute/path>`
  - `loon file cp --replace <from-namespace:/absolute/path> <to-namespace:/absolute/path>`
  - `loon file mkdir <namespace:/absolute/path>`
  - `loon file rm [--recursive] <namespace:/absolute/path>`
  - `loon file mv <from-namespace:/absolute/path> <to-namespace:/absolute/path>`
- those product commands must read and write authority directly from object storage and verified
  namespace basis rather than wrapping client import/observe/sync flows
- product write commands must resolve selectors and commit inside one authoritative server-side
  mutation flow so selector resolution and commit validation use one coherent leased basis
- `loon ops ...` still requires explicit `--config`
- config discovery convenience is limited to `loon config ...` and `loon doctor`
- the shell is intentionally narrow in the current phase; there is still no watcher, and subtree
  move inference is restricted to:
  - unique digest-equal file pairs
  - unique exact-subtree directory pairs
- explicit `observe-move` remains the override for non-exact directory refactors
- `ops smoke` remains bootstrap/inspection-only and does not compose the import path yet
- stale writers are fenced by control-plane head rotation, not by a special shell recovery command
- broader future `loon-cli` work must continue to reuse the `loon-ops` command contract rather
  than fork it
- native filesystem object ids in the reducer are advisory within one batch only and are never
  durable truth

Current local RC path:

- `xtask rc-local --config <path> --namespace <id>`

Constraints for that path:

- it is repo automation owned by `xtask`, not part of the `loon-ops` command contract
- it runs the strict baseline:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`
  - `cargo test -p loon-objectstore --test conformance`
  - the existing `loon-ops` smoke command path
- real-provider validation remains documented/manual rather than auto-run by `rc-local`
- future `loon-cli` activation still reuses `loon-ops`; it does not inherit `rc-local`

Current delivery gates:

- object-store contract changes require the local FS conformance suite in-repo
- object-store contract changes also require the external AWS S3 and Cloudflare R2 conformance jobs
  documented in `docs/runbooks/provider-conformance.md`
- no provider CI workflow config lives in-repo; only the contract, path filters, commands, and env
  requirements are tracked here

Current quarantined delivery surfaces:

- the `loond` binary shell
- `loon-server` HTTP/app placeholders

These quarantined surfaces stay in the repository to preserve delivery intent and crate names, but
they should not advertise themselves as active product entrypoints until they wrap real behavior.

For `loon-cli`, the intended activation path is now active: it is a frontend over `loon-ops`, not
a second owner of config parsing, command grammar, or rendering semantics. The current checked-in
operator manual is `docs/runbooks/loon-cli.md`, and generated `loon manpages ...` output must stay
consistent with the active clap grammar.

That binary may now also grow a separate user-facing authoritative file surface, but only as a thin
frontend over shared library behavior. The product shell must not fork namespace semantics away
from the existing authoritative core.

## What should happen early

The repo should accumulate:

- more fixtures
- more invariants
- more model transitions
- better rendered traces

before it accumulates a large amount of production code.

## Current macOS File Provider spike boundary

The first macOS File Provider slice is now an active Rust bridge surface in `loon-macos`.

Rules:

- it is read-only
- it projects from existing client SQLite state plus local-only parent links
- it uses one account/root domain with namespaces as top-level directories
- it exposes a static-library C ABI with UTF-8 JSON payloads for the out-of-tree native sample
- it now includes an in-repo developer sample app and extension that call that C ABI
- the containing app owns domain registration; the extension owns enumeration, lookup, and targeted
  hydration
- File Provider item ids exposed to native code are opaque encoded bridge ids
- the native sample must still call the Rust bridge rather than re-implementing projection or
  hydration logic
- ordinary workspace validation must stay cargo-safe; Xcode validation is opt-in/manual
