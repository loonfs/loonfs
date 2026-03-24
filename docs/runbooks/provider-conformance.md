# Runbook: provider conformance

LoonDB is allowed to depend only on the provider behavior that this suite verifies.

## Required behaviors

- active trait surface only:
  `head`, `get`, `put`, `delete`, `list_prefix`, opaque compare tokens, and trait-level errors
- create-if-absent for immutable objects
- compare-and-swap update on small mutable control objects
- strong visibility after write and delete
- range reads
- overwrite behavior
- key scoping and traversal rejection

## Why this runbook exists

“S3-compatible” is not a correctness proof.

The provider contract must be tested directly for local FS, AWS S3, and Cloudflare R2.

Just as important, higher layers must not learn provider behavior any other way. If a semantic crate
needs a new provider capability, the change should land first in `loon-objectstore`, then in this
runbook, then in conformance tests.

## Credential layout

Keep the checked-in template next to the object-store conformance tests:

```text
crates/loon-objectstore/tests/provider-conformance.env.example
```

If you want a local file to source before running the ignored real-provider tests, use:

```text
crates/loon-objectstore/tests/provider-conformance.env.local
```

That local file is gitignored. The test harness reads process environment only; it does not load dotenv files for you.

## Environment variables

AWS S3:

- `LOON_TEST_S3_BUCKET`
- `LOON_TEST_S3_REGION`
- `LOON_TEST_S3_ACCESS_KEY_ID`
- `LOON_TEST_S3_SECRET_ACCESS_KEY`
- `LOON_TEST_S3_ENDPOINT` optional
- `LOON_TEST_S3_SESSION_TOKEN` optional
- `LOON_TEST_S3_PREFIX` optional

Cloudflare R2:

- `LOON_TEST_R2_BUCKET`
- `LOON_TEST_R2_ACCOUNT_ID`
- `LOON_TEST_R2_ENDPOINT`
- `LOON_TEST_R2_ACCESS_KEY_ID`
- `LOON_TEST_R2_SECRET_ACCESS_KEY`
- `LOON_TEST_R2_PREFIX` optional

Why the names use `LOON_TEST_`:

- they avoid accidentally picking up ambient `AWS_*` shell state
- CI can scope them to one conformance job without affecting production config paths

## Boundary rules

- `ObjectMetadata.etag` is an opaque compare token only
- callers may map `ObjectStoreError::PreconditionFailed` into domain-specific concurrency errors
- callers must not parse ETags, inspect provider SDK errors, or branch on provider-specific
  transport strings
- conditional-header construction (`If-Match`, `If-None-Match`), key-prefix behavior, and endpoint
  quirks stay inside `loon-objectstore`

## Current conformance scope

The current suite proves the active v1 contract only:

- create-if-absent
- compare-and-swap on small mutable objects
- compare-and-swap on a missing object returns `PreconditionFailed`
- immediate visibility after write and delete
- overwrite visibility plus `head` freshness
- delete of a missing key is idempotent
- range reads
- sorted `list_prefix`
- invalid-key and traversal rejection across all trait methods
- scoped key-prefix isolation for providers that support `key_prefix`

`future_capabilities.multipart_upload` still exists in provider profiles as a future-facing flag,
but it is not part of the active v1 `ObjectStore` trait surface and is not yet a correctness
dependency for other crates.

## Local usage

One straightforward local flow is:

```bash
cp crates/loon-objectstore/tests/provider-conformance.env.example \
  crates/loon-objectstore/tests/provider-conformance.env.local

set -a
source crates/loon-objectstore/tests/provider-conformance.env.local
set +a

cargo test -p loon-objectstore --test conformance aws_s3_real_provider_conformance -- --ignored --exact
cargo test -p loon-objectstore --test conformance cloudflare_r2_real_provider_conformance -- --ignored --exact
```

Today those ignored tests execute the same conformance assertions the local FS adapter runs, using
real AWS S3 and Cloudflare R2 resources.

## Provider-backed smoke with xtask

Conformance proves the provider contract. It does not prove that the current operability shell can
open the configured store and reconstruct namespace state.

Use the tracked ops config templates under:

```text
configs/
```

Then run:

```bash
cp configs/loondb-demo.aws-s3.example.toml loondb-demo.aws-s3.local.toml
# edit values
cargo run -p xtask -- ops smoke --config ./loondb-demo.aws-s3.local.toml --namespace demo

cp configs/loondb-demo.cloudflare-r2.example.toml loondb-demo.cloudflare-r2.local.toml
# edit values
cargo run -p xtask -- ops smoke --config ./loondb-demo.cloudflare-r2.local.toml --namespace demo
```

The canonical local-FS RC path is documented separately in `docs/runbooks/local-rc.md`.

## External CI gate

Required external jobs:

- `objectstore-conformance-localfs`
- `objectstore-conformance-aws-s3`
- `objectstore-conformance-cloudflare-r2`

Minimum required path filters:

- `crates/loon-objectstore/**`
- `docs/specs/020-objectstore-contract.md`
- `docs/runbooks/provider-conformance.md`
- `crates/loon-objectstore/tests/provider-conformance.env.example`

Required commands:

```bash
cargo test -p loon-objectstore --test conformance
cargo test -p loon-objectstore --test conformance aws_s3_real_provider_conformance -- --ignored --exact
cargo test -p loon-objectstore --test conformance cloudflare_r2_real_provider_conformance -- --ignored --exact
```

Rules:

- real-provider jobs use pre-provisioned buckets or containers only
- tests must not create or destroy buckets
- each job must inject a unique `LOON_TEST_S3_PREFIX` or `LOON_TEST_R2_PREFIX`
- provider credentials stay in the external CI secret store, not in-repo

## CI guidance

When external CI is wired:

- inject the `LOON_TEST_*` variables only into the provider jobs
- require the provider jobs for the path filters above
- allow the real-provider jobs to run slower than the default workspace path

The important split is:

- docs and templates live in-repo
- secrets live in the CI secret store or the developer's untracked local file
- production adapter constructors take explicit config values from the caller
