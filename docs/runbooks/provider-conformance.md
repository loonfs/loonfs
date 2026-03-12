# Runbook: provider conformance

LoonDB is allowed to depend only on the provider behavior that this suite verifies.

## Required behaviors

- create-if-absent for immutable objects
- compare-and-swap update on small mutable control objects
- strong visibility after write and delete
- range reads
- overwrite behavior
- multipart behavior for large immutable blobs

## Why this runbook exists

“S3-compatible” is not a correctness proof.

The provider contract must be tested directly for local FS, AWS S3, and Cloudflare R2.

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

Today those ignored tests lock the secret-loading contract and invocation path. Once the real adapters exist, they should execute the full conformance suite instead of only loading config.

## CI guidance

When CI is wired:

- create a dedicated real-provider conformance job
- inject the `LOON_TEST_*` variables only into that job
- require the job for changes under `crates/loon-objectstore/`
- keep the job allowed to run slower than the default workspace test path

The important split is:

- docs and templates live in-repo
- secrets live in the CI secret store or the developer's untracked local file
- production adapter constructors take explicit config values from the caller
