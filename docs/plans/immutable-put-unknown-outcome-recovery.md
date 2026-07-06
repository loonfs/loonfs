# Immutable PUT unknown-outcome recovery plan

## Context

The S3 10k benchmark failed while the first namespace maintenance checkpoint was writing immutable metadata SST objects. The failed objects were later visible in S3 and were small metadata table files, which points to an unknown-outcome PUT: S3 likely committed the object, but the client timed out before receiving the response.

`write_immutable_object` already has the important semantic escape hatch: when create-if-absent reports `PreconditionFailed`, it proves that the existing object at the exact key has the exact expected bytes and then treats the immutable write as successful. The implementation PR should apply that same proof after a transport error from the create-if-absent PUT.

## Implementation PR

**Title:** `fix(core): recover immutable put after transport error`

**Branch:** `conor/recover-immutable-put-transport`

**Effort:** S

**Why:** An immutable create-if-absent PUT can commit server-side even when the client observes a timeout or connection failure. LoonFS should not fail checkpoint or upload staging work when the immutable-write postcondition can be verified immediately afterward.

### Do

- Keep the code change in `crates/loonfs-core/src/storage/content.rs` unless a focused test wrapper needs local support in the same test module.
- Add an `Err(ObjectStoreError::Transport { .. })` arm in `write_immutable_object` after the existing `PreconditionFailed` arm.
- Reuse the existing `existing_object_matches_expected_bytes` helper for the post-transport read-back proof.
- If the helper's missing-object error text still says "after precondition failure", make that wording outcome-neutral, such as "object is missing while verifying immutable write".
- On a verified match, return `Ok(())`.
- On a missing object, verification error, or byte mismatch, return an `ImmutableObjectWriteError::Store` that preserves the original transport message and adds the verification result.
- Leave `provider_object_store`, the public `ObjectStore` trait, provider retry settings, and timeout configuration alone.

### Do not

- Do not add a new retry loop, sleep, backoff, prefix scan, cache, or reconciliation pass.
- Do not make provider-specific S3/R2/GCS/Azure branches.
- Do not treat transport errors as success without proving the exact key contains the exact expected bytes.
- Do not broaden this to mutable control object writes.
- Do not add metrics or a logging layer in the first implementation PR; a one-line warning is acceptable only if it stays local and does not complicate the patch.

### Minimal shape

The patch can stay very small. One acceptable shape is:

```rust
Err(err @ ObjectStoreError::Transport { .. }) => {
    let original = err.message();
    match existing_object_matches_expected_bytes(store, object_key, expected_bytes).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(ImmutableObjectWriteError::Store {
            object_key: object_key.to_owned(),
            message: format!(
                "{original}; immutable object exists with different bytes after transport error"
            ),
        }),
        Err(verify_err) => Err(ImmutableObjectWriteError::Store {
            object_key: object_key.to_owned(),
            message: format!(
                "{original}; failed to verify immutable write after transport error: {verify_err}"
            ),
        }),
    }
}
```

A tri-state helper is also fine if it makes the error text clearer, but it is not required for this fix. Prefer the smallest readable diff.

### Tests

Add focused tests in the existing `content.rs` test module, following the local wrapper-store style already used there.

- A store that writes the object and then returns `ObjectStoreError::transport(key, "simulated timeout")` from `put(..., PutMode::CreateIfAbsent)`; `write_immutable_object` should return `Ok(())` and the object should remain readable.
- A store that returns the same transport error without writing the object; `write_immutable_object` should return an error that includes the original transport message and says verification failed.
- If it stays compact, add a mismatch case where the key contains different bytes and recovery is rejected.

Do not use real sleeps, real timeouts, or live S3 in unit tests.

### Done when

- `cargo fmt --all --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all`
- The S3 10k benchmark no longer fails when the timed-out immutable PUT is later verifiably present with the expected bytes.

## Fit with the codebase

This belongs in `write_immutable_object` because that helper already owns the immutable-write invariant and already has the expected bytes needed to prove idempotent success. Putting the recovery in provider adapters would make the provider guess about caller semantics; putting it in each checkpoint or upload caller would duplicate edge-case handling.

The implementation should look like a continuation of the existing code: same `ObjectStoreError` variants, same `ImmutableObjectWriteError::Store` wrapper, same `sha256_digest`/metadata/read-back validation path, and same in-module async wrapper-store tests. The result should be a small semantic extension to the existing precondition-failure recovery, not a new subsystem.