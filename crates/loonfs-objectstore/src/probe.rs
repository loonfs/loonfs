//! Checks whether a configured store supports the guarantees LoonFS needs.
//!
//! The probe runs only when requested because it writes and deletes objects.
//! It runs every check even after a failure so operators receive a complete
//! report. Objects use the `probe-runs/{run_id}/` prefix, and the final check
//! removes them.
//!
//! This probe does not test presigned direct transfers. Those are enabled only
//! when [`crate::ConfiguredObjectStore::direct_transfers`] returns a bundle.

use crate::object_store::Result as StoreResult;
use crate::{
    ByteRange, ObjectStore, ObjectStoreError, PROVIDER_MULTIPART_PART_BYTES,
    PROVIDER_MULTIPART_THRESHOLD_BYTES,
};
use bytes::Bytes;
use futures::StreamExt;
use loonfs_types::api::v0;
use loonfs_types::{Checksum, ProbeRunId};

/// Results from one store probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreProbeReport {
    /// Label used to isolate this run's objects.
    pub run_id: ProbeRunId,
    /// Checks in execution order.
    pub checks: Vec<StoreProbeCheck>,
}

impl From<StoreProbeReport> for v0::StoreProbeResponse {
    fn from(report: StoreProbeReport) -> Self {
        Self {
            run_id: report.run_id,
            checks: report
                .checks
                .into_iter()
                .map(|check| v0::StoreProbeCheck {
                    name: check.name.to_owned(),
                    outcome: match check.outcome {
                        StoreProbeOutcome::Passed => v0::StoreProbeCheckOutcome::Passed,
                        StoreProbeOutcome::Unsupported => v0::StoreProbeCheckOutcome::Unsupported,
                        StoreProbeOutcome::Failed { message } => {
                            v0::StoreProbeCheckOutcome::Failed { message }
                        }
                    },
                })
                .collect(),
        }
    }
}

/// The result of one store contract check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreProbeCheck {
    /// Stable name used by callers and operators.
    pub name: &'static str,
    /// Result of the check.
    pub outcome: StoreProbeOutcome,
}

/// Result of a store contract check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreProbeOutcome {
    /// The store meets the requirement.
    Passed,
    /// The store does not support this optional capability.
    Unsupported,
    /// The store failed the check.
    Failed {
        /// Safe explanation of the failure.
        message: String,
    },
}

/// Runs every contract check and stores temporary objects under `run_id`.
///
/// A failed check does not stop the remaining checks. Callers must provide a
/// unique `run_id` when probes may run at the same time.
pub async fn run_store_contract_probe(
    store: &dyn ObjectStore,
    run_id: &ProbeRunId,
) -> StoreProbeReport {
    let run = ProbeRun {
        prefix: crate::keys::probe_run_prefix(run_id),
    };
    let checks = vec![
        check(
            "create_if_absent_enforced",
            create_if_absent_enforced(store, &run).await,
        ),
        check(
            "compare_and_swap_rejects_stale",
            compare_and_swap_rejects_stale(store, &run).await,
        ),
        check(
            "compare_and_swap_missing_object_rejected",
            compare_and_swap_missing_object_rejected(store, &run).await,
        ),
        check(
            "get_with_metadata_round_trip",
            get_with_metadata_round_trip(store, &run).await,
        ),
        check(
            "head_reports_last_modified",
            head_reports_last_modified(store, &run).await,
        ),
        check(
            "visibility_after_write",
            visibility_after_write(store, &run).await,
        ),
        check(
            "visibility_after_delete",
            visibility_after_delete(store, &run).await,
        ),
        check(
            "delete_missing_idempotent",
            delete_missing_idempotent(store, &run).await,
        ),
        check("sorted_listing", sorted_listing(store, &run).await),
        check("range_reads", range_reads(store, &run).await),
        check(
            "multipart_round_trip",
            multipart_round_trip(store, &run).await,
        ),
        check(
            "stored_checksum_readback",
            stored_checksum_readback(store, &run).await,
        ),
        // Run cleanup last so it can remove objects created by every check.
        check(
            "cleanup_leaves_prefix_empty",
            cleanup_leaves_prefix_empty(store, &run).await,
        ),
    ];

    StoreProbeReport {
        run_id: run_id.clone(),
        checks,
    }
}

/// Key prefix for one run.
struct ProbeRun {
    prefix: String,
}

impl ProbeRun {
    /// Returns a key inside this run's prefix.
    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    /// Returns a listing prefix inside this run's prefix.
    fn listing(&self, name: &str) -> String {
        format!("{}{name}/", self.prefix)
    }
}

/// Internal check result.
enum CheckFailure {
    /// The store does not support this optional capability.
    Unsupported,
    /// The store returned the wrong result or the operation failed.
    Failed(String),
}

type CheckResult = std::result::Result<(), CheckFailure>;

fn check(name: &'static str, result: CheckResult) -> StoreProbeCheck {
    let outcome = match result {
        Ok(()) => StoreProbeOutcome::Passed,
        Err(CheckFailure::Unsupported) => StoreProbeOutcome::Unsupported,
        Err(CheckFailure::Failed(message)) => StoreProbeOutcome::Failed { message },
    };
    StoreProbeCheck { name, outcome }
}

/// Turns a store failure into a reportable one without exposing provider or
/// object identity.
fn failed(operation: &str, error: &ObjectStoreError) -> CheckFailure {
    CheckFailure::Failed(format!("{operation} failed: {}", error.public_message()))
}

/// Reports what the store did instead of what the contract requires.
fn wrong(message: impl Into<String>) -> CheckFailure {
    CheckFailure::Failed(message.into())
}

/// Unwraps a store call, attributing any failure to `operation`.
fn ok<T>(operation: &str, result: StoreResult<T>) -> std::result::Result<T, CheckFailure> {
    result.map_err(|error| failed(operation, &error))
}

/// Unwraps a store call for an optional capability: a store that declares
/// the capability absent ends the check as [`StoreProbeOutcome::Unsupported`]
/// rather than failing it.
fn ok_optional<T>(operation: &str, result: StoreResult<T>) -> std::result::Result<T, CheckFailure> {
    match result {
        Ok(value) => Ok(value),
        Err(ObjectStoreError::Unsupported(_)) => Err(CheckFailure::Unsupported),
        Err(error) => Err(failed(operation, &error)),
    }
}

/// Requires a present object, so an absent one reads as the contract
/// violation it is rather than a `None` the check silently tolerates.
fn present<T>(what: &str, value: Option<T>) -> std::result::Result<T, CheckFailure> {
    value.ok_or_else(|| wrong(format!("{what} read back as absent")))
}

/// Requires the store to have refused a request with the error that
/// `matches` accepts, which `expected` describes. Anything else (acceptance,
/// or a different failure) means the store does not enforce the rule that
/// the check is about.
fn expect_error<T>(
    what: &str,
    result: StoreResult<T>,
    expected: &str,
    matches: fn(&ObjectStoreError) -> bool,
) -> CheckResult {
    match result {
        Err(error) if matches(&error) => Ok(()),
        Err(error) => Err(wrong(format!(
            "{what} should have been refused as {expected}, but failed differently: {}",
            safe_message(&error)
        ))),
        Ok(_) => Err(wrong(format!(
            "{what} was accepted, so the store does not enforce this rule"
        ))),
    }
}

/// Requires the store to have refused a write whose precondition did not
/// hold, as fencing assumes.
fn refused<T>(what: &str, result: StoreResult<T>) -> CheckResult {
    expect_error(what, result, "a failed precondition", |error| {
        matches!(error, ObjectStoreError::PreconditionFailed { .. })
    })
}

/// Create-if-absent is how a namespace's first writer claims a mutable key
/// nobody else may claim. A store that lets the second create through hands
/// two writers the same claim.
async fn create_if_absent_enforced(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let key = run.key("create-if-absent");

    ok(
        "create-if-absent write",
        store
            .put_if_absent(&key, Bytes::from_static(br#"{"seq":41}"#))
            .await,
    )?;
    refused(
        "a create-if-absent write over an existing object",
        store
            .put_if_absent(&key, Bytes::from_static(br#"{"seq":42}"#))
            .await,
    )?;

    let body = present(
        "the created object",
        ok("read", store.get(&key, None).await)?,
    )?;
    if body.as_ref() != br#"{"seq":41}"# {
        return Err(wrong(
            "a refused create-if-absent write still changed the stored bytes",
        ));
    }
    Ok(())
}

/// Compare-and-swap is the fence itself: an evicted writer's stale token
/// must be refused, or two writers publish over each other.
async fn compare_and_swap_rejects_stale(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let key = run.key("compare-and-swap");

    ok(
        "seed write",
        store
            .put_if_absent(&key, Bytes::from_static(br#"{"seq":41,"writer_epoch":8}"#))
            .await,
    )?;
    let first_token = present(
        "the seeded object's metadata",
        ok("head", store.head(&key).await)?,
    )?
    .etag
    .ok_or_else(|| {
        wrong("the store reports no compare token, so compare-and-swap cannot fence anything")
    })?;

    ok(
        "compare-and-swap on a current token",
        store
            .compare_and_swap(
                &key,
                &first_token,
                Bytes::from_static(br#"{"seq":42,"writer_epoch":8}"#),
            )
            .await,
    )?;
    refused(
        "a compare-and-swap on a stale token",
        store
            .compare_and_swap(
                &key,
                &first_token,
                Bytes::from_static(br#"{"seq":43,"writer_epoch":9}"#),
            )
            .await,
    )?;

    let body = present(
        "the compare-and-swap object",
        ok("read", store.get(&key, None).await)?,
    )?;
    if body.as_ref() != br#"{"seq":42,"writer_epoch":8}"# {
        return Err(wrong(
            "a refused compare-and-swap still changed the stored bytes",
        ));
    }
    Ok(())
}

/// A compare-and-swap against a key that does not exist has no version to
/// match, so it is a failed precondition — not a create.
async fn compare_and_swap_missing_object_rejected(
    store: &dyn ObjectStore,
    run: &ProbeRun,
) -> CheckResult {
    let key = run.key("compare-and-swap-missing");
    refused(
        "a compare-and-swap against a missing object",
        store
            .compare_and_swap(&key, "missing-etag", Bytes::from_static(br#"{"seq":1}"#))
            .await,
    )
}

/// Reading bytes and identity from one observation is what lets a caller
/// trust that the metadata describes the bytes it just read, rather than a
/// version that replaced them between two requests.
async fn get_with_metadata_round_trip(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let key = run.key("get-with-metadata");
    let bytes = br#"{"seq":41,"source":"get-with-metadata"}"#;

    let written = ok(
        "write",
        store
            .put_if_absent(&key, Bytes::copy_from_slice(bytes))
            .await,
    )?;
    let loaded = present(
        "the written object",
        ok("full-object read", store.get_with_metadata(&key).await)?,
    )?;

    if loaded.bytes != bytes.as_slice() {
        return Err(wrong("a full-object read returned unexpected bytes"));
    }
    if loaded.metadata.size_bytes != bytes.len() as u64 {
        return Err(wrong(
            "a full-object read reports a size that disagrees with its own bytes",
        ));
    }
    if loaded.metadata.etag != written.etag {
        return Err(wrong(
            "a full-object read reports an identity that disagrees with the write that produced it",
        ));
    }
    Ok(())
}

/// Garbage collection measures its grace window against the object's own
/// last-modified time, so a store that does not report one — or reports the
/// unix epoch, which is what an S3-compatible endpoint that omits the header
/// turns into — hands every object an infinite age and reclamation loses the
/// window that protects in-flight publications and live readers.
///
/// The write's own result carries no timestamp, so the check heads the
/// object the way the sweep does.
async fn head_reports_last_modified(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let key = run.key("last-modified");

    ok(
        "write",
        store
            .put_if_absent(&key, Bytes::from_static(br#"{"seq":1}"#))
            .await,
    )?;
    let head = present(
        "the written object's metadata",
        ok("head", store.head(&key).await)?,
    )?;
    match head.last_modified_at_ms {
        None => Err(wrong(
            "the store reports no last-modified time, so nothing can age out of the grace window",
        )),
        // Nothing LoonFS writes predates the software, so a stamp at the
        // epoch is a synthesized one rather than a real age.
        Some(0) => Err(wrong(
            "the store reports the unix epoch as an object's last-modified time, \
             which reads as infinitely old and defeats the grace window",
        )),
        Some(_) => Ok(()),
    }
}

/// A write must be listable immediately. Recovery and garbage collection
/// both enumerate prefixes to find what exists, so a listing that lags a
/// write hides objects from the code that owns them.
async fn visibility_after_write(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let prefix = run.listing("visibility-after-write");
    let key = format!("{prefix}object");

    ok(
        "write",
        store
            .put_if_absent(&key, Bytes::from_static(br#"{"created":true}"#))
            .await,
    )?;
    let listed = ok("list", store.list_prefix(&prefix).await)?;
    if listed != vec![key.clone()] {
        return Err(wrong(
            "listing immediately after a write did not return exactly the newly written object",
        ));
    }
    Ok(())
}

/// A delete must be listable-absent immediately, for the same reason: a
/// listing that still reports a deleted object makes reclamation re-read
/// objects that are already gone.
async fn visibility_after_delete(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let prefix = run.listing("visibility-after-delete");
    let key = format!("{prefix}object");

    ok(
        "write",
        store
            .put_if_absent(&key, Bytes::from_static(br#"{"created":true}"#))
            .await,
    )?;
    ok("delete", store.delete(&key).await)?;
    let listed = ok("list", store.list_prefix(&prefix).await)?;
    if !listed.is_empty() {
        return Err(wrong(
            "listing immediately after deleting its only object still returned objects",
        ));
    }
    Ok(())
}

/// Deleting what is not there succeeds. Every cleanup path deletes without
/// first proving what it is cleaning up, so a store that errors here turns
/// ordinary cleanup into a failure.
async fn delete_missing_idempotent(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let key = run.key("delete-missing");
    ok("delete of a missing object", store.delete(&key).await)?;
    if ok("head", store.head(&key).await)?.is_some() {
        return Err(wrong("an object that was never written reads as present"));
    }
    Ok(())
}

/// A prefix listing must answer with exactly the objects under it, in
/// ascending key order.
async fn sorted_listing(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let prefix = run.listing("sorted");
    let keys = vec![
        format!("{prefix}a"),
        format!("{prefix}b"),
        format!("{prefix}c"),
    ];

    // Written out of order, so a store that echoes write order rather than
    // key order is caught.
    for index in [1usize, 2, 0] {
        ok(
            "write",
            store
                .put_if_absent(&keys[index], Bytes::from_static(br#"{"seq":1}"#))
                .await,
        )?;
    }

    // Both listing methods preserve the provider's order.
    let mut streamed = Vec::new();
    let mut stream = store.list_prefix_stream(&prefix);
    while let Some(item) = stream.next().await {
        streamed.push(ok("list", item)?);
    }
    let listed = ok("list", store.list_prefix(&prefix).await)?;
    if streamed != keys {
        return Err(wrong(format!(
            "streaming a prefix did not return the {} objects written under it",
            keys.len()
        )));
    }
    if listed != keys {
        return Err(wrong(format!(
            "listing a prefix did not return the {} objects written under it in key order",
            keys.len()
        )));
    }
    Ok(())
}

/// Bounded reads are how metadata segments are read at all: a wrong range
/// answer is a wrong block, which decodes as corruption rather than as an
/// error.
async fn range_reads(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let key = run.key("range");
    ok(
        "write",
        store
            .put_if_absent(&key, Bytes::from_static(b"abcdef"))
            .await,
    )?;

    let bounded = |start_inclusive, end_exclusive| {
        Some(ByteRange {
            start_inclusive,
            end_exclusive,
        })
    };

    let read = ok("bounded read", store.get(&key, bounded(1, 4)).await)?;
    if read != Some(Bytes::from_static(b"bcd")) {
        return Err(wrong("a bounded read of bytes 1..4 returned wrong bytes"));
    }
    // The bounded-read contract, uniform across providers: an end past the
    // object clamps, reading at the exact end is empty, and a start past
    // the end is an invalid range.
    let clamped = ok("clamped read", store.get(&key, bounded(4, 99)).await)?;
    if clamped != Some(Bytes::from_static(b"ef")) {
        return Err(wrong(
            "a read whose end runs past the object did not clamp to the expected bytes",
        ));
    }
    let at_end = ok(
        "read at the exact end",
        store.get(&key, bounded(6, 8)).await,
    )?;
    if at_end != Some(Bytes::new()) {
        return Err(wrong(
            "a read starting at the object's exact end did not return an empty range",
        ));
    }
    expect_error(
        "a read starting past the object's end",
        store.get(&key, bounded(7, 8)).await,
        "an invalid range",
        is_invalid_range,
    )?;

    ok("delete", store.delete(&key).await)?;
    let missing = ok(
        "bounded read of a missing object",
        store.get(&key, bounded(0, 4)).await,
    )?;
    if missing.is_some() {
        return Err(wrong(
            "a bounded read of a deleted object answered bytes instead of absence",
        ));
    }
    expect_error(
        "a descending range of a missing object",
        store.get(&key, bounded(4, 1)).await,
        "an invalid range",
        is_invalid_range,
    )
}

fn is_invalid_range(error: &ObjectStoreError) -> bool {
    matches!(error, ObjectStoreError::InvalidRange { .. })
}

/// Checks the multipart path that every large content object and segment
/// write takes.
///
/// A verified immutable write past the multipart threshold goes through the
/// provider's own multipart rules: non-final part sizes, a create-only
/// completion, and the stored full-object checksum. The object must read
/// back byte-identical with the checksum of those bytes stored, and a second
/// write of different bytes to the same key must leave it unchanged.
/// Cloudflare R2 requires every non-final part to have the same size, and
/// this geometry exists to satisfy that rule.
async fn multipart_round_trip(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let key = run.key("multipart");

    // The smallest payload with a middle part, which is where a provider's
    // own rules about non-final part sizes apply.
    let payload_len = PROVIDER_MULTIPART_THRESHOLD_BYTES + PROVIDER_MULTIPART_PART_BYTES + 4096;
    let payload = multipart_payload(payload_len, 0);

    let metadata = ok_immutable(
        "multipart immutable write",
        store
            .put_immutable_verified(&key, Bytes::from(payload.clone()))
            .await,
    )?;
    if metadata.size_bytes != payload_len {
        return Err(wrong(format!(
            "a multipart write of {payload_len} bytes reports {} stored",
            metadata.size_bytes
        )));
    }
    let stored = present(
        "the assembled object's metadata",
        ok("head", store.head(&key).await)?,
    )?;
    if stored.checksum != Some(Checksum::compute(store.checksum_algorithm(), &payload)) {
        return Err(wrong(
            "the stored checksum of an object assembled from parts does not describe its bytes",
        ));
    }

    expect_error(
        "a create-only multipart write over an existing object",
        store
            .put_immutable_verified(&key, Bytes::from(multipart_payload(payload_len, 1)))
            .await,
        "a different object",
        |error| matches!(error, ObjectStoreError::DifferentObject { .. }),
    )?;
    let read_back = present(
        "the assembled object",
        ok("read", store.get(&key, None).await)?,
    )?;
    if read_back.as_ref() != payload.as_slice() {
        return Err(wrong(
            "an object assembled from parts does not read back as the bytes first written",
        ));
    }
    Ok(())
}

/// Builds `len` bytes of a repeating pattern, rotated by `offset` so that
/// two payloads of one length differ.
fn multipart_payload(len: u64, offset: usize) -> Vec<u8> {
    (0..251_u8)
        .cycle()
        .skip(offset)
        .take(loonfs_types::usize_from_u64(len))
        .collect()
}

/// Unwraps a verified immutable write to a new key, attributing any
/// failure to `operation`.
fn ok_immutable<T>(
    operation: &str,
    result: StoreResult<T>,
) -> std::result::Result<T, CheckFailure> {
    result.map_err(|error| match error {
        ObjectStoreError::DifferentObject { .. }
        | ObjectStoreError::StoredChecksumMissing { .. } => wrong(format!(
            "{operation} to a new key failed: {}",
            safe_message(&error)
        )),
        error => failed(operation, &error),
    })
}

/// Describes a failed request without exposing provider or object identity.
fn safe_message(error: &ObjectStoreError) -> String {
    match error {
        ObjectStoreError::DifferentObject { .. } => {
            "the key already contains different bytes".to_owned()
        }
        ObjectStoreError::StoredChecksumMissing { .. } => {
            "the key's existing object reports no stored checksum".to_owned()
        }
        error => error.public_message().into_owned(),
    }
}

async fn stored_checksum_readback(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let key = run.key("stored-checksum");
    let payload = Bytes::from_static(b"stored checksum readback payload");

    let absent = ok_optional(
        "stored-checksum read of a missing object",
        store.head(&key).await,
    )?;
    if absent.is_some() {
        return Err(wrong("an object that does not exist reports a checksum"));
    }

    ok("write", store.put_if_absent(&key, payload.clone()).await)?;
    let stored = present(
        "a present object's checksum",
        ok("head", store.head(&key).await)?,
    )?;

    if stored.size_bytes != payload.len() as u64 {
        return Err(wrong(format!(
            "a stored-checksum read of a {}-byte object reports {} bytes",
            payload.len(),
            stored.size_bytes
        )));
    }
    let checksum = stored
        .checksum
        .ok_or_else(|| wrong("the provider reports no stored checksum"))?;
    if checksum != Checksum::compute(store.checksum_algorithm(), &payload) {
        return Err(wrong(
            "the stored checksum does not describe the bytes actually stored",
        ));
    }
    Ok(())
}

/// The run's own cleanup, and a check in its own right: after deleting
/// everything the run wrote, listing the run's prefix must answer empty.
async fn cleanup_leaves_prefix_empty(store: &dyn ObjectStore, run: &ProbeRun) -> CheckResult {
    let prefix = &run.prefix;
    for key in ok("list", store.list_prefix(prefix).await)? {
        ok("cleanup delete", store.delete(&key).await)?;
    }
    let remaining = ok("list after cleanup", store.list_prefix(prefix).await)?;
    if !remaining.is_empty() {
        return Err(wrong(format!(
            "the probe's own prefix still holds {} objects after cleanup",
            remaining.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_fs_store::LocalFsStore;
    use crate::{ListedObject, ObjectBody, ObjectMetadata, PutMode};
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use loonfs_types::api::v0::StoreProbeResponse;
    use loonfs_types::{EffectiveLimit, Page};
    use std::assert_matches;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tempfile::TempDir;

    const POISON_PROVIDER_DETAIL: &str = "<Error>AccessDenied</Error> \
        arn:aws:iam::123456789012:role/private-role private-bucket \
        namespaces/customer-a/hint.json x-amz-request-id=provider-request \
        x-amz-id-2=provider-host-id AKIAEXAMPLE";

    fn outcome<'a>(report: &'a StoreProbeReport, name: &str) -> &'a StoreProbeOutcome {
        &report
            .checks
            .iter()
            .find(|check| check.name == name)
            .expect("report should carry the named check")
            .outcome
    }

    #[derive(Debug)]
    struct PoisonPermissionStore;

    impl PoisonPermissionStore {
        fn denied(key: &str) -> ObjectStoreError {
            ObjectStoreError::PermissionDenied {
                object_key: key.to_owned(),
                message: POISON_PROVIDER_DETAIL.to_owned(),
            }
        }
    }

    #[async_trait]
    impl ObjectStore for PoisonPermissionStore {
        fn checksum_algorithm(&self) -> loonfs_types::ChecksumAlgorithm {
            loonfs_types::ChecksumAlgorithm::Crc64nvme
        }

        async fn put_streamed(
            &self,
            key: &str,
            body: crate::ByteStream,
            mode: crate::PutMode,
        ) -> crate::Result<u64> {
            crate::buffered::put_streamed(self, key, body, mode).await
        }

        async fn assemble(
            &self,
            key: &str,
            sources: &[crate::AssemblySource],
            tail: Vec<bytes::Bytes>,
            expected: &loonfs_types::Checksum,
        ) -> crate::Result<crate::ObjectMetadata> {
            crate::buffered::assemble(self, key, sources, tail, expected).await
        }

        async fn head(&self, key: &str) -> StoreResult<Option<ObjectMetadata>> {
            Err(Self::denied(key))
        }

        async fn get_with_metadata(&self, key: &str) -> StoreResult<Option<ObjectBody>> {
            Err(Self::denied(key))
        }

        async fn get(&self, key: &str, _range: Option<ByteRange>) -> StoreResult<Option<Bytes>> {
            Err(Self::denied(key))
        }

        async fn put(
            &self,
            key: &str,
            _bytes: Bytes,
            _mode: PutMode,
        ) -> StoreResult<ObjectMetadata> {
            Err(Self::denied(key))
        }

        async fn delete(&self, key: &str) -> StoreResult<()> {
            Err(Self::denied(key))
        }

        fn list_entries_from_stream(
            &self,
            prefix: &str,
            _start_after: Option<&str>,
        ) -> BoxStream<'static, StoreResult<ListedObject>> {
            Box::pin(futures::stream::once(std::future::ready(Err(
                Self::denied(prefix),
            ))))
        }

        async fn list_child_prefixes(
            &self,
            prefix: &str,
            _start_after: Option<&str>,
            _limit: EffectiveLimit,
        ) -> StoreResult<Page<String, String>> {
            Err(Self::denied(prefix))
        }
    }

    #[tokio::test]
    async fn provider_failures_make_every_probe_message_safe_to_paste() {
        let run_id = ProbeRunId::generate();
        let run_prefix = crate::keys::probe_run_prefix(&run_id);
        let report = run_store_contract_probe(&PoisonPermissionStore, &run_id).await;
        let messages: Vec<_> = report
            .checks
            .iter()
            .filter_map(|check| match &check.outcome {
                StoreProbeOutcome::Failed { message } => Some(message.as_str()),
                StoreProbeOutcome::Passed | StoreProbeOutcome::Unsupported => None,
            })
            .collect();

        assert!(!messages.is_empty());
        for message in messages {
            assert!(
                message.contains("object-store permission denied"),
                "{message}"
            );
            for marker in [
                "<Error>AccessDenied</Error>",
                "arn:aws:iam::123456789012:role/private-role",
                "private-bucket",
                "namespaces/customer-a/hint.json",
                "x-amz-request-id=provider-request",
                "x-amz-id-2=provider-host-id",
                "AKIAEXAMPLE",
                run_prefix.as_str(),
            ] {
                assert!(
                    !message.contains(marker),
                    "probe message leaked {marker}: {message}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_conforming_store_passes_every_check() {
        let temp_dir = TempDir::new().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");

        let run_id = ProbeRunId::generate();
        let report = run_store_contract_probe(&store, &run_id).await;

        assert_eq!(report.run_id, run_id);
        let failures: Vec<_> = report
            .checks
            .iter()
            .filter(|check| matches!(check.outcome, StoreProbeOutcome::Failed { .. }))
            .collect();
        assert!(failures.is_empty(), "unexpected failures: {failures:?}");
        assert!(StoreProbeResponse::from(report).all_passed());
    }

    #[tokio::test]
    async fn the_report_contains_the_complete_probe_set_once() {
        let temp_dir = TempDir::new().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");

        let report = run_store_contract_probe(&store, &ProbeRunId::generate()).await;

        let expected = std::collections::BTreeSet::from([
            "create_if_absent_enforced",
            "compare_and_swap_rejects_stale",
            "compare_and_swap_missing_object_rejected",
            "get_with_metadata_round_trip",
            "head_reports_last_modified",
            "visibility_after_write",
            "visibility_after_delete",
            "delete_missing_idempotent",
            "sorted_listing",
            "range_reads",
            "multipart_round_trip",
            "stored_checksum_readback",
            "cleanup_leaves_prefix_empty",
        ]);
        let names: std::collections::BTreeSet<_> =
            report.checks.iter().map(|check| check.name).collect();
        assert_eq!(
            report.checks.len(),
            names.len(),
            "probe names must be unique"
        );
        assert_eq!(
            names, expected,
            "the object-store layer owns the complete probe inventory"
        );
    }

    #[tokio::test]
    async fn a_probe_run_leaves_its_prefix_empty() {
        let temp_dir = TempDir::new().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");

        let report = run_store_contract_probe(&store, &ProbeRunId::generate()).await;

        assert_eq!(
            outcome(&report, "cleanup_leaves_prefix_empty"),
            &StoreProbeOutcome::Passed
        );
        assert!(store
            .list_prefix("probe-runs/")
            .await
            .expect("list the probe prefix")
            .is_empty());
    }

    /// A store whose local filesystem honours everything except
    /// compare-and-swap: the one shape a preconditions-ignoring S3 gateway
    /// presents, and the one that silently corrupts fenced writes.
    #[derive(Debug)]
    struct StaleCompareAndSwapAcceptingStore {
        inner: LocalFsStore,
        accepted_a_stale_swap: Arc<AtomicBool>,
    }

    #[async_trait]
    impl ObjectStore for StaleCompareAndSwapAcceptingStore {
        fn checksum_algorithm(&self) -> loonfs_types::ChecksumAlgorithm {
            self.inner.checksum_algorithm()
        }

        async fn put_streamed(
            &self,
            key: &str,
            body: crate::ByteStream,
            mode: crate::PutMode,
        ) -> crate::Result<u64> {
            crate::buffered::put_streamed(self, key, body, mode).await
        }

        async fn assemble(
            &self,
            key: &str,
            sources: &[crate::AssemblySource],
            tail: Vec<bytes::Bytes>,
            expected: &loonfs_types::Checksum,
        ) -> crate::Result<crate::ObjectMetadata> {
            crate::buffered::assemble(self, key, sources, tail, expected).await
        }

        async fn head(&self, key: &str) -> StoreResult<Option<ObjectMetadata>> {
            self.inner.head(key).await
        }

        async fn get_with_metadata(&self, key: &str) -> StoreResult<Option<ObjectBody>> {
            self.inner.get_with_metadata(key).await
        }

        async fn get(&self, key: &str, range: Option<ByteRange>) -> StoreResult<Option<Bytes>> {
            self.inner.get(key, range).await
        }

        async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> StoreResult<ObjectMetadata> {
            if let PutMode::CompareAndSwap { .. } = mode {
                self.accepted_a_stale_swap.store(true, Ordering::SeqCst);
                self.inner.delete(key).await?;
                return self.inner.put_if_absent(key, bytes).await;
            }
            self.inner.put(key, bytes, mode).await
        }

        async fn delete(&self, key: &str) -> StoreResult<()> {
            self.inner.delete(key).await
        }

        fn list_entries_from_stream(
            &self,
            prefix: &str,
            start_after: Option<&str>,
        ) -> BoxStream<'static, StoreResult<ListedObject>> {
            self.inner.list_entries_from_stream(prefix, start_after)
        }

        async fn list_child_prefixes(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            limit: EffectiveLimit,
        ) -> StoreResult<Page<String, String>> {
            self.inner
                .list_child_prefixes(prefix, start_after, limit)
                .await
        }
    }

    #[tokio::test]
    async fn a_store_that_ignores_compare_and_swap_fails_only_the_checks_about_it() {
        let temp_dir = TempDir::new().expect("tempdir");
        let accepted_a_stale_swap = Arc::new(AtomicBool::new(false));
        let store = StaleCompareAndSwapAcceptingStore {
            inner: LocalFsStore::new(temp_dir.path()).expect("create local object store"),
            accepted_a_stale_swap: Arc::clone(&accepted_a_stale_swap),
        };

        let report = run_store_contract_probe(&store, &ProbeRunId::generate()).await;

        assert!(accepted_a_stale_swap.load(Ordering::SeqCst));
        assert!(!StoreProbeResponse::from(report.clone()).all_passed());
        assert_matches!(
            outcome(&report, "compare_and_swap_rejects_stale"),
            StoreProbeOutcome::Failed { message } if message.contains("does not enforce")
        );
        assert_matches!(
            outcome(&report, "compare_and_swap_missing_object_rejected"),
            StoreProbeOutcome::Failed { .. }
        );
        // A failed check ends that check, not the run: everything after it
        // still reports, and the run still cleans up after itself.
        assert_eq!(
            outcome(&report, "create_if_absent_enforced"),
            &StoreProbeOutcome::Passed
        );
        assert_eq!(outcome(&report, "range_reads"), &StoreProbeOutcome::Passed);
        assert_eq!(
            outcome(&report, "cleanup_leaves_prefix_empty"),
            &StoreProbeOutcome::Passed
        );
        assert!(store
            .list_prefix("probe-runs/")
            .await
            .expect("list the probe prefix")
            .is_empty());
    }

    /// A checksum-capable provider may encounter an object written without
    /// a stored checksum. That per-object answer is distinct from the store
    /// lacking the readback capability altogether.
    #[derive(Debug)]
    struct MissingStoredChecksumStore {
        inner: LocalFsStore,
    }

    #[async_trait]
    impl ObjectStore for MissingStoredChecksumStore {
        fn checksum_algorithm(&self) -> loonfs_types::ChecksumAlgorithm {
            self.inner.checksum_algorithm()
        }

        async fn put_streamed(
            &self,
            key: &str,
            body: crate::ByteStream,
            mode: crate::PutMode,
        ) -> crate::Result<u64> {
            crate::buffered::put_streamed(self, key, body, mode).await
        }

        async fn assemble(
            &self,
            key: &str,
            sources: &[crate::AssemblySource],
            tail: Vec<bytes::Bytes>,
            expected: &loonfs_types::Checksum,
        ) -> crate::Result<crate::ObjectMetadata> {
            crate::buffered::assemble(self, key, sources, tail, expected).await
        }

        async fn head(&self, key: &str) -> StoreResult<Option<ObjectMetadata>> {
            Ok(self.inner.head(key).await?.map(|mut metadata| {
                metadata.checksum = None;
                metadata
            }))
        }

        async fn get_with_metadata(&self, key: &str) -> StoreResult<Option<ObjectBody>> {
            self.inner.get_with_metadata(key).await
        }

        async fn get(&self, key: &str, range: Option<ByteRange>) -> StoreResult<Option<Bytes>> {
            self.inner.get(key, range).await
        }

        async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> StoreResult<ObjectMetadata> {
            self.inner.put(key, bytes, mode).await
        }

        async fn delete(&self, key: &str) -> StoreResult<()> {
            self.inner.delete(key).await
        }

        fn list_entries_from_stream(
            &self,
            prefix: &str,
            start_after: Option<&str>,
        ) -> BoxStream<'static, StoreResult<ListedObject>> {
            self.inner.list_entries_from_stream(prefix, start_after)
        }

        async fn list_child_prefixes(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            limit: EffectiveLimit,
        ) -> StoreResult<Page<String, String>> {
            self.inner
                .list_child_prefixes(prefix, start_after, limit)
                .await
        }
    }

    /// A store that stamps every object at the unix epoch — what an
    /// S3-compatible endpoint that omits `Last-Modified` becomes once the
    /// AWS client path synthesizes a time for it.
    #[derive(Debug)]
    struct EpochLastModifiedStore {
        inner: LocalFsStore,
    }

    #[async_trait]
    impl ObjectStore for EpochLastModifiedStore {
        fn checksum_algorithm(&self) -> loonfs_types::ChecksumAlgorithm {
            self.inner.checksum_algorithm()
        }

        async fn put_streamed(
            &self,
            key: &str,
            body: crate::ByteStream,
            mode: crate::PutMode,
        ) -> crate::Result<u64> {
            crate::buffered::put_streamed(self, key, body, mode).await
        }

        async fn assemble(
            &self,
            key: &str,
            sources: &[crate::AssemblySource],
            tail: Vec<bytes::Bytes>,
            expected: &loonfs_types::Checksum,
        ) -> crate::Result<crate::ObjectMetadata> {
            crate::buffered::assemble(self, key, sources, tail, expected).await
        }

        async fn head(&self, key: &str) -> StoreResult<Option<ObjectMetadata>> {
            Ok(self.inner.head(key).await?.map(|mut metadata| {
                metadata.last_modified_at_ms = Some(0);
                metadata
            }))
        }

        async fn get_with_metadata(&self, key: &str) -> StoreResult<Option<ObjectBody>> {
            self.inner.get_with_metadata(key).await
        }

        async fn get(&self, key: &str, range: Option<ByteRange>) -> StoreResult<Option<Bytes>> {
            self.inner.get(key, range).await
        }

        async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> StoreResult<ObjectMetadata> {
            self.inner.put(key, bytes, mode).await
        }

        async fn delete(&self, key: &str) -> StoreResult<()> {
            self.inner.delete(key).await
        }

        fn list_entries_from_stream(
            &self,
            prefix: &str,
            start_after: Option<&str>,
        ) -> BoxStream<'static, StoreResult<ListedObject>> {
            self.inner.list_entries_from_stream(prefix, start_after)
        }

        async fn list_child_prefixes(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            limit: EffectiveLimit,
        ) -> StoreResult<Page<String, String>> {
            self.inner
                .list_child_prefixes(prefix, start_after, limit)
                .await
        }
    }

    #[tokio::test]
    async fn a_store_that_stamps_every_object_at_the_epoch_fails_loudly() {
        let temp_dir = TempDir::new().expect("tempdir");
        let store = EpochLastModifiedStore {
            inner: LocalFsStore::new(temp_dir.path()).expect("create local object store"),
        };

        let report = run_store_contract_probe(&store, &ProbeRunId::generate()).await;

        assert!(!StoreProbeResponse::from(report.clone()).all_passed());
        assert_matches!(
            outcome(&report, "head_reports_last_modified"),
            StoreProbeOutcome::Failed { message } if message.contains("unix epoch")
        );
    }

    #[tokio::test]
    async fn a_present_object_without_a_stored_checksum_fails_the_probe() {
        let temp_dir = TempDir::new().expect("tempdir");
        let store = MissingStoredChecksumStore {
            inner: LocalFsStore::new(temp_dir.path()).expect("create local object store"),
        };

        let report = run_store_contract_probe(&store, &ProbeRunId::generate()).await;

        assert_matches!(
            outcome(&report, "stored_checksum_readback"),
            StoreProbeOutcome::Failed { .. }
        );
        assert!(!StoreProbeResponse::from(report).all_passed());
    }
}
