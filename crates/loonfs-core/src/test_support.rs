//! Test helpers and recording implementations.

/// Shares the read request bound with tests outside this crate.
pub const STORE_READS_IN_FLIGHT: usize = crate::in_flight::STORE_READS_IN_FLIGHT;

/// A standard monotonic timer, for a test whose deadlines don't matter.
pub fn std_timer() -> std::sync::Arc<dyn loonfs_types::MonotonicTimer> {
    std::sync::Arc::new(loonfs_types::StdMonotonicTimer::default())
}

/// An observation of a standard monotonic timer's current time.
pub fn observed_now() -> crate::time::Observation {
    crate::time::Observation::now(std_timer())
}

/// A pool of 32 content write permits that reports its counts to nothing,
/// for a test whose content write limit doesn't matter.
pub fn content_writes() -> crate::PermitPool {
    crate::PermitPool::new(
        std::num::NonZeroUsize::new(32).expect("32 is nonzero"),
        None,
    )
}

/// Builds the record of a service-proxied upload session with new IDs.
#[cfg(test)]
pub(crate) fn proxied_upload_session(
    namespace_id: &loonfs_types::NamespaceId,
    expires_at_ms: u64,
) -> loonfs_types::format::control::UploadSessionPayload {
    loonfs_types::format::control::UploadSessionPayload {
        namespace_id: namespace_id.clone(),
        upload_id: loonfs_types::UploadId::generate(),
        content_id: loonfs_types::ContentId::generate(),
        subject_id: None,
        mode: loonfs_types::format::control::UploadSessionMode::ServiceProxied {},
        expires_at_ms,
    }
}

/// Writes an upload session's record, and its outcome record when `outcome`
/// is set, as a session that ended that way leaves them. Returns the key of
/// the session record.
#[cfg(test)]
pub(crate) async fn write_upload_records<S: loonfs_objectstore::ObjectStore + ?Sized>(
    store: &S,
    session: &loonfs_types::format::control::UploadSessionPayload,
    outcome: Option<loonfs_types::format::control::UploadOutcome>,
) -> String {
    use loonfs_types::format::control::{
        encode_control_state, ControlObjectKind, UploadOutcomePayload,
    };
    let key = loonfs_objectstore::keys::upload_session(&session.namespace_id, &session.upload_id);
    let bytes =
        encode_control_state(ControlObjectKind::UploadSession, session).expect("encode session");
    store
        .put_if_absent(&key, bytes::Bytes::from(bytes))
        .await
        .expect("write session");
    if let Some(outcome) = outcome {
        let record = UploadOutcomePayload {
            namespace_id: session.namespace_id.clone(),
            upload_id: session.upload_id.clone(),
            outcome,
        };
        let bytes = encode_control_state(ControlObjectKind::UploadOutcome, &record)
            .expect("encode outcome");
        store
            .put_if_absent(
                &loonfs_objectstore::keys::upload_outcome(
                    &session.namespace_id,
                    &session.upload_id,
                ),
                bytes::Bytes::from(bytes),
            )
            .await
            .expect("write outcome");
    }
    key
}

/// Compares an opaque token with its golden fixture in `tests/golden/tokens`.
///
/// `UPDATE_GOLDEN=1` rewrites the fixture first.
#[cfg(test)]
pub(crate) fn assert_matches_golden_token(name: &str, actual: &str) {
    let path = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/tokens"))
        .join(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().expect("fixture parent dir"))
            .expect("create golden dir");
        std::fs::write(&path, actual).expect("write golden fixture");
    }
    assert!(
        path.exists(),
        "golden token `{name}` is missing; run with UPDATE_GOLDEN=1 to write it"
    );
    let expected = std::fs::read_to_string(&path).expect("read golden token");
    assert_eq!(expected, actual, "golden token `{name}` changed");
}

/// Returns the wall-clock reading at which a commit stamped at
/// `committed_at_ms` is one default garbage collection grace window old, so a
/// retention advance measured at that reading can pass the commit.
#[cfg(test)]
pub(crate) fn grace_elapsed_after(committed_at_ms: u64) -> u64 {
    committed_at_ms + crate::limits::GC_DEFAULT_GRACE_WINDOW_MS
}

/// Raises the namespace's hint to its current manifest, as the runtime that
/// published that manifest does after the publication.
///
/// A publication in core leaves the hint where it was, and a runtime skips a
/// raise less than `HINT_RAISE_INTERVAL_MS` after its last one, so a test
/// that needs garbage collection or discovery to see a current hint calls
/// this.
pub async fn raise_hint_to_current_manifest<S: loonfs_objectstore::ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &loonfs_types::NamespaceId,
) {
    let current = crate::namespace::control::load_current_manifest(store, namespace_id)
        .await
        .expect("load the current manifest")
        .state
        .manifest()
        .manifest_no;
    crate::namespace::control::raise_namespace_hint(store, namespace_id, current, None)
        .await
        .expect("raise the hint");
}

/// Builds the context a test mutation runs under: `writer_id` at `now_ms`.
pub fn mutation_context(writer_id: &str, now_ms: u64) -> crate::MutationContext {
    crate::MutationContext {
        writer_id: loonfs_types::WriterId::parse(writer_id).expect("a test writer id should parse"),
        now_ms,
    }
}

/// Returns the content head of `content_id` that commit validation reads,
/// from a view over `state` at its newest sequence.
pub fn content_head(
    state: &crate::metadata::MetadataState,
    content_id: &loonfs_types::ContentId,
) -> Option<u64> {
    use futures::FutureExt;
    let tail = crate::wal::ProjectedWalTail::from_rows(state.clone());
    crate::metadata::InMemoryMetadataView::in_memory(&tail, None, state.indexed_seq())
        .content_head(content_id)
        .now_or_never()
        .expect("a view without manifest segments should answer without waiting")
        .expect("a view without manifest segments should not fail")
}

// These path-mutation helpers use dev-dependencies and are only needed by
// in-crate checkpoint and planner tests.
#[cfg(test)]
mod content_write;
#[cfg(test)]
pub(crate) mod ops;

use crate::time::{Deadline, StdMonotonicTimer};
use std::sync::Arc;

/// Appends one small commit per WAL object without folding the tail.
#[cfg(any(test, feature = "test-support"))]
pub async fn append_wal_objects<S: loonfs_objectstore::ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &loonfs_types::NamespaceId,
    count: u64,
    context: &crate::MutationContext,
) -> crate::error::Result<()> {
    let mut engine = crate::publish::NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    for _ in 0..count {
        let commit_id = loonfs_types::CommitId::generate();
        let path = loonfs_types::AbsolutePath::parse(format!("/wal-tail-{commit_id}"))
            .expect("a generated WAL-tail test path should parse");
        let request = crate::publish::CommitIntent::single(
            commit_id,
            loonfs_types::ActorId::loonfs(),
            None,
            crate::publish::FilesystemOperation::CreateDirectory {
                path,
                parents: false,
            },
        );
        let mut results = engine
            .publish_batch(
                store,
                vec![crate::publish::CommitCandidate::new(request)],
                context,
                &Deadline::start(Arc::new(StdMonotonicTimer::default())),
            )
            .await
            .results;
        assert_eq!(
            results.len(),
            1,
            "a single-candidate test publish should return one result"
        );
        results
            .pop()
            .expect("a single-candidate test publish should hold one result")?;
    }
    Ok(())
}
