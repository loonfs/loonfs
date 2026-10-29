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

mod content_write;
pub mod ops;

use crate::engine::NamespaceWriterEngine;
use crate::manifest::{MetadataRunPolicy, MetadataSegmentCache};
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{NamespaceId, WallClock, WriterId};
use std::sync::Arc;

/// Returns `namespace_id`'s components over `store`, for a test whose caches,
/// clocks, write permits, and run policy don't matter.
///
/// The components are a segment cache that keeps no blocks, the system
/// clocks, 32 content write permits, and the default run policy.
pub fn namespace_store<S>(store: S, namespace_id: &NamespaceId) -> NamespaceStore<S> {
    NamespaceStore::new(
        store,
        namespace_id.clone(),
        Arc::new(MetadataSegmentCache::unshared(0)),
        Arc::new(crate::time::SystemWallClock),
        std_timer(),
        Arc::new(content_writes()),
        MetadataRunPolicy::default(),
    )
}

/// Returns the components of [`namespace_store`] with `wall_clock` as their
/// wall clock, for a test that sets the time its operations record.
pub fn namespace_store_with_wall_clock<S>(
    store: S,
    namespace_id: &NamespaceId,
    wall_clock: Arc<dyn WallClock>,
) -> NamespaceStore<S> {
    NamespaceStore {
        wall_clock,
        ..namespace_store(store, namespace_id)
    }
}

/// A wall clock that always reads the same time.
#[derive(Debug)]
struct FixedWallClock(u64);

impl WallClock for FixedWallClock {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

/// One namespace that a test writes to: its components, whose wall clock
/// reads one fixed time, and the writer ID that its commits record.
#[derive(Debug, Clone)]
pub struct NamespaceFixture<S> {
    pub namespace: NamespaceStore<S>,
    pub writer_id: WriterId,
}

impl<S: ObjectStore + Clone> NamespaceFixture<S> {
    /// Returns a fixture over `store` whose clock reads `context.now_ms` and
    /// whose writer is `context.writer_id`, with the other components of
    /// [`namespace_store`].
    pub fn new(store: S, namespace_id: &NamespaceId, context: &crate::MutationContext) -> Self {
        Self {
            namespace: namespace_store_with_wall_clock(
                store,
                namespace_id,
                Arc::new(FixedWallClock(context.now_ms)),
            ),
            writer_id: context.writer_id.clone(),
        }
    }

    /// Returns a writable engine over the fixture's namespace, which commits
    /// as the fixture's writer at the fixture's time.
    pub fn engine(&self) -> NamespaceWriterEngine<S> {
        NamespaceWriterEngine::writer(self.namespace.clone(), self.writer_id.clone())
    }

    /// Returns the context the fixture's commits run under.
    pub fn context(&self) -> crate::MutationContext {
        crate::MutationContext {
            writer_id: self.writer_id.clone(),
            now_ms: self.namespace.wall_clock.now_ms(),
        }
    }
}
