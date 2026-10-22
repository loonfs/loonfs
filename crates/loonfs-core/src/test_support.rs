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

/// Builds the context a test mutation runs under: `writer_id` at `now_ms`.
pub fn mutation_context(writer_id: &str, now_ms: u64) -> crate::MutationContext {
    crate::MutationContext {
        writer_id: loonfs_types::WriterId::parse(writer_id).expect("a test writer id should parse"),
        now_ms,
    }
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
