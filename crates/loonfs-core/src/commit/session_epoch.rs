//! [`SessionEpoch`], through which a writer session sends every put that it
//! makes as the namespace's writer.

use super::published_manifest::PublishedManifest;
use crate::error::Result;
use crate::namespace::head_cache::HeadAndWalTailCache;
use crate::namespace::store::NamespaceStore;
use loonfs_types::NamespaceId;
use std::future::Future;

/// A writer session's epoch, through which the session sends each of its
/// puts as the writer: a commit's WAL put, a floor object's put, and every
/// manifest put of an item of its publication lane (format spec, "Failed
/// and unknown outcomes"). A claim's puts and the publications that
/// initialize a namespace aren't sent under an epoch.
///
/// The session keeps its epoch across the put. A put whose outcome stays
/// unknown, or whose future is dropped or panics while the put is in
/// flight, ends the epoch at once, so the session has no position from then
/// on, and its next publication claims a new epoch and writes a fence. A put
/// whose outcome the session learns changes nothing, so a known outcome
/// never restores an epoch that an unknown outcome of a put in the other
/// queue ended.
///
/// A put that ends, however it ends, while the session has no epoch also
/// drops this runtime's cached head of the namespace before the put's
/// caller hears the outcome. The session then has no position to record
/// the put in, and a read that ran meanwhile may have cached a head below
/// the put, so the next read loads the head from the store instead.
#[derive(Debug)]
pub(crate) struct SessionEpoch<'a> {
    session: &'a PublishedManifest,
    heads: &'a HeadAndWalTailCache,
    namespace_id: &'a NamespaceId,
}

impl<'a> SessionEpoch<'a> {
    /// Returns the epoch of the session of `namespace` whose status
    /// `session` keeps.
    pub(crate) fn new<S>(session: &'a PublishedManifest, namespace: &'a NamespaceStore<S>) -> Self {
        Self {
            session,
            heads: &namespace.head_and_wal_tail_cache,
            namespace_id: &namespace.namespace_id,
        }
    }

    /// Sends `put` under the epoch and returns its result. The session loses
    /// its epoch when the result is `outcome_unknown` (see
    /// [`CoreError::is_outcome_unknown`](crate::error::CoreError::is_outcome_unknown)),
    /// and when the returned future is dropped, or panics, before `put`
    /// returns.
    pub(crate) async fn send_put<T>(&self, put: impl Future<Output = Result<T>>) -> Result<T> {
        let mut in_flight = PutInFlight {
            epoch: self,
            known: false,
        };
        let result = put.await;
        in_flight.known = !result
            .as_ref()
            .is_err_and(crate::error::CoreError::is_outcome_unknown);
        result
    }
}

/// A put of the session in flight, which ends the session's epoch when it's
/// dropped before the put's outcome is known, and drops the cached head when
/// the session has no epoch at the put's end (see [`SessionEpoch`]).
struct PutInFlight<'a> {
    epoch: &'a SessionEpoch<'a>,
    known: bool,
}

impl Drop for PutInFlight<'_> {
    fn drop(&mut self) {
        if !self.epoch.session.settle_put(self.known) {
            self.epoch.heads.invalidate_head(self.epoch.namespace_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::engine::WriterSessionStatus;
    use crate::namespace::writer_epoch::AcquiredWriter;
    use crate::time::Observation;
    use loonfs_test_support::clock::ManualClock;
    use loonfs_types::format::manifest::NamespaceManifestPayload;
    use std::assert_matches;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    /// Checks that a known outcome of one queue's put doesn't restore the
    /// epoch that an unknown outcome of the other queue's put ended while
    /// both puts were in flight (`writer_lane`
    /// `MC_TwoQueuesKnownRestoresEpoch`), and that the session keeps its
    /// epoch while both are in flight.
    #[tokio::test]
    async fn a_known_outcome_does_not_restore_the_epoch_after_the_other_queues_unknown_outcome() {
        let session = PublishedManifest::default();
        session.lock().acquire(
            AcquiredWriter {
                writer_id: loonfs_test_support::ids::writer_id("writer-a"),
                writer_epoch: loonfs_types::WriterEpoch(1),
            },
            crate::test_support::held_manifest(
                NamespaceManifestPayload::initial(
                    loonfs_test_support::ids::namespace_id("demo"),
                    loonfs_test_support::ids::numbered_pool_id(1),
                    1_000,
                    loonfs_test_support::test_actor(),
                ),
                Observation::now(Arc::new(ManualClock::new(1_000))),
            ),
        );
        let directory = tempfile::tempdir().expect("tempdir");
        let namespace = crate::test_support::namespace_store(
            loonfs_objectstore::local_fs_store::LocalFsStore::new(directory.path()).expect("store"),
            &loonfs_test_support::ids::namespace_id("demo"),
        );
        let epoch = SessionEpoch::new(&session, &namespace);
        let (settle_commit, commit_settled) = oneshot::channel::<()>();
        let (settle_fold, fold_settled) = oneshot::channel::<()>();
        let commit = epoch.send_put(async {
            commit_settled.await.expect("the commit's put settles");
            Ok("written")
        });
        let fold = epoch.send_put(async {
            fold_settled.await.expect("the fold's put settles");
            Err::<(), _>(crate::error::CoreError::OutcomeUnknown {
                object_key: "manifest".to_owned(),
                message: "lost acknowledgment".to_owned(),
            })
        });
        let settle = async {
            tokio::task::yield_now().await;
            assert_matches!(
                *session.lock(),
                WriterSessionStatus::Acquired { .. },
                "the session keeps its epoch while both puts are in flight"
            );
            settle_fold.send(()).expect("the fold's put is in flight");
            tokio::task::yield_now().await;
            settle_commit
                .send(())
                .expect("the commit's put is in flight");
        };
        let (written, unknown, ()) = tokio::join!(commit, fold, settle);
        assert_matches!(written, Ok("written"));
        assert_matches!(unknown, Err(crate::error::CoreError::OutcomeUnknown { .. }));
        assert_matches!(
            *session.lock(),
            WriterSessionStatus::Unacquired,
            "the commit's known outcome leaves the session without its epoch"
        );
    }
}
