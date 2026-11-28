//! Reads that run again after a newer manifest replaced the manifest that
//! they read above.

use super::{HeadConfirmation, RuntimeCore};
use crate::pinned_read::PinnedRead;
use crate::trace::phase_span;
use crate::{CommitSeq, Consistency, CoreError, Error, NamespaceId, Result};
use loonfs_core::limits::MAX_CONTENTION_ATTEMPTS;
use loonfs_core::Phase;
use loonfs_types::ManifestNo;
use tracing::Instrument;

/// Returns the error of a read that found its manifest replaced on every
/// run: the `StoreContention` variant, which a caller's own
/// [`RuntimeCore::run_again_after_replaced_manifest`] doesn't run again, so
/// a read inside [`crate::Namespace::read_through_views`] runs at most
/// `MAX_CONTENTION_ATTEMPTS` times in all.
fn replaced_on_every_run(namespace_id: &NamespaceId, current_manifest_no: ManifestNo) -> Error {
    CoreError::StoreContention {
        object_key: format!("{namespace_id} manifest {current_manifest_no}"),
    }
    .into()
}

impl RuntimeCore {
    /// Runs `read` at the namespace's live head, confirmed as `confirmation`
    /// asks, and again from the newest manifest, confirmed as a `strong` read
    /// confirms it, each time a newer manifest replaced the read's basis, up
    /// to `MAX_CONTENTION_ATTEMPTS` reads in all (see
    /// [`Self::run_again_after_replaced_manifest`]). A read whose basis was
    /// replaced on every attempt returns `store_contention`.
    pub(crate) async fn read<T, F, Fut>(
        &self,
        namespace_id: &NamespaceId,
        confirmation: impl Into<HeadConfirmation>,
        read: F,
    ) -> Result<T>
    where
        F: Fn(PinnedRead) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.read_at_least(namespace_id, confirmation, None, read)
            .await
    }

    /// Runs `read` as [`Self::read`] does, at a head that is at least
    /// `min_head_seq` when the namespace has reached that sequence.
    ///
    /// `min_head_seq` is the sequence that the request already includes, such as
    /// a cursor's `head_seq`, a change feed page's `after_seq`, or a diff's
    /// `to_seq`. The caller learned that sequence from an earlier read, so a
    /// cached head below the sequence is older than a head that the caller
    /// has seen. When an `eventual` read's cached head is below
    /// `min_head_seq`, the read confirms the head as a `strong` read does and
    /// runs at the confirmed head. With a maximum staleness of zero, an
    /// `eventual` read has already confirmed its head that way, so it doesn't
    /// confirm the head again. A sequence that is still above the
    /// confirmed head reaches `read`, which returns the core's error for it.
    pub(crate) async fn read_at_least<T, F, Fut>(
        &self,
        namespace_id: &NamespaceId,
        confirmation: impl Into<HeadConfirmation>,
        min_head_seq: Option<CommitSeq>,
        read: F,
    ) -> Result<T>
    where
        F: Fn(PinnedRead) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut confirmation = confirmation.into();
        // Boxed, because without the box, a crate that awaits a read fails to
        // compile with "queries overflow the depth limit!" while the compiler
        // computes the layout of the caller's future.
        Box::pin(async move {
            let mut pinned = self
                .pinned_read(namespace_id, confirmation)
                .instrument(phase_span!(self, Phase::PinRead, namespace_id))
                .await?;
            if min_head_seq.is_some_and(|seq| pinned.head_seq() < seq)
                && self.reads_eventual(confirmation)
            {
                confirmation = HeadConfirmation::from(Consistency::Strong);
                pinned = self
                    .pinned_read(namespace_id, confirmation)
                    .instrument(phase_span!(self, Phase::PinRead, namespace_id))
                    .await?;
            }
            // The first run reads at the head pinned above, and each later
            // run pins the head again as a `strong` read does, so a run after
            // a replaced manifest that a writer position of this runtime
            // started from checks that position and reads from the store.
            let mut first = Some(pinned);
            let read = &read;
            self.run_again_after_replaced_manifest(|| {
                let first = first.take();
                async move {
                    let pinned = match first {
                        Some(pinned) => pinned,
                        None => {
                            self.pinned_read(namespace_id, Consistency::Strong)
                                .instrument(phase_span!(self, Phase::PinRead, namespace_id))
                                .await?
                        }
                    };
                    read(pinned).await
                }
            })
            .await
        })
        .await
    }

    /// Runs `attempt`, and runs it again each time it returns
    /// `ManifestReplaced`, up to `MAX_CONTENTION_ATTEMPTS` runs in all. If
    /// every run returns `ManifestReplaced`, this method returns
    /// `store_contention`.
    ///
    /// Before each run after the first, the method drops this runtime's
    /// cached head of the namespace whose manifest the last run found
    /// replaced (see [`Self::invalidate_namespace_read_cache`]), which can
    /// differ from the namespace that the read started from, such as a fork
    /// that a merge reads. `attempt` reads each namespace at its head as the
    /// head is when the run starts, so a run after the drop loads that
    /// namespace's head by discovery, and reads a newer manifest than the run
    /// before it read. A writer session's position stays: its manifest's
    /// segments are missing only after another writer's claim, and the
    /// session finds that claim at its next put or confirmation.
    /// [`Self::read_at_least`] runs an ordinary read through this method,
    /// with a head that it pins for each run.
    pub(crate) async fn run_again_after_replaced_manifest<T, F, Fut>(
        &self,
        mut attempt: F,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        for _ in 1..MAX_CONTENTION_ATTEMPTS {
            match attempt().await {
                Err(Error::Core(CoreError::ManifestReplaced { namespace_id, .. })) => {
                    self.invalidate_namespace_read_cache(&namespace_id);
                }
                result => return result,
            }
        }
        match attempt().await {
            Err(Error::Core(CoreError::ManifestReplaced {
                namespace_id,
                current_manifest_no,
                ..
            })) => Err(replaced_on_every_run(&namespace_id, current_manifest_no)),
            result => result,
        }
    }

    /// Returns whether a read confirmed as `confirmation` can start from a
    /// head that it didn't confirm: an `eventual` read in a runtime whose
    /// maximum staleness is above zero.
    fn reads_eventual(&self, confirmation: HeadConfirmation) -> bool {
        match confirmation {
            HeadConfirmation::Consistency(Consistency::Eventual) => {
                self.inner.config.max_staleness_ms > 0
            }
            HeadConfirmation::Consistency(Consistency::Strong) | HeadConfirmation::View => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{CoreError, Error, ErrorCode, LoonFs};
    use loonfs_core::limits::MAX_CONTENTION_ATTEMPTS;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::ids::namespace_id;
    use loonfs_types::ManifestNo;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Checks that a read through views runs again after each run that
    /// returns `ManifestReplaced`. A read replaced on its first run returns
    /// its second run's result, and a read replaced on every run returns
    /// `store_contention`, with the cause `lost_race`, after
    /// `MAX_CONTENTION_ATTEMPTS` runs.
    #[tokio::test]
    async fn reads_through_views_run_again_after_a_replaced_manifest() {
        let temp_dir = tempdir().expect("tempdir");
        let runtime = LoonFs::builder_with_store(
            Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store")),
            crate::Context::Service,
        )
        .writer_id(loonfs_test_support::ids::writer_id("replaced-reads"))
        .build()
        .await
        .expect("build writer");
        let id = namespace_id("replaced-reads");
        runtime
            .create_namespace(&id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = runtime.namespace(&id);
        for replaced_runs in [1, MAX_CONTENTION_ATTEMPTS] {
            let runs = AtomicUsize::new(0);
            let result = namespace
                .read_through_views(|| async {
                    if runs.fetch_add(1, Ordering::SeqCst) < replaced_runs {
                        return Err(Error::from(CoreError::ManifestReplaced {
                            namespace_id: namespace_id("replaced-reads"),
                            replaced_manifest_no: ManifestNo(1),
                            current_manifest_no: ManifestNo(2),
                        }));
                    }
                    Ok("read")
                })
                .await;
            if replaced_runs < MAX_CONTENTION_ATTEMPTS {
                assert_eq!(result.expect("the second run returns its read"), "read");
                assert_eq!(runs.into_inner(), replaced_runs + 1);
            } else {
                let error = result.expect_err("every run finds its manifest replaced");
                assert_eq!(error.code(), ErrorCode::StoreContention);
                assert_eq!(
                    error.details().and_then(|details| details.cause),
                    Some(loonfs_types::ErrorCause::LostRace)
                );
                assert_eq!(runs.into_inner(), MAX_CONTENTION_ATTEMPTS);
            }
        }
    }
}
