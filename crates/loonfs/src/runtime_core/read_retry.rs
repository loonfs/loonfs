//! Read failure classification and one refresh of an ordinary read.

use super::{HeadConfirmation, RuntimeCore};
use crate::pinned_read::PinnedRead;
use crate::read_view::ReadSource;
use crate::trace::phase_span;
use crate::{CommitSeq, Consistency, CoreError, Error, NamespaceId, Result};
use loonfs_core::limits::MAX_CONTENTION_ATTEMPTS;
use loonfs_core::Phase;
use loonfs_core::{ManifestLoadError, MetadataProjectionLoadError, RuntimeReadContext};
use loonfs_types::ManifestNo;
use tracing::Instrument;

/// A read that finds a segment missing returns what a new read of `source`
/// would. A snapshot that still admits a read keeps the segment, so the
/// original error stands as corruption.
///
/// A read at the head that finds its manifest missing does the same when
/// the manifest is at or below the manifest boundary. A missing manifest
/// above the boundary is a corrupt store, and the original error stands.
pub(crate) async fn classify_read_result<T>(
    core: &RuntimeCore,
    context: &RuntimeReadContext,
    source: &ReadSource,
    result: Result<T>,
) -> Result<T> {
    let missing_manifest = match &result {
        Err(Error::Core(CoreError::MetadataProjection(
            MetadataProjectionLoadError::ManifestLoad(error),
        ))) => match (error, source) {
            (ManifestLoadError::MissingSegment { .. }, _) => false,
            (ManifestLoadError::MissingManifest { .. }, ReadSource::Head) => true,
            _ => return result,
        },
        _ => return result,
    };
    if missing_manifest
        && core
            .namespace_store(&context.read_basis().head.namespace_id)
            .above_boundary(context.read_basis().basis.manifest_no(), None)
            .await?
            .0
    {
        return result;
    }
    ensure_source_current(core, context, source).await?;
    result
}

/// Returns what a new read of `source` would return before it reads: for the
/// head, `ManifestReplaced` when a newer manifest replaced the manifest that
/// `context` reads above, and for a snapshot, the snapshot's error when the
/// snapshot no longer admits a read.
pub(crate) async fn ensure_source_current(
    core: &RuntimeCore,
    context: &RuntimeReadContext,
    source: &ReadSource,
) -> Result<()> {
    let namespace = core.namespace_store(&context.read_basis().head.namespace_id);
    match source {
        ReadSource::Head => {
            let replaced_manifest_no = context.read_basis().basis.manifest_no();
            let current = namespace.load_current_manifest().await?;
            let current_manifest_no = current.payload().manifest_no;
            if current_manifest_no > replaced_manifest_no {
                return Err(CoreError::ManifestReplaced {
                    replaced_manifest_no,
                    current_manifest_no,
                }
                .into());
            }
        }
        ReadSource::Snapshot(snapshot_id) => {
            namespace
                .load_snapshot_read_basis(&context.read_basis().head, snapshot_id)
                .await?;
        }
    }
    Ok(())
}

/// Returns the error of a read that found its manifest replaced on every
/// run: `store_contention`, because the `stale_head` code is a write's
/// precondition failure, and the read reports the runs it used instead.
fn replaced_on_every_run(namespace_id: &NamespaceId, current_manifest_no: ManifestNo) -> Error {
    CoreError::StoreContention {
        object_key: format!("{namespace_id} manifest {current_manifest_no}"),
    }
    .into()
}

impl RuntimeCore {
    /// Runs `read` at the namespace's live head, confirmed as `confirmation`
    /// asks, and again at a reloaded head each time a newer manifest replaced
    /// the read's basis, up to `MAX_CONTENTION_ATTEMPTS` reads in all. A read
    /// whose basis was replaced on every attempt returns `store_contention`.
    ///
    /// A read whose maximum staleness is longer than the revalidation bound
    /// returns `invalid_request` before the read sends a request to the store
    /// (see [`Self::current_read_context`]).
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
    /// runs at the confirmed head. A sequence that is still above the
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
        for _ in 1..MAX_CONTENTION_ATTEMPTS {
            let context = pinned.context.clone();
            let result =
                classify_read_result(self, &context, &ReadSource::Head, read(pinned).await).await;
            if !matches!(result, Err(Error::Core(CoreError::ManifestReplaced { .. }))) {
                return result;
            }
            self.reload_replaced_head(namespace_id).await?;
            pinned = self
                .pinned_read(namespace_id, confirmation)
                .instrument(phase_span!(self, Phase::PinRead, namespace_id))
                .await?;
        }
        let context = pinned.context.clone();
        let result =
            classify_read_result(self, &context, &ReadSource::Head, read(pinned).await).await;
        match result {
            Err(Error::Core(CoreError::ManifestReplaced {
                current_manifest_no,
                ..
            })) => Err(replaced_on_every_run(namespace_id, current_manifest_no)),
            result => result,
        }
    }

    /// Runs `attempt`, and runs it again from a reloaded head each time it
    /// returns `ManifestReplaced`, up to `MAX_CONTENTION_ATTEMPTS` runs in
    /// all. If every run returns `ManifestReplaced`, this method returns
    /// `store_contention`.
    ///
    /// `attempt` reads at the namespace's head as the head is when the run
    /// starts, so a run after a reload reads a newer manifest than the run
    /// before it read. [`Self::read_at_least`] runs an ordinary read the same
    /// way at a head that it pins for each run.
    pub(crate) async fn read_at_reloaded_head<T, F, Fut>(
        &self,
        namespace_id: &NamespaceId,
        mut attempt: F,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        for _ in 1..MAX_CONTENTION_ATTEMPTS {
            match attempt().await {
                Err(Error::Core(CoreError::ManifestReplaced { .. })) => {
                    self.reload_replaced_head(namespace_id).await?;
                }
                result => return result,
            }
        }
        match attempt().await {
            Err(Error::Core(CoreError::ManifestReplaced {
                current_manifest_no,
                ..
            })) => Err(replaced_on_every_run(namespace_id, current_manifest_no)),
            result => result,
        }
    }

    /// Loads the namespace's head from the store after a newer manifest
    /// replaced the manifest that this runtime's head of the namespace
    /// reads, so the next read starts from the newer manifest.
    ///
    /// The position becomes the cached head when it's dropped, so the head
    /// is dropped after the position, and then loaded from the store, which
    /// finds the manifest that replaced the read's basis.
    pub(crate) async fn reload_replaced_head(&self, namespace_id: &NamespaceId) -> Result<()> {
        self.drop_writer_position(namespace_id).await;
        self.invalidate_namespace_read_cache(namespace_id);
        self.reload_namespace_head(namespace_id).await?;
        Ok(())
    }

    /// Returns whether a read confirmed as `confirmation` reads an `eventual`
    /// head, with a read that sets no consistency taking the runtime's
    /// default.
    fn reads_eventual(&self, confirmation: HeadConfirmation) -> bool {
        match confirmation {
            HeadConfirmation::Consistency(consistency) => matches!(
                consistency.unwrap_or(self.inner.config.consistency.into()),
                Consistency::Eventual { .. }
            ),
            HeadConfirmation::View => false,
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
    /// `store_contention` after `MAX_CONTENTION_ATTEMPTS` runs.
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
                assert_eq!(runs.into_inner(), MAX_CONTENTION_ATTEMPTS);
            }
        }
    }
}
