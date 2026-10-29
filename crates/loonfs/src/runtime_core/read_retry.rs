//! Read failure classification and one refresh of an ordinary read.

use super::RuntimeCore;
use crate::pinned_read::PinnedRead;
use crate::read_view::ReadSource;
use crate::trace::phase_span;
use crate::{Consistency, CoreError, Error, NamespaceId, Result};
use loonfs_core::limits::READ_REVALIDATION_BOUND_MS;
use loonfs_core::Phase;
use loonfs_core::{ManifestLoadError, MetadataProjectionLoadError, RuntimeReadContext};
use tracing::Instrument;

/// A read that finds a segment missing returns what a new read of `source`
/// would. A snapshot that still admits a read keeps the segment, so the
/// original error stands as corruption.
pub(crate) async fn classify_read_result<T>(
    core: &RuntimeCore,
    context: &RuntimeReadContext,
    source: &ReadSource,
    result: Result<T>,
) -> Result<T> {
    if !matches!(
        &result,
        Err(Error::Core(CoreError::MetadataProjection(
            MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::MissingSegment { .. })
        )))
    ) {
        return result;
    }
    let namespace = core.namespace_store(&context.read_basis.head.namespace_id);
    match source {
        ReadSource::Head => {
            let current = namespace.load_current_manifest().await?;
            let expected_manifest_no = context.read_basis.basis.manifest_no();
            let actual_manifest_no = current.payload().manifest_no;
            if actual_manifest_no > expected_manifest_no {
                return Err(Error::StaleHead {
                    expected_manifest_no,
                    actual_manifest_no,
                });
            }
        }
        ReadSource::Snapshot(snapshot_id) => {
            namespace
                .load_snapshot_read_basis(&context.read_basis.head, snapshot_id)
                .await?;
        }
    }
    result
}

impl RuntimeCore {
    /// Runs `read` at the namespace's live head, confirmed as `consistency`
    /// asks, and once more at a reloaded head if a newer manifest replaced
    /// the read's basis.
    ///
    /// A maximum staleness longer than the revalidation bound returns
    /// `invalid_request` before the read sends a request.
    pub(crate) async fn read<T, F, Fut>(
        &self,
        namespace_id: &NamespaceId,
        consistency: Option<Consistency>,
        read: F,
    ) -> Result<T>
    where
        F: Fn(PinnedRead) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if let Some(Consistency::Eventual {
            max_staleness_ms: Some(max_staleness_ms),
        }) = consistency
        {
            if max_staleness_ms > READ_REVALIDATION_BOUND_MS {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "an eventual read's maximum staleness may not exceed the revalidation \
                         bound ({READ_REVALIDATION_BOUND_MS} ms)"
                    ),
                    param: "max_staleness_ms",
                });
            }
        }
        let pinned = self
            .pinned_read(namespace_id, consistency)
            .instrument(phase_span!(self, Phase::PinRead, namespace_id))
            .await?;
        let context = pinned.context.clone();
        let result =
            classify_read_result(self, &context, &ReadSource::Head, read(pinned).await).await;
        if !matches!(result, Err(Error::StaleHead { .. })) {
            return result;
        }
        // The position becomes the cached head when it's dropped, so the head
        // is dropped after the position, and then loaded from the store,
        // which finds the manifest that replaced the read's basis.
        self.drop_writer_position(namespace_id).await;
        self.invalidate_namespace_read_cache(namespace_id);
        self.reload_namespace_head(namespace_id).await?;
        let pinned = self
            .pinned_read(namespace_id, consistency)
            .instrument(phase_span!(self, Phase::PinRead, namespace_id))
            .await?;
        let context = pinned.context.clone();
        classify_read_result(self, &context, &ReadSource::Head, read(pinned).await).await
    }
}
