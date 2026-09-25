//! Read failure classification and one refresh of an ordinary read.

use super::ReadCore;
use crate::{CoreError, NamespaceId, Result, RuntimeError, SharedObjectStore};
use loonfs_core::{
    ManifestLoadError, MetadataProjectionLoadError, NamespaceReaderEngine, RuntimeReadContext,
};
use tracing::Instrument;

pub(super) async fn classify_read_result<T>(
    store: &dyn loonfs_objectstore::ObjectStore,
    context: &RuntimeReadContext,
    result: Result<T>,
) -> Result<T> {
    if matches!(
        &result,
        Err(RuntimeError::Core(CoreError::MetadataProjection(
            MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::MissingSegment { .. })
        )))
    ) {
        let current = loonfs_core::control::load_namespace_current_manifest(
            store,
            &context.head.namespace_id,
        )
        .await
        .map_err(CoreError::from)?;
        let expected_manifest_no = context.basis.manifest_no();
        let actual_manifest_no = current.state.envelope.payload().manifest_no;
        if actual_manifest_no > expected_manifest_no {
            return Err(RuntimeError::StaleHead {
                expected_manifest_no,
                actual_manifest_no,
            });
        }
    }
    result
}

impl ReadCore {
    pub(crate) async fn read<T, F, Fut>(&self, namespace_id: &NamespaceId, read: F) -> Result<T>
    where
        F: Fn(NamespaceReaderEngine<SharedObjectStore>, RuntimeReadContext) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let (engine, context) = self
            .pinned_metadata_read(namespace_id)
            .instrument(
                tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "pin_read"),
            )
            .await?;
        let result =
            classify_read_result(self.store(), &context, read(engine, context.clone()).await).await;
        if !matches!(result, Err(RuntimeError::StaleHead { .. })) {
            return result;
        }
        self.invalidate_namespace_read_cache(namespace_id);
        let (engine, context) = self
            .pinned_metadata_read(namespace_id)
            .instrument(
                tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "pin_read"),
            )
            .await?;
        classify_read_result(self.store(), &context, read(engine, context.clone()).await).await
    }
}
