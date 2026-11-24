//! What keeps a manifest's segments in the store while this process reads
//! them, and the check that a load runs when it finds one of them missing.
//!
//! A collection keeps the segments of a namespace's current manifest and of
//! the manifests that snapshots keep. It can delete the segments of a
//! manifest that a newer manifest replaced as soon as they're older than the
//! grace window, however long that manifest was current (format spec,
//! "Candidate and age rules"). So a process that reads a manifest it keeps
//! in memory can find a segment missing in a store that isn't corrupt. The
//! missing segment means a corrupt store only while the manifest is still
//! current, or the snapshot that keeps it still admits a read. Every segment
//! load of a
//! manifest goes through its [`VerifiedMetadataSegments`](super::VerifiedMetadataSegments),
//! which checks its [`SegmentKeeper`] when a load finds a segment missing.

use crate::engine::RuntimeReadContext;
use crate::error::{CoreError, Result};
use crate::manifest::ManifestLoadError;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{ManifestNo, SnapshotId};
use std::sync::Arc;
use tokio::sync::OnceCell;

/// What keeps the segments of a manifest in the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeptBy {
    /// The manifest is the namespace's current manifest, or was when this
    /// process loaded it.
    Current,
    /// The snapshot with this ID keeps the manifest.
    Snapshot(SnapshotId),
}

/// The check of a namespace's store that a keeper runs.
#[async_trait::async_trait]
pub(crate) trait KeeperChecks: Send + Sync {
    /// Returns `Ok` when `kept_by` still keeps the segments of manifest
    /// `manifest_no`, and otherwise `manifest_replaced` for a replaced
    /// current manifest, `snapshot_not_found` for a snapshot that no longer
    /// admits a read, or the error of the check's request.
    async fn check_kept(&self, manifest_no: ManifestNo, kept_by: &KeptBy) -> Result<()>;
}

#[async_trait::async_trait]
impl<S: ObjectStore> KeeperChecks for NamespaceStore<S> {
    async fn check_kept(&self, manifest_no: ManifestNo, kept_by: &KeptBy) -> Result<()> {
        match kept_by {
            KeptBy::Current => self.ensure_manifest_current(manifest_no).await,
            KeptBy::Snapshot(snapshot_id) => self.ensure_snapshot_readable(snapshot_id).await,
        }
    }
}

#[async_trait::async_trait]
impl<T: KeeperChecks + ?Sized> KeeperChecks for &T {
    async fn check_kept(&self, manifest_no: ManifestNo, kept_by: &KeptBy) -> Result<()> {
        (**self).check_kept(manifest_no, kept_by).await
    }
}

/// What keeps one manifest's segments, the namespace store that checks it,
/// and the result of the first check, which every load of the segments
/// shares.
#[derive(Clone)]
pub(crate) struct SegmentKeeper<'a> {
    checks: Arc<dyn KeeperChecks + 'a>,
    kept_by: KeptBy,
    checked: Arc<OnceCell<Result<()>>>,
}

impl<'a> SegmentKeeper<'a> {
    /// Returns a keeper for `kept_by` whose check runs against `checks`, the
    /// store of the namespace that the manifest or the snapshot belongs to.
    pub(crate) fn new(checks: impl KeeperChecks + 'a, kept_by: KeptBy) -> Self {
        Self {
            checks: Arc::new(checks),
            kept_by,
            checked: Arc::new(OnceCell::new()),
        }
    }

    /// Returns `error`, the error of a load of manifest `manifest_no`'s
    /// segments, or the error of the keeper's check when `error` is a missing
    /// segment and the keeper no longer keeps the segments. The keeper runs
    /// its check once, so concurrent loads that find segments missing share
    /// one check. A check that fails, for example with a store error, is kept
    /// too, so every later load of the same segments returns that error. A
    /// set of segments lives for one read, one commit batch, one fold, one
    /// retention plan or one compaction unit, and each of those returns the
    /// error and starts again from new segments.
    pub(crate) async fn load_error(
        &self,
        manifest_no: ManifestNo,
        error: ManifestLoadError,
    ) -> CoreError {
        if !matches!(error, ManifestLoadError::MissingSegment { .. }) {
            return error.into();
        }
        match self
            .checked
            .get_or_init(|| self.checks.check_kept(manifest_no, &self.kept_by))
            .await
        {
            Ok(()) => error.into(),
            Err(failed) => failed.clone(),
        }
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Returns `Ok` when the manifest that `context` reads above is still
    /// current, or the snapshot that keeps it still admits a read, and
    /// otherwise the error of the keeper's check, for an
    /// extension that finds one of its own segments missing under that
    /// manifest.
    pub async fn ensure_context_kept(&self, context: &RuntimeReadContext) -> Result<()> {
        self.check_kept(
            self.context_read_basis(context).basis().manifest_no(),
            &context.kept_by,
        )
        .await
    }
}
