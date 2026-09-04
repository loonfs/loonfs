//! Snapshot reads and mutations.

use crate::{
    Checkpoint, CheckpointId, CreateSnapshotOptions, FsReader, FsWriter, ListSnapshotsResponse,
    NamespaceId, ReleaseSnapshotResponse, Result, RuntimeError, SnapshotSummary,
};
use loonfs_api::PageRequest;
use loonfs_core::CheckpointPageCursor;
use std::num::NonZeroU32;

/// A pager over live snapshots.
pub type SnapshotsPager = loonfs_api::Pager<ListSnapshotsResponse, RuntimeError>;

impl FsReader {
    /// Creates a snapshot pager beginning at `request.cursor`.
    pub fn list_snapshots_pager(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<CheckpointPageCursor>,
    ) -> SnapshotsPager {
        let cursor = request.cursor.as_ref().map(|cursor| {
            loonfs_api::encode_cursor(cursor).expect("typed checkpoint cursor should encode")
        });
        let limit = request.limit;
        let reader = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let reader = reader.clone();
            let namespace_id = namespace_id.clone();
            async move {
                let cursor = cursor
                    .as_deref()
                    .map(loonfs_api::decode_cursor)
                    .transpose()
                    .map_err(|error| crate::CoreError::InvalidCursor(error.to_string()))?;
                reader
                    .list_snapshots_page(&namespace_id, PageRequest { limit, cursor })
                    .await
            }
        })
    }

    /// Lists one page of live snapshots.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_snapshots",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_snapshots",
            method = "list_snapshots_page",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn list_snapshots_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<CheckpointPageCursor>,
    ) -> Result<ListSnapshotsResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let now_ms = loonfs_core::time::current_time_ms()?;
        let requested = request.limit.as_usize();
        let mut cursor = request.cursor;
        let mut snapshots = Vec::with_capacity(requested);
        let engine = self.core.reader_engine(namespace_id);
        loop {
            let remaining = requested - snapshots.len();
            let limit = NonZeroU32::new(u32::try_from(remaining).map_err(|error| {
                RuntimeError::Core(loonfs_core::Error::Internal(format!(
                    "snapshot page limit does not fit u32: {error}"
                )))
            })?)
            .expect("a snapshot page with room remaining has a nonzero limit");
            let page = engine
                .list_checkpoints_page(PageRequest {
                    limit: loonfs_api::EffectiveLimit::new(limit),
                    cursor,
                })
                .await
                .map_err(RuntimeError::from)?;
            snapshots.extend(page.items.into_iter().filter_map(|checkpoint| {
                SnapshotSummary::from_checkpoint(checkpoint)
                    .filter(|snapshot| snapshot.expires_at_ms > now_ms)
            }));
            match page.next_cursor {
                Some(next_cursor) if snapshots.len() < requested => cursor = Some(next_cursor),
                next_cursor => {
                    return Ok(ListSnapshotsResponse {
                        namespace_id: namespace_id.clone(),
                        snapshots,
                        next_cursor: super::core::encode_next_cursor(next_cursor.as_ref())?,
                    })
                }
            }
        }
    }
}

impl FsWriter {
    /// Creates a snapshot of the current namespace state.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.snapshot_create",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "snapshot_create",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_snapshot(
        &self,
        namespace_id: &NamespaceId,
        options: CreateSnapshotOptions,
    ) -> Result<Checkpoint> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .core
            .writer_engine(&self.bits.identity, namespace_id)
            .create_snapshot(options.name, options.expires_at_ms)
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Creates a snapshot only when the namespace has quota for it.
    ///
    /// A caller that exceeds the quota releases its tentative snapshot before
    /// returning the quota error.
    pub async fn create_snapshot_with_quota(
        &self,
        namespace_id: &NamespaceId,
        options: CreateSnapshotOptions,
        now_ms: u64,
        max_live: usize,
    ) -> Result<Checkpoint> {
        let checkpoint = self.create_snapshot(namespace_id, options).await?;
        if let Err(error) = self
            .ensure_live_snapshot_limit(namespace_id, now_ms, max_live, 0)
            .await
        {
            self.release_snapshot(namespace_id, &checkpoint.checkpoint_id)
                .await?;
            return Err(error);
        }
        Ok(checkpoint)
    }

    async fn ensure_live_snapshot_limit(
        &self,
        namespace_id: &NamespaceId,
        now_ms: u64,
        max_live: usize,
        additional_live: usize,
    ) -> Result<()> {
        let page_limit = loonfs_api::PaginationPolicy::default().max_limit();
        let mut cursor = None;
        let mut live_with_additional = additional_live;
        let quota_error = || {
            RuntimeError::Core(loonfs_core::Error::SnapshotQuotaExceeded {
                namespace_id: namespace_id.clone(),
                max_live,
            })
        };
        if live_with_additional > max_live {
            return Err(quota_error());
        }
        let engine = self.core.writer_engine(&self.bits.identity, namespace_id);
        loop {
            let page = engine
                .list_checkpoints_page(PageRequest {
                    limit: loonfs_api::EffectiveLimit::new(page_limit),
                    cursor,
                })
                .await
                .map_err(RuntimeError::from)?;
            for checkpoint in page.items {
                if SnapshotSummary::from_checkpoint(checkpoint)
                    .is_some_and(|snapshot| snapshot.expires_at_ms > now_ms)
                {
                    live_with_additional = live_with_additional.saturating_add(1);
                    if live_with_additional > max_live {
                        return Err(quota_error());
                    }
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(());
            };
            cursor = Some(next_cursor);
        }
    }

    /// Extends a live snapshot, capped from its durable creation time.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.snapshot_extend",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "snapshot_extend",
            namespace_id = %namespace_id,
            snapshot_id = %snapshot_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn extend_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
        requested_expires_at_ms: u64,
        max_lifetime_ms: u64,
    ) -> Result<SnapshotSummary> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .core
            .writer_engine(&self.bits.identity, namespace_id)
            .extend_snapshot(snapshot_id, requested_expires_at_ms, max_lifetime_ms)
            .await
            .map_err(RuntimeError::from)
            .and_then(|checkpoint| {
                SnapshotSummary::from_checkpoint(checkpoint).ok_or_else(|| {
                    RuntimeError::Core(loonfs_core::Error::Internal(
                        "snapshot extension returned a non-snapshot checkpoint".to_owned(),
                    ))
                })
            });
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Releases a snapshot. Repeated releases succeed.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.snapshot_release",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "snapshot_release",
            namespace_id = %namespace_id,
            snapshot_id = %snapshot_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn release_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
    ) -> Result<ReleaseSnapshotResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .core
            .writer_engine(&self.bits.identity, namespace_id)
            .release_snapshot(snapshot_id)
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }
}
