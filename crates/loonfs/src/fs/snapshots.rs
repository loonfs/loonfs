//! Snapshot reads and mutations.

use crate::{
    Checkpoint, DeleteSnapshotResponse, Error, ListSnapshotsResponse, Namespace, Result,
    SnapshotSummary, Writable,
};
use loonfs_core::CheckpointPageCursor;
use loonfs_types::PageRequest;
use loonfs_types::PinId;
use std::num::NonZeroU32;

/// Limits applied to snapshot lifetimes and namespace quota.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotPolicy {
    /// Largest requested lifetime from the current time.
    pub max_ttl_ms: u64,
    /// Largest lifetime from the snapshot's creation time.
    pub max_lifetime_ms: u64,
    /// Most live snapshots one namespace may hold.
    pub max_live_per_namespace: usize,
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        Self {
            max_ttl_ms: 86_400_000,
            max_lifetime_ms: 604_800_000,
            max_live_per_namespace: 16,
        }
    }
}

impl SnapshotPolicy {
    /// Returns the expiry for a snapshot created at `now_ms` with `ttl_ms`,
    /// rejecting a lifetime outside this policy's limits.
    pub fn expires_at_ms(&self, now_ms: u64, ttl_ms: u64) -> Result<u64> {
        let message = if ttl_ms == 0 || ttl_ms > self.max_ttl_ms {
            Some(format!("ttl_ms must be greater than zero and may not exceed the `snapshot.max_ttl_ms` limit of {} milliseconds", self.max_ttl_ms))
        } else if ttl_ms > self.max_lifetime_ms {
            Some(format!(
                "ttl_ms may not exceed the `snapshot.max_lifetime_ms` limit of {} milliseconds",
                self.max_lifetime_ms
            ))
        } else {
            None
        };
        if let Some(message) = message {
            return Err(Error::InvalidRequest {
                message,
                param: "/ttl_ms",
            });
        }
        Ok(now_ms.saturating_add(ttl_ms))
    }
}

/// A pager over live snapshots.
pub type SnapshotsPager = loonfs_types::Pager<ListSnapshotsResponse, Error>;

impl<M> Namespace<M> {
    /// Lists live snapshots.
    pub fn list_snapshots(&self) -> SnapshotsPager {
        let reader = self.read_only();
        loonfs_types::Pager::new(move |request| {
            let reader = reader.clone();
            async move {
                reader
                    .snapshots_page(super::core::decode_page_request(request)?)
                    .await
            }
        })
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_snapshots",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_snapshots",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn snapshots_page(
        &self,
        request: PageRequest<CheckpointPageCursor>,
    ) -> Result<ListSnapshotsResponse> {
        if self.core.subject.is_some() {
            self.core
                .read(&self.namespace_id, |engine, context| async move {
                    Ok(engine.require_administrator(&context).await?)
                })
                .await?;
        }
        self.core.record_trace_context(&tracing::Span::current());
        let now_ms = self.core.now_ms()?;
        let requested = request.limit.as_usize();
        let mut cursor = request.cursor;
        let mut snapshots = Vec::with_capacity(requested);
        let engine = self.core.reader_engine(&self.namespace_id);
        loop {
            let remaining = requested - snapshots.len();
            let limit = NonZeroU32::new(u32::try_from(remaining).map_err(|error| {
                Error::Core(loonfs_core::Error::Internal(format!(
                    "snapshot page limit does not fit u32: {error}"
                )))
            })?)
            .expect("a snapshot page with room remaining has a nonzero limit");
            let page = engine
                .list_checkpoints_page(PageRequest {
                    limit: loonfs_types::EffectiveLimit::new(limit),
                    cursor,
                })
                .await
                .map_err(Error::from)?;
            snapshots.extend(page.items.into_iter().filter_map(|checkpoint| {
                SnapshotSummary::from_checkpoint(checkpoint)
                    .filter(|snapshot| snapshot.is_live(now_ms))
            }));
            match page.next_cursor {
                Some(next_cursor) if snapshots.len() < requested => cursor = Some(next_cursor),
                next_cursor => {
                    return Ok(ListSnapshotsResponse {
                        namespace_id: self.namespace_id.clone(),
                        snapshots,
                        next_cursor: super::core::encode_next_cursor(next_cursor.as_ref())?,
                    })
                }
            }
        }
    }
}

impl Namespace<Writable> {
    /// Creates a snapshot of the current namespace state. The name is a
    /// label that does not need to be unique; `expires_at_ms` is in Unix
    /// milliseconds.
    ///
    /// Returns `snapshot_quota_exceeded` and writes nothing when the namespace
    /// already holds `policy.max_live_per_namespace` live snapshots. It counts
    /// again after it writes the pin and, when the count is over that limit,
    /// deletes the pin and returns the same error, so two creates that race at
    /// the limit can both be refused. A failed delete is logged, and the pin
    /// stays until it expires.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.create_snapshot",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "create_snapshot",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_snapshot(
        &self,
        name: &str,
        expires_at_ms: u64,
        policy: &SnapshotPolicy,
    ) -> Result<Checkpoint> {
        self.core.record_trace_context(&tracing::Span::current());
        self.require_administrator().await?;
        self.ensure_live_snapshot_limit(policy.max_live_per_namespace, 1)
            .await?;
        let engine = self
            .core
            .writer_engine(&self.mode.bits.identity, &self.namespace_id);
        let result = engine
            .create_snapshot(name.to_owned(), expires_at_ms)
            .await
            .map_err(Error::from);
        let checkpoint = self.finish_namespace_mutation(result)?;
        if let Err(error) = self
            .ensure_live_snapshot_limit(policy.max_live_per_namespace, 0)
            .await
        {
            if let Err(cleanup_error) = engine.delete_snapshot(&checkpoint.checkpoint_id).await {
                tracing::warn!(
                    namespace_id = %self.namespace_id,
                    snapshot_id = %checkpoint.checkpoint_id,
                    error = %error,
                    cleanup_error = %cleanup_error,
                    "failed to delete a refused snapshot; it stays until it expires"
                );
            }
            return Err(error);
        }
        Ok(checkpoint)
    }

    async fn ensure_live_snapshot_limit(
        &self,
        max_live: usize,
        additional_live: usize,
    ) -> Result<()> {
        let now_ms = self.core.now_ms()?;
        let page_limit = loonfs_types::PaginationPolicy::default().max_limit();
        let engine = self
            .core
            .writer_engine(&self.mode.bits.identity, &self.namespace_id);
        let mut live = additional_live;
        let mut cursor = None;
        loop {
            let page = engine
                .list_checkpoints_page(PageRequest {
                    limit: loonfs_types::EffectiveLimit::new(page_limit),
                    cursor,
                })
                .await
                .map_err(Error::from)?;
            live += page
                .items
                .into_iter()
                .filter_map(SnapshotSummary::from_checkpoint)
                .filter(|snapshot| snapshot.is_live(now_ms))
                .count();
            if live > max_live {
                return Err(Error::Core(loonfs_core::Error::SnapshotQuotaExceeded {
                    namespace_id: self.namespace_id.clone(),
                    max_live,
                }));
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(());
            };
            cursor = Some(next_cursor);
        }
    }

    /// Extends a live snapshot, capped at `policy.max_lifetime_ms` from its
    /// durable creation time.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.extend_snapshot",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "extend_snapshot",
            namespace_id = %self.namespace_id,
            snapshot_id = %snapshot_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn extend_snapshot(
        &self,
        snapshot_id: &PinId,
        requested_expires_at_ms: u64,
        policy: &SnapshotPolicy,
    ) -> Result<SnapshotSummary> {
        self.require_administrator().await?;
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .core
            .writer_engine(&self.mode.bits.identity, &self.namespace_id)
            .extend_snapshot(snapshot_id, requested_expires_at_ms, policy.max_lifetime_ms)
            .await
            .map_err(Error::from)
            .and_then(|checkpoint| {
                SnapshotSummary::from_checkpoint(checkpoint).ok_or_else(|| {
                    Error::Core(loonfs_core::Error::Internal(
                        "snapshot extension returned a non-snapshot checkpoint".to_owned(),
                    ))
                })
            });
        self.finish_namespace_mutation(result)
    }

    /// Deletes a snapshot pin. A missing id returns `snapshot_not_found`.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.delete_snapshot",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "delete_snapshot",
            namespace_id = %self.namespace_id,
            snapshot_id = %snapshot_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn delete_snapshot(&self, snapshot_id: &PinId) -> Result<DeleteSnapshotResponse> {
        self.require_administrator().await?;
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .core
            .writer_engine(&self.mode.bits.identity, &self.namespace_id)
            .delete_snapshot(snapshot_id)
            .await
            .map_err(Error::from);
        self.finish_namespace_mutation(result)
    }
}
