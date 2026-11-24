//! The heads that runtime reads start from: the position of the namespace's
//! writer session in this runtime, or else the namespace's cached head. Each
//! strong read of a cached head waits for a refresh, whose WAL probe finds
//! new commits and whose interval check finds a newer manifest. Each strong
//! read of the position waits for the session's check for a newer manifest.
//! The reads that need a refresh or a check share one. An eventual read
//! skips these while its head was confirmed less than its maximum staleness
//! ago.

use super::RuntimeCore;
use crate::metrics::ReadHeadSource;
use crate::pinned_read::PinnedRead;
use crate::trace::phase_span;
use crate::{Consistency, CoreError, NamespaceId, SnapshotId};
use crate::{Error, Result};
use loonfs_core::cache::{CachedHead, ProjectedWalTail, WalTailCacheKey};
use loonfs_core::control::{
    project_anchor_tail, ActiveNamespaceCatalogEntry, Held, LoadedManifest, NamespaceReadAnchor,
    ReadBasis, SnapshotReadBasis, VerifiedNamespaceCatalogEntry,
};
use loonfs_core::limits::READ_REVALIDATION_BOUND_MS;
use loonfs_core::time::Observation;
use loonfs_core::Phase;
use loonfs_core::{CreatedSnapshot, MetadataProjectionLoadError, RuntimeReadContext};
use std::sync::Arc;
use tracing::Instrument;

/// How a read confirms the namespace head that it starts from.
#[derive(Debug, Clone, Copy)]
pub(crate) enum HeadConfirmation {
    /// As the read's consistency asks, or as the runtime's default
    /// consistency when the read sets none.
    Consistency(Option<Consistency>),
    /// As a load of the namespace's immutable identity, or a check before a
    /// write that commits through the writer session, needs: the writer
    /// position as it is, in a runtime with the namespace's writer session,
    /// and otherwise the cached head confirmed as a `strong` read confirms
    /// it.
    ///
    /// The position includes every access change and receipt that the
    /// session committed, and another writer's takeover fences the session
    /// at the write's own publish, so a check there needs no request. A
    /// check before an operation that the session doesn't publish, such as
    /// a snapshot change or a fork, confirms the head as a `strong` read.
    View,
}

impl From<Option<Consistency>> for HeadConfirmation {
    fn from(consistency: Option<Consistency>) -> Self {
        Self::Consistency(consistency)
    }
}

impl From<Consistency> for HeadConfirmation {
    fn from(consistency: Consistency) -> Self {
        Self::Consistency(Some(consistency))
    }
}

impl RuntimeCore {
    /// Returns the namespace's head, confirmed or loaded for a read with the
    /// maximum staleness `max_staleness_ms`, or for a `strong` read when
    /// it's `None`.
    ///
    /// One refresh of the namespace's head runs at a time in this runtime,
    /// and the reads that need one share it (see
    /// [`HeadAndWalTailCache::check_or_join_head`](loonfs_core::cache::HeadAndWalTailCache::check_or_join_head)).
    /// A read waits for the refresh in flight, and then an eventual read
    /// takes the head that the refresh confirmed if it's within the read's
    /// maximum staleness. Otherwise the read shares the next refresh, or
    /// runs it. So a wave of concurrent eventual reads costs at most one
    /// refresh, and a wave of strong reads at most two.
    ///
    /// The refresh installs its result through
    /// [`HeadAndWalTailCache::replace_head`](loonfs_core::cache::HeadAndWalTailCache::replace_head),
    /// which keeps the newer of the result and the cached head, so the
    /// cached head doesn't move backwards whatever order a refresh and the
    /// end of a writer position finish in. The reads start from whichever
    /// head that returns.
    pub(crate) async fn load_namespace_head_cached(
        &self,
        namespace_id: &NamespaceId,
        max_staleness_ms: Option<u64>,
    ) -> std::result::Result<Arc<CachedHead>, CoreError> {
        let cache = &self.inner.head_and_wal_tail_cache;
        cache
            .check_or_join_head(
                namespace_id,
                || self.eventual_head(namespace_id, max_staleness_ms),
                || async {
                    let cached = cache.get_head(namespace_id);
                    self.refresh_namespace_head(namespace_id, cached.as_deref().cloned())
                        .instrument(phase_span!(self, Phase::ValidationRefresh, namespace_id))
                        .await
                        .inspect_err(|_| cache.invalidate_head(namespace_id))
                },
            )
            .await
    }

    async fn refresh_namespace_head(
        &self,
        namespace_id: &NamespaceId,
        cached: Option<CachedHead>,
    ) -> std::result::Result<CachedHead, CoreError> {
        let namespace = self.namespace_store(namespace_id);
        if let Some(mut head) = cached {
            // The WAL probe comes first, and the manifest check after its
            // response. If a fold moved a WAL object above the view's tip into
            // a newer manifest and a collection then deleted the WAL object,
            // the WAL probe finds nothing, but the manifest check finds the
            // newer manifest, or the boundary above the view's manifest.
            let check_due = head.basis().checked().age_ms()
                >= self.inner.config.manifest_revalidation_interval_ms;
            let observed = self.observation();
            let probed = head
                .probe_wal(
                    self.store(),
                    &self.inner.head_and_wal_tail_cache,
                    observed.clone(),
                )
                .instrument(phase_span!(self, Phase::ValidationWalProbe, namespace_id))
                .await
                .map_err(MetadataProjectionLoadError::LoadHead)?;
            let current = probed
                && (!check_due
                    || head
                        .check_manifest(&namespace, observed)
                        .instrument(phase_span!(
                            self,
                            Phase::ValidationManifestProbe,
                            namespace_id
                        ))
                        .await?);
            if current {
                return Ok(head);
            }
        }
        let observed = self.observation();
        let loaded = namespace
            .load_read_anchor(observed.clone())
            .instrument(phase_span!(self, Phase::ValidationAnchorLoad, namespace_id))
            .await?;
        if loaded.read_state().status.is_active() {
            let tail = project_anchor_tail(&loaded)?;
            self.inner.head_and_wal_tail_cache.insert_tail(
                WalTailCacheKey {
                    namespace_id: namespace_id.clone(),
                    manifest_no: loaded.basis().manifest_no(),
                    head_seq: loaded.read_state().seq,
                    wal_no: loaded.read_state().wal_no,
                },
                tail,
            );
        }
        Ok(cached_head(&loaded, observed))
    }

    /// Returns the read context of the namespace's current head, confirmed
    /// as `confirmation` asks.
    ///
    /// In a runtime with the namespace's writer session, the head is the
    /// session's position (see [`Self::checked_writer_position`]). Otherwise
    /// it's the namespace's cached head. An eventual read takes either head
    /// as it is while it was confirmed less than the read's maximum
    /// staleness ago (see [`Self::eventual_head`] for the cached head), and
    /// every other read confirms or loads the head.
    ///
    /// Every read with a caller's consistency calls this method before it
    /// starts from a head, so a read whose maximum staleness is longer than
    /// the revalidation bound returns `invalid_request` here, before the read
    /// sends a request to the store (see [`Self::max_staleness_ms`]). An
    /// eventual read therefore starts from the writer position without a new
    /// check only while the position's last check is younger than the
    /// revalidation bound, the same limit that [`Self::eventual_head`]
    /// applies to the cached head's manifest check.
    pub(crate) async fn current_read_context(
        &self,
        namespace_id: &NamespaceId,
        confirmation: impl Into<HeadConfirmation>,
    ) -> Result<RuntimeReadContext> {
        self.with_current_head(namespace_id, confirmation, |basis, tail| {
            self.read_context(basis, tail)
        })
        .await
    }

    /// Finds the namespace's current head as [`Self::current_read_context`]
    /// does, and returns what `build` returns for the head's basis and, for a
    /// writer session's position, the position's WAL tail.
    async fn with_current_head<T>(
        &self,
        namespace_id: &NamespaceId,
        confirmation: impl Into<HeadConfirmation>,
        build: impl FnOnce(&Held<ReadBasis>, Option<Arc<ProjectedWalTail>>) -> Result<T>,
    ) -> Result<T> {
        let (max_staleness_ms, position_staleness_ms) = match confirmation.into() {
            HeadConfirmation::Consistency(consistency) => {
                let consistency = consistency.unwrap_or(self.inner.config.consistency.into());
                let max_staleness_ms = self.max_staleness_ms(consistency)?;
                (max_staleness_ms, max_staleness_ms)
            }
            HeadConfirmation::View => (None, Some(READ_REVALIDATION_BOUND_MS)),
        };
        let (built, source) = match self
            .checked_writer_position(namespace_id, position_staleness_ms)
            .await?
        {
            Some(position) => (
                build(position.basis(), Some(Arc::clone(position.tail()))),
                ReadHeadSource::Position,
            ),
            None => {
                let (head, source) = match self.eventual_head(namespace_id, max_staleness_ms) {
                    Some(head) => (head, ReadHeadSource::Eventual),
                    None => {
                        let cache = &self.inner.head_and_wal_tail_cache;
                        let source = match cache.get_head(namespace_id) {
                            Some(_) => ReadHeadSource::Refreshed,
                            None => ReadHeadSource::Loaded,
                        };
                        let head = self
                            .load_namespace_head_cached(namespace_id, max_staleness_ms)
                            .await?;
                        (head, source)
                    }
                };
                (build(head.basis(), None), source)
            }
        };
        self.inner.instruments.read_head(source);
        built
    }

    /// Returns the namespace's cached head for an eventual read that can take
    /// it without a request, or `None` when the read confirms the head the
    /// way a `strong` read does.
    ///
    /// An eventual read takes the cached head while the head was confirmed
    /// less than the read's maximum staleness ago, so the read sees every
    /// commit acknowledged before that confirmation. The head's manifest
    /// check must also be younger than the revalidation bound: garbage
    /// collection deletes the segments of a replaced manifest once the
    /// manifest is older than the grace window, and the bound is shorter than
    /// the grace window (format section 4.2). A read that still finds a
    /// segment missing runs once more from the current manifest.
    fn eventual_head(
        &self,
        namespace_id: &NamespaceId,
        max_staleness_ms: Option<u64>,
    ) -> Option<Arc<CachedHead>> {
        let max_staleness_ms = max_staleness_ms?;
        let cached = self.inner.head_and_wal_tail_cache.get_head(namespace_id)?;
        let confirmed = cached.head_confirmed().age_ms() < max_staleness_ms
            && cached.basis().checked().is_within_revalidation_bound();
        if !confirmed {
            return None;
        }
        Some(cached)
    }

    /// Returns the maximum staleness of a read with `consistency`, or `None`
    /// for a `strong` read.
    ///
    /// A read's consistency becomes a maximum staleness only in this method,
    /// so a read whose maximum staleness is longer than the revalidation
    /// bound returns `invalid_request` from it.
    fn max_staleness_ms(&self, consistency: Consistency) -> Result<Option<u64>> {
        let Consistency::Eventual { max_staleness_ms } = consistency else {
            return Ok(None);
        };
        let max_staleness_ms = max_staleness_ms.unwrap_or(self.inner.config.max_staleness_ms);
        if max_staleness_ms > READ_REVALIDATION_BOUND_MS {
            return Err(Error::InvalidRequest {
                message: format!(
                    "max_staleness_ms {max_staleness_ms} exceeds the revalidation bound \
                     ({READ_REVALIDATION_BOUND_MS} ms)"
                ),
                param: "max_staleness_ms",
            });
        }
        Ok(Some(max_staleness_ms))
    }

    /// Returns the read context of `basis`, which is a cached head's basis or
    /// a writer position's basis, or `namespace_not_found` or
    /// `namespace_deleted` when the head isn't active.
    ///
    /// `tail` is the WAL tail of a writer session's position, which reads
    /// from the position take instead of a tail from the read cache. Every
    /// other read passes `None` and looks its tail up in the read cache.
    pub(crate) fn read_context(
        &self,
        basis: &Held<ReadBasis>,
        tail: Option<Arc<ProjectedWalTail>>,
    ) -> Result<RuntimeReadContext> {
        RuntimeReadContext::new(basis, tail).map_err(Error::from)
    }

    /// The shared preamble of every pinned read: the read context of the
    /// namespace's current head. The context's `head` is the head the read
    /// is pinned to, and the namespace's immutable identity travels inside
    /// it, so a read resolves everything it needs from one object.
    pub(crate) async fn pinned_read(
        &self,
        namespace_id: &NamespaceId,
        confirmation: impl Into<HeadConfirmation>,
    ) -> Result<PinnedRead> {
        let context = self
            .current_read_context(namespace_id, confirmation)
            .await?;
        Ok(PinnedRead {
            engine: self.reader_engine(namespace_id),
            context,
        })
    }

    /// Pins the metadata view that an unexpired snapshot, or one of the
    /// namespace's fork snapshots, captured.
    pub(crate) async fn pinned_read_at_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &SnapshotId,
    ) -> Result<PinnedRead> {
        let live = self
            .current_read_context(namespace_id, Consistency::Strong)
            .await?;
        let basis = self
            .namespace_store(namespace_id)
            .load_snapshot_read_basis(live.read_basis().head(), snapshot_id)
            .await
            .map_err(Error::from)?;
        Ok(self.pinned_read_at_basis(namespace_id, basis, live))
    }

    /// Pins the metadata view of the snapshot `created`, which this runtime
    /// wrote on the namespace, from the record in memory.
    ///
    /// `verified` is the newest manifest, with the check that the snapshot's
    /// verification made after the record write. That check confirms the
    /// position of the namespace's writer session for the live head, as a
    /// `strong` read would. Without a position that the check confirms, the
    /// live head is confirmed as a `strong` read confirms it.
    pub(crate) async fn pinned_read_at_created_snapshot(
        &self,
        namespace_id: &NamespaceId,
        created: &CreatedSnapshot,
        verified: &Held<LoadedManifest>,
    ) -> Result<PinnedRead> {
        let checked_position = self.record_writer_position_check(namespace_id, verified);
        let live = match checked_position {
            Some(position) => {
                self.inner.instruments.read_head(ReadHeadSource::Position);
                self.read_context(position.basis(), Some(Arc::clone(position.tail())))?
            }
            None => {
                self.current_read_context(namespace_id, Consistency::Strong)
                    .await?
            }
        };
        let basis = self
            .namespace_store(namespace_id)
            .load_created_snapshot_read_basis(live.read_basis().head(), created.record.clone())
            .await
            .map_err(Error::from)?;
        Ok(self.pinned_read_at_basis(namespace_id, basis, live))
    }

    fn pinned_read_at_basis(
        &self,
        namespace_id: &NamespaceId,
        basis: SnapshotReadBasis,
        live: RuntimeReadContext,
    ) -> PinnedRead {
        PinnedRead {
            engine: self.reader_engine(namespace_id),
            context: RuntimeReadContext::at_snapshot(basis, live),
        }
    }

    /// Returns the namespace's immutable identity, read off the namespace's
    /// current head, whatever the head's status.
    pub(crate) async fn load_namespace_catalog_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<VerifiedNamespaceCatalogEntry> {
        self.with_current_head(namespace_id, HeadConfirmation::View, |basis, _| {
            Ok(VerifiedNamespaceCatalogEntry::from_head(
                basis.read_basis().head(),
            ))
        })
        .await
    }

    /// Returns the namespace's immutable identity like
    /// [`Self::load_namespace_catalog_cached`], and refuses a namespace whose
    /// head is initializing or deleted.
    ///
    /// The head is the one that [`HeadConfirmation::View`] confirms, so the
    /// refusal follows another process's tombstone within the revalidation
    /// bound.
    pub(crate) async fn load_active_namespace_catalog_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<ActiveNamespaceCatalogEntry> {
        let context = self
            .current_read_context(namespace_id, HeadConfirmation::View)
            .await?;
        Ok(ActiveNamespaceCatalogEntry::from_context(&context))
    }

    /// Drops the namespace's cached head, so the next read that doesn't start
    /// from a writer position loads the namespace by discovery, whatever its
    /// consistency (see
    /// [`HeadAndWalTailCache::invalidate_head`](loonfs_core::cache::HeadAndWalTailCache::invalidate_head)).
    /// A caller that finds this runtime's view of the namespace out of date
    /// calls [`Self::forget_namespace_view`] instead, which also drops the
    /// position of the namespace's writer session. A release of the
    /// session's engine and a deletion call this method directly, because
    /// the engine has already dropped its own position by then.
    ///
    /// Cached WAL-tail projections stay. A tail is keyed by namespace,
    /// manifest number, and head sequence, and that key identifies one
    /// immutable fact: a namespace id identifies one lifetime, each manifest
    /// and WAL number identifies one object that is never replaced or
    /// recreated, a fence adds no rows, and a read inserts only a tail it
    /// read from the store or extended with WAL objects it read. The tail
    /// that a writer session's position ends with (see
    /// [`ReadPositionSlot::clear`]) is the tail of a head that the session
    /// wrote. A tail whose key the confirmed head no longer uses is never
    /// wrong, only unused, and recency evicts it.
    pub(crate) fn invalidate_namespace_read_cache(&self, namespace_id: &NamespaceId) {
        let _span = phase_span!(self, Phase::UpdateCache, namespace_id).entered();
        self.inner
            .head_and_wal_tail_cache
            .invalidate_head(namespace_id);
    }
}

/// Returns the head that a load observed at `observed` read, with the
/// anchor's check and its head confirmed at that observation.
fn cached_head(anchor: &NamespaceReadAnchor, observed: Observation) -> CachedHead {
    CachedHead::new(anchor.held_basis(), observed)
}
