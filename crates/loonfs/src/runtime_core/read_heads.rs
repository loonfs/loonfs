//! The heads that runtime reads start from: the position of the namespace's
//! writer session in this runtime, or else the namespace's cached head. Each
//! strong read of a cached head waits for a refresh, whose WAL probe finds
//! new commits and whose interval check finds a newer manifest. Each strong
//! read of the position waits for the session's check for a newer manifest.
//! The reads that need a refresh or a check share one. An eventual read
//! skips these while its head was confirmed less than its maximum staleness
//! ago.

use super::RuntimeCore;
use crate::metadata_cache::CacheLookup;
use crate::metrics::ReadHeadSource;
use crate::pinned_read::PinnedRead;
use crate::trace::phase_span;
use crate::{Consistency, CoreError, NamespaceId, PinId};
use crate::{Error, Result};
use loonfs_core::cache::{CachedHead, ProjectedWalTail, WalTailCacheKey};
use loonfs_core::control::{
    project_anchor_tail, NamespaceReadAnchor, ReadBasis, VerifiedNamespaceCatalogEntry,
};
use loonfs_core::limits::READ_REVALIDATION_BOUND_MS;
use loonfs_core::time::Observation;
use loonfs_core::Phase;
use loonfs_core::{MetadataProjectionLoadError, RuntimeReadContext};
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
    /// Records whether a load found the namespace's head in the read cache.
    fn record_head_lookup(&self, found: bool) {
        self.inner.metadata_cache.record_head_lookup(if found {
            CacheLookup::Hit
        } else {
            CacheLookup::Miss
        });
    }

    /// Returns the namespace's head, confirmed or loaded for a read with
    /// `consistency`.
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
        consistency: Consistency,
    ) -> std::result::Result<Arc<CachedHead>, CoreError> {
        let cache = &self.inner.head_and_wal_tail_cache;
        cache
            .check_or_join_head(
                namespace_id,
                || self.eventual_head(namespace_id, consistency),
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

    /// Loads the namespace's head from the store, without starting from the
    /// cached head, and installs it through
    /// [`HeadAndWalTailCache::replace_head`](loonfs_core::cache::HeadAndWalTailCache::replace_head).
    ///
    /// A read that finds a segment of its basis missing reloads this way,
    /// after it drops the cached head, because the newer manifest that
    /// replaced its basis is what it needs, and a refresh from the cached
    /// head would read the same basis again. The reload shares the refresh
    /// of the head in flight the way a strong read does, because a refresh
    /// that starts after the drop finds that newer manifest too.
    pub(crate) async fn reload_namespace_head(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<Arc<CachedHead>, CoreError> {
        self.inner
            .head_and_wal_tail_cache
            .check_or_join_head(
                namespace_id,
                || None,
                || self.refresh_namespace_head(namespace_id, None),
            )
            .await
    }

    async fn refresh_namespace_head(
        &self,
        namespace_id: &NamespaceId,
        cached: Option<CachedHead>,
    ) -> std::result::Result<CachedHead, CoreError> {
        self.record_head_lookup(cached.is_some());
        let namespace = self.namespace_store(namespace_id);
        if let Some(mut head) = cached {
            // Measure every answer against the previous check, not against the probe
            // it answers. A successor published after that check cannot be collected
            // within the bound. An answer that arrives later may miss one, however
            // recently its probe was sent, so the new check replaces the previous one
            // only after both answers are in.
            let checked = head.basis_checked.clone();
            if checked.is_within_revalidation_bound() {
                let check_due =
                    checked.age_ms() >= self.inner.config.manifest_revalidation_interval_ms;
                let observed = self.observation();
                let matches = !check_due
                    || !namespace
                        .manifest_has_successor(head.read_basis.basis.manifest_no())
                        .instrument(phase_span!(
                            self,
                            Phase::ValidationManifestProbe,
                            namespace_id
                        ))
                        .await
                        .map_err(MetadataProjectionLoadError::LoadHead)?;
                if matches {
                    let mut context = self.read_context(head.read_basis.clone(), None);
                    let probed = loonfs_core::control::probe_namespace_wal(
                        self.store(),
                        &self.inner.head_and_wal_tail_cache,
                        &mut context,
                    )
                    .instrument(phase_span!(self, Phase::ValidationWalProbe, namespace_id))
                    .await;
                    // Check the bound before using the probe's result, so a view that passed
                    // the bound while the probe ran is reloaded whatever the probe returned.
                    // By then, garbage collection might have deleted WAL objects after the
                    // view's tip that a fold moved into a newer manifest, and the probe can
                    // fail on them.
                    if checked.is_within_revalidation_bound()
                        && probed.map_err(MetadataProjectionLoadError::LoadHead)?
                    {
                        head.head_confirmed = observed.clone();
                        if check_due {
                            head.basis_checked = observed;
                        }
                        head.read_basis.head = context.read_basis.head;
                        return Ok(head);
                    }
                }
            }
        }
        let observed = self.observation();
        let loaded = namespace
            .load_read_anchor(observed.clone())
            .instrument(phase_span!(self, Phase::ValidationAnchorLoad, namespace_id))
            .await?;
        if !loaded.read_state.status.is_deleted() {
            let tail =
                project_anchor_tail(self.store(), &self.inner.metadata_segment_cache, &loaded)?;
            self.inner.head_and_wal_tail_cache.insert_tail(
                WalTailCacheKey {
                    namespace_id: namespace_id.clone(),
                    manifest_no: loaded.basis().manifest_no(),
                    head_seq: loaded.read_state.seq,
                },
                tail,
            );
        }
        Ok(cached_head(loaded, observed))
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
    pub(crate) async fn current_read_context(
        &self,
        namespace_id: &NamespaceId,
        confirmation: impl Into<HeadConfirmation>,
    ) -> Result<RuntimeReadContext> {
        let (consistency, position_staleness_ms) = match confirmation.into() {
            HeadConfirmation::Consistency(consistency) => {
                let consistency = consistency.unwrap_or(self.inner.config.consistency.into());
                (consistency, self.max_staleness_ms(consistency))
            }
            HeadConfirmation::View => (Consistency::Strong, Some(READ_REVALIDATION_BOUND_MS)),
        };
        let (context, source) = match self
            .checked_writer_position(namespace_id, position_staleness_ms)
            .await?
        {
            Some(position) => (
                self.read_context(
                    position.read_basis.clone(),
                    Some(Arc::clone(&position.tail)),
                ),
                ReadHeadSource::Position,
            ),
            None => {
                let (head, source) = match self.eventual_head(namespace_id, consistency) {
                    Some(head) => (head, ReadHeadSource::Eventual),
                    None => (
                        self.load_namespace_head_cached(namespace_id, consistency)
                            .await?,
                        ReadHeadSource::Anchor,
                    ),
                };
                (self.read_context(head.read_basis.clone(), None), source)
            }
        };
        self.inner.instruments.read_head(source);
        Ok(context)
    }

    /// Returns the namespace's cached head for an eventual read that can take
    /// it without a request, or `None` when the read confirms the head the
    /// way a `strong` read does.
    ///
    /// An eventual read takes the cached head while the head was confirmed
    /// less than the read's maximum staleness ago, so the read sees every
    /// commit acknowledged before that confirmation. The head's manifest
    /// check must also be younger than the revalidation bound, because
    /// garbage collection may delete the segments of a superseded manifest
    /// once the bound passes (format section 4.2).
    fn eventual_head(
        &self,
        namespace_id: &NamespaceId,
        consistency: Consistency,
    ) -> Option<Arc<CachedHead>> {
        let max_staleness_ms = self.max_staleness_ms(consistency)?;
        let cached = self.inner.head_and_wal_tail_cache.get_head(namespace_id)?;
        let confirmed = cached.head_confirmed.age_ms() < max_staleness_ms
            && cached.basis_checked.is_within_revalidation_bound();
        if !confirmed {
            return None;
        }
        self.record_head_lookup(true);
        Some(cached)
    }

    /// Returns the maximum staleness of a read with `consistency`, or `None`
    /// for a `strong` read.
    fn max_staleness_ms(&self, consistency: Consistency) -> Option<u64> {
        let Consistency::Eventual { max_staleness_ms } = consistency else {
            return None;
        };
        Some(max_staleness_ms.unwrap_or(self.inner.config.max_staleness_ms))
    }

    /// Returns the read context of `head` and `basis`.
    ///
    /// `tail` is the WAL tail of a writer session's position, which reads
    /// from the position take instead of a tail from the read cache. Every
    /// other read passes `None` and looks its tail up in the read cache.
    pub(crate) fn read_context(
        &self,
        read_basis: ReadBasis,
        tail: Option<Arc<ProjectedWalTail>>,
    ) -> RuntimeReadContext {
        RuntimeReadContext { read_basis, tail }
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

    /// Pins the metadata view an unexpired snapshot captured.
    pub(crate) async fn pinned_read_at_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &PinId,
    ) -> Result<PinnedRead> {
        let live = self
            .current_read_context(namespace_id, Consistency::Strong)
            .await?;
        let pinned = self
            .namespace_store(namespace_id)
            .load_snapshot_read_basis(&live.read_basis.head, snapshot_id)
            .await
            .map_err(Error::from)?;
        Ok(self.pinned_read_at_basis(namespace_id, pinned, live))
    }

    fn pinned_read_at_basis(
        &self,
        namespace_id: &NamespaceId,
        pinned: ReadBasis,
        live: RuntimeReadContext,
    ) -> PinnedRead {
        PinnedRead {
            engine: self
                .reader_engine(namespace_id)
                .with_authorization_head(live),
            context: self.read_context(pinned, None),
        }
    }

    /// Returns the namespace's immutable identity, read off the namespace's
    /// current head.
    pub(crate) async fn load_namespace_catalog_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<VerifiedNamespaceCatalogEntry> {
        let context = self
            .current_read_context(namespace_id, HeadConfirmation::View)
            .await?;
        Ok(VerifiedNamespaceCatalogEntry::from_head(
            &context.read_basis.head,
        ))
    }

    /// Drops the namespace's cached head, after this runtime learns of a
    /// change whose effect on the head it can't identify, so the next read
    /// loads the namespace by discovery, whatever its consistency (see
    /// [`HeadAndWalTailCache::invalidate_head`](loonfs_core::cache::HeadAndWalTailCache::invalidate_head)).
    /// A writer runtime calls `PublisherRegistry::invalidate_namespace`
    /// instead, which also tells the namespace's publisher what changed.
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

/// Returns the head that a load observed at `observed` read, with both its
/// manifest check and its head confirmed at that observation.
fn cached_head(anchor: NamespaceReadAnchor, observed: Observation) -> CachedHead {
    CachedHead {
        read_basis: ReadBasis {
            basis: anchor.basis(),
            head: anchor.read_state,
        },
        basis_checked: observed.clone(),
        head_confirmed: observed,
    }
}
