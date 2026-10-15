//! The heads that runtime reads start from: the position of the namespace's
//! writer session in this runtime, or else the read cache's anchor. An
//! anchor's WAL probe finds new commits, and the interval check of either
//! head finds a newer manifest. A bounded read skips both while the anchor's
//! head was confirmed less than its interval ago.

use crate::fs::RuntimeCore;
use crate::metadata_cache::CacheLookup;
use crate::metrics::ReadHeadSource;
use crate::trace::phase_span;
use crate::{CoreError, Freshness, NamespaceId, PinId};
use crate::{Error, Result};
use loonfs_core::cache::{CachedReadAnchor, WalTailProjectionCacheKey};
use loonfs_core::control::{
    load_checkpoint_read_basis, load_read_anchor_cached, load_snapshot_read_basis,
    manifest_has_successor, project_anchor_tail, CheckpointReadBasis, MetadataBasis,
    NamespaceReadAnchor, NamespaceReadState, VerifiedNamespaceCatalogEntry,
};
use loonfs_core::time::Observation;
use loonfs_core::{MetadataProjectionLoadError, RuntimeReadContext};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::Instrument;

mod read_position;

pub(crate) use read_position::{HeadPositions, ReadPositionSlot};

impl RuntimeCore {
    /// Returns the read context of the head that the namespace's next read
    /// starts from, without checking that the head is current, or `None`
    /// when this runtime has no head for the namespace yet.
    pub(crate) fn cached_read_context(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<RuntimeReadContext> {
        if let Some(position) = self.writer_position(namespace_id) {
            return Some(self.read_context(position.head.clone(), position.basis.clone()));
        }
        let anchor = self.inner.head_state.get_anchor(namespace_id)?;
        Some(self.read_context(anchor.head.clone(), anchor.basis.clone()))
    }

    /// Looks up the head for a load and counts the hit or miss. A
    /// speculative read peeks first and then loads, so only the load counts.
    fn find_namespace_head(&self, namespace_id: &NamespaceId) -> Option<Arc<CachedReadAnchor>> {
        let head = self.inner.head_state.get_anchor(namespace_id);
        self.inner
            .metadata_cache
            .record_head_anchor_lookup(if head.is_some() {
                CacheLookup::Hit
            } else {
                CacheLookup::Miss
            });
        head
    }

    pub(crate) async fn load_namespace_head_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<Arc<CachedReadAnchor>, CoreError> {
        let validation = self
            .inner
            .head_state
            .peek_anchor(namespace_id)
            .map(|head| Arc::clone(&head.validation))
            .unwrap_or_default();
        let observed_validation_no = validation.started.load(Ordering::SeqCst);
        let _validation = validation
            .lock
            .lock()
            .instrument(tracing::debug_span!(
                target: "loonfs::page",
                "loonfs.phase",
                phase = "validation_wait",
                namespace_id = %namespace_id,
            ))
            .await;
        let reusable = self
            .inner
            .head_state
            .get_anchor(namespace_id)
            .filter(|head| {
                Arc::ptr_eq(&head.validation, &validation)
                    && head.completed_validation_no > observed_validation_no
            });
        if let Some(head) = reusable {
            // A successful validation started after this read arrived, so
            // its observation is inside our read interval. A probe already
            // in flight when we arrived can't authorize this shortcut,
            // because it may have observed the store before an intervening
            // write completed.
            let _span = tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "validation_reuse").entered();
            self.inner
                .metadata_cache
                .record_head_anchor_lookup(CacheLookup::Hit);
            return Ok(head);
        }
        // Saturate instead of wrapping: at exhaustion reads simply stop
        // sharing, since no later validation number can exceed the observed one.
        let validation_no = validation
            .started
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
            .map(|previous| previous + 1)
            .unwrap_or(u64::MAX);
        let result = self
            .refresh_namespace_head(namespace_id)
            .instrument(tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "validation_refresh"))
            .await;
        match result {
            Ok(mut head) => {
                head.validation = Arc::clone(&validation);
                head.completed_validation_no = validation_no;
                let head = Arc::new(head);
                self.inner.head_state.insert_anchor(Arc::clone(&head));
                Ok(head)
            }
            Err(error) => {
                self.inner.head_state.invalidate_anchor(namespace_id);
                Err(error)
            }
        }
    }

    async fn refresh_namespace_head(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<CachedReadAnchor, CoreError> {
        if let Some(cached) = self.find_namespace_head(namespace_id) {
            let mut head = CachedReadAnchor::clone(&cached);
            // Measure every answer against the previous check, not against the probe
            // it answers. A successor published after that check cannot be collected
            // within the bound. An answer that arrives later may miss one, however
            // recently its probe was sent, so the new check replaces the previous one
            // only after both answers are in.
            if let Some(checked) = head
                .basis_checked
                .clone()
                .filter(Observation::is_within_revalidation_bound)
            {
                let interval_ms = self.inner.config.manifest_revalidation_interval_ms;
                let check_due = checked.age_ms() >= interval_ms;
                let observed = Observation::now(Arc::clone(&self.inner.timer));
                let matches = !check_due || !manifest_has_successor(self.store(), namespace_id, head.basis.manifest_no())
                    .instrument(tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "validation_manifest_probe"))
                    .await.map_err(MetadataProjectionLoadError::LoadHead)?;
                if matches {
                    let mut context = self.read_context(head.head.clone(), head.basis.clone());
                    let probed = loonfs_core::control::probe_namespace_wal(self.store(), &mut context)
                        .instrument(tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "validation_wal_probe"))
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
                            head.basis_checked = Some(observed);
                        }
                        head.head = context.head;
                        return Ok(head);
                    }
                }
            }
        }
        let observed = Observation::now(Arc::clone(&self.inner.timer));
        let loaded = load_read_anchor_cached(
            self.store(),
            &self.inner.object_cache,
            namespace_id,
            observed.clone(),
        )
            .instrument(tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "validation_anchor_load"))
            .await
            .map_err(MetadataProjectionLoadError::LoadHead)?;
        if !loaded.read_state.status.is_deleted() {
            let tail = project_anchor_tail(
                self.store(),
                Some(self.inner.metadata_segment_cache.as_ref()),
                &loaded,
            )
            .await?;
            self.inner.head_state.insert_tail(
                WalTailProjectionCacheKey {
                    namespace_id: namespace_id.clone(),
                    manifest_no: loaded.basis().manifest_no(),
                    head_seq: loaded.read_state.seq,
                },
                tail,
            );
        }
        Ok(cached_anchor(loaded, observed))
    }

    /// Returns the read context of the namespace's current head.
    ///
    /// In a runtime with the namespace's writer session, the head is the
    /// session's position (see [`Self::checked_writer_position`]), whatever
    /// the freshness. Otherwise it's the read cache's anchor. A bounded read
    /// takes the anchor as it is while its head was confirmed less than the
    /// read's interval ago (see [`Self::bounded_anchor`]), and every other
    /// read revalidates or loads the anchor.
    pub(crate) async fn current_read_context(
        &self,
        namespace_id: &NamespaceId,
        freshness: Freshness,
    ) -> Result<RuntimeReadContext> {
        let (context, source) = match self.checked_writer_position(namespace_id).await? {
            Some(position) => (
                self.read_context(position.head.clone(), position.basis.clone()),
                ReadHeadSource::Position,
            ),
            None => {
                let (anchor, source) = match self.bounded_anchor(namespace_id, freshness) {
                    Some(anchor) => (anchor, ReadHeadSource::Bounded),
                    None => (
                        self.load_namespace_head_cached(namespace_id).await?,
                        ReadHeadSource::Anchor,
                    ),
                };
                (
                    self.read_context(anchor.head.clone(), anchor.basis.clone()),
                    source,
                )
            }
        };
        self.inner.instruments.read_head(source);
        Ok(context)
    }

    /// Returns the read cache's anchor for a bounded read that can take it
    /// without a request, or `None` when the read confirms the head the way a
    /// `latest` read does.
    ///
    /// A bounded read takes the anchor while the anchor's head was confirmed
    /// less than the read's interval ago, so the read sees every commit
    /// acknowledged before that confirmation. The anchor's manifest check
    /// must also be younger than the revalidation bound, because garbage
    /// collection may delete the segments of a superseded manifest once the
    /// bound passes (format section 4.2).
    fn bounded_anchor(
        &self,
        namespace_id: &NamespaceId,
        freshness: Freshness,
    ) -> Option<Arc<CachedReadAnchor>> {
        let Freshness::Bounded { interval } = freshness else {
            return None;
        };
        let interval_ms = interval.map_or(self.inner.config.bounded_read_interval_ms, |interval| {
            u64::try_from(interval.as_millis()).unwrap_or(u64::MAX)
        });
        let anchor = self.inner.head_state.get_anchor(namespace_id)?;
        let confirmed = anchor.head_confirmed.age_ms() < interval_ms
            && anchor
                .basis_checked
                .as_ref()
                .is_some_and(Observation::is_within_revalidation_bound);
        if !confirmed {
            return None;
        }
        self.inner
            .metadata_cache
            .record_head_anchor_lookup(CacheLookup::Hit);
        Some(anchor)
    }

    pub(crate) fn read_context(
        &self,
        head: NamespaceReadState,
        basis: MetadataBasis,
    ) -> RuntimeReadContext {
        RuntimeReadContext {
            head,
            basis,
            segment_cache: Arc::clone(&self.inner.metadata_segment_cache),
            head_state: Arc::clone(&self.inner.head_state),
            object_cache: Arc::clone(&self.inner.object_cache),
        }
    }

    /// The shared preamble of every pinned read: the read context of the
    /// namespace's current head. The context's `head` is the head the read
    /// is pinned to, and the namespace's immutable identity travels inside
    /// it, so a read resolves everything it needs from one object.
    pub(crate) async fn pinned_read(
        &self,
        namespace_id: &NamespaceId,
        freshness: Freshness,
    ) -> Result<(
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let read_context = self.current_read_context(namespace_id, freshness).await?;
        Ok((self.reader_engine(namespace_id), read_context))
    }

    /// Pins the metadata view captured by a checkpoint.
    pub(crate) async fn pinned_read_at_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &PinId,
    ) -> Result<(
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let live = self
            .current_read_context(namespace_id, Freshness::Latest)
            .await?;
        let pinned = load_checkpoint_read_basis(
            self.store(),
            Some(self.inner.metadata_segment_cache.as_ref()),
            &live.head,
            checkpoint_id,
        )
        .await
        .map_err(Error::from)?;
        Ok(self.pinned_read_at_basis(namespace_id, pinned, live))
    }

    /// Pins a snapshot-owned checkpoint while enforcing its live lease.
    pub(crate) async fn pinned_read_at_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &PinId,
    ) -> Result<(
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let live = self
            .current_read_context(namespace_id, Freshness::Latest)
            .await?;
        let pinned = load_snapshot_read_basis(
            self.store(),
            Some(self.inner.metadata_segment_cache.as_ref()),
            &live.head,
            snapshot_id,
            self.now_ms(),
        )
        .await
        .map_err(Error::from)?;
        Ok(self.pinned_read_at_basis(namespace_id, pinned, live))
    }

    fn pinned_read_at_basis(
        &self,
        namespace_id: &NamespaceId,
        pinned: CheckpointReadBasis,
        live: RuntimeReadContext,
    ) -> (
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    ) {
        (
            self.reader_engine(namespace_id)
                .with_authorization_head(live),
            self.read_context(pinned.head, pinned.basis),
        )
    }

    /// Pins the latest metadata view and records the read in the view-read
    /// metric.
    pub(crate) async fn pinned_metadata_read(
        &self,
        namespace_id: &NamespaceId,
        freshness: Freshness,
    ) -> Result<(
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let pinned = self.pinned_read(namespace_id, freshness).await?;
        self.inner.instruments.latest_metadata_view_read();
        Ok(pinned)
    }

    /// Returns the namespace's immutable identity, read off the namespace's
    /// current head.
    pub(crate) async fn load_namespace_catalog_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<VerifiedNamespaceCatalogEntry> {
        let context = self
            .current_read_context(namespace_id, Freshness::Latest)
            .await?;
        Ok(VerifiedNamespaceCatalogEntry::from_head(&context.head))
    }

    /// Drops the namespace's head anchor, so its next read revalidates from
    /// the store. A writer runtime calls `PublisherRegistry::invalidate_namespace`
    /// instead, which also tells the namespace's publisher what changed.
    ///
    /// Cached WAL-tail projections stay, the tails writers publish from
    /// included. A tail is keyed by namespace, manifest number, and head
    /// sequence, and that key names one immutable fact: a namespace id names
    /// one lifetime, each manifest and WAL number names one object that is
    /// never replaced or recreated, a fence adds no rows, and a writer inserts
    /// only a tail it read from the store, a put it saw land, or the commits
    /// since a fold it published. A tail whose key the reloaded anchor no
    /// longer uses is never wrong, only unused, and recency evicts it.
    pub(crate) fn invalidate_namespace_read_cache(&self, namespace_id: &NamespaceId) {
        let _span = phase_span!(self, "update_cache", namespace_id).entered();
        self.inner.head_state.invalidate_anchor(namespace_id);
    }
}

/// Returns the anchor that a load observed at `observed` read, with both its
/// manifest check and its head confirmed at that observation.
fn cached_anchor(anchor: NamespaceReadAnchor, observed: Observation) -> CachedReadAnchor {
    CachedReadAnchor {
        basis: anchor.basis(),
        head: anchor.read_state,
        basis_checked: Some(observed.clone()),
        head_confirmed: observed,
        validation: Arc::default(),
        completed_validation_no: 0,
    }
}
