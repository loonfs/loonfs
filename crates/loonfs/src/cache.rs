//! The head anchors and WAL-tail projections runtime reads start from.
//! WAL probes observe commits; interval checks observe manifest changes.

use crate::fs::RuntimeCore;
use crate::trace::phase_span;
use crate::{CoreError, NamespaceId, PinId};
use crate::{Error, Result};
use loonfs_core::cache::{CachedReadAnchor, WalTailProjectionCacheKey};
use loonfs_core::control::{
    load_checkpoint_read_basis, load_read_anchor, load_snapshot_read_basis, manifest_has_successor,
    project_anchor_tail, CheckpointReadBasis, NamespaceReadAnchor, VerifiedNamespaceCatalogEntry,
};
use loonfs_core::time::Observation;
use loonfs_core::{MetadataProjectionLoadError, RuntimeReadContext};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::Instrument;

impl RuntimeCore {
    pub(crate) fn cached_read_context(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<RuntimeReadContext> {
        let anchor = self.inner.head_state.get_anchor(namespace_id)?;
        Some(self.runtime_read_context(&anchor))
    }

    /// Looks up the head for a load and counts the hit or miss. A
    /// speculative read peeks first and then loads, so only the load counts.
    fn lookup_namespace_head(&self, namespace_id: &NamespaceId) -> Option<Arc<CachedReadAnchor>> {
        let head = self.inner.head_state.get_anchor(namespace_id);
        self.inner
            .metadata_cache
            .record_head_anchor_lookup(head.is_some());
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
            .instrument(phase_span!(self, "namespace_validation_wait", namespace_id))
            .instrument(tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "validation_wait"))
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
            // A successful remote validation STARTED after this read
            // arrived. Its observation is inside our read interval. A
            // probe already in flight when we arrived cannot authorize
            // this shortcut: it may have observed before an intervening
            // write completed. Local publication is not a remote proof.
            let _span = tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "validation_reuse").entered();
            self.inner.metadata_cache.record_head_anchor_lookup(true);
            return Ok(head);
        }
        // Saturate instead of wrapping: at exhaustion reads simply stop
        // sharing, since no later validation number can exceed the observed one.
        let validation_no = validation
            .started
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
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
        if let Some(cached) = self.lookup_namespace_head(namespace_id) {
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
                    let mut context = self.runtime_read_context(&head);
                    if loonfs_core::control::probe_namespace_wal(self.store(), &mut context)
                        .instrument(tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "validation_wal_probe"))
                    .await.map_err(MetadataProjectionLoadError::LoadHead)? && checked.is_within_revalidation_bound() {
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
        let loaded = load_read_anchor(self.store(), namespace_id)
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
        Ok(cached_anchor(loaded, Some(observed)))
    }

    /// Loads the read anchor, mapping an absent head to the one answer it
    /// can mean: the namespace does not exist.
    pub(crate) async fn head_for_metadata_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Arc<CachedReadAnchor>> {
        self.load_namespace_head_cached(namespace_id)
            .await
            .map_err(Error::Core)
    }

    pub(crate) fn runtime_read_context(&self, anchor: &CachedReadAnchor) -> RuntimeReadContext {
        RuntimeReadContext {
            head: anchor.head.clone(),
            basis: anchor.basis.clone(),
            segment_cache: Arc::clone(&self.inner.metadata_segment_cache),
            head_state: Arc::clone(&self.inner.head_state),
        }
    }

    /// The shared preamble of every pinned read: revalidate or load the head
    /// anchor and pin the read context to it. The context's `head` is the
    /// anchor the read is pinned to, and the namespace's immutable identity
    /// travels inside it, so a read resolves everything it needs from one
    /// object.
    pub(crate) async fn pinned_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let anchor = self.head_for_metadata_read(namespace_id).await?;
        let read_context = self.runtime_read_context(&anchor);
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
        let live = self.head_for_metadata_read(namespace_id).await?;
        let pinned = load_checkpoint_read_basis(
            self.store(),
            Some(self.inner.metadata_segment_cache.as_ref()),
            &live.head,
            checkpoint_id,
        )
        .await
        .map_err(Error::from)?;
        Ok(self.pinned_read_at_basis(namespace_id, pinned, &live))
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
        let live = self.head_for_metadata_read(namespace_id).await?;
        let pinned = load_snapshot_read_basis(
            self.store(),
            Some(self.inner.metadata_segment_cache.as_ref()),
            &live.head,
            snapshot_id,
            self.now_ms()?,
        )
        .await
        .map_err(Error::from)?;
        Ok(self.pinned_read_at_basis(namespace_id, pinned, &live))
    }

    fn pinned_read_at_basis(
        &self,
        namespace_id: &NamespaceId,
        pinned: CheckpointReadBasis,
        live: &CachedReadAnchor,
    ) -> (
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    ) {
        let read_context = self.runtime_read_context(&CachedReadAnchor {
            head: pinned.head,
            basis: pinned.basis,
            basis_checked: None,
            validation: Arc::default(),
            completed_validation_no: 0,
        });
        (
            self.reader_engine(namespace_id)
                .with_authorization_head(self.runtime_read_context(live)),
            read_context,
        )
    }

    /// Pins the latest metadata view and records the read in the view-read
    /// metric.
    pub(crate) async fn pinned_metadata_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let pinned = self.pinned_read(namespace_id).await?;
        self.inner.instruments.latest_metadata_view_read();
        Ok(pinned)
    }

    /// Returns the namespace's immutable identity, read off the cached head
    /// anchor.
    pub(crate) async fn load_namespace_catalog_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<VerifiedNamespaceCatalogEntry> {
        let anchor = self.head_for_metadata_read(namespace_id).await?;
        Ok(VerifiedNamespaceCatalogEntry::from_head(&anchor.head))
    }

    /// Seeds the anchor of a landed publish. The publisher's engine has
    /// already put the WAL tail it names in the head-state cache.
    pub(crate) fn seed_namespace_read_cache(
        &self,
        namespace_id: &NamespaceId,
        state: loonfs_core::publish::ResultingReadState,
    ) {
        let (cached_check, validation) = self
            .inner
            .head_state
            .peek_anchor(namespace_id)
            .map(|cached| {
                // The cached check also confirms the seeded view only if it checked
                // the same basis and the seeded tip is not behind the cached one.
                // Otherwise it says nothing about the seeded basis, or about the WAL
                // numbers just after the seeded tip.
                let confirms_seed =
                    cached.basis == state.basis && cached.head.wal_no <= state.head.wal_no;
                (
                    cached.basis_checked.clone().filter(|_| confirms_seed),
                    Arc::clone(&cached.validation),
                )
            })
            .unwrap_or_default();
        let basis_checked = cached_check
            .into_iter()
            .chain([state.basis_checked])
            .min_by_key(Observation::age_ms);
        self.inner
            .head_state
            .insert_anchor(Arc::new(CachedReadAnchor {
                head: state.head,
                basis: state.basis,
                basis_checked,
                validation,
                completed_validation_no: 0,
            }));
    }

    /// Drops the namespace's head anchor, so its next read revalidates from
    /// the store. A caller that owns a publication service also drops its
    /// publisher's position; see `LoonFs::invalidate_namespace`.
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

fn cached_anchor(
    anchor: NamespaceReadAnchor,
    basis_checked: Option<Observation>,
) -> CachedReadAnchor {
    CachedReadAnchor {
        basis: anchor.basis(),
        head: anchor.read_state,
        basis_checked,
        validation: Arc::default(),
        completed_validation_no: 0,
    }
}
