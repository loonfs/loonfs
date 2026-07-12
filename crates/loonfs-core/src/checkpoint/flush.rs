//! The WAL flush: fold the visible WAL tail into metadata tables under a
//! manifest covering the current head and advance `metadata/root.json` by
//! compare-and-swap, creating no durable checkpoint record.
//!
//! This is the latest-state maintenance path: superseded manifests become
//! garbage-collection candidates once nothing pins them. Pinning a manifest
//! version for retention is a separate concern layered on top by
//! [`create`](super::create).

use super::build::build_manifest_l0_run_tables;
use super::error::ManifestLoadError;
use super::load::{
    head_from_manifest, load_namespace_manifest_envelope_if_present, load_verified_manifest_tables,
};
use super::publish::{publish_metadata_root, write_namespace_manifest, ManifestPublicationOutcome};
use super::runs::flatten_manifest_tables;
use super::scan::VerifiedMetadataTables;
use crate::commit::CommitHeadPublishError;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::error::Result;
use crate::limits::METADATA_PUBLICATION_BUDGET_MS;
use crate::metadata::MetadataState;
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::namespace::control::{read_head_and_metadata_root, read_wal_floor_seq_or_zero};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use crate::wal::{load_validated_wal_chain, project_validated_wal_tail, WalChainLoadRequest};
use loonfs_api::wire::control::{HeadState, MetadataRootState, NamespaceState};
use loonfs_api::wire::manifest::{NamespaceManifestEnvelope, NamespaceManifestPayload};
use loonfs_api::{
    ChangeSeq, CommitId, FlushWalOutcome, FlushWalResponse, ManifestId, ManifestObjectId,
    NamespaceId,
};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;
use tracing::Instrument;

// Manifest id allocation can race with other manifest publishers. Exhausting
// this loop means the candidate id range was already occupied.
pub(super) const MANIFEST_ALLOCATION_RETRY_LIMIT: usize = 8;
// A WAL flush can race with another manifest publisher. Each retry
// rebases onto a fresh projection of the newest head and root.
const WAL_FLUSH_RETRY_LIMIT: usize = 8;

/// The basis one flush attempt settled on: the manifest this attempt
/// published, or the one already covering the head.
pub(super) struct FlushedBasis {
    pub(super) manifest_id: ManifestId,
    pub(super) manifest_object_id: ManifestObjectId,
    pub(super) manifest_head_seq: ChangeSeq,
    pub(super) manifest_payload_checksum: String,
    /// Head commit the basis covers.
    pub(super) head_commit_id: CommitId,
    /// Head sequence the attempt targeted.
    pub(super) target_head_seq: ChangeSeq,
    /// Root manifest observed when the projection loaded, for callers that
    /// report the newest manifest id they saw.
    pub(super) root_manifest_id_at_load: ManifestId,
    /// Manifest `metadata/root.json` references after the attempt — the
    /// basis itself, or the newer root that superseded it.
    pub(super) root_after_manifest_id: ManifestId,
    /// Sequence covered by `root_after_manifest_id`.
    pub(super) root_after_head_seq: ChangeSeq,
    pub(super) outcome: FlushWalOutcome,
}

pub(super) enum TryFlushWal {
    Flushed(Box<FlushedBasis>),
    /// The root compare-and-swap raced another publisher to exhaustion;
    /// retry against a fresh projection.
    RaceLost,
}

/// Flushes the visible WAL tail into metadata tables and advances
/// `metadata/root.json` to a manifest covering the current head.
///
/// The WAL delta lands as one new L0 run when the root lags the head; the
/// manifest publishes and the root advances. No checkpoint record is
/// created. Returns `StaleHead` when every attempt lost the root race.
pub(crate) async fn flush_wal<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<FlushWalResponse> {
    let timer = StdMonotonicTimer::default();
    flush_wal_with_timer(store, namespace_id, context, &timer).await
}

pub(super) async fn flush_wal_with_timer<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    timer: &dyn MonotonicTimer,
) -> Result<FlushWalResponse> {
    for _attempt in 0..WAL_FLUSH_RETRY_LIMIT {
        match try_flush_wal(store, namespace_id, context, timer).await? {
            TryFlushWal::Flushed(basis) => {
                return Ok(FlushWalResponse {
                    namespace_id: namespace_id.clone(),
                    target_head_seq: basis.target_head_seq,
                    manifest_id: basis.root_after_manifest_id,
                    manifest_head_seq: basis.root_after_head_seq,
                    outcome: basis.outcome,
                });
            }
            TryFlushWal::RaceLost => continue,
        }
    }
    Err(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
}

/// One flush attempt against one fresh projection.
///
/// The metadata publication budget covers this attempt end to end: the
/// measurement starts before any table object is written and gates the root
/// compare-and-swap, so an over-budget build aborts with only unreachable
/// immutable outputs behind it.
pub(super) async fn try_flush_wal<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    timer: &dyn MonotonicTimer,
) -> Result<TryFlushWal> {
    let publication_started_ms = timer.monotonic_now_ms();
    let projection = load_root_projection(store, namespace_id)
        .instrument(tracing::info_span!(
            "loon.phase",
            phase = "scan_namespace_state"
        ))
        .await?;
    let head_seq = projection.head.seq;
    let root_manifest_id_at_load = projection.root.manifest_id;

    if projection.root.manifest_head_seq == head_seq {
        return Ok(TryFlushWal::Flushed(Box::new(FlushedBasis {
            manifest_id: projection.root.manifest_id,
            manifest_object_id: projection.root.manifest_object_id.clone(),
            manifest_head_seq: projection.root.manifest_head_seq,
            manifest_payload_checksum: projection.root.manifest_payload_checksum.clone(),
            head_commit_id: projection.head.head_commit_id.clone(),
            target_head_seq: head_seq,
            root_manifest_id_at_load,
            root_after_manifest_id: projection.root.manifest_id,
            root_after_head_seq: projection.root.manifest_head_seq,
            outcome: FlushWalOutcome::AlreadyCurrent,
        })));
    }

    let manifest_id = next_manifest_id_after(projection.root.manifest_id)?;
    let mut written_manifest = None;
    for _allocation_attempt in 0..MANIFEST_ALLOCATION_RETRY_LIMIT {
        let manifest_object_id = ManifestObjectId::generate(manifest_id);
        let manifest_key = metadata_manifest_object(namespace_id.as_str(), &manifest_object_id);
        match load_namespace_manifest_envelope_if_present(
            store,
            namespace_id,
            &manifest_object_id,
            &manifest_key,
        )
        .await
        {
            Ok(Some(_existing)) => {
                // Physical-id collision or retry; keep the logical slot and
                // generate another object id.
                continue;
            }
            Ok(None) => {
                let manifest = build_namespace_manifest_for_projection(
                    store,
                    namespace_id,
                    &projection,
                    &context.writer_version,
                    manifest_id,
                    manifest_object_id,
                )
                .await?;
                // `write_namespace_manifest` owns the idempotent "manifest
                // already exists" path. A same-id/different-payload conflict
                // means another writer won this slot, so try the next
                // manifest id.
                match write_namespace_manifest(store, &manifest).await {
                    Ok(()) => {}
                    Err(MetadataProjectionLoadError::ManifestLoad(
                        ManifestLoadError::ManifestConflict { .. },
                    )) => {
                        continue;
                    }
                    Err(error) => return Err(CoreError::MetadataProjection(error)),
                }
                written_manifest = Some(manifest);
                break;
            }
            Err(error) => {
                return Err(CoreError::MetadataProjection(
                    MetadataProjectionLoadError::ManifestLoad(error),
                ))
            }
        }
    }
    let Some(manifest) = written_manifest else {
        return Err(CoreError::Internal(
            "manifest id allocation retry exhausted".to_owned(),
        ));
    };
    // The publication budget gates the root compare-and-swap: past it, the
    // written tables and manifest may have aged into the GC grace window,
    // so this attempt must abort without publishing (format spec, "Garbage
    // collection", rule 1). The orphans are reclaimed by a later pass.
    ensure_metadata_publication_budget(timer, publication_started_ms, namespace_id)?;
    // Advance the root for readers. A superseded outcome is fine: the
    // manifest we wrote stays durable and valid as a basis even when a
    // newer root already won.
    let (outcome, root_after_manifest_id, root_after_head_seq) = match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
        &projection
            .manifest_tables
            .manifest()
            .payload
            .manifest_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await?
    {
        ManifestPublicationOutcome::Published(_) => (
            FlushWalOutcome::Published,
            manifest.payload.manifest_id,
            manifest.payload.head_seq,
        ),
        ManifestPublicationOutcome::Superseded(current) => (
            FlushWalOutcome::Superseded,
            current.manifest_id,
            current.manifest_head_seq,
        ),
        ManifestPublicationOutcome::RootCasRaceLost => {
            return Ok(TryFlushWal::RaceLost);
        }
    };
    Ok(TryFlushWal::Flushed(Box::new(FlushedBasis {
        manifest_id: manifest.payload.manifest_id,
        manifest_object_id: manifest.payload.manifest_object_id.clone(),
        manifest_head_seq: manifest.payload.head_seq,
        manifest_payload_checksum: manifest.payload_checksum.clone(),
        head_commit_id: projection.head.head_commit_id.clone(),
        target_head_seq: head_seq,
        root_manifest_id_at_load,
        root_after_manifest_id,
        root_after_head_seq,
        outcome,
    })))
}

pub(super) struct RootProjection<'a, S: ObjectStore + ?Sized> {
    pub(super) head: HeadState,
    pub(super) root: MetadataRootState,
    pub(super) floor_seq: ChangeSeq,
    pub(super) manifest_tables: VerifiedMetadataTables<'a, S>,
    pub(super) tail_state: MetadataState,
}

pub(super) async fn load_root_projection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
) -> Result<RootProjection<'a, S>> {
    load_namespace_catalog_entry(store, namespace_id)
        .await
        .map_err(|error| CoreError::MetadataProjection(error.into()))?;
    let (loaded_head, loaded_root) = read_head_and_metadata_root(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let floor_seq = read_wal_floor_seq_or_zero(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let head = loaded_head.envelope.state;
    let root = loaded_root.envelope.state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    let manifest_tables =
        load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
    if manifest_tables.manifest().payload_checksum != root.manifest_payload_checksum {
        return Err(CoreError::NamespaceCorrupt(format!(
            "metadata root for `{}` references manifest `{}` with checksum {} but the manifest carries {}",
            namespace_id.as_str(),
            root.manifest_id,
            root.manifest_payload_checksum,
            manifest_tables.manifest().payload_checksum,
        )));
    }
    let manifest_head = head_from_manifest(&head, manifest_tables.manifest());
    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: manifest_head.seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: None,
            recent_segments: &head.recent_segments,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    let replayed = {
        let _span = tracing::info_span!("loon.phase", phase = "project_metadata_state").entered();
        project_validated_wal_tail(
            &manifest_head,
            &MetadataState::default(),
            Some(head.writer_epoch),
            &wal_chain,
        )
        .map_err(MetadataProjectionLoadError::WalReplay)
        .map_err(CoreError::MetadataProjection)?
    };
    ensure_reconstructed_head_matches(&head, &replayed.resulting_head)?;
    Ok(RootProjection {
        head,
        root,
        floor_seq,
        manifest_tables,
        tail_state: replayed.resulting_metadata_state,
    })
}

fn ensure_reconstructed_head_matches(
    current_head: &HeadState,
    reconstructed: &HeadState,
) -> Result<()> {
    if current_head.namespace_id != reconstructed.namespace_id
        || current_head.seq != reconstructed.seq
        || current_head.head_commit_id != reconstructed.head_commit_id
        || current_head.next_inode_id != reconstructed.next_inode_id
        || (reconstructed.visible_wal_tip.is_some()
            && current_head.visible_wal_tip != reconstructed.visible_wal_tip)
    {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::ReplayedHeadMismatch {
                expected: Box::new(current_head.clone()),
                actual: Box::new(reconstructed.clone()),
            },
        ));
    }
    Ok(())
}

pub(super) fn next_manifest_id_after(current: ManifestId) -> Result<ManifestId> {
    current
        .0
        .checked_add(1)
        .map(ManifestId)
        .ok_or_else(|| CoreError::Internal("manifest id overflow".to_owned()))
}

/// Refuses to initiate a root compare-and-swap once the publication budget
/// is spent (format spec, "Garbage collection", rule 1).
pub(super) fn ensure_metadata_publication_budget(
    timer: &dyn MonotonicTimer,
    publication_started_ms: u64,
    namespace_id: &NamespaceId,
) -> Result<()> {
    let elapsed_ms = timer
        .monotonic_now_ms()
        .saturating_sub(publication_started_ms);
    if elapsed_ms <= METADATA_PUBLICATION_BUDGET_MS {
        return Ok(());
    }
    tracing::error!(
        namespace_id = namespace_id.as_str(),
        elapsed_ms,
        budget_ms = METADATA_PUBLICATION_BUDGET_MS,
        "metadata publication overran its budget; aborting before the root compare-and-swap",
    );
    Err(CoreError::MetadataPublicationBudgetExceeded {
        elapsed_ms,
        budget_ms: METADATA_PUBLICATION_BUDGET_MS,
    })
}

async fn build_namespace_manifest_for_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &RootProjection<'_, S>,
    writer_version: &str,
    manifest_id: ManifestId,
    manifest_object_id: ManifestObjectId,
) -> Result<NamespaceManifestEnvelope> {
    let head_seq = projection.head.seq;
    let previous_manifest = projection.manifest_tables.manifest();

    // A WAL flush is manifest-only: every prior run is referenced
    // unchanged and the WAL delta lands as one new L0 run, so the cost
    // follows the delta, never the namespace. Folding L0 runs back into the
    // base is reorganization's job (`reorganize.rs`), off this path.
    let base_seq = previous_manifest.payload.base_seq;
    let mut metadata_files = previous_manifest.payload.metadata_files.clone();
    if previous_manifest.payload.head_seq < head_seq {
        metadata_files.extend(flatten_manifest_tables(
            build_manifest_l0_run_tables(
                store,
                namespace_id,
                head_seq,
                previous_manifest.payload.head_seq,
                &projection.tail_state,
            )
            .await?,
        ));
    }

    NamespaceManifestEnvelope::from_payload(
        writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id,
            manifest_object_id,
            head_seq,
            head_commit_id: projection.head.head_commit_id.clone(),
            base_seq,
            writer_epoch: projection.head.writer_epoch,
            next_inode_id: projection.head.next_inode_id,
            retention_floor_seq: projection.floor_seq,
            initialized: true,
            verified: true,
            fork: None,
            // A checkpoint appends one delta run; it never changes what is
            // materialized on the namespace, so feature declarations and
            // derived-index segments carry forward verbatim.
            features: previous_manifest.payload.features.clone(),
            metadata_files,
            index_files: previous_manifest.payload.index_files.clone(),
        },
    )
    .map_err(|err| {
        CoreError::Internal(format!(
            "failed to build namespace manifest envelope: {err}"
        ))
    })
}
