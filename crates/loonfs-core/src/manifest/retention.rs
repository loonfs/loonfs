//! Retention floor advancement, and the namespace's retention window, through
//! a verified successor manifest.

use super::error::ManifestLoadError;
use super::load::metadata_basis_from_manifest;
use super::publish::ManifestChange;
use super::published::PublicationReport;
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result, WriterFence};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::GC_DEFAULT_GRACE_WINDOW_MS;
use crate::metadata::manifest_index::commits_after_page;
use crate::namespace::control::{ensure_namespace_live, LoadedManifest};
use crate::namespace::store::NamespaceStore;
use crate::namespace::PublicationBasis;
use crate::segment::cache::MetadataSegmentCache;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{ChangeSeq, SetRetentionWindowResponse, WriterEpoch};
use std::collections::BTreeSet;
use std::num::NonZeroU64;

/// Where a retention advance moves the floor.
///
/// The floor never moves down and never passes the folded manifest head.
/// Whatever the target, the advance also treats [`GC_DEFAULT_GRACE_WINDOW_MS`]
/// before its clock as a cutoff, so the floor stops at or below the last
/// commit at or before that time, found by the same walk as a
/// [`Self::Cutoff`] target (format spec, "Advancing the retention floor"). A
/// target at or below the current floor leaves the floor where it is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RetentionTarget {
    /// The folded manifest head.
    #[default]
    Head,
    /// This sequence.
    Seq(ChangeSeq),
    /// The last commit committed at or before `cutoff_at_ms`.
    ///
    /// Commit times come from writers' clocks and need not increase with
    /// sequence, so the advance reads commits upward from the floor and stops
    /// at the first one committed after the cutoff.
    Cutoff {
        /// A Unix time in milliseconds.
        cutoff_at_ms: u64,
    },
    /// The last commit committed at or before the current time minus the
    /// namespace's retention window, or minus `default_window_ms` when the
    /// namespace has no window. With neither window, the floor stays where it
    /// is.
    ///
    /// The advance reads the namespace's window from the manifest it loads
    /// to find the target, so a host that advances floors on a schedule
    /// needs no extra request to read each namespace's window.
    Window {
        /// The window for a namespace that has no window of its own, such as
        /// a host's setting.
        default_window_ms: Option<NonZeroU64>,
    },
}

async fn verify_manifest_segments_exist<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &NamespaceManifestPayload,
) -> std::result::Result<(), ManifestLoadError> {
    let object_keys = manifest
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect::<BTreeSet<_>>();
    stream::iter(object_keys)
        .map(|object_key| async move {
            match store
                .head(&object_key)
                .await
                .map_err(|error| ManifestLoadError::ReadSegment {
                    object_key: object_key.clone(),
                    message: error.public_message().into_owned(),
                    kind: error.kind(),
                })? {
                Some(_) => Ok(()),
                None => Err(ManifestLoadError::MissingSegment { object_key }),
            }
        })
        .buffer_unordered(STORE_READS_IN_FLIGHT)
        .try_collect()
        .await
}

/// What a retention advance does, planned from one manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPlan {
    /// The floor stays at this sequence, the manifest's floor.
    Stays(ChangeSeq),
    /// The floor moves up to this sequence.
    Moves(ChangeSeq),
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Plans where a retention advance toward `target` moves the floor of
    /// `current`, and publishes nothing (format spec, "Advancing the
    /// retention floor"). The plan uses the namespace's wall clock as the
    /// current time for the grace-window check and for a
    /// [`RetentionTarget::Window`] cutoff.
    ///
    /// The plan makes no request when the target is at or below the floor.
    /// Otherwise, it reads commits upward from the floor once, and it stops
    /// at the target or at the first commit after the cutoff, which is the
    /// earlier of the target's own cutoff, if it has one, and the current
    /// time minus the grace.
    pub async fn plan_retention_floor(
        &self,
        current: &LoadedManifest,
        target: RetentionTarget,
    ) -> Result<RetentionPlan> {
        let now_ms = self.start_clock().request_at_ms();
        let manifest = current.payload();
        ensure_namespace_live(&manifest.namespace_id, manifest.status)?;
        let grace_cutoff_at_ms = now_ms.saturating_sub(GC_DEFAULT_GRACE_WINDOW_MS);
        let (through, cutoff_at_ms) = match target {
            RetentionTarget::Head => (manifest.head_seq, grace_cutoff_at_ms),
            RetentionTarget::Seq(seq) => (seq.min(manifest.head_seq), grace_cutoff_at_ms),
            RetentionTarget::Cutoff { cutoff_at_ms } => {
                (manifest.head_seq, cutoff_at_ms.min(grace_cutoff_at_ms))
            }
            RetentionTarget::Window { default_window_ms } => {
                match manifest.retention_window_ms.or(default_window_ms) {
                    Some(window_ms) => (
                        manifest.head_seq,
                        now_ms
                            .saturating_sub(window_ms.get())
                            .min(grace_cutoff_at_ms),
                    ),
                    // With neither window, the floor stays where it is.
                    None => (manifest.retention_floor_seq, grace_cutoff_at_ms),
                }
            }
        };
        if through <= manifest.retention_floor_seq {
            return Ok(RetentionPlan::Stays(manifest.retention_floor_seq));
        }
        let floor = last_commit_at_or_before(
            &self.store,
            &self.segment_cache,
            current,
            through,
            cutoff_at_ms,
        )
        .await?;
        Ok(if floor <= manifest.retention_floor_seq {
            RetentionPlan::Stays(manifest.retention_floor_seq)
        } else {
            RetentionPlan::Moves(floor)
        })
    }

    /// Publishes the successor of the current manifest with its retention
    /// floor raised to `target`, under `writer_epoch` (format spec,
    /// "Advancing the retention floor").
    ///
    /// The caller has acquired `writer_epoch`, and it has folded every WAL
    /// object that it wrote or found since it acquired the epoch. The
    /// publication builds on `basis` when one is given, and otherwise loads
    /// the current manifest. When a put collides, the publication builds on
    /// the manifest that won. If that manifest's floor is at or above
    /// `target`, the call returns that floor and publishes nothing. If that
    /// manifest has another writer epoch, another writer took the namespace
    /// over, so the call returns `writer_fenced` and publishes nothing. The
    /// report's response is the floor of the manifest that the call
    /// published or found.
    pub async fn publish_retention_floor(
        &self,
        basis: Option<PublicationBasis>,
        writer_epoch: WriterEpoch,
        target: ChangeSeq,
    ) -> Result<PublicationReport<ChangeSeq>> {
        let clock = self.start_clock();
        let update = self
            .update_live_manifest(
                basis,
                &clock,
                |mut payload: NamespaceManifestPayload| async move {
                    if payload.retention_floor_seq >= target {
                        return Ok(ManifestChange::Finished(payload.retention_floor_seq));
                    }
                    if payload.writer_epoch != writer_epoch {
                        return Err(CoreError::WriterFenced(WriterFence {
                            fenced_epoch: writer_epoch,
                            active_epoch: payload.writer_epoch,
                            active_writer: payload.writer,
                        }));
                    }
                    verify_manifest_segments_exist(&self.store, &payload).await?;
                    payload.retention_floor_seq = target;
                    Ok(ManifestChange::Next(Box::new(payload), target))
                },
            )
            .await?;
        Ok(update.into_report())
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Sets the namespace's retention window, or clears it when
    /// `retention_window_ms` is `None`, through a successor manifest that changes
    /// nothing else (format spec, "Advancing the retention floor").
    ///
    /// If the current manifest already has the requested window, the call
    /// publishes nothing. The report includes the manifest that records the
    /// change.
    pub async fn set_retention_window(
        &self,
        retention_window_ms: Option<NonZeroU64>,
    ) -> Result<PublicationReport<SetRetentionWindowResponse>> {
        let clock = self.namespace.start_clock();
        let update = self
            .namespace
            .update_live_manifest(
                None,
                &clock,
                |mut payload: NamespaceManifestPayload| async move {
                    if payload.retention_window_ms == retention_window_ms {
                        return Ok(ManifestChange::Finished(()));
                    }
                    payload.retention_window_ms = retention_window_ms;
                    Ok(ManifestChange::Next(Box::new(payload), ()))
                },
            )
            .await?;
        Ok(update.into_report().map(|()| SetRetentionWindowResponse {
            namespace_id: self.namespace.namespace_id.clone(),
            retention_window_ms: retention_window_ms.map(NonZeroU64::get),
        }))
    }
}

/// Returns the sequence of the last commit at or before `cutoff_at_ms` and at
/// or below `through`, read upward from `current`'s floor.
///
/// The walk stops at the first commit committed after the cutoff, even if a
/// later commit is older, because commit times come from writers' clocks and
/// need not increase with sequence. If the first commit above the floor is
/// later than the cutoff, the walk returns the floor.
async fn last_commit_at_or_before<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: &MetadataSegmentCache,
    current: &LoadedManifest,
    through: ChangeSeq,
    cutoff_at_ms: u64,
) -> Result<ChangeSeq> {
    let basis = metadata_basis_from_manifest(store, Some(segment_cache), current);
    let mut last = current.retention_floor_seq();
    // One commit per scan: a commit row can be large, and the memo keeps the
    // block for the next call within the caller's read budget.
    while last < through {
        let Some(commit) = commits_after_page(&basis.segments, last, 1, usize::MAX)
            .await?
            .pop()
        else {
            break;
        };
        if commit.committed_at_ms > cutoff_at_ms {
            break;
        }
        last = commit.committed_seq;
    }
    Ok(last)
}
