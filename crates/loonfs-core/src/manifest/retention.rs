//! Retention floor advancement, and the namespace's retention window, through
//! a verified successor manifest.

use super::cache::MetadataSegmentCache;
use super::error::ManifestLoadError;
use super::load::metadata_basis_from_manifest;
use super::publish::ManifestChange;
use super::published::{PublicationReport, PublishedManifest};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::{GC_DEFAULT_GRACE_WINDOW_MS, MAX_CONTENTION_ATTEMPTS};
use crate::metadata::manifest_index::commits_after_page;
use crate::namespace::control::{ensure_namespace_live, LoadedManifest};
use crate::namespace::state::NamespaceReadState;
use crate::namespace::PublicationBasis;
use crate::time::{Deadline, Observation};
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{AdvanceRetentionResponse, ChangeSeq, SetRetentionWindowResponse};
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::Arc;

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

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Advances the retention floor toward `target` when a verified checkpoint
    /// makes it safe (format spec, "Advancing the retention floor"). The
    /// advance uses this engine's wall clock as the current time for the
    /// grace-window check and for a [`RetentionTarget::Window`] cutoff. The
    /// report includes the manifest that records the new floor.
    ///
    /// The advance loads the current manifest once, and it returns that
    /// manifest's floor without another request when the target is at or below
    /// the floor. Otherwise, it reads commits upward from the floor once, and it
    /// stops at the target or at the first commit after the cutoff, which is the
    /// earlier of the target's own cutoff, if it has one, and the current time
    /// minus the grace. A floor that moves is published through
    /// `update_manifest`, which builds on that manifest.
    pub async fn advance_retention_floor(
        &self,
        target: RetentionTarget,
    ) -> Result<PublicationReport<AdvanceRetentionResponse>> {
        let now_ms = self.now_ms();
        let checked = Observation::now(Arc::clone(&self.namespace.timer));
        let current = self
            .namespace
            .load_current_manifest()
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        let manifest = current.state.envelope.payload();
        ensure_namespace_live(&NamespaceReadState::from(manifest))?;
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
        let target = if through <= manifest.retention_floor_seq {
            manifest.retention_floor_seq
        } else {
            last_commit_at_or_before(
                &self.namespace.store,
                &self.namespace.segment_cache,
                &current,
                through,
                cutoff_at_ms,
            )
            .await?
        };
        if target <= manifest.retention_floor_seq {
            return Ok(PublicationReport {
                response: AdvanceRetentionResponse {
                    namespace_id: self.namespace.namespace_id.clone(),
                    retention_floor_seq: manifest.retention_floor_seq,
                },
                published: None,
            });
        }
        let deadline = Deadline::start(Arc::clone(&self.namespace.timer));
        let basis = PublicationBasis {
            manifest: current,
            checked,
        };
        let ((floor, advanced), current, _) = self
            .namespace
            .update_manifest(
                Some(basis),
                &deadline,
                MAX_CONTENTION_ATTEMPTS,
                |current: LoadedManifest| async move {
                    let mut payload = current.state.envelope.payload().clone();
                    ensure_namespace_live(&NamespaceReadState::from(&payload))?;
                    // Another advance can publish a floor at or above `target` after
                    // the first load.
                    if payload.retention_floor_seq >= target {
                        return Ok(ManifestChange::Finished((
                            payload.retention_floor_seq,
                            false,
                        )));
                    }
                    verify_manifest_segments_exist(&self.namespace.store, &payload).await?;
                    payload.retention_floor_seq = target;
                    Ok(ManifestChange::Next(Box::new(payload), (target, true)))
                },
            )
            .await?;
        Ok(PublicationReport {
            response: AdvanceRetentionResponse {
                namespace_id: self.namespace.namespace_id.clone(),
                retention_floor_seq: floor,
            },
            published: advanced
                .then(|| PublishedManifest::new(&current.state.envelope, Some(deadline.origin()))),
        })
    }

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
        let deadline = Deadline::start(Arc::clone(&self.namespace.timer));
        let (changed, current, _) = self
            .namespace
            .update_manifest(
                None,
                &deadline,
                MAX_CONTENTION_ATTEMPTS,
                |current: LoadedManifest| async move {
                    let mut payload = current.state.envelope.payload().clone();
                    ensure_namespace_live(&NamespaceReadState::from(&payload))?;
                    if payload.retention_window_ms == retention_window_ms {
                        return Ok(ManifestChange::Finished(false));
                    }
                    payload.retention_window_ms = retention_window_ms;
                    Ok(ManifestChange::Next(Box::new(payload), true))
                },
            )
            .await?;
        Ok(PublicationReport {
            response: SetRetentionWindowResponse {
                namespace_id: self.namespace.namespace_id.clone(),
                retention_window_ms: retention_window_ms.map(NonZeroU64::get),
            },
            published: changed
                .then(|| PublishedManifest::new(&current.state.envelope, Some(deadline.origin()))),
        })
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
    let mut last = current.state.retention_floor_seq();
    // One commit per scan, as the change feed reads them: a commit row can
    // be large, and the memo keeps the block for the next call within the
    // caller's read budget.
    while last < through {
        let Some(commit) = commits_after_page(&basis.segments, last, 1).await?.pop() else {
            break;
        };
        if commit.committed_at_ms > cutoff_at_ms {
            break;
        }
        last = commit.committed_seq;
    }
    Ok(last)
}
