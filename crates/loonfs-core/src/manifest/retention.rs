//! Retention floor advancement through a verified successor manifest.

use super::cache::MetadataSegmentCache;
use super::error::ManifestLoadError;
use super::load::metadata_basis_from_manifest;
use super::publish::{update_manifest, ManifestChange};
use super::published::{PublicationReport, PublishedManifest};
use super::scan::Readahead;
use crate::error::{CoreError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::{GC_DEFAULT_GRACE_WINDOW_MS, MAX_CONTENTION_ATTEMPTS};
use crate::metadata::manifest_index::{commit_at_seq, commits_after_page};
use crate::namespace::control::{ensure_namespace_live, load_current_manifest, LoadedManifest};
use crate::namespace::state::NamespaceReadState;
use crate::time::{Deadline, MonotonicTimer};
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{AdvanceRetentionResponse, ChangeSeq, NamespaceId};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Where a retention advance moves the floor.
///
/// The floor never moves down and never passes the folded manifest head.
/// Whatever the target, the floor stops at the newest commit at or below the
/// target that's at least [`GC_DEFAULT_GRACE_WINDOW_MS`] old, and it passes
/// every commit below that commit, whatever their timestamps (format spec,
/// "Advancing the retention floor"). A target at or below the current floor
/// leaves the floor where it is.
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
                    kind: loonfs_objectstore::ObjectStoreErrorKind::of(&error),
                })? {
                Some(_) => Ok(()),
                None => Err(ManifestLoadError::MissingSegment { object_key }),
            }
        })
        .buffer_unordered(STORE_READS_IN_FLIGHT)
        .try_collect()
        .await
}

/// Advances the retention floor toward `target` (format spec, "Advancing the
/// retention floor"), measuring commit ages against `now_ms`.
///
/// The advance loads the current manifest once to find the target, and it
/// returns that manifest's floor without another request when the target is
/// at or below the floor. A floor that moves is published through
/// [`update_manifest`], which loads the manifest again.
pub(crate) async fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    target: RetentionTarget,
    now_ms: u64,
    timer: Arc<dyn MonotonicTimer>,
) -> Result<PublicationReport<AdvanceRetentionResponse>> {
    let current = load_current_manifest(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let manifest = current.state.envelope.payload();
    ensure_namespace_live(&NamespaceReadState::from(manifest))?;
    let target = match target {
        RetentionTarget::Head => manifest.head_seq,
        RetentionTarget::Seq(seq) => seq.min(manifest.head_seq),
        RetentionTarget::Cutoff { cutoff_at_ms } => {
            last_commit_at_or_before(store, segment_cache, &current, cutoff_at_ms).await?
        }
    };
    let target =
        newest_commit_past_the_grace_window(store, segment_cache, &current, target, now_ms).await?;
    if target <= manifest.retention_floor_seq {
        return Ok(PublicationReport {
            response: AdvanceRetentionResponse {
                namespace_id: namespace_id.clone(),
                retention_floor_seq: manifest.retention_floor_seq,
            },
            published: None,
        });
    }
    let deadline = Deadline::start(timer);
    let ((floor, advanced), current, _) = update_manifest(
        store,
        namespace_id,
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
            verify_manifest_segments_exist(store, &payload).await?;
            payload.retention_floor_seq = target;
            Ok(ManifestChange::Next(Box::new(payload), (target, true)))
        },
    )
    .await?;
    Ok(PublicationReport {
        response: AdvanceRetentionResponse {
            namespace_id: namespace_id.clone(),
            retention_floor_seq: floor,
        },
        published: advanced
            .then(|| PublishedManifest::new(&current.state.envelope, Some(deadline.origin()))),
    })
}

/// The sequence of the last commit at or before `cutoff_at_ms`, read upward
/// from `current`'s floor. The floor itself when the first commit above it
/// is later than the cutoff.
async fn last_commit_at_or_before<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    current: &LoadedManifest,
    cutoff_at_ms: u64,
) -> Result<ChangeSeq> {
    let basis = metadata_basis_from_manifest(store, segment_cache, current);
    let mut last = current.state.retention_floor_seq();
    // One commit per scan, as the change feed reads them: a commit row can
    // be large, and the memo keeps the block for the next call within the
    // caller's read budget.
    while let Some(commit) = commits_after_page(&basis.segments, last, 1).await?.pop() {
        if commit.committed_at_ms > cutoff_at_ms {
            break;
        }
        last = commit.committed_seq;
    }
    Ok(last)
}

/// Returns the sequence of the newest commit at or below `target` that's at
/// least [`GC_DEFAULT_GRACE_WINDOW_MS`] old at `now_ms`.
///
/// If no commit above `current`'s floor is that old, returns that floor. A
/// commit's age is `now_ms` minus its `committed_at_ms`, and a commit dated
/// after `now_ms` has an age of zero, as an object dated in the future does in
/// garbage collection. Every sequence above the floor has a commit row, so the
/// walk reads one commit row per sequence, downward from `target`. It reads
/// the commits younger than the window at the top of the history, which is
/// about an hour of commits, and at worst every commit above the floor, if
/// writers dated every commit above the floor in the future.
///
/// The walk doesn't read ahead, so it fetches each block of commit rows that
/// it misses and no others. Commit rows are sorted by sequence, and a
/// read-ahead window covers the missed block and the blocks after it, which
/// contain newer commits. The walk moves toward older commits, so it has already
/// read every block that a window would add.
async fn newest_commit_past_the_grace_window<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    current: &LoadedManifest,
    target: ChangeSeq,
    now_ms: u64,
) -> Result<ChangeSeq> {
    let floor = current.state.retention_floor_seq();
    if target <= floor {
        return Ok(floor);
    }
    let basis = metadata_basis_from_manifest(store, segment_cache, current);
    for seq in (floor.0 + 1..=target.0).rev().map(ChangeSeq) {
        let commit = commit_at_seq(&basis.segments, seq, Readahead::Disabled)
            .await?
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "the `commits` family has no row at sequence {seq}, which is above the retention floor {floor}"
                ))
            })?;
        if now_ms.saturating_sub(commit.committed_at_ms) >= GC_DEFAULT_GRACE_WINDOW_MS {
            return Ok(seq);
        }
    }
    Ok(floor)
}
