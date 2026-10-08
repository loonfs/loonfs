//! Retention floor advancement through a verified successor manifest.

use super::cache::MetadataSegmentCache;
use super::error::ManifestLoadError;
use super::load::metadata_basis_from_manifest;
use super::publish::{update_manifest, ManifestChange};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::metadata::manifest_index::commits_after_page;
use crate::namespace::control::{ensure_namespace_live, load_current_manifest};
use crate::namespace::state::NamespaceReadState;
use crate::store_waves::STORE_READ_WAVE;
use crate::time::{Deadline, StdMonotonicTimer};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{AdvanceRetentionResponse, ChangeSeq, NamespaceId};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Where a retention advance moves the floor.
///
/// The floor never moves down and never passes the folded manifest head. A
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
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    for chunk in object_keys.chunks(STORE_READ_WAVE) {
        futures::future::try_join_all(chunk.iter().map(|object_key| async move {
            match store
                .head(object_key)
                .await
                .map_err(|error| ManifestLoadError::ReadSegment {
                    object_key: object_key.clone(),
                    message: error.public_message().into_owned(),
                })? {
                Some(_) => Ok(()),
                None => Err(ManifestLoadError::MissingSegment {
                    object_key: object_key.clone(),
                }),
            }
        }))
        .await?;
    }
    Ok(())
}

pub(crate) async fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    target: RetentionTarget,
) -> Result<AdvanceRetentionResponse> {
    let target_seq = match target {
        RetentionTarget::Head => None,
        RetentionTarget::Seq(seq) => Some(seq),
        RetentionTarget::Cutoff { cutoff_at_ms } => {
            Some(last_commit_at_or_before(store, segment_cache, namespace_id, cutoff_at_ms).await?)
        }
    };
    let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
    let (floor, _, _) = update_manifest(store, namespace_id, &deadline, |mut payload| async move {
        ensure_namespace_live(&NamespaceReadState::from(&payload))?;
        let target = target_seq.map_or(payload.head_seq, |seq| seq.min(payload.head_seq));
        if payload.retention_floor_seq >= target {
            return Ok(ManifestChange::Finished(payload.retention_floor_seq));
        }
        verify_manifest_segments_exist(store, &payload)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
        payload.retention_floor_seq = target;
        Ok(ManifestChange::Next(Box::new(payload), target))
    })
    .await?;
    Ok(AdvanceRetentionResponse {
        namespace_id: namespace_id.clone(),
        retention_floor_seq: floor,
    })
}

/// The sequence of the last commit at or before `cutoff_at_ms`, read upward
/// from the current manifest's floor. The floor itself when the first commit
/// above it is later than the cutoff.
async fn last_commit_at_or_before<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    cutoff_at_ms: u64,
) -> Result<ChangeSeq> {
    let current = load_current_manifest(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    ensure_namespace_live(&NamespaceReadState::from(current.state.envelope.payload()))?;
    let basis = metadata_basis_from_manifest(store, segment_cache, &current);
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
