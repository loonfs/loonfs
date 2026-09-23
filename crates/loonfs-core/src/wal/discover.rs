//! Discovers the WAL tip and advances cached namespace views.

use super::frame::{ValidatedWalTail, WalTailLoadError};
use super::reader::{load_wal_segment, WalWalk};
use super::replay::{project_validated_wal_tail, validate_wal_segment_for_replay};
use crate::cache::WalTailProjectionCacheKey;
use crate::control_object::ControlObjectLoadError;
use crate::namespace::control::LoadedManifest;
use crate::namespace::state::NamespaceReadState;
use crate::RuntimeReadContext;
use loonfs_api::wire::wal::WalSegmentEnvelope;
use loonfs_api::{NamespaceId, WalNo, WriterEpoch};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub(crate) async fn discover_tip<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &LoadedManifest,
) -> Result<NamespaceReadState, ControlObjectLoadError> {
    let mut state = NamespaceReadState::from(manifest.envelope.payload());
    if state.status.is_deleted() {
        return Ok(state);
    }
    let start = manifest.hinted_wal_no.max(state.last_folded_wal_no);
    let mut previous_epoch = WriterEpoch(0);
    if start > state.last_folded_wal_no {
        // The hint skips the folded prefix, so the hinted segment is the
        // first position the walk can be contiguous from.
        let (object_key, segment) = load_required_segment(store, namespace_id, start).await?;
        let payload = segment.payload();
        validate_wal_segment_for_replay(namespace_id, payload.prior_head_seq, &segment)
            .map_err(|error| corrupt(&object_key, error))?;
        apply_segment(&mut state, &segment, &object_key)?;
        previous_epoch = payload.writer_epoch;
    }
    let mut walk = WalWalk::after(namespace_id, state.wal_no, state.seq);
    while let Some(segment) = walk.next(store).await.map_err(wal_error)? {
        let payload = segment.envelope().payload();
        if previous_epoch > payload.writer_epoch {
            return Err(corrupt(segment.object_key(), "WAL writer epoch decreases"));
        }
        apply_segment(&mut state, segment.envelope(), segment.object_key())?;
        previous_epoch = payload.writer_epoch;
    }
    Ok(state)
}

async fn load_required_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    wal_no: WalNo,
) -> Result<(String, WalSegmentEnvelope), ControlObjectLoadError> {
    let loaded = load_wal_segment(store, namespace_id, wal_no).await;
    let envelope = loaded
        .envelope
        .map_err(wal_error)?
        .ok_or_else(|| corrupt(&loaded.object_key, "hinted WAL object is missing"))?;
    Ok((loaded.object_key, envelope))
}

pub(super) fn apply_segment(
    state: &mut NamespaceReadState,
    segment: &WalSegmentEnvelope,
    object_key: &str,
) -> Result<(), ControlObjectLoadError> {
    let payload = segment.payload();
    if payload.writer_epoch > state.writer_epoch {
        return Err(corrupt(object_key, "WAL writer epoch exceeds the manifest"));
    }
    state.wal_no = payload.wal_no;
    state.seq = payload.head_seq;
    state.next_inode_id = payload.next_inode_id;
    state.head_commit_id = payload.head_commit_id.clone();
    Ok(())
}

pub(super) fn wal_error(error: WalTailLoadError) -> ControlObjectLoadError {
    match error {
        WalTailLoadError::ReadWal {
            object_key,
            message,
            class,
        } => ControlObjectLoadError::Store {
            object_key,
            message,
            class,
        },
        WalTailLoadError::MissingWalObject { ref object_key }
        | WalTailLoadError::NumberMismatch { ref object_key }
        | WalTailLoadError::HeadSeqMismatch { ref object_key, .. }
        | WalTailLoadError::Replay { ref object_key, .. } => corrupt(object_key, &error),
    }
}

pub(super) fn corrupt(object_key: &str, error: impl std::fmt::Display) -> ControlObjectLoadError {
    ControlObjectLoadError::Codec {
        object_key: object_key.to_owned(),
        message: error.to_string(),
    }
}

/// Probes the next WAL number and extends the cached tail with returned segments.
/// Returns false when a different writer epoch requires full discovery.
pub async fn probe_namespace_wal<S: ObjectStore + ?Sized>(
    store: &S,
    context: &mut RuntimeReadContext,
) -> Result<bool, ControlObjectLoadError> {
    let mut state = context.head.clone();
    let mut cache_key = WalTailProjectionCacheKey {
        namespace_id: state.namespace_id.clone(),
        manifest_no: context.basis.manifest_no(),
        manifest_head_seq: context.basis.manifest().manifest_head_seq,
        head_seq: state.seq,
    };
    let mut projected_tail = None;
    let mut walk = WalWalk::after(&context.head.namespace_id, state.wal_no, state.seq);
    loop {
        let segment = match walk.next(store).await {
            Ok(Some(segment)) => segment,
            Ok(None) => break,
            // A segment the cached head cannot explain belongs to a manifest
            // published after it, such as a recreated generation whose
            // sequences start over. The fresh load is authoritative for
            // corruption.
            Err(WalTailLoadError::Replay { .. }) => return Ok(false),
            Err(error) => return Err(wal_error(error)),
        };
        if segment.envelope().payload().writer_epoch != state.writer_epoch {
            return Ok(false);
        }
        if state.wal_no == context.head.wal_no {
            projected_tail = context.tail_cache.get(&cache_key);
        }
        let before = state.clone();
        apply_segment(&mut state, segment.envelope(), segment.object_key())?;
        if let Some(current) = projected_tail {
            let object_key = segment.object_key().to_owned();
            let tail = ValidatedWalTail::new(vec![segment]);
            let replayed =
                project_validated_wal_tail(&before, &current, Some(state.writer_epoch), &tail)
                    .map_err(|error| corrupt(&object_key, error))?;
            projected_tail = Some(Arc::new(replayed.projected_tail));
        }
    }
    if let Some(projected_tail) = projected_tail {
        cache_key.head_seq = state.seq;
        context.tail_cache.insert(cache_key, projected_tail);
    }
    context.head = state;
    Ok(true)
}
