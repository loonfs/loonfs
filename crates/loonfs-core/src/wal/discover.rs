//! Discovers the WAL tip and advances cached namespace views.

use super::frame::{ValidatedWalSegment, ValidatedWalTail, WalTailLoadError};
use super::reader::{load_wal_segment, LoadedWalSegment};
use super::replay::{project_validated_wal_tail, validate_wal_segment_for_replay};
use crate::cache::WalTailProjectionCacheKey;
use crate::control_object::ControlObjectLoadError;
use crate::namespace::control::LoadedManifest;
use crate::namespace::state::NamespaceReadState;
use crate::RuntimeReadContext;
use loonfs_api::{ChangeSeq, NamespaceId};
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
    let mut number = start;
    let mut previous_epoch = loonfs_api::WriterEpoch(0);
    let mut last_record = None;
    if number > state.last_folded_wal_no {
        let segment = load_required_segment(store, namespace_id, number).await?;
        let payload = segment.payload();
        validate_segment(
            namespace_id,
            payload.base_head_seq,
            &segment,
            &manifest.object_key,
        )?;
        apply_segment(&mut state, &segment, &mut last_record, &manifest.object_key)?;
        previous_epoch = payload.writer_epoch;
    }
    while let Ok(next) = number.successor() {
        let loaded = load_wal_segment(store, namespace_id, next).await;
        let Some(segment) = loaded
            .envelope
            .map_err(|error| wal_error(&manifest.object_key, error))?
        else {
            break;
        };
        let payload = segment.payload();
        if previous_epoch > payload.writer_epoch {
            return Err(corrupt(&manifest.object_key, "WAL writer epoch decreases"));
        }
        validate_segment(namespace_id, state.seq, &segment, &manifest.object_key)?;
        apply_segment(&mut state, &segment, &mut last_record, &manifest.object_key)?;
        previous_epoch = payload.writer_epoch;
        number = next;
    }
    let mut prior = start;
    while last_record.is_none()
        && state.seq > manifest.envelope.payload().head_seq
        && prior > state.last_folded_wal_no
    {
        let segment = load_required_segment(store, namespace_id, prior).await?;
        last_record = segment
            .payload()
            .records
            .last()
            .map(|record| record.commit_id.clone());
        prior = loonfs_api::WalNo(prior.0 - 1);
    }
    if let Some(commit_id) = last_record {
        state.head_commit_id = commit_id;
    }
    Ok(state)
}

pub(super) fn validate_segment(
    namespace_id: &NamespaceId,
    base_seq: ChangeSeq,
    segment: &loonfs_api::wire::wal::WalSegmentEnvelope,
    object_key: &str,
) -> Result<(), ControlObjectLoadError> {
    validate_wal_segment_for_replay(namespace_id, base_seq, segment)
        .map_err(|error| corrupt(object_key, error))
}

async fn load_required_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    wal_no: loonfs_api::WalNo,
) -> Result<loonfs_api::wire::wal::WalSegmentEnvelope, ControlObjectLoadError> {
    let loaded = load_wal_segment(store, namespace_id, wal_no).await;
    loaded
        .envelope
        .map_err(|error| wal_error(&loaded.object_key, error))?
        .ok_or_else(|| corrupt(&loaded.object_key, "hinted WAL object is missing"))
}

pub(super) fn apply_segment(
    state: &mut NamespaceReadState,
    segment: &loonfs_api::wire::wal::WalSegmentEnvelope,
    last_record: &mut Option<loonfs_api::CommitId>,
    object_key: &str,
) -> Result<(), ControlObjectLoadError> {
    let payload = segment.payload();
    if payload.writer_epoch > state.writer_epoch {
        return Err(corrupt(object_key, "WAL writer epoch exceeds the manifest"));
    }
    state.wal_no = payload.wal_no;
    state.seq = payload.end_seq;
    state.next_inode_id = payload.next_inode_id;
    if let Some(record) = payload.records.last() {
        *last_record = Some(record.commit_id.clone());
    }
    Ok(())
}

pub(super) fn wal_error(object_key: &str, error: WalTailLoadError) -> ControlObjectLoadError {
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
        error => corrupt(object_key, error),
    }
}

pub(super) fn corrupt(object_key: &str, error: impl std::fmt::Display) -> ControlObjectLoadError {
    ControlObjectLoadError::Codec {
        object_key: object_key.to_owned(),
        message: error.to_string(),
    }
}

/// Probes the next WAL number and extends cached rows with the returned segments.
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
    let mut rows = None;
    let mut last_record = None;
    while let Ok(next) = state.wal_no.successor() {
        let LoadedWalSegment {
            object_key: key,
            envelope,
        } = load_wal_segment(store, &state.namespace_id, next).await;
        let Some(segment) = envelope.map_err(|error| wal_error(&key, error))? else {
            break;
        };
        let epoch = segment.payload().writer_epoch;
        if epoch != state.writer_epoch {
            return Ok(false);
        }
        validate_segment(&state.namespace_id, state.seq, &segment, &key)?;
        if state.wal_no == context.head.wal_no {
            rows = context.tail_cache.get(&cache_key);
        }
        let before = state.clone();
        apply_segment(&mut state, &segment, &mut last_record, &key)?;
        if let Some(current) = rows {
            let tail = ValidatedWalTail::new(vec![ValidatedWalSegment::new(key.clone(), segment)]);
            let replayed =
                project_validated_wal_tail(&before, &current, Some(state.writer_epoch), &tail)
                    .map_err(|error| corrupt(&key, error))?;
            rows = Some(Arc::new(replayed.resulting_metadata_state));
        }
        if let Some(commit_id) = &last_record {
            state.head_commit_id = commit_id.clone();
        }
    }
    if let Some(rows) = rows {
        cache_key.head_seq = state.seq;
        context.tail_cache.insert(cache_key, rows);
    }
    context.head = state;
    Ok(true)
}
