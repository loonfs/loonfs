//! Discovers the WAL tip and advances cached namespace views.

use super::frame::{ValidatedWalTail, WalTailLoadError};
use super::reader::{load_wal_segment, WalWalk};
use super::replay::project_validated_wal_tail;
use crate::cache::WalTailProjectionCacheKey;
use crate::control_object::ControlObjectLoadError;
use crate::namespace::control::LoadedManifest;
use crate::namespace::state::NamespaceReadState;
use crate::RuntimeReadContext;
use loonfs_api::{NamespaceId, WalNo};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub(crate) async fn discover_tip<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &LoadedManifest,
    hinted_wal_no: WalNo,
) -> Result<NamespaceReadState, ControlObjectLoadError> {
    let mut state = NamespaceReadState::from(manifest.state.envelope.payload());
    if state.status.is_deleted() {
        return Ok(state);
    }
    let start = hinted_wal_no.max(state.folded_wal_no);
    let mut walk = WalWalk::after(namespace_id, state.wal_no, state.seq, state.writer_epoch);
    if start > state.folded_wal_no {
        let segment = walk.at_hint(store, start).await.map_err(wal_error)?;
        state = state.after_segment(segment.envelope().payload());
    }
    while let Some(segment) = walk.next(store).await.map_err(wal_error)? {
        state = state.after_segment(segment.envelope().payload());
    }
    Ok(state)
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
/// Returns false when the next segment has another writer epoch, which requires
/// full discovery. Writer acquisition raises the epoch, so a
/// segment at the cached epoch must continue the cached head.
pub async fn probe_namespace_wal<S: ObjectStore + ?Sized>(
    store: &S,
    context: &mut RuntimeReadContext,
) -> Result<bool, ControlObjectLoadError> {
    let mut state = context.head.clone();
    let mut cache_key = WalTailProjectionCacheKey {
        namespace_id: state.namespace_id.clone(),
        manifest_no: context.basis.manifest_no(),
        head_seq: state.seq,
    };
    let mut projected_tail = None;
    let namespace_id = state.namespace_id.clone();
    let mut walk = WalWalk::after(&namespace_id, state.wal_no, state.seq, state.writer_epoch);
    while let Ok(wal_no) = state.wal_no.successor() {
        let loaded = load_wal_segment(store, &state.namespace_id, wal_no).await;
        let Some(envelope) = loaded.envelope.map_err(wal_error)? else {
            break;
        };
        if envelope.payload().writer_epoch != state.writer_epoch {
            return Ok(false);
        }
        let segment = walk
            .validate(loaded.object_key, envelope)
            .map_err(wal_error)?;
        if state.wal_no == context.head.wal_no {
            projected_tail = context.tail_cache.get(&cache_key);
        }
        let before = state.clone();
        state = state.after_segment(segment.envelope().payload());
        if let Some(current) = projected_tail {
            let object_key = segment.object_key().to_owned();
            let tail = ValidatedWalTail::new(vec![segment]);
            let replayed = project_validated_wal_tail(&before, &current, &tail)
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
