//! Advances a runtime read context through newly published WAL segments.

use super::read_anchor::{apply_segment, corrupt, validate_segment, wal_error};
use crate::cache::WalTailProjectionCacheKey;
use crate::control_object::ControlObjectLoadError;
use crate::wal::{
    load_wal_segment, project_validated_wal_tail, ValidatedWalSegment, ValidatedWalTail,
};
use crate::RuntimeReadContext;
use loonfs_objectstore::{keys::wal_segment, ObjectStore};
use std::sync::Arc;

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
        let key = wal_segment(&state.namespace_id, &next);
        let Some(segment) = load_wal_segment(store, &state.namespace_id, next)
            .await
            .map_err(|error| wal_error(&key, error))?
        else {
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
