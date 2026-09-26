//! Discovers the WAL tip and advances cached namespace views.

use super::frame::{ValidatedWalTail, WalTailLoadError};
use super::reader::{load_wal_segment, WalWalk, WAL_REPLAY_READ_CONCURRENCY};
use super::replay::project_validated_wal_tail;
use crate::cache::WalTailProjectionCacheKey;
use crate::control_object::ControlObjectLoadError;
use crate::namespace::control::LoadedManifest;
use crate::namespace::state::NamespaceReadState;
use crate::RuntimeReadContext;
use futures::{stream, StreamExt};
use loonfs_api::{NamespaceId, WalNo, MAX_PUBLIC_INTEGER};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub(crate) struct DiscoveredTail {
    pub(crate) head: NamespaceReadState,
    pub(crate) segments: ValidatedWalTail,
}

pub(crate) async fn discover_tail<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &LoadedManifest,
) -> Result<DiscoveredTail, ControlObjectLoadError> {
    let mut head = NamespaceReadState::from(manifest.state.envelope.payload());
    let mut segments = Vec::new();
    if head.status.is_deleted() {
        return Ok(DiscoveredTail {
            head,
            segments: ValidatedWalTail::new(segments),
        });
    }
    let mut walk = WalWalk::after(namespace_id, head.wal_no, head.seq, head.writer_epoch);
    let mut window = 1;
    // One pass reads the tail and finds its end: replay needs every object above the
    // folded boundary anyway, so the walk keeps the bodies. Windows grow from one
    // number to the replay concurrency; the last window can spend up to seven reads
    // on absent numbers.
    loop {
        let end = (head.wal_no.0 + window as u64).min(MAX_PUBLIC_INTEGER);
        let loaded = stream::iter(head.wal_no.0..end)
            .map(|number| load_wal_segment(store, namespace_id, WalNo(number + 1)))
            .buffered(window)
            .collect::<Vec<_>>()
            .await;
        let mut absent: Option<String> = None;
        for loaded in loaded {
            if let Some(object_key) = &absent {
                if matches!(loaded.envelope, Ok(Some(_))) {
                    return Err(wal_error(WalTailLoadError::MissingWalObject {
                        object_key: object_key.clone(),
                    }));
                }
                continue;
            }
            let Some(envelope) = loaded.envelope.map_err(wal_error)? else {
                absent = Some(loaded.object_key);
                continue;
            };
            let segment = walk
                .validate(loaded.object_key, envelope)
                .map_err(wal_error)?;
            head = head.after_segment(segment.envelope().payload());
            segments.push(segment);
        }
        if absent.is_some() || end == MAX_PUBLIC_INTEGER {
            break;
        }
        window = (window * 2).min(WAL_REPLAY_READ_CONCURRENCY);
    }
    Ok(DiscoveredTail {
        head,
        segments: ValidatedWalTail::new(segments),
    })
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
