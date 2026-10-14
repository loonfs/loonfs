//! Discovers the WAL tip and advances cached namespace views.

use super::frame::{ValidatedWalTail, WalTailLoadError};
use super::reader::{load_wal_object, WalWalk};
use super::replay::project_validated_wal_tail;
use crate::cache::WalTailProjectionCacheKey;
use crate::control_object::ControlObjectLoadError;
use crate::namespace::control::LoadedManifest;
use crate::namespace::state::NamespaceReadState;
use crate::RuntimeReadContext;
use futures::{stream, StreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::WalObjectEnvelope;
use loonfs_types::{NamespaceId, WalNo, MAX_PUBLIC_INTEGER};
use std::ops::ControlFlow;
use std::sync::Arc;

/// Largest window of WAL numbers that one round of [`read_wal_after`] reads,
/// at most 16 GETs in flight. A round is a lockstep wave by design, because
/// each round's results decide whether the reads stop and how wide the next
/// window is.
const WAL_DISCOVERY_PROBE_WAVE: usize = 16;

pub(crate) struct DiscoveredTail {
    pub(crate) head: NamespaceReadState,
    pub(crate) tail: ValidatedWalTail,
}

pub(crate) async fn discover_tail<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &LoadedManifest,
) -> Result<DiscoveredTail, ControlObjectLoadError> {
    let mut head = NamespaceReadState::from(manifest.state.envelope.payload());
    let mut objects = Vec::new();
    if head.status.is_deleted() {
        return Ok(DiscoveredTail {
            head,
            tail: ValidatedWalTail::new(objects),
        });
    }
    let mut walk = WalWalk::after(namespace_id, head.wal_no, head.seq, head.writer_epoch);
    // Replay needs every object above the folded boundary, so discovery keeps the bodies.
    read_wal_after(store, namespace_id, head.wal_no, |object_key, envelope| {
        let object = walk.validate(object_key, envelope).map_err(wal_error)?;
        head = head.after_wal_object(object.envelope().payload());
        objects.push(object);
        Ok(ControlFlow::Continue(()))
    })
    .await?;
    Ok(DiscoveredTail {
        head,
        tail: ValidatedWalTail::new(objects),
    })
}

/// Reads the WAL numbers after `after` in widening windows, and passes each
/// object to `visit` in WAL order, until the first number that's still missing
/// or until `visit` returns `ControlFlow::Break`.
///
/// The first window is one number, so a caller at the tip sends one GET. Each
/// window that finds every number doubles, up to [`WAL_DISCOVERY_PROBE_WAVE`].
/// A number that's missing below a present one is read again, and if it's
/// still missing, this function returns a missing-object error.
async fn read_wal_after<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after: WalNo,
    mut visit: impl FnMut(String, WalObjectEnvelope) -> Result<ControlFlow<()>, ControlObjectLoadError>,
) -> Result<(), ControlObjectLoadError> {
    let mut through = after.0;
    let mut window = 1;
    loop {
        let first = through + 1;
        let end = (through + window as u64).min(MAX_PUBLIC_INTEGER);
        let loaded = stream::iter(through..end)
            .map(|number| load_wal_object(store, namespace_id, WalNo(number + 1)))
            .buffered(window)
            .collect::<Vec<_>>()
            .await;
        let last_present = loaded
            .iter()
            .rposition(|loaded| matches!(loaded.envelope, Ok(Some(_))));
        for (index, loaded) in loaded.into_iter().enumerate() {
            let (object_key, envelope) = match loaded.envelope.map_err(wal_error)? {
                Some(envelope) => (loaded.object_key, envelope),
                None if last_present.is_none_or(|last| index > last) => return Ok(()),
                None => {
                    // The window's reads don't happen at one moment, so a number that's
                    // missing here might have been published before a later number was
                    // read. Only a number that's still missing when it's read again is
                    // missing.
                    let again =
                        load_wal_object(store, namespace_id, WalNo(first + index as u64)).await;
                    match again.envelope.map_err(wal_error)? {
                        Some(envelope) => (again.object_key, envelope),
                        None => {
                            return Err(wal_error(WalTailLoadError::MissingWalObject {
                                object_key: again.object_key,
                            }))
                        }
                    }
                }
            };
            if visit(object_key, envelope)?.is_break() {
                return Ok(());
            }
        }
        if end == MAX_PUBLIC_INTEGER {
            return Ok(());
        }
        through = end;
        window = (window * 2).min(WAL_DISCOVERY_PROBE_WAVE);
    }
}

pub(super) fn wal_error(error: WalTailLoadError) -> ControlObjectLoadError {
    match error {
        WalTailLoadError::ReadWal {
            object_key,
            message,
            kind,
        } => ControlObjectLoadError::Store {
            object_key,
            message,
            kind,
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

/// Reads the WAL numbers after the cached head, in the same windows as
/// discovery, and extends the cached tail with the objects it finds.
///
/// Returns false when the next object has another writer epoch, which requires
/// full discovery. Writer acquisition raises the epoch, so a WAL object at the
/// cached epoch must continue the cached head.
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
    let mut tail = None;
    let mut extended = false;
    let namespace_id = state.namespace_id.clone();
    let mut walk = WalWalk::after(&namespace_id, state.wal_no, state.seq, state.writer_epoch);
    let mut other_epoch = false;
    let visit = |object_key: String,
                 envelope: WalObjectEnvelope|
     -> Result<ControlFlow<()>, ControlObjectLoadError> {
        if envelope.payload().writer_epoch != state.writer_epoch {
            other_epoch = true;
            return Ok(ControlFlow::Break(()));
        }
        let object = walk.validate(object_key, envelope).map_err(wal_error)?;
        if state.wal_no == context.head.wal_no {
            tail = context.head_state.get_tail(&cache_key);
        }
        let before = state.clone();
        state = state.after_wal_object(object.envelope().payload());
        cache_key.head_seq = state.seq;
        if let Some(cached) = context.head_state.get_tail(&cache_key) {
            tail = Some(cached);
            extended = false;
        } else if let Some(current) = tail.take() {
            let object_key = object.object_key().to_owned();
            let objects = ValidatedWalTail::new(vec![object]);
            let replayed =
                project_validated_wal_tail(&before, Arc::unwrap_or_clone(current), &objects)
                    .map_err(|error| corrupt(&object_key, error))?;
            tail = Some(Arc::new(replayed.projected_tail));
            extended = true;
        }
        Ok(ControlFlow::Continue(()))
    };
    read_wal_after(store, &namespace_id, context.head.wal_no, visit).await?;
    if other_epoch {
        return Ok(false);
    }
    if let Some(tail) = tail.filter(|_| extended) {
        context.head_state.insert_tail(cache_key, tail);
    }
    context.head = state;
    Ok(true)
}
