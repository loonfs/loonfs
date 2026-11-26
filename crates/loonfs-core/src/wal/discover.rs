//! Discovers the WAL tip and advances cached namespace views.

use super::frame::{ValidatedWalTail, WalTailLoadError};
use super::reader::{load_wal_object, StoredWalEnvelope, WalWalk};
use super::replay::project_validated_wal_tail;
use crate::cache::{HeadAndWalTailCache, WalTailCacheKey};
use crate::control_object::ControlObjectLoadError;
use crate::namespace::basis::ReadBasis;
use crate::namespace::control::LoadedManifest;
use crate::namespace::state::NamespaceReadState;
use futures::{stream, StreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{NamespaceId, WalNo, MAX_PUBLIC_INTEGER};
use std::ops::ControlFlow;
use std::sync::Arc;

/// Largest window of WAL numbers that one round of [`load_wal_after`] reads,
/// at most 16 GETs in flight. A round is a lockstep wave by design, because
/// each round's results determine whether the reads stop and how wide the next
/// window is.
const WAL_DISCOVERY_PROBE_WAVE: usize = 16;

pub(crate) struct DiscoveredTail {
    pub(crate) head: NamespaceReadState,
    pub(crate) tail: ValidatedWalTail,
}

pub(crate) async fn discover_tail<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &LoadedManifest,
) -> Result<DiscoveredTail, ControlObjectLoadError> {
    let namespace_id = &manifest.payload().namespace_id;
    let mut head = NamespaceReadState::from(manifest.payload());
    let mut objects = Vec::new();
    if !head.status.is_active() {
        return Ok(DiscoveredTail {
            head,
            tail: ValidatedWalTail::new(objects),
        });
    }
    let mut walk = WalWalk::after(namespace_id, head.wal_no, head.seq, head.writer_epoch);
    // Replay needs every object above the folded boundary, so discovery keeps the bodies.
    let outcome = load_wal_after(store, namespace_id, head.wal_no, |object_key, envelope| {
        let object = walk.validate(object_key, envelope).map_err(wal_error)?;
        head = head.after_wal_object(object.envelope().payload());
        objects.push(object);
        Ok(ControlFlow::Continue(()))
    })
    .await?;
    if let WalReadOutcome::Gap { object_key } = outcome {
        return Err(wal_error(WalTailLoadError::MissingWalObject { object_key }));
    }
    Ok(DiscoveredTail {
        head,
        tail: ValidatedWalTail::new(objects),
    })
}

/// Returns the tail above `manifest` that a discovery already read above an
/// earlier manifest, or `None` when the discovery reads the tail above
/// `manifest` instead.
///
/// `read_head` and `read_tail` are the head and the tail that the discovery
/// found above the earlier manifest. The returned tail is the objects of
/// `read_tail` above `manifest`'s `folded_wal_no`, validated again from
/// `manifest`'s head and writer epoch, so it costs no request. That needs the
/// earlier tail to start at or below `manifest`'s `folded_wal_no` and to end
/// at or above it, and both manifests to be active, because discovery reads
/// no WAL above an initializing manifest or a tombstone (format spec,
/// "Resolving the metadata basis").
pub(crate) fn find_kept_tail(
    manifest: &LoadedManifest,
    read_head: &NamespaceReadState,
    read_tail: &ValidatedWalTail,
) -> Option<DiscoveredTail> {
    let mut head = NamespaceReadState::from(manifest.payload());
    let folded = head.folded_wal_no;
    // The number after `read_head.wal_no` was missing when the discovery read
    // it, and `manifest` folds below that number. If a collection had emptied
    // the number, a manifest that folds the number would have been published
    // before that read. The discovery found `manifest` after the read, so
    // `manifest` would fold past the read.
    if !read_head.status.is_active()
        || !head.status.is_active()
        || read_head.folded_wal_no > folded
        || read_head.wal_no < folded
    {
        return None;
    }
    let namespace_id = &manifest.payload().namespace_id;
    let mut walk = WalWalk::after(namespace_id, head.wal_no, head.seq, head.writer_epoch);
    let mut objects = Vec::new();
    for object in read_tail.objects() {
        let payload = object.envelope().payload();
        if payload.wal_no <= folded {
            continue;
        }
        walk.validate_again(object).ok()?;
        head = head.after_wal_object(payload);
        objects.push(object.clone());
    }
    Some(DiscoveredTail {
        head,
        tail: ValidatedWalTail::new(objects),
    })
}

/// How [`load_wal_after`] ended.
enum WalReadOutcome {
    /// The reads reached the first missing number or the largest WAL number,
    /// or `each` stopped them.
    Ended,
    /// A number was still missing when it was read again, and a later number
    /// in the same window was present.
    Gap { object_key: String },
}

/// Loads the WAL objects after `after` in widening windows, and passes each
/// object to `each` in WAL order, until the first number that's still missing
/// or until `each` returns `ControlFlow::Break`.
///
/// The first window is one number, so a caller at the tip sends one GET. Each
/// window that finds every number doubles, up to [`WAL_DISCOVERY_PROBE_WAVE`].
/// A number that's missing below a present one is read again, and if it's
/// still missing, this function returns [`WalReadOutcome::Gap`] with that
/// number's object key. Discovery reports the gap as corruption, and a warm
/// probe returns false so that the read runs discovery.
async fn load_wal_after<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after: WalNo,
    mut each: impl FnMut(String, StoredWalEnvelope) -> Result<ControlFlow<()>, ControlObjectLoadError>,
) -> Result<WalReadOutcome, ControlObjectLoadError> {
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
                None if last_present.is_none_or(|last| index > last) => {
                    return Ok(WalReadOutcome::Ended)
                }
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
                            return Ok(WalReadOutcome::Gap {
                                object_key: again.object_key,
                            })
                        }
                    }
                }
            };
            if each(object_key, envelope)?.is_break() {
                return Ok(WalReadOutcome::Ended);
            }
        }
        if end == MAX_PUBLIC_INTEGER {
            return Ok(WalReadOutcome::Ended);
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

/// How [`load_tail_after_head`] ended.
#[allow(
    clippy::large_enum_variant,
    reason = "one value per read, moved once: indirection would buy an allocation and save nothing"
)]
pub(crate) enum TailLoadOutcome {
    /// The WAL objects after the head up to the first missing number, which
    /// may be none, and the head after them.
    Loaded(DiscoveredTail),
    /// The view needs discovery from the current manifest (see
    /// [`load_tail_after_head`]).
    NeedsDiscovery,
}

/// Loads the WAL objects after `head`, a head that a reader already has, in
/// the same windows as discovery, each validated as the successor of the one
/// before. A cached view's WAL probe reads the WAL this way, and so does a
/// writer session or a floor object put whose put found its number taken,
/// from that number on (format spec, "Publication and group commit" and
/// "Advancing the retention floor").
///
/// Returns [`TailLoadOutcome::NeedsDiscovery`] in two cases:
///
/// - The next object has another writer epoch than `head`. Writer acquisition
///   raises the epoch, so a WAL object at `head`'s epoch must continue `head`,
///   and an object that doesn't is corruption, which the call returns.
/// - A WAL number is still missing below a present one. Maintenance might have
///   folded the objects after `head` into a newer manifest, and garbage
///   collection might be deleting them while the call reads. Discovery from
///   the current manifest still reports a gap above its folded boundary as
///   corruption.
pub(crate) async fn load_tail_after_head<S: ObjectStore + ?Sized>(
    store: &S,
    head: &NamespaceReadState,
) -> Result<TailLoadOutcome, ControlObjectLoadError> {
    let namespace_id = &head.namespace_id;
    let mut walk = WalWalk::after(namespace_id, head.wal_no, head.seq, head.writer_epoch);
    let mut loaded = head.clone();
    let mut objects = Vec::new();
    let mut other_epoch = false;
    let outcome = load_wal_after(store, namespace_id, head.wal_no, |object_key, envelope| {
        if envelope.envelope.payload().writer_epoch != head.writer_epoch {
            other_epoch = true;
            return Ok(ControlFlow::Break(()));
        }
        let object = walk.validate(object_key, envelope).map_err(wal_error)?;
        loaded = loaded.after_wal_object(object.envelope().payload());
        objects.push(object);
        Ok(ControlFlow::Continue(()))
    })
    .await?;
    if other_epoch || matches!(outcome, WalReadOutcome::Gap { .. }) {
        return Ok(TailLoadOutcome::NeedsDiscovery);
    }
    Ok(TailLoadOutcome::Loaded(DiscoveredTail {
        head: loaded,
        tail: ValidatedWalTail::new(objects),
    }))
}

/// Reads the WAL numbers after the cached head (see
/// [`load_tail_after_head`]), and extends the cached tail with the objects it
/// finds. Returns false when the cached view needs full discovery from the
/// current manifest.
pub(crate) async fn probe_namespace_wal<S: ObjectStore + ?Sized>(
    store: &S,
    head_and_wal_tail_cache: &HeadAndWalTailCache,
    basis: &mut ReadBasis,
) -> Result<bool, ControlObjectLoadError> {
    let TailLoadOutcome::Loaded(loaded) = load_tail_after_head(store, &basis.head).await? else {
        return Ok(false);
    };
    if loaded.tail.objects().is_empty() {
        return Ok(true);
    }
    let mut state = basis.head.clone();
    let mut cache_key = WalTailCacheKey {
        namespace_id: state.namespace_id.clone(),
        manifest_no: basis.basis.manifest_no(),
        head_seq: state.seq,
        wal_no: state.wal_no,
    };
    let mut tail = head_and_wal_tail_cache.get_tail(&cache_key);
    let mut extended = false;
    for object in loaded.tail.objects() {
        let before = state.clone();
        state = state.after_wal_object(object.envelope().payload());
        cache_key.head_seq = state.seq;
        cache_key.wal_no = state.wal_no;
        if let Some(cached) = head_and_wal_tail_cache.get_tail(&cache_key) {
            tail = Some(cached);
            extended = false;
        } else if let Some(current) = tail.take() {
            let objects = ValidatedWalTail::new(vec![object.clone()]);
            let replayed =
                project_validated_wal_tail(&before, Arc::unwrap_or_clone(current), &objects)
                    .map_err(|error| corrupt(object.object_key(), error))?;
            tail = Some(Arc::new(replayed.projected_tail));
            extended = true;
        }
    }
    if let Some(tail) = tail.filter(|_| extended) {
        head_and_wal_tail_cache.insert_tail(cache_key, tail);
    }
    basis.head = loaded.head;
    Ok(true)
}
