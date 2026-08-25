//! One reconciled read of a namespace's control objects: `wal/head.json`,
//! `metadata/root.json`, and `wal/floor.json`. Every reconciliation of that
//! tuple lives here.

use crate::control_object::ControlObjectLoadError;
use crate::namespace::basis::{metadata_basis_without_root, namespace_birth_seq, MetadataBasis};
use crate::namespace::control::{
    load_head_object, load_metadata_root_object_if_present, load_wal_floor_object,
    LoadedHeadObject, LoadedMetadataRootObject,
};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// Reconciliation rounds one load may spend.
///
/// Each round rereads the one object monotonicity implicates, and per-key
/// read-after-write means that reread cannot return the version the
/// observation already contradicts. Only two objects are ever implicated, so
/// a quiet namespace settles within two rounds; the third is slack for one
/// written throughout the load.
const RECONCILE_ROUNDS: usize = 3;

/// The namespace's control objects, read as one reconciled observation.
pub(crate) struct NamespaceControlSnapshot {
    pub(crate) head: LoadedHeadObject,
    pub(crate) root: Option<LoadedMetadataRootObject>,
    pub(crate) retention_floor_seq: ChangeSeq,
}

impl NamespaceControlSnapshot {
    /// The metadata basis this snapshot authorizes.
    pub(crate) fn basis(&self) -> MetadataBasis {
        basis_of(&self.head.state, self.root.as_ref())
    }
}

/// The head and its resolved basis, read together.
pub(crate) struct LoadedNamespaceBasis {
    pub(crate) head: LoadedHeadObject,
    pub(crate) basis: MetadataBasis,
}

/// Reads the head and retention floor, reconciled with each other.
///
/// The metadata root is not read: callers that only report namespace state do
/// not need it.
pub(crate) async fn load_head_and_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(LoadedHeadObject, ChangeSeq), ControlObjectLoadError> {
    let (head, floor) = futures::join!(
        load_head_object(store, namespace_id),
        load_floor_seq_if_present(store, namespace_id)
    );
    let head = head?;
    let floor_seq = Some(resolve_floor(floor?, &head.state));
    let reads = reconcile_reads(
        store,
        namespace_id,
        ControlReads {
            head,
            root: RootObservation::Unread,
            floor_seq,
        },
    )
    .await?;
    let retention_floor_seq = reads.retention_floor_seq();
    Ok((reads.head, retention_floor_seq))
}

/// Reads the head and the basis it authorizes.
///
/// The floor is read only when no root exists, where it separates a namespace
/// that has published no root from one whose root was lost.
pub(crate) async fn load_head_and_metadata_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<LoadedNamespaceBasis, ControlObjectLoadError> {
    let (head, root) = futures::join!(
        load_head_object(store, namespace_id),
        load_metadata_root_object_if_present(store, namespace_id)
    );
    let reads = reconcile_reads(
        store,
        namespace_id,
        ControlReads {
            head: head?,
            root: RootObservation::of(root?),
            floor_seq: None,
        },
    )
    .await?;
    let head = reads.head;
    let root = reads.root.into_loaded();
    Ok(LoadedNamespaceBasis {
        basis: basis_of(&head.state, root.as_ref()),
        head,
    })
}

/// Reads the head, metadata root, and retention floor as one snapshot.
pub(crate) async fn load_control_snapshot<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceControlSnapshot, ControlObjectLoadError> {
    let (head, root, floor) = futures::join!(
        load_head_object(store, namespace_id),
        load_metadata_root_object_if_present(store, namespace_id),
        load_floor_seq_if_present(store, namespace_id)
    );
    let head = head?;
    let floor_seq = Some(resolve_floor(floor?, &head.state));
    let reads = reconcile_reads(
        store,
        namespace_id,
        ControlReads {
            head,
            root: RootObservation::of(root?),
            floor_seq,
        },
    )
    .await?;
    Ok(NamespaceControlSnapshot {
        retention_floor_seq: reads.retention_floor_seq(),
        root: reads.root.into_loaded(),
        head: reads.head,
    })
}

/// Reads the retention floor, treating a missing floor object as the
/// namespace's birth sequence.
pub(crate) async fn resolve_retention_floor_seq<S: ObjectStore + ?Sized>(
    store: &S,
    head: &HeadState,
) -> Result<ChangeSeq, ControlObjectLoadError> {
    let stored = load_floor_seq_if_present(store, &head.namespace_id).await?;
    Ok(resolve_floor(stored, head))
}

/// The basis a head authorizes: its own root when it has one, otherwise the
/// fork source's manifest or genesis.
fn basis_of(head: &HeadState, root: Option<&LoadedMetadataRootObject>) -> MetadataBasis {
    match root {
        Some(root) => MetadataBasis::Manifest(root.state.manifest.clone()),
        None => metadata_basis_without_root(head),
    }
}

/// What a load observed of `metadata/root.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootObservation<T> {
    /// This load does not read the root, so invariants that need it go
    /// unchecked rather than reading absence into it.
    Unread,
    Absent,
    Present(T),
}

impl RootObservation<LoadedMetadataRootObject> {
    fn of(root: Option<LoadedMetadataRootObject>) -> Self {
        root.map_or(Self::Absent, Self::Present)
    }

    fn coverage(&self) -> RootObservation<ChangeSeq> {
        match self {
            Self::Unread => RootObservation::Unread,
            Self::Absent => RootObservation::Absent,
            Self::Present(root) => RootObservation::Present(root.state.manifest.manifest_head_seq),
        }
    }

    /// The loaded root, if this load read one and found it.
    fn into_loaded(self) -> Option<LoadedMetadataRootObject> {
        match self {
            Self::Unread | Self::Absent => None,
            Self::Present(root) => Some(root),
        }
    }
}

/// The control objects one load holds so far.
struct ControlReads {
    head: LoadedHeadObject,
    root: RootObservation<LoadedMetadataRootObject>,
    /// `None` while this load has not read the floor.
    floor_seq: Option<ChangeSeq>,
}

impl ControlReads {
    fn seqs(&self) -> ControlSeqs {
        ControlSeqs {
            head_seq: self.head.state.seq,
            birth_seq: namespace_birth_seq(&self.head.state),
            root: self.root.coverage(),
            floor_seq: self.floor_seq,
        }
    }

    /// The floor this load resolved. A load that never read the floor
    /// resolves it the way an absent floor object resolves: the namespace's
    /// birth sequence, below which it has no history to retain.
    fn retention_floor_seq(&self) -> ChangeSeq {
        resolve_floor(self.floor_seq, &self.head.state)
    }
}

/// Rereads implicated objects until the tuple is one the protocol can
/// produce, or reports the state that remains impossible.
async fn reconcile_reads<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    mut reads: ControlReads,
) -> Result<ControlReads, ControlObjectLoadError> {
    let mut rounds = 0;
    loop {
        // A basis load reads the floor only once it finds no root: there the
        // floor is what separates a namespace that has published none from
        // one whose root was lost.
        if reads.floor_seq.is_none() && matches!(reads.root, RootObservation::Absent) {
            reads.floor_seq = Some(resolve_retention_floor_seq(store, &reads.head.state).await?);
        }
        let inconsistency = match reconcile(namespace_id, reads.seqs()) {
            Ok(()) => return Ok(reads),
            Err(inconsistency) => inconsistency,
        };
        if rounds == RECONCILE_ROUNDS {
            return Err(inconsistency.error);
        }
        rounds += 1;
        match inconsistency.stale {
            StaleObject::Head => reads.head = load_head_object(store, namespace_id).await?,
            StaleObject::Root => {
                let root = load_metadata_root_object_if_present(store, namespace_id).await?;
                reads.root = RootObservation::of(root);
            }
        }
    }
}

/// The sequences one observation of the control objects carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ControlSeqs {
    head_seq: ChangeSeq,
    birth_seq: ChangeSeq,
    root: RootObservation<ChangeSeq>,
    /// `None` while the floor is unread.
    floor_seq: Option<ChangeSeq>,
}

/// The object a reread can still explain an impossible observation by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaleObject {
    Head,
    Root,
}

/// An observation the protocol cannot produce, with the object to reread and
/// the error it settles into if rereading does not explain it.
struct ControlInconsistency {
    stale: StaleObject,
    error: ControlObjectLoadError,
}

/// Checks the namespace control invariants over one observation.
///
/// Each object advances monotonically, so whichever one sits behind another
/// is the stale read:
///
/// ```text
/// namespace_birth_seq <= retention_floor_seq <= head.seq
/// retention_floor_seq <= root.manifest_head_seq <= head.seq   (with a root)
/// retention_floor_seq == namespace_birth_seq                  (without one)
/// ```
///
/// A floor below the birth sequence is not reported: retaining history the
/// namespace never wrote is conservative.
fn reconcile(namespace_id: &NamespaceId, seqs: ControlSeqs) -> Result<(), ControlInconsistency> {
    if let RootObservation::Present(root_seq) = seqs.root {
        if root_seq > seqs.head_seq {
            return Err(ControlInconsistency {
                stale: StaleObject::Head,
                error: ControlObjectLoadError::RootAheadOfHead {
                    root_manifest_head_seq: root_seq,
                    head_seq: seqs.head_seq,
                },
            });
        }
    }
    let Some(floor_seq) = seqs.floor_seq else {
        return Ok(());
    };
    if floor_seq > seqs.head_seq {
        return Err(ControlInconsistency {
            stale: StaleObject::Head,
            error: ControlObjectLoadError::FloorAheadOfHead {
                floor_seq,
                head_seq: seqs.head_seq,
            },
        });
    }
    match seqs.root {
        RootObservation::Present(root_seq) if floor_seq > root_seq => Err(ControlInconsistency {
            stale: StaleObject::Root,
            error: ControlObjectLoadError::FloorAheadOfRoot {
                floor_seq,
                root_manifest_head_seq: root_seq,
            },
        }),
        // Only a published root can carry the floor above the birth sequence,
        // so an absent root under a raised floor was read stale or is lost.
        RootObservation::Absent if floor_seq > seqs.birth_seq => Err(ControlInconsistency {
            stale: StaleObject::Root,
            error: ControlObjectLoadError::MissingRootAfterFloor {
                namespace_id: namespace_id.clone(),
                floor_seq,
            },
        }),
        _ => Ok(()),
    }
}

/// Reads the floor object's sequence, or `None` when no floor object exists.
async fn load_floor_seq_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Option<ChangeSeq>, ControlObjectLoadError> {
    match load_wal_floor_object(store, namespace_id).await {
        Ok(loaded) => Ok(Some(loaded.state.floor_seq)),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Resolves an unread or absent floor object to the namespace's birth
/// sequence.
///
/// A namespace has no WAL history below its birth sequence, so "retain from
/// birth" is the most conservative reading of an absent floor: create and
/// fork write no floor, and the first advance publishes one.
fn resolve_floor(stored: Option<ChangeSeq>, head: &HeadState) -> ChangeSeq {
    stored.unwrap_or_else(|| namespace_birth_seq(head))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace() -> NamespaceId {
        NamespaceId::parse("demo").expect("valid namespace id")
    }

    fn seqs(
        head_seq: u64,
        birth_seq: u64,
        root: RootObservation<ChangeSeq>,
        floor_seq: Option<u64>,
    ) -> ControlSeqs {
        ControlSeqs {
            head_seq: ChangeSeq(head_seq),
            birth_seq: ChangeSeq(birth_seq),
            root,
            floor_seq: floor_seq.map(ChangeSeq),
        }
    }

    /// The object to reread and the error the observation settles into, or
    /// `None` when the observation is one the protocol can produce.
    type Verdict = Option<(StaleObject, ControlObjectLoadError)>;

    #[test]
    fn the_reconciler_classifies_every_control_state() {
        let namespace_id = namespace();
        let root_at = |seq: u64| RootObservation::Present(ChangeSeq(seq));
        let cases: Vec<(&str, ControlSeqs, Verdict)> = vec![
            (
                "rootless namespace at birth",
                seqs(0, 0, RootObservation::Absent, Some(0)),
                None,
            ),
            (
                "rootless fork target at its fork point",
                seqs(7, 7, RootObservation::Absent, Some(7)),
                None,
            ),
            (
                "rooted namespace whose floor trails its root",
                seqs(9, 0, root_at(6), Some(4)),
                None,
            ),
            (
                "status load that never reads the root",
                seqs(9, 0, RootObservation::Unread, Some(9)),
                None,
            ),
            (
                "raised floor beside an unread root",
                seqs(9, 0, RootObservation::Unread, Some(6)),
                None,
            ),
            (
                "basis load that has not read the floor",
                seqs(9, 0, root_at(6), None),
                None,
            ),
            (
                "root ahead of a stale head",
                seqs(4, 0, root_at(6), Some(0)),
                Some((
                    StaleObject::Head,
                    ControlObjectLoadError::RootAheadOfHead {
                        root_manifest_head_seq: ChangeSeq(6),
                        head_seq: ChangeSeq(4),
                    },
                )),
            ),
            (
                "floor ahead of a stale head",
                seqs(4, 0, root_at(4), Some(6)),
                Some((
                    StaleObject::Head,
                    ControlObjectLoadError::FloorAheadOfHead {
                        floor_seq: ChangeSeq(6),
                        head_seq: ChangeSeq(4),
                    },
                )),
            ),
            (
                "floor ahead of a stale root",
                seqs(9, 0, root_at(4), Some(6)),
                Some((
                    StaleObject::Root,
                    ControlObjectLoadError::FloorAheadOfRoot {
                        floor_seq: ChangeSeq(6),
                        root_manifest_head_seq: ChangeSeq(4),
                    },
                )),
            ),
            (
                "root absent under a raised floor",
                seqs(9, 0, RootObservation::Absent, Some(6)),
                Some((
                    StaleObject::Root,
                    ControlObjectLoadError::MissingRootAfterFloor {
                        namespace_id: namespace(),
                        floor_seq: ChangeSeq(6),
                    },
                )),
            ),
        ];

        for (name, observation, expected) in cases {
            let verdict = reconcile(&namespace_id, observation)
                .err()
                .map(|inconsistency| (inconsistency.stale, inconsistency.error));
            assert_eq!(verdict, expected, "{name}");
        }
    }
}
