//! Loads consistent combinations of a namespace's head, metadata root, and
//! retention floor.

use crate::control_object::ControlObjectLoadError;
use crate::limits::CONTROL_SNAPSHOT_REREAD_LIMIT;
use crate::namespace::basis::{metadata_basis_without_root, namespace_birth_seq, MetadataBasis};
use crate::namespace::control::{
    load_head_object, load_metadata_root_object_if_present, load_wal_floor_object,
    LoadedHeadObject, LoadedMetadataRootObject,
};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// The namespace's control objects, read as one reconciled observation.
pub(crate) struct NamespaceControlSnapshot {
    pub(crate) head: LoadedHeadObject,
    pub(crate) root: Option<LoadedMetadataRootObject>,
    pub(crate) retention_floor_seq: ChangeSeq,
}

impl NamespaceControlSnapshot {
    pub(crate) fn basis(&self) -> MetadataBasis {
        basis_of(&self.head.state, self.root.as_ref())
    }
}

/// The head and its resolved basis, read together.
pub(crate) struct LoadedNamespaceBasis {
    pub(crate) head: LoadedHeadObject,
    pub(crate) basis: MetadataBasis,
}

/// Reads a consistent head and retention floor without reading the root.
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

/// Reads the head and its metadata basis. The floor is read only when the root
/// is absent.
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

/// Reads all three namespace control objects as one snapshot.
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

/// Reads the retention floor, defaulting to the namespace's birth sequence.
pub(crate) async fn resolve_retention_floor_seq<S: ObjectStore + ?Sized>(
    store: &S,
    head: &HeadState,
) -> Result<ChangeSeq, ControlObjectLoadError> {
    let stored = load_floor_seq_if_present(store, &head.namespace_id).await?;
    Ok(resolve_floor(stored, head))
}

fn basis_of(head: &HeadState, root: Option<&LoadedMetadataRootObject>) -> MetadataBasis {
    match root {
        Some(root) => MetadataBasis::Manifest(root.state.manifest.clone()),
        None => metadata_basis_without_root(head),
    }
}

/// What a load observed of `metadata/root.json`.
enum RootObservation<T> {
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

    fn into_loaded(self) -> Option<LoadedMetadataRootObject> {
        match self {
            Self::Unread | Self::Absent => None,
            Self::Present(root) => Some(root),
        }
    }
}

struct ControlReads {
    head: LoadedHeadObject,
    root: RootObservation<LoadedMetadataRootObject>,
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

    fn retention_floor_seq(&self) -> ChangeSeq {
        resolve_floor(self.floor_seq, &self.head.state)
    }
}

/// Rereads stale objects until the snapshot is consistent.
async fn reconcile_reads<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    mut reads: ControlReads,
) -> Result<ControlReads, ControlObjectLoadError> {
    let mut rounds = 0;
    loop {
        // A raised floor distinguishes a missing root from a namespace that
        // has never published one.
        if reads.floor_seq.is_none() && matches!(reads.root, RootObservation::Absent) {
            reads.floor_seq = Some(resolve_retention_floor_seq(store, &reads.head.state).await?);
        }
        let inconsistency = match reconcile(namespace_id, reads.seqs()) {
            Ok(()) => return Ok(reads),
            Err(inconsistency) => inconsistency,
        };
        if rounds == CONTROL_SNAPSHOT_REREAD_LIMIT {
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

struct ControlSeqs {
    head_seq: ChangeSeq,
    birth_seq: ChangeSeq,
    root: RootObservation<ChangeSeq>,
    floor_seq: Option<ChangeSeq>,
}

enum StaleObject {
    Head,
    Root,
}

struct ControlInconsistency {
    stale: StaleObject,
    error: ControlObjectLoadError,
}

/// Checks the sequence relationships between the control objects.
///
/// ```text
/// namespace_birth_seq <= retention_floor_seq <= head.seq
/// retention_floor_seq <= root.manifest_head_seq <= head.seq   (with a root)
/// retention_floor_seq == namespace_birth_seq                  (without one)
/// ```
///
/// A floor below the birth sequence is conservative and remains valid.
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
        // A raised floor requires a published root.
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

/// A namespace without a floor retains history from its birth sequence.
fn resolve_floor(stored: Option<ChangeSeq>, head: &HeadState) -> ChangeSeq {
    stored.unwrap_or_else(|| namespace_birth_seq(head))
}
