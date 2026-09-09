//! Loads a current manifest and discovers its numbered WAL tip.

use crate::control_object::{ControlObjectLoadError, LoadedControl};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control::{load_current_manifest, LoadedHeadObject, LoadedManifest};
use crate::namespace::state::NamespaceReadState;
use crate::wal::load_wal_segment;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(crate) struct NamespaceControlSnapshot {
    pub(crate) head: LoadedHeadObject,
    pub(crate) root: LoadedManifest,
    pub(crate) retention_floor_seq: ChangeSeq,
}

impl NamespaceControlSnapshot {
    pub(crate) fn basis(&self) -> MetadataBasis {
        MetadataBasis(self.root.state.manifest.clone())
    }
}

pub(crate) struct LoadedNamespaceBasis {
    pub(crate) head: LoadedHeadObject,
    pub(crate) basis: MetadataBasis,
    pub(crate) retention_floor_seq: Option<ChangeSeq>,
}

pub(crate) async fn load_head_and_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(LoadedHeadObject, ChangeSeq), ControlObjectLoadError> {
    let snapshot = load_control_snapshot(store, namespace_id).await?;
    Ok((snapshot.head, snapshot.retention_floor_seq))
}

pub(crate) async fn load_head_and_metadata_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<LoadedNamespaceBasis, ControlObjectLoadError> {
    let snapshot = load_control_snapshot(store, namespace_id).await?;
    Ok(LoadedNamespaceBasis {
        basis: snapshot.basis(),
        retention_floor_seq: Some(snapshot.retention_floor_seq),
        head: snapshot.head,
    })
}

pub(crate) async fn load_control_snapshot<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceControlSnapshot, ControlObjectLoadError> {
    let mut root = load_current_manifest(store, namespace_id).await?;
    loop {
        match discover_head(store, namespace_id, &root).await {
            Ok(state) => {
                if let Ok(next) = root.state.manifest.manifest_no.successor() {
                    let key =
                        loonfs_objectstore::keys::metadata_manifest_object(namespace_id, &next);
                    if store
                        .head(&key)
                        .await
                        .map_err(|error| ControlObjectLoadError::Store {
                            object_key: key.clone(),
                            message: error.public_message().into_owned(),
                            class: crate::error::StoreFailureClass::of(&error),
                        })?
                        .is_some()
                    {
                        root = load_current_manifest(store, namespace_id).await?;
                        continue;
                    }
                }
                return Ok(NamespaceControlSnapshot {
                    retention_floor_seq: root.state.retention_floor_seq,
                    head: LoadedControl {
                        object_key: loonfs_objectstore::keys::hint(namespace_id),
                        etag: root.hint_etag.clone(),
                        state,
                    },
                    root,
                });
            }
            Err(error @ ControlObjectLoadError::Codec { .. }) => {
                let current = load_current_manifest(store, namespace_id).await?;
                if current.state.manifest.manifest_no == root.state.manifest.manifest_no {
                    return Err(error);
                }
                root = current;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn discover_head<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    root: &LoadedManifest,
) -> Result<NamespaceReadState, ControlObjectLoadError> {
    let mut state = NamespaceReadState::from(root.envelope.payload());
    if state.status.is_deleted() {
        return Ok(state);
    }
    let start = root.hinted_wal_no.max(state.last_folded_wal_no);
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
            &root.object_key,
        )?;
        apply_segment(&mut state, &segment, &mut last_record, &root.object_key)?;
        previous_epoch = payload.writer_epoch;
    }
    while let Ok(next) = number.successor() {
        let Some(segment) = load_wal_segment(store, namespace_id, next)
            .await
            .map_err(|error| wal_error(&root.object_key, error))?
        else {
            break;
        };
        let payload = segment.payload();
        if previous_epoch > payload.writer_epoch {
            return Err(corrupt(&root.object_key, "WAL writer epoch decreases"));
        }
        validate_segment(namespace_id, state.seq, &segment, &root.object_key)?;
        apply_segment(&mut state, &segment, &mut last_record, &root.object_key)?;
        previous_epoch = payload.writer_epoch;
        number = next;
    }
    let mut prior = start;
    while last_record.is_none()
        && state.seq > root.envelope.payload().head_seq
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
    crate::wal::validate_wal_segment_for_replay(namespace_id, base_seq, segment)
        .map_err(|error| corrupt(object_key, error))
}

async fn load_required_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    wal_no: loonfs_api::WalNo,
) -> Result<loonfs_api::wire::wal::WalSegmentEnvelope, ControlObjectLoadError> {
    let key = loonfs_objectstore::keys::wal_segment(namespace_id, &wal_no);
    load_wal_segment(store, namespace_id, wal_no)
        .await
        .map_err(|error| wal_error(&key, error))?
        .ok_or_else(|| corrupt(&key, "hinted WAL object is missing"))
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

pub(super) fn wal_error(
    object_key: &str,
    error: crate::wal::WalChainLoadError,
) -> ControlObjectLoadError {
    match error {
        crate::wal::WalChainLoadError::ReadWal {
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

pub(crate) async fn resolve_retention_floor_seq<S: ObjectStore + ?Sized>(
    store: &S,
    head: &NamespaceReadState,
) -> Result<ChangeSeq, ControlObjectLoadError> {
    Ok(load_current_manifest(store, &head.namespace_id)
        .await?
        .state
        .retention_floor_seq)
}
