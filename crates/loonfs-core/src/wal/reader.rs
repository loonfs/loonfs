//! Reads numbered WAL objects: one at a time after a position, or a bounded tail between two heads.
// This module is the physical WAL boundary.
#![allow(clippy::disallowed_methods)]

use super::frame::ReplayedWalTail;
use super::replay::{project_validated_wal_tail, validate_wal_object_for_replay};
use super::{
    ValidatedWalObject, ValidatedWalTail, WalObjectError, WalTailLoadError, WalTailLoadRequest,
};
use crate::error::MetadataProjectionLoadError;
use crate::metadata::MetadataState;
use crate::namespace::state::NamespaceReadState;
use crate::store_waves::STORE_READ_WAVE;
use futures::{stream, StreamExt};
use loonfs_objectstore::keys::wal_object;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::format::wal::{decode_wal_object_envelope_zstd, WalObjectEnvelope};
use loonfs_types::{ChangeSeq, NamespaceId, WalNo, WriterEpoch};

// Missing and malformed objects still need their numbered key in caller diagnostics.
pub(super) struct LoadedWalObject {
    pub(super) object_key: String,
    pub(super) envelope: Result<Option<WalObjectEnvelope>, WalTailLoadError>,
}

pub(super) async fn load_wal_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    wal_no: WalNo,
) -> LoadedWalObject {
    let object_key = wal_object(namespace_id, &wal_no);
    let envelope = async {
        let Some(bytes) =
            store
                .get(&object_key, None)
                .await
                .map_err(|error| WalTailLoadError::ReadWal {
                    object_key: object_key.clone(),
                    message: error.public_message().into_owned(),
                    class: crate::error::StoreFailureClass::of(&error),
                })?
        else {
            return Ok(None);
        };
        let envelope =
            decode_wal_object_envelope_zstd(&bytes).map_err(|error| WalTailLoadError::Replay {
                object_key: object_key.clone(),
                error: WalObjectError::Codec(error.to_string()),
            })?;
        if envelope.payload().wal_no != wal_no {
            return Err(WalTailLoadError::NumberMismatch {
                object_key: object_key.clone(),
            });
        }
        if envelope.payload().namespace_id != *namespace_id {
            return Err(WalTailLoadError::Replay {
                object_key: object_key.clone(),
                error: WalObjectError::NamespaceMismatch {
                    expected: namespace_id.clone(),
                    actual: envelope.payload().namespace_id.clone(),
                },
            });
        }
        Ok(Some(envelope))
    }
    .await;
    LoadedWalObject {
        object_key,
        envelope,
    }
}

/// Checks for the WAL object at `wal_no` with a HEAD, without reading it.
pub(crate) async fn wal_object_exists<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    wal_no: WalNo,
) -> Result<bool, ObjectStoreError> {
    Ok(store
        .head(&wal_object(namespace_id, &wal_no))
        .await?
        .is_some())
}

/// Reads consecutive numbered objects after a position, each validated as
/// the contiguous successor of the one before.
pub(super) struct WalWalk<'a> {
    namespace_id: &'a NamespaceId,
    wal_no: WalNo,
    seq: ChangeSeq,
    epoch: WriterEpoch,
    epoch_bound: WriterEpoch,
}

impl<'a> WalWalk<'a> {
    /// Reads until the first absent number.
    pub(super) fn after(
        namespace_id: &'a NamespaceId,
        wal_no: WalNo,
        seq: ChangeSeq,
        epoch_bound: WriterEpoch,
    ) -> Self {
        Self {
            namespace_id,
            wal_no,
            seq,
            epoch: WriterEpoch(0),
            epoch_bound,
        }
    }

    pub(super) fn seq(&self) -> ChangeSeq {
        self.seq
    }

    pub(super) fn object_key(&self) -> String {
        wal_object(self.namespace_id, &self.wal_no)
    }

    pub(super) fn validate(
        &mut self,
        object_key: String,
        envelope: WalObjectEnvelope,
    ) -> Result<ValidatedWalObject, WalTailLoadError> {
        let payload = envelope.payload();
        if payload.writer_epoch < self.epoch || payload.writer_epoch > self.epoch_bound {
            return Err(WalTailLoadError::Replay {
                object_key,
                error: WalObjectError::WriterEpochMismatch {
                    expected_max: self.epoch_bound,
                    actual: payload.writer_epoch,
                },
            });
        }
        validate_wal_object_for_replay(self.seq, &envelope).map_err(|error| {
            WalTailLoadError::Replay {
                object_key: object_key.clone(),
                error,
            }
        })?;
        self.wal_no = payload.wal_no;
        self.seq = payload.head_seq;
        self.epoch = payload.writer_epoch;
        Ok(ValidatedWalObject::new(object_key, envelope))
    }
}

pub(super) async fn load_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    request: WalTailLoadRequest<'_>,
) -> Result<ValidatedWalTail, WalTailLoadError> {
    let mut walk = WalWalk::after(
        request.namespace_id,
        request.base_wal_no,
        request.base_seq,
        request.writer_epoch,
    );
    let mut loaded = stream::iter(request.base_wal_no.0..request.tip_wal_no.0)
        .map(|number| load_wal_object(store, request.namespace_id, WalNo(number + 1)))
        .buffered(STORE_READ_WAVE);
    let mut objects = Vec::new();
    while let Some(LoadedWalObject {
        object_key,
        envelope,
    }) = loaded.next().await
    {
        let envelope = envelope?.ok_or_else(|| WalTailLoadError::MissingWalObject {
            object_key: object_key.clone(),
        })?;
        objects.push(walk.validate(object_key, envelope)?);
    }
    if walk.seq() != request.head_seq {
        return Err(WalTailLoadError::HeadSeqMismatch {
            object_key: walk.object_key(),
            expected: request.head_seq,
            actual: walk.seq(),
        });
    }
    Ok(ValidatedWalTail::new(objects))
}

pub(crate) async fn load_replayed_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    base_head: &NamespaceReadState,
    current_head: &NamespaceReadState,
    base_metadata_state: &MetadataState,
) -> Result<ReplayedWalTail, MetadataProjectionLoadError> {
    let tail = load_wal_tail(
        store,
        WalTailLoadRequest {
            namespace_id: &current_head.namespace_id,
            base_seq: base_head.seq,
            head_seq: current_head.seq,
            base_wal_no: base_head.wal_no,
            tip_wal_no: current_head.wal_no,
            writer_epoch: current_head.writer_epoch,
        },
    )
    .await?;
    replay_discovered_tail(base_head, base_metadata_state, &tail)
}

pub(crate) fn replay_discovered_tail(
    base_head: &NamespaceReadState,
    base_metadata_state: &MetadataState,
    tail: &ValidatedWalTail,
) -> Result<ReplayedWalTail, MetadataProjectionLoadError> {
    let _span = tracing::debug_span!("loonfs.phase", phase = "project_metadata_state").entered();
    Ok(project_validated_wal_tail(
        base_head,
        &super::ProjectedWalTail::from_rows(base_metadata_state.clone()),
        tail,
    )?)
}
