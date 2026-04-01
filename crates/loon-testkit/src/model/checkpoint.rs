use super::{
    ModelCheckpoint, ModelCheckpointFamily, ModelCheckpointPage, ModelCheckpointRow,
    ModelDirentryRecord, ModelError, ModelInodeRecord, ModelMetadataState, ModelRevisionRecord,
    ModelSubtreeTombstoneRecord,
};
use loon_types::{ChangeSeq, NamespaceId};
use std::collections::BTreeSet;

pub(crate) fn ensure_checkpoint_is_restorable(
    checkpoint: &ModelCheckpoint,
    available_segment_keys: &[String],
) -> Result<(), ModelError> {
    if !checkpoint.verified {
        return Err(ModelError::UnverifiedCheckpoint {
            checkpoint_seq: checkpoint.checkpoint_seq,
        });
    }

    let available: BTreeSet<&str> = available_segment_keys.iter().map(String::as_str).collect();
    for table in &checkpoint.tables {
        for segment in &table.segments {
            if !available.contains(segment.object_key.as_str()) {
                return Err(ModelError::MissingCheckpointSegment {
                    object_key: segment.object_key.clone(),
                });
            }
        }
    }

    Ok(())
}

pub(crate) fn metadata_state_from_checkpoint(
    checkpoint: &ModelCheckpoint,
) -> Result<ModelMetadataState, ModelError> {
    let mut metadata_state = ModelMetadataState::default();

    for table in &checkpoint.tables {
        for segment in &table.segments {
            for page in &segment.pages {
                for row in &page.rows {
                    match row {
                        ModelCheckpointRow::Inode {
                            inode_id,
                            inode_kind,
                            created_seq,
                        } => metadata_state.inodes.push(ModelInodeRecord {
                            inode_id: *inode_id,
                            inode_kind: inode_kind.clone(),
                            created_seq: *created_seq,
                        }),
                        ModelCheckpointRow::Direntry {
                            parent_inode_id,
                            name_key,
                            display_name,
                            child_inode_id,
                            bind_seq,
                            bind_op_index,
                        } => metadata_state.direntries.push(ModelDirentryRecord {
                            parent_inode_id: *parent_inode_id,
                            name_key: name_key.clone(),
                            display_name: display_name.clone(),
                            child_inode_id: *child_inode_id,
                            bind_seq: *bind_seq,
                            bind_op_index: *bind_op_index,
                        }),
                        ModelCheckpointRow::Revision {
                            inode_id,
                            revision_no,
                            committed_seq,
                            revision_op_index,
                            content_manifest_digest,
                        } => metadata_state.revisions.push(ModelRevisionRecord {
                            inode_id: *inode_id,
                            revision_no: *revision_no,
                            committed_seq: *committed_seq,
                            revision_op_index: *revision_op_index,
                            content_manifest_digest: content_manifest_digest.clone(),
                        }),
                        ModelCheckpointRow::Tombstone {
                            root_inode_id,
                            tombstone_seq,
                            tombstone_op_index,
                        } => metadata_state
                            .subtree_tombstones
                            .push(ModelSubtreeTombstoneRecord {
                                root_inode_id: *root_inode_id,
                                tombstone_seq: *tombstone_seq,
                                tombstone_op_index: *tombstone_op_index,
                            }),
                    }
                }
            }
        }
    }

    Ok(metadata_state)
}

pub(crate) fn build_model_checkpoint_page(
    family: ModelCheckpointFamily,
    metadata_state: &ModelMetadataState,
) -> ModelCheckpointPage {
    let rows = checkpoint_rows_for_family(family, metadata_state);
    let row_keys = rows
        .iter()
        .map(ModelCheckpointRow::row_key)
        .collect::<Vec<_>>();
    let min_key = row_keys.first().cloned().unwrap_or_default();
    let max_key = row_keys.last().cloned().unwrap_or_default();

    ModelCheckpointPage {
        page_index: 0,
        min_key,
        max_key,
        row_keys,
        rows,
    }
}

fn checkpoint_rows_for_family(
    family: ModelCheckpointFamily,
    metadata_state: &ModelMetadataState,
) -> Vec<ModelCheckpointRow> {
    match family {
        ModelCheckpointFamily::Inodes => {
            let mut rows = metadata_state
                .inodes
                .iter()
                .map(|inode| ModelCheckpointRow::Inode {
                    inode_id: inode.inode_id,
                    inode_kind: inode.inode_kind.clone(),
                    created_seq: inode.created_seq,
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(ModelCheckpointRow::row_key);
            rows
        }
        ModelCheckpointFamily::Direntries => {
            let mut rows = metadata_state
                .direntries
                .iter()
                .map(|direntry| ModelCheckpointRow::Direntry {
                    parent_inode_id: direntry.parent_inode_id,
                    name_key: direntry.name_key.clone(),
                    display_name: direntry.display_name.clone(),
                    child_inode_id: direntry.child_inode_id,
                    bind_seq: direntry.bind_seq,
                    bind_op_index: direntry.bind_op_index,
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(ModelCheckpointRow::row_key);
            rows
        }
        ModelCheckpointFamily::Revisions => {
            let mut rows = metadata_state
                .revisions
                .iter()
                .map(|revision| ModelCheckpointRow::Revision {
                    inode_id: revision.inode_id,
                    revision_no: revision.revision_no,
                    committed_seq: revision.committed_seq,
                    revision_op_index: revision.revision_op_index,
                    content_manifest_digest: revision.content_manifest_digest.clone(),
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(ModelCheckpointRow::row_key);
            rows
        }
        ModelCheckpointFamily::Tombstones => {
            let mut rows = metadata_state
                .subtree_tombstones
                .iter()
                .map(|tombstone| ModelCheckpointRow::Tombstone {
                    root_inode_id: tombstone.root_inode_id,
                    tombstone_seq: tombstone.tombstone_seq,
                    tombstone_op_index: tombstone.tombstone_op_index,
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(ModelCheckpointRow::row_key);
            rows
        }
    }
}

pub(crate) fn checkpoint_segment_object_key(
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
    family: ModelCheckpointFamily,
    segment_index: u32,
) -> String {
    format!(
        "namespaces/{}/snapshots/{:020}/tables/{}-{segment_index:05}.sst.zst",
        namespace_id.as_str(),
        checkpoint_seq.0,
        family.as_str()
    )
}

impl ModelCheckpointRow {
    fn row_key(&self) -> String {
        match self {
            Self::Inode { inode_id, .. } => format!("inode-{:020}", inode_id.0),
            Self::Direntry {
                parent_inode_id,
                name_key,
                bind_seq,
                bind_op_index,
                ..
            } => {
                if *bind_op_index == 0 {
                    format!(
                        "direntry-{:020}-{name_key}-{:020}",
                        parent_inode_id.0, bind_seq.0
                    )
                } else {
                    format!(
                        "direntry-{:020}-{name_key}-{:020}-{:010}",
                        parent_inode_id.0, bind_seq.0, bind_op_index
                    )
                }
            }
            Self::Revision {
                inode_id,
                revision_no,
                revision_op_index,
                ..
            } => {
                if *revision_op_index == 0 {
                    format!("revision-{:020}-{:020}", inode_id.0, revision_no.0)
                } else {
                    format!(
                        "revision-{:020}-{:020}-{:010}",
                        inode_id.0, revision_no.0, revision_op_index
                    )
                }
            }
            Self::Tombstone {
                root_inode_id,
                tombstone_seq,
                tombstone_op_index,
            } => {
                if *tombstone_op_index == 0 {
                    format!("tombstone-{:020}-{:020}", root_inode_id.0, tombstone_seq.0)
                } else {
                    format!(
                        "tombstone-{:020}-{:020}-{:010}",
                        root_inode_id.0, tombstone_seq.0, tombstone_op_index
                    )
                }
            }
        }
    }
}

impl ModelCheckpointFamily {
    fn as_str(self) -> &'static str {
        match self {
            Self::Inodes => "inodes",
            Self::Direntries => "direntries",
            Self::Revisions => "revisions",
            Self::Tombstones => "tombstones",
        }
    }
}
