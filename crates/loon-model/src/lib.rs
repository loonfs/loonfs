#![forbid(unsafe_code)]

use loon_types::{ChangeSeq, FenceToken, InodeId, NamespaceId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelNamespace {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub active_fence_token: FenceToken,
    pub next_inode_id: InodeId,
    pub snapshot_hint_seq: Option<ChangeSeq>,
    pub retention_floor_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelWalCommit {
    pub namespace_id: NamespaceId,
    pub seq: ChangeSeq,
    pub base_head_seq: ChangeSeq,
    pub commit_id: String,
    pub writer_fence_token: FenceToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCheckpoint {
    pub namespace_id: NamespaceId,
    pub checkpoint_seq: ChangeSeq,
    pub active_fence_token: FenceToken,
    pub next_inode_id: InodeId,
    pub retention_floor_seq: ChangeSeq,
    pub verified: bool,
    pub tables: Vec<ModelCheckpointTable>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCheckpointFamily {
    Inodes,
    Direntries,
    Revisions,
    Tombstones,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCheckpointSegment {
    pub object_key: String,
    pub segment_index: u32,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCheckpointTable {
    pub family: ModelCheckpointFamily,
    pub segments: Vec<ModelCheckpointSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelAction {
    CreateDir {
        inode_id: InodeId,
        writer_fence_token: FenceToken,
    },
    DeleteSubtree {
        root_inode: InodeId,
        writer_fence_token: FenceToken,
    },
    BumpSeq {
        writer_fence_token: FenceToken,
    },
    RotateFence {
        new_fence_token: FenceToken,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelError {
    StaleWriterFenceToken {
        expected: FenceToken,
        actual: FenceToken,
    },
    NamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    BaseHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    NonContiguousSeq {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    UnverifiedCheckpoint {
        checkpoint_seq: ChangeSeq,
    },
    MissingCheckpointSegment {
        object_key: String,
    },
    CheckpointAheadOfHead {
        checkpoint_seq: ChangeSeq,
        head_seq: ChangeSeq,
    },
    RetentionFloorRegression {
        current: ChangeSeq,
        requested: ChangeSeq,
    },
    RetentionFloorBeyondCheckpoint {
        checkpoint_seq: ChangeSeq,
        requested: ChangeSeq,
    },
    MissingDerivedProgressFloor {
        requested: ChangeSeq,
    },
    DerivedProgressLag {
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    MissingRetentionPolicyFloor {
        requested: ChangeSeq,
    },
    RetentionPolicyLag {
        requested: ChangeSeq,
        allowed: ChangeSeq,
    },
}

impl ModelNamespace {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            head_seq: ChangeSeq(0),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(1),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        }
    }

    pub fn apply(&mut self, action: ModelAction) -> Result<(), ModelError> {
        match action {
            ModelAction::CreateDir {
                inode_id,
                writer_fence_token,
            } => {
                if writer_fence_token != self.active_fence_token {
                    return Err(ModelError::StaleWriterFenceToken {
                        expected: self.active_fence_token,
                        actual: writer_fence_token,
                    });
                }
                self.head_seq = ChangeSeq(self.head_seq.0 + 1);
                self.next_inode_id =
                    InodeId(self.next_inode_id.0.max(inode_id.0.saturating_add(1)));
                Ok(())
            }
            ModelAction::DeleteSubtree {
                writer_fence_token, ..
            }
            | ModelAction::BumpSeq { writer_fence_token } => {
                if writer_fence_token != self.active_fence_token {
                    return Err(ModelError::StaleWriterFenceToken {
                        expected: self.active_fence_token,
                        actual: writer_fence_token,
                    });
                }
                self.head_seq = ChangeSeq(self.head_seq.0 + 1);
                Ok(())
            }
            ModelAction::RotateFence { new_fence_token } => {
                self.active_fence_token = new_fence_token;
                Ok(())
            }
        }
    }

    pub fn prepare_wal_commit(
        &self,
        commit_id: impl Into<String>,
        writer_fence_token: FenceToken,
    ) -> Result<ModelWalCommit, ModelError> {
        if writer_fence_token != self.active_fence_token {
            return Err(ModelError::StaleWriterFenceToken {
                expected: self.active_fence_token,
                actual: writer_fence_token,
            });
        }

        Ok(ModelWalCommit {
            namespace_id: self.namespace_id.clone(),
            seq: ChangeSeq(self.head_seq.0 + 1),
            base_head_seq: self.head_seq,
            commit_id: commit_id.into(),
            writer_fence_token,
        })
    }

    pub fn replay_wal_commit(&mut self, wal: &ModelWalCommit) -> Result<(), ModelError> {
        if wal.namespace_id != self.namespace_id {
            return Err(ModelError::NamespaceMismatch {
                expected: self.namespace_id.clone(),
                actual: wal.namespace_id.clone(),
            });
        }

        if wal.base_head_seq != self.head_seq {
            return Err(ModelError::BaseHeadSeqMismatch {
                expected: self.head_seq,
                actual: wal.base_head_seq,
            });
        }

        let expected_next_seq = ChangeSeq(self.head_seq.0 + 1);
        if wal.seq != expected_next_seq {
            return Err(ModelError::NonContiguousSeq {
                expected: expected_next_seq,
                actual: wal.seq,
            });
        }

        self.head_seq = wal.seq;
        self.active_fence_token = wal.writer_fence_token;
        Ok(())
    }

    pub fn checkpoint(&self) -> ModelCheckpoint {
        ModelCheckpoint {
            namespace_id: self.namespace_id.clone(),
            checkpoint_seq: self.head_seq,
            active_fence_token: self.active_fence_token,
            next_inode_id: self.next_inode_id,
            retention_floor_seq: self.retention_floor_seq,
            verified: true,
            tables: vec![
                ModelCheckpointTable {
                    family: ModelCheckpointFamily::Inodes,
                    segments: vec![ModelCheckpointSegment {
                        object_key: checkpoint_segment_object_key(
                            &self.namespace_id,
                            self.head_seq,
                            ModelCheckpointFamily::Inodes,
                            0,
                        ),
                        segment_index: 0,
                        row_count: 0,
                    }],
                },
                ModelCheckpointTable {
                    family: ModelCheckpointFamily::Direntries,
                    segments: vec![ModelCheckpointSegment {
                        object_key: checkpoint_segment_object_key(
                            &self.namespace_id,
                            self.head_seq,
                            ModelCheckpointFamily::Direntries,
                            0,
                        ),
                        segment_index: 0,
                        row_count: 0,
                    }],
                },
                ModelCheckpointTable {
                    family: ModelCheckpointFamily::Revisions,
                    segments: vec![ModelCheckpointSegment {
                        object_key: checkpoint_segment_object_key(
                            &self.namespace_id,
                            self.head_seq,
                            ModelCheckpointFamily::Revisions,
                            0,
                        ),
                        segment_index: 0,
                        row_count: 0,
                    }],
                },
                ModelCheckpointTable {
                    family: ModelCheckpointFamily::Tombstones,
                    segments: vec![ModelCheckpointSegment {
                        object_key: checkpoint_segment_object_key(
                            &self.namespace_id,
                            self.head_seq,
                            ModelCheckpointFamily::Tombstones,
                            0,
                        ),
                        segment_index: 0,
                        row_count: 0,
                    }],
                },
            ],
        }
    }

    pub fn publish_checkpoint(
        &mut self,
        checkpoint: &ModelCheckpoint,
        available_segment_keys: &[String],
        requested_retention_floor_seq: Option<ChangeSeq>,
        derived_progress_floor_seq: Option<ChangeSeq>,
        retention_policy_floor_seq: Option<ChangeSeq>,
    ) -> Result<(), ModelError> {
        ensure_checkpoint_is_restorable(checkpoint, available_segment_keys)?;

        if checkpoint.checkpoint_seq > self.head_seq {
            return Err(ModelError::CheckpointAheadOfHead {
                checkpoint_seq: checkpoint.checkpoint_seq,
                head_seq: self.head_seq,
            });
        }

        self.snapshot_hint_seq = Some(
            self.snapshot_hint_seq
                .unwrap_or(checkpoint.checkpoint_seq)
                .max(checkpoint.checkpoint_seq),
        );

        if let Some(requested) = requested_retention_floor_seq {
            if requested < self.retention_floor_seq {
                return Err(ModelError::RetentionFloorRegression {
                    current: self.retention_floor_seq,
                    requested,
                });
            }

            if requested > checkpoint.checkpoint_seq {
                return Err(ModelError::RetentionFloorBeyondCheckpoint {
                    checkpoint_seq: checkpoint.checkpoint_seq,
                    requested,
                });
            }

            let derived_progress_floor_seq = derived_progress_floor_seq
                .ok_or(ModelError::MissingDerivedProgressFloor { requested })?;
            if derived_progress_floor_seq < requested {
                return Err(ModelError::DerivedProgressLag {
                    requested,
                    available: derived_progress_floor_seq,
                });
            }

            let retention_policy_floor_seq = retention_policy_floor_seq
                .ok_or(ModelError::MissingRetentionPolicyFloor { requested })?;
            if retention_policy_floor_seq < requested {
                return Err(ModelError::RetentionPolicyLag {
                    requested,
                    allowed: retention_policy_floor_seq,
                });
            }

            self.retention_floor_seq = requested;
        }

        Ok(())
    }

    pub fn restore_from_checkpoint(
        checkpoint: &ModelCheckpoint,
        available_segment_keys: &[String],
    ) -> Result<Self, ModelError> {
        ensure_checkpoint_is_restorable(checkpoint, available_segment_keys)?;

        Ok(Self {
            namespace_id: checkpoint.namespace_id.clone(),
            head_seq: checkpoint.checkpoint_seq,
            active_fence_token: checkpoint.active_fence_token,
            next_inode_id: checkpoint.next_inode_id,
            snapshot_hint_seq: Some(checkpoint.checkpoint_seq),
            retention_floor_seq: checkpoint.retention_floor_seq,
        })
    }
}

fn ensure_checkpoint_is_restorable(
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

fn checkpoint_segment_object_key(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_advances_seq() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        assert_eq!(ns.head_seq.0, 1);
    }

    #[test]
    fn model_create_dir_advances_next_inode_id() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::CreateDir {
            inode_id: InodeId(7),
            writer_fence_token: FenceToken(0),
        })
        .expect("create dir should advance next inode id");

        assert_eq!(ns.head_seq, ChangeSeq(1));
        assert_eq!(ns.next_inode_id, InodeId(8));
    }

    #[test]
    fn model_rejects_stale_writer_after_fence_rotation() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::RotateFence {
            new_fence_token: FenceToken(9),
        })
        .expect("fence rotation should succeed");

        let error = ns
            .apply(ModelAction::BumpSeq {
                writer_fence_token: FenceToken(8),
            })
            .expect_err("stale writer should be rejected");

        assert_eq!(
            error,
            ModelError::StaleWriterFenceToken {
                expected: FenceToken(9),
                actual: FenceToken(8),
            }
        );
    }

    #[test]
    fn model_prepares_next_wal_commit_seq_for_active_writer() {
        let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let wal = ns
            .prepare_wal_commit("req-20260311-0001", FenceToken(0))
            .expect("active writer should prepare WAL");

        assert_eq!(wal.seq, ChangeSeq(1));
        assert_eq!(wal.base_head_seq, ChangeSeq(0));
        assert_eq!(wal.commit_id, "req-20260311-0001");
    }

    #[test]
    fn model_replays_contiguous_wal_commit() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let wal = ns
            .prepare_wal_commit("req-20260311-0001", FenceToken(0))
            .expect("active writer should prepare WAL");

        ns.replay_wal_commit(&wal)
            .expect("contiguous WAL should replay");

        assert_eq!(ns.head_seq, ChangeSeq(1));
        assert_eq!(ns.active_fence_token, FenceToken(0));
    }

    #[test]
    fn model_rejects_non_contiguous_wal_commit() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let wal = ModelWalCommit {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(2),
            base_head_seq: ChangeSeq(0),
            commit_id: "req-20260311-0001".to_owned(),
            writer_fence_token: FenceToken(0),
        };

        let error = ns
            .replay_wal_commit(&wal)
            .expect_err("gap should be rejected");

        assert_eq!(
            error,
            ModelError::NonContiguousSeq {
                expected: ChangeSeq(1),
                actual: ChangeSeq(2),
            }
        );
    }

    #[test]
    fn model_restores_from_verified_checkpoint() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::RotateFence {
            new_fence_token: FenceToken(9),
        })
        .expect("fence rotation should succeed");
        ns.apply(ModelAction::CreateDir {
            inode_id: InodeId(41),
            writer_fence_token: FenceToken(9),
        })
        .expect("active writer should advance seq");

        let checkpoint = ns.checkpoint();
        let available_segment_keys = checkpoint
            .tables
            .iter()
            .flat_map(|table| {
                table
                    .segments
                    .iter()
                    .map(|segment| segment.object_key.clone())
            })
            .collect::<Vec<_>>();
        let restored =
            ModelNamespace::restore_from_checkpoint(&checkpoint, &available_segment_keys)
                .expect("checkpoint restore");

        assert_eq!(restored.namespace_id, NamespaceId::from("ns-1"));
        assert_eq!(restored.head_seq, ChangeSeq(1));
        assert_eq!(restored.active_fence_token, FenceToken(9));
        assert_eq!(restored.next_inode_id, InodeId(42));
        assert_eq!(restored.snapshot_hint_seq, Some(ChangeSeq(1)));
        assert_eq!(restored.retention_floor_seq, ChangeSeq(0));
    }

    #[test]
    fn model_publishes_verified_checkpoint_into_head_summary() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::RotateFence {
            new_fence_token: FenceToken(9),
        })
        .expect("fence rotation should succeed");
        ns.apply(ModelAction::CreateDir {
            inode_id: InodeId(41),
            writer_fence_token: FenceToken(9),
        })
        .expect("active writer should advance seq");

        let checkpoint = ns.checkpoint();
        ns.publish_checkpoint(
            &checkpoint,
            &available_segment_keys(&checkpoint),
            Some(ChangeSeq(1)),
            Some(ChangeSeq(1)),
            Some(ChangeSeq(1)),
        )
        .expect("checkpoint publication should succeed");

        assert_eq!(ns.head_seq, ChangeSeq(1));
        assert_eq!(ns.active_fence_token, FenceToken(9));
        assert_eq!(ns.next_inode_id, InodeId(42));
        assert_eq!(ns.snapshot_hint_seq, Some(ChangeSeq(1)));
        assert_eq!(ns.retention_floor_seq, ChangeSeq(1));
    }

    #[test]
    fn model_rejects_retention_floor_without_derived_progress() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        let checkpoint = ns.checkpoint();

        let error = ns
            .publish_checkpoint(
                &checkpoint,
                &available_segment_keys(&checkpoint),
                Some(ChangeSeq(1)),
                None,
                Some(ChangeSeq(1)),
            )
            .expect_err("missing derived progress should fail");

        assert_eq!(
            error,
            ModelError::MissingDerivedProgressFloor {
                requested: ChangeSeq(1),
            }
        );
    }

    #[test]
    fn model_rejects_retention_floor_above_checkpoint() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        let checkpoint = ns.checkpoint();

        let error = ns
            .publish_checkpoint(
                &checkpoint,
                &available_segment_keys(&checkpoint),
                Some(ChangeSeq(2)),
                Some(ChangeSeq(2)),
                Some(ChangeSeq(2)),
            )
            .expect_err("retention floor beyond checkpoint should fail");

        assert_eq!(
            error,
            ModelError::RetentionFloorBeyondCheckpoint {
                checkpoint_seq: ChangeSeq(1),
                requested: ChangeSeq(2),
            }
        );
    }

    #[test]
    fn model_checkpoint_includes_one_empty_segment_per_family() {
        let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let checkpoint = ns.checkpoint();

        assert_eq!(checkpoint.tables.len(), 4);
        assert!(checkpoint
            .tables
            .iter()
            .all(|table| table.segments.len() == 1));
        assert!(checkpoint
            .tables
            .iter()
            .all(|table| table.segments[0].segment_index == 0));
        assert!(checkpoint
            .tables
            .iter()
            .all(|table| table.segments[0].row_count == 0));
        assert!(checkpoint
            .tables
            .iter()
            .all(|table| table.segments[0].object_key.contains("/tables/")));
    }

    #[test]
    fn model_rejects_restore_when_checkpoint_segment_is_missing() {
        let checkpoint = ModelNamespace::new(NamespaceId::from("ns-1")).checkpoint();
        let error = ModelNamespace::restore_from_checkpoint(&checkpoint, &[])
            .expect_err("missing checkpoint segment should fail");

        assert_eq!(
            error,
            ModelError::MissingCheckpointSegment {
                object_key:
                    "namespaces/ns-1/snapshots/00000000000000000000/tables/inodes-00000.sst.zst"
                        .to_owned(),
            }
        );
    }

    #[test]
    fn model_rejects_unverified_checkpoint() {
        let checkpoint = ModelCheckpoint {
            namespace_id: NamespaceId::from("ns-1"),
            checkpoint_seq: ChangeSeq(40),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            retention_floor_seq: ChangeSeq(40),
            verified: false,
            tables: vec![],
        };

        let error = ModelNamespace::restore_from_checkpoint(&checkpoint, &[])
            .expect_err("unverified checkpoint should fail");

        assert_eq!(
            error,
            ModelError::UnverifiedCheckpoint {
                checkpoint_seq: ChangeSeq(40),
            }
        );
    }

    fn available_segment_keys(checkpoint: &ModelCheckpoint) -> Vec<String> {
        checkpoint
            .tables
            .iter()
            .flat_map(|table| {
                table
                    .segments
                    .iter()
                    .map(|segment| segment.object_key.clone())
            })
            .collect()
    }
}
