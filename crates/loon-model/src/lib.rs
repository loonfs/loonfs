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
pub struct ModelProgressObject {
    pub namespace_id: NamespaceId,
    pub work_class: String,
    pub through_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCheckpointPublishAuthorizers {
    pub required_progress: Vec<ModelProgressObject>,
    pub retention_policy: ModelProgressObject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelQueueWorkClass {
    BuildSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelQueueJobState {
    Ready,
    Claimed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQueueSeqPayload {
    pub namespace_id: NamespaceId,
    pub through_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQueueJob {
    pub job_id: String,
    pub dedupe_key: String,
    pub state: ModelQueueJobState,
    pub payload: ModelQueueSeqPayload,
    pub follow_up: Option<ModelQueueSeqPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQueueShard {
    pub work_class: ModelQueueWorkClass,
    pub shard_id: u32,
    pub jobs: Vec<ModelQueueJob>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelQueueRepairOutcome {
    NoRepairNeeded,
    Enqueued { through_seq: ChangeSeq },
    RaisedReadyJob { through_seq: ChangeSeq },
    AttachedFollowUp { through_seq: ChangeSeq },
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
    MissingRetentionAuthorizers {
        requested: ChangeSeq,
    },
    ProgressNamespaceMismatch {
        work_class: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    ProgressWorkClassMismatch {
        expected: String,
        actual: String,
    },
    RequiredProgressLag {
        work_class: String,
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    RetentionPolicyLag {
        work_class: String,
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    QueueWorkClassMismatch {
        expected: ModelQueueWorkClass,
        actual: ModelQueueWorkClass,
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

    pub fn publish_progress(
        &self,
        current: Option<&ModelProgressObject>,
        work_class: &str,
        requested_through_seq: ChangeSeq,
    ) -> Result<ModelProgressObject, ModelError> {
        if let Some(current) = current {
            if current.namespace_id != self.namespace_id {
                return Err(ModelError::ProgressNamespaceMismatch {
                    work_class: current.work_class.clone(),
                    expected: self.namespace_id.clone(),
                    actual: current.namespace_id.clone(),
                });
            }

            if current.work_class != work_class {
                return Err(ModelError::ProgressWorkClassMismatch {
                    expected: work_class.to_owned(),
                    actual: current.work_class.clone(),
                });
            }

            if current.through_seq >= requested_through_seq {
                return Ok(current.clone());
            }
        }

        Ok(ModelProgressObject {
            namespace_id: self.namespace_id.clone(),
            work_class: work_class.to_owned(),
            through_seq: requested_through_seq,
        })
    }

    pub fn repair_lost_snapshot_enqueue(
        &self,
        queue: &mut ModelQueueShard,
        progress: Option<&ModelProgressObject>,
    ) -> Result<ModelQueueRepairOutcome, ModelError> {
        if queue.work_class != ModelQueueWorkClass::BuildSnapshot {
            return Err(ModelError::QueueWorkClassMismatch {
                expected: ModelQueueWorkClass::BuildSnapshot,
                actual: queue.work_class,
            });
        }

        if self.head_seq == ChangeSeq(0) {
            return Ok(ModelQueueRepairOutcome::NoRepairNeeded);
        }

        if let Some(progress) = progress {
            if progress.namespace_id != self.namespace_id {
                return Err(ModelError::ProgressNamespaceMismatch {
                    work_class: progress.work_class.clone(),
                    expected: self.namespace_id.clone(),
                    actual: progress.namespace_id.clone(),
                });
            }

            if progress.work_class != build_snapshot_work_class() {
                return Err(ModelError::ProgressWorkClassMismatch {
                    expected: build_snapshot_work_class().to_owned(),
                    actual: progress.work_class.clone(),
                });
            }

            if progress.through_seq >= self.head_seq {
                return Ok(ModelQueueRepairOutcome::NoRepairNeeded);
            }
        }

        let desired_payload = ModelQueueSeqPayload {
            namespace_id: self.namespace_id.clone(),
            through_seq: self.head_seq,
        };
        let dedupe_key = build_snapshot_dedupe_key(&self.namespace_id);

        if let Some(job) = queue
            .jobs
            .iter_mut()
            .find(|job| job.dedupe_key == dedupe_key)
        {
            match job.state {
                ModelQueueJobState::Ready => {
                    if desired_payload.through_seq > job.payload.through_seq {
                        job.payload.through_seq = desired_payload.through_seq;
                        return Ok(ModelQueueRepairOutcome::RaisedReadyJob {
                            through_seq: job.payload.through_seq,
                        });
                    }
                }
                ModelQueueJobState::Claimed => match &mut job.follow_up {
                    Some(existing) => {
                        if desired_payload.through_seq > existing.through_seq {
                            existing.through_seq = desired_payload.through_seq;
                            return Ok(ModelQueueRepairOutcome::AttachedFollowUp {
                                through_seq: existing.through_seq,
                            });
                        }
                    }
                    None => {
                        job.follow_up = Some(desired_payload.clone());
                        return Ok(ModelQueueRepairOutcome::AttachedFollowUp {
                            through_seq: desired_payload.through_seq,
                        });
                    }
                },
            }

            return Ok(ModelQueueRepairOutcome::NoRepairNeeded);
        }

        queue.jobs.push(ModelQueueJob {
            job_id: build_snapshot_repair_job_id(&self.namespace_id),
            dedupe_key,
            state: ModelQueueJobState::Ready,
            payload: desired_payload.clone(),
            follow_up: None,
        });

        Ok(ModelQueueRepairOutcome::Enqueued {
            through_seq: desired_payload.through_seq,
        })
    }

    pub fn publish_checkpoint(
        &mut self,
        checkpoint: &ModelCheckpoint,
        available_segment_keys: &[String],
        requested_retention_floor_seq: Option<ChangeSeq>,
        authorizers: Option<&ModelCheckpointPublishAuthorizers>,
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

            let authorizers =
                authorizers.ok_or(ModelError::MissingRetentionAuthorizers { requested })?;

            for progress in &authorizers.required_progress {
                if progress.namespace_id != self.namespace_id {
                    return Err(ModelError::ProgressNamespaceMismatch {
                        work_class: progress.work_class.clone(),
                        expected: self.namespace_id.clone(),
                        actual: progress.namespace_id.clone(),
                    });
                }

                if progress.through_seq < requested {
                    return Err(ModelError::RequiredProgressLag {
                        work_class: progress.work_class.clone(),
                        requested,
                        available: progress.through_seq,
                    });
                }
            }

            if authorizers.retention_policy.namespace_id != self.namespace_id {
                return Err(ModelError::ProgressNamespaceMismatch {
                    work_class: authorizers.retention_policy.work_class.clone(),
                    expected: self.namespace_id.clone(),
                    actual: authorizers.retention_policy.namespace_id.clone(),
                });
            }

            if authorizers.retention_policy.through_seq < requested {
                return Err(ModelError::RetentionPolicyLag {
                    work_class: authorizers.retention_policy.work_class.clone(),
                    requested,
                    available: authorizers.retention_policy.through_seq,
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

fn build_snapshot_work_class() -> &'static str {
    "BuildSnapshot"
}

fn build_snapshot_dedupe_key(namespace_id: &NamespaceId) -> String {
    format!("{}:{namespace_id}", build_snapshot_work_class())
}

fn build_snapshot_repair_job_id(namespace_id: &NamespaceId) -> String {
    format!("repair-{}-{namespace_id}", build_snapshot_work_class())
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
            Some(&sample_publish_authorizers(ChangeSeq(1))),
        )
        .expect("checkpoint publication should succeed");

        assert_eq!(ns.head_seq, ChangeSeq(1));
        assert_eq!(ns.active_fence_token, FenceToken(9));
        assert_eq!(ns.next_inode_id, InodeId(42));
        assert_eq!(ns.snapshot_hint_seq, Some(ChangeSeq(1)));
        assert_eq!(ns.retention_floor_seq, ChangeSeq(1));
    }

    #[test]
    fn model_progress_publish_is_monotonic() {
        let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let current = ModelProgressObject {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildSnapshot".to_owned(),
            through_seq: ChangeSeq(42),
        };

        let published = ns
            .publish_progress(Some(&current), "BuildSnapshot", ChangeSeq(41))
            .expect("stale progress publish should no-op");

        assert_eq!(published, current);
    }

    #[test]
    fn model_repair_enqueues_snapshot_job_when_progress_lags() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        let progress = ModelProgressObject {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildSnapshot".to_owned(),
            through_seq: ChangeSeq(0),
        };
        let mut queue = ModelQueueShard {
            work_class: ModelQueueWorkClass::BuildSnapshot,
            shard_id: 17,
            jobs: vec![],
        };

        let outcome = ns
            .repair_lost_snapshot_enqueue(&mut queue, Some(&progress))
            .expect("repair should enqueue missing snapshot job");

        assert_eq!(
            outcome,
            ModelQueueRepairOutcome::Enqueued {
                through_seq: ChangeSeq(1),
            }
        );
        assert_eq!(queue.jobs.len(), 1);
        assert_eq!(queue.jobs[0].dedupe_key, "BuildSnapshot:ns-1");
        assert_eq!(queue.jobs[0].payload.through_seq, ChangeSeq(1));
    }

    #[test]
    fn model_repair_attaches_follow_up_for_claimed_snapshot_job() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq again");
        let progress = ModelProgressObject {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildSnapshot".to_owned(),
            through_seq: ChangeSeq(0),
        };
        let mut queue = ModelQueueShard {
            work_class: ModelQueueWorkClass::BuildSnapshot,
            shard_id: 17,
            jobs: vec![ModelQueueJob {
                job_id: "job-1".to_owned(),
                dedupe_key: "BuildSnapshot:ns-1".to_owned(),
                state: ModelQueueJobState::Claimed,
                payload: ModelQueueSeqPayload {
                    namespace_id: NamespaceId::from("ns-1"),
                    through_seq: ChangeSeq(1),
                },
                follow_up: None,
            }],
        };

        let outcome = ns
            .repair_lost_snapshot_enqueue(&mut queue, Some(&progress))
            .expect("repair should attach follow-up to claimed job");

        assert_eq!(
            outcome,
            ModelQueueRepairOutcome::AttachedFollowUp {
                through_seq: ChangeSeq(2),
            }
        );
        assert_eq!(
            queue.jobs[0].follow_up,
            Some(ModelQueueSeqPayload {
                namespace_id: NamespaceId::from("ns-1"),
                through_seq: ChangeSeq(2),
            })
        );
    }

    #[test]
    fn model_rejects_retention_floor_without_authorizers() {
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
            )
            .expect_err("missing authorizers should fail");

        assert_eq!(
            error,
            ModelError::MissingRetentionAuthorizers {
                requested: ChangeSeq(1),
            }
        );
    }

    #[test]
    fn model_rejects_retention_floor_when_required_progress_lags() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        let checkpoint = ns.checkpoint();
        let authorizers = sample_publish_authorizers(ChangeSeq(0));

        let error = ns
            .publish_checkpoint(
                &checkpoint,
                &available_segment_keys(&checkpoint),
                Some(ChangeSeq(1)),
                Some(&authorizers),
            )
            .expect_err("lagging required progress should fail");

        assert_eq!(
            error,
            ModelError::RequiredProgressLag {
                work_class: "BuildListingIndex".to_owned(),
                requested: ChangeSeq(1),
                available: ChangeSeq(0),
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
                Some(&sample_publish_authorizers(ChangeSeq(2))),
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

    fn sample_publish_authorizers(through_seq: ChangeSeq) -> ModelCheckpointPublishAuthorizers {
        ModelCheckpointPublishAuthorizers {
            required_progress: vec![ModelProgressObject {
                namespace_id: NamespaceId::from("ns-1"),
                work_class: "BuildListingIndex".to_owned(),
                through_seq,
            }],
            retention_policy: ModelProgressObject {
                namespace_id: NamespaceId::from("ns-1"),
                work_class: "RetentionPolicy".to_owned(),
                through_seq,
            },
        }
    }
}
