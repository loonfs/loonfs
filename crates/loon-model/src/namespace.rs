use crate::checkpoint::{
    build_model_checkpoint_page, checkpoint_segment_object_key, ensure_checkpoint_is_restorable,
    metadata_state_from_checkpoint,
};
use crate::queue::{
    build_snapshot_dedupe_key, build_snapshot_repair_job_id, build_snapshot_work_class,
};
use crate::{
    ModelAction, ModelCheckpoint, ModelCheckpointFamily, ModelCheckpointPublishAuthorizers,
    ModelCheckpointSegment, ModelCheckpointTable, ModelCommitValidationError,
    ModelCommitValidationOutcome, ModelCommitValidationRequest, ModelError,
    ModelMetadataApplyError, ModelMetadataMutation, ModelMetadataState, ModelNamespace,
    ModelProgressObject, ModelQueueJob, ModelQueueJobState, ModelQueueRepairOutcome,
    ModelQueueSeqPayload, ModelQueueShard, ModelQueueWorkClass, ModelWalCommit,
};
use loon_types::{ChangeSeq, FenceToken, InodeId, LeaseState, NamespaceId};

impl ModelNamespace {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            head_seq: ChangeSeq(0),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(1),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
            metadata_state: ModelMetadataState::default(),
        }
    }

    pub fn apply(&mut self, action: ModelAction) -> Result<(), ModelError> {
        match action {
            ModelAction::CreateDir {
                inode_id,
                writer_fence_token,
            }
            | ModelAction::CreateFile {
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
            ops: Vec::new(),
        })
    }

    pub fn validate_commit_attempt(
        &self,
        request: &ModelCommitValidationRequest,
        lease: &LeaseState,
        now_ms: u64,
    ) -> Result<ModelCommitValidationOutcome, ModelCommitValidationError> {
        if request.namespace_id != self.namespace_id || request.namespace_id != lease.namespace_id {
            return Err(ModelCommitValidationError::NamespaceMismatch);
        }

        if self.namespace_id != lease.namespace_id {
            return Err(ModelCommitValidationError::HeadLeaseNamespaceMismatch);
        }

        if self.active_fence_token != lease.fence_token {
            return Err(ModelCommitValidationError::HeadLeaseFenceMismatch {
                head: self.active_fence_token,
                lease: lease.fence_token,
            });
        }

        if request.planned_head_seq != self.head_seq {
            return Err(ModelCommitValidationError::PlannedHeadSeqMismatch {
                expected: self.head_seq,
                actual: request.planned_head_seq,
            });
        }

        if request.writer_fence_token != self.active_fence_token {
            return Err(ModelCommitValidationError::StaleWriterFenceToken {
                expected: self.active_fence_token,
                actual: request.writer_fence_token,
            });
        }

        if request.writer_id != lease.holder_id {
            return Err(ModelCommitValidationError::LeaseHolderMismatch {
                expected: lease.holder_id.clone(),
                actual: request.writer_id.clone(),
            });
        }

        if !lease.is_valid_at(now_ms) {
            return Err(ModelCommitValidationError::LeaseExpired {
                lease_expires_at_ms: lease.lease_expires_at_ms,
                now_ms,
            });
        }

        let next_seq = self
            .head_seq
            .0
            .checked_add(1)
            .map(ChangeSeq)
            .ok_or(ModelCommitValidationError::SeqOverflow)?;

        Ok(ModelCommitValidationOutcome { next_seq })
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

        let applied_metadata = self
            .metadata_state
            .apply_committed_mutations(wal.seq, &wal.ops)
            .map_err(|err| match err {
                ModelMetadataApplyError::RevisionOverflow {
                    inode_id,
                    base_revision_no,
                } => ModelError::MetadataRevisionOverflow {
                    inode_id,
                    base_revision_no,
                },
                ModelMetadataApplyError::RestoreSourceRevisionMissing {
                    inode_id,
                    restore_from_revision_no,
                } => ModelError::MetadataRestoreSourceRevisionMissing {
                    inode_id,
                    restore_from_revision_no,
                },
            })?;

        self.head_seq = wal.seq;
        self.active_fence_token = wal.writer_fence_token;
        self.metadata_state = applied_metadata.metadata_state;
        let replay_next_inode_id =
            wal.ops
                .iter()
                .fold(self.next_inode_id, |current, op| match op {
                    ModelMetadataMutation::CreateDir { inode_id, .. }
                    | ModelMetadataMutation::CreateFile { inode_id, .. } => {
                        InodeId(current.0.max(inode_id.0.saturating_add(1)))
                    }
                    ModelMetadataMutation::ReplaceFile { .. }
                    | ModelMetadataMutation::Rename { .. }
                    | ModelMetadataMutation::RestoreRevision { .. }
                    | ModelMetadataMutation::DeleteSubtree { .. } => current,
                });
        self.next_inode_id = replay_next_inode_id;
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
                        row_count: self.metadata_state.inodes.len() as u64,
                        pages: vec![build_model_checkpoint_page(
                            ModelCheckpointFamily::Inodes,
                            &self.metadata_state,
                        )],
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
                        row_count: self.metadata_state.direntries.len() as u64,
                        pages: vec![build_model_checkpoint_page(
                            ModelCheckpointFamily::Direntries,
                            &self.metadata_state,
                        )],
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
                        row_count: self.metadata_state.revisions.len() as u64,
                        pages: vec![build_model_checkpoint_page(
                            ModelCheckpointFamily::Revisions,
                            &self.metadata_state,
                        )],
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
                        row_count: self.metadata_state.subtree_tombstones.len() as u64,
                        pages: vec![build_model_checkpoint_page(
                            ModelCheckpointFamily::Tombstones,
                            &self.metadata_state,
                        )],
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
            claim: None,
            attempts: 0,
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
            metadata_state: metadata_state_from_checkpoint(checkpoint)?,
        })
    }
}
