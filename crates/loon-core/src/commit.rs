use crate::invariants::INVARIANTS;
use crate::namespace::head_and_lease_fence_tokens_agree;
use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::keys::namespace_head;
use loon_objectstore::{ObjectMetadata, ObjectStore};
use loon_types::{
    ChangeSeq, ControlObjectKind, FenceToken, HeadState, HeadStateEnvelope, InodeId, LeaseState,
    NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub namespace_id: NamespaceId,
    pub request_id: String,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
    pub planned_head_seq: ChangeSeq,
    pub ops: Vec<CommitOp>,
    pub preconditions: Vec<Precondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOp {
    CreateDir {
        parent_inode: InodeId,
        display_name: String,
    },
    CreateFile {
        parent_inode: InodeId,
        display_name: String,
        content_manifest_digest: String,
    },
    ReplaceFile {
        inode_id: InodeId,
        base_revision: RevisionNo,
        content_manifest_digest: String,
    },
    Rename {
        inode_id: InodeId,
        new_parent_inode: InodeId,
        new_display_name: String,
    },
    DeleteSubtree {
        root_inode: InodeId,
    },
    RestoreRevision {
        inode_id: InodeId,
        restore_from_revision: RevisionNo,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precondition {
    HeadSeqIs(ChangeSeq),
    InodeRevisionIs {
        inode_id: InodeId,
        revision: RevisionNo,
    },
    AncestorsNotSubtreeDeleted {
        inode_id: InodeId,
    },
    ChildNameAbsent {
        parent_inode: InodeId,
        name_key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlan {
    pub namespace_id: NamespaceId,
    pub commit_id: String,
    pub base_head_seq: ChangeSeq,
    pub next_seq: ChangeSeq,
    pub allocated_inode_ids: Vec<InodeId>,
    pub resulting_next_inode_id: InodeId,
    pub durable_content_required: bool,
    pub wal_object_must_be_written: bool,
    pub head_cas_must_succeed: bool,
    pub metadata_preconditions: Vec<Precondition>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitValidationContext {
    pub head: HeadState,
    pub lease: LeaseState,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCommitHeadPublish {
    pub object_key: String,
    pub resulting_head: HeadState,
    pub envelope: HeadStateEnvelope,
    pub encoded_bytes: Vec<u8>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitValidationError {
    EmptyCommit,
    NamespaceMismatch,
    HeadLeaseNamespaceMismatch,
    HeadLeaseFenceMismatch {
        head: FenceToken,
        lease: FenceToken,
    },
    PlannedHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    MissingHeadSeqPrecondition {
        expected: ChangeSeq,
    },
    ConflictingHeadSeqPrecondition {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    StaleWriterFenceToken {
        active: FenceToken,
        requested: FenceToken,
    },
    LeaseHolderMismatch {
        expected: String,
        actual: String,
    },
    LeaseExpired {
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    SeqOverflow,
    NextInodeOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitHeadPublishError {
    EmptyWriterVersion,
    EmptyExpectedHeadEtag,
    NamespaceMismatch {
        head: NamespaceId,
        plan: NamespaceId,
    },
    PlanBaseHeadSeqMismatch {
        head: ChangeSeq,
        plan: ChangeSeq,
    },
    PlanNextSeqMismatch {
        head: ChangeSeq,
        plan: ChangeSeq,
    },
    Codec(String),
    Store(String),
}

pub fn build_commit_plan(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Result<CommitPlan, CommitValidationError> {
    if request.ops.is_empty() {
        return Err(CommitValidationError::EmptyCommit);
    }

    if request.namespace_id != context.head.namespace_id
        || request.namespace_id != context.lease.namespace_id
    {
        return Err(CommitValidationError::NamespaceMismatch);
    }

    if context.head.namespace_id != context.lease.namespace_id {
        return Err(CommitValidationError::HeadLeaseNamespaceMismatch);
    }

    if !head_and_lease_fence_tokens_agree(&context.head, &context.lease) {
        return Err(CommitValidationError::HeadLeaseFenceMismatch {
            head: context.head.active_fence_token,
            lease: context.lease.fence_token,
        });
    }

    if request.planned_head_seq != context.head.seq {
        return Err(CommitValidationError::PlannedHeadSeqMismatch {
            expected: context.head.seq,
            actual: request.planned_head_seq,
        });
    }

    validate_head_seq_preconditions(&request.preconditions, request.planned_head_seq)?;

    if request.writer_fence_token != context.head.active_fence_token {
        return Err(CommitValidationError::StaleWriterFenceToken {
            active: context.head.active_fence_token,
            requested: request.writer_fence_token,
        });
    }

    if request.writer_id != context.lease.holder_id {
        return Err(CommitValidationError::LeaseHolderMismatch {
            expected: context.lease.holder_id.clone(),
            actual: request.writer_id.clone(),
        });
    }

    if !context.lease.is_valid_at(context.now_ms) {
        return Err(CommitValidationError::LeaseExpired {
            lease_expires_at_ms: context.lease.lease_expires_at_ms,
            now_ms: context.now_ms,
        });
    }

    let next_seq = context
        .head
        .seq
        .0
        .checked_add(1)
        .map(ChangeSeq)
        .ok_or(CommitValidationError::SeqOverflow)?;
    let create_op_count = request
        .ops
        .iter()
        .filter(|op| matches!(op, CommitOp::CreateDir { .. } | CommitOp::CreateFile { .. }))
        .count();
    let allocated_inode_ids = (0..create_op_count)
        .map(|offset| {
            let offset =
                u64::try_from(offset).map_err(|_| CommitValidationError::NextInodeOverflow)?;
            context
                .head
                .next_inode_id
                .0
                .checked_add(offset)
                .map(InodeId)
                .ok_or(CommitValidationError::NextInodeOverflow)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let resulting_next_inode_id = context
        .head
        .next_inode_id
        .0
        .checked_add(
            u64::try_from(create_op_count).map_err(|_| CommitValidationError::NextInodeOverflow)?,
        )
        .map(InodeId)
        .ok_or(CommitValidationError::NextInodeOverflow)?;

    let durable_content_required = request.ops.iter().any(|op| {
        matches!(
            op,
            CommitOp::CreateFile { .. } | CommitOp::ReplaceFile { .. }
        )
    });
    let mut checked_invariants = INVARIANTS
        .iter()
        .copied()
        .filter(|name| {
            matches!(
                *name,
                "stale_writer_cannot_publish"
                    | "head_and_lease_fence_tokens_agree"
                    | "next_inode_id_is_monotonic"
            )
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if create_op_count > 0 {
        push_unique_invariant(
            &mut checked_invariants,
            "create_mutation_consumes_next_inode_id",
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::CreateFile { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            "create_file_requires_durable_content",
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::ReplaceFile { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            "replace_file_requires_durable_content",
        );
    }

    Ok(CommitPlan {
        namespace_id: request.namespace_id.clone(),
        commit_id: request.request_id.clone(),
        base_head_seq: request.planned_head_seq,
        next_seq,
        allocated_inode_ids,
        resulting_next_inode_id,
        durable_content_required,
        wal_object_must_be_written: true,
        head_cas_must_succeed: true,
        metadata_preconditions: request.preconditions.clone(),
        checked_invariants,
    })
}

pub fn prepare_commit_head_publish(
    current_head: &HeadState,
    plan: &CommitPlan,
    writer_version: &str,
) -> Result<PreparedCommitHeadPublish, CommitHeadPublishError> {
    if writer_version.trim().is_empty() {
        return Err(CommitHeadPublishError::EmptyWriterVersion);
    }

    if current_head.namespace_id != plan.namespace_id {
        return Err(CommitHeadPublishError::NamespaceMismatch {
            head: current_head.namespace_id.clone(),
            plan: plan.namespace_id.clone(),
        });
    }

    if current_head.seq != plan.base_head_seq {
        return Err(CommitHeadPublishError::PlanBaseHeadSeqMismatch {
            head: current_head.seq,
            plan: plan.base_head_seq,
        });
    }

    if current_head.seq.0.checked_add(1).map(ChangeSeq) != Some(plan.next_seq) {
        return Err(CommitHeadPublishError::PlanNextSeqMismatch {
            head: current_head.seq,
            plan: plan.next_seq,
        });
    }

    let resulting_head = HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: plan.next_seq,
        active_fence_token: current_head.active_fence_token,
        next_inode_id: plan.resulting_next_inode_id,
        snapshot_hint_seq: current_head.snapshot_hint_seq,
        retention_floor_seq: current_head.retention_floor_seq,
    };
    let envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        writer_version,
        resulting_head.clone(),
    )
    .map_err(|err| CommitHeadPublishError::Codec(err.to_string()))?;
    let encoded_bytes = serde_json::to_vec(&envelope)
        .map_err(|err| CommitHeadPublishError::Codec(err.to_string()))?;

    let mut checked_invariants = plan.checked_invariants.clone();
    push_unique_invariant(&mut checked_invariants, "head_publish_requires_durable_wal");

    Ok(PreparedCommitHeadPublish {
        object_key: namespace_head(current_head.namespace_id.as_str()),
        resulting_head,
        envelope,
        encoded_bytes,
        checked_invariants,
    })
}

pub fn publish_commit_head<S: ObjectStore>(
    store: &S,
    expected_head_etag: &str,
    prepared: &PreparedCommitHeadPublish,
) -> Result<ObjectMetadata, CommitHeadPublishError> {
    if expected_head_etag.trim().is_empty() {
        return Err(CommitHeadPublishError::EmptyExpectedHeadEtag);
    }

    store
        .compare_and_swap(
            &prepared.object_key,
            expected_head_etag,
            &prepared.encoded_bytes,
        )
        .map_err(map_object_store_error)
}

fn validate_head_seq_preconditions(
    preconditions: &[Precondition],
    planned_head_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let mut saw_head_seq_precondition = false;

    for precondition in preconditions {
        if let Precondition::HeadSeqIs(actual) = precondition {
            saw_head_seq_precondition = true;
            if *actual != planned_head_seq {
                return Err(CommitValidationError::ConflictingHeadSeqPrecondition {
                    expected: planned_head_seq,
                    actual: *actual,
                });
            }
        }
    }

    if !saw_head_seq_precondition {
        return Err(CommitValidationError::MissingHeadSeqPrecondition {
            expected: planned_head_seq,
        });
    }

    Ok(())
}

fn push_unique_invariant(invariants: &mut Vec<String>, name: &str) {
    if !invariants.iter().any(|existing| existing == name) {
        invariants.push(name.to_owned());
    }
}

fn map_object_store_error(err: ObjectStoreError) -> CommitHeadPublishError {
    CommitHeadPublishError::Store(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        build_commit_plan, prepare_commit_head_publish, publish_commit_head, CommitOp,
        CommitRequest, CommitValidationContext, CommitValidationError, Precondition,
    };
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::namespace_head;
    use loon_objectstore::ObjectStore;
    use loon_types::{
        ChangeSeq, ControlObjectKind, FenceToken, HeadState, HeadStateEnvelope, InodeId,
        LeaseState, NamespaceId, RevisionNo,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn build_commit_plan_accepts_active_writer() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-1".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::Rename {
                inode_id: InodeId(42),
                new_parent_inode: InodeId(2),
                new_display_name: "renamed.txt".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let plan = build_commit_plan(&request, &validation_context(999)).expect("valid plan");

        assert_eq!(plan.next_seq, ChangeSeq(42));
        assert!(!plan.durable_content_required);
        assert_eq!(plan.resulting_next_inode_id, InodeId(501));
        assert!(plan
            .checked_invariants
            .contains(&"stale_writer_cannot_publish".to_owned()));
    }

    #[test]
    fn build_commit_plan_requires_durable_content_for_replace_file() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-2".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(42),
                base_revision: RevisionNo(7),
                content_manifest_digest: "sha256:manifest".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let plan = build_commit_plan(&request, &validation_context(999)).expect("valid plan");

        assert!(plan.durable_content_required);
    }

    #[test]
    fn build_commit_plan_allocates_inode_for_create_mutation() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-dir".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(2),
                display_name: "drafts".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(2),
                    name_key: "drafts".to_owned(),
                },
            ],
        };

        let plan = build_commit_plan(&request, &validation_context(999)).expect("valid plan");

        assert_eq!(plan.allocated_inode_ids, vec![InodeId(501)]);
        assert_eq!(plan.resulting_next_inode_id, InodeId(502));
        assert!(plan
            .checked_invariants
            .contains(&"create_mutation_consumes_next_inode_id".to_owned()));
    }

    #[test]
    fn build_commit_plan_requires_durable_content_for_create_file() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-file".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(902),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:child-note".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let plan = build_commit_plan(&request, &validation_context(999)).expect("valid plan");

        assert!(plan.durable_content_required);
        assert!(plan
            .checked_invariants
            .contains(&"create_file_requires_durable_content".to_owned()));
    }

    #[test]
    fn build_commit_plan_rejects_stale_writer_token() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-3".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(7),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(2),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let error = build_commit_plan(&request, &validation_context(1_000))
            .expect_err("stale writer should be rejected");

        assert_eq!(
            error,
            CommitValidationError::StaleWriterFenceToken {
                active: FenceToken(8),
                requested: FenceToken(7),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_expired_lease() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-4".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(2),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let error = build_commit_plan(&request, &validation_context(1_001))
            .expect_err("expired lease should be rejected");

        assert_eq!(
            error,
            CommitValidationError::LeaseExpired {
                lease_expires_at_ms: 1_000,
                now_ms: 1_001,
            }
        );
    }

    #[test]
    fn build_commit_plan_requires_explicit_head_seq_precondition() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-5".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(2),
            }],
            preconditions: Vec::new(),
        };

        let error = build_commit_plan(&request, &validation_context(1_000))
            .expect_err("missing head precondition should be rejected");

        assert_eq!(
            error,
            CommitValidationError::MissingHeadSeqPrecondition {
                expected: ChangeSeq(41),
            }
        );
    }

    #[test]
    fn prepare_commit_head_publish_advances_seq_and_next_inode_id() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-dir".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(2),
                display_name: "drafts".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };
        let context = validation_context(999);
        let plan = build_commit_plan(&request, &context).expect("valid plan");

        let prepared =
            prepare_commit_head_publish(&context.head, &plan, "loon-core-test").expect("prepare");

        assert_eq!(prepared.object_key, namespace_head("ns-1"));
        assert_eq!(prepared.resulting_head.seq, ChangeSeq(42));
        assert_eq!(prepared.resulting_head.next_inode_id, InodeId(502));
        assert!(prepared
            .checked_invariants
            .contains(&"head_publish_requires_durable_wal".to_owned()));
    }

    #[test]
    fn publish_commit_head_compare_and_swap_writes_new_head() {
        let temp_dir = TestDir::new("commit-head-publish");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
        let initial_envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
            "loon-core-test",
            validation_context(999).head.clone(),
        )
        .expect("build initial head envelope");
        let head_key = namespace_head("ns-1");
        let initial_bytes =
            serde_json::to_vec(&initial_envelope).expect("encode initial head envelope");
        let initial_metadata = store
            .put_if_absent(&head_key, &initial_bytes)
            .expect("seed current head");
        let etag = initial_metadata.etag.expect("seeded head should have etag");
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-dir".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(2),
                display_name: "drafts".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };
        let context = validation_context(999);
        let plan = build_commit_plan(&request, &context).expect("valid plan");
        let prepared =
            prepare_commit_head_publish(&context.head, &plan, "loon-core-test").expect("prepare");

        publish_commit_head(&store, &etag, &prepared).expect("publish head");

        let stored_bytes = store
            .get(&head_key, None)
            .expect("read published head")
            .expect("published head should exist");
        let stored: HeadStateEnvelope =
            serde_json::from_slice(&stored_bytes).expect("decode published head");

        assert_eq!(stored.state.seq, ChangeSeq(42));
        assert_eq!(stored.state.next_inode_id, InodeId(502));
    }

    fn validation_context(now_ms: u64) -> CommitValidationContext {
        CommitValidationContext {
            head: HeadState {
                namespace_id: NamespaceId::from("ns-1"),
                seq: ChangeSeq(41),
                active_fence_token: FenceToken(8),
                next_inode_id: InodeId(501),
                snapshot_hint_seq: Some(ChangeSeq(40)),
                retention_floor_seq: ChangeSeq(40),
            },
            lease: LeaseState {
                namespace_id: NamespaceId::from("ns-1"),
                holder_id: "writer-a".to_owned(),
                fence_token: FenceToken(8),
                lease_expires_at_ms: 1_000,
            },
            now_ms,
        }
    }

    #[derive(Debug)]
    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(label: &str) -> Self {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "loondb-core-{label}-{}-{stamp}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
