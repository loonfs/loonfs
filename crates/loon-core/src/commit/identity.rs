use super::api_adapter::{commit_op_from_v0, commit_precondition_from_v0};
use super::{CommitOp, CommitRequest, Precondition};
use loon_api::{payload_checksum_sha256, v0 as api_v0, ChangeSeq, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const SEMANTIC_CORE_COMMIT_DOMAIN: &str = "loonfs.core.commit.semantic.v0";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SemanticCommitFingerprint(String);

impl SemanticCommitFingerprint {
    pub(crate) fn new_unchecked(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommitFingerprintError {
    #[error("commit fingerprint codec error: {0}")]
    Codec(String),
}

pub fn semantic_commit_fingerprint(
    request: &CommitRequest,
) -> Result<SemanticCommitFingerprint, CommitFingerprintError> {
    semantic_commit_fingerprint_from_parts(
        &request.namespace_id,
        request.planned_head_seq,
        &request.preconditions,
        &request.ops,
        &request.message,
        &request.annotations,
    )
}

pub fn semantic_commit_fingerprint_for_v0_request(
    namespace_id: &NamespaceId,
    request: &api_v0::CommitRequest,
) -> Result<SemanticCommitFingerprint, CommitFingerprintError> {
    let preconditions = request
        .preconditions
        .iter()
        .cloned()
        .map(commit_precondition_from_v0)
        .collect::<Vec<_>>();
    let ops = request
        .ops
        .iter()
        .cloned()
        .map(commit_op_from_v0)
        .collect::<Vec<_>>();
    semantic_commit_fingerprint_from_parts(
        namespace_id,
        request.planned_head_seq,
        &preconditions,
        &ops,
        &request.message,
        &request.annotations,
    )
}

fn semantic_commit_fingerprint_from_parts(
    namespace_id: &NamespaceId,
    planned_head_seq: ChangeSeq,
    preconditions: &[Precondition],
    ops: &[CommitOp],
    message: &Option<String>,
    annotations: &Option<api_v0::CommitAnnotations>,
) -> Result<SemanticCommitFingerprint, CommitFingerprintError> {
    #[derive(Serialize)]
    struct CanonicalSemanticCommit<'a> {
        domain: &'static str,
        namespace_id: &'a NamespaceId,
        planned_head_seq: ChangeSeq,
        preconditions: &'a [Precondition],
        ops: &'a [CommitOp],
        message: &'a Option<String>,
        annotations: &'a Option<api_v0::CommitAnnotations>,
    }

    payload_checksum_sha256(&CanonicalSemanticCommit {
        domain: SEMANTIC_CORE_COMMIT_DOMAIN,
        namespace_id,
        planned_head_seq,
        preconditions,
        ops,
        message,
        annotations,
    })
    .map(SemanticCommitFingerprint::new_unchecked)
    .map_err(|err| CommitFingerprintError::Codec(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitOp, CommitRequest, Precondition};
    use loon_api::{ChangeSeq, CommitId, FenceToken, InodeId, NamespaceId};

    fn core_request(writer_fence_token: FenceToken) -> CommitRequest {
        CommitRequest {
            namespace_id: NamespaceId::from("demo"),
            commit_id: CommitId::from("commit-a"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token,
            planned_head_seq: ChangeSeq(7),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(7))],
            message: Some("create docs".to_owned()),
            annotations: None,
        }
    }

    #[test]
    fn semantic_fingerprint_is_stable_for_same_logical_commit() {
        let left =
            semantic_commit_fingerprint(&core_request(FenceToken(1))).expect("left fingerprint");
        let right =
            semantic_commit_fingerprint(&core_request(FenceToken(1))).expect("right fingerprint");

        assert_eq!(left, right);
    }

    #[test]
    fn semantic_fingerprint_excludes_writer_context_and_commit_id() {
        let left =
            semantic_commit_fingerprint(&core_request(FenceToken(1))).expect("left fingerprint");
        let different_fence = semantic_commit_fingerprint(&core_request(FenceToken(2)))
            .expect("different fence fingerprint");
        let mut different_writer = core_request(FenceToken(1));
        different_writer.writer_id = "writer-b".to_owned();
        let different_writer =
            semantic_commit_fingerprint(&different_writer).expect("different writer fingerprint");
        let mut different_commit_id = core_request(FenceToken(1));
        different_commit_id.commit_id = CommitId::from("commit-b");
        let different_commit_id = semantic_commit_fingerprint(&different_commit_id)
            .expect("different commit id fingerprint");

        assert_eq!(left, different_fence);
        assert_eq!(left, different_writer);
        assert_eq!(left, different_commit_id);
    }

    #[test]
    fn semantic_fingerprint_changes_when_logical_inputs_change() {
        let baseline = semantic_commit_fingerprint(&core_request(FenceToken(1)))
            .expect("baseline fingerprint");
        let mut changed = core_request(FenceToken(1));
        changed.ops = vec![CommitOp::CreateDir {
            parent_inode: InodeId(1),
            display_name: "drafts".to_owned(),
        }];

        let changed = semantic_commit_fingerprint(&changed).expect("changed fingerprint");

        assert_ne!(baseline, changed);
    }

    #[test]
    fn v0_semantic_fingerprint_matches_core_semantic_fingerprint() {
        let namespace_id = NamespaceId::from("demo");
        let api_request = api_v0::CommitRequest {
            commit_id: CommitId::from("commit-a"),
            planned_head_seq: ChangeSeq(7),
            preconditions: vec![api_v0::CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(7),
            }],
            ops: vec![api_v0::CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            message: Some("create docs".to_owned()),
            annotations: None,
        };
        let core = super::super::commit_request_from_v0(
            super::super::CommitExecutionContext {
                namespace_id: namespace_id.clone(),
                writer_id: "writer-a".to_owned(),
                writer_fence_token: FenceToken(1),
            },
            api_request.clone(),
        )
        .expect("core request");

        assert_eq!(
            semantic_commit_fingerprint_for_v0_request(&namespace_id, &api_request)
                .expect("api fingerprint"),
            semantic_commit_fingerprint(&core).expect("core fingerprint")
        );
    }
}
