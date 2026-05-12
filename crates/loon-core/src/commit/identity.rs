use loon_api::{payload_checksum_sha256, v0 as api_v0};
use serde::Serialize;
use thiserror::Error;

const SEMANTIC_CORE_COMMIT_DOMAIN: &str = "loonfs.core.commit.semantic.v0";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommitFingerprintError {
    #[error("commit fingerprint codec error: {0}")]
    Codec(String),
}

pub fn semantic_commit_fingerprint_sha256(
    request: &super::CommitRequest,
) -> Result<String, CommitFingerprintError> {
    if let Some(fingerprint) = &request.semantic_commit_fingerprint_sha256 {
        return Ok(fingerprint.clone());
    }

    #[derive(Serialize)]
    struct CanonicalSemanticCommit<'a> {
        domain: &'static str,
        namespace_id: &'a loon_api::NamespaceId,
        planned_head_seq: loon_api::ChangeSeq,
        preconditions: &'a [super::Precondition],
        ops: &'a [super::CommitOp],
        message: &'a Option<String>,
        annotations: &'a Option<api_v0::CommitAnnotations>,
    }

    payload_checksum_sha256(&CanonicalSemanticCommit {
        domain: SEMANTIC_CORE_COMMIT_DOMAIN,
        namespace_id: &request.namespace_id,
        planned_head_seq: request.planned_head_seq,
        preconditions: &request.preconditions,
        ops: &request.ops,
        message: &request.message,
        annotations: &request.annotations,
    })
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
            semantic_commit_fingerprint_sha256: None,
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
        let left = semantic_commit_fingerprint_sha256(&core_request(FenceToken(1)))
            .expect("left fingerprint");
        let right = semantic_commit_fingerprint_sha256(&core_request(FenceToken(1)))
            .expect("right fingerprint");

        assert_eq!(left, right);
    }

    #[test]
    fn semantic_fingerprint_excludes_writer_context_and_commit_id() {
        let left = semantic_commit_fingerprint_sha256(&core_request(FenceToken(1)))
            .expect("left fingerprint");
        let different_fence = semantic_commit_fingerprint_sha256(&core_request(FenceToken(2)))
            .expect("different fence fingerprint");
        let mut different_writer = core_request(FenceToken(1));
        different_writer.writer_id = "writer-b".to_owned();
        let different_writer = semantic_commit_fingerprint_sha256(&different_writer)
            .expect("different writer fingerprint");
        let mut different_commit_id = core_request(FenceToken(1));
        different_commit_id.commit_id = CommitId::from("commit-b");
        let different_commit_id = semantic_commit_fingerprint_sha256(&different_commit_id)
            .expect("different commit id fingerprint");

        assert_eq!(left, different_fence);
        assert_eq!(left, different_writer);
        assert_eq!(left, different_commit_id);
    }

    #[test]
    fn semantic_fingerprint_changes_when_logical_inputs_change() {
        let baseline = semantic_commit_fingerprint_sha256(&core_request(FenceToken(1)))
            .expect("baseline fingerprint");
        let mut changed = core_request(FenceToken(1));
        changed.ops = vec![CommitOp::CreateDir {
            parent_inode: InodeId(1),
            display_name: "drafts".to_owned(),
        }];

        let changed = semantic_commit_fingerprint_sha256(&changed).expect("changed fingerprint");

        assert_ne!(baseline, changed);
    }
}
