use super::CommitRequest;
use loon_api::{payload_checksum_sha256, v0 as api_v0};
use serde::Serialize;
use thiserror::Error;

const SOURCE_API_COMMIT_DOMAIN: &str = "loonfs.api.v0.commit";
const DURABLE_CORE_COMMIT_DOMAIN: &str = "loonfs.core.commit.durable.v0";
const SEMANTIC_CORE_COMMIT_DOMAIN: &str = "loonfs.core.commit.semantic.v0";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommitIdentityError {
    #[error("commit identity codec error: {0}")]
    Codec(String),
}

pub fn source_api_commit_checksum_sha256(
    request: &api_v0::CommitRequest,
) -> Result<String, CommitIdentityError> {
    #[derive(Serialize)]
    struct CanonicalSourceApiCommit<'a> {
        domain: &'static str,
        request: &'a api_v0::CommitRequest,
    }

    payload_checksum_sha256(&CanonicalSourceApiCommit {
        domain: SOURCE_API_COMMIT_DOMAIN,
        request,
    })
    .map_err(|err| CommitIdentityError::Codec(err.to_string()))
}

pub fn durable_commit_checksum_sha256(
    request: &CommitRequest,
) -> Result<String, CommitIdentityError> {
    #[derive(Serialize)]
    struct CanonicalDurableCommit<'a> {
        domain: &'static str,
        request: &'a CommitRequest,
    }

    payload_checksum_sha256(&CanonicalDurableCommit {
        domain: DURABLE_CORE_COMMIT_DOMAIN,
        request,
    })
    .map_err(|err| CommitIdentityError::Codec(err.to_string()))
}

pub fn semantic_commit_fingerprint_sha256(
    request: &CommitRequest,
) -> Result<String, CommitIdentityError> {
    if let Some(fingerprint) = &request.semantic_commit_fingerprint_sha256 {
        return Ok(fingerprint.clone());
    }

    #[derive(Serialize)]
    struct CanonicalSemanticCommit<'a> {
        domain: &'static str,
        namespace_id: &'a loon_api::NamespaceId,
        commit_id: &'a loon_api::CommitId,
        writer_id: &'a str,
        planned_head_seq: loon_api::ChangeSeq,
        preconditions: &'a [super::Precondition],
        ops: &'a [super::CommitOp],
        message: &'a Option<String>,
        annotations: &'a Option<api_v0::CommitAnnotations>,
    }

    payload_checksum_sha256(&CanonicalSemanticCommit {
        domain: SEMANTIC_CORE_COMMIT_DOMAIN,
        namespace_id: &request.namespace_id,
        commit_id: &request.commit_id,
        writer_id: &request.writer_id,
        planned_head_seq: request.planned_head_seq,
        preconditions: &request.preconditions,
        ops: &request.ops,
        message: &request.message,
        annotations: &request.annotations,
    })
    .map_err(|err| CommitIdentityError::Codec(err.to_string()))
}

pub fn commit_identity(
    source_api_commit_checksum_sha256: Option<String>,
    request: &CommitRequest,
) -> Result<super::CommitIdentity, CommitIdentityError> {
    Ok(super::CommitIdentity {
        source_api_commit_checksum_sha256,
        durable_commit_checksum_sha256: durable_commit_checksum_sha256(request)?,
        semantic_commit_fingerprint_sha256: semantic_commit_fingerprint_sha256(request)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitOp, CommitRequest, Precondition};
    use loon_api::{
        v0::CommitOp as ApiCommitOp, ChangeSeq, CommitId, FenceToken, InodeId, NamespaceId,
    };

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
    fn semantic_fingerprint_excludes_writer_fence_token() {
        let left = semantic_commit_fingerprint_sha256(&core_request(FenceToken(1)))
            .expect("left fingerprint");
        let right = semantic_commit_fingerprint_sha256(&core_request(FenceToken(2)))
            .expect("right fingerprint");

        assert_eq!(left, right);
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

    #[test]
    fn source_api_checksum_uses_public_request_shape() {
        let request = api_v0::CommitRequest {
            commit_id: CommitId::from("commit-a"),
            planned_head_seq: ChangeSeq(7),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            message: None,
            annotations: None,
        };

        let checksum = source_api_commit_checksum_sha256(&request).expect("source checksum");

        assert!(!checksum.is_empty());
    }
}
