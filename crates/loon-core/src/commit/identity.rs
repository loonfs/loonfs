use super::api_adapter::{commit_op_from_v0, commit_precondition_from_v0};
use super::{CommitOp, CommitRequest, Precondition};
use loon_api::{payload_checksum_sha256, v0 as api_v0, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const CORE_COMMIT_FINGERPRINT_DOMAIN: &str = "loonfs.core.commit.semantic.v0";
pub(crate) const PATH_INTENT_FINGERPRINT_DOMAIN: &str = "loonfs.path.intent.semantic.v0";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CoreCommitFingerprint(String);

impl CoreCommitFingerprint {
    pub(crate) fn new_unchecked(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PathIntentFingerprint(String);

impl PathIntentFingerprint {
    pub(crate) fn new_unchecked(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SemanticMutationIdentity {
    CoreCommit(CoreCommitFingerprint),
    PathIntent(PathIntentFingerprint),
}

impl SemanticMutationIdentity {
    pub fn as_str(&self) -> &str {
        match self {
            Self::CoreCommit(fingerprint) => fingerprint.as_str(),
            Self::PathIntent(fingerprint) => fingerprint.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommitFingerprintError {
    #[error("commit fingerprint codec error: {0}")]
    Codec(String),
}

pub fn core_commit_fingerprint(
    request: &CommitRequest,
) -> Result<CoreCommitFingerprint, CommitFingerprintError> {
    core_commit_fingerprint_from_parts(
        &request.namespace_id,
        &request.preconditions,
        &request.ops,
        &request.message,
        &request.annotations,
    )
}

pub fn core_commit_fingerprint_for_v0_request(
    namespace_id: &NamespaceId,
    request: &api_v0::CommitRequest,
) -> Result<CoreCommitFingerprint, CommitFingerprintError> {
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
    core_commit_fingerprint_from_parts(
        namespace_id,
        &preconditions,
        &ops,
        &request.message,
        &request.annotations,
    )
}

fn core_commit_fingerprint_from_parts(
    namespace_id: &NamespaceId,
    preconditions: &[Precondition],
    ops: &[CommitOp],
    message: &Option<String>,
    annotations: &Option<api_v0::CommitAnnotations>,
) -> Result<CoreCommitFingerprint, CommitFingerprintError> {
    #[derive(Serialize)]
    struct CanonicalCoreCommit<'a> {
        domain: &'static str,
        namespace_id: &'a NamespaceId,
        preconditions: &'a [Precondition],
        ops: &'a [CommitOp],
        message: &'a Option<String>,
        annotations: &'a Option<api_v0::CommitAnnotations>,
    }

    payload_checksum_sha256(&CanonicalCoreCommit {
        domain: CORE_COMMIT_FINGERPRINT_DOMAIN,
        namespace_id,
        preconditions,
        ops,
        message,
        annotations,
    })
    .map(CoreCommitFingerprint::new_unchecked)
    .map_err(|err| CommitFingerprintError::Codec(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitOp, CommitRequest};
    use loon_api::{CommitId, FenceToken, InodeId, NamespaceId};

    fn core_request(writer_fence_token: FenceToken) -> CommitRequest {
        CommitRequest {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token,
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            preconditions: Vec::new(),
            message: Some("create docs".to_owned()),
            annotations: None,
        }
    }

    #[test]
    fn core_commit_fingerprint_is_stable_for_same_logical_commit() {
        let left = core_commit_fingerprint(&core_request(FenceToken(1))).expect("left fingerprint");
        let right =
            core_commit_fingerprint(&core_request(FenceToken(1))).expect("right fingerprint");

        assert_eq!(left, right);
    }

    #[test]
    fn core_commit_fingerprint_excludes_writer_context_and_commit_id() {
        let left = core_commit_fingerprint(&core_request(FenceToken(1))).expect("left fingerprint");
        let different_fence = core_commit_fingerprint(&core_request(FenceToken(2)))
            .expect("different fence fingerprint");
        let mut different_writer = core_request(FenceToken(1));
        different_writer.writer_id = "writer-b".to_owned();
        let different_writer =
            core_commit_fingerprint(&different_writer).expect("different writer fingerprint");
        let mut different_commit_id = core_request(FenceToken(1));
        different_commit_id.commit_id = CommitId::parse("commit-b").expect("valid commit id");
        let different_commit_id =
            core_commit_fingerprint(&different_commit_id).expect("different commit id fingerprint");

        assert_eq!(left, different_fence);
        assert_eq!(left, different_writer);
        assert_eq!(left, different_commit_id);
    }

    #[test]
    fn core_commit_fingerprint_changes_when_logical_inputs_change() {
        let baseline =
            core_commit_fingerprint(&core_request(FenceToken(1))).expect("baseline fingerprint");
        let mut changed = core_request(FenceToken(1));
        changed.ops = vec![CommitOp::CreateDir {
            parent_inode: InodeId(1),
            display_name: "drafts".to_owned(),
        }];

        let changed = core_commit_fingerprint(&changed).expect("changed fingerprint");

        assert_ne!(baseline, changed);
    }

    #[test]
    fn v0_core_commit_fingerprint_matches_core_commit_fingerprint() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let api_request = api_v0::CommitRequest {
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
            preconditions: Vec::new(),
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
            core_commit_fingerprint_for_v0_request(&namespace_id, &api_request)
                .expect("api fingerprint"),
            core_commit_fingerprint(&core).expect("core fingerprint")
        );
    }
}
