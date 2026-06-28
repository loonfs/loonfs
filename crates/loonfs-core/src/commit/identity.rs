use super::api_adapter::{commit_op_from_v0, commit_precondition_from_v0};
use super::{CommitOp, CommitRequest, Precondition};
use loonfs_api::v0 as api_v0;
use loonfs_api::NamespaceId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use thiserror::Error;

const CORE_COMMIT_FINGERPRINT_DOMAIN: &str = "loonfs.core.commit.semantic.v0";
pub(crate) const PATH_INTENT_FINGERPRINT_DOMAIN: &str = "loonfs.path.intent.semantic.v0";

/// Scheme-and-algorithm tag carried by every stored fingerprint value.
///
/// `v0` names the canonicalization rules (domain string plus the v0 wire
/// encoding of the preimage; format spec, "Commit identity fingerprints")
/// and `sha256` the digest
/// algorithm, so either can change later without re-interpreting values
/// already stored in WAL records and commit receipts.
const FINGERPRINT_SCHEME: &str = "v0:sha256";

/// Computes a stored fingerprint value (`v0:sha256:<64 lowercase hex>`) from
/// a canonical preimage.
///
/// The preimage's compact JSON encoding is the durable contract: pinned-value
/// tests below (and in `path::write::planner`) fail if it drifts.
pub(crate) fn fingerprint_digest<T>(preimage: &T) -> Result<String, serde_json::Error>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(preimage)?;
    let digest = Sha256::digest(&bytes);
    let mut value = String::with_capacity(FINGERPRINT_SCHEME.len() + 1 + digest.len() * 2);
    value.push_str(FINGERPRINT_SCHEME);
    value.push(':');
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to a String should not fail");
    }
    Ok(value)
}

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
    core_commit_fingerprint_from_parts(namespace_id, &preconditions, &ops, &request.message)
}

fn core_commit_fingerprint_from_parts(
    namespace_id: &NamespaceId,
    preconditions: &[Precondition],
    ops: &[CommitOp],
    message: &Option<String>,
) -> Result<CoreCommitFingerprint, CommitFingerprintError> {
    // This struct's compact JSON is the v0 fingerprint preimage (format
    // spec, "Commit identity fingerprints"): field order, field names, and
    // the v0-mirroring encodings of
    // `Precondition` and `CommitOp` are all durable contract.
    #[derive(Serialize)]
    struct CanonicalCoreCommit<'a> {
        domain: &'static str,
        namespace_id: &'a NamespaceId,
        preconditions: &'a [Precondition],
        ops: &'a [CommitOp],
        message: &'a Option<String>,
    }

    fingerprint_digest(&CanonicalCoreCommit {
        domain: CORE_COMMIT_FINGERPRINT_DOMAIN,
        namespace_id,
        preconditions,
        ops,
        message,
    })
    .map(CoreCommitFingerprint::new_unchecked)
    .map_err(|err| CommitFingerprintError::Codec(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitOp, CommitRequest};
    use loonfs_api::{CommitId, InodeId, NamespaceId, WriterEpoch};

    fn core_request(writer_epoch: WriterEpoch) -> CommitRequest {
        CommitRequest {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch,
            ops: vec![CommitOp::CreateDirectory {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            preconditions: Vec::new(),
            message: Some("create docs".to_owned()),
        }
    }

    #[test]
    fn core_commit_fingerprint_is_stable_for_same_logical_commit() {
        let left =
            core_commit_fingerprint(&core_request(WriterEpoch(1))).expect("left fingerprint");
        let right =
            core_commit_fingerprint(&core_request(WriterEpoch(1))).expect("right fingerprint");

        assert_eq!(left, right);
    }

    /// Pins the exact stored fingerprint for a fixed logical commit.
    ///
    /// If this fails, the canonical preimage changed (format spec, "Commit
    /// identity fingerprints") and every
    /// persisted fingerprint would disagree with recomputed ones, breaking
    /// retry idempotency across versions. Do not update the literal without
    /// bumping the fingerprint scheme tag.
    #[test]
    fn core_commit_fingerprint_value_is_pinned() {
        let fingerprint =
            core_commit_fingerprint(&core_request(WriterEpoch(1))).expect("fingerprint");

        assert_eq!(
            fingerprint.as_str(),
            "v0:sha256:99748ecdf47502f2be08817699c571d4cd69ecabd6827fb66c9e2f587384a841"
        );
    }

    /// The internal commit vocabulary must serialize exactly like the v0 wire
    /// vocabulary: the fingerprint preimage is defined over the v0 encoding.
    #[test]
    fn internal_commit_vocabulary_matches_v0_wire_encoding() {
        let content_ref = loonfs_api::ContentRef::whole_file_v0(b"fingerprint bytes");
        let name_key = loonfs_api::NameKey::for_display_name(
            loonfs_api::NamePolicy::default(),
            &loonfs_api::DisplayName::parse("Docs").expect("display name"),
        );
        let v0_ops = vec![
            api_v0::CommitOp::CreateDirectory {
                parent_inode: InodeId(1),
                display_name: "Docs".to_owned(),
            },
            api_v0::CommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "a.txt".to_owned(),
                content_ref: content_ref.clone(),
            },
            api_v0::CommitOp::ReplaceFile {
                inode_id: InodeId(5),
                base_revision_no: loonfs_api::RevisionNo(1),
                content_ref: content_ref.clone(),
            },
            api_v0::CommitOp::RestoreRevision {
                inode_id: InodeId(5),
                source_revision_no: loonfs_api::RevisionNo(1),
                base_revision_no: loonfs_api::RevisionNo(2),
            },
            api_v0::CommitOp::DeleteFile {
                inode_id: InodeId(5),
            },
            api_v0::CommitOp::Rename {
                inode_id: InodeId(5),
                new_parent_inode: InodeId(2),
                new_display_name: "b.txt".to_owned(),
                behavior: api_v0::MoveBehavior::NoReplace,
            },
            api_v0::CommitOp::DeleteSubtree {
                root_inode: InodeId(9),
            },
        ];
        let v0_preconditions = vec![
            api_v0::CommitPrecondition::InodeRevisionIs {
                inode_id: InodeId(5),
                revision_no: loonfs_api::RevisionNo(1),
            },
            api_v0::CommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: InodeId(5),
            },
            api_v0::CommitPrecondition::ChildNameAbsent {
                parent_inode: InodeId(1),
                name_key: name_key.clone(),
            },
            api_v0::CommitPrecondition::BindingIs {
                parent_inode: InodeId(1),
                name_key,
                child_inode: InodeId(5),
                bind_seq: loonfs_api::ChangeSeq(7),
                bind_delta_index: 3,
            },
            api_v0::CommitPrecondition::DirectoryEmpty {
                inode_id: InodeId(9),
            },
        ];

        for v0_op in v0_ops {
            let internal = commit_op_from_v0(v0_op.clone());
            assert_eq!(
                serde_json::to_value(&internal).expect("internal op"),
                serde_json::to_value(&v0_op).expect("v0 op"),
            );
        }
        for v0_precondition in v0_preconditions {
            let internal = commit_precondition_from_v0(v0_precondition.clone());
            assert_eq!(
                serde_json::to_value(&internal).expect("internal precondition"),
                serde_json::to_value(&v0_precondition).expect("v0 precondition"),
            );
        }
    }

    #[test]
    fn core_commit_fingerprint_excludes_writer_context_and_commit_id() {
        let left =
            core_commit_fingerprint(&core_request(WriterEpoch(1))).expect("left fingerprint");
        let different_epoch = core_commit_fingerprint(&core_request(WriterEpoch(2)))
            .expect("different epoch fingerprint");
        let mut different_writer = core_request(WriterEpoch(1));
        different_writer.writer_id = "writer-b".to_owned();
        let different_writer =
            core_commit_fingerprint(&different_writer).expect("different writer fingerprint");
        let mut different_session = core_request(WriterEpoch(1));
        different_session.writer_session_id = "wrs_other".to_owned();
        let different_session =
            core_commit_fingerprint(&different_session).expect("different session fingerprint");
        let mut different_commit_id = core_request(WriterEpoch(1));
        different_commit_id.commit_id = CommitId::parse("commit-b").expect("valid commit id");
        let different_commit_id =
            core_commit_fingerprint(&different_commit_id).expect("different commit id fingerprint");

        assert_eq!(left, different_epoch);
        assert_eq!(left, different_writer);
        assert_eq!(left, different_session);
        assert_eq!(left, different_commit_id);
    }

    #[test]
    fn core_commit_fingerprint_changes_when_logical_inputs_change() {
        let baseline =
            core_commit_fingerprint(&core_request(WriterEpoch(1))).expect("baseline fingerprint");
        let mut changed = core_request(WriterEpoch(1));
        changed.ops = vec![CommitOp::CreateDirectory {
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
            ops: vec![api_v0::CommitOp::CreateDirectory {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            message: Some("create docs".to_owned()),
        };
        let core = super::super::commit_request_from_v0(
            super::super::CommitExecutionContext {
                namespace_id: namespace_id.clone(),
                writer_id: "writer-a".to_owned(),
                writer_session_id: "wrs_test".to_owned(),
                writer_epoch: WriterEpoch(1),
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
