//! Commit identity fingerprints (format spec, "Commit identity
//! fingerprints"): stable digests over a commit's semantic content, used to
//! decide whether a reused commit id carries the same mutation or a
//! conflicting one.

use super::CommitRequest;
use loonfs_api::v0::{CommitOp, CommitPrecondition, CommitRequest as ApiCommitRequest};
use loonfs_api::{ContentRef, NamespaceId};
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use thiserror::Error;

const CORE_COMMIT_FINGERPRINT_DOMAIN: &str = "loonfs.core.commit.semantic.v0";
pub(crate) const PATH_INTENT_FINGERPRINT_DOMAIN: &str = "loonfs.path.intent.semantic.v0";

/// Scheme-and-algorithm tag carried by every stored fingerprint value.
///
/// `v0` names the canonicalization rules (domain string plus the frozen v0
/// preimage encoding; format spec, "Commit identity fingerprints") and
/// `sha256` the digest algorithm, so either can change later without
/// re-interpreting values already stored in WAL records and commit receipts.
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
    Ok(fingerprint_bytes(&bytes))
}

fn fingerprint_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut value = String::with_capacity(FINGERPRINT_SCHEME.len() + 1 + digest.len() * 2);
    value.push_str(FINGERPRINT_SCHEME);
    value.push(':');
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to a String should not fail");
    }
    value
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

/// Encodes the frozen v0 semantic-commit preimage.
///
/// This JSON encoding is a durable format. Its field order, tags, and value
/// representations must not change when the API vocabulary's serde encoding
/// evolves. A new fingerprint scheme is required for any format change.
fn encode_core_commit_preimage(
    namespace_id: &NamespaceId,
    preconditions: &[CommitPrecondition],
    ops: &[CommitOp],
    message: &Option<String>,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&FrozenCoreCommitPreimage {
        namespace_id,
        preconditions,
        ops,
        message,
    })
}

struct FrozenCoreCommitPreimage<'a> {
    namespace_id: &'a NamespaceId,
    preconditions: &'a [CommitPrecondition],
    ops: &'a [CommitOp],
    message: &'a Option<String>,
}

impl Serialize for FrozenCoreCommitPreimage<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CoreCommitPreimage", 5)?;
        state.serialize_field("domain", CORE_COMMIT_FINGERPRINT_DOMAIN)?;
        state.serialize_field("namespace_id", self.namespace_id.as_str())?;
        state.serialize_field(
            "preconditions",
            &FrozenCommitPreconditions(self.preconditions),
        )?;
        state.serialize_field("ops", &FrozenCommitOps(self.ops))?;
        state.serialize_field("message", self.message)?;
        state.end()
    }
}

struct FrozenCommitPreconditions<'a>(&'a [CommitPrecondition]);

impl Serialize for FrozenCommitPreconditions<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for precondition in self.0 {
            sequence.serialize_element(&FrozenCommitPrecondition(precondition))?;
        }
        sequence.end()
    }
}

struct FrozenCommitPrecondition<'a>(&'a CommitPrecondition);

impl Serialize for FrozenCommitPrecondition<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            CommitPrecondition::InodeRevisionIs {
                inode_id,
                revision_no,
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("kind", "inode_revision_is")?;
                map.serialize_entry("inode_id", &inode_id.0)?;
                map.serialize_entry("revision_no", &revision_no.0)?;
                map.end()
            }
            CommitPrecondition::AncestorsNotSubtreeDeleted { inode_id } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("kind", "ancestors_not_subtree_deleted")?;
                map.serialize_entry("inode_id", &inode_id.0)?;
                map.end()
            }
            CommitPrecondition::ChildNameAbsent {
                parent_inode_id,
                name_key,
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("kind", "child_name_absent")?;
                map.serialize_entry("parent_inode_id", &parent_inode_id.0)?;
                map.serialize_entry("name_key", name_key.as_str())?;
                map.end()
            }
            CommitPrecondition::BindingIs {
                parent_inode_id,
                name_key,
                child_inode_id,
                bind_seq,
                bind_delta_index,
            } => {
                let mut map = serializer.serialize_map(Some(6))?;
                map.serialize_entry("kind", "binding_is")?;
                map.serialize_entry("parent_inode_id", &parent_inode_id.0)?;
                map.serialize_entry("name_key", name_key.as_str())?;
                map.serialize_entry("child_inode_id", &child_inode_id.0)?;
                map.serialize_entry("bind_seq", &bind_seq.0)?;
                map.serialize_entry("bind_delta_index", bind_delta_index)?;
                map.end()
            }
            CommitPrecondition::DirectoryEmpty { inode_id } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("kind", "directory_empty")?;
                map.serialize_entry("inode_id", &inode_id.0)?;
                map.end()
            }
        }
    }
}

struct FrozenCommitOps<'a>(&'a [CommitOp]);

impl Serialize for FrozenCommitOps<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for op in self.0 {
            sequence.serialize_element(&FrozenCommitOp(op))?;
        }
        sequence.end()
    }
}

struct FrozenCommitOp<'a>(&'a CommitOp);

impl Serialize for FrozenCommitOp<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            CommitOp::CreateDirectory {
                parent_inode_id,
                display_name,
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("kind", "create_directory")?;
                map.serialize_entry("parent_inode_id", &parent_inode_id.0)?;
                map.serialize_entry("display_name", display_name)?;
                map.end()
            }
            CommitOp::CreateFile {
                parent_inode_id,
                display_name,
                content_ref,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("kind", "create_file")?;
                map.serialize_entry("parent_inode_id", &parent_inode_id.0)?;
                map.serialize_entry("display_name", display_name)?;
                map.serialize_entry("content_ref", &FrozenContentRef(content_ref))?;
                map.end()
            }
            CommitOp::ReplaceFile {
                inode_id,
                base_revision_no,
                content_ref,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("kind", "replace_file")?;
                map.serialize_entry("inode_id", &inode_id.0)?;
                map.serialize_entry("base_revision_no", &base_revision_no.0)?;
                map.serialize_entry("content_ref", &FrozenContentRef(content_ref))?;
                map.end()
            }
            CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("kind", "restore_revision")?;
                map.serialize_entry("inode_id", &inode_id.0)?;
                map.serialize_entry("source_revision_no", &source_revision_no.0)?;
                map.serialize_entry("base_revision_no", &base_revision_no.0)?;
                map.end()
            }
            CommitOp::DeleteFile { inode_id } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("kind", "delete_file")?;
                map.serialize_entry("inode_id", &inode_id.0)?;
                map.end()
            }
            CommitOp::Rename {
                inode_id,
                new_parent_inode_id,
                new_display_name,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("kind", "rename")?;
                map.serialize_entry("inode_id", &inode_id.0)?;
                map.serialize_entry("new_parent_inode_id", &new_parent_inode_id.0)?;
                map.serialize_entry("new_display_name", new_display_name)?;
                map.end()
            }
            CommitOp::DeleteSubtree { root_inode_id } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("kind", "delete_subtree")?;
                map.serialize_entry("root_inode_id", &root_inode_id.0)?;
                map.end()
            }
            CommitOp::Undelete {
                inode_id,
                deleted_at_seq,
                parent_inode_id,
                display_name,
            } => {
                let mut map = serializer.serialize_map(Some(5))?;
                map.serialize_entry("kind", "undelete")?;
                map.serialize_entry("inode_id", &inode_id.0)?;
                map.serialize_entry("deleted_at_seq", &deleted_at_seq.0)?;
                map.serialize_entry("parent_inode_id", &parent_inode_id.0)?;
                map.serialize_entry("display_name", display_name)?;
                map.end()
            }
        }
    }
}

struct FrozenContentRef<'a>(&'a ContentRef);

impl Serialize for FrozenContentRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ContentRef", 3)?;
        state.serialize_field("kind", self.0.kind.as_str())?;
        state.serialize_field("digest", self.0.digest.as_str())?;
        state.serialize_field("size_bytes", &self.0.size_bytes)?;
        state.end()
    }
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
    request: &ApiCommitRequest,
) -> Result<CoreCommitFingerprint, CommitFingerprintError> {
    core_commit_fingerprint_from_parts(
        namespace_id,
        &request.preconditions,
        &request.ops,
        &request.message,
    )
}

fn core_commit_fingerprint_from_parts(
    namespace_id: &NamespaceId,
    preconditions: &[CommitPrecondition],
    ops: &[CommitOp],
    message: &Option<String>,
) -> Result<CoreCommitFingerprint, CommitFingerprintError> {
    encode_core_commit_preimage(namespace_id, preconditions, ops, message)
        .map(|bytes| CoreCommitFingerprint::new_unchecked(fingerprint_bytes(&bytes)))
        .map_err(|err| CommitFingerprintError::Codec(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitExecutionContext, CommitRequest};
    use loonfs_api::v0::{CommitOp, CommitPrecondition, CommitRequest as ApiCommitRequest};
    use loonfs_api::{
        ChangeSeq, CommitId, ContentRef, DisplayName, InodeId, NameKey, NamespaceId, RevisionNo,
        WriterEpoch,
    };

    fn core_request(writer_epoch: WriterEpoch) -> CommitRequest {
        CommitRequest {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch,
            ops: vec![CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
            }],
            preconditions: Vec::new(),
            message: Some("create docs".to_owned()),
        }
    }

    fn representative_request() -> CommitRequest {
        let content_ref = ContentRef::whole_file_v0(b"fingerprint bytes");
        let name_key =
            NameKey::for_display_name(&DisplayName::parse("Docs").expect("display name"));
        CommitRequest {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit-representative").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(9),
            preconditions: vec![
                CommitPrecondition::InodeRevisionIs {
                    inode_id: InodeId(5),
                    revision_no: RevisionNo(1),
                },
                CommitPrecondition::AncestorsNotSubtreeDeleted {
                    inode_id: InodeId(5),
                },
                CommitPrecondition::ChildNameAbsent {
                    parent_inode_id: InodeId(1),
                    name_key: name_key.clone(),
                },
                CommitPrecondition::BindingIs {
                    parent_inode_id: InodeId(1),
                    name_key,
                    child_inode_id: InodeId(5),
                    bind_seq: ChangeSeq(7),
                    bind_delta_index: 3,
                },
                CommitPrecondition::DirectoryEmpty {
                    inode_id: InodeId(9),
                },
            ],
            ops: vec![
                CommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("Docs")
                        .expect("valid display name"),
                },
                CommitOp::CreateFile {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("a.txt")
                        .expect("valid display name"),
                    content_ref: content_ref.clone(),
                },
                CommitOp::ReplaceFile {
                    inode_id: InodeId(5),
                    base_revision_no: RevisionNo(1),
                    content_ref,
                },
                CommitOp::RestoreRevision {
                    inode_id: InodeId(5),
                    source_revision_no: RevisionNo(1),
                    base_revision_no: RevisionNo(2),
                },
                CommitOp::DeleteFile {
                    inode_id: InodeId(5),
                },
                CommitOp::Rename {
                    inode_id: InodeId(5),
                    new_parent_inode_id: InodeId(2),
                    new_display_name: loonfs_api::DisplayName::parse("b.txt")
                        .expect("valid display name"),
                },
                CommitOp::DeleteSubtree {
                    root_inode_id: InodeId(9),
                },
                CommitOp::Undelete {
                    inode_id: InodeId(9),
                    deleted_at_seq: ChangeSeq(11),
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("restored")
                        .expect("valid display name"),
                },
            ],
            message: Some("all operations and preconditions".to_owned()),
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

    /// Pins the exact stored fingerprint captured from the pre-unification
    /// implementation for a representative logical commit.
    ///
    /// If this fails, the canonical preimage changed (format spec, "Commit
    /// identity fingerprints") and every
    /// persisted fingerprint would disagree with recomputed ones, breaking
    /// retry idempotency across versions. Do not update the literal without
    /// bumping the fingerprint scheme tag.
    #[test]
    fn core_commit_fingerprint_value_is_pinned() {
        let fingerprint = core_commit_fingerprint(&representative_request()).expect("fingerprint");

        assert_eq!(
            fingerprint.as_str(),
            "v0:sha256:1dd0ebc0070ba30946d9f29aa08b09c89404d2db05a1f7ab21197bc6feff118a"
        );
    }

    /// Pins the complete durable preimage captured before the unified API
    /// vocabulary stopped participating through its derived serializer.
    #[test]
    fn core_commit_preimage_bytes_are_frozen() {
        let request = representative_request();
        let preimage = encode_core_commit_preimage(
            &request.namespace_id,
            &request.preconditions,
            &request.ops,
            &request.message,
        )
        .expect("encode preimage");
        let expected = concat!(
            r#"{"domain":"loonfs.core.commit.semantic.v0","namespace_id":"demo","preconditions":["#,
            r#"{"kind":"inode_revision_is","inode_id":5,"revision_no":1},"#,
            r#"{"kind":"ancestors_not_subtree_deleted","inode_id":5},"#,
            r#"{"kind":"child_name_absent","parent_inode_id":1,"name_key":"docs"},"#,
            r#"{"kind":"binding_is","parent_inode_id":1,"name_key":"docs","child_inode_id":5,"bind_seq":7,"bind_delta_index":3},"#,
            r#"{"kind":"directory_empty","inode_id":9}],"ops":["#,
            r#"{"kind":"create_directory","parent_inode_id":1,"display_name":"Docs"},"#,
            r#"{"kind":"create_file","parent_inode_id":1,"display_name":"a.txt","content_ref":{"kind":"whole_file_v0","digest":"sha256:f332dea40056a654e2ec6201ce338e01123e67722207e67c4c950d02ba9fb4bb","size_bytes":17}},"#,
            r#"{"kind":"replace_file","inode_id":5,"base_revision_no":1,"content_ref":{"kind":"whole_file_v0","digest":"sha256:f332dea40056a654e2ec6201ce338e01123e67722207e67c4c950d02ba9fb4bb","size_bytes":17}},"#,
            r#"{"kind":"restore_revision","inode_id":5,"source_revision_no":1,"base_revision_no":2},"#,
            r#"{"kind":"delete_file","inode_id":5},"#,
            r#"{"kind":"rename","inode_id":5,"new_parent_inode_id":2,"new_display_name":"b.txt"},"#,
            r#"{"kind":"delete_subtree","root_inode_id":9},"#,
            r#"{"kind":"undelete","inode_id":9,"deleted_at_seq":11,"parent_inode_id":1,"display_name":"restored"}],"message":"all operations and preconditions"}"#,
        );

        assert_eq!(preimage, expected.as_bytes());
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
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse("drafts").expect("valid display name"),
        }];

        let changed = core_commit_fingerprint(&changed).expect("changed fingerprint");

        assert_ne!(baseline, changed);
    }

    #[test]
    fn v0_core_commit_fingerprint_matches_core_commit_fingerprint() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let api_request = ApiCommitRequest {
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
            }],
            message: Some("create docs".to_owned()),
        };
        let core = CommitRequest::from_v0(
            CommitExecutionContext {
                namespace_id: namespace_id.clone(),
                writer_id: "writer-a".to_owned(),
                writer_session_id: "wrs_test".to_owned(),
                writer_epoch: WriterEpoch(1),
            },
            api_request.clone(),
        );

        assert_eq!(
            core_commit_fingerprint_for_v0_request(&namespace_id, &api_request)
                .expect("api fingerprint"),
            core_commit_fingerprint(&core).expect("core fingerprint")
        );
    }
}
