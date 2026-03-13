use loon_core::commit::{
    build_commit_plan, prepare_commit_head_publish, publish_commit_head, CommitHeadPublishError,
    CommitOp, CommitPlan, CommitRequest, CommitValidationContext, CommitValidationError,
    Precondition, PreparedCommitHeadPublish,
};
use loon_core::wal::{prepare_wal_commit, PreparedWalCommit, WalBuildError};
use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::keys::{namespace_head, namespace_lease};
use loon_objectstore::{ObjectMetadata, ObjectStore};
use loon_types::{
    payload_checksum_sha256, ClientMutationOp, ClientMutationRequest, ClientMutationResponse,
    ControlObjectKind, CreatedRemoteInode, HeadState, HeadStateEnvelope, LeaseStateEnvelope,
    NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMutationExecutionParams {
    pub writer_id: String,
    pub writer_version: String,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutedClientMutation {
    pub commit_request: CommitRequest,
    pub plan: CommitPlan,
    pub wal: PreparedWalCommit,
    pub wal_metadata: ObjectMetadata,
    pub head_publish: PreparedCommitHeadPublish,
    pub head_metadata: ObjectMetadata,
    pub response: ClientMutationResponse,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LoadedHeadObject {
    object_key: String,
    metadata: ObjectMetadata,
    envelope: HeadStateEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LoadedLeaseObject {
    object_key: String,
    envelope: LeaseStateEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ControlObjectLoadError {
    #[error("missing control object `{object_key}`")]
    MissingObject { object_key: String },
    #[error("missing control object after head `{object_key}`")]
    MissingObjectAfterHead { object_key: String },
    #[error(
        "control object kind mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    KindMismatch {
        object_key: String,
        expected: ControlObjectKind,
        actual: ControlObjectKind,
    },
    #[error(
        "control object namespace mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    NamespaceMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error(
        "control object checksum mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ChecksumMismatch {
        object_key: String,
        expected: String,
        actual: String,
    },
    #[error("control object codec error for `{object_key}`: {message}")]
    Codec { object_key: String, message: String },
    #[error("control object store error: {0}")]
    Store(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ClientMutationTranslationError {
    #[error("empty client request id")]
    EmptyClientRequestId,
    #[error("empty writer id")]
    EmptyWriterId,
    #[error("empty display name")]
    EmptyDisplayName,
    #[error("empty content manifest digest")]
    EmptyContentManifestDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ClientMutationExecutionError {
    #[error(transparent)]
    LoadHead(#[from] ControlObjectLoadError),
    #[error("failed to load lease object: {0}")]
    LoadLease(ControlObjectLoadError),
    #[error("missing head etag for `{object_key}`")]
    MissingHeadEtag { object_key: String },
    #[error(transparent)]
    Translate(#[from] ClientMutationTranslationError),
    #[error("commit validation failed: {0:?}")]
    CommitValidation(CommitValidationError),
    #[error("WAL build failed: {0:?}")]
    WalBuild(WalBuildError),
    #[error("head preparation failed: {0:?}")]
    HeadPrepare(CommitHeadPublishError),
    #[error("failed to write WAL object: {0}")]
    WalWrite(String),
    #[error("failed to publish head object: {0}")]
    HeadWrite(String),
    #[error("client mutation must allocate exactly one inode id, got {actual}")]
    UnexpectedAllocatedInodeCount { actual: usize },
}

pub fn execute_client_mutation<S: ObjectStore>(
    store: &S,
    request: &ClientMutationRequest,
    params: &ClientMutationExecutionParams,
) -> Result<ExecutedClientMutation, ClientMutationExecutionError> {
    let loaded_head = read_head_object(store, &request.namespace_id)?;
    let loaded_lease = read_lease_object(store, &request.namespace_id)
        .map_err(ClientMutationExecutionError::LoadLease)?;
    let head_etag = loaded_head.metadata.etag.clone().ok_or_else(|| {
        ClientMutationExecutionError::MissingHeadEtag {
            object_key: loaded_head.object_key.clone(),
        }
    })?;
    let commit_request =
        translate_client_mutation_request(request, params, &loaded_head.envelope.state)?;
    let context = CommitValidationContext {
        head: loaded_head.envelope.state.clone(),
        lease: loaded_lease.envelope.state.clone(),
        now_ms: params.now_ms,
    };
    let plan = build_commit_plan(&commit_request, &context)?;
    let wal = prepare_wal_commit(&commit_request, &plan, &params.writer_version)?;
    let head_publish =
        prepare_commit_head_publish(&loaded_head.envelope.state, &plan, &params.writer_version)?;
    let wal_metadata = store
        .put_if_absent(&wal.object_key, &wal.encoded_bytes)
        .map_err(map_store_write_error)?;
    let head_metadata = publish_commit_head(store, &head_etag, &head_publish)
        .map_err(|err| ClientMutationExecutionError::HeadWrite(format!("{err:?}")))?;
    let response = build_client_mutation_response(request, &plan, &head_publish)?;

    let mut checked_invariants = Vec::new();
    extend_invariants(&mut checked_invariants, &plan.checked_invariants);
    extend_invariants(&mut checked_invariants, &wal.checked_invariants);
    extend_invariants(&mut checked_invariants, &head_publish.checked_invariants);

    Ok(ExecutedClientMutation {
        commit_request,
        plan,
        wal,
        wal_metadata,
        head_publish,
        head_metadata,
        response,
        checked_invariants,
    })
}

fn build_client_mutation_response(
    request: &ClientMutationRequest,
    plan: &CommitPlan,
    head_publish: &PreparedCommitHeadPublish,
) -> Result<ClientMutationResponse, ClientMutationExecutionError> {
    if plan.allocated_inode_ids.len() != 1 {
        return Err(
            ClientMutationExecutionError::UnexpectedAllocatedInodeCount {
                actual: plan.allocated_inode_ids.len(),
            },
        );
    }

    let inode_id = plan.allocated_inode_ids[0];
    let created_inode = match &request.op {
        ClientMutationOp::CreateDir {
            parent_inode_id,
            display_name,
        } => CreatedRemoteInode {
            inode_id,
            inode_kind: loon_types::InodeKind::Dir,
            revision_no: RevisionNo(1),
            parent_inode_id: *parent_inode_id,
            display_name: display_name.clone(),
            content_digest: None,
        },
        ClientMutationOp::CreateFile {
            parent_inode_id,
            display_name,
            content_manifest_digest,
        } => CreatedRemoteInode {
            inode_id,
            inode_kind: loon_types::InodeKind::File,
            revision_no: RevisionNo(1),
            parent_inode_id: *parent_inode_id,
            display_name: display_name.clone(),
            content_digest: Some(content_manifest_digest.clone()),
        },
    };

    Ok(ClientMutationResponse {
        namespace_id: request.namespace_id.clone(),
        client_request_id: request.client_request_id.clone(),
        committed_seq: head_publish.resulting_head.seq,
        created_inode,
    })
}

fn translate_client_mutation_request(
    request: &ClientMutationRequest,
    params: &ClientMutationExecutionParams,
    current_head: &HeadState,
) -> Result<CommitRequest, ClientMutationTranslationError> {
    if request.client_request_id.trim().is_empty() {
        return Err(ClientMutationTranslationError::EmptyClientRequestId);
    }

    if params.writer_id.trim().is_empty() {
        return Err(ClientMutationTranslationError::EmptyWriterId);
    }

    let (op, parent_inode, display_name) = match &request.op {
        ClientMutationOp::CreateDir {
            parent_inode_id,
            display_name,
        } => (
            CommitOp::CreateDir {
                parent_inode: *parent_inode_id,
                display_name: display_name.clone(),
            },
            *parent_inode_id,
            display_name.clone(),
        ),
        ClientMutationOp::CreateFile {
            parent_inode_id,
            display_name,
            content_manifest_digest,
        } => {
            if content_manifest_digest.trim().is_empty() {
                return Err(ClientMutationTranslationError::EmptyContentManifestDigest);
            }

            (
                CommitOp::CreateFile {
                    parent_inode: *parent_inode_id,
                    display_name: display_name.clone(),
                    content_manifest_digest: content_manifest_digest.clone(),
                },
                *parent_inode_id,
                display_name.clone(),
            )
        }
    };

    if display_name.trim().is_empty() {
        return Err(ClientMutationTranslationError::EmptyDisplayName);
    }

    Ok(CommitRequest {
        namespace_id: request.namespace_id.clone(),
        request_id: request.client_request_id.clone(),
        writer_id: params.writer_id.clone(),
        writer_fence_token: current_head.active_fence_token,
        planned_head_seq: current_head.seq,
        ops: vec![op],
        preconditions: vec![
            Precondition::HeadSeqIs(current_head.seq),
            Precondition::ChildNameAbsent {
                parent_inode,
                name_key: display_name,
            },
            Precondition::AncestorsNotSubtreeDeleted {
                inode_id: parent_inode,
            },
        ],
    })
}

fn read_head_object<S: ObjectStore>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedHeadObject, ControlObjectLoadError> {
    let object_key = namespace_head(expected_namespace.as_str());
    let metadata = store
        .head(&object_key)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObject {
            object_key: object_key.clone(),
        })?;
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObjectAfterHead {
            object_key: object_key.clone(),
        })?;
    let envelope: HeadStateEnvelope =
        serde_json::from_slice(&encoded_bytes).map_err(|err| ControlObjectLoadError::Codec {
            object_key: object_key.clone(),
            message: err.to_string(),
        })?;
    validate_head_envelope(expected_namespace, &object_key, &envelope)?;

    Ok(LoadedHeadObject {
        object_key,
        metadata,
        envelope,
    })
}

fn read_lease_object<S: ObjectStore>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedLeaseObject, ControlObjectLoadError> {
    let object_key = namespace_lease(expected_namespace.as_str());
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObject {
            object_key: object_key.clone(),
        })?;
    let envelope: LeaseStateEnvelope =
        serde_json::from_slice(&encoded_bytes).map_err(|err| ControlObjectLoadError::Codec {
            object_key: object_key.clone(),
            message: err.to_string(),
        })?;
    validate_lease_envelope(expected_namespace, &object_key, &envelope)?;

    Ok(LoadedLeaseObject {
        object_key,
        envelope,
    })
}

fn validate_head_envelope(
    expected_namespace: &NamespaceId,
    object_key: &str,
    envelope: &HeadStateEnvelope,
) -> Result<(), ControlObjectLoadError> {
    if envelope.kind != ControlObjectKind::NamespaceHead {
        return Err(ControlObjectLoadError::KindMismatch {
            object_key: object_key.to_owned(),
            expected: ControlObjectKind::NamespaceHead,
            actual: envelope.kind,
        });
    }

    validate_control_checksum(
        object_key,
        &envelope.payload_checksum_sha256,
        &envelope.state,
    )?;

    if &envelope.state.namespace_id != expected_namespace {
        return Err(ControlObjectLoadError::NamespaceMismatch {
            object_key: object_key.to_owned(),
            expected: expected_namespace.clone(),
            actual: envelope.state.namespace_id.clone(),
        });
    }

    Ok(())
}

fn validate_lease_envelope(
    expected_namespace: &NamespaceId,
    object_key: &str,
    envelope: &LeaseStateEnvelope,
) -> Result<(), ControlObjectLoadError> {
    if envelope.kind != ControlObjectKind::NamespaceLease {
        return Err(ControlObjectLoadError::KindMismatch {
            object_key: object_key.to_owned(),
            expected: ControlObjectKind::NamespaceLease,
            actual: envelope.kind,
        });
    }

    validate_control_checksum(
        object_key,
        &envelope.payload_checksum_sha256,
        &envelope.state,
    )?;

    if &envelope.state.namespace_id != expected_namespace {
        return Err(ControlObjectLoadError::NamespaceMismatch {
            object_key: object_key.to_owned(),
            expected: expected_namespace.clone(),
            actual: envelope.state.namespace_id.clone(),
        });
    }

    Ok(())
}

fn validate_control_checksum<T: Serialize>(
    object_key: &str,
    expected_checksum: &str,
    state: &T,
) -> Result<(), ControlObjectLoadError> {
    let actual_checksum =
        payload_checksum_sha256(state).map_err(|err| ControlObjectLoadError::Codec {
            object_key: object_key.to_owned(),
            message: err.to_string(),
        })?;

    if expected_checksum != actual_checksum {
        return Err(ControlObjectLoadError::ChecksumMismatch {
            object_key: object_key.to_owned(),
            expected: expected_checksum.to_owned(),
            actual: actual_checksum,
        });
    }

    Ok(())
}

fn extend_invariants(out: &mut Vec<String>, next: &[String]) {
    for name in next {
        if !out.iter().any(|existing| existing == name) {
            out.push(name.clone());
        }
    }
}

fn map_store_load_error(err: ObjectStoreError) -> ControlObjectLoadError {
    ControlObjectLoadError::Store(err.to_string())
}

fn map_store_write_error(err: ObjectStoreError) -> ClientMutationExecutionError {
    ClientMutationExecutionError::WalWrite(err.to_string())
}

impl From<CommitValidationError> for ClientMutationExecutionError {
    fn from(value: CommitValidationError) -> Self {
        Self::CommitValidation(value)
    }
}

impl From<WalBuildError> for ClientMutationExecutionError {
    fn from(value: WalBuildError) -> Self {
        Self::WalBuild(value)
    }
}

impl From<CommitHeadPublishError> for ClientMutationExecutionError {
    fn from(value: CommitHeadPublishError) -> Self {
        Self::HeadPrepare(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        execute_client_mutation, translate_client_mutation_request, ClientMutationExecutionParams,
    };
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::{namespace_head, namespace_lease};
    use loon_objectstore::ObjectStore;
    use loon_testkit::scenario::Scenario;
    use loon_types::{
        decode_wal_commit_envelope_zstd, ClientMutationRequest, ControlObjectKind, HeadState,
        HeadStateEnvelope, LeaseState, LeaseStateEnvelope, WalOp,
    };
    use serde::Deserialize;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn translate_create_remote_dir_request_builds_create_preconditions() {
        let request = ClientMutationRequest {
            namespace_id: loon_types::NamespaceId::from("ns-1"),
            client_request_id: "client-req-0002".to_owned(),
            op: loon_types::ClientMutationOp::CreateDir {
                parent_inode_id: loon_types::InodeId(2),
                display_name: "drafts".to_owned(),
            },
        };
        let params = ClientMutationExecutionParams {
            writer_id: "writer-a".to_owned(),
            writer_version: "loon-server-test".to_owned(),
            now_ms: 1_500,
        };
        let head = HeadState {
            namespace_id: loon_types::NamespaceId::from("ns-1"),
            seq: loon_types::ChangeSeq(41),
            active_fence_token: loon_types::FenceToken(8),
            next_inode_id: loon_types::InodeId(501),
            snapshot_hint_seq: Some(loon_types::ChangeSeq(40)),
            retention_floor_seq: loon_types::ChangeSeq(40),
        };

        let translated = translate_client_mutation_request(&request, &params, &head)
            .expect("translate create dir request");

        assert_eq!(translated.request_id, "client-req-0002");
        assert_eq!(translated.writer_id, "writer-a");
        assert!(matches!(
            &translated.ops[0],
            loon_core::commit::CommitOp::CreateDir {
                parent_inode,
                display_name
            } if *parent_inode == loon_types::InodeId(2) && display_name == "drafts"
        ));
        assert_eq!(
            translated.preconditions,
            vec![
                loon_core::commit::Precondition::HeadSeqIs(loon_types::ChangeSeq(41)),
                loon_core::commit::Precondition::ChildNameAbsent {
                    parent_inode: loon_types::InodeId(2),
                    name_key: "drafts".to_owned(),
                },
                loon_core::commit::Precondition::AncestorsNotSubtreeDeleted {
                    inode_id: loon_types::InodeId(2),
                },
            ]
        );
    }

    #[test]
    fn client_create_file_fixture_writes_wal_and_publishes_head() {
        let scenario = load_fixture("native/client_create_file_commit_writes_wal_and_head.yaml");
        let initial: MutationInitial = scenario.decode_initial().expect("decode initial state");
        let expect: MutationExpect = scenario.decode_expect().expect("decode expectations");
        let temp_dir = TestDir::new("client-create-file");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");

        seed_head_and_lease(&store, &initial.head, &initial.lease);

        let executed = execute_client_mutation(
            &store,
            &ClientMutationRequest::from(initial.client_request.clone()),
            &ClientMutationExecutionParams {
                writer_id: initial.lease.holder_id.clone(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_500,
            },
        )
        .expect("execute client create-file mutation");

        assert_eq!(executed.wal.object_key, expect.wal_object.key);
        let decoded_wal = decode_wal_commit_envelope_zstd(&executed.wal.encoded_bytes)
            .expect("decode written WAL envelope");
        assert_eq!(decoded_wal.payload.seq, expect.wal_object.payload.seq);
        assert_eq!(
            decoded_wal.payload.base_head_seq,
            expect.wal_object.payload.base_head_seq
        );
        assert_eq!(
            decoded_wal.payload.commit_id,
            expect.wal_object.payload.commit_id
        );
        assert_eq!(
            decoded_wal.payload.writer_fence_token,
            expect.wal_object.payload.writer_fence_token
        );
        assert_eq!(
            decoded_wal.payload.ops,
            vec![WalOp::CreateFile {
                inode_id: expect.wal_object.payload.create_file_inode_id,
                parent_inode: loon_types::InodeId(902),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:child-note".to_owned(),
            }]
        );
        assert_eq!(
            executed.response.namespace_id,
            loon_types::NamespaceId::from("ns-1")
        );
        assert_eq!(executed.response.client_request_id, "client-req-0001");
        assert_eq!(executed.response.committed_seq, loon_types::ChangeSeq(42));
        assert_eq!(
            executed.response.created_inode,
            loon_types::CreatedRemoteInode {
                inode_id: loon_types::InodeId(501),
                inode_kind: loon_types::InodeKind::File,
                revision_no: loon_types::RevisionNo(1),
                parent_inode_id: loon_types::InodeId(902),
                display_name: "note.txt".to_owned(),
                content_digest: Some("sha256:child-note".to_owned()),
            }
        );
        assert_eq!(executed.head_publish.resulting_head, expect.head);
        for invariant in &expect.invariants {
            assert!(
                executed.checked_invariants.contains(invariant),
                "missing invariant `{invariant}`"
            );
        }

        let stored_wal = store
            .get(&expect.wal_object.key, None)
            .expect("read stored WAL")
            .expect("stored WAL should exist");
        assert_eq!(stored_wal, executed.wal.encoded_bytes);

        let stored_head_bytes = store
            .get(&namespace_head(initial.head.namespace_id.as_str()), None)
            .expect("read stored head")
            .expect("stored head should exist");
        let stored_head: HeadStateEnvelope =
            serde_json::from_slice(&stored_head_bytes).expect("decode stored head");
        assert_eq!(stored_head.state, expect.head);
    }

    #[derive(Debug, Deserialize)]
    struct MutationInitial {
        head: HeadState,
        lease: LeaseState,
        client_request: RawClientMutationRequest,
    }

    #[derive(Debug, Deserialize)]
    struct MutationExpect {
        wal_object: ExpectedWalObject,
        head: HeadState,
        invariants: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    struct ExpectedWalObject {
        key: String,
        payload: ExpectedWalPayload,
    }

    #[derive(Debug, Deserialize)]
    struct ExpectedWalPayload {
        seq: loon_types::ChangeSeq,
        base_head_seq: loon_types::ChangeSeq,
        commit_id: String,
        writer_fence_token: loon_types::FenceToken,
        create_file_inode_id: loon_types::InodeId,
    }

    #[derive(Debug, Clone, Deserialize)]
    struct RawClientMutationRequest {
        namespace_id: loon_types::NamespaceId,
        client_request_id: String,
        op: RawClientMutationOp,
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(untagged)]
    enum RawClientMutationOp {
        CreateDir { create_dir: RawCreateDir },
        CreateFile { create_file: RawCreateFile },
    }

    #[derive(Debug, Clone, Deserialize)]
    struct RawCreateDir {
        parent_inode_id: loon_types::InodeId,
        display_name: String,
    }

    #[derive(Debug, Clone, Deserialize)]
    struct RawCreateFile {
        parent_inode_id: loon_types::InodeId,
        display_name: String,
        content_manifest_digest: String,
    }

    impl From<RawClientMutationRequest> for ClientMutationRequest {
        fn from(value: RawClientMutationRequest) -> Self {
            let op = match value.op {
                RawClientMutationOp::CreateDir { create_dir } => {
                    loon_types::ClientMutationOp::CreateDir {
                        parent_inode_id: create_dir.parent_inode_id,
                        display_name: create_dir.display_name,
                    }
                }
                RawClientMutationOp::CreateFile { create_file } => {
                    loon_types::ClientMutationOp::CreateFile {
                        parent_inode_id: create_file.parent_inode_id,
                        display_name: create_file.display_name,
                        content_manifest_digest: create_file.content_manifest_digest,
                    }
                }
            };

            Self {
                namespace_id: value.namespace_id,
                client_request_id: value.client_request_id,
                op,
            }
        }
    }

    fn load_fixture(relative_path: &str) -> Scenario {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/scenarios")
            .join(relative_path);
        Scenario::load(&path).unwrap_or_else(|err| panic!("load fixture {}: {err}", path.display()))
    }

    fn seed_head_and_lease(store: &LocalFsStore, head: &HeadState, lease: &LeaseState) {
        let head_envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
            "loon-server-test",
            head.clone(),
        )
        .expect("encode head envelope");
        let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize head envelope");
        store
            .put_if_absent(&namespace_head(head.namespace_id.as_str()), &head_bytes)
            .expect("seed head object");

        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "loon-server-test",
            lease.clone(),
        )
        .expect("encode lease envelope");
        let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize lease envelope");
        store
            .put_if_absent(&namespace_lease(lease.namespace_id.as_str()), &lease_bytes)
            .expect("seed lease object");
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
                "loondb-server-{label}-{}-{stamp}",
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
