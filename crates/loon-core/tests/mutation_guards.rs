#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use loon_api::{
    sha256_digest,
    v0::{
        CommitDelta, CommitOp as ApiCommitOp, CommitPrecondition,
        CommitRequest as ApiCommitRequest, CompleteUploadRequest,
    },
    wire::control::{
        ContentStoreDescriptorEnvelope, ControlObjectKind, HeadState, LeaseState,
        NamespaceDescriptorEnvelope, NamespaceDescriptorState, NamespaceForkStateEnvelope,
        NamespaceGcPinStateEnvelope,
    },
    wire::manifest::decode_namespace_manifest_json,
    wire::wal::{decode_wal_segment_envelope_zstd, WalDelta},
    AbsolutePath, ChangeSeq, CommitId, ContentRef, ContentRefKind, FenceToken, InodeId, InodeKind,
    ManifestId, NameKey, NamespaceId, RevisionNo,
};
use loon_core::cache::{load_verified_namespace_basis, BasisLoadError, VerifiedNamespaceBasis};
use loon_core::commit::{
    build_commit_plan, materialize_commit, CommitOp, CommitRequest, CommitValidationContext,
    CommitValidationError, PreparedCommit,
};
use loon_core::content::store_bytes_as_content;
use loon_core::control::load_namespace_head_control;
use loon_core::metadata::MetadataState;
use loon_core::publish::{
    DirectObjectStorePublisher, NamespaceCommitEngine, NamespaceMutationCandidate,
    PathMutationIntent, PublishOptions,
};
use loon_core::{
    BootstrapOptions, Error as CoreError, ErrorCode, ForkOptions, MutationContext, NamespaceEngine,
    PutFileBehavior, ReadOptions, WriteOptions,
};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{
    content_blob, content_store_descriptor, gc_pin, namespace_descriptor, namespace_fork_state,
    namespace_head, namespace_lease, namespace_manifest, upload_session_prefix,
};
use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataReadSource {
    MaterializedTables,
    FullBasisFallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataRead<T> {
    value: T,
    source: MetadataReadSource,
}

fn namespace_engine<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> NamespaceEngine<&'a S> {
    NamespaceEngine::builder(store)
        .namespace(namespace_id.clone())
        .writer(context.writer_id.clone())
        .writer_version(context.writer_version.clone())
        .lease_duration_ms(context.lease_duration_ms)
        .build()
        .expect("test context should build namespace engine")
}

fn bootstrap_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    allow_existing: bool,
) -> Result<loon_api::NamespaceSummary, loon_core::BootstrapNamespaceError> {
    namespace_engine(store, namespace_id, context)
        .bootstrap_namespace(BootstrapOptions { allow_existing })
}

fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loon_api::NamespaceSummary, CoreError> {
    namespace_engine(store, source_namespace_id, context)
        .fork_namespace(new_namespace_id, ForkOptions::default())
}

fn list_namespaces<S: ObjectStore + ?Sized>(
    store: &S,
) -> Result<Vec<loon_api::NamespaceSummary>, CoreError> {
    loon_core::namespace::list_namespaces(store)
}

fn commit_operations<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: ApiCommitRequest,
    context: &MutationContext,
) -> Result<loon_api::v0::CommitResponse, CoreError> {
    publish_namespace_mutations_batch(
        store,
        namespace_id,
        vec![NamespaceMutationCandidate::Commit(request)],
        context,
    )
    .into_iter()
    .next()
    .expect("single commit result")
}

fn commit_operations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    requests: Vec<ApiCommitRequest>,
    context: &MutationContext,
) -> Vec<Result<loon_api::v0::CommitResponse, CoreError>> {
    publish_namespace_mutations_batch(
        store,
        namespace_id,
        requests
            .into_iter()
            .map(NamespaceMutationCandidate::Commit)
            .collect(),
        context,
    )
}

fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<loon_api::v0::CommitResponse, CoreError>> {
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    engine.publish_batch(store, candidates, context).results
}

fn begin_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loon_api::v0::BeginUploadResponse, CoreError> {
    namespace_engine(store, namespace_id, context).begin_upload()
}

fn upload_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
    bytes: &[u8],
    context: &MutationContext,
) -> Result<loon_api::v0::UploadContentResponse, CoreError> {
    namespace_engine(store, namespace_id, context).upload_content(upload_id, bytes)
}

fn complete_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
    request: &CompleteUploadRequest,
    context: &MutationContext,
) -> Result<loon_api::v0::CompleteUploadResponse, CoreError> {
    namespace_engine(store, namespace_id, context).complete_upload(upload_id, request)
}

fn list_changes_after<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after_seq: ChangeSeq,
) -> Result<loon_api::v0::ChangesResponse, CoreError> {
    namespace_engine(store, namespace_id, &mutation_context()).list_changes_after(after_seq)
}

fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loon_api::CreateCheckpointResponse, CoreError> {
    namespace_engine(store, namespace_id, context).create_checkpoint()
}

fn write_options(commit_id: Option<&str>, behavior: PutFileBehavior) -> WriteOptions {
    WriteOptions {
        commit_id: commit_id.map(|value| CommitId::parse(value).expect("valid test commit id")),
        put_file_behavior: behavior,
        ..WriteOptions::default()
    }
}

fn put_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    behavior: PutFileBehavior,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loon_api::MutationResult, CoreError> {
    namespace_engine(store, namespace_id, context).put_file(
        absolute_path,
        bytes,
        write_options(commit_id, behavior),
    )
}

fn write_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loon_api::MutationResult, CoreError> {
    put_file_bytes(
        store,
        namespace_id,
        absolute_path,
        bytes,
        PutFileBehavior::ReplaceExisting,
        context,
        commit_id,
    )
}

fn create_dir_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loon_api::MutationResult, CoreError> {
    namespace_engine(store, namespace_id, context).create_dir(
        absolute_path,
        WriteOptions {
            commit_id: commit_id.map(|value| CommitId::parse(value).expect("valid test commit id")),
            ..WriteOptions::default()
        },
    )
}

fn delete_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loon_api::MutationResult, CoreError> {
    namespace_engine(store, namespace_id, context).delete_path(
        absolute_path,
        WriteOptions {
            commit_id: commit_id.map(|value| CommitId::parse(value).expect("valid test commit id")),
            recursive_delete: true,
            ..WriteOptions::default()
        },
    )
}

fn delete_path_non_recursive<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loon_api::MutationResult, CoreError> {
    namespace_engine(store, namespace_id, context).delete_path(
        absolute_path,
        WriteOptions {
            commit_id: commit_id.map(|value| CommitId::parse(value).expect("valid test commit id")),
            recursive_delete: false,
            ..WriteOptions::default()
        },
    )
}

fn move_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loon_api::MutationResult, CoreError> {
    namespace_engine(store, namespace_id, context).move_path(
        from_path,
        to_path,
        WriteOptions {
            commit_id: commit_id.map(|value| CommitId::parse(value).expect("valid test commit id")),
            ..WriteOptions::default()
        },
    )
}

fn copy_file_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loon_api::MutationResult, CoreError> {
    namespace_engine(store, namespace_id, context).copy_path(
        from_path,
        to_path,
        WriteOptions {
            commit_id: commit_id.map(|value| CommitId::parse(value).expect("valid test commit id")),
            ..WriteOptions::default()
        },
    )
}

fn restore_file_revision<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    source_revision_no: RevisionNo,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loon_api::MutationResult, CoreError> {
    namespace_engine(store, namespace_id, context).restore_file_revision(
        absolute_path,
        source_revision_no,
        WriteOptions {
            commit_id: commit_id.map(|value| CommitId::parse(value).expect("valid test commit id")),
            ..WriteOptions::default()
        },
    )
}

fn resolve_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loon_api::AuthoritativePathEntry, CoreError> {
    namespace_engine(store, namespace_id, &mutation_context())
        .resolve_path(absolute_path, ReadOptions::default())
}

fn list_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<loon_api::AuthoritativePathEntry>, CoreError> {
    namespace_engine(store, namespace_id, &mutation_context())
        .list_path(absolute_path, ReadOptions::default())
}

fn read_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loon_api::AuthoritativeFileBytes, CoreError> {
    namespace_engine(store, namespace_id, &mutation_context())
        .read_file(absolute_path, ReadOptions::default())
}

fn list_file_revisions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loon_api::ListFileRevisionsResponse, CoreError> {
    namespace_engine(store, namespace_id, &mutation_context())
        .list_file_revisions(absolute_path, ReadOptions::default())
}

fn list_file_revisions_for_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<loon_api::ListFileRevisionsResponse, CoreError> {
    namespace_engine(store, namespace_id, &mutation_context())
        .list_file_revisions_for_inode(inode_id, ReadOptions::default())
}

fn read_file_revision_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    revision_no: RevisionNo,
) -> Result<loon_api::AuthoritativeFileBytes, CoreError> {
    namespace_engine(store, namespace_id, &mutation_context()).read_file_revision(
        absolute_path,
        revision_no,
        ReadOptions::default(),
    )
}

fn read_file_revision_bytes_for_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Vec<u8>, CoreError> {
    namespace_engine(store, namespace_id, &mutation_context()).read_file_revision_for_inode(
        inode_id,
        revision_no,
        ReadOptions::default(),
    )
}

fn resolve_path_using_basis_option(
    basis: &VerifiedNamespaceBasis,
    absolute_path: &str,
) -> Result<loon_api::AuthoritativePathEntry, CoreError> {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    NamespaceEngine::builder(&store)
        .namespace(basis.head.namespace_id.clone())
        .writer("test")
        .build()
        .expect("test engine")
        .resolve_path(
            absolute_path,
            ReadOptions::verified_basis(Arc::new(basis.clone())),
        )
}

fn list_path_using_basis_option(
    basis: &VerifiedNamespaceBasis,
    absolute_path: &str,
) -> Result<Vec<loon_api::AuthoritativePathEntry>, CoreError> {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    NamespaceEngine::builder(&store)
        .namespace(basis.head.namespace_id.clone())
        .writer("test")
        .build()
        .expect("test engine")
        .list_path(
            absolute_path,
            ReadOptions::verified_basis(Arc::new(basis.clone())),
        )
}

fn resolve_path_with_read_source<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<MetadataRead<loon_api::AuthoritativePathEntry>, CoreError> {
    let engine = namespace_engine(store, namespace_id, &mutation_context());
    let head = load_namespace_head_control(store, namespace_id)
        .map_err(BasisLoadError::LoadHead)?
        .state;
    if head.current_manifest_id.is_some() {
        let value = engine.resolve_path(
            absolute_path,
            ReadOptions::materialized_tables_at_head(head.clone(), None),
        )?;
        return Ok(MetadataRead {
            value,
            source: MetadataReadSource::MaterializedTables,
        });
    }
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    Ok(MetadataRead {
        value: engine.resolve_path(absolute_path, ReadOptions::verified_basis(Arc::new(basis)))?,
        source: MetadataReadSource::FullBasisFallback,
    })
}

fn list_path_with_read_source<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<MetadataRead<Vec<loon_api::AuthoritativePathEntry>>, CoreError> {
    let engine = namespace_engine(store, namespace_id, &mutation_context());
    let head = load_namespace_head_control(store, namespace_id)
        .map_err(BasisLoadError::LoadHead)?
        .state;
    if head.current_manifest_id.is_some() {
        let value = engine.list_path(
            absolute_path,
            ReadOptions::materialized_tables_at_head(head.clone(), None),
        )?;
        return Ok(MetadataRead {
            value,
            source: MetadataReadSource::MaterializedTables,
        });
    }
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    Ok(MetadataRead {
        value: engine.list_path(absolute_path, ReadOptions::verified_basis(Arc::new(basis)))?,
        source: MetadataReadSource::FullBasisFallback,
    })
}

fn wal_create_dir(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode: InodeId,
    display_name: String,
) -> Vec<WalDelta> {
    vec![
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind: InodeKind::Dir,
        },
        WalDelta::BindDirentry {
            delta_index: delta_index.saturating_add(1),
            parent_inode,
            name_key: loon_api::name_key_for_display_name(
                loon_api::NamePolicy::default(),
                &display_name,
            ),
            display_name,
            child_inode: inode_id,
        },
    ]
}

fn wal_create_file(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode: InodeId,
    display_name: String,
    content_ref: ContentRef,
) -> Vec<WalDelta> {
    vec![
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind: InodeKind::File,
        },
        WalDelta::BindDirentry {
            delta_index: delta_index.saturating_add(1),
            parent_inode,
            name_key: loon_api::name_key_for_display_name(
                loon_api::NamePolicy::default(),
                &display_name,
            ),
            display_name,
            child_inode: inode_id,
        },
        WalDelta::AppendFileRevision {
            delta_index: delta_index.saturating_add(2),
            inode_id,
            revision_no: RevisionNo(1),
            content_ref,
        },
    ]
}

fn wal_append_revision(
    delta_index: u32,
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: ContentRef,
) -> Vec<WalDelta> {
    vec![WalDelta::AppendFileRevision {
        delta_index,
        inode_id,
        revision_no,
        content_ref,
    }]
}

fn wal_tombstone(delta_index: u32, root_inode: InodeId) -> Vec<WalDelta> {
    vec![WalDelta::TombstoneSubtree {
        delta_index,
        root_inode,
    }]
}

#[test]
fn stale_revision_precondition_is_rejected() {
    let metadata_state = metadata_state_after(&[
        wal_create_dir(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        commit_id: CommitId::parse("stale-revision").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        ops: vec![CommitOp::ReplaceFile {
            inode_id: InodeId(3),
            base_revision_no: RevisionNo(1),
            content_ref: content_ref("content-3"),
        }],
        preconditions: Vec::new(),
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("stale revision");
    assert!(matches!(
        error,
        CommitValidationError::ReplaceFileBaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    ));
}

#[test]
fn failed_multi_op_plan_uses_preview_without_mutating_base_metadata() {
    let metadata_state = metadata_state_after(&[]);
    let context = validation_context(&metadata_state, ChangeSeq(0), InodeId(2));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        commit_id: CommitId::parse("preview-rollback").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        ops: vec![
            CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            },
            CommitOp::CreateFile {
                parent_inode: InodeId(2),
                display_name: "readme.txt".to_owned(),
                content_ref: content_ref("content-1"),
            },
            CommitOp::ReplaceFile {
                inode_id: InodeId(99),
                base_revision_no: RevisionNo(1),
                content_ref: content_ref("content-2"),
            },
        ],
        preconditions: Vec::new(),
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("late op fails");
    assert!(matches!(
        error,
        CommitValidationError::ReplaceFileInodeMissing {
            inode_id: InodeId(99)
        }
    ));
    assert!(metadata_state
        .visible_child(InodeId(1), "docs", ChangeSeq(1))
        .is_none());
}

#[test]
fn create_and_replace_under_ancestor_tombstone_are_rejected() {
    let metadata_state = metadata_state_after(&[
        wal_create_dir(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_tombstone(0, InodeId(2)),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let create_error = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            commit_id: CommitId::parse("create-under-tombstone").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(2),
                display_name: "new.txt".to_owned(),
                content_ref: content_ref("content-2"),
            }],
            preconditions: Vec::new(),
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("create under tombstone");
    assert!(matches!(
        create_error,
        CommitValidationError::CreateUnderSubtreeTombstone {
            parent_inode: InodeId(2),
            ..
        }
    ));

    let replace_error = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            commit_id: CommitId::parse("replace-under-tombstone").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(3),
                base_revision_no: RevisionNo(1),
                content_ref: content_ref("content-2"),
            }],
            preconditions: Vec::new(),
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("replace under tombstone");
    assert!(matches!(
        replace_error,
        CommitValidationError::ReplaceFileUnderSubtreeTombstone {
            inode_id: InodeId(3),
            ..
        }
    ));
}

#[test]
fn restore_revision_validation_rejects_missing_inode() {
    let metadata_state =
        metadata_state_after(&[wal_create_dir(0, InodeId(2), InodeId(1), "docs".to_owned())]);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        commit_id: CommitId::parse("restore-missing-inode").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(99),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
        }],
        preconditions: Vec::new(),
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("restore missing inode");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionInodeMissing {
            inode_id: InodeId(99),
        }
    ));
}

#[test]
fn restore_revision_validation_rejects_non_file_target() {
    let metadata_state =
        metadata_state_after(&[wal_create_dir(0, InodeId(2), InodeId(1), "docs".to_owned())]);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        commit_id: CommitId::parse("restore-non-file").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(2),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
        }],
        preconditions: Vec::new(),
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("restore non-file");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionInodeNotFile {
            inode_id: InodeId(2),
            actual_kind: InodeKind::Dir,
        }
    ));
}

#[test]
fn restore_revision_validation_rejects_stale_or_missing_source_revision() {
    let metadata_state = metadata_state_after(&[
        wal_create_dir(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let stale_base = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            commit_id: CommitId::parse("restore-stale-base").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(1),
            }],
            preconditions: Vec::new(),
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("restore stale base");
    assert!(matches!(
        stale_base,
        CommitValidationError::RestoreRevisionBaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    ));

    let missing_source = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            commit_id: CommitId::parse("restore-missing-source").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(99),
                base_revision_no: RevisionNo(2),
            }],
            preconditions: Vec::new(),
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("restore missing source");
    assert!(matches!(
        missing_source,
        CommitValidationError::RestoreRevisionSourceRevisionMissing {
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(99),
        }
    ));
}

#[test]
fn restore_revision_can_reference_revision_created_earlier_in_same_request() {
    let metadata_state = metadata_state_after(&[
        wal_create_dir(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(2), InodeId(4));

    let request = CommitRequest {
        namespace_id: namespace_id(),
        commit_id: CommitId::parse("restore-same-request-source").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        ops: vec![
            CommitOp::ReplaceFile {
                inode_id: InodeId(3),
                base_revision_no: RevisionNo(1),
                content_ref: content_ref("content-2"),
            },
            CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(2),
                base_revision_no: RevisionNo(2),
            },
        ],
        preconditions: Vec::new(),
        message: None,
        annotations: None,
    };
    let plan = build_commit_plan(&request, &context)
        .expect("replace then restore in same request should validate");
    let materialized =
        materialize_commit(PreparedCommit::new(request, plan).expect("prepare commit"));
    let expected = content_ref("content-2");
    assert!(matches!(
        &materialized.results[1],
        loon_api::v0::CommitOpResult::RestoreRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
}

#[test]
fn restore_revision_can_reference_restore_created_earlier_in_same_request() {
    let metadata_state = metadata_state_after(&[
        wal_create_dir(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let request = CommitRequest {
        namespace_id: namespace_id(),
        commit_id: CommitId::parse("restore-after-restore-same-request").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        ops: vec![
            CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(2),
            },
            CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(3),
                base_revision_no: RevisionNo(3),
            },
        ],
        preconditions: Vec::new(),
        message: None,
        annotations: None,
    };
    let plan = build_commit_plan(&request, &context)
        .expect("restore then restore in same request should validate");
    let materialized =
        materialize_commit(PreparedCommit::new(request, plan).expect("prepare commit"));
    let expected = content_ref("content-1");
    assert!(matches!(
        &materialized.results[0],
        loon_api::v0::CommitOpResult::RestoreRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
    assert!(matches!(
        &materialized.results[1],
        loon_api::v0::CommitOpResult::RestoreRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
}

#[test]
fn restore_revision_under_tombstoned_ancestor_is_rejected() {
    let metadata_state = metadata_state_after(&[
        wal_create_dir(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_tombstone(0, InodeId(2)),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let error = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id(),
            commit_id: CommitId::parse("restore-under-tombstone").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(1),
            }],
            preconditions: Vec::new(),
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("restore tombstone conflict");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionUnderSubtreeTombstone {
            inode_id: InodeId(3),
            ..
        }
    ));
}

#[test]
fn restore_revision_overflow_is_rejected() {
    let mut deltas = wal_create_file(
        0,
        InodeId(2),
        InodeId(1),
        "overflow.txt".to_owned(),
        content_ref("content-max"),
    );
    deltas[2] = WalDelta::AppendFileRevision {
        delta_index: 2,
        inode_id: InodeId(2),
        revision_no: RevisionNo(u64::MAX),
        content_ref: content_ref("content-max"),
    };
    let metadata_state = MetadataState::default()
        .apply_committed_wal_deltas(
            ChangeSeq(0),
            &[WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
            }],
        )
        .expect("bootstrap root")
        .metadata_state
        .apply_committed_wal_deltas(ChangeSeq(1), &deltas)
        .expect("create max revision")
        .metadata_state;
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = CommitRequest {
        namespace_id: namespace_id(),
        commit_id: CommitId::parse("restore-overflow").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(1),
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(2),
            source_revision_no: RevisionNo(u64::MAX),
            base_revision_no: RevisionNo(u64::MAX),
        }],
        preconditions: Vec::new(),
        message: None,
        annotations: None,
    };

    let error = build_commit_plan(&request, &context).expect_err("restore overflow");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionOverflow {
            inode_id: InodeId(2),
            base_revision_no: RevisionNo(u64::MAX),
        }
    ));
}

#[test]
fn namespace_creation_writes_descriptors_and_listing_uses_completion_marker() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap namespace");
    let duplicate_error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .expect_err("complete namespace id reuse should be rejected");
    assert!(matches!(
        duplicate_error,
        loon_core::BootstrapNamespaceError::NamespaceAlreadyExists { .. }
    ));
    let existing =
        bootstrap_namespace(&store, &namespace_id, &context, true).expect("allow existing");
    assert_eq!(existing.namespace_id, namespace_id);

    let basis = load_verified_namespace_basis(&store, &namespace_id).expect("load namespace basis");
    let descriptor_key = namespace_descriptor(namespace_id.as_str());
    let descriptor_bytes = store
        .get(&descriptor_key, None)
        .expect("read namespace descriptor")
        .expect("namespace descriptor exists");
    let descriptor: NamespaceDescriptorEnvelope =
        serde_json::from_slice(&descriptor_bytes).expect("decode namespace descriptor");
    assert_eq!(descriptor.kind, ControlObjectKind::NamespaceDescriptor);
    assert_eq!(descriptor.state.namespace_id, namespace_id);
    assert_eq!(descriptor.state.content_store_id, basis.content_store_id);
    assert!(descriptor.has_valid_payload_checksum().expect("checksum"));
    assert!(
        store
            .head(&namespace_fork_state(namespace_id.as_str()))
            .expect("head root fork state")
            .is_none(),
        "root namespace creation must not write fork provenance"
    );

    let content_descriptor_key = content_store_descriptor(basis.content_store_id.as_str());
    let content_descriptor_bytes = store
        .get(&content_descriptor_key, None)
        .expect("read content-store descriptor")
        .expect("content-store descriptor exists");
    let content_descriptor: ContentStoreDescriptorEnvelope =
        serde_json::from_slice(&content_descriptor_bytes).expect("decode content-store descriptor");
    assert_eq!(
        content_descriptor.kind,
        ControlObjectKind::ContentStoreDescriptor
    );
    assert_eq!(
        content_descriptor.state.content_store_id,
        basis.content_store_id
    );
    assert!(content_descriptor
        .has_valid_payload_checksum()
        .expect("checksum"));

    let content_store_descriptors = store
        .list_prefix("content-stores/")
        .expect("list content stores");
    assert_eq!(
        content_store_descriptors,
        vec![content_descriptor_key],
        "new root namespace should create exactly one content store descriptor"
    );

    store
        .put_if_absent(&namespace_head("partial"), br#"{"not":"a descriptor"}"#)
        .expect("write partial namespace key");
    let listed = list_namespaces(&store).expect("list namespaces");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].namespace_id.as_str(), "demo");

    let partial_error = bootstrap_namespace(
        &store,
        &NamespaceId::parse("partial").expect("valid namespace id"),
        &context,
        false,
    )
    .expect_err("partial namespace should be rejected");
    assert!(matches!(
        partial_error,
        loon_core::BootstrapNamespaceError::NamespacePartiallyInitialized { .. }
    ));
}

#[test]
fn bootstrap_head_reservation_failure_does_not_allocate_content_store() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(namespace_head(namespace_id.as_str())),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: true,
            additional_writes: Vec::new(),
        },
    );

    let error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .expect_err("target head precondition should fail bootstrap");
    assert!(matches!(
        error,
        loon_core::BootstrapNamespaceError::HeadWrite(_)
    ));
    assert!(
        store
            .list_prefix("content-stores/")
            .expect("list content stores")
            .is_empty(),
        "content-store descriptor must not be allocated before namespace head reservation"
    );
    assert_namespace_partial(&store, &namespace_id, &context);
}

#[test]
fn begin_upload_rejects_missing_and_partial_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    let missing_error =
        begin_upload(&store, &namespace_id, &context).expect_err("missing namespace");
    assert_eq!(missing_error.code(), ErrorCode::NamespaceNotFound);

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    store
        .delete(&namespace_descriptor(namespace_id.as_str()))
        .expect("delete descriptor");

    let partial_error =
        begin_upload(&store, &namespace_id, &context).expect_err("partial namespace");
    assert_eq!(partial_error.code(), ErrorCode::NamespacePartial);
}

#[test]
fn begin_upload_does_not_read_manifest_or_wal_replay_objects() {
    let temp_dir = tempdir().expect("tempdir");
    let setup_store = LocalFsStore::new(temp_dir.path()).expect("setup store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&setup_store, &namespace_id, &context, false).expect("bootstrap");
    put_file_bytes(
        &setup_store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileBehavior::CreateOnly,
        &context,
        Some("upload-guard-create"),
    )
    .expect("create file");
    create_checkpoint(&setup_store, &namespace_id, &context).expect("checkpoint");
    put_file_bytes(
        &setup_store,
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        PutFileBehavior::ReplaceExisting,
        &context,
        Some("upload-guard-replace"),
    )
    .expect("replace file");

    let guarded_store = ReplayReadGuardStore::new(temp_dir.path(), namespace_id.as_str());
    let begin = begin_upload(&guarded_store, &namespace_id, &context).expect("begin upload");
    assert_eq!(begin.namespace_id, namespace_id);
    assert_eq!(guarded_store.guarded_get_count(), 0);
}

#[test]
fn complete_upload_does_not_get_content_blob_after_staging() {
    let temp_dir = tempdir().expect("tempdir");
    let store = ContentBlobGetCountingStore::new(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let begin = begin_upload(&store, &namespace_id, &context).expect("begin upload");
    let uploaded = upload_content(&store, &namespace_id, &begin.upload_id, b"hello", &context)
        .expect("upload content");

    store.reset_content_blob_get_count();
    let completed = complete_upload(
        &store,
        &namespace_id,
        &begin.upload_id,
        &CompleteUploadRequest {
            content_ref: uploaded.content_ref.clone(),
        },
        &context,
    )
    .expect("complete upload");
    assert_eq!(completed.content_ref, uploaded.content_ref);
    assert_eq!(store.content_blob_get_count(), 0);

    store.reset_content_blob_get_count();
    let completed_again = complete_upload(
        &store,
        &namespace_id,
        &begin.upload_id,
        &CompleteUploadRequest {
            content_ref: uploaded.content_ref,
        },
        &context,
    )
    .expect("complete upload idempotently");
    assert_eq!(completed_again.content_ref, completed.content_ref);
    assert_eq!(store.content_blob_get_count(), 0);

    let mismatch_begin = begin_upload(&store, &namespace_id, &context).expect("begin mismatch");
    let mismatch_uploaded = upload_content(
        &store,
        &namespace_id,
        &mismatch_begin.upload_id,
        b"staged",
        &context,
    )
    .expect("upload mismatch content");
    let wrong_ref = ContentRef::whole_file_v0(b"different");
    assert_ne!(wrong_ref, mismatch_uploaded.content_ref);

    store.reset_content_blob_get_count();
    let mismatch = complete_upload(
        &store,
        &namespace_id,
        &mismatch_begin.upload_id,
        &CompleteUploadRequest {
            content_ref: wrong_ref,
        },
        &context,
    )
    .expect_err("mismatched content ref");
    assert_eq!(mismatch.code(), ErrorCode::InvalidUploadContent);
    assert_eq!(store.content_blob_get_count(), 0);
}

#[test]
fn path_put_file_uses_checksum_metadata_for_content_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = ContentBlobGetCountingStore::new(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"hello").expect("stage content");

    store.reset_content_blob_get_count();
    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![NamespaceMutationCandidate::Path(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-cold-content").expect("valid commit id"),
                absolute_path: "/docs/hello.txt".to_owned(),
                content_ref: content.content_ref,
                behavior: PutFileBehavior::CreateOnly,
            },
        )],
        &context,
    );

    assert!(responses[0].is_ok());
    assert_eq!(store.content_blob_get_count(), 0);
}

#[test]
fn path_planning_does_not_validate_content() {
    let temp_dir = tempdir().expect("tempdir");
    let store = ContentBlobGetCountingStore::new(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"planned").expect("stage content");

    store.reset_content_blob_get_count();
    let planned = DirectObjectStorePublisher::new(&store)
        .plan_path_intent(
            &namespace_id,
            &PathMutationIntent::PutFile {
                commit_id: CommitId::parse("plan-put-content").expect("valid commit id"),
                absolute_path: "/docs/planned.txt".to_owned(),
                content_ref: content.content_ref,
                behavior: PutFileBehavior::CreateOnly,
            },
        )
        .expect("plan path intent");

    assert_eq!(
        planned.commit_id,
        CommitId::parse("plan-put-content").expect("valid commit id")
    );
    assert_eq!(store.content_blob_get_count(), 0);
}

#[test]
fn path_batch_validates_repeated_content_ref_without_blob_gets() {
    let temp_dir = tempdir().expect("tempdir");
    let store = ContentBlobGetCountingStore::new(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"shared").expect("stage content");

    store.reset_content_blob_get_count();
    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![
            NamespaceMutationCandidate::Path(PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-shared-a").expect("valid commit id"),
                absolute_path: "/docs/a.txt".to_owned(),
                content_ref: content.content_ref.clone(),
                behavior: PutFileBehavior::CreateOnly,
            }),
            NamespaceMutationCandidate::Path(PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-shared-b").expect("valid commit id"),
                absolute_path: "/docs/b.txt".to_owned(),
                content_ref: content.content_ref,
                behavior: PutFileBehavior::CreateOnly,
            }),
        ],
        &context,
    );

    assert!(responses.iter().all(Result::is_ok));
    assert_eq!(store.content_blob_get_count(), 0);
}

#[test]
fn metadata_queries_do_not_get_content_blobs_but_file_reads_do_once() {
    let temp_dir = tempdir().expect("tempdir");
    let store = ContentBlobGetCountingStore::new(temp_dir.path());
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    create_dir_path(&store, &namespace_id, "/docs", &context, Some("mkdir-docs"))
        .expect("create docs");

    for index in 0..3 {
        let path = format!("/docs/file-{index}.txt");
        let bytes = format!("file-{index}-bytes");
        let commit_id = format!("put-file-{index}");
        put_file_bytes(
            &store,
            &namespace_id,
            &path,
            bytes.as_bytes(),
            PutFileBehavior::CreateOnly,
            &context,
            Some(&commit_id),
        )
        .expect("put file");
    }

    store.reset_content_blob_get_count();
    let stat = resolve_path(&store, &namespace_id, "/docs/file-1.txt").expect("stat file");
    assert_eq!(stat.inode_kind, InodeKind::File);
    assert_eq!(stat.size_bytes, Some("file-1-bytes".len() as u64));
    assert!(stat.content_ref.is_some());
    assert_eq!(store.content_blob_get_count(), 0);

    store.reset_content_blob_get_count();
    let entries = list_path(&store, &namespace_id, "/docs").expect("list docs");
    assert_eq!(entries.len(), 3);
    for entry in entries {
        assert_eq!(entry.inode_kind, InodeKind::File);
        assert_eq!(entry.size_bytes, Some("file-0-bytes".len() as u64));
        assert!(entry.content_ref.is_some());
    }
    assert_eq!(store.content_blob_get_count(), 0);

    store.reset_content_blob_get_count();
    let read = read_file_bytes(&store, &namespace_id, "/docs/file-1.txt").expect("read file");
    assert_eq!(read.bytes, b"file-1-bytes");
    assert_eq!(store.content_blob_get_count(), 1);
}

#[test]
fn query_driven_reads_fallback_without_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileBehavior::CreateOnly,
        &context,
        Some("put-file"),
    )
    .expect("put file");

    let stat = resolve_path_with_read_source(&store, &namespace_id, "/docs/file.txt")
        .expect("stat with fallback");
    let list =
        list_path_with_read_source(&store, &namespace_id, "/docs").expect("list with fallback");

    assert_eq!(stat.source, MetadataReadSource::FullBasisFallback);
    assert_eq!(list.source, MetadataReadSource::FullBasisFallback);
    assert_eq!(stat.value.size_bytes, Some(4));
    assert_eq!(list.value.len(), 1);
}

#[test]
fn query_driven_stat_and_list_match_full_basis_with_l0_run_and_wal_overlay() {
    let temp_dir = tempdir().expect("tempdir");
    let store = ContentBlobGetCountingStore::new(temp_dir.path());
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"alpha",
        PutFileBehavior::CreateOnly,
        &context,
        Some("put-alpha"),
    )
    .expect("put alpha");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/b.txt",
        b"bravo",
        PutFileBehavior::CreateOnly,
        &context,
        Some("put-bravo"),
    )
    .expect("put bravo");
    put_file_bytes(
        &store,
        &namespace_id,
        "/dead/leaf.txt",
        b"dead",
        PutFileBehavior::CreateOnly,
        &context,
        Some("put-dead"),
    )
    .expect("put dead");
    create_checkpoint(&store, &namespace_id, &context).expect("base checkpoint");

    move_path(
        &store,
        &namespace_id,
        "/docs/a.txt",
        "/docs/moved.txt",
        &context,
        Some("move-alpha"),
    )
    .expect("move alpha");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/b.txt",
        b"bravo two",
        PutFileBehavior::ReplaceExisting,
        &context,
        Some("replace-bravo"),
    )
    .expect("replace bravo");
    delete_path(
        &store,
        &namespace_id,
        "/dead",
        &context,
        Some("delete-dead"),
    )
    .expect("delete dead");
    create_checkpoint(&store, &namespace_id, &context).expect("l0 checkpoint");

    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/wal.txt",
        b"wal",
        PutFileBehavior::CreateOnly,
        &context,
        Some("put-wal"),
    )
    .expect("put wal tail");

    let basis = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
    let expected_stat =
        resolve_path_using_basis_option(&basis, "/docs/moved.txt").expect("basis stat");
    let expected_list = list_path_using_basis_option(&basis, "/docs").expect("basis list");
    let expected_file_list =
        list_path_using_basis_option(&basis, "/docs/moved.txt").expect("basis file list");

    store.reset_content_blob_get_count();
    let actual_stat = resolve_path_with_read_source(&store, &namespace_id, "/docs/moved.txt")
        .expect("materialized stat");
    let actual_list =
        list_path_with_read_source(&store, &namespace_id, "/docs").expect("materialized list");
    let actual_file_list = list_path_with_read_source(&store, &namespace_id, "/docs/moved.txt")
        .expect("materialized file list");
    let actual_casefold_stat =
        resolve_path_with_read_source(&store, &namespace_id, "/DOCS/MOVED.TXT")
            .expect("materialized casefold stat");

    assert_eq!(actual_stat.source, MetadataReadSource::MaterializedTables);
    assert_eq!(actual_list.source, MetadataReadSource::MaterializedTables);
    assert_eq!(
        actual_file_list.source,
        MetadataReadSource::MaterializedTables
    );
    assert_eq!(
        actual_casefold_stat.source,
        MetadataReadSource::MaterializedTables
    );
    assert_eq!(actual_stat.value, expected_stat);
    assert_eq!(actual_casefold_stat.value, expected_stat);
    assert_eq!(actual_list.value, expected_list);
    assert_eq!(actual_file_list.value, expected_file_list);
    assert_eq!(store.content_blob_get_count(), 0);

    assert!(resolve_path_with_read_source(&store, &namespace_id, "/docs/a.txt").is_err());
    assert!(resolve_path_with_read_source(&store, &namespace_id, "/dead/leaf.txt").is_err());
}

#[test]
fn query_driven_stat_uses_exact_name_key_for_dash_containing_siblings() {
    let temp_dir = tempdir().expect("tempdir");
    let store = ContentBlobGetCountingStore::new(temp_dir.path());
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/report",
        b"short",
        PutFileBehavior::CreateOnly,
        &context,
        Some("put-report"),
    )
    .expect("put report");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/report-2024",
        b"newer-longer",
        PutFileBehavior::CreateOnly,
        &context,
        Some("put-report-2024"),
    )
    .expect("put report-2024");
    create_checkpoint(&store, &namespace_id, &context).expect("checkpoint");

    let basis = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
    let expected = resolve_path_using_basis_option(&basis, "/docs/report").expect("basis stat");
    let actual = resolve_path_with_read_source(&store, &namespace_id, "/docs/report")
        .expect("materialized stat");

    assert_eq!(actual.source, MetadataReadSource::MaterializedTables);
    assert_eq!(actual.value, expected);
    assert_eq!(actual.value.absolute_path, "/docs/report");
    assert_eq!(actual.value.size_bytes, Some(5));
}

#[test]
fn upload_content_rejects_invalid_upload_id_before_key_construction() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

    let invalid_upload_id = ["upl", "123"].join("-");
    let error = upload_content(
        &store,
        &namespace_id,
        &invalid_upload_id,
        b"hello",
        &context,
    )
    .expect_err("invalid upload_id should be rejected");

    assert_eq!(error.code(), ErrorCode::InvalidUploadId);
    assert_eq!(
        store
            .list_prefix(&upload_session_prefix(namespace_id.as_str()))
            .expect("list upload sessions"),
        Vec::<String>::new()
    );
}

#[test]
fn revision_queries_read_historical_bytes_and_path_restore_appends_revision() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/rev.txt",
        b"one",
        PutFileBehavior::CreateOnly,
        &context,
        Some("rev-create"),
    )
    .expect("create file");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/rev.txt",
        b"two",
        &context,
        Some("rev-replace"),
    )
    .expect("replace file");

    let entry = resolve_path(&store, &namespace_id, "/docs/rev.txt").expect("stat file");
    let inode_id = entry.inode_id;
    let path_revisions =
        list_file_revisions(&store, &namespace_id, "/docs/rev.txt").expect("path revisions");
    assert_eq!(path_revisions.inode_id, inode_id);
    assert_eq!(
        path_revisions
            .revisions
            .iter()
            .map(|revision| revision.revision_no)
            .collect::<Vec<_>>(),
        vec![RevisionNo(1), RevisionNo(2)]
    );

    let historical =
        read_file_revision_bytes(&store, &namespace_id, "/docs/rev.txt", RevisionNo(1))
            .expect("read first revision");
    assert_eq!(historical.bytes, b"one");
    let inode_historical =
        read_file_revision_bytes_for_inode(&store, &namespace_id, inode_id, RevisionNo(2))
            .expect("read inode revision");
    assert_eq!(inode_historical, b"two");

    move_path(
        &store,
        &namespace_id,
        "/docs/rev.txt",
        "/docs/moved.txt",
        &context,
        Some("rev-move"),
    )
    .expect("move file");
    assert_eq!(
        list_file_revisions(&store, &namespace_id, "/docs/rev.txt")
            .expect_err("old path no longer resolves")
            .code(),
        ErrorCode::PathNotFound
    );
    let inode_revisions =
        list_file_revisions_for_inode(&store, &namespace_id, inode_id).expect("inode revisions");
    assert_eq!(inode_revisions.revisions.len(), 2);

    restore_file_revision(
        &store,
        &namespace_id,
        "/docs/moved.txt",
        RevisionNo(1),
        &context,
        Some("rev-restore"),
    )
    .expect("restore first revision");
    let restored =
        read_file_bytes(&store, &namespace_id, "/docs/moved.txt").expect("read restored");
    assert_eq!(restored.bytes, b"one");
    let restored_revisions =
        list_file_revisions_for_inode(&store, &namespace_id, inode_id).expect("restored revisions");
    assert_eq!(
        restored_revisions
            .revisions
            .iter()
            .map(|revision| revision.revision_no)
            .collect::<Vec<_>>(),
        vec![RevisionNo(1), RevisionNo(2), RevisionNo(3)]
    );
}

#[test]
fn batch_commit_writes_one_segment_and_expands_change_feed() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                commit_id: CommitId::parse("req-batch-a").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "alpha".to_owned(),
                }],
                message: None,
                annotations: None,
            },
            ApiCommitRequest {
                commit_id: CommitId::parse("req-batch-b").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "beta".to_owned(),
                }],
                message: None,
                annotations: None,
            },
        ],
        &context,
    );
    let first = responses[0].as_ref().expect("first commit");
    let second = responses[1].as_ref().expect("second commit");
    assert_eq!(first.committed_seq, ChangeSeq(1));
    assert_eq!(second.committed_seq, ChangeSeq(2));

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.start_seq, ChangeSeq(1));
    assert_eq!(segment.payload.end_seq, ChangeSeq(2));
    assert_eq!(segment.payload.records.len(), 2);
    assert_eq!(segment.payload.records[0].deltas.len(), 2);
    assert_eq!(segment.payload.records[0].deltas[0].semantic_op_index, 0);
    assert_eq!(segment.payload.records[0].deltas[1].semantic_op_index, 0);
    match &segment.payload.records[0].deltas[1].delta {
        WalDelta::BindDirentry {
            name_key,
            display_name,
            ..
        } => {
            assert_eq!(name_key, "alpha");
            assert_eq!(display_name, "alpha");
        }
        delta => panic!("expected bind delta, got {delta:?}"),
    }
    store
        .put_if_absent(
            "namespaces/demo/wal/seg_99999999999999999999999999999999.wal.zst",
            &wal_bytes,
        )
        .expect("write unreachable orphan");

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0)).expect("changes");
    assert_eq!(changes.changes.len(), 2);
    assert_eq!(
        changes.changes[0].commit_id,
        CommitId::parse("req-batch-a").expect("valid commit id")
    );
    assert_eq!(
        changes.changes[1].commit_id,
        CommitId::parse("req-batch-b").expect("valid commit id")
    );
    assert_eq!(changes.changes[0].ops, first.results);
    assert_eq!(changes.changes[0].deltas.len(), 2);
    assert!(matches!(
        &changes.changes[0].deltas[0],
        CommitDelta::CreateInode {
            semantic_op_index: 0,
            delta_index: 0,
            inode_kind: InodeKind::Dir,
            ..
        }
    ));
    assert!(matches!(
        &changes.changes[0].deltas[1],
        CommitDelta::BindDirentry {
            semantic_op_index: 0,
            delta_index: 1,
            name_key,
            display_name,
            ..
        } if name_key.as_str() == "alpha" && display_name == "alpha"
    ));
}

#[test]
fn change_feed_validates_wal_chain_before_current_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    create_dir_path(&store, &namespace_id, "/docs", &context, None).expect("create docs");
    create_checkpoint(&store, &namespace_id, &context).expect("checkpoint");

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    store
        .put_overwrite(&wal_keys[0], b"not a wal segment")
        .expect("corrupt wal");

    load_verified_namespace_basis(&store, &namespace_id)
        .expect("checkpoint-backed basis should not read pre-checkpoint wal");
    let error =
        list_changes_after(&store, &namespace_id, ChangeSeq(0)).expect_err("corrupt wal chain");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
}

#[test]
fn binding_is_precondition_observes_earlier_batch_candidate() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/readme.txt",
        b"hello",
        &context,
        Some("seed-child-name-is"),
    )
    .expect("seed file");
    let docs_inode = resolve_path(&store, &namespace_id, "/docs")
        .expect("resolve docs")
        .inode_id;
    let file_inode = resolve_path(&store, &namespace_id, "/docs/readme.txt")
        .expect("resolve file")
        .inode_id;
    let basis = load_verified_namespace_basis(&store, &namespace_id).expect("load basis");
    let original_binding = basis
        .metadata_state
        .current_parent_binding_for_child(file_inode, basis.head.seq)
        .expect("source binding");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                commit_id: CommitId::parse("move-before-child-name-check")
                    .expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::Rename {
                    inode_id: file_inode,
                    new_parent_inode: docs_inode,
                    new_display_name: "moved.txt".to_owned(),
                    mode: loon_api::v0::RenameMode::NoReplace,
                }],
                message: None,
                annotations: None,
            },
            ApiCommitRequest {
                commit_id: CommitId::parse("delete-with-stale-binding").expect("valid commit id"),
                preconditions: vec![CommitPrecondition::BindingIs {
                    parent_inode: docs_inode,
                    name_key: NameKey::try_new(original_binding.name_key).expect("valid name key"),
                    child_inode: file_inode,
                    bind_seq: original_binding.bind_seq,
                    bind_delta_index: original_binding.bind_delta_index,
                }],
                ops: vec![ApiCommitOp::DeleteFile {
                    inode_id: file_inode,
                }],
                message: None,
                annotations: None,
            },
        ],
        &context,
    );

    assert_eq!(
        responses[0].as_ref().expect("rename").committed_seq,
        ChangeSeq(2)
    );
    let error = responses[1]
        .as_ref()
        .expect_err("stale binding precondition");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[test]
fn directory_empty_precondition_observes_earlier_batch_candidate() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let seed = commit_operations(
        &store,
        &namespace_id,
        ApiCommitRequest {
            commit_id: CommitId::parse("seed-empty-dir").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("seed docs");
    let docs_inode = match seed.results[0] {
        loon_api::v0::CommitOpResult::CreateDir { inode_id, .. } => inode_id,
        ref other => panic!("unexpected seed result: {other:?}"),
    };
    let content = store_bytes_as_content(&store, &namespace_id, b"child").expect("stage content");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                commit_id: CommitId::parse("create-child-before-empty-check")
                    .expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateFile {
                    parent_inode: docs_inode,
                    display_name: "child.txt".to_owned(),
                    content_ref: content.content_ref,
                }],
                message: None,
                annotations: None,
            },
            ApiCommitRequest {
                commit_id: CommitId::parse("delete-dir-with-stale-empty-check")
                    .expect("valid commit id"),
                preconditions: vec![CommitPrecondition::DirectoryEmpty {
                    inode_id: docs_inode,
                }],
                ops: vec![ApiCommitOp::DeleteSubtree {
                    root_inode: docs_inode,
                }],
                message: None,
                annotations: None,
            },
        ],
        &context,
    );

    assert_eq!(
        responses[0].as_ref().expect("create child").committed_seq,
        ChangeSeq(2)
    );
    let error = responses[1]
        .as_ref()
        .expect_err("directory empty precondition");
    assert_eq!(error.code(), ErrorCode::DirectoryNotEmpty);
}

#[test]
fn direct_publisher_retries_after_wal_orphaned_by_stale_head_cas() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = StaleHeadAfterWalWriteStore::new(temp_dir.path(), &namespace_id);
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"retry").expect("stage content");
    let publisher = DirectObjectStorePublisher::new(&store);

    let result = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("retry-after-orphan").expect("valid commit id"),
                absolute_path: "/retry.txt".to_owned(),
                content_ref: content.content_ref,
                behavior: PutFileBehavior::CreateOnly,
            },
            &context,
            PublishOptions::default(),
        )
        .expect("path intent retries stale head");
    assert_eq!(result.committed_seq, ChangeSeq(1));
    assert!(store.injected_stale_head());

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 2);

    let basis = load_verified_namespace_basis(&store, &namespace_id).expect("load basis");
    assert_eq!(basis.head.seq, ChangeSeq(1));
    let visible_tip = basis
        .head
        .visible_wal_tip
        .as_ref()
        .expect("visible wal tip");
    assert!(wal_keys.contains(&visible_tip.object_key));
    let orphan_keys = wal_keys
        .iter()
        .filter(|key| *key != &visible_tip.object_key)
        .collect::<Vec<_>>();
    assert_eq!(orphan_keys.len(), 1);

    let visible_wal = store
        .get(&visible_tip.object_key, None)
        .expect("read visible wal")
        .expect("visible wal exists");
    let visible_segment =
        decode_wal_segment_envelope_zstd(&visible_wal).expect("decode visible segment");
    assert_eq!(visible_segment.payload.start_seq, ChangeSeq(1));
    assert_eq!(visible_segment.payload.end_seq, ChangeSeq(1));
    assert_eq!(visible_segment.payload.records.len(), 1);
    assert_eq!(
        visible_segment.payload.records[0].commit_id,
        CommitId::parse("retry-after-orphan").expect("valid commit id")
    );

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0)).expect("changes");
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(
        changes.changes[0].commit_id,
        CommitId::parse("retry-after-orphan").expect("valid commit id")
    );
}

#[test]
fn batch_commit_aliases_duplicate_commit_id_with_same_fingerprint() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

    let request = ApiCommitRequest {
        commit_id: CommitId::parse("req-duplicate").expect("valid commit id"),
        preconditions: Vec::new(),
        ops: vec![ApiCommitOp::CreateDir {
            parent_inode: InodeId(1),
            display_name: "alpha".to_owned(),
        }],
        message: None,
        annotations: None,
    };

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![request.clone(), request],
        &context,
    );
    let first = responses[0].as_ref().expect("primary commit");
    let duplicate = responses[1].as_ref().expect("duplicate commit");
    assert_eq!(first, duplicate);

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 1);

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0)).expect("changes");
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(
        changes.changes[0].commit_id,
        CommitId::parse("req-duplicate").expect("valid commit id")
    );
}

#[test]
fn visible_commit_id_retry_aliases_across_writer_takeover() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer_a = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &writer_a, false).expect("bootstrap");

    let request = ApiCommitRequest {
        commit_id: CommitId::parse("retry-across-writer").expect("valid commit id"),
        preconditions: Vec::new(),
        ops: vec![ApiCommitOp::CreateDir {
            parent_inode: InodeId(1),
            display_name: "alpha".to_owned(),
        }],
        message: None,
        annotations: None,
    };

    let first = commit_operations(&store, &namespace_id, request.clone(), &writer_a)
        .expect("writer a commit");
    let writer_b = MutationContext {
        writer_id: "writer-b".to_owned(),
        writer_version: "writer-b/0.1.0".to_owned(),
        now_ms: writer_a
            .now_ms
            .saturating_add(writer_a.lease_duration_ms)
            .saturating_add(1),
        lease_duration_ms: writer_a.lease_duration_ms,
    };

    let retry =
        commit_operations(&store, &namespace_id, request, &writer_b).expect("writer b retry");

    assert_eq!(first, retry);
}

#[test]
fn batch_commit_rejects_duplicate_commit_id_with_different_fingerprint() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                commit_id: CommitId::parse("req-conflict").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "alpha".to_owned(),
                }],
                message: None,
                annotations: None,
            },
            ApiCommitRequest {
                commit_id: CommitId::parse("req-conflict").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "beta".to_owned(),
                }],
                message: None,
                annotations: None,
            },
        ],
        &context,
    );

    responses[0].as_ref().expect("primary commit");
    let error = responses[1].as_ref().expect_err("duplicate conflict");
    assert!(matches!(
        error,
        CoreError::CommitIdReuseConflict(commit_id) if commit_id == "req-conflict"
    ));

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 1);

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0)).expect("changes");
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(
        changes.changes[0].commit_id,
        CommitId::parse("req-conflict").expect("valid commit id")
    );
}

#[test]
fn explicit_commit_rejects_invalid_display_names() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

    let create_error = commit_operations(
        &store,
        &namespace_id,
        ApiCommitRequest {
            commit_id: CommitId::parse("invalid-create-name").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "a/b".to_owned(),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("invalid create display name");
    assert_eq!(create_error.code(), ErrorCode::InvalidPath);
    assert!(matches!(
        create_error,
        CoreError::CommitValidation(CommitValidationError::InvalidDisplayName {
            display_name
        }) if display_name == "a/b"
    ));

    write_file_bytes(
        &store,
        &namespace_id,
        "/file.txt",
        b"hello",
        &context,
        Some("seed-for-invalid-rename"),
    )
    .expect("seed file");
    let basis = load_verified_namespace_basis(&store, &namespace_id).expect("load basis");
    let file = basis
        .metadata_state
        .resolve_visible_path(
            &AbsolutePath::parse("/file.txt").expect("path"),
            basis.head.name_policy,
            basis.head.seq,
        )
        .expect("resolve file");

    let rename_error = commit_operations(
        &store,
        &namespace_id,
        ApiCommitRequest {
            commit_id: CommitId::parse("invalid-rename-name").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::Rename {
                inode_id: file.inode_id,
                new_parent_inode: InodeId(1),
                new_display_name: ".".to_owned(),
                mode: loon_api::v0::RenameMode::NoReplace,
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("invalid rename display name");
    assert_eq!(rename_error.code(), ErrorCode::InvalidPath);
    assert!(matches!(
        rename_error,
        CoreError::CommitValidation(CommitValidationError::InvalidDisplayName {
            display_name
        }) if display_name == "."
    ));
}

#[test]
fn direct_publisher_path_intents_cover_basic_mutations() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let publisher = DirectObjectStorePublisher::new(&store);

    let content = store_bytes_as_content(&store, &namespace_id, b"hello").expect("stage content");
    let put = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-path").expect("valid commit id"),
                absolute_path: "/docs/a.txt".to_owned(),
                content_ref: content.content_ref.clone(),
                behavior: PutFileBehavior::CreateOnly,
            },
            &context,
            PublishOptions::default(),
        )
        .expect("put path");
    assert_eq!(put.committed_seq, ChangeSeq(1));

    let moved = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::MovePath {
                commit_id: CommitId::parse("move-path").expect("valid commit id"),
                from_path: "/docs/a.txt".to_owned(),
                to_path: "/docs/b.txt".to_owned(),
                mode: loon_api::v0::RenameMode::NoReplace,
            },
            &context,
            PublishOptions::default(),
        )
        .expect("move path");
    assert_eq!(moved.committed_seq, ChangeSeq(2));

    let copied = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::CopyFilePath {
                commit_id: CommitId::parse("copy-path").expect("valid commit id"),
                from_path: "/docs/b.txt".to_owned(),
                to_path: "/docs/c.txt".to_owned(),
            },
            &context,
            PublishOptions::default(),
        )
        .expect("copy path");
    assert_eq!(copied.committed_seq, ChangeSeq(3));

    let deleted = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::DeletePath {
                commit_id: CommitId::parse("delete-path").expect("valid commit id"),
                absolute_path: "/docs/b.txt".to_owned(),
                recursive: false,
            },
            &context,
            PublishOptions::default(),
        )
        .expect("delete path");
    assert_eq!(deleted.committed_seq, ChangeSeq(4));

    let copied_bytes =
        read_file_bytes(&store, &namespace_id, "/docs/c.txt").expect("read copied file");
    assert_eq!(copied_bytes.bytes, b"hello");
}

#[test]
fn direct_publisher_uses_durable_path_commit_receipt_index() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let publisher = DirectObjectStorePublisher::new(&store);
    let content = store_bytes_as_content(&store, &namespace_id, b"hello").expect("stage content");

    let intent = PathMutationIntent::PutFile {
        commit_id: CommitId::parse("same-path-request").expect("valid commit id"),
        absolute_path: "/same//path.txt".to_owned(),
        content_ref: content.content_ref.clone(),
        behavior: PutFileBehavior::CreateOnly,
    };
    let first = publisher
        .submit_path_intent(
            &namespace_id,
            intent.clone(),
            &context,
            PublishOptions::default(),
        )
        .expect("first publish");
    let retry = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("same-path-request").expect("valid commit id"),
                absolute_path: "/same/path.txt".to_owned(),
                content_ref: content.content_ref.clone(),
                behavior: PutFileBehavior::CreateOnly,
            },
            &context,
            PublishOptions::default(),
        )
        .expect("idempotent retry");
    assert_eq!(retry.committed_seq, first.committed_seq);

    let conflict = publisher
        .submit_path_intent(
            &namespace_id,
            PathMutationIntent::DeletePath {
                commit_id: CommitId::parse("same-path-request").expect("valid commit id"),
                absolute_path: "/same/path.txt".to_owned(),
                recursive: false,
            },
            &context,
            PublishOptions::default(),
        )
        .expect_err("conflicting retry");
    assert!(matches!(
        conflict,
        CoreError::CommitIdReuseConflict(commit_id) if commit_id == "same-path-request"
    ));

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
}

#[test]
fn path_intents_in_one_batch_see_tentative_state() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"hello").expect("stage content");

    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![
            NamespaceMutationCandidate::Path(PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-batched-path").expect("valid commit id"),
                absolute_path: "/docs/a.txt".to_owned(),
                content_ref: content.content_ref,
                behavior: PutFileBehavior::CreateOnly,
            }),
            NamespaceMutationCandidate::Path(PathMutationIntent::MovePath {
                commit_id: CommitId::parse("move-batched-path").expect("valid commit id"),
                from_path: "/docs/a.txt".to_owned(),
                to_path: "/docs/b.txt".to_owned(),
                mode: loon_api::v0::RenameMode::NoReplace,
            }),
        ],
        &context,
    );

    assert_eq!(
        responses[0].as_ref().expect("put").committed_seq,
        ChangeSeq(1)
    );
    assert_eq!(
        responses[1].as_ref().expect("move").committed_seq,
        ChangeSeq(2)
    );
    let moved_bytes =
        read_file_bytes(&store, &namespace_id, "/docs/b.txt").expect("read moved file");
    assert_eq!(moved_bytes.bytes, b"hello");

    let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 2);
}

#[test]
fn namespace_descriptor_checksum_is_validated() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap namespace");

    let descriptor_key = namespace_descriptor(namespace_id.as_str());
    let descriptor_bytes = store
        .get(&descriptor_key, None)
        .expect("read namespace descriptor")
        .expect("namespace descriptor exists");
    let mut descriptor: NamespaceDescriptorEnvelope =
        serde_json::from_slice(&descriptor_bytes).expect("decode namespace descriptor");
    descriptor.payload_checksum_sha256 = "not-the-payload-checksum".to_owned();
    let corrupted = serde_json::to_vec(&descriptor).expect("encode corrupted descriptor");
    store
        .put_overwrite(&descriptor_key, &corrupted)
        .expect("overwrite descriptor");

    let error =
        load_verified_namespace_basis(&store, &namespace_id).expect_err("descriptor checksum");
    assert!(
        error.to_string().contains("checksum mismatch"),
        "unexpected error: {error}"
    );
}

#[test]
fn fork_namespace_reuses_content_store_and_isolates_metadata() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    bootstrap_namespace(&store, &source_namespace_id, &context, false)
        .expect("bootstrap source namespace");
    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"base",
        &context,
        Some("seed-shared"),
    )
    .expect("seed shared file");

    let source_basis =
        load_verified_namespace_basis(&store, &source_namespace_id).expect("source basis");
    assert_eq!(source_basis.head.seq, ChangeSeq(1));
    let content_store_id = source_basis.content_store_id.clone();
    let blobs_before = store
        .list_prefix(&format!(
            "content-stores/{}/blobs/",
            content_store_id.as_str()
        ))
        .expect("list blobs before fork");

    fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect("fork namespace");

    let fork_state_key = namespace_fork_state(clone_namespace_id.as_str());
    let fork_state_bytes = store
        .get(&fork_state_key, None)
        .expect("read fork state")
        .expect("fork state exists");
    let fork_state: NamespaceForkStateEnvelope =
        serde_json::from_slice(&fork_state_bytes).expect("decode fork state");
    assert_eq!(fork_state.kind, ControlObjectKind::NamespaceForkState);
    assert_eq!(fork_state.state.namespace_id, clone_namespace_id);
    assert_eq!(fork_state.state.source_namespace_id, source_namespace_id);
    assert_eq!(fork_state.state.fork_seq, ChangeSeq(1));
    assert!(fork_state.state.source_checkpoint_id.starts_with("chk_"));
    assert_eq!(fork_state.state.source_manifest_id, ManifestId(1));
    assert_eq!(fork_state.state.source_head_seq, ChangeSeq(1));
    assert!(fork_state.state.created_at_ms > 0);
    assert!(fork_state.has_valid_payload_checksum().expect("checksum"));

    let blobs_after = store
        .list_prefix(&format!(
            "content-stores/{}/blobs/",
            content_store_id.as_str()
        ))
        .expect("list blobs after fork");
    assert_eq!(blobs_after, blobs_before, "fork must not copy content");

    let clone_basis =
        load_verified_namespace_basis(&store, &clone_namespace_id).expect("clone basis");
    assert_eq!(clone_basis.content_store_id, content_store_id);
    assert_eq!(clone_basis.head.seq, ChangeSeq(1));
    assert_eq!(clone_basis.head.current_manifest_id, Some(ManifestId(1)));
    assert_eq!(clone_basis.head.retention_floor_seq, ChangeSeq(1));

    let target_manifest_key = namespace_manifest(clone_namespace_id.as_str(), ManifestId(1));
    let target_manifest_bytes = store
        .get(&target_manifest_key, None)
        .expect("read target manifest")
        .expect("target manifest exists");
    let target_manifest =
        decode_namespace_manifest_json(&target_manifest_bytes).expect("decode target manifest");
    assert!(target_manifest.payload.fork.is_some());
    assert_eq!(target_manifest.payload.checkpoints.len(), 1);
    assert_eq!(
        target_manifest.payload.checkpoints[0].head_seq,
        ChangeSeq(1)
    );
    assert_eq!(
        target_manifest.payload.checkpoints[0].manifest_id,
        ManifestId(1)
    );
    assert!(
        target_manifest
            .payload
            .metadata_files
            .iter()
            .all(|metadata_file| metadata_file.owner_namespace_id == source_namespace_id),
        "fork target manifest should reference source-owned metadata SSTs"
    );
    assert!(
        store
            .list_prefix(&format!(
                "namespaces/{}/compacted/metadata/",
                clone_namespace_id.as_str()
            ))
            .expect("list target metadata SSTs")
            .is_empty(),
        "COW fork should not copy metadata SSTs into the target namespace"
    );

    let pin_keys = store
        .list_prefix(&format!(
            "namespaces/{}/gc/pins/",
            source_namespace_id.as_str()
        ))
        .expect("list source pins");
    assert_eq!(
        pin_keys.len(),
        1,
        "fork should write one source-local GC pin"
    );
    let pin_bytes = store
        .get(&pin_keys[0], None)
        .expect("read source pin")
        .expect("source pin exists");
    let pin: NamespaceGcPinStateEnvelope =
        serde_json::from_slice(&pin_bytes).expect("decode source pin");
    assert_eq!(pin.kind, ControlObjectKind::NamespaceGcPinState);
    assert_eq!(pin.state.source_namespace_id, source_namespace_id);
    assert_eq!(pin.state.target_namespace_id, clone_namespace_id);
    assert_eq!(
        pin.state.source_checkpoint_id,
        fork_state.state.source_checkpoint_id
    );
    assert_eq!(pin.state.source_manifest_id, ManifestId(1));
    assert_eq!(pin.state.source_head_seq, ChangeSeq(1));
    let referenced_metadata_files_debug = target_manifest
        .payload
        .metadata_files
        .iter()
        .map(|metadata_file| metadata_file.object_key.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        pin.state.referenced_metadata_files_debug,
        referenced_metadata_files_debug
    );
    assert!(store
        .head(&gc_pin(source_namespace_id.as_str(), &pin.state.pin_id))
        .expect("head source pin")
        .is_some());

    let duplicate_error =
        fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
            .expect_err("duplicate fork target");
    assert_eq!(duplicate_error.code(), ErrorCode::NamespaceExists);

    let source_entry =
        resolve_path(&store, &source_namespace_id, "/docs/shared.txt").expect("source stat");
    let clone_entry =
        resolve_path(&store, &clone_namespace_id, "/docs/shared.txt").expect("clone stat");
    assert_eq!(source_entry.content_ref, clone_entry.content_ref);
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .expect("read clone")
            .bytes,
        b"base"
    );
    store.delete(&fork_state_key).expect("delete fork state");
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .expect("read clone without fork state")
            .bytes,
        b"base"
    );
    store
        .put_overwrite(&fork_state_key, b"not-json")
        .expect("corrupt fork state");
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .expect("read clone with corrupt fork state")
            .bytes,
        b"base"
    );

    let stale_clone_changes =
        list_changes_after(&store, &clone_namespace_id, ChangeSeq(0)).expect_err("old cursor");
    assert_eq!(stale_clone_changes.code(), ErrorCode::RebootstrapRequired);
    let empty_clone_changes =
        list_changes_after(&store, &clone_namespace_id, ChangeSeq(1)).expect("empty changes");
    assert!(empty_clone_changes.changes.is_empty());

    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"source-after-fork",
        &context,
        Some("source-after-fork"),
    )
    .expect("source replace");
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .expect("read clone after source write")
            .bytes,
        b"base"
    );

    let clone_write = write_file_bytes(
        &store,
        &clone_namespace_id,
        "/docs/shared.txt",
        b"clone-after-fork",
        &context,
        Some("clone-after-fork"),
    )
    .expect("clone replace");
    assert_eq!(clone_write.committed_seq, ChangeSeq(2));
    assert_eq!(
        read_file_bytes(&store, &source_namespace_id, "/docs/shared.txt")
            .expect("read source")
            .bytes,
        b"source-after-fork"
    );
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .expect("read clone")
            .bytes,
        b"clone-after-fork"
    );

    let clone_changes =
        list_changes_after(&store, &clone_namespace_id, ChangeSeq(1)).expect("clone changes");
    assert_eq!(clone_changes.changes.len(), 1);
    assert_eq!(clone_changes.changes[0].seq, ChangeSeq(2));

    for prefix in [
        format!("namespaces/{}/control/", source_namespace_id.as_str()),
        format!("namespaces/{}/wal/", source_namespace_id.as_str()),
        format!("namespaces/{}/manifest/", source_namespace_id.as_str()),
    ] {
        for key in store
            .list_prefix(&prefix)
            .expect("list source mutable keys")
        {
            store.delete(&key).expect("delete source mutable key");
        }
    }
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .expect("clone remains readable")
            .bytes,
        b"clone-after-fork"
    );

    let referenced_sst = referenced_metadata_files_debug
        .first()
        .expect("fork should reference source metadata SST")
        .clone();
    store
        .delete(&referenced_sst)
        .expect("delete referenced source metadata SST");
    let corrupt_target = read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
        .expect_err("target should fail when referenced source SST is missing");
    assert_eq!(corrupt_target.code(), ErrorCode::NamespaceCorrupt);
}

#[test]
fn fork_target_head_reservation_failure_keeps_descriptor_unpublished() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(namespace_head(clone_namespace_id.as_str())),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: true,
            additional_writes: Vec::new(),
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context);

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect_err("target head precondition should re-check partial namespace");
    assert_eq!(error.code(), ErrorCode::NamespacePartial);
    assert!(
        !store
            .list_prefix(&format!(
                "namespaces/{}/manifest/",
                clone_namespace_id.as_str()
            ))
            .expect("list target manifests")
            .is_empty(),
        "target manifest should be written before target head reservation"
    );
    assert!(
        store
            .head(&namespace_descriptor(clone_namespace_id.as_str()))
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    assert_namespace_partial(&store, &clone_namespace_id, &context);
}

#[test]
fn fork_source_gc_pin_failure_leaves_target_namespace_absent() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Prefix(format!(
            "namespaces/{}/gc/pins/",
            source_namespace_id.as_str()
        )),
        InjectedCreateFailure::Transport {
            message: "injected source gc pin failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context);

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect_err("source GC pin failure should abort fork before target publication");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(
        store
            .head(&namespace_head(clone_namespace_id.as_str()))
            .expect("head target head")
            .is_none(),
        "target head must not be reserved before source retention is pinned"
    );
    assert!(
        store
            .head(&namespace_descriptor(clone_namespace_id.as_str()))
            .expect("head target descriptor")
            .is_none(),
        "target descriptor must remain unpublished"
    );
    assert!(
        store
            .list_prefix(&format!(
                "namespaces/{}/manifest/",
                clone_namespace_id.as_str()
            ))
            .expect("list target manifests")
            .is_empty(),
        "target manifest must not be written before source retention is pinned"
    );
    assert_eq!(
        list_namespaces(&store)
            .expect("list namespaces")
            .into_iter()
            .map(|summary| summary.namespace_id)
            .collect::<Vec<_>>(),
        vec![source_namespace_id]
    );
}

#[test]
fn fork_target_manifest_failure_leaves_target_namespace_absent() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Prefix(format!(
            "namespaces/{}/manifest/",
            clone_namespace_id.as_str()
        )),
        InjectedCreateFailure::Transport {
            message: "injected target manifest failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context);

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect_err("target manifest write should fail");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(
        store
            .head(&namespace_head(clone_namespace_id.as_str()))
            .expect("head target head")
            .is_none(),
        "target head must not be reserved before target manifest exists"
    );
    assert!(
        store
            .head(&namespace_descriptor(clone_namespace_id.as_str()))
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    assert!(
        store
            .list_prefix(&format!(
                "namespaces/{}/manifest/",
                clone_namespace_id.as_str()
            ))
            .expect("list target manifests")
            .is_empty(),
        "target manifest should not exist after injected manifest write failure"
    );
    assert_eq!(
        list_namespaces(&store)
            .expect("list namespaces")
            .into_iter()
            .map(|summary| summary.namespace_id)
            .collect::<Vec<_>>(),
        vec![source_namespace_id]
    );
}

#[test]
fn fork_failure_after_target_manifest_before_fork_state_remains_partial() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(namespace_fork_state(clone_namespace_id.as_str())),
        InjectedCreateFailure::Transport {
            message: "injected target fork state failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context);

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect_err("target fork-state write should fail");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(
        store
            .head(&namespace_head(clone_namespace_id.as_str()))
            .expect("head target head")
            .is_some(),
        "target head should still reserve namespace"
    );
    assert!(
        store
            .head(&namespace_descriptor(clone_namespace_id.as_str()))
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    let target_manifest_keys = store
        .list_prefix(&format!(
            "namespaces/{}/manifest/",
            clone_namespace_id.as_str()
        ))
        .expect("list target manifests");
    assert!(
        !target_manifest_keys.is_empty(),
        "target manifest should have been written before fork-state failure"
    );
    assert!(
        store
            .head(&namespace_fork_state(clone_namespace_id.as_str()))
            .expect("head target fork state")
            .is_none(),
        "fork state should not be durable after injected fork-state failure"
    );
    assert_namespace_partial(&store, &clone_namespace_id, &context);
}

#[test]
fn fork_failure_after_target_manifest_artifacts_remains_partial() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(namespace_lease(clone_namespace_id.as_str())),
        InjectedCreateFailure::Transport {
            message: "injected target lease failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context);

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect_err("target lease write should fail");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(
        store
            .head(&namespace_head(clone_namespace_id.as_str()))
            .expect("head target head")
            .is_some(),
        "target head should still reserve namespace"
    );
    assert!(
        store
            .head(&namespace_descriptor(clone_namespace_id.as_str()))
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    let target_manifest_keys = store
        .list_prefix(&format!(
            "namespaces/{}/manifest/",
            clone_namespace_id.as_str()
        ))
        .expect("list target manifests");
    assert!(
        !target_manifest_keys.is_empty(),
        "target manifest should have been written before lease failure"
    );
    assert_namespace_partial(&store, &clone_namespace_id, &context);
}

#[test]
fn fork_target_control_conflict_rechecks_complete_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id();
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_source_namespace_for_fork(&inner, &source_namespace_id, &context);
    let content_store_id = load_verified_namespace_basis(&inner, &source_namespace_id)
        .expect("source basis")
        .content_store_id;
    let descriptor = NamespaceDescriptorEnvelope::from_state(
        ControlObjectKind::NamespaceDescriptor,
        &context.writer_version,
        NamespaceDescriptorState {
            namespace_id: clone_namespace_id.clone(),
            content_store_id,
        },
    )
    .expect("descriptor envelope");
    let store = InjectCreateFailureStore::new(
        inner,
        KeyMatcher::Exact(namespace_head(clone_namespace_id.as_str())),
        InjectedCreateFailure::Conflict {
            write_attempted_object: true,
            additional_writes: vec![
                (
                    namespace_lease(clone_namespace_id.as_str()),
                    b"lease-present".to_vec(),
                ),
                (
                    namespace_descriptor(clone_namespace_id.as_str()),
                    serde_json::to_vec(&descriptor).expect("descriptor bytes"),
                ),
            ],
        },
    );

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .expect_err("target head conflict should re-check complete namespace");
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
    assert_eq!(
        list_namespaces(&store)
            .expect("list namespaces")
            .into_iter()
            .map(|summary| summary.namespace_id)
            .collect::<Vec<_>>(),
        vec![clone_namespace_id, source_namespace_id]
    );
}

#[test]
fn restore_revision_revalidates_durable_content_before_publish() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");

    let first = store_bytes_as_content(&store, &namespace_id(), b"first").expect("stage first");
    let create = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-create").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "restore.txt".to_owned(),
                content_ref: first.content_ref.clone(),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("create file");
    let inode_id = match &create.results[0] {
        loon_api::v0::CommitOpResult::CreateFile { inode_id, .. } => *inode_id,
        other => panic!("unexpected create result: {other:?}"),
    };

    let second = store_bytes_as_content(&store, &namespace_id(), b"second").expect("stage second");
    commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-replace").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::ReplaceFile {
                inode_id,
                base_revision_no: RevisionNo(1),
                content_ref: second.content_ref,
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("replace file");

    store
        .delete(
            &content_blob(first.content_store_id.as_str(), &first.content_ref.digest)
                .expect("first content key"),
        )
        .expect("delete first content");

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-missing-content").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::RestoreRevision {
                inode_id,
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(2),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("restore missing durable content");
    assert!(matches!(
        error,
        CoreError::DurableContent(
            loon_core::content::DurableContentValidationError::MissingContentObject { .. }
        )
    ));
}

#[test]
fn metadata_only_commit_does_not_validate_content_store_refs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/delete-me.txt",
        b"hello",
        &context,
        Some("seed-metadata-only-delete"),
    )
    .expect("seed file");
    let inode_id = resolve_path(&store, &namespace_id(), "/docs/delete-me.txt")
        .expect("resolve seeded file")
        .inode_id;

    let guarded_store = ContentStoreAccessLimitStore::new(temp_dir.path(), 2);
    let response = commit_operations(
        &guarded_store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("metadata-only-delete").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::DeleteFile { inode_id }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("metadata-only delete should not perform content validation");

    assert_eq!(response.committed_seq, ChangeSeq(2));
    assert_eq!(
        guarded_store.content_store_access_count(),
        2,
        "basis loading performs one content-store descriptor head/get; metadata-only validation must not add another lookup",
    );
}

#[test]
fn create_file_prioritizes_missing_durable_content_over_missing_parent() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("create-missing-parent-missing-content")
                .expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateFile {
                parent_inode: InodeId(99),
                display_name: "missing.txt".to_owned(),
                content_ref: content_ref("missing-content"),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("missing content should win before missing parent");
    assert!(matches!(
        error,
        CoreError::DurableContent(
            loon_core::content::DurableContentValidationError::MissingContentObject { .. }
        )
    ));
}

#[test]
fn replace_file_prioritizes_missing_durable_content_over_stale_revision() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/replace.txt",
        b"first",
        &context,
        Some("seed-replace"),
    )
    .expect("seed replace target");
    let inode_id = resolve_path(&store, &namespace_id(), "/docs/replace.txt")
        .expect("resolve path")
        .inode_id;

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("replace-stale-missing-content").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::ReplaceFile {
                inode_id,
                base_revision_no: RevisionNo(99),
                content_ref: content_ref("missing-content"),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("missing content should win before stale revision");
    assert!(matches!(
        error,
        CoreError::DurableContent(
            loon_core::content::DurableContentValidationError::MissingContentObject { .. }
        )
    ));
}

#[test]
fn restore_revision_missing_source_is_revision_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/restore.txt",
        b"first",
        &context,
        Some("seed-restore"),
    )
    .expect("seed restore target");
    let inode_id = resolve_path(&store, &namespace_id(), "/docs/restore.txt")
        .expect("resolve path")
        .inode_id;

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-missing-source").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::RestoreRevision {
                inode_id,
                source_revision_no: RevisionNo(99),
                base_revision_no: RevisionNo(1),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("missing restore source should fail");
    assert_eq!(error.code(), ErrorCode::RevisionNotFound);
}

#[test]
fn restore_revision_resolves_same_request_source_before_durable_content_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");

    let first = store_bytes_as_content(&store, &namespace_id(), b"first").expect("stage first");
    let create = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("resolve-before-durable-check-create")
                .expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "restore.txt".to_owned(),
                content_ref: first.content_ref.clone(),
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect("create file");
    let inode_id = match &create.results[0] {
        loon_api::v0::CommitOpResult::CreateFile { inode_id, .. } => *inode_id,
        other => panic!("unexpected create result: {other:?}"),
    };

    let second = store_bytes_as_content(&store, &namespace_id(), b"second").expect("stage second");
    store
        .delete(
            &content_blob(second.content_store_id.as_str(), &second.content_ref.digest)
                .expect("second content key"),
        )
        .expect("delete second content");

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("resolve-before-durable-check-commit")
                .expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![
                ApiCommitOp::ReplaceFile {
                    inode_id,
                    base_revision_no: RevisionNo(1),
                    content_ref: second.content_ref.clone(),
                },
                ApiCommitOp::RestoreRevision {
                    inode_id,
                    source_revision_no: RevisionNo(2),
                    base_revision_no: RevisionNo(2),
                },
            ],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("missing same-request content should fail durable-content validation");
    assert!(matches!(
        error,
        CoreError::DurableContent(
            loon_core::content::DurableContentValidationError::MissingContentObject { .. }
        )
    ));
}

#[test]
fn move_path_into_occupied_target_is_path_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/a.txt",
        b"alpha",
        &context,
        Some("seed-docs"),
    )
    .expect("seed docs");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/tmp/a.txt",
        b"tmp-a",
        &context,
        Some("seed-tmp"),
    )
    .expect("seed tmp");

    let error = move_path(
        &store,
        &namespace_id(),
        "/tmp/a.txt",
        "/docs/a.txt",
        &context,
        Some("move-conflict"),
    )
    .expect_err("move conflict");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[test]
fn move_path_directory_cycle_is_would_cycle() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/archive/leaf.txt",
        b"leaf",
        &context,
        Some("seed-cycle"),
    )
    .expect("seed cycle dirs");

    let error = move_path(
        &store,
        &namespace_id(),
        "/docs",
        "/docs/archive/docs",
        &context,
        Some("cycle"),
    )
    .expect_err("cycle");
    assert_eq!(error.code(), ErrorCode::WouldCycle);
}

#[test]
fn write_and_move_under_tombstoned_ancestor_are_tombstone_conflicts() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/old.txt",
        b"old",
        &context,
        Some("seed-docs"),
    )
    .expect("seed docs");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/tmp/source.txt",
        b"source",
        &context,
        Some("seed-source"),
    )
    .expect("seed source");
    delete_path(
        &store,
        &namespace_id(),
        "/docs",
        &context,
        Some("delete-docs"),
    )
    .expect("delete docs");

    let put_error = write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/new.txt",
        b"new",
        &context,
        Some("put-under-tombstone"),
    )
    .expect_err("put tombstone conflict");
    assert_eq!(put_error.code(), ErrorCode::TombstoneConflict);

    let move_error = move_path(
        &store,
        &namespace_id(),
        "/tmp/source.txt",
        "/docs/source.txt",
        &context,
        Some("move-under-tombstone"),
    )
    .expect_err("move tombstone conflict");
    assert_eq!(move_error.code(), ErrorCode::TombstoneConflict);
}

#[test]
fn create_dir_path_creates_directory_without_auto_parents() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");

    let created = create_dir_path(
        &store,
        &namespace_id(),
        "/docs",
        &context,
        Some("mkdir-docs"),
    )
    .expect("create dir");
    assert_eq!(created.committed_seq, ChangeSeq(1));
    let docs = resolve_path(&store, &namespace_id(), "/docs").expect("resolve docs");
    assert_eq!(docs.inode_kind, InodeKind::Dir);

    let missing_parent = create_dir_path(
        &store,
        &namespace_id(),
        "/missing/nested",
        &context,
        Some("mkdir-missing-parent"),
    )
    .expect_err("mkdir does not auto-create parents");
    assert_eq!(missing_parent.code(), ErrorCode::PathNotFound);
}

#[test]
fn path_move_writes_unbind_and_stale_binding_is_fails() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/a.txt",
        b"hello",
        &context,
        Some("seed-a"),
    )
    .expect("seed file");
    let basis_before_move =
        load_verified_namespace_basis(&store, &namespace_id()).expect("load basis");
    let file = basis_before_move
        .metadata_state
        .resolve_visible_path(
            &AbsolutePath::parse("/docs/a.txt").expect("path"),
            basis_before_move.head.name_policy,
            basis_before_move.head.seq,
        )
        .expect("resolve file");
    let old_binding = basis_before_move
        .metadata_state
        .current_parent_binding_for_child(file.inode_id, basis_before_move.head.seq)
        .expect("old binding");

    move_path(
        &store,
        &namespace_id(),
        "/docs/a.txt",
        "/docs/b.txt",
        &context,
        Some("move-a-to-b"),
    )
    .expect("move file");
    let moved_basis = load_verified_namespace_basis(&store, &namespace_id()).expect("load basis");
    assert_eq!(moved_basis.metadata_state.direntry_unbinds().len(), 1);
    assert!(resolve_path(&store, &namespace_id(), "/docs/a.txt").is_err());
    resolve_path(&store, &namespace_id(), "/docs/b.txt").expect("new path visible");

    let stale_binding = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("delete-with-stale-binding").expect("valid commit id"),
            preconditions: vec![CommitPrecondition::BindingIs {
                parent_inode: old_binding.parent_inode_id,
                name_key: NameKey::try_new(old_binding.name_key).expect("valid name key"),
                child_inode: old_binding.child_inode_id,
                bind_seq: old_binding.bind_seq,
                bind_delta_index: old_binding.bind_delta_index,
            }],
            ops: vec![ApiCommitOp::DeleteFile {
                inode_id: file.inode_id,
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("stale binding should fail");
    assert_eq!(stale_binding.code(), ErrorCode::PathConflict);
}

#[test]
fn unsupported_rename_mode_is_named_bad_request_failure() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/a.txt",
        b"hello",
        &context,
        Some("seed-rename-mode"),
    )
    .expect("seed file");
    let basis = load_verified_namespace_basis(&store, &namespace_id()).expect("load basis");
    let file = basis
        .metadata_state
        .resolve_visible_path(
            &AbsolutePath::parse("/docs/a.txt").expect("path"),
            basis.head.name_policy,
            basis.head.seq,
        )
        .expect("resolve file");

    let error = commit_operations(
        &store,
        &namespace_id(),
        ApiCommitRequest {
            commit_id: CommitId::parse("unsupported-rename-mode").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::Rename {
                inode_id: file.inode_id,
                new_parent_inode: InodeId(1),
                new_display_name: "b.txt".to_owned(),
                mode: loon_api::v0::RenameMode::ReplaceExisting,
            }],
            message: None,
            annotations: None,
        },
        &context,
    )
    .expect_err("unsupported rename mode");
    assert_eq!(error.code(), ErrorCode::UnsupportedRenameMode);
}

#[test]
fn put_file_create_only_rejects_existing_target_without_force() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/hello.txt",
        b"hello",
        &context,
        Some("seed-hello"),
    )
    .expect("seed file");

    let error = put_file_bytes(
        &store,
        &namespace_id(),
        "/docs/hello.txt",
        b"new-bytes",
        PutFileBehavior::CreateOnly,
        &context,
        Some("put-no-force"),
    )
    .expect_err("put without force");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[test]
fn delete_path_non_recursive_rejects_non_empty_directory() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/hello.txt",
        b"hello",
        &context,
        Some("seed-docs"),
    )
    .expect("seed file");

    let error = delete_path_non_recursive(
        &store,
        &namespace_id(),
        "/docs",
        &context,
        Some("delete-docs"),
    )
    .expect_err("non-recursive delete should reject non-empty dir");
    assert!(matches!(error, CoreError::DirectoryNotEmpty(path) if path == "/docs"));
}

#[test]
fn copy_file_path_creates_new_inode_and_reuses_content_blob() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/docs/source.txt",
        b"hello",
        &context,
        Some("seed-source"),
    )
    .expect("seed source");

    copy_file_path(
        &store,
        &namespace_id(),
        "/docs/source.txt",
        "/docs/copy.txt",
        &context,
        Some("copy-file"),
    )
    .expect("copy file");

    let source = resolve_path(&store, &namespace_id(), "/docs/source.txt").expect("source stat");
    let copy = resolve_path(&store, &namespace_id(), "/docs/copy.txt").expect("copy stat");
    assert_ne!(source.inode_id, copy.inode_id);
    assert_eq!(
        source.content_ref, copy.content_ref,
        "copy should reuse stored content ref"
    );
}

#[test]
fn resolve_path_uses_nfc_casefold_name_policy() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let stored_path = "/Cafe\u{0301}.txt";
    let lookup_path = "/CAF\u{00c9}.TXT";
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        stored_path,
        b"hello",
        &context,
        Some("seed-unicode-name"),
    )
    .expect("seed unicode name");

    let resolved = resolve_path(&store, &namespace_id(), lookup_path).expect("resolve path");
    assert_eq!(resolved.absolute_path, stored_path);
    assert_eq!(resolved.display_name, "Cafe\u{0301}.txt");
}

#[test]
fn create_only_put_rejects_casefold_and_normalization_equivalent_name() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id(), &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id(),
        "/Cafe\u{0301}.txt",
        b"hello",
        &context,
        Some("seed-unicode-name"),
    )
    .expect("seed unicode name");

    let error = put_file_bytes(
        &store,
        &namespace_id(),
        "/CAF\u{00c9}.TXT",
        b"new-bytes",
        PutFileBehavior::CreateOnly,
        &context,
        Some("create-only-conflict"),
    )
    .expect_err("create-only conflict");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

fn seed_source_namespace_for_fork<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    bootstrap_namespace(store, source_namespace_id, context, false)
        .expect("bootstrap source namespace");
    write_file_bytes(
        store,
        source_namespace_id,
        "/docs/shared.txt",
        b"base",
        context,
        Some("seed-shared"),
    )
    .expect("seed shared file");
}

fn assert_namespace_partial<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    let partial_error =
        bootstrap_namespace(store, namespace_id, context, false).expect_err("partial namespace");
    assert!(matches!(
        partial_error,
        loon_core::BootstrapNamespaceError::NamespacePartiallyInitialized { .. }
    ));
}

#[derive(Debug)]
struct InjectCreateFailureStore {
    inner: LocalFsStore,
    matcher: KeyMatcher,
    failure: InjectedCreateFailure,
    injected: Mutex<bool>,
}

impl InjectCreateFailureStore {
    fn new(inner: LocalFsStore, matcher: KeyMatcher, failure: InjectedCreateFailure) -> Self {
        Self {
            inner,
            matcher,
            failure,
            injected: Mutex::new(false),
        }
    }
}

#[derive(Debug)]
enum KeyMatcher {
    Exact(String),
    Prefix(String),
}

impl KeyMatcher {
    fn matches(&self, key: &str) -> bool {
        match self {
            Self::Exact(expected) => key == expected,
            Self::Prefix(prefix) => key.starts_with(prefix),
        }
    }
}

#[derive(Debug)]
enum InjectedCreateFailure {
    Transport {
        message: &'static str,
    },
    Conflict {
        write_attempted_object: bool,
        additional_writes: Vec<(String, Vec<u8>)>,
    },
    PreconditionFailed {
        write_attempted_object: bool,
        additional_writes: Vec<(String, Vec<u8>)>,
    },
}

impl InjectedCreateFailure {
    fn apply_before_error(
        &self,
        inner: &LocalFsStore,
        attempted_key: &str,
        attempted_bytes: &[u8],
    ) -> Result<(), ObjectStoreError> {
        match self {
            Self::Transport { .. } => Ok(()),
            Self::Conflict {
                write_attempted_object,
                additional_writes,
            }
            | Self::PreconditionFailed {
                write_attempted_object,
                additional_writes,
            } => {
                if *write_attempted_object {
                    inner.put_overwrite(attempted_key, attempted_bytes)?;
                }
                for (key, bytes) in additional_writes {
                    inner.put_overwrite(key, bytes)?;
                }
                Ok(())
            }
        }
    }

    fn error(&self) -> ObjectStoreError {
        match self {
            Self::Transport { message } => ObjectStoreError::Transport((*message).to_owned()),
            Self::Conflict { .. } => ObjectStoreError::Conflict,
            Self::PreconditionFailed { .. } => ObjectStoreError::PreconditionFailed,
        }
    }
}

impl ObjectStore for InjectCreateFailureStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if matches!(&mode, PutMode::CreateIfAbsent) && self.matcher.matches(key) {
            let should_inject = {
                let mut injected = self
                    .injected
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *injected {
                    false
                } else {
                    *injected = true;
                    true
                }
            };
            if should_inject {
                self.failure.apply_before_error(&self.inner, key, bytes)?;
                return Err(self.failure.error());
            }
        }

        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}

fn metadata_state_after(sequences: &[Vec<WalDelta>]) -> MetadataState {
    let mut state = MetadataState::default()
        .apply_committed_wal_deltas(
            ChangeSeq(0),
            &[WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
            }],
        )
        .expect("bootstrap root")
        .metadata_state;

    for (index, deltas) in sequences.iter().enumerate() {
        state = state
            .apply_committed_wal_deltas(ChangeSeq(u64::try_from(index + 1).expect("seq")), deltas)
            .expect("apply deltas")
            .metadata_state;
    }

    state
}

fn validation_context(
    metadata_state: &MetadataState,
    seq: ChangeSeq,
    next_inode_id: InodeId,
) -> CommitValidationContext<'_> {
    let namespace_id = namespace_id();
    let head = HeadState {
        namespace_id: namespace_id.clone(),
        seq,
        head_commit_id: CommitId::parse("c_00000000000000000000000000000000").expect("commit id"),
        active_fence_token: FenceToken(1),
        next_inode_id,
        name_policy: loon_api::NamePolicy::default(),
        current_manifest_id: Some(ManifestId(0)),
        latest_checkpoint_id: Some("chk_00000000000000000000000000000000".to_owned()),
        retention_floor_seq: ChangeSeq(0),
        visible_wal_tip: None,
    };
    let lease = LeaseState {
        namespace_id,
        holder_id: "writer-a".to_owned(),
        fence_token: head.active_fence_token,
        lease_expires_at_ms: 10_000,
    };

    CommitValidationContext {
        head,
        lease,
        now_ms: 1_000,
        metadata_state,
    }
}

fn mutation_context() -> MutationContext {
    MutationContext {
        writer_id: "writer-a".to_owned(),
        writer_version: "writer-a/0.1.0".to_owned(),
        now_ms: 1_000,
        lease_duration_ms: 60_000,
    }
}

fn namespace_id() -> NamespaceId {
    NamespaceId::parse("demo").expect("valid namespace id")
}

fn content_ref(seed: &str) -> ContentRef {
    ContentRef {
        kind: ContentRefKind::WholeFileV0,
        digest: sha256_digest(seed.as_bytes()),
        size_bytes: seed.len() as u64,
    }
}

struct ReplayReadGuardStore {
    inner: LocalFsStore,
    guarded_prefixes: Vec<String>,
    guarded_gets: AtomicUsize,
}

impl ReplayReadGuardStore {
    fn new(root: impl AsRef<Path>, namespace: &str) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            guarded_prefixes: vec![
                format!("namespaces/{namespace}/wal/"),
                format!("namespaces/{namespace}/manifest/"),
            ],
            guarded_gets: AtomicUsize::new(0),
        }
    }

    fn guarded_get_count(&self) -> usize {
        self.guarded_gets.load(Ordering::SeqCst)
    }

    fn reject_replay_read(&self, key: &str) -> Result<(), ObjectStoreError> {
        if self
            .guarded_prefixes
            .iter()
            .any(|prefix| key.starts_with(prefix))
        {
            self.guarded_gets.fetch_add(1, Ordering::SeqCst);
            return Err(ObjectStoreError::Transport(format!(
                "begin_upload unexpectedly read replay object `{key}`"
            )));
        }
        Ok(())
    }
}

impl ObjectStore for ReplayReadGuardStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.reject_replay_read(key)?;
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}

struct ContentBlobGetCountingStore {
    inner: LocalFsStore,
    content_blob_gets: AtomicUsize,
}

impl ContentBlobGetCountingStore {
    fn new(root: impl AsRef<Path>) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            content_blob_gets: AtomicUsize::new(0),
        }
    }

    fn content_blob_get_count(&self) -> usize {
        self.content_blob_gets.load(Ordering::SeqCst)
    }

    fn reset_content_blob_get_count(&self) {
        self.content_blob_gets.store(0, Ordering::SeqCst);
    }

    fn record_content_blob_get(&self, key: &str) {
        if key.starts_with("content-stores/") && key.contains("/blobs/") {
            self.content_blob_gets.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl ObjectStore for ContentBlobGetCountingStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn head_with_checksum(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head_with_checksum(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.record_content_blob_get(key);
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}

struct ContentStoreAccessLimitStore {
    inner: LocalFsStore,
    content_store_accesses: AtomicUsize,
    max_content_store_accesses: usize,
}

impl ContentStoreAccessLimitStore {
    fn new(root: impl AsRef<Path>, max_content_store_accesses: usize) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            content_store_accesses: AtomicUsize::new(0),
            max_content_store_accesses,
        }
    }

    fn content_store_access_count(&self) -> usize {
        self.content_store_accesses.load(Ordering::SeqCst)
    }

    fn record_content_store_access(&self, key: &str) -> Result<(), ObjectStoreError> {
        if !key.starts_with("content-stores/") {
            return Ok(());
        }

        let previous = self.content_store_accesses.fetch_add(1, Ordering::SeqCst);
        if previous >= self.max_content_store_accesses {
            return Err(ObjectStoreError::Transport(format!(
                "unexpected content-store descriptor access: {key}",
            )));
        }
        Ok(())
    }
}

impl ObjectStore for ContentStoreAccessLimitStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.head(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}

struct StaleHeadAfterWalWriteStore {
    inner: LocalFsStore,
    head_key: String,
    injected_stale_head: Mutex<bool>,
}

impl StaleHeadAfterWalWriteStore {
    fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            head_key: namespace_head(namespace_id.as_str()),
            injected_stale_head: Mutex::new(false),
        }
    }

    fn injected_stale_head(&self) -> bool {
        *self
            .injected_stale_head
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ObjectStore for StaleHeadAfterWalWriteStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.head_key && matches!(mode, PutMode::CompareAndSwap { .. }) {
            let should_inject = {
                let mut injected = self
                    .injected_stale_head
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *injected {
                    false
                } else {
                    *injected = true;
                    true
                }
            };
            if should_inject {
                if let Some(existing) = self.inner.get(key, None)? {
                    self.inner.put_overwrite(key, &existing)?;
                }
                return Err(ObjectStoreError::PreconditionFailed);
            }
        }
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}
