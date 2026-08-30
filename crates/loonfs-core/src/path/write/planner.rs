//! Commit fingerprinting and sequential resolution of a request's
//! operations into one commit's operations.

use super::intent::{CommitRequest, FilesystemOperation};
use super::plan_attributes::plan_publish_update_attributes;
use super::plan_by_inode::{
    plan_publish_create_by_inode, plan_publish_delete_by_inode, plan_publish_move_by_inode,
    plan_publish_put_file_revision_by_inode, NewChild,
};
use super::plan_create::{
    plan_publish_create_directory, plan_publish_put_file_content_ref, plan_publish_undelete,
};
use super::plan_delete::plan_publish_delete_path;
use super::plan_restore::plan_publish_restore_revision;
use super::plan_transfer::{plan_publish_copy_file_path, plan_publish_move_path};
use super::publish_path_planning::{CompiledFilesystemOperation, PublishPathPlanningView};
use super::validate_expected_file_state;
use crate::commit::{
    validate_ops, CandidateAllocation, CommitFingerprint, CommitNumbering, PublishValidationView,
    ValidatedCommitPlan, ValidatedOp,
};
use crate::error::{CoreError, Result};
use crate::metadata::{MetadataState, MetadataView};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{next_public_ordinal, ChangeSeq, NamespaceId, MAX_PUBLIC_INTEGER};
use loonfs_objectstore::ObjectStore;

/// Computes the semantic fingerprint of a mutation request.
///
/// `loonfs-api` owns the fingerprint algorithm so clients and the core use
/// the same definition. The commit ID is excluded: it identifies the request
/// record, while the fingerprint identifies the requested mutation.
pub(crate) fn commit_fingerprint(
    namespace_id: &NamespaceId,
    request: &CommitRequest,
) -> Result<CommitFingerprint> {
    loonfs_api::semantic_commit_fingerprint(
        namespace_id,
        &request.actor,
        request.message.as_deref(),
        &request.operations,
    )
    .map(CommitFingerprint::new_unchecked)
    .map_err(|err| CoreError::Internal(format!("failed to fingerprint mutation: {err}")))
}

/// Compiles and validates a mutation request into one commit in a single
/// pass.
///
/// Each semantic operation is planned against the view and its compiled
/// operations are immediately validated, with accepted effects applied to the
/// shared view so later operations observe earlier ones. The first failure
/// aborts the request; multi-operation requests attach the failing
/// operation's index, while single-operation requests return the raw error.
///
/// The commit's identity moves into the returned [`ValidatedCommitPlan`]:
/// the head is the already-guarded source of the namespace and writer epoch
/// (`load_publish_metadata_view` fences on epoch equality, and the batch
/// rejects namespace mismatches before admission).
pub(crate) async fn prepare_commit_against_publish_view<S: ObjectStore + ?Sized>(
    request: &CommitRequest,
    semantic_identity: CommitFingerprint,
    head: &HeadState,
    base_view: MetadataView<'_, '_, S>,
    accepted_rows: &MetadataState,
    committed_at_ms: u64,
    allocation: &mut CandidateAllocation,
) -> Result<ValidatedCommitPlan> {
    if request.operations.is_empty() {
        return Err(CoreError::InvalidCommitRequest(
            "mutation request carries no operations".to_owned(),
        ));
    }
    let committed_seq = next_public_ordinal(head.seq.0)
        .map(ChangeSeq)
        .ok_or_else(|| {
            CoreError::Internal(format!(
                "namespace sequence cannot exceed {MAX_PUBLIC_INTEGER}"
            ))
        })?;

    let mut resolved = PublishValidationView::new(base_view, accepted_rows, committed_seq);
    let mut numbering = CommitNumbering::default();
    let mut validated_ops: Vec<ValidatedOp> = Vec::new();
    let operation_count = request.operations.len();
    for (index, operation) in request.operations.iter().enumerate() {
        let unit = {
            let resolution_view = resolved.view();
            let view = PublishPathPlanningView {
                namespace_id: &head.namespace_id,
                metadata_state: &resolution_view,
            };
            plan_operation(operation, &view, allocation)
                .await
                .map_err(|error| attribute(error, index, operation_count))?
        };
        let unit_ops = unit.into_planned_ops();
        let validated_unit = validate_ops(
            &unit_ops,
            &mut resolved,
            &mut numbering,
            committed_seq,
            &request.commit_id,
            &request.actor,
            committed_at_ms,
        )
        .await
        .map_err(|error| attribute(error, index, operation_count))?;
        validated_ops.extend(validated_unit);
    }

    Ok(ValidatedCommitPlan {
        namespace_id: head.namespace_id.clone(),
        commit_id: request.commit_id.clone(),
        actor: request.actor.clone(),
        writer_epoch: head.writer_epoch,
        message: request.message.clone(),
        semantic_identity,
        apply_after_seq: head.seq,
        assigned_seq: committed_seq,
        validated_ops,
    })
}

async fn plan_operation<S: ObjectStore + ?Sized>(
    operation: &FilesystemOperation,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    allocation: &mut CandidateAllocation,
) -> Result<CompiledFilesystemOperation> {
    match operation {
        FilesystemOperation::CreateDirectory { path, parents } => {
            plan_publish_create_directory(path, *parents, view, allocation).await
        }
        FilesystemOperation::PutFile {
            path,
            content_ref,
            behavior,
            expected_inode_id,
            expected_revision_no,
        } => {
            plan_publish_put_file_content_ref(
                path,
                content_ref.clone(),
                *behavior,
                validate_expected_file_state(
                    *behavior,
                    *expected_inode_id,
                    *expected_revision_no,
                    "expected_revision_no",
                    "expected_inode_id",
                )?,
                view,
                allocation,
            )
            .await
        }
        FilesystemOperation::CreateDirectoryByInode {
            parent_inode_id,
            display_name,
        } => {
            plan_publish_create_by_inode(
                *parent_inode_id,
                display_name,
                NewChild::Directory,
                view,
                allocation,
            )
            .await
        }
        FilesystemOperation::PutFileByInode {
            parent_inode_id,
            display_name,
            content_ref,
        } => {
            plan_publish_create_by_inode(
                *parent_inode_id,
                display_name,
                NewChild::File(content_ref.clone()),
                view,
                allocation,
            )
            .await
        }
        FilesystemOperation::PutFileRevisionByInode {
            inode_id,
            content_ref,
            expected_revision_no,
        } => {
            plan_publish_put_file_revision_by_inode(
                *inode_id,
                content_ref.clone(),
                *expected_revision_no,
                view,
            )
            .await
        }
        FilesystemOperation::DeletePath {
            path,
            behavior,
            expected_inode_id,
        } => plan_publish_delete_path(path, *behavior, *expected_inode_id, view).await,
        FilesystemOperation::DeleteByInode {
            inode_id,
            expected_binding_generation,
            behavior,
        } => {
            plan_publish_delete_by_inode(*inode_id, expected_binding_generation, *behavior, view)
                .await
        }
        FilesystemOperation::MoveByInode {
            inode_id,
            expected_binding_generation,
            to_parent_inode_id,
            to_display_name,
            behavior,
            expected_destination_inode_id,
            expected_destination_revision_no,
        } => {
            plan_publish_move_by_inode(
                *inode_id,
                expected_binding_generation,
                *to_parent_inode_id,
                to_display_name,
                *behavior,
                validate_expected_file_state(
                    *behavior,
                    *expected_destination_inode_id,
                    *expected_destination_revision_no,
                    "expected_destination_revision_no",
                    "expected_destination_inode_id",
                )?,
                view,
            )
            .await
        }
        FilesystemOperation::MovePath {
            from_path,
            to_path,
            behavior,
            expected_destination_inode_id,
            expected_destination_revision_no,
        } => {
            plan_publish_move_path(
                from_path,
                to_path,
                *behavior,
                validate_expected_file_state(
                    *behavior,
                    *expected_destination_inode_id,
                    *expected_destination_revision_no,
                    "expected_destination_revision_no",
                    "expected_destination_inode_id",
                )?,
                view,
            )
            .await
        }
        FilesystemOperation::CopyPath {
            from_path,
            to_path,
            behavior,
            expected_destination_inode_id,
            expected_destination_revision_no,
        } => {
            plan_publish_copy_file_path(
                from_path,
                to_path,
                *behavior,
                validate_expected_file_state(
                    *behavior,
                    *expected_destination_inode_id,
                    *expected_destination_revision_no,
                    "expected_destination_revision_no",
                    "expected_destination_inode_id",
                )?,
                view,
                allocation,
            )
            .await
        }
        FilesystemOperation::RestoreRevision {
            path,
            source_revision_no,
        } => plan_publish_restore_revision(path, *source_revision_no, view).await,
        FilesystemOperation::Undelete {
            inode_id,
            deletion_seq,
            path,
        } => plan_publish_undelete(*inode_id, *deletion_seq, path.as_ref(), view).await,
        FilesystemOperation::UpdateAttributes {
            path,
            set,
            remove,
            expected_inode_id,
            expected_attributes_revision_no,
        } => {
            plan_publish_update_attributes(
                path,
                set,
                remove,
                *expected_inode_id,
                *expected_attributes_revision_no,
                view,
            )
            .await
        }
    }
}

/// Names the operation a batch stopped at.
///
/// A one-operation request has one place to fail, so its error stays exactly
/// what the operation produced; there is nothing to disambiguate.
fn attribute(error: CoreError, index: usize, operation_count: usize) -> CoreError {
    if operation_count < 2 {
        return error;
    }
    error.at_operation(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CandidateAllocation, InodeAllocator};
    use crate::context::MutationContext;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::path::read::load_current_metadata_view;
    use crate::storage::content::store_bytes_as_content;
    use crate::test_support::ops::{delete_path, put_file_bytes};
    use loonfs_api::{
        AbsolutePath, CommitId, DeleteDirectoryBehavior, DestinationBehavior, InodeId,
    };
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    #[derive(Debug)]
    struct TestPreparedCommit {
        ops: Vec<ValidatedOp>,
        allocation: CandidateAllocation,
    }

    fn test_context() -> MutationContext {
        MutationContext {
            writer_id: "writer".to_owned(),
            now_ms: 1,
        }
    }

    fn request(operation: FilesystemOperation) -> CommitRequest {
        CommitRequest::single(
            CommitId::parse("plan-request").expect("valid commit id"),
            loonfs_test_support::test_actor(),
            None,
            operation,
        )
    }

    fn create_dir(path: &str) -> FilesystemOperation {
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(path).expect("path"),
            parents: false,
        }
    }

    #[test]
    fn commit_fingerprint_is_stable_for_canonical_paths() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let left = commit_fingerprint(
            &namespace_id,
            &CommitRequest::single(
                CommitId::parse("mkdir-docs-a").expect("valid commit id"),
                loonfs_test_support::test_actor(),
                None,
                create_dir("/docs/a"),
            ),
        )
        .expect("left fingerprint");
        let right = commit_fingerprint(
            &namespace_id,
            &CommitRequest::single(
                CommitId::parse("mkdir-docs-b").expect("valid commit id"),
                loonfs_test_support::test_actor(),
                None,
                create_dir("/docs/a"),
            ),
        )
        .expect("right fingerprint");

        assert_eq!(left, right);
    }

    #[test]
    fn one_operation_request_and_one_element_batch_share_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let commit_id = CommitId::parse("mkdir-docs").expect("valid commit id");
        let convenience = CommitRequest::single(
            commit_id.clone(),
            loonfs_test_support::test_actor(),
            None,
            create_dir("/docs"),
        );
        let batch = CommitRequest {
            commit_id,
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![create_dir("/docs")],
        };

        assert_eq!(
            commit_fingerprint(&namespace_id, &convenience).expect("convenience fingerprint"),
            commit_fingerprint(&namespace_id, &batch).expect("batch fingerprint")
        );
    }

    async fn setup_namespace() -> (
        tempfile::TempDir,
        LocalFsStore,
        NamespaceId,
        MutationContext,
    ) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");
        (temp_dir, store, namespace_id, context)
    }

    async fn try_plan_against_current_state(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        request: &CommitRequest,
    ) -> Result<TestPreparedCommit> {
        let view = load_current_metadata_view(store, namespace_id)
            .await
            .expect("metadata view");
        let empty_overlay = MetadataState::default();
        let allocator = InodeAllocator::new(view.head().next_inode_id);
        let mut allocation = allocator.begin_candidate();
        let validated = prepare_commit_against_publish_view(
            request,
            CommitFingerprint::new_unchecked("v1:sha256:test".to_owned()),
            view.head(),
            view.projected_metadata_view(),
            &empty_overlay,
            1,
            &mut allocation,
        )
        .await?;
        Ok(TestPreparedCommit {
            ops: validated.validated_ops,
            allocation,
        })
    }

    async fn plan_against_current_state(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        request: &CommitRequest,
    ) -> TestPreparedCommit {
        try_plan_against_current_state(store, namespace_id, request)
            .await
            .expect("plan")
    }

    #[tokio::test]
    async fn put_file_creates_missing_parent_directories() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/nested/a.txt",
            b"hello",
            DestinationBehavior::NoReplace,
            &context,
            Some(&CommitId::parse("put-with-parents").expect("valid commit id")),
        )
        .await
        .expect("put file");

        let view = load_current_metadata_view(&store, &namespace_id)
            .await
            .expect("metadata view");
        let resolved = view
            .projected_metadata_view()
            .resolve_visible_path(&AbsolutePath::parse("/docs/nested/a.txt").expect("path"))
            .await
            .expect("resolve created file");
        assert_eq!(resolved.absolute_path, "/docs/nested/a.txt");
    }

    #[tokio::test]
    async fn recreate_after_delete_succeeds_at_the_same_path() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/tmp.txt",
            b"first",
            DestinationBehavior::NoReplace,
            &context,
            Some(&CommitId::parse("recreate-seed").expect("valid commit id")),
        )
        .await
        .expect("seed file");
        delete_path(
            &store,
            &namespace_id,
            "/docs/tmp.txt",
            &context,
            Some(&CommitId::parse("recreate-delete").expect("valid commit id")),
        )
        .await
        .expect("delete file");

        // The tombstone covers the dead inode, not the name: the name is
        // reusable immediately, with or without an intervening rebuild.
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/tmp.txt",
            b"second",
            DestinationBehavior::NoReplace,
            &context,
            Some(&CommitId::parse("recreate-put").expect("valid commit id")),
        )
        .await
        .expect("recreate at the deleted path");
    }

    #[tokio::test]
    async fn deleted_subtree_names_replan_as_fresh_state() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let seed_commit_id = CommitId::parse("seed-dead-tree").expect("valid commit id");
        put_file_bytes(
            &store,
            &namespace_id,
            "/dead/file.txt",
            b"hello",
            DestinationBehavior::NoReplace,
            &context,
            Some(&seed_commit_id),
        )
        .await
        .expect("seed file");
        let delete_commit_id = CommitId::parse("delete-dead-tree").expect("valid commit id");
        delete_path(
            &store,
            &namespace_id,
            "/dead",
            &context,
            Some(&delete_commit_id),
        )
        .await
        .expect("delete tree");
        let staged = store_bytes_as_content(&store, &namespace_id, b"new")
            .await
            .expect("stage");
        // The deleted name is invisible, so planning under it starts a
        // fresh subtree instead of conflicting with the dead one — the same
        // answer callers get after compaction drops the dead rows.
        try_plan_against_current_state(
            &store,
            &namespace_id,
            &request(FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/dead/new.txt").expect("path"),
                content_ref: staged.into_content_ref(),
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            }),
        )
        .await
        .expect("recreating a deleted subtree plans as fresh state");
    }

    #[tokio::test]
    async fn later_operations_resolve_against_earlier_ones() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace().await;
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello")
            .await
            .expect("stage");
        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &CommitRequest {
                commit_id: CommitId::parse("batch-create-then-put").expect("valid commit id"),
                actor: loonfs_test_support::test_actor(),
                message: None,
                operations: vec![
                    create_dir("/reports"),
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/reports/a.txt").expect("path"),
                        content_ref: staged.content_ref().clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ],
            },
        )
        .await;

        assert_eq!(planned.ops.len(), 2);
        assert!(matches!(
            &planned.ops[0],
            ValidatedOp::CreateDir {
                child_inode_id: InodeId(2),
                parent_inode_id: InodeId(1),
                ..
            }
        ));
        // The put binds under the directory the create allocated rather than
        // re-creating it, and validation receives the exact ID planning
        // assigned to the file.
        assert!(matches!(
            &planned.ops[1],
            ValidatedOp::CreateFile {
                child_inode_id: InodeId(3),
                parent_inode_id,
                display_name,
                ..
            } if *parent_inode_id == InodeId(2) && display_name.as_str() == "a.txt"
        ));
        assert_eq!(planned.allocation.resulting_next_inode_id(), InodeId(4));
    }

    #[tokio::test]
    async fn delete_then_create_resolves_against_the_delete() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/tmp.txt",
            b"first",
            DestinationBehavior::NoReplace,
            &context,
            Some(&CommitId::parse("seed-replaceable").expect("valid commit id")),
        )
        .await
        .expect("seed file");
        let staged = store_bytes_as_content(&store, &namespace_id, b"second")
            .await
            .expect("stage");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &CommitRequest {
                commit_id: CommitId::parse("batch-delete-then-create").expect("valid commit id"),
                actor: loonfs_test_support::test_actor(),
                message: None,
                operations: vec![
                    FilesystemOperation::DeletePath {
                        path: AbsolutePath::parse("/docs/tmp.txt").expect("path"),
                        behavior: DeleteDirectoryBehavior::NonRecursive,
                        expected_inode_id: None,
                    },
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/tmp.txt").expect("path"),
                        content_ref: staged.content_ref().clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ],
            },
        )
        .await;

        // The name is free once the delete is applied, so the put creates a
        // fresh file rather than failing with a destination conflict.
        assert!(matches!(planned.ops[0], ValidatedOp::DeleteFile { .. }));
        assert!(matches!(
            &planned.ops[1],
            ValidatedOp::CreateFile { display_name, .. } if display_name.as_str() == "tmp.txt"
        ));
    }

    #[tokio::test]
    async fn a_failing_operation_names_its_position() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace().await;
        let error = try_plan_against_current_state(
            &store,
            &namespace_id,
            &CommitRequest {
                commit_id: CommitId::parse("batch-with-a-bad-op").expect("valid commit id"),
                actor: loonfs_test_support::test_actor(),
                message: None,
                operations: vec![
                    create_dir("/first"),
                    FilesystemOperation::DeletePath {
                        path: AbsolutePath::parse("/missing").expect("path"),
                        behavior: DeleteDirectoryBehavior::NonRecursive,
                        expected_inode_id: None,
                    },
                    create_dir("/third"),
                ],
            },
        )
        .await
        .expect_err("the delete cannot resolve");

        assert_eq!(
            error
                .details()
                .expect("failing-operation details")
                .operation_index,
            Some(1)
        );
    }

    #[tokio::test]
    async fn an_empty_request_is_rejected() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace().await;
        let error = try_plan_against_current_state(
            &store,
            &namespace_id,
            &CommitRequest {
                commit_id: CommitId::parse("empty-request").expect("valid commit id"),
                actor: loonfs_test_support::test_actor(),
                message: None,
                operations: Vec::new(),
            },
        )
        .await
        .expect_err("an empty request has nothing to commit");

        assert_eq!(error.code(), crate::error::ErrorCode::InvalidRequest);
    }
}
