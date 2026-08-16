//! Commit fingerprinting and sequential resolution of a request's
//! operations into one commit's operations.

use super::intent::{CommitRequest, FilesystemOperation};
use super::plan_attributes::plan_publish_update_attributes;
use super::plan_create::{
    plan_publish_create_directory, plan_publish_put_file_content_ref, plan_publish_undelete,
};
use super::plan_delete::plan_publish_delete_path;
use super::plan_restore::plan_publish_restore_revision;
use super::plan_transfer::{plan_publish_copy_file_path, plan_publish_move_path};
use super::planning_helpers::{PlannedOperation, PublishPathPlanningView};
use crate::commit::{
    validate_ops, CandidateAllocation, CommitFingerprint, OpValidationCursor, PlannedOp,
    PublishValidationView,
};
use crate::error::{CoreError, Result};
use crate::metadata::{MetadataState, MetadataView};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{next_public_ordinal, ChangeSeq, NamespaceId, MAX_PUBLIC_INTEGER};
use loonfs_objectstore::ObjectStore;

/// One mutation request compiled into a commit's operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedCommit {
    pub(crate) ops: Vec<PlannedOp>,
}

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

/// Compiles a mutation request into one ordered commit plan.
///
/// Each operation sees the effects of all earlier operations in the same
/// request. This allows sequences such as creating a directory and then
/// writing into it.
///
/// The first failure aborts the request. Multi-operation requests validate
/// each operation here so the returned error can include its operation
/// index. Single-operation requests rely on the final commit-plan
/// validation.
pub(crate) async fn plan_commit_against_publish_view<S: ObjectStore + ?Sized>(
    request: &CommitRequest,
    head: &HeadState,
    base_view: MetadataView<'_, '_, S>,
    accepted_rows: &MetadataState,
    committed_at_ms: u64,
    allocation: &mut CandidateAllocation,
) -> Result<PlannedCommit> {
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
    let mut cursor = OpValidationCursor::new();
    let mut ops: Vec<PlannedOp> = Vec::new();
    let operation_count = request.operations.len();
    for (index, operation) in request.operations.iter().enumerate() {
        let unit = {
            let resolution_view = resolved.view();
            let view = PublishPathPlanningView {
                metadata_state: &resolution_view,
            };
            plan_operation(operation, &view, allocation)
                .await
                .map_err(|error| attribute(error, index, operation_count))?
        };
        let unit_ops = unit.into_planned_ops();
        if operation_count > 1 {
            validate_ops(
                &unit_ops,
                &mut resolved,
                &mut cursor,
                committed_seq,
                &request.actor,
                committed_at_ms,
            )
            .await
            .map_err(|error| attribute(error, index, operation_count))?;
        }
        ops.extend(unit_ops);
    }

    Ok(PlannedCommit { ops })
}

async fn plan_operation<S: ObjectStore + ?Sized>(
    operation: &FilesystemOperation,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    allocation: &mut CandidateAllocation,
) -> Result<PlannedOperation> {
    match operation {
        FilesystemOperation::CreateDirectory { path, parents } => {
            plan_publish_create_directory(path, *parents, view, allocation).await
        }
        FilesystemOperation::PutFile {
            path,
            content_ref,
            behavior,
            expected_revision_no,
        } => {
            plan_publish_put_file_content_ref(
                path,
                content_ref.clone(),
                *behavior,
                *expected_revision_no,
                view,
                allocation,
            )
            .await
        }
        FilesystemOperation::DeletePath {
            path,
            behavior,
            expected_inode_id,
        } => plan_publish_delete_path(path, *behavior, *expected_inode_id, view).await,
        FilesystemOperation::MovePath {
            from_path,
            to_path,
            behavior,
        } => plan_publish_move_path(from_path, to_path, *behavior, view).await,
        FilesystemOperation::CopyPath {
            from_path,
            to_path,
            behavior,
        } => plan_publish_copy_file_path(from_path, to_path, *behavior, view, allocation).await,
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
    use crate::commit::{
        validate_commit_for_publish, CandidateAllocation, CommitIr, CommitOp,
        CommitValidationError, InodeAllocator, ValidatedCommitPlan,
    };
    use crate::commit_engine::{publish_namespace_commits_batch, CommitCandidate};
    use crate::context::MutationContext;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::path::read::load_current_metadata_view;
    use crate::path::write::ops::{delete_path, put_file_bytes};
    use crate::storage::content::store_bytes_as_content;
    use loonfs_api::{
        AbsolutePath, CommitId, DeleteDirectoryBehavior, DestinationBehavior, InodeId,
    };
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use std::ops::Deref;
    use tempfile::tempdir;

    #[derive(Debug)]
    struct TestPlannedCommit {
        commit: PlannedCommit,
        allocation: CandidateAllocation,
    }

    impl Deref for TestPlannedCommit {
        type Target = PlannedCommit;

        fn deref(&self) -> &Self::Target {
            &self.commit
        }
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

    /// The commit id is the key, not the identity: two requests that differ
    /// only in the id they are filed under are the same mutation, which is
    /// what makes a reused id comparable against a receipt at all.
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

    /// A one-operation convenience call and a one-element batch are the same
    /// request, so they cannot fingerprint differently.
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
    ) -> Result<TestPlannedCommit> {
        let view = load_current_metadata_view(store, namespace_id)
            .await
            .expect("metadata view");
        let empty_overlay = MetadataState::default();
        let allocator = InodeAllocator::new(view.head().next_inode_id);
        let mut allocation = allocator.begin_candidate();
        let commit = plan_commit_against_publish_view(
            request,
            view.head(),
            view.projected_metadata_view(),
            &empty_overlay,
            1,
            &mut allocation,
        )
        .await?;
        Ok(TestPlannedCommit { commit, allocation })
    }

    async fn plan_against_current_state(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        request: &CommitRequest,
    ) -> TestPlannedCommit {
        try_plan_against_current_state(store, namespace_id, request)
            .await
            .expect("plan")
    }

    async fn validate_planned_against_current_state(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        commit_id: &str,
        planned: TestPlannedCommit,
    ) -> Result<ValidatedCommitPlan> {
        let view = load_current_metadata_view(store, namespace_id).await?;
        let empty_overlay = MetadataState::default();
        validate_commit_for_publish(
            &CommitIr {
                namespace_id: namespace_id.clone(),
                commit_id: CommitId::parse(commit_id).expect("valid commit id"),
                actor: loonfs_test_support::test_actor(),
                writer_epoch: view.head().writer_epoch,
                ops: planned.commit.ops,
                message: None,
            },
            1,
            view.head(),
            view.projected_metadata_view(),
            &empty_overlay,
        )
        .await
    }

    async fn publish_operation(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        commit_id: &str,
        operation: FilesystemOperation,
        context: &MutationContext,
    ) -> Result<loonfs_api::CommitResponse> {
        let candidate = CommitCandidate::new(CommitRequest::single(
            CommitId::parse(commit_id).expect("valid commit id"),
            loonfs_test_support::test_actor(),
            None,
            operation,
        ));
        publish_namespace_commits_batch(store, namespace_id, vec![candidate], context)
            .await
            .pop()
            .expect("one candidate produces one outcome")
    }

    #[tokio::test]
    async fn a_planned_directory_create_loses_if_another_commit_claims_the_name() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let planned =
            plan_against_current_state(&store, &namespace_id, &request(create_dir("/docs"))).await;
        publish_operation(
            &store,
            &namespace_id,
            "competing-mkdir",
            create_dir("/docs"),
            &context,
        )
        .await
        .expect("competing create");

        let error =
            validate_planned_against_current_state(&store, &namespace_id, "stale-mkdir", planned)
                .await
                .expect_err("the planned name is no longer free");
        assert!(matches!(
            error,
            CoreError::CommitValidation(CommitValidationError::CreateChildNameCollision { .. })
        ));
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
    async fn a_planned_move_loses_if_the_source_is_rebound() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let seed_commit_id = CommitId::parse("seed-file").expect("valid commit id");
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/a.txt",
            b"hello",
            DestinationBehavior::NoReplace,
            &context,
            Some(&seed_commit_id),
        )
        .await
        .expect("seed file");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &request(FilesystemOperation::MovePath {
                from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
                behavior: DestinationBehavior::NoReplace,
            }),
        )
        .await;
        crate::path::write::ops::move_path(
            &store,
            &namespace_id,
            "/docs/a.txt",
            "/docs/c.txt",
            &context,
            Some(&CommitId::parse("competing-move").expect("valid commit id")),
        )
        .await
        .expect("competing move");

        let error =
            validate_planned_against_current_state(&store, &namespace_id, "stale-move", planned)
                .await
                .expect_err("the source binding changed");
        assert!(matches!(
            error,
            CoreError::CommitValidation(CommitValidationError::BindingPreconditionMissing { .. })
        ));
    }

    #[tokio::test]
    async fn a_planned_copy_loses_if_the_source_revision_changes() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let seed_commit_id = CommitId::parse("seed-copy-source").expect("valid commit id");
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/a.txt",
            b"hello",
            DestinationBehavior::NoReplace,
            &context,
            Some(&seed_commit_id),
        )
        .await
        .expect("seed file");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &request(FilesystemOperation::CopyPath {
                from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                to_path: AbsolutePath::parse("/docs/copy.txt").expect("path"),
                behavior: DestinationBehavior::NoReplace,
            }),
        )
        .await;
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/a.txt",
            b"new contents",
            DestinationBehavior::Replace,
            &context,
            Some(&CommitId::parse("competing-replace").expect("valid commit id")),
        )
        .await
        .expect("competing replace");

        let error =
            validate_planned_against_current_state(&store, &namespace_id, "stale-copy", planned)
                .await
                .expect_err("the source revision changed");
        assert!(matches!(
            error,
            CoreError::CommitValidation(
                CommitValidationError::ReplaceFileBaseRevisionMismatch { .. }
            )
        ));
    }

    #[tokio::test]
    async fn a_planned_attribute_update_loses_if_another_update_publishes_first() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/a.txt",
            b"hello",
            DestinationBehavior::NoReplace,
            &context,
            Some(&CommitId::parse("seed-attributes").expect("valid commit id")),
        )
        .await
        .expect("seed file");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &request(FilesystemOperation::UpdateAttributes {
                path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                set: std::collections::BTreeMap::from([(
                    loonfs_api::AttributeKey::parse("owner").expect("valid attribute key"),
                    loonfs_api::AttributeValue::parse("ada").expect("valid attribute value"),
                )]),
                remove: Vec::new(),
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            }),
        )
        .await;
        publish_operation(
            &store,
            &namespace_id,
            "competing-attributes",
            FilesystemOperation::UpdateAttributes {
                path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                set: std::collections::BTreeMap::from([(
                    loonfs_api::AttributeKey::parse("owner").expect("valid attribute key"),
                    loonfs_api::AttributeValue::parse("grace").expect("valid attribute value"),
                )]),
                remove: Vec::new(),
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            },
            &context,
        )
        .await
        .expect("competing attribute update");

        let error = validate_planned_against_current_state(
            &store,
            &namespace_id,
            "stale-attributes",
            planned,
        )
        .await
        .expect_err("the attribute revision changed");
        assert!(matches!(
            error,
            CoreError::CommitValidation(
                CommitValidationError::UpdateAttributesBaseRevisionMismatch { .. }
            )
        ));
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
                expected_revision_no: None,
            }),
        )
        .await
        .expect("recreating a deleted subtree plans as fresh state");
    }

    /// A batch resolves each operation against what the earlier ones did:
    /// the put walks into a directory that only exists because of the
    /// create ahead of it, and both land in one commit.
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
                        expected_revision_no: None,
                    },
                ],
            },
        )
        .await;

        assert_eq!(planned.ops.len(), 2);
        assert!(matches!(
            &planned.ops[0].op,
            CommitOp::CreateDirectory {
                child_inode_id: InodeId(2),
                parent_inode_id: InodeId(1),
                ..
            }
        ));
        // The put binds under the directory the create allocated rather than
        // re-creating it, and validation receives the exact ID planning
        // assigned to the file.
        assert!(matches!(
            &planned.ops[1].op,
            CommitOp::CreateFile {
                child_inode_id: InodeId(3),
                parent_inode_id,
                display_name,
                ..
            } if *parent_inode_id == InodeId(2) && display_name.as_str() == "a.txt"
        ));
        assert_eq!(planned.allocation.resulting_next_inode_id(), InodeId(4));
    }

    /// A batch that deletes a path and recreates it resolves the create
    /// against the delete, not against the state the request started from.
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
                        expected_revision_no: None,
                    },
                ],
            },
        )
        .await;

        // The name is free once the delete is applied, so the put creates a
        // fresh file rather than failing with a destination conflict.
        assert!(matches!(planned.ops[0].op, CommitOp::DeleteFile { .. }));
        assert!(matches!(
            &planned.ops[1].op,
            CommitOp::CreateFile { display_name, .. } if display_name.as_str() == "tmp.txt"
        ));
    }

    /// The first failing operation aborts the request and names its own
    /// position.
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
