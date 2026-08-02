//! Commit fingerprinting and sequential resolution of a request's
//! operations into one commit's operations.

use super::intent::{CommitRequest, FilesystemOperation};
use super::plan_create::{
    plan_publish_create_directory, plan_publish_put_file_content_ref, plan_publish_undelete,
};
use super::plan_delete::plan_publish_delete_path;
use super::plan_restore::plan_publish_restore_revision;
use super::plan_transfer::{plan_publish_copy_file_path, plan_publish_move_path};
use super::planning_helpers::{PlannedOperation, PublishPathPlanningView};
use crate::commit::{
    allocates_inode, validate_ops, CommitFingerprint, CommitOp, OpValidationCursor, PlannedOp,
    PublishValidationView,
};
use crate::error::{CoreError, Result};
use crate::metadata::{MetadataState, MetadataView};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{ChangeSeq, InodeId, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// One mutation request compiled into a commit's operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedCommit {
    pub(crate) ops: Vec<PlannedOp>,
    /// The next free inode id the planner predicted while resolving. The
    /// commit plan recomputes it from the operation list; a disagreement
    /// means prediction and allocation drifted.
    pub(crate) resulting_next_inode_id: InodeId,
}

/// The semantic identity of one mutation request.
///
/// The fingerprint itself is `loonfs-api`'s: the crate that owns the one
/// operation language owns its identity function, because the HTTP client
/// has to compute the same value to reconcile a reused-id conflict and does
/// not depend on this crate. All this adds is core's vocabulary — the
/// request shape on the way in, `CommitFingerprint` and `CoreError` on the
/// way out.
///
/// The commit id is not an input. It is the key the mutation is filed
/// under; the fingerprint is what was filed.
pub(crate) fn commit_fingerprint(
    namespace_id: &NamespaceId,
    request: &CommitRequest,
) -> Result<CommitFingerprint> {
    loonfs_api::semantic_commit_fingerprint(
        namespace_id,
        request.message.as_deref(),
        &request.operations,
    )
    .map(CommitFingerprint::new_unchecked)
    .map_err(|err| CoreError::Internal(format!("failed to fingerprint mutation: {err}")))
}

/// Compiles one mutation request into the operations of a single commit.
///
/// Operations resolve in order. Operation `k` reads a metadata view that
/// already carries what operations `0..k` would persist, so a request can
/// create a directory and write into it, or delete a path and recreate it.
/// Those effects are computed by the same op validation the commit plan
/// performs, so resolution and validation cannot disagree about them.
///
/// The first operation that fails aborts the whole request, and its error
/// names the operation's position. Naming the position is why a batch
/// validates each operation here as well as in the commit plan: the plan
/// validates the whole operation list at once and has no request-level
/// position to report. A one-operation request skips the extra pass — it has
/// one place to fail, and the plan is the only validation it needs.
pub(crate) async fn plan_commit_against_publish_view<S: ObjectStore + ?Sized>(
    request: &CommitRequest,
    head: &HeadState,
    base_view: MetadataView<'_, '_, S>,
    accepted_rows: &MetadataState,
    committed_at_ms: u64,
) -> Result<PlannedCommit> {
    if request.operations.is_empty() {
        return Err(CoreError::InvalidCommitRequest(
            "mutation request carries no operations".to_owned(),
        ));
    }
    let committed_seq = head
        .seq
        .0
        .checked_add(1)
        .map(ChangeSeq)
        .ok_or_else(|| CoreError::Internal("namespace sequence overflow".to_owned()))?;

    let mut resolved = PublishValidationView::new(base_view, accepted_rows, committed_seq);
    let mut cursor = OpValidationCursor::new();
    let mut next_inode_id = head.next_inode_id;
    let mut ops: Vec<PlannedOp> = Vec::new();
    let operation_count = request.operations.len();
    for (index, operation) in request.operations.iter().enumerate() {
        let unit = {
            let resolution_view = resolved.view();
            let view = PublishPathPlanningView {
                next_inode_id,
                metadata_state: &resolution_view,
            };
            plan_operation(operation, &view)
                .await
                .map_err(|error| attribute(error, index, operation_count))?
        };
        let unit_ops = unit.into_planned_ops();
        let allocated = allocate_inode_ids(&unit_ops, &mut next_inode_id)?;
        debug_assert_parents_are_allocated(&unit_ops, next_inode_id);
        if operation_count > 1 {
            validate_ops(
                &unit_ops,
                &mut resolved,
                &mut cursor,
                committed_seq,
                committed_at_ms,
                &mut allocated.iter().copied(),
            )
            .await
            .map_err(|error| attribute(error, index, operation_count))?;
        }
        ops.extend(unit_ops);
    }

    Ok(PlannedCommit {
        ops,
        resulting_next_inode_id: next_inode_id,
    })
}

async fn plan_operation<S: ObjectStore + ?Sized>(
    operation: &FilesystemOperation,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<PlannedOperation> {
    match operation {
        FilesystemOperation::CreateDirectory { path, parents } => {
            plan_publish_create_directory(path, *parents, view).await
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
        } => plan_publish_copy_file_path(from_path, to_path, *behavior, view).await,
        FilesystemOperation::RestoreRevision {
            path,
            source_revision_no,
        } => plan_publish_restore_revision(path, *source_revision_no, view).await,
        FilesystemOperation::Undelete {
            inode_id,
            deleted_at_seq,
            path,
        } => plan_publish_undelete(*inode_id, *deleted_at_seq, path.as_ref(), view).await,
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

/// Hands out the inode ids the commit plan will allocate for `ops`, in
/// operation order, and advances the running counter.
///
/// One counter serves the whole request: it is what the planner predicts
/// parent ids from and what the commit plan re-derives, so the two cannot
/// number the same creation differently.
fn allocate_inode_ids(ops: &[PlannedOp], next_inode_id: &mut InodeId) -> Result<Vec<InodeId>> {
    let mut allocated = Vec::new();
    for planned in ops {
        if !allocates_inode(&planned.op) {
            continue;
        }
        allocated.push(*next_inode_id);
        *next_inode_id = next_inode_id
            .0
            .checked_add(1)
            .map(InodeId)
            .ok_or_else(|| CoreError::Internal("next inode id counter overflow".to_owned()))?;
    }
    Ok(allocated)
}

/// Every parent an operation names is either an inode that existed before
/// this request or one the request has already allocated, so no parent id can
/// be at or past the running counter. A violation would mean the planner
/// predicted an id the commit plan never hands out, and the operation would
/// be parented under nothing.
fn debug_assert_parents_are_allocated(ops: &[PlannedOp], next_inode_id: InodeId) {
    debug_assert!(
        ops.iter().all(|planned| {
            let parent = match &planned.op {
                CommitOp::CreateDirectory {
                    parent_inode_id, ..
                }
                | CommitOp::CreateFile {
                    parent_inode_id, ..
                }
                | CommitOp::Undelete {
                    parent_inode_id, ..
                } => *parent_inode_id,
                CommitOp::Rename {
                    new_parent_inode_id,
                    ..
                } => *new_parent_inode_id,
                _ => return true,
            };
            parent < next_inode_id
        }),
        "planner predicted an inode id the commit plan has not allocated"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitPrecondition;
    use crate::context::MutationContext;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::path::write::ops::{delete_path, put_file_bytes};
    use crate::protocol::{load_publish_metadata_view, PublishTailOptions};
    use crate::storage::content::store_bytes_as_content;
    use loonfs_api::{
        AbsolutePath, CommitId, DeleteDirectoryBehavior, DestinationBehavior, RevisionNo,
    };
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    fn test_context() -> MutationContext {
        MutationContext {
            writer_id: "writer".to_owned(),
            now_ms: 1,
        }
    }

    fn request(operation: FilesystemOperation) -> CommitRequest {
        CommitRequest::single(
            CommitId::parse("plan-request").expect("valid commit id"),
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
                None,
                create_dir("/docs/a"),
            ),
        )
        .expect("left fingerprint");
        let right = commit_fingerprint(
            &namespace_id,
            &CommitRequest::single(
                CommitId::parse("mkdir-docs-b").expect("valid commit id"),
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
        let convenience = CommitRequest::single(commit_id.clone(), None, create_dir("/docs"));
        let batch = CommitRequest {
            commit_id,
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
    ) -> Result<PlannedCommit> {
        let (view, _projection) = load_publish_metadata_view(
            store,
            None,
            namespace_id,
            None,
            None,
            &PublishTailOptions::default(),
        )
        .await
        .expect("publish view");
        let empty_overlay = MetadataState::default();
        plan_commit_against_publish_view(
            request,
            view.head(),
            view.metadata_view(),
            &empty_overlay,
            1,
        )
        .await
    }

    async fn plan_against_current_state(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        request: &CommitRequest,
    ) -> PlannedCommit {
        try_plan_against_current_state(store, namespace_id, request)
            .await
            .expect("plan")
    }

    #[tokio::test]
    async fn create_directory_plan_contains_semantic_op_and_target_absence_precondition() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace().await;
        let planned =
            plan_against_current_state(&store, &namespace_id, &request(create_dir("/docs"))).await;

        assert_eq!(planned.ops.len(), 1);
        assert_eq!(
            planned.ops[0].op,
            CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
            }
        );
        assert!(planned.ops[0]
            .preconditions
            .iter()
            .any(|precondition| matches!(
                precondition,
                CommitPrecondition::ChildNameAbsent {
                    parent_inode_id: InodeId(1),
                    name_key,
                } if name_key.as_str() == "docs"
            )));
    }

    #[tokio::test]
    async fn put_file_plan_auto_creates_missing_parent_directories() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace().await;
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello")
            .await
            .expect("stage");
        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &request(FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/docs/nested/a.txt").expect("path"),
                content_ref: staged.content_ref.clone(),
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            }),
        )
        .await;

        assert_eq!(planned.ops.len(), 3);
        assert!(matches!(
            &planned.ops[0].op,
            CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name,
            } if display_name.as_str() == "docs"
        ));
        assert!(matches!(
            &planned.ops[1].op,
            CommitOp::CreateDirectory { display_name, .. } if display_name.as_str() == "nested"
        ));
        assert!(matches!(
            &planned.ops[2].op,
            CommitOp::CreateFile {
                display_name,
                content_ref,
                ..
            } if display_name.as_str() == "a.txt" && content_ref == &staged.content_ref
        ));
    }

    #[tokio::test]
    async fn move_path_plan_contains_binding_and_target_absence_preconditions() {
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

        assert!(matches!(
            planned.ops.as_slice(),
            [PlannedOp {
                op: CommitOp::Rename {
                    new_display_name,
                    ..
                },
                ..
            }] if new_display_name.as_str() == "b.txt"
        ));
        assert!(planned.ops[0]
            .preconditions
            .iter()
            .any(|precondition| matches!(precondition, CommitPrecondition::BindingIs { .. })));
        assert!(planned.ops[0]
            .preconditions
            .iter()
            .any(|precondition| matches!(
                precondition,
                CommitPrecondition::ChildNameAbsent { name_key, .. } if name_key.as_str() == "b.txt"
            )));
    }

    #[tokio::test]
    async fn copy_file_plan_validates_source_revision_and_target_absence() {
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

        assert!(matches!(
            planned.ops.as_slice(),
            [PlannedOp {
                op: CommitOp::CreateFile { display_name, .. },
                ..
            }] if display_name.as_str() == "copy.txt"
        ));
        assert!(planned.ops[0]
            .preconditions
            .iter()
            .any(|precondition| matches!(
                precondition,
                CommitPrecondition::InodeRevisionIs {
                    revision_no: RevisionNo(1),
                    ..
                }
            )));
        assert!(planned.ops[0]
            .preconditions
            .iter()
            .any(|precondition| matches!(
                precondition,
                CommitPrecondition::ChildNameAbsent { name_key, .. } if name_key.as_str() == "copy.txt"
            )));
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
                content_ref: staged.content_ref,
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
                message: None,
                operations: vec![
                    create_dir("/reports"),
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/reports/a.txt").expect("path"),
                        content_ref: staged.content_ref.clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_revision_no: None,
                    },
                ],
            },
        )
        .await;

        assert_eq!(planned.ops.len(), 2);
        // The put binds under the directory the create allocated rather than
        // re-creating it.
        assert!(matches!(
            &planned.ops[1].op,
            CommitOp::CreateFile {
                parent_inode_id,
                display_name,
                ..
            } if *parent_inode_id == InodeId(2) && display_name.as_str() == "a.txt"
        ));
        assert_eq!(planned.resulting_next_inode_id, InodeId(4));
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
                message: None,
                operations: vec![
                    FilesystemOperation::DeletePath {
                        path: AbsolutePath::parse("/docs/tmp.txt").expect("path"),
                        behavior: DeleteDirectoryBehavior::NonRecursive,
                        expected_inode_id: None,
                    },
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/tmp.txt").expect("path"),
                        content_ref: staged.content_ref.clone(),
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
                message: None,
                operations: Vec::new(),
            },
        )
        .await
        .expect_err("an empty request has nothing to commit");

        assert_eq!(error.code(), crate::error::ErrorCode::InvalidRequest);
    }
}
