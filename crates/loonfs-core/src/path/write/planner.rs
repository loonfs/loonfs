//! Commit fingerprinting and the planning of one filesystem operation into
//! the commit operations it needs.

use super::intent::{CommitIntent, FilesystemOperation};
use super::plan_create::undelete_destination;
use super::planned_pieces::CommitPieces;
use super::preconditions::{
    validate_access_precondition, validate_attributes_precondition, validate_expected_file_state,
    PreconditionFields,
};
use super::publish_path_planning::{
    CompiledFilesystemOperation, Destination, PlanningView, Target,
};
use crate::commit::{CandidateAllocation, CommitFingerprint, NewChild};
use crate::error::{CoreError, Result};
use crate::storage::tail_content::ContentWrites;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    CommitFingerprintInput, ContentId, DestinationBehavior, FileContent, InlineContentIdentity,
    NamespaceId,
};
use std::collections::BTreeMap;

/// Computes the semantic fingerprint of a mutation request.
///
/// The commit ID is excluded: it identifies the request record, while the
/// fingerprint identifies the requested mutation.
pub(crate) fn commit_fingerprint(
    namespace_id: &NamespaceId,
    request: &CommitIntent,
    inline_content_identities: &BTreeMap<ContentId, InlineContentIdentity>,
) -> CommitFingerprint {
    CommitFingerprintInput {
        namespace_id,
        actor_id: &request.actor_id,
        subject_id: request.subject.as_ref().map(|subject| &subject.subject_id),
        message: request.message.as_deref(),
        operations: &request.operations,
        preconditions: &request.preconditions,
        inline_content_identities,
    }
    .fingerprint()
}

/// The planning of one operation: the planning view, the commit's pieces,
/// where content writes go, and the candidate's inode allocation.
pub(super) struct OperationPlanningSession<'p, S: ObjectStore + ?Sized> {
    pub(super) view: PlanningView<'p, S>,
    pub(super) pieces: &'p CommitPieces<'p, S>,
    pub(super) content_writes: ContentWrites<'p, S>,
    pub(super) allocation: &'p mut CandidateAllocation,
}

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, S> {
    /// Plans `operation`, mapping its path or inode address onto the one
    /// planner for its kind.
    pub(super) async fn plan(
        &mut self,
        operation: &FilesystemOperation,
    ) -> Result<CompiledFilesystemOperation> {
        use FilesystemOperation as Op;
        match operation {
            Op::CreateDirectory { path, parents } => {
                self.plan_create_directory(path, *parents).await
            }
            Op::CreateDirectoryByInode {
                parent_inode_id,
                display_name,
            } => {
                let destination = Destination::name(*parent_inode_id, display_name);
                self.plan_create(destination, NewChild::Directory).await
            }
            Op::CreateFileByInode {
                parent_inode_id,
                display_name,
                content,
            } => {
                let child = NewChild::File(prepared_reference(content)?.clone());
                let destination = Destination::name(*parent_inode_id, display_name);
                self.plan_create(destination, child).await
            }
            Op::PutFile {
                path,
                content,
                behavior,
                expected_inode_id,
                expected_revision_no,
            } => {
                let expected = validate_expected_file_state(
                    *behavior,
                    *expected_inode_id,
                    *expected_revision_no,
                    PreconditionFields::Put,
                )?;
                let content_ref = prepared_reference(content)?.clone();
                self.plan_put_file_content_ref(path, content_ref, *behavior, expected)
                    .await
            }
            Op::PutFileRevisionByInode {
                inode_id,
                content,
                expected_revision_no,
            } => {
                let content_ref = prepared_reference(content)?.clone();
                self.plan_put_file_revision_by_inode(*inode_id, content_ref, *expected_revision_no)
                    .await
            }
            Op::AppendFile {
                path,
                inline_content,
                expected_inode_id,
                expected_revision_no,
            } => {
                let expected = validate_expected_file_state(
                    DestinationBehavior::Replace,
                    *expected_inode_id,
                    *expected_revision_no,
                    PreconditionFields::Put,
                )?;
                let target = Target::path(path, expected.map(|expected| expected.inode_id));
                let expected_revision_no = expected.and_then(|expected| expected.revision_no);
                self.plan_append(target, expected_revision_no, inline_content)
                    .await
            }
            Op::AppendFileByInode {
                inode_id,
                inline_content,
                expected_revision_no,
            } => {
                let target = Target::Inode(*inode_id);
                self.plan_append(target, *expected_revision_no, inline_content)
                    .await
            }
            Op::DeletePath {
                path,
                behavior,
                expected_inode_id,
            } => {
                let target = Target::path(path, *expected_inode_id);
                self.plan_delete(target, None, *behavior).await
            }
            Op::DeleteByInode {
                inode_id,
                expected_binding_version,
                behavior,
            } => {
                let target = Target::Inode(*inode_id);
                self.plan_delete(target, Some(expected_binding_version), *behavior)
                    .await
            }
            Op::MovePath {
                source_path,
                destination_path,
                precondition,
            } => {
                let source = Target::path(source_path, None);
                let destination = Destination::Path(destination_path);
                self.plan_move(source, None, destination, precondition)
                    .await
            }
            Op::MoveByInode {
                inode_id,
                expected_binding_version,
                destination_parent_inode_id,
                destination_display_name,
                precondition,
            } => {
                let source = Target::Inode(*inode_id);
                let destination =
                    Destination::name(*destination_parent_inode_id, destination_display_name);
                self.plan_move(
                    source,
                    Some(expected_binding_version),
                    destination,
                    precondition,
                )
                .await
            }
            Op::CopyPath {
                source_path,
                destination_path,
                precondition,
            } => {
                let source = Target::path(source_path, None);
                let destination = Destination::Path(destination_path);
                self.plan_copy(source, destination, precondition).await
            }
            Op::CopyByInode {
                inode_id,
                destination_parent_inode_id,
                destination_display_name,
                precondition,
            } => {
                let source = Target::Inode(*inode_id);
                let destination =
                    Destination::name(*destination_parent_inode_id, destination_display_name);
                self.plan_copy(source, destination, precondition).await
            }
            Op::RestoreRevision {
                path,
                source_revision_no,
            } => {
                let target = Target::path(path, None);
                self.plan_restore_revision(target, *source_revision_no)
                    .await
            }
            Op::RestoreRevisionByInode {
                inode_id,
                source_revision_no,
            } => {
                let target = Target::Inode(*inode_id);
                self.plan_restore_revision(target, *source_revision_no)
                    .await
            }
            Op::Undelete {
                inode_id,
                deletion_seq,
                destination_path,
                destination_parent_inode_id,
                destination_display_name,
            } => {
                let destination = undelete_destination(
                    destination_path.as_ref(),
                    *destination_parent_inode_id,
                    destination_display_name.as_ref(),
                )?;
                self.plan_undelete(*inode_id, *deletion_seq, destination)
                    .await
            }
            Op::UpdateAttributes {
                path,
                set,
                remove,
                expected_inode_id,
                expected_attributes_revision_no: expected,
            } => {
                validate_attributes_precondition(*expected_inode_id, *expected)?;
                let target = Target::path(path, *expected_inode_id);
                self.plan_update_attributes(target, set, remove, *expected)
                    .await
            }
            Op::UpdateAttributesByInode {
                inode_id,
                set,
                remove,
                expected_attributes_revision_no: expected,
            } => {
                let target = Target::Inode(*inode_id);
                self.plan_update_attributes(target, set, remove, *expected)
                    .await
            }
            Op::UpdateAccess {
                path,
                boundary,
                grants,
                expected_inode_id,
                expected_access_revision_no: expected,
            } => {
                validate_access_precondition(*expected_inode_id, *expected)?;
                let target = Target::path(path, *expected_inode_id);
                self.plan_update_access(target, *boundary, grants, *expected)
                    .await
            }
            Op::UpdateAccessByInode {
                inode_id,
                boundary,
                grants,
                expected_access_revision_no: expected,
            } => {
                let target = Target::Inode(*inode_id);
                self.plan_update_access(target, *boundary, grants, *expected)
                    .await
            }
        }
    }
}

fn prepared_reference(content: &FileContent) -> Result<&loonfs_types::ContentRef> {
    match content {
        FileContent::Ref(reference) => Ok(reference),
        FileContent::Inline(_) => Err(CoreError::InvalidCommitRequest(
            "content must be prepared before planning".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authorize::Authorizer;
    use crate::commit::engine::CommitCandidate;
    use crate::commit::CommitPlan;
    use crate::path::write::PublishPlanningSession;
    use crate::test_support::WriterAt;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_types::format::wal::{WalCommitDelta, WalDelta};
    use loonfs_types::FilesystemChange;
    use loonfs_types::{
        AbsolutePath, AccessGrants, AccessRight, AccessRights, AttributeKey, AttributeValue,
        CommitId, ContentRef, DeleteDirectoryBehavior, DestinationBehavior,
        DestinationPrecondition, InodeId, NamespaceAccess, PrincipalId, PrincipalScope,
        PrincipalSet, RevisionNo, Subject, SubjectId,
    };
    use std::assert_matches;
    use std::collections::BTreeSet;
    use tempfile::tempdir;

    fn test_context() -> WriterAt {
        crate::test_support::writer_at("writer", 1)
    }

    fn request(operation: FilesystemOperation) -> CommitIntent {
        CommitIntent::single(
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

    fn put(
        path: &str,
        content_ref: &ContentRef,
        behavior: DestinationBehavior,
    ) -> FilesystemOperation {
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse(path).expect("path"),
            content: loonfs_types::FileContent::Ref(content_ref.clone()),
            behavior,
            expected_inode_id: None,
            expected_revision_no: None,
        }
    }

    fn move_to(
        source: &str,
        destination: &str,
        behavior: DestinationBehavior,
    ) -> FilesystemOperation {
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse(source).expect("path"),
            destination_path: AbsolutePath::parse(destination).expect("path"),
            precondition: DestinationPrecondition {
                behavior,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        }
    }

    fn set_owner(path: &str, owner: &str) -> FilesystemOperation {
        FilesystemOperation::UpdateAttributes {
            path: AbsolutePath::parse(path).expect("path"),
            set: BTreeMap::from([(
                AttributeKey::parse("owner").expect("attribute key"),
                AttributeValue::parse(owner).expect("attribute value"),
            )]),
            remove: Vec::new(),
            expected_inode_id: None,
            expected_attributes_revision_no: None,
        }
    }

    async fn setup_namespace() -> (tempfile::TempDir, LocalFsStore, NamespaceId, WriterAt) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
        (temp_dir, store, namespace_id, context)
    }

    async fn try_plan_against_current_state(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        request: &CommitIntent,
    ) -> Result<CommitPlan> {
        let namespace = crate::test_support::namespace_store(store, namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("metadata view");
        let content_writes = crate::test_support::content_writes();
        let session = PublishPlanningSession::new(
            view.head(),
            view.wal_tail(),
            view.projected_metadata_view(),
            ContentWrites::new(&namespace.store, &content_writes),
            1,
        );
        let content = request
            .operations
            .iter()
            .filter_map(FilesystemOperation::content_ref)
            .cloned()
            .map(crate::storage::content_admission::PreparedContent::for_durable_content_write)
            .collect();
        let candidate = CommitCandidate::prepared(request.clone(), content);
        let authorizer =
            Authorizer::for_request(namespace_id, &view.head().access, candidate.authority())?;
        session
            .prepare_commit(
                &candidate,
                authorizer,
                loonfs_test_support::ids::commit_fingerprint("test"),
            )
            .await
    }

    async fn plan_against_current_state(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        request: &CommitIntent,
    ) -> CommitPlan {
        try_plan_against_current_state(store, namespace_id, request)
            .await
            .expect("plan")
    }

    #[tokio::test]
    async fn put_file_creates_missing_parent_directories() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(
                "/docs/nested/a.txt",
                b"hello",
                DestinationBehavior::NoReplace,
                Some(&CommitId::parse("put-with-parents").expect("valid commit id")),
            )
            .await
            .expect("put file");

        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let view = namespace
            .load_current_metadata_view()
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
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(
                "/docs/tmp.txt",
                b"first",
                DestinationBehavior::NoReplace,
                Some(&CommitId::parse("recreate-seed").expect("valid commit id")),
            )
            .await
            .expect("seed file");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .delete_path(
                "/docs/tmp.txt",
                Some(&CommitId::parse("recreate-delete").expect("valid commit id")),
            )
            .await
            .expect("delete file");

        // The tombstone covers the dead inode, not the name: the name is
        // reusable immediately, with or without an intervening rebuild.
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(
                "/docs/tmp.txt",
                b"second",
                DestinationBehavior::NoReplace,
                Some(&CommitId::parse("recreate-put").expect("valid commit id")),
            )
            .await
            .expect("recreate at the deleted path");
    }

    #[tokio::test]
    async fn deleted_subtree_names_replan_as_fresh_state() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let seed_commit_id = CommitId::parse("seed-dead-tree").expect("valid commit id");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(
                "/dead/file.txt",
                b"hello",
                DestinationBehavior::NoReplace,
                Some(&seed_commit_id),
            )
            .await
            .expect("seed file");
        let delete_commit_id = CommitId::parse("delete-dead-tree").expect("valid commit id");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .delete_path("/dead", Some(&delete_commit_id))
            .await
            .expect("delete tree");
        let staged = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"new")
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
                content: loonfs_types::FileContent::Ref(staged.into_content_ref()),
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
        let staged = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"hello")
            .await
            .expect("stage");
        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &CommitIntent {
                preconditions: Vec::new(),
                commit_id: CommitId::parse("batch-create-then-put").expect("valid commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations: vec![
                    create_dir("/reports"),
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/reports/a.txt").expect("path"),
                        content: loonfs_types::FileContent::Ref(staged.content_ref().clone()),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ],
            },
        )
        .await;

        assert_matches!(
            planned.deltas.as_slice(),
            [
                WalCommitDelta {
                    semantic_operation_index: 0,
                    delta: WalDelta::CreateInode { delta_index: 0, inode_id: InodeId(2), .. },
                },
                WalCommitDelta {
                    semantic_operation_index: 0,
                    delta: WalDelta::BindDirentry {
                        delta_index: 1,
                        parent_inode_id: InodeId(1),
                        child_inode_id: InodeId(2),
                        ..
                    },
                },
                WalCommitDelta {
                    semantic_operation_index: 1,
                    delta: WalDelta::CreateInode { delta_index: 2, inode_id: InodeId(3), .. },
                },
                WalCommitDelta {
                    semantic_operation_index: 1,
                    delta: WalDelta::BindDirentry {
                        delta_index: 3,
                        parent_inode_id: InodeId(2),
                        child_inode_id: InodeId(3),
                        display_name,
                        ..
                    },
                },
                WalCommitDelta {
                    semantic_operation_index: 1,
                    delta: WalDelta::AppendFileRevision {
                        delta_index: 4,
                        inode_id: InodeId(3),
                        ..
                    },
                },
            ] if display_name.as_str() == "a.txt"
        );
        assert_eq!(planned.resulting_next_inode_id, InodeId(4));
    }

    #[tokio::test]
    async fn delete_then_create_resolves_against_the_delete() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(
                "/docs/tmp.txt",
                b"first",
                DestinationBehavior::NoReplace,
                Some(&CommitId::parse("seed-replaceable").expect("valid commit id")),
            )
            .await
            .expect("seed file");
        let staged = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"second")
            .await
            .expect("stage");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &CommitIntent {
                preconditions: Vec::new(),
                commit_id: CommitId::parse("batch-delete-then-create").expect("valid commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations: vec![
                    FilesystemOperation::DeletePath {
                        path: AbsolutePath::parse("/docs/tmp.txt").expect("path"),
                        behavior: DeleteDirectoryBehavior::NonRecursive,
                        expected_inode_id: None,
                    },
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/tmp.txt").expect("path"),
                        content: loonfs_types::FileContent::Ref(staged.content_ref().clone()),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ],
            },
        )
        .await;

        assert_matches!(planned.deltas[0].delta, WalDelta::UnbindDirentry { .. });
        assert_matches!(planned.deltas[1].delta, WalDelta::TombstoneSubtree { .. });
        assert_matches!(planned.deltas[2].delta, WalDelta::CreateInode { .. });
        assert_matches!(
            &planned.deltas[3].delta,
            WalDelta::BindDirentry { display_name, .. } if display_name.as_str() == "tmp.txt"
        );
    }

    #[tokio::test]
    async fn a_failing_operation_names_its_position() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace().await;
        let error = try_plan_against_current_state(
            &store,
            &namespace_id,
            &CommitIntent {
                preconditions: Vec::new(),
                commit_id: CommitId::parse("batch-with-a-bad-op").expect("valid commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
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
    async fn the_change_feed_maps_every_operation_the_planner_emits() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        for (path, body, behavior) in [
            ("/docs/a.txt", "first", DestinationBehavior::NoReplace),
            ("/docs/a.txt", "second", DestinationBehavior::Replace),
            ("/docs/b.txt", "occupant", DestinationBehavior::NoReplace),
            ("/docs/gone.txt", "gone", DestinationBehavior::NoReplace),
        ] {
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
                .put_file(path, body.as_bytes(), behavior, None)
                .await
                .expect("seed file");
        }
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .publish_candidates(vec![CommitCandidate::new(CommitIntent::single(
                CommitId::parse("seed-owner").expect("valid commit id"),
                loonfs_test_support::test_actor(),
                None,
                set_owner("/docs/a.txt", "ada"),
            ))])
            .await
            .pop()
            .expect("one result")
            .expect("seed attributes");
        let deletion = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .delete_path("/docs/gone.txt", None)
            .await
            .expect("seed deletion");
        let deleted_inode_id = deletion
            .events
            .iter()
            .find_map(|event| match event {
                FilesystemChange::Deleted { inode_id, .. } => Some(*inode_id),
                _ => None,
            })
            .expect("the delete names its inode");
        let undelete = |destination: Option<&str>| FilesystemOperation::Undelete {
            inode_id: deleted_inode_id,
            deletion_seq: deletion.committed_seq,
            destination_path: destination.map(|path| AbsolutePath::parse(path).expect("path")),
            destination_parent_inode_id: None,
            destination_display_name: None,
        };
        let staged = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"staged")
            .await
            .expect("stage")
            .into_content_ref();
        let inline = crate::storage::content_admission::PreparedContent::inline(
            namespace_id.clone(),
            bytes::Bytes::from_static(b"inline"),
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        )
        .content_ref()
        .clone();

        // Access rows exist only in a restricted namespace.
        let restricted = NamespaceId::parse("restricted").expect("valid namespace id");
        let principal_scope = PrincipalScope::parse("org_demo").expect("principal scope");
        let administrator = PrincipalId::parse("prn_root").expect("principal id");
        crate::test_support::NamespaceFixture::new(&store, &restricted, &context)
            .engine()
            .bootstrap_namespace(
                &loonfs_test_support::test_actor(),
                &crate::options::CreateNamespaceOptions {
                    access: NamespaceAccess::Acl {
                        principal_scope: principal_scope.clone(),
                        root_grants: AccessGrants::new(BTreeMap::from([(
                            administrator.clone(),
                            AccessRights::ADMIN,
                        )]))
                        .expect("root grants"),
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("bootstrap a restricted namespace");
        let update_access = request(FilesystemOperation::UpdateAccess {
            path: AbsolutePath::root(),
            boundary: false,
            grants: AccessGrants::new(BTreeMap::from([
                (administrator.clone(), AccessRights::ADMIN),
                (
                    PrincipalId::parse("prn_team").expect("principal id"),
                    AccessRight::Read.into(),
                ),
            ]))
            .expect("grants"),
            expected_inode_id: None,
            expected_access_revision_no: None,
        })
        .with_subject(Subject {
            principal_scope,
            subject_id: SubjectId::parse("root").expect("subject id"),
            principals: PrincipalSet::new(BTreeSet::from([administrator])).expect("principals"),
        });

        let rows: Vec<(&str, &NamespaceId, CommitIntent, &[&str])> = vec![
            (
                "create a directory",
                &namespace_id,
                request(create_dir("/new")),
                &["directory_created"],
            ),
            (
                "create a file from staged content",
                &namespace_id,
                request(put(
                    "/docs/staged.txt",
                    &staged,
                    DestinationBehavior::NoReplace,
                )),
                &["file_created"],
            ),
            (
                "create a file from inline content",
                &namespace_id,
                request(put(
                    "/docs/inline.txt",
                    &inline,
                    DestinationBehavior::NoReplace,
                )),
                &["file_created"],
            ),
            (
                "create a file under a missing directory",
                &namespace_id,
                request(put(
                    "/fresh/staged.txt",
                    &staged,
                    DestinationBehavior::NoReplace,
                )),
                &["directory_created", "file_created"],
            ),
            (
                "replace a file",
                &namespace_id,
                request(put("/docs/a.txt", &staged, DestinationBehavior::Replace)),
                &["content_changed"],
            ),
            (
                "delete a file",
                &namespace_id,
                request(FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: None,
                }),
                &["deleted"],
            ),
            (
                "delete a subtree",
                &namespace_id,
                request(FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/docs").expect("path"),
                    behavior: DeleteDirectoryBehavior::Recursive,
                    expected_inode_id: None,
                }),
                &["deleted"],
            ),
            (
                "move to another directory",
                &namespace_id,
                request(move_to(
                    "/docs/a.txt",
                    "/a.txt",
                    DestinationBehavior::NoReplace,
                )),
                &["moved"],
            ),
            (
                "move over an existing file",
                &namespace_id,
                request(move_to(
                    "/docs/a.txt",
                    "/docs/b.txt",
                    DestinationBehavior::Replace,
                )),
                &["deleted", "moved"],
            ),
            (
                "rename",
                &namespace_id,
                request(move_to(
                    "/docs/a.txt",
                    "/docs/renamed.txt",
                    DestinationBehavior::NoReplace,
                )),
                &["moved"],
            ),
            (
                "rename by case only",
                &namespace_id,
                request(move_to(
                    "/docs/a.txt",
                    "/docs/A.txt",
                    DestinationBehavior::NoReplace,
                )),
                &["moved"],
            ),
            (
                "copy a file with attributes",
                &namespace_id,
                request(FilesystemOperation::CopyPath {
                    source_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                    destination_path: AbsolutePath::parse("/docs/copy.txt").expect("path"),
                    precondition: DestinationPrecondition {
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                }),
                &["file_created", "attributes_changed"],
            ),
            (
                "restore a revision",
                &namespace_id,
                request(FilesystemOperation::RestoreRevision {
                    path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                    source_revision_no: RevisionNo(1),
                }),
                &["content_changed"],
            ),
            (
                "undelete in place",
                &namespace_id,
                request(undelete(None)),
                &["undeleted"],
            ),
            (
                "undelete to a destination",
                &namespace_id,
                request(undelete(Some("/docs/found.txt"))),
                &["undeleted"],
            ),
            (
                "update attributes",
                &namespace_id,
                request(set_owner("/docs/a.txt", "grace")),
                &["attributes_changed"],
            ),
            (
                "update access",
                &restricted,
                update_access,
                &["access_changed"],
            ),
        ];

        for (label, namespace_id, commit_request, expected) in rows {
            let planned = try_plan_against_current_state(&store, namespace_id, &commit_request)
                .await
                .expect(label);
            let events = crate::changes::events_from_wal_deltas(
                namespace_id,
                planned.assigned_seq,
                &planned.deltas,
            )
            .expect(label);
            let kinds: Vec<String> = events
                .iter()
                .map(|event| {
                    serde_json::to_value(event).expect("serialize event")["kind"]
                        .as_str()
                        .expect("an event is tagged with its kind")
                        .to_owned()
                })
                .collect();
            assert_eq!(kinds, expected, "{label}");
        }
    }
}
