//! Commit fingerprinting and the planning of one filesystem operation, which
//! calls the commit's writes for its rows.

use super::intent::{CommitIntent, FilesystemOperation};
use super::planned_pieces::CommitPieces;
use super::preconditions::{
    validate_expected_file_state, validate_revision_precondition, ExpectedFileState,
    PreconditionFields,
};
use super::publish_path_planning::{Destination, PlanningView, Target};
use crate::authorize::Authorizer;
use crate::commit::engine::TemporaryMove;
use crate::commit::{CommitFingerprint, CommitSession, NewChild};
use crate::error::{CoreError, Result};
use crate::metadata::MetadataView;
use crate::storage::tail_content::ContentWrites;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::MergeRecord;
use loonfs_types::{
    CommitFingerprintInput, CommitSeq, ContentId, DestinationBehavior, FileContent,
    InlineContentIdentity, InodeId, MergeFingerprintInput, NamespaceId,
};
use std::collections::BTreeMap;
use std::num::NonZeroU32;

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
        message: request.message.as_deref(),
        operations: &request.operations,
        preconditions: &request.preconditions,
        inline_content_identities,
    }
    .fingerprint()
}

/// Computes the semantic fingerprint of a merge, which covers the merge record
/// in place of the request's operations and preconditions.
pub(crate) fn merge_fingerprint(
    namespace_id: &NamespaceId,
    request: &CommitIntent,
    merge: &MergeRecord,
) -> CommitFingerprint {
    MergeFingerprintInput {
        namespace_id,
        actor_id: &request.actor_id,
        merge,
        message: request.message.as_deref(),
    }
    .fingerprint()
}

/// The planning of one operation: the namespace, the request's authorizer,
/// the commit's state, which the operation's writes change, the commit's
/// pieces, where content writes go, the commit's sequence, and where the IDs
/// of the inodes it creates come from.
pub(super) struct OperationPlanningSession<'p, 'c, S: ObjectStore + ?Sized> {
    pub(super) namespace_id: &'p NamespaceId,
    pub(super) authorizer: Authorizer<'p>,
    pub(super) commit: &'p mut CommitSession<'c, S>,
    pub(super) pieces: &'p mut CommitPieces<'c, S>,
    pub(super) content_writes: ContentWrites<'p, S>,
    /// The sequence that the commit is assigned.
    pub(super) committed_seq: CommitSeq,
    pub(super) created_inode_ids: CreatedInodeIds<'p>,
    /// The leg of a merge's move through a temporary name that the operation
    /// is, if it's one.
    pub(super) temporary_move: Option<TemporaryMove>,
}

/// The ID of an inode that a create writes.
///
/// Only [`OperationPlanningSession::next_inode_id`] and [`NewInodeId::merged`]
/// make one, so the ID is new to the namespace. A generated ID includes the
/// low 32 bits of the commit's sequence, the create's number within the
/// commit and 64 random bits, so it differs from every existing inode's ID,
/// and [`NewInodeId::merged`] asserts that no inode has a merge's assigned ID.
pub(crate) struct NewInodeId(InodeId);

impl NewInodeId {
    /// Returns `inode_id`, the ID that a merge assigns to an inode that it
    /// creates, after asserting that no inode in `view` has it.
    ///
    /// `view` includes the merge's earlier operations, so an ID assigned to
    /// two creates fails at the second.
    ///
    /// An inode row is immutable and unique, so a second row for the same ID
    /// would corrupt the namespace. A merge creates an inode under the ID
    /// that a fork's create gave it. Of the namespaces that can merge into
    /// this one, only the fork and the new forks that rebases replayed the
    /// fork's commits into can have that ID while this namespace doesn't, and
    /// every merge of one of them covers a range of the fork that starts
    /// where this namespace's merge index says. The range rule admits one
    /// merge for each such start, before the merge's operations are planned
    /// (format spec, "Merges"). So the ID is present only if the merge's
    /// planner has a bug: it mistook an inode that this namespace already has
    /// for a new one, or it assigned one ID to two creates.
    pub(super) async fn merged<S: ObjectStore + ?Sized>(
        view: &MetadataView<'_, '_, S>,
        inode_id: InodeId,
    ) -> Result<Self> {
        assert!(
            view.inode_at_seq(inode_id).await?.is_none(),
            "a merge should create inode `{inode_id}` only where it doesn't exist"
        );
        Ok(Self(inode_id))
    }

    pub(crate) fn inode_id(&self) -> InodeId {
        self.0
    }
}

/// Where the IDs of the inodes that one operation creates come from.
pub(super) enum CreatedInodeIds<'p> {
    /// Each ID is built from the commit's sequence and the create number of
    /// the next inode, which every operation in the commit shares.
    Generated { next_create_no: &'p mut NonZeroU32 },
    /// The ID that the inode has in the fork, for a merge's operation, which
    /// creates at most one inode. The operation takes the ID when it creates
    /// the inode.
    Merge(Option<NewInodeId>),
}

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, '_, S> {
    /// Returns what the operation reads and authorizes against: the
    /// metadata as the commit's writes so far have left it.
    pub(super) fn view(&self) -> PlanningView<'_, S> {
        PlanningView {
            namespace_id: self.namespace_id,
            authorizer: self.authorizer,
            metadata: self.commit.view(),
        }
    }

    /// Returns the ID of the next inode that the commit creates.
    pub(super) fn next_inode_id(&mut self) -> NewInodeId {
        match &mut self.created_inode_ids {
            CreatedInodeIds::Generated { next_create_no } => {
                let create_no = **next_create_no;
                **next_create_no = create_no.checked_add(1).expect(
                    "a commit should create fewer inodes than its 32-bit delta positions allow",
                );
                NewInodeId(InodeId::generate(self.committed_seq, create_no))
            }
            CreatedInodeIds::Merge(assigned) => assigned.take().expect(
                "a merge operation that creates an inode should have an assigned inode ID \
                 and create no other inode",
            ),
        }
    }

    /// Plans `operation`, mapping its path or inode address onto the one
    /// planner for its kind, which calls the commit's writes for its rows.
    pub(super) async fn plan(&mut self, operation: &FilesystemOperation) -> Result<()> {
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
                let expected = ExpectedFileState::new(*inode_id, Some(*expected_revision_no));
                self.plan_put_file_revision_by_inode(*inode_id, content_ref, expected)
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
                self.plan_append(target, expected, inline_content).await
            }
            Op::AppendFileByInode {
                inode_id,
                inline_content,
                expected_revision_no,
            } => {
                let target = Target::Inode(*inode_id);
                let expected = ExpectedFileState::new(*inode_id, *expected_revision_no);
                self.plan_append(target, Some(expected), inline_content)
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
            Op::RestoreDeleted {
                inode_id,
                deletion_seq,
                destination,
            } => {
                self.plan_restore_deleted(*inode_id, *deletion_seq, destination.clone())
                    .await
            }
            Op::UpdateAttributes {
                path,
                set,
                remove,
                expected_inode_id,
                expected_attributes_revision_no: expected,
            } => {
                validate_revision_precondition(
                    *expected_inode_id,
                    expected.is_some(),
                    "expected_attributes_revision_no",
                )?;
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
                validate_revision_precondition(
                    *expected_inode_id,
                    expected.is_some(),
                    "expected_access_revision_no",
                )?;
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
    use crate::commit::engine::CommitCandidate;
    use crate::commit::{CommitPlan, CommitValidationError};
    use crate::error::ErrorCode;
    use crate::path::write::PublishPlanningSession;
    use crate::test_support::WriterAt;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_types::format::wal::{WalCommitDelta, WalDelta};
    use loonfs_types::FilesystemEvent;
    use loonfs_types::{
        AbsolutePath, AccessGrants, AccessRight, AccessRights, AttributeKey, AttributeValue,
        AttributesRevisionNo, CommitId, ContentRef, DeleteDirectoryBehavior, DestinationBehavior,
        DestinationPrecondition, DisplayName, InodeKind, NameKey, PrincipalId, RevisionNo,
        ROOT_INODE_ID,
    };
    use std::assert_matches;

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
        crate::test_support::created_namespace("demo", test_context()).await
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
            view.metadata_view(),
            ContentWrites::new(&namespace.store, &content_writes, view.head()),
            1,
        );
        let content = request
            .operations
            .iter()
            .filter_map(FilesystemOperation::content_ref)
            .map(|content_ref| {
                crate::storage::content_admission::PreparedContent::for_durable_content_write(
                    namespace_id.clone(),
                    content_ref.clone(),
                )
            })
            .collect();
        let candidate = CommitCandidate::prepared(request.clone(), content);
        let authorizer = candidate.authorizer();
        session
            .prepare_commit(
                &candidate,
                authorizer,
                loonfs_test_support::ids::commit_fingerprint("test"),
            )
            .await
    }

    /// Returns the error of the operation that `error` reports as failed.
    fn failed_operation_error(error: CoreError) -> CoreError {
        match error {
            CoreError::FailedOperation {
                operation_index: 0,
                error,
            } => *error,
            other => panic!("expected the first operation to fail, got {other:?}"),
        }
    }

    fn set_owner_by_inode(
        inode_id: InodeId,
        owner: &str,
        expected: Option<AttributesRevisionNo>,
    ) -> FilesystemOperation {
        FilesystemOperation::UpdateAttributesByInode {
            inode_id,
            set: BTreeMap::from([(
                AttributeKey::parse("owner").expect("attribute key"),
                AttributeValue::parse(owner).expect("attribute value"),
            )]),
            remove: Vec::new(),
            expected_attributes_revision_no: expected,
        }
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
            .metadata_view()
            .resolve_visible_path(&AbsolutePath::parse("/docs/nested/a.txt").expect("path"))
            .await
            .expect("resolve created file");
        assert_eq!(resolved.absolute_path, "/docs/nested/a.txt");
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
                    delta: WalDelta::CreateInode { delta_index: 0, inode_id: directory, .. },
                },
                WalCommitDelta {
                    semantic_operation_index: 0,
                    delta: WalDelta::BindDirentry {
                        delta_index: 1,
                        parent_inode_id: ROOT_INODE_ID,
                        child_inode_id: bound_directory,
                        ..
                    },
                },
                WalCommitDelta {
                    semantic_operation_index: 1,
                    delta: WalDelta::CreateInode { delta_index: 2, inode_id: file, .. },
                },
                WalCommitDelta {
                    semantic_operation_index: 1,
                    delta: WalDelta::BindDirentry {
                        delta_index: 3,
                        parent_inode_id: file_parent,
                        child_inode_id: bound_file,
                        display_name,
                        ..
                    },
                },
                WalCommitDelta {
                    semantic_operation_index: 1,
                    delta: WalDelta::AppendFileRevision {
                        delta_index: 4,
                        inode_id: revised_file,
                        ..
                    },
                },
            ] if display_name.as_str() == "a.txt"
                && *directory != ROOT_INODE_ID
                && directory != file
                && bound_directory == directory
                && file_parent == directory
                && bound_file == file
                && revised_file == file
        );
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
                FilesystemEvent::Deleted { inode_id, .. } => Some(*inode_id),
                _ => None,
            })
            .expect("the delete names its inode");
        let restore_deleted = |destination: Option<&str>| FilesystemOperation::RestoreDeleted {
            inode_id: deleted_inode_id,
            deletion_seq: deletion.committed_seq,
            destination: destination.map_or(
                loonfs_types::options::RestoreDeletedDestination::Recorded,
                |path| {
                    loonfs_types::options::RestoreDeletedDestination::Path(
                        AbsolutePath::parse(path).expect("path"),
                    )
                },
            ),
        };
        let staged = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"staged")
            .await
            .expect("stage")
            .into_content_ref();
        let inline = crate::storage::content_admission::PreparedContent::inline(
            bytes::Bytes::from_static(b"inline"),
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        )
        .content_ref()
        .clone();

        let restricted = NamespaceId::parse("restricted").expect("valid namespace id");
        let administrator = PrincipalId::parse("prn_root").expect("principal id");
        crate::test_support::NamespaceFixture::new(&store, &restricted, &context)
            .engine()
            .bootstrap_namespace(
                &loonfs_test_support::test_actor(),
                &loonfs_types::options::CreateNamespaceOptions::default(),
            )
            .await
            .expect("bootstrap a namespace");
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
                "restore in place",
                &namespace_id,
                request(restore_deleted(None)),
                &["restored"],
            ),
            (
                "restore to a destination",
                &namespace_id,
                request(restore_deleted(Some("/docs/found.txt"))),
                &["restored"],
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

    #[tokio::test]
    async fn stale_revision_precondition_is_rejected() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let fixture = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
        for body in ["content-1", "content-2"] {
            fixture
                .put_file(
                    "/docs/readme.txt",
                    body.as_bytes(),
                    DestinationBehavior::Replace,
                    None,
                )
                .await
                .expect("seed a revision");
        }
        let file =
            crate::test_support::visible_inode_id(&store, &namespace_id, "/docs/readme.txt").await;
        let replacement = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"content-3")
            .await
            .expect("stage")
            .into_content_ref();

        let error = try_plan_against_current_state(
            &store,
            &namespace_id,
            &request(FilesystemOperation::PutFileRevisionByInode {
                inode_id: file,
                content: loonfs_types::FileContent::Ref(replacement),
                expected_revision_no: RevisionNo(1),
            }),
        )
        .await
        .expect_err("the expected revision is stale");
        assert_eq!(error.code(), ErrorCode::StaleRevision);
        assert_matches!(
            failed_operation_error(error),
            CoreError::CommitValidation(CommitValidationError::BaseRevisionMismatch {
                inode_id,
                expected: RevisionNo(1),
                actual: Some(RevisionNo(2)),
            }) if inode_id == file
        );
    }

    /// A revision restore copies the revision it identifies, which an earlier
    /// operation of the same commit may have written, and fails when the file
    /// has no such revision.
    #[tokio::test]
    async fn a_revision_restore_copies_a_revision_that_the_same_commit_wrote() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(
                "/docs/readme.txt",
                b"content-1",
                DestinationBehavior::Replace,
                None,
            )
            .await
            .expect("seed a revision");
        let file =
            crate::test_support::visible_inode_id(&store, &namespace_id, "/docs/readme.txt").await;
        let replacement = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"content-2")
            .await
            .expect("stage")
            .into_content_ref();
        let restore = |source_revision_no| FilesystemOperation::RestoreRevisionByInode {
            inode_id: file,
            source_revision_no: RevisionNo(source_revision_no),
        };

        // The put writes revision 2, the first restore copies it as revision
        // 3, and the second restore copies revision 3 as revision 4.
        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &CommitIntent {
                preconditions: Vec::new(),
                commit_id: CommitId::parse("restore-request").expect("valid commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations: vec![
                    FilesystemOperation::PutFileRevisionByInode {
                        inode_id: file,
                        content: loonfs_types::FileContent::Ref(replacement.clone()),
                        expected_revision_no: RevisionNo(1),
                    },
                    restore(2),
                    restore(3),
                ],
            },
        )
        .await;
        let copied: Vec<_> = planned
            .deltas
            .iter()
            .map(|delta| match &delta.delta {
                WalDelta::AppendFileRevision {
                    revision_no,
                    content_ref,
                    ..
                } => (*revision_no, content_ref),
                other => panic!("expected a revision delta, got {other:?}"),
            })
            .collect();
        assert_eq!(
            copied,
            [2, 3, 4].map(|revision_no| (RevisionNo(revision_no), &replacement))
        );

        let error = try_plan_against_current_state(&store, &namespace_id, &request(restore(99)))
            .await
            .expect_err("the file has no revision 99");
        assert_eq!(error.code(), ErrorCode::RevisionNotFound);
        assert_matches!(
            failed_operation_error(error),
            CoreError::CommitValidation(
                CommitValidationError::RestoreRevisionSourceRevisionMissing {
                    inode_id,
                    source_revision_no: RevisionNo(99),
                }
            ) if inode_id == file
        );
    }

    /// A revision restore of a file whose current revision is the largest
    /// public integer has no next revision to write.
    #[tokio::test]
    async fn a_revision_restore_past_the_largest_revision_number_is_rejected() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(
                "/file.txt",
                b"content-1",
                DestinationBehavior::Replace,
                None,
            )
            .await
            .expect("seed a revision");
        let file = crate::test_support::visible_inode_id(&store, &namespace_id, "/file.txt").await;
        let maximum = RevisionNo(loonfs_types::MAX_PUBLIC_INTEGER);
        let content_id = loonfs_types::ContentId::generate();
        let checksum = loonfs_types::Checksum::crc64nvme(b"content-max");
        let layout = loonfs_types::ContentLayout {
            creator_namespace_id: namespace_id.clone(),
            extents: vec![loonfs_types::ContentExtent {
                content_id: content_id.clone(),
                object: loonfs_types::ExtentObject::Whole,
                object_bytes: 11,
                object_checksum: checksum.clone(),
                offset: 0,
                length: 11,
            }],
        };
        let content_ref = ContentRef {
            kind: loonfs_types::ContentRefKind::BlobV1,
            content_id,
            size_bytes: 11,
            checksum,
        };
        crate::test_support::namespace_store(&store, &namespace_id)
            .append_wal_commit(
                vec![crate::test_support::file_revision_delta(
                    0,
                    file,
                    maximum,
                    content_ref,
                    Some(layout),
                )],
                Vec::new(),
            )
            .await
            .expect("write a revision at the largest revision number");

        let error = try_plan_against_current_state(
            &store,
            &namespace_id,
            &request(FilesystemOperation::RestoreRevisionByInode {
                inode_id: file,
                source_revision_no: maximum,
            }),
        )
        .await
        .expect_err("revision numbers stop at the largest public integer");
        assert_eq!(error.code(), ErrorCode::ServerError);
        assert_matches!(
            failed_operation_error(error),
            CoreError::CommitValidation(CommitValidationError::RevisionOverflow {
                inode_id,
                base_revision_no,
            }) if inode_id == file && base_revision_no == maximum
        );
    }

    #[tokio::test]
    async fn a_stale_expected_attributes_revision_is_rejected() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let fixture = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
        fixture
            .put_file("/docs/a.txt", b"a", DestinationBehavior::NoReplace, None)
            .await
            .expect("seed a file");
        let docs = crate::test_support::visible_inode_id(&store, &namespace_id, "/docs").await;
        for owner in ["ada", "grace"] {
            fixture
                .submit_operation(
                    CommitId::parse(format!("seed-owner-{owner}")).expect("valid commit id"),
                    set_owner_by_inode(docs, owner, None),
                    Vec::new(),
                )
                .await
                .expect("seed an attributes revision");
        }

        let error = try_plan_against_current_state(
            &store,
            &namespace_id,
            &request(set_owner_by_inode(
                docs,
                "hopper",
                Some(AttributesRevisionNo(1)),
            )),
        )
        .await
        .expect_err("the expected attributes revision is stale");
        assert_eq!(
            error.code(),
            ErrorCode::StaleAttributes,
            "the precondition reports the attribute-conflict code"
        );
        assert_matches!(
            failed_operation_error(error),
            CoreError::CommitValidation(
                CommitValidationError::UpdateAttributesBaseRevisionMismatch {
                    inode_id,
                    expected: AttributesRevisionNo(1),
                    actual: Some(AttributesRevisionNo(2)),
                }
            ) if inode_id == docs
        );
    }

    #[tokio::test]
    async fn a_first_attribute_write_states_revision_zero() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file("/docs/a.txt", b"a", DestinationBehavior::NoReplace, None)
            .await
            .expect("seed a file");
        let docs = crate::test_support::visible_inode_id(&store, &namespace_id, "/docs").await;
        let update = |expected: u64| {
            request(set_owner_by_inode(
                docs,
                "ada",
                Some(AttributesRevisionNo(expected)),
            ))
        };

        try_plan_against_current_state(&store, &namespace_id, &update(0))
            .await
            .expect("a first write states revision zero");
        let error = try_plan_against_current_state(&store, &namespace_id, &update(1))
            .await
            .expect_err("nothing has written revision 1 yet");
        assert_eq!(error.code(), ErrorCode::StaleAttributes);
        assert_matches!(
            failed_operation_error(error),
            CoreError::CommitValidation(
                CommitValidationError::UpdateAttributesBaseRevisionMismatch {
                    expected: AttributesRevisionNo(1),
                    actual: Some(AttributesRevisionNo(0)),
                    ..
                }
            )
        );
    }

    /// A namespace whose directory `/docs` was deleted, with the inode ID of
    /// `/docs` and the sequence of each of its deletions.
    struct DeletedDocs {
        _temp_dir: tempfile::TempDir,
        store: LocalFsStore,
        namespace_id: NamespaceId,
        docs: InodeId,
        deletion_seqs: Vec<CommitSeq>,
    }

    fn restore_docs(docs: InodeId, deletion_seq: CommitSeq) -> FilesystemOperation {
        FilesystemOperation::RestoreDeleted {
            inode_id: docs,
            deletion_seq,
            destination: loonfs_types::options::RestoreDeletedDestination::Recorded,
        }
    }

    /// Deletes `/docs` once, then restores and deletes it again `redeletes`
    /// times, and with `restore_last`, restores its last deletion.
    async fn deleted_docs(redeletes: usize, restore_last: bool) -> DeletedDocs {
        let (temp_dir, store, namespace_id, context) = setup_namespace().await;
        let fixture = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
        fixture
            .submit_operation(
                CommitId::parse("create-docs").expect("valid commit id"),
                create_dir("/docs"),
                Vec::new(),
            )
            .await
            .expect("create /docs");
        let docs = crate::test_support::visible_inode_id(&store, &namespace_id, "/docs").await;
        let mut deletion_seqs = Vec::new();
        for redelete in 0..=redeletes {
            if let Some(&deletion_seq) = deletion_seqs.last() {
                fixture
                    .submit_operation(
                        CommitId::parse(format!("restore-docs-{redelete}"))
                            .expect("valid commit id"),
                        restore_docs(docs, deletion_seq),
                        Vec::new(),
                    )
                    .await
                    .expect("restore /docs");
            }
            let deletion = fixture.delete_path("/docs", None).await.expect("delete");
            deletion_seqs.push(deletion.committed_seq);
        }
        if let Some(&deletion_seq) = deletion_seqs.last().filter(|_| restore_last) {
            fixture
                .submit_operation(
                    CommitId::parse("restore-docs-last").expect("valid commit id"),
                    restore_docs(docs, deletion_seq),
                    Vec::new(),
                )
                .await
                .expect("restore /docs");
        }
        DeletedDocs {
            _temp_dir: temp_dir,
            store,
            namespace_id,
            docs,
            deletion_seqs,
        }
    }

    /// A bind requires an inode with no current binding. If a corrupt
    /// namespace still binds a deleted directory somewhere, a restore of that
    /// directory passes the planner's checks and then fails with
    /// `namespace_corrupt`.
    #[tokio::test]
    async fn a_restore_of_an_inode_that_is_still_bound_reports_corruption() {
        let deleted = deleted_docs(0, false).await;
        let other = DisplayName::parse("other").expect("display name");
        crate::test_support::namespace_store(&deleted.store, &deleted.namespace_id)
            .append_wal_commit(
                vec![WalDelta::BindDirentry {
                    delta_index: 0,
                    parent_inode_id: ROOT_INODE_ID,
                    name_key: NameKey::for_display_name(&other),
                    display_name: other,
                    child_inode_id: deleted.docs,
                    child_kind: InodeKind::Directory,
                    child_created_by: loonfs_test_support::test_actor(),
                    child_created_at_ms: 0,
                    revision: None,
                }],
                Vec::new(),
            )
            .await
            .expect("bind /docs again without revoking its deletion");

        let error = try_plan_against_current_state(
            &deleted.store,
            &deleted.namespace_id,
            &request(FilesystemOperation::RestoreDeleted {
                inode_id: deleted.docs,
                deletion_seq: deleted.deletion_seqs[0],
                destination: loonfs_types::options::RestoreDeletedDestination::Name {
                    parent_inode_id: ROOT_INODE_ID,
                    display_name: DisplayName::parse("restored").expect("display name"),
                },
            }),
        )
        .await
        .expect_err("inode /docs already has a parent");
        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
        let error = failed_operation_error(error);
        assert_matches!(error, CoreError::NamespaceCorrupt(_));
        assert_eq!(
            error.to_string(),
            format!(
                "namespace corrupt: a commit binds inode `{}`, which is already bound as \
                 `other` under parent inode `{ROOT_INODE_ID}`",
                deleted.docs
            )
        );
    }

    /// A restore of a deleted item succeeds only while the deletion that the
    /// request identifies is the latest tombstone event of the item's root,
    /// so a stale request can't cancel a later deletion.
    #[tokio::test]
    async fn a_restore_of_a_deleted_item_applies_only_while_its_deletion_is_the_roots_latest_event()
    {
        let not_deleted =
            |deleted: &DeletedDocs| CommitValidationError::RestoreDeletedTargetNotDeleted {
                inode_id: deleted.docs,
            };
        let mismatch =
            |deleted: &DeletedDocs| CommitValidationError::RestoreDeletedSequenceMismatch {
                inode_id: deleted.docs,
                requested_seq: deleted.deletion_seqs[0],
                actual_deletion_seq: deleted.deletion_seqs[1],
            };
        // Each case: its name, how often `/docs` was restored and deleted
        // again since the first deletion, whether its last deletion was
        // restored, the number of restores of the first deletion in the
        // request, and the error.
        type Expected<'a> = &'a dyn Fn(&DeletedDocs) -> CommitValidationError;
        let cases: [(&str, usize, bool, usize, Expected); 3] = [
            ("restored twice in one commit", 0, false, 2, &not_deleted),
            ("already restored", 0, true, 1, &not_deleted),
            ("deleted again since", 1, false, 1, &mismatch),
        ];
        for (name, redeletes, restore_last, restores, expected) in cases {
            let deleted = deleted_docs(redeletes, restore_last).await;
            let error = try_plan_against_current_state(
                &deleted.store,
                &deleted.namespace_id,
                &CommitIntent {
                    preconditions: Vec::new(),
                    commit_id: CommitId::parse("restore-request").expect("valid commit id"),
                    actor_id: loonfs_test_support::test_actor(),
                    subject: None,
                    message: None,
                    operations: vec![
                        restore_docs(deleted.docs, deleted.deletion_seqs[0]);
                        restores
                    ],
                },
            )
            .await
            .expect_err(name);
            assert_eq!(error.code(), ErrorCode::NotDeleted, "{name}");
            let operation_index = u32::try_from(restores - 1).expect("operation index");
            assert_matches!(
                error,
                CoreError::FailedOperation { operation_index: index, error }
                    if index == operation_index
                        && matches!(
                            &*error,
                            CoreError::CommitValidation(actual) if *actual == expected(&deleted)
                        ),
                "{name}"
            );
        }

        let deleted = deleted_docs(0, false).await;
        let planned = plan_against_current_state(
            &deleted.store,
            &deleted.namespace_id,
            &request(restore_docs(deleted.docs, deleted.deletion_seqs[0])),
        )
        .await;
        assert_matches!(
            planned.deltas.as_slice(),
            [
                WalCommitDelta {
                    delta: WalDelta::RevokeSubtreeTombstone { target, .. },
                    ..
                },
                WalCommitDelta {
                    delta: WalDelta::BindDirentry { .. },
                    ..
                },
            ] if target.seq == deleted.deletion_seqs[0]
        );
    }
}
