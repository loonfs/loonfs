//! Merge planning: the fork's net change over a range, its classification
//! against the merged-into namespace's head under the conflict rule, and the
//! commit that applies it in the order that its changes need.

use crate::common::{default_writer, namespace_engine, read_context, resolve_path, submit_commit};
use loonfs_core::publish::{CommitIntent, FilesystemOperation};
use loonfs_core::test_support::{NamespaceFixture, WriterAt};
use loonfs_core::{
    CoreError, CreateNamespaceOptions, MergeAllowance, MergeBound, MergeRequest,
    NamespaceWriterEngine, RetentionTarget,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{attribute_key, attribute_text, namespace_id};
use loonfs_types::options::StatOptions;
use loonfs_types::{
    AbsolutePath, AccessGrants, AccessRight, AccessRights, ChangeSeq, Commit, CommitId, ErrorCode,
    InodeId, MergeConflictKind, NamespaceAccess, NamespaceId, PathEntry, PrincipalId,
    PrincipalScope, Subject,
};
use std::assert_matches;
use tempfile::TempDir;

/// A namespace and a fork of it, over one local store.
struct Family {
    _directory: TempDir,
    store: LocalFsStore,
    main: NamespaceId,
    fork: NamespaceId,
    writer: WriterAt,
    /// The subject that writes and merges act as, in a namespace with ACLs.
    subject: Option<Subject>,
    /// The merge allowance that merges are planned against, which admits
    /// any merge unless a test lowers it.
    allowance: MergeAllowance,
}

impl Family {
    fn empty(subject: Option<Subject>) -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        Self {
            store: LocalFsStore::new(directory.path()).expect("store"),
            _directory: directory,
            main: namespace_id("main"),
            fork: namespace_id("main-fork"),
            writer: default_writer(),
            subject,
            allowance: MergeAllowance {
                max_bytes: usize::MAX,
                peak_per_planned_byte: 3,
            },
        }
    }

    /// Creates `main` with `/docs/a.txt`, `/docs/b.txt`, `/old/x.txt`, and
    /// `/p/q/f.txt`, each holding its own path, and forks it.
    async fn new() -> Self {
        let family = Self::empty(None);
        family.main().create().await.expect("create main");
        for path in ["/docs/a.txt", "/docs/b.txt", "/old/x.txt", "/p/q/f.txt"] {
            family
                .main()
                .write_file_bytes(path, path.as_bytes(), None)
                .await
                .expect("seed");
        }
        family.fork_main().await;
        family
    }

    /// Creates `main` with ACLs and the directory `/docs`, and forks it.
    /// `prn_root` administers the root, `prn_team` can read, write, create,
    /// and remove there, and writes and merges act as `prn_root`.
    async fn acl() -> Self {
        let family = Self::empty(Some(subject("root", &["prn_root"])));
        family
            .main()
            .create_with_options(&CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                    root_grants: grants(&[
                        ("prn_root", &[AccessRight::Admin]),
                        (
                            "prn_team",
                            &[
                                AccessRight::Read,
                                AccessRight::Write,
                                AccessRight::Create,
                                AccessRight::Remove,
                            ],
                        ),
                    ]),
                },
                ..CreateNamespaceOptions::default()
            })
            .await
            .expect("create main");
        family
            .commit(&family.main, create_directory("/docs"))
            .await
            .expect("seed");
        family.fork_main().await;
        family
    }

    async fn fork_main(&self) {
        namespace_engine(&self.store, &self.main, &self.writer)
            .fork_namespace(&self.fork, &loonfs_test_support::test_actor(), None)
            .await
            .expect("fork main");
    }

    fn main(&self) -> NamespaceFixture<&LocalFsStore> {
        NamespaceFixture::new(&self.store, &self.main, &self.writer)
    }

    fn fork(&self) -> NamespaceFixture<&LocalFsStore> {
        NamespaceFixture::new(&self.store, &self.fork, &self.writer)
    }

    /// Commits `operations` in `namespace_id` as the family's subject.
    async fn commit_all(
        &self,
        namespace_id: &NamespaceId,
        operations: Vec<FilesystemOperation>,
    ) -> Result<Commit, CoreError> {
        let intent = CommitIntent {
            commit_id: CommitId::generate(),
            actor_id: loonfs_test_support::test_actor(),
            subject: self.subject.clone(),
            message: None,
            operations,
            preconditions: Vec::new(),
        };
        submit_commit(&self.store, namespace_id, intent, &self.writer).await
    }

    async fn commit(
        &self,
        namespace_id: &NamespaceId,
        operation: FilesystemOperation,
    ) -> Result<Commit, CoreError> {
        self.commit_all(namespace_id, vec![operation]).await
    }

    fn request(&self, commit_id: &str, through_seq: Option<ChangeSeq>) -> MergeRequest {
        MergeRequest {
            source_namespace_id: self.fork.clone(),
            through_seq,
            after_seq: None,
            commit_id: CommitId::parse(commit_id).expect("commit id"),
            actor_id: loonfs_test_support::test_actor(),
            message: None,
        }
    }

    /// Merges the fork into `main` through `through_seq` under `commit_id`.
    async fn merge(
        &self,
        commit_id: &str,
        through_seq: Option<ChangeSeq>,
    ) -> Result<Commit, CoreError> {
        self.merge_as(&self.request(commit_id, through_seq), self.subject.as_ref())
            .await
    }

    /// Merges as the runtime does, as `subject`, with the fork's fold, the
    /// receipt lookup and the plan at one view of `main`, and the commit.
    async fn merge_as(
        &self,
        request: &MergeRequest,
        subject: Option<&Subject>,
    ) -> Result<Commit, CoreError> {
        let main = self.engine(&self.main, subject);
        let fork = self.engine(&self.fork, subject);
        fork.fold_wal().await?;
        let main_context = read_context(&self.store, &self.main).await;
        if let Some(commit) = main.find_merge(&main_context, request).await? {
            return Ok(commit);
        }
        let fork_context = read_context(&self.store, &self.fork).await;
        let candidate = main
            .plan_merge(
                &main_context,
                &fork,
                &fork_context,
                request,
                self.writer.now_ms,
                self.allowance,
            )
            .await?
            .into_candidate()?;
        self.main()
            .publish_candidates(vec![candidate])
            .await
            .pop()
            .expect("one result")
    }

    /// Returns an engine over `namespace_id` that acts as `subject`.
    fn engine(
        &self,
        namespace_id: &NamespaceId,
        subject: Option<&Subject>,
    ) -> NamespaceWriterEngine<&LocalFsStore> {
        let engine = namespace_engine(&self.store, namespace_id, &self.writer);
        match subject {
            Some(subject) => engine.with_subject(subject.clone()),
            None => engine,
        }
    }

    async fn entry(&self, namespace_id: &NamespaceId, path: &str) -> Option<PathEntry> {
        self.engine(namespace_id, self.subject.as_ref())
            .resolve_path(
                path,
                StatOptions::default(),
                &read_context(&self.store, namespace_id).await,
            )
            .await
            .ok()
    }

    async fn inode(&self, namespace_id: &NamespaceId, path: &str) -> InodeId {
        self.entry(namespace_id, path)
            .await
            .unwrap_or_else(|| panic!("{path} in {namespace_id}"))
            .inode_id
    }

    /// Returns the bytes of the file at `path` in `main`, or `None` when
    /// it has no entry there.
    async fn main_bytes(&self, path: &str) -> Option<Vec<u8>> {
        crate::common::read_file_bytes(&self.store, &self.main, path)
            .await
            .ok()
            .map(|file| file.bytes)
    }

    async fn set_attribute(
        &self,
        fixture: NamespaceFixture<&LocalFsStore>,
        path: &str,
        value: &str,
    ) {
        fixture
            .submit_operation(CommitId::generate(), set_attribute(path, value), Vec::new())
            .await
            .expect("set an attribute");
    }

    /// Advances `namespace_id`'s retention floor to its folded head, and
    /// returns the floor.
    async fn advance_floor(&self, namespace_id: &NamespaceId) -> ChangeSeq {
        let engine = namespace_engine(&self.store, namespace_id, &self.writer);
        engine.fold_wal().await.expect("fold");
        engine
            .advance_retention_floor(RetentionTarget::Head)
            .await
            .expect("advance the floor")
            .response
            .retention_floor_seq
    }
}

fn subject(id: &str, principals: &[&str]) -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: loonfs_types::SubjectId::parse(id).expect("subject id"),
        principals: loonfs_types::PrincipalSet::new(
            principals
                .iter()
                .map(|id| PrincipalId::parse(id).expect("principal id"))
                .collect(),
        )
        .expect("principals"),
    }
}

fn grants(entries: &[(&str, &[AccessRight])]) -> AccessGrants {
    AccessGrants::new(
        entries
            .iter()
            .map(|(id, rights)| {
                (
                    PrincipalId::parse(id).expect("principal"),
                    AccessRights::from_iter(rights.iter().copied()),
                )
            })
            .collect(),
    )
    .expect("grants")
}

fn create_directory(path: &str) -> FilesystemOperation {
    FilesystemOperation::CreateDirectory {
        path: AbsolutePath::parse(path).expect("path"),
        parents: true,
    }
}

fn set_attribute(path: &str, value: &str) -> FilesystemOperation {
    FilesystemOperation::UpdateAttributes {
        path: AbsolutePath::parse(path).expect("path"),
        set: [(attribute_key("stage"), attribute_text(value))].into(),
        remove: Vec::new(),
        expected_inode_id: None,
        expected_attributes_revision_no: None,
    }
}

fn update_access(path: &str, boundary: bool, grants: AccessGrants) -> FilesystemOperation {
    FilesystemOperation::UpdateAccess {
        path: AbsolutePath::parse(path).expect("path"),
        boundary,
        grants,
        expected_inode_id: None,
        expected_access_revision_no: None,
    }
}

fn content(entry: &PathEntry) -> Option<&loonfs_types::ContentRef> {
    match &entry.kind {
        loonfs_types::PathEntryKind::File { content_ref, .. } => content_ref.as_ref(),
        _ => None,
    }
}

/// Checks that a merge applies the fork's creates, an edit, a move, and a
/// directory delete as one commit with the merge record, that the inodes it
/// creates keep their IDs in the fork, and that a directory the fork
/// created is created before the file in it.
#[tokio::test]
async fn a_merge_applies_the_forks_net_change_as_one_commit() {
    let family = Family::new().await;
    family
        .fork()
        .write_file_bytes("/new/deep/c.txt", b"c", None)
        .await
        .expect("create");
    family
        .fork()
        .write_file_bytes("/docs/a.txt", b"edited", None)
        .await
        .expect("edit");
    family
        .fork()
        .move_path("/docs/b.txt", "/new/b.txt", None)
        .await
        .expect("move");
    family
        .fork()
        .delete_path("/old", None)
        .await
        .expect("delete");

    let commit = family.merge("merge-1", None).await.expect("merge");
    let merge = commit.merge.expect("the commit is a merge");
    assert_eq!(merge.source_namespace_id, family.fork);
    for path in ["/new", "/new/deep", "/new/deep/c.txt"] {
        assert_eq!(
            family.inode(&family.main, path).await,
            family.inode(&family.fork, path).await,
            "{path} keeps its ID"
        );
    }
    let edited = family
        .entry(&family.main, "/docs/a.txt")
        .await
        .expect("a.txt");
    let ours = family
        .entry(&family.fork, "/docs/a.txt")
        .await
        .expect("a.txt");
    assert_eq!(content(&edited), content(&ours));
    assert!(family.entry(&family.main, "/docs/b.txt").await.is_none());
    assert!(family.entry(&family.main, "/new/b.txt").await.is_some());
    assert!(family.entry(&family.main, "/old").await.is_none());
    assert!(family.entry(&family.main, "/old/x.txt").await.is_none());

    let repeat = family.merge("merge-1", None).await.expect("a retry");
    assert_eq!(
        repeat.committed_seq, commit.committed_seq,
        "a retry returns the merge"
    );
}

/// One case of the conflict rule: a change in `main` after the fork point,
/// the fork's change to the same inode, and the conflict that the merge
/// reports.
struct Case {
    name: &'static str,
    kind: MergeConflictKind,
    /// The inode's path in `main` at the fork point, or in the fork for an
    /// inode that the fork created.
    path: &'static str,
    /// The path of the conflict's base, which an inode that the fork
    /// created doesn't have.
    base: Option<&'static str>,
}

/// Checks each check of the conflict rule. The merge is refused with
/// `merge_conflict`, the details count the conflicting inodes and report
/// the first, with main's version at the fork point as the base, and
/// nothing is committed.
#[tokio::test]
async fn each_check_of_the_conflict_rule_refuses_the_merge() {
    let case = |name, kind, path, base| Case {
        name,
        kind,
        path,
        base,
    };
    let cases = [
        case(
            "content",
            MergeConflictKind::Content,
            "/docs/a.txt",
            Some("/docs/a.txt"),
        ),
        case(
            "binding",
            MergeConflictKind::Binding,
            "/docs/a.txt",
            Some("/docs/a.txt"),
        ),
        case(
            "attributes",
            MergeConflictKind::Attributes,
            "/docs/a.txt",
            Some("/docs/a.txt"),
        ),
        case(
            "visibility",
            MergeConflictKind::Visibility,
            "/docs/a.txt",
            Some("/docs/a.txt"),
        ),
        case(
            "visibility of a create's parent",
            MergeConflictKind::Visibility,
            "/docs/new.txt",
            None,
        ),
        case(
            "visibility of a move's parent",
            MergeConflictKind::Visibility,
            "/docs/a.txt",
            Some("/docs/a.txt"),
        ),
        case("name", MergeConflictKind::Name, "/docs/n.txt", None),
        case(
            "name of a move",
            MergeConflictKind::Name,
            "/docs/a.txt",
            Some("/docs/a.txt"),
        ),
        case("subtree", MergeConflictKind::Subtree, "/old", Some("/old")),
        case(
            "subtree of a move",
            MergeConflictKind::Subtree,
            "/old",
            Some("/old"),
        ),
    ];
    for case in cases {
        let family = Family::new().await;
        let main = family.main();
        let fork = family.fork();
        let existing = family.entry(&family.main, case.path).await;
        match case.name {
            "content" => {
                main.write_file_bytes(case.path, b"theirs", None)
                    .await
                    .expect("main");
                fork.write_file_bytes(case.path, b"ours", None)
                    .await
                    .expect("fork");
            }
            "binding" => {
                main.move_path(case.path, "/docs/theirs.txt", None)
                    .await
                    .expect("main");
                fork.move_path(case.path, "/docs/ours.txt", None)
                    .await
                    .expect("fork");
            }
            "attributes" => {
                family.set_attribute(main, case.path, "theirs").await;
                family.set_attribute(fork, case.path, "ours").await;
            }
            "visibility" => {
                main.delete_path(case.path, None).await.expect("main");
                fork.write_file_bytes(case.path, b"ours", None)
                    .await
                    .expect("fork");
            }
            "visibility of a create's parent" => {
                main.delete_path("/docs", None).await.expect("main");
                fork.write_file_bytes(case.path, b"ours", None)
                    .await
                    .expect("fork");
            }
            "visibility of a move's parent" => {
                main.delete_path("/old", None).await.expect("main");
                fork.move_path(case.path, "/old/a.txt", None)
                    .await
                    .expect("fork");
            }
            "name" => {
                main.write_file_bytes(case.path, b"theirs", None)
                    .await
                    .expect("main");
                fork.write_file_bytes(case.path, b"ours", None)
                    .await
                    .expect("fork");
            }
            "name of a move" => {
                main.write_file_bytes("/docs/n.txt", b"theirs", None)
                    .await
                    .expect("main");
                fork.move_path(case.path, "/docs/n.txt", None)
                    .await
                    .expect("fork");
            }
            "subtree" => {
                main.write_file_bytes("/old/y.txt", b"theirs", None)
                    .await
                    .expect("main");
                fork.delete_path(case.path, None).await.expect("fork");
            }
            "subtree of a move" => {
                main.write_file_bytes("/old/y.txt", b"theirs", None)
                    .await
                    .expect("main");
                fork.move_path(case.path, "/older", None)
                    .await
                    .expect("fork");
            }
            other => panic!("no setup for {other}"),
        }
        let head = resolve_path(&family.store, &family.main, "/")
            .await
            .expect("root")
            .head_seq;
        let error = family.merge("conflict", None).await.expect_err(case.name);
        assert_eq!(error.code(), ErrorCode::MergeConflict, "{}", case.name);
        let details = error.details().expect("details");
        assert_eq!(details.conflict_count, Some(1), "{}", case.name);
        let conflict = details.conflict.expect("the first conflict");
        assert_eq!(conflict.kind, case.kind, "{}", case.name);
        let inode_id = match existing {
            Some(entry) => entry.inode_id,
            None => family.inode(&family.fork, case.path).await,
        };
        assert_eq!(conflict.inode_id, inode_id, "{}", case.name);
        let base = conflict.base.as_ref().map(|base| base.path.as_str());
        assert_eq!(base, case.base, "{}", case.name);
        assert_eq!(
            resolve_path(&family.store, &family.main, "/")
                .await
                .expect("root")
                .head_seq,
            head,
            "{} commits nothing",
            case.name
        );
    }
}

/// Checks that an inode that takes a name conflicts too when the change of
/// the inode that has the name conflicts. The fork replaces `a.txt` with a
/// new file, and `main` edited `a.txt` after the fork point, so the old
/// file's delete conflicts and the new file can't take its name.
#[tokio::test]
async fn a_create_that_needs_a_conflicting_inodes_name_conflicts_too() {
    let family = Family::new().await;
    let old = family.inode(&family.main, "/docs/a.txt").await;
    family
        .main()
        .write_file_bytes("/docs/a.txt", b"theirs", None)
        .await
        .expect("main");
    let fork = family.fork();
    fork.delete_path("/docs/a.txt", None).await.expect("delete");
    fork.write_file_bytes("/docs/a.txt", b"new", None)
        .await
        .expect("create");

    let error = family.merge("replace", None).await.expect_err("conflict");
    let details = error.details().expect("details");
    assert_eq!(details.conflict_count, Some(2));
    let conflict = details.conflict.expect("the first conflict");
    assert_eq!(conflict.inode_id, old);
    assert_eq!(conflict.kind, MergeConflictKind::Content);
}

/// Undoes the fork's deletion of `inode_id`, whose commit was `deleted`, so
/// the inode comes back where it was.
async fn undelete_in_the_fork(family: &Family, inode_id: InodeId, deleted: &Commit) {
    family
        .fork()
        .submit_operation(
            CommitId::generate(),
            FilesystemOperation::Undelete {
                inode_id,
                deletion_seq: deleted.committed_seq,
                destination_path: None,
                destination_parent_inode_id: None,
                destination_display_name: None,
            },
            Vec::new(),
        )
        .await
        .expect("undelete in the fork");
}

/// Checks that a deletion that an earlier merge of the fork made, and that
/// the fork then undid, merges as an undelete. The fork deletes `a.txt`, the
/// merge deletes it in `main`, the fork undeletes it, and the next merge
/// undeletes it in `main` under the same inode ID.
#[tokio::test]
async fn a_file_that_the_fork_undeleted_after_a_merge_merges() {
    let family = Family::new().await;
    let inode_id = family.inode(&family.main, "/docs/a.txt").await;
    let deleted = family
        .fork()
        .delete_path("/docs/a.txt", None)
        .await
        .expect("delete");
    family
        .merge("delete", None)
        .await
        .expect("merge the delete");
    assert!(family.entry(&family.main, "/docs/a.txt").await.is_none());
    undelete_in_the_fork(&family, inode_id, &deleted).await;

    family
        .merge("undelete", None)
        .await
        .expect("merge the undelete");
    assert_eq!(family.inode(&family.main, "/docs/a.txt").await, inode_id);
    assert_eq!(
        family.main_bytes("/docs/a.txt").await.as_deref(),
        Some(&b"/docs/a.txt"[..])
    );
}

/// Checks the undelete of a directory. The items below it come back with it,
/// with no change of their own.
#[tokio::test]
async fn a_directory_that_the_fork_undeleted_after_a_merge_merges() {
    let family = Family::new().await;
    let inode_id = family.inode(&family.main, "/old").await;
    let deleted = family
        .fork()
        .delete_path("/old", None)
        .await
        .expect("delete");
    family
        .merge("delete", None)
        .await
        .expect("merge the delete");
    undelete_in_the_fork(&family, inode_id, &deleted).await;

    family
        .merge("undelete", None)
        .await
        .expect("merge the undelete");
    assert_eq!(
        family.main_bytes("/old/x.txt").await.as_deref(),
        Some(&b"/old/x.txt"[..])
    );
}

/// Checks that a deletion that isn't the fork's own still conflicts. `main`
/// deleted `a.txt` after the fork point, and the fork changed the file.
#[tokio::test]
async fn a_deletion_that_isnt_the_forks_own_conflicts() {
    let family = Family::new().await;
    family
        .main()
        .delete_path("/docs/a.txt", None)
        .await
        .expect("main");
    family
        .set_attribute(family.fork(), "/docs/a.txt", "ours")
        .await;

    let error = family.merge("foreign", None).await.expect_err("conflict");
    let conflict = error
        .details()
        .and_then(|details| details.conflict)
        .expect("the first conflict");
    assert_eq!(conflict.kind, MergeConflictKind::Visibility);
}

/// Checks the `access` check of the conflict rule in a namespace with ACLs.
/// A directory whose access row `main` changed after the fork point, and
/// whose access row the fork changed too, conflicts.
#[tokio::test]
async fn a_changed_access_row_conflicts() {
    let family = Family::acl().await;
    family
        .commit(
            &family.main,
            update_access(
                "/docs",
                false,
                grants(&[("prn_team", &[AccessRight::Read])]),
            ),
        )
        .await
        .expect("main");
    family
        .commit(
            &family.fork,
            update_access(
                "/docs",
                false,
                grants(&[("prn_team", &[AccessRight::Write])]),
            ),
        )
        .await
        .expect("fork");

    let error = family.merge("access", None).await.expect_err("conflict");
    assert_eq!(error.code(), ErrorCode::MergeConflict);
    let conflict = error
        .details()
        .and_then(|details| details.conflict)
        .expect("the first conflict");
    assert_eq!(conflict.kind, MergeConflictKind::Access);
    assert_eq!(conflict.inode_id, family.inode(&family.main, "/docs").await);
}

/// Checks that the merge's subject has to be able to read each inode whose
/// change the merge applies in the fork. A subject that can write a
/// directory, but not read the directory, can't merge the fork's change to
/// the directory.
#[tokio::test]
async fn the_merges_subject_has_to_read_the_forks_change() {
    let family = Family::acl().await;
    family
        .commit_all(
            &family.fork,
            vec![
                create_directory("/docs/secret"),
                update_access(
                    "/docs/secret",
                    true,
                    grants(&[("prn_team", &[AccessRight::Write])]),
                ),
            ],
        )
        .await
        .expect("fork");

    let secret = family.inode(&family.fork, "/docs/secret").await;
    let error = family
        .merge_as(
            &family.request("unreadable", None),
            Some(&subject("team", &["prn_team"])),
        )
        .await
        .expect_err("the subject can't read the change");
    assert_matches!(error, CoreError::Forbidden { inode_id } if inode_id == secret);
    family
        .merge("readable", None)
        .await
        .expect("an administrator can read it");
}

/// Checks that a merge that would exceed one WAL object fails with
/// `merge_too_large` before the conflict check, so the error isn't
/// `merge_conflict` although a create's name conflicts.
#[tokio::test]
async fn a_merge_over_one_wal_object_is_too_large_before_its_conflicts() {
    let family = Family::new().await;
    // Each directory's attribute update bounds about 66 KB, so 8,192 of them
    // bound more than one WAL object's 512 MiB.
    for batch in 0..4 {
        let directories: Vec<_> = (0..2_048)
            .map(|index| format!("/c{batch}-{index}"))
            .collect();
        family
            .commit_all(
                &family.fork,
                directories
                    .iter()
                    .map(|path| create_directory(path))
                    .chain(directories.iter().map(|path| set_attribute(path, "ours")))
                    .collect(),
            )
            .await
            .expect("fork");
    }
    family
        .main()
        .write_file_bytes("/c0-0", b"theirs", None)
        .await
        .expect("a name that conflicts");

    let error = family
        .merge("too-large", None)
        .await
        .expect_err("too large");
    assert_matches!(
        error,
        CoreError::MergeTooLarge {
            bound: MergeBound::WalObject,
            estimated_bytes,
            max_bytes,
            split_at_seq: None,
        } if estimated_bytes > max_bytes
    );
}

/// Checks that a range whose commits touched more inodes than one WAL object
/// could contain operations for fails with `merge_too_large` before the plan
/// classifies those inodes. After a commit that writes one file, one commit
/// of the fork creates 291,328 directories in chains 128 deep, and the next
/// commit deletes the chains, so the net change is empty. The error's
/// `split_at_seq` is the file's commit, and a merge through it commits.
#[tokio::test]
#[ignore = "creates 291,328 directories, which takes about two minutes in a debug build"]
async fn a_range_that_touched_too_many_inodes_is_too_large() {
    let family = Family::new().await;
    let small = family
        .fork()
        .write_file_bytes("/small.txt", b"small", None)
        .await
        .expect("one file");
    let chain = (1..128).fold(String::new(), |path, _| path + "/a");
    let chains: Vec<_> = (0..2_276).map(|index| format!("/c{index}")).collect();
    family
        .commit_all(
            &family.fork,
            chains
                .iter()
                .map(|root| create_directory(&format!("{root}{chain}")))
                .collect(),
        )
        .await
        .expect("create the chains");
    family
        .commit_all(
            &family.fork,
            chains
                .iter()
                .map(|root| FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse(root).expect("path"),
                    behavior: loonfs_types::DeleteDirectoryBehavior::Recursive,
                    expected_inode_id: None,
                })
                .collect(),
        )
        .await
        .expect("delete the chains");

    let error = family.merge("touched", None).await.expect_err("too large");
    assert_eq!(
        error.details().and_then(|details| details.split_at_seq),
        Some(small.committed_seq)
    );
    assert_matches!(
        error,
        CoreError::MergeTooLarge {
            bound: MergeBound::TouchedInodes,
            ..
        }
    );
    family
        .merge("narrowed", Some(small.committed_seq))
        .await
        .expect("a merge through the split");
    assert!(family.entry(&family.main, "/small.txt").await.is_some());
}

/// Checks that the planner stops a merge whose estimated peak exceeds the
/// merge allowance with `merge_too_large` before the conflict check, and that
/// the error reports the peak and the allowance, as admission does.
#[tokio::test]
async fn a_merge_over_the_allowance_is_too_large_before_its_conflicts() {
    let mut family = Family::new().await;
    // A file create plans about 4,850 bytes, which the allowance is charged
    // three times, so 64 KiB admits only a few creates.
    family.allowance = MergeAllowance {
        max_bytes: 64 * 1024,
        peak_per_planned_byte: 3,
    };
    for index in 0..16 {
        family
            .fork()
            .write_file_bytes(&format!("/f{index}.txt"), b"ours", None)
            .await
            .expect("fork");
    }
    family
        .main()
        .write_file_bytes("/f0.txt", b"theirs", None)
        .await
        .expect("a name that conflicts");

    let error = family
        .merge("over-the-allowance", None)
        .await
        .expect_err("too large");
    assert_matches!(
        error,
        CoreError::MergeTooLarge {
            bound: MergeBound::MergeAllowance,
            estimated_bytes,
            max_bytes,
            split_at_seq: None,
        } if estimated_bytes > max_bytes && max_bytes == 64 * 1024 && estimated_bytes % 3 == 0
    );
}

/// Checks that a file that the fork replaced with a new file of the same
/// name merges. The merge deletes the old file before it creates the new
/// one, so the name is free when the create needs the name.
#[tokio::test]
async fn a_name_that_the_merge_frees_is_free_for_its_create() {
    let family = Family::new().await;
    let old = family.inode(&family.main, "/docs/a.txt").await;
    family
        .fork()
        .delete_path("/docs/a.txt", None)
        .await
        .expect("delete");
    family
        .fork()
        .write_file_bytes("/docs/a.txt", b"new", None)
        .await
        .expect("create");
    let new = family.inode(&family.fork, "/docs/a.txt").await;
    assert_ne!(new, old);

    family.merge("replace", None).await.expect("merge");
    assert_eq!(family.inode(&family.main, "/docs/a.txt").await, new);
}

/// Checks that a directory's delete covers the inodes below the directory.
/// The fork renames a directory below `/p` and then deletes `/p`, and the
/// merge deletes `/p` alone, which deletes the renamed directory too.
#[tokio::test]
async fn a_directory_delete_covers_the_changes_below_it() {
    let family = Family::new().await;
    family
        .fork()
        .move_path("/p/q", "/p/r", None)
        .await
        .expect("rename");
    family.fork().delete_path("/p", None).await.expect("delete");

    let commit = family.merge("nested", None).await.expect("merge");
    assert_eq!(commit.events.len(), 1, "{:?}", commit.events);
    assert!(family.entry(&family.main, "/p").await.is_none());
    assert!(family.entry(&family.main, "/p/q/f.txt").await.is_none());
}

/// Merges the fork and checks the bytes at each path in `main`, where
/// `None` means that the path has no entry.
async fn merges_to(family: &Family, expected: &[(&str, Option<&[u8]>)]) -> Commit {
    let commit = family.merge("ordered", None).await.expect("merge");
    for (path, bytes) in expected {
        match bytes {
            Some(bytes) => assert_eq!(
                family.main_bytes(path).await.as_deref(),
                Some(*bytes),
                "{path}"
            ),
            None => assert!(family.entry(&family.main, path).await.is_none(), "{path}"),
        }
    }
    commit
}

/// Checks `rm -r old && mkdir old`. The merge deletes the directory before
/// it creates the new directory with the same name.
#[tokio::test]
async fn a_directory_replaced_by_a_new_one_merges() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.delete_path("/old", None).await.expect("delete");
    fork.write_file_bytes("/old/y.txt", b"y", None)
        .await
        .expect("create");
    merges_to(&family, &[("/old/y.txt", Some(b"y")), ("/old/x.txt", None)]).await;
}

/// Checks `mv old old.bak && mkdir old`. The merge moves the directory away
/// before it creates the new directory with the same name.
#[tokio::test]
async fn a_directory_moved_aside_for_a_new_one_merges() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.move_path("/old", "/old.bak", None)
        .await
        .expect("move");
    fork.write_file_bytes("/old/y.txt", b"y", None)
        .await
        .expect("create");
    merges_to(
        &family,
        &[
            ("/old.bak/x.txt", Some(b"/old/x.txt")),
            ("/old/y.txt", Some(b"y")),
        ],
    )
    .await;
}

/// Checks `mv a c && mv b a`. The merge moves `a` before `b` takes the name
/// `a`.
#[tokio::test]
async fn a_name_that_one_move_frees_is_free_for_another() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.move_path("/docs/a.txt", "/docs/c.txt", None)
        .await
        .expect("move a");
    fork.move_path("/docs/b.txt", "/docs/a.txt", None)
        .await
        .expect("move b");
    merges_to(
        &family,
        &[
            ("/docs/c.txt", Some(b"/docs/a.txt")),
            ("/docs/a.txt", Some(b"/docs/b.txt")),
            ("/docs/b.txt", None),
        ],
    )
    .await;
}

/// Checks `rm -r old && mv docs old`. The merge deletes `old` before `docs`
/// takes the name `old`.
#[tokio::test]
async fn a_name_that_a_directory_delete_frees_is_free_for_a_move() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.delete_path("/old", None).await.expect("delete");
    fork.move_path("/docs", "/old", None).await.expect("move");
    merges_to(
        &family,
        &[
            ("/old/a.txt", Some(b"/docs/a.txt")),
            ("/old/x.txt", None),
            ("/docs", None),
        ],
    )
    .await;
}

/// Checks `/p/q` to `/q/p`. The merge moves `q` out of `p` before it moves
/// `p` into `q`, which would otherwise move `p` below itself.
#[tokio::test]
async fn a_parent_moved_below_its_child_merges() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.move_path("/p/q", "/q", None).await.expect("move q");
    fork.move_path("/p", "/q/p", None).await.expect("move p");
    merges_to(&family, &[("/q/f.txt", Some(b"/p/q/f.txt")), ("/p", None)]).await;
    assert!(family.entry(&family.main, "/q/p").await.is_some());
}

/// Replaces `/old` in the fork with a new directory and keeps `/old/x.txt`
/// in it: `mv /old /old.prev && mkdir /old && mv /old.prev/x.txt /old/ && rm
/// -r /old.prev`. The new directory waits for the old one to free its name,
/// the old one waits for `x.txt` to leave it, and `x.txt` waits for the new
/// directory, so the merge first moves `x.txt` to a temporary name in the
/// root, outside the directory that the merge deletes.
async fn replace_old_and_keep_its_file(family: &Family) {
    let fork = family.fork();
    fork.move_path("/old", "/old.prev", None)
        .await
        .expect("move aside");
    family
        .commit(&family.fork, create_directory("/old"))
        .await
        .expect("create the new directory");
    fork.move_path("/old.prev/x.txt", "/old/x.txt", None)
        .await
        .expect("keep the file");
    fork.delete_path("/old.prev", None)
        .await
        .expect("delete the old directory");
    let replaced = family.inode(&family.main, "/old").await;
    let kept = family.inode(&family.main, "/old/x.txt").await;
    merges_to(
        family,
        &[("/old/x.txt", Some(b"/old/x.txt")), ("/old.prev", None)],
    )
    .await;
    assert_eq!(family.inode(&family.main, "/old/x.txt").await, kept);
    assert_ne!(family.inode(&family.main, "/old").await, replaced);
}

/// Checks the directory replacement when the kept file's inode ID is above
/// the directory's.
#[tokio::test]
async fn a_directory_replaced_while_keeping_a_file_merges() {
    let family = Family::new().await;
    assert!(
        family.inode(&family.main, "/old").await < family.inode(&family.main, "/old/x.txt").await
    );
    replace_old_and_keep_its_file(&family).await;
}

/// Checks the directory replacement when the kept file's inode ID is below
/// the directory's, because the file was created before the directory and
/// moved into it.
#[tokio::test]
async fn a_directory_replaced_while_keeping_an_older_file_merges() {
    let family = Family::empty(None);
    family.main().create().await.expect("create main");
    family
        .main()
        .write_file_bytes("/x.txt", b"/old/x.txt", None)
        .await
        .expect("the file");
    family
        .commit(&family.main, create_directory("/old"))
        .await
        .expect("the directory");
    family
        .main()
        .move_path("/x.txt", "/old/x.txt", None)
        .await
        .expect("move the file in");
    family.fork_main().await;
    assert!(
        family.inode(&family.main, "/old").await > family.inode(&family.main, "/old/x.txt").await
    );
    replace_old_and_keep_its_file(&family).await;
}

/// Checks `mv /old/x.txt /docs/ && rm -r /old`. The merge moves the file out
/// before it deletes the directory.
#[tokio::test]
async fn a_file_moved_out_of_a_deleted_directory_merges() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.move_path("/old/x.txt", "/docs/x.txt", None)
        .await
        .expect("move out");
    fork.delete_path("/old", None).await.expect("delete");
    merges_to(
        &family,
        &[("/docs/x.txt", Some(b"/old/x.txt")), ("/old", None)],
    )
    .await;
}

/// Checks `mv /p/q /q && rm /q/f.txt && rm -r /p`. The file's delete stays,
/// because the file left `/p` with its directory, and the merge moves `q` out
/// before it deletes `/p`.
#[tokio::test]
async fn a_delete_below_a_moved_directory_stays_beside_a_directory_delete() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.move_path("/p/q", "/q", None).await.expect("move q");
    fork.delete_path("/q/f.txt", None)
        .await
        .expect("delete the file");
    fork.delete_path("/p", None).await.expect("delete p");
    merges_to(&family, &[("/q/f.txt", None), ("/p", None)]).await;
    assert!(family.entry(&family.main, "/q").await.is_some());
}

/// Checks a swap of two directories' names.
#[tokio::test]
async fn a_swap_of_two_directories_merges() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.move_path("/docs", "/t", None)
        .await
        .expect("move docs");
    fork.move_path("/old", "/docs", None)
        .await
        .expect("move old");
    fork.move_path("/t", "/old", None).await.expect("move t");
    merges_to(
        &family,
        &[
            ("/docs/x.txt", Some(b"/old/x.txt")),
            ("/old/a.txt", Some(b"/docs/a.txt")),
        ],
    )
    .await;
}

/// Checks a cycle of three directory moves, where each directory takes the
/// next one's name.
#[tokio::test]
async fn a_cycle_of_three_directory_moves_merges() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.move_path("/docs", "/t", None)
        .await
        .expect("move docs");
    fork.move_path("/p", "/docs", None).await.expect("move p");
    fork.move_path("/old", "/p", None).await.expect("move old");
    fork.move_path("/t", "/old", None).await.expect("move t");
    merges_to(
        &family,
        &[
            ("/old/a.txt", Some(b"/docs/a.txt")),
            ("/docs/q/f.txt", Some(b"/p/q/f.txt")),
            ("/p/x.txt", Some(b"/old/x.txt")),
        ],
    )
    .await;
}

/// Checks a temporary move of a directory whose move goes below itself. `q`
/// takes the name `p`, and `p` moves into `q`, which is below `p`, so `p`
/// takes its temporary name in its current directory.
#[tokio::test]
async fn a_directory_that_moves_below_its_child_takes_a_temporary_name_beside_itself() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.move_path("/p", "/t", None)
        .await
        .expect("move p aside");
    fork.move_path("/t/q", "/p", None).await.expect("move q up");
    fork.move_path("/t", "/p/p", None)
        .await
        .expect("move p into q");
    let commit = merges_to(&family, &[("/p/f.txt", Some(b"/p/q/f.txt")), ("/t", None)]).await;
    assert!(family.entry(&family.main, "/p/p").await.is_some());
    assert_eq!(commit.events.len(), 3, "{:?}", commit.events);
}

/// Checks that a temporary name that an inode in the directory already has
/// takes a suffix. `main` has a file whose name is the inode ID of `a.txt`,
/// which takes the temporary name when the fork swaps `a.txt` and `b.txt`.
#[tokio::test]
async fn a_temporary_name_that_is_taken_takes_a_suffix() {
    let family = Family::new().await;
    let a_id = family.inode(&family.main, "/docs/a.txt").await;
    let taken = format!("/docs/{a_id}");
    family
        .main()
        .write_file_bytes(&taken, b"taken", None)
        .await
        .expect("a file with the temporary name");
    let fork = family.fork();
    fork.move_path("/docs/a.txt", "/docs/t.txt", None)
        .await
        .expect("move a");
    fork.move_path("/docs/b.txt", "/docs/a.txt", None)
        .await
        .expect("move b");
    fork.move_path("/docs/t.txt", "/docs/b.txt", None)
        .await
        .expect("move t");
    merges_to(
        &family,
        &[
            ("/docs/a.txt", Some(b"/docs/b.txt")),
            ("/docs/b.txt", Some(b"/docs/a.txt")),
            (taken.as_str(), Some(b"taken")),
        ],
    )
    .await;
}

/// Checks that the search for a temporary name stops after the inode ID and
/// eight suffixes. `main` has a file under each of those names, so the swap
/// of `a.txt` and `b.txt` can't take a temporary name, and the merge returns
/// `invalid_request`.
#[tokio::test]
async fn a_temporary_name_search_gives_up_after_eight_suffixes() {
    let family = Family::new().await;
    let a_id = family.inode(&family.main, "/docs/a.txt").await;
    for suffix in 0..=8 {
        let name = match suffix {
            0 => format!("/docs/{a_id}"),
            _ => format!("/docs/{a_id}-{suffix}"),
        };
        family
            .main()
            .write_file_bytes(&name, b"taken", None)
            .await
            .expect("a taken name");
    }
    let fork = family.fork();
    fork.move_path("/docs/a.txt", "/docs/t.txt", None)
        .await
        .expect("move a");
    fork.move_path("/docs/b.txt", "/docs/a.txt", None)
        .await
        .expect("move b");
    fork.move_path("/docs/t.txt", "/docs/b.txt", None)
        .await
        .expect("move t");

    let error = family.merge("no-name", None).await.expect_err("no name");
    assert_matches!(
        error,
        CoreError::InvalidMerge {
            param: "/through_seq",
            ..
        }
    );
}

/// Checks a swap of two names. Each move waits for the other, so the merge
/// first moves one file to a temporary name in the same commit, and the
/// commit shows three moves.
#[tokio::test]
async fn a_swap_of_two_names_merges_through_a_temporary_name() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.move_path("/docs/a.txt", "/docs/t.txt", None)
        .await
        .expect("move a");
    fork.move_path("/docs/b.txt", "/docs/a.txt", None)
        .await
        .expect("move b");
    fork.move_path("/docs/t.txt", "/docs/b.txt", None)
        .await
        .expect("move t");
    let commit = merges_to(
        &family,
        &[
            ("/docs/a.txt", Some(b"/docs/b.txt")),
            ("/docs/b.txt", Some(b"/docs/a.txt")),
        ],
    )
    .await;
    assert_eq!(commit.events.len(), 3, "{:?}", commit.events);
}

/// Checks `mv old tmp && mkdir old && mv tmp old/prev`. The directory that
/// the move goes into takes the moved directory's name, so the merge first
/// moves the moved directory to a temporary name in its current directory.
#[tokio::test]
async fn a_directory_moved_into_its_replacement_merges() {
    let family = Family::new().await;
    let fork = family.fork();
    fork.move_path("/old", "/tmp", None).await.expect("move");
    fork.write_file_bytes("/old/new.txt", b"new", None)
        .await
        .expect("create");
    fork.move_path("/tmp", "/old/prev", None)
        .await
        .expect("move back");
    merges_to(
        &family,
        &[
            ("/old/prev/x.txt", Some(b"/old/x.txt")),
            ("/old/new.txt", Some(b"new")),
            ("/tmp", None),
        ],
    )
    .await;
}

/// Checks that a row that an earlier merge from the fork wrote is the fork's
/// own. A second edit of a file that the first merge created and a second
/// edit of a file that the first merge edited both merge, and the file that
/// the first merge created is changed in place, not created again.
#[tokio::test]
async fn rows_that_an_earlier_merge_wrote_are_the_forks_own() {
    let family = Family::new().await;
    family
        .fork()
        .write_file_bytes("/docs/a.txt", b"first", None)
        .await
        .expect("edit");
    family
        .fork()
        .write_file_bytes("/docs/n.txt", b"first", None)
        .await
        .expect("create");
    family.merge("first", None).await.expect("first merge");

    family
        .fork()
        .write_file_bytes("/docs/a.txt", b"second", None)
        .await
        .expect("edit");
    family
        .fork()
        .write_file_bytes("/docs/n.txt", b"second", None)
        .await
        .expect("edit");
    let second = family.merge("second", None).await.expect("second merge");
    assert_eq!(
        second.events.len(),
        2,
        "two revisions and no create: {:?}",
        second.events
    );
    for path in ["/docs/a.txt", "/docs/n.txt"] {
        let theirs = family.entry(&family.main, path).await.expect("main");
        let ours = family.entry(&family.fork, path).await.expect("fork");
        assert_eq!(content(&theirs), content(&ours), "{path}");
    }
}

/// Checks that a change below a directory that an earlier merge from the
/// fork made doesn't conflict with the fork's later delete of the directory.
#[tokio::test]
async fn a_subtree_change_from_an_earlier_merge_is_the_forks_own() {
    let family = Family::new().await;
    family
        .fork()
        .write_file_bytes("/old/y.txt", b"y", None)
        .await
        .expect("create");
    family.merge("first", None).await.expect("first merge");
    family
        .fork()
        .delete_path("/old", None)
        .await
        .expect("delete");

    family.merge("second", None).await.expect("second merge");
    assert!(family.entry(&family.main, "/old").await.is_none());
}

/// Checks that a fork that undid its own change in the range has an empty
/// net change, so the merge commits its record with no operations and
/// advances the merge index.
#[tokio::test]
async fn an_empty_net_change_commits_the_record_alone() {
    let family = Family::new().await;
    family
        .set_attribute(family.fork(), "/docs/a.txt", "draft")
        .await;
    family
        .fork()
        .submit_operation(
            CommitId::generate(),
            FilesystemOperation::UpdateAttributes {
                path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                set: Default::default(),
                remove: vec![attribute_key("stage")],
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            },
            Vec::new(),
        )
        .await
        .expect("undo");
    let commit = family.merge("empty", None).await.expect("merge");
    assert!(commit.events.is_empty());
    let through = commit.merge.expect("merge record").source_through_seq;

    family
        .fork()
        .write_file_bytes("/docs/z.txt", b"z", None)
        .await
        .expect("create");
    let next = family.merge("next", None).await.expect("next merge");
    assert_eq!(next.merge.expect("merge record").source_after_seq, through);
}

/// Checks that a merge through a sequence below the fork's head applies the
/// fork's commits through that sequence, and the next merge applies the
/// rest.
#[tokio::test]
async fn a_merge_through_a_sequence_below_the_head_leaves_the_rest() {
    let family = Family::new().await;
    let first = family
        .fork()
        .write_file_bytes("/p.txt", b"p", None)
        .await
        .expect("p");
    family
        .fork()
        .write_file_bytes("/q.txt", b"q", None)
        .await
        .expect("q");

    family
        .merge("through-p", Some(first.committed_seq))
        .await
        .expect("merge through p");
    assert!(family.entry(&family.main, "/p.txt").await.is_some());
    assert!(family.entry(&family.main, "/q.txt").await.is_none());
    family.merge("rest", None).await.expect("merge the rest");
    assert!(family.entry(&family.main, "/q.txt").await.is_some());

    let error = family
        .merge("nothing", None)
        .await
        .expect_err("nothing to merge");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}

/// Checks the `invalid_request` errors for `through_seq`. A sequence ahead of
/// the fork's head, one at or below the range's start, and no sequence when
/// the fork has no commits after the range's start all return it with
/// `param` set to `/through_seq`.
#[tokio::test]
async fn a_through_seq_outside_the_forks_range_is_an_invalid_request() {
    let family = Family::new().await;
    let fork_point = resolve_path(&family.store, &family.fork, "/")
        .await
        .expect("root")
        .head_seq;
    let nothing = family.merge("nothing", None).await.expect_err("no commits");
    family
        .fork()
        .write_file_bytes("/f.txt", b"f", None)
        .await
        .expect("fork");
    let ahead = family
        .merge("ahead", Some(ChangeSeq(fork_point.0 + 5)))
        .await
        .expect_err("ahead of the head");
    let at_start = family
        .merge("at-start", Some(fork_point))
        .await
        .expect_err("at the range's start");
    for error in [nothing, ahead, at_start] {
        assert_matches!(
            error,
            CoreError::InvalidMerge {
                param: "/through_seq",
                ..
            }
        );
    }
}

/// Checks that a retry with the commit ID of a merge returns the merge, and
/// that a request under that ID with another message or another
/// `through_seq` is `commit_id_reuse_conflict`.
#[tokio::test]
async fn a_reused_commit_id_with_another_request_conflicts() {
    let family = Family::new().await;
    let first = family
        .fork()
        .write_file_bytes("/p.txt", b"p", None)
        .await
        .expect("p");
    let second = family
        .fork()
        .write_file_bytes("/q.txt", b"q", None)
        .await
        .expect("q");
    let merged = family
        .merge("merge", Some(first.committed_seq))
        .await
        .expect("merge");

    let retried = family.merge("merge", None).await.expect("a retry");
    assert_eq!(retried.committed_seq, merged.committed_seq);
    let mut message = family.request("merge", None);
    message.message = Some("another message".to_owned());
    let another_range = family.request("merge", Some(second.committed_seq));
    for request in [message, another_range] {
        let error = family
            .merge_as(&request, None)
            .await
            .expect_err("another request");
        assert_eq!(
            error.code(),
            ErrorCode::CommitIdReuseConflict,
            "{request:?}"
        );
    }
}

/// Checks that a merge that needs history below a retention floor returns
/// `rebootstrap_required`, and that the details say which floor: `main`'s
/// floor above the fork point, or the fork's own floor above the range's
/// start.
#[tokio::test]
async fn a_merge_below_either_floor_needs_a_rebootstrap() {
    let family = Family::new().await;
    let fork_point = resolve_path(&family.store, &family.main, "/")
        .await
        .expect("root")
        .head_seq;
    family
        .fork()
        .write_file_bytes("/f.txt", b"f", None)
        .await
        .expect("fork");
    family
        .main()
        .write_file_bytes("/m.txt", b"m", None)
        .await
        .expect("main");
    let floor = family.advance_floor(&family.main).await;
    assert!(floor > fork_point);
    let error = family
        .merge("main-floor", None)
        .await
        .expect_err("main's floor");
    assert_eq!(error.code(), ErrorCode::RebootstrapRequired);
    let details = error.details().expect("details");
    assert_eq!(details.fork_point_seq, Some(fork_point));
    assert_eq!(details.retention_floor_seq, Some(floor));

    let family = Family::new().await;
    let fork_point = resolve_path(&family.store, &family.main, "/")
        .await
        .expect("root")
        .head_seq;
    for path in ["/f.txt", "/g.txt"] {
        family
            .fork()
            .write_file_bytes(path, b"f", None)
            .await
            .expect("fork");
    }
    let floor = family.advance_floor(&family.fork).await;
    assert!(floor > fork_point);
    let error = family
        .merge("fork-floor", None)
        .await
        .expect_err("fork's floor");
    assert_eq!(error.code(), ErrorCode::RebootstrapRequired);
    let details = error.details().expect("details");
    assert_eq!(details.source_namespace_id, Some(family.fork.clone()));
    assert_eq!(details.actual_after_seq, Some(fork_point));
    assert_eq!(details.retention_floor_seq, Some(floor));
    assert_eq!(details.fork_point_seq, None);
}

/// Checks that a namespace that isn't a fork of `main` can't be merged into
/// it, and that the request fails before anything is read from the fork.
#[tokio::test]
async fn a_namespace_that_isnt_a_fork_of_main_is_refused() {
    let family = Family::new().await;
    let other = namespace_id("other");
    NamespaceFixture::new(&family.store, &other, &family.writer)
        .create()
        .await
        .expect("create other");
    let request = MergeRequest {
        source_namespace_id: other.clone(),
        through_seq: None,
        after_seq: None,
        commit_id: CommitId::generate(),
        actor_id: loonfs_test_support::test_actor(),
        message: None,
    };
    let main = namespace_engine(&family.store, &family.main, &family.writer);
    let error = main
        .plan_merge(
            &read_context(&family.store, &family.main).await,
            &namespace_engine(&family.store, &other, &family.writer),
            &read_context(&family.store, &other).await,
            &request,
            family.writer.now_ms,
            family.allowance,
        )
        .await
        .expect_err("not a fork");
    assert_matches!(
        error,
        CoreError::InvalidMerge {
            param: "/source_namespace_id",
            ..
        }
    );
}
