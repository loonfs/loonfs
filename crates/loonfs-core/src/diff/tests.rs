//! The diff's rules, checked by name against histories written through
//! the commit engine.

use super::*;
use crate::commit::engine::CommitCandidate;
use crate::path::write::{CommitIntent, FilesystemOperation};
use crate::test_support::{namespace_store, writer_at, NamespaceFixture};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_types::{
    AccessGrants, AccessRight, AccessRights, Commit, CommitId, EntryInodeKind, FilesystemChange,
    NamespaceAccess, PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use std::assert_matches;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use tempfile::TempDir;

/// The time of every commit in these tests. A retention advance at one
/// grace window later can pass every commit.
const COMMITTED_AT_MS: u64 = 1_000;

/// A namespace that a test writes history into and then diffs.
struct History {
    _directory: TempDir,
    store: LocalFsStore,
    namespace_id: NamespaceId,
}

impl History {
    async fn new(access: NamespaceAccess) -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let history = Self {
            _directory: directory,
            store,
            namespace_id: NamespaceId::parse("diff").expect("namespace id"),
        };
        history
            .fixture()
            .create_with_options(&crate::options::CreateNamespaceOptions {
                access,
                ..Default::default()
            })
            .await
            .expect("create the namespace");
        history
    }

    async fn unrestricted() -> Self {
        Self::new(NamespaceAccess::Unrestricted {}).await
    }

    fn fixture(&self) -> NamespaceFixture<&LocalFsStore> {
        NamespaceFixture::new(
            &self.store,
            &self.namespace_id,
            &writer_at("writer", COMMITTED_AT_MS),
        )
    }

    async fn write(&self, path: &str) -> Commit {
        self.fixture()
            .write_file_bytes(path, path.as_bytes(), None)
            .await
            .expect("write the file")
    }

    async fn append(&self, path: &str) -> Commit {
        self.commit(FilesystemOperation::AppendFile {
            path: AbsolutePath::parse(path).expect("path"),
            inline_content: b"more\n".to_vec(),
            expected_inode_id: None,
            expected_revision_no: None,
        })
        .await
    }

    async fn delete(&self, path: &str) -> Commit {
        self.fixture()
            .delete_path(path, None)
            .await
            .expect("delete the path")
    }

    async fn move_path(&self, source_path: &str, destination_path: &str) -> Commit {
        self.fixture()
            .move_path(source_path, destination_path, None)
            .await
            .expect("move the path")
    }

    async fn commit(&self, operation: FilesystemOperation) -> Commit {
        self.fixture()
            .submit_operation(CommitId::generate(), operation, Vec::new())
            .await
            .expect("commit the operation")
    }

    async fn fold(&self) {
        namespace_store(&self.store, &self.namespace_id)
            .fold_wal()
            .await
            .expect("fold the WAL tail");
    }

    async fn inode_id(&self, path: &str) -> InodeId {
        crate::test_support::visible_inode_id(&self.store, &self.namespace_id, path).await
    }

    /// Reads one page against the namespace's current head.
    async fn page(
        &self,
        from_seq: ChangeSeq,
        to_seq: Option<ChangeSeq>,
        after_seq: Option<ChangeSeq>,
        limit: u32,
    ) -> Result<DiffPage> {
        let namespace = namespace_store(&self.store, &self.namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("load the current view");
        let limit = EffectiveLimit::new(NonZeroU32::new(limit).expect("a nonzero limit"));
        diff_page(
            &view,
            from_seq,
            to_seq,
            after_seq,
            limit,
            &AbsolutePath::root(),
        )
        .await
    }

    /// Reads every page of the diff from `from_seq` to the head under
    /// `path`, `limit` commits at a time, and returns their entries.
    async fn diff(&self, from_seq: ChangeSeq, path: &str, limit: u32) -> Vec<DiffEntry> {
        let namespace = namespace_store(&self.store, &self.namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("load the current view");
        let path = AbsolutePath::parse(path).expect("path");
        let limit = EffectiveLimit::new(NonZeroU32::new(limit).expect("a nonzero limit"));
        let (mut to_seq, mut after_seq, mut entries) = (None, None, Vec::new());
        loop {
            let page = diff_page(&view, from_seq, to_seq, after_seq, limit, &path)
                .await
                .expect("read a page")
                .response;
            assert_eq!(page.to_seq, to_seq.unwrap_or(view.head().seq));
            to_seq = Some(page.to_seq);
            entries.extend(page.entries);
            let Some(next) = page.next_after_seq else {
                return entries;
            };
            after_seq = Some(next);
        }
    }
}

/// The fields of an entry that say what changed, with paths as text.
fn summary(entry: &DiffEntry) -> (DiffEntryKind, Option<&str>, Option<&str>) {
    (
        entry.kind,
        entry.path.as_deref(),
        entry.previous_path.as_deref(),
    )
}

#[tokio::test]
async fn a_file_appended_by_every_commit_yields_one_entry_at_its_last_touch() {
    let history = History::unrestricted().await;
    let from_seq = history.write("/log.txt").await.committed_seq;
    for number in 0..6 {
        // The range's first half is folded, so the view at `from_seq`
        // is below the manifest's head.
        if number == 3 {
            history.fold().await;
        }
        history.append("/log.txt").await;
    }

    for limit in [1, 2, 1000] {
        let entries = history.diff(from_seq, "/", limit).await;
        assert_eq!(entries.len(), 1, "limit {limit}");
        let entry = &entries[0];
        assert_eq!(entry.inode_id, history.inode_id("/log.txt").await);
        assert_eq!(
            summary(entry),
            (DiffEntryKind::Changed, Some("/log.txt"), None)
        );
        assert_eq!(entry.revision_no, Some(RevisionNo(7)));
        assert_eq!(
            (entry.attributes_revision_no, entry.access_revision_no),
            (None, None)
        );
    }
}

#[tokio::test]
async fn a_one_commit_file_creation_yields_one_entry() {
    let history = History::unrestricted().await;
    history.write("/docs/old.txt").await;
    history.fold().await;
    let from_seq = history.write("/docs/kept.txt").await.committed_seq;
    let created = history.write("/docs/new.txt").await;

    let entries = history.diff(from_seq, "/", 1000).await;
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(
        summary(entry),
        (DiffEntryKind::Created, Some("/docs/new.txt"), None)
    );
    assert_eq!(entry.inode_kind, EntryInodeKind::File);
    assert_eq!(entry.revision_no, Some(RevisionNo(1)));
    let Some(FilesystemChange::FileCreated {
        inode_id,
        binding_version,
        ..
    }) = created.events.first()
    else {
        panic!("expected a file creation, got {:?}", created.events);
    };
    assert_eq!(entry.inode_id, *inode_id);
    assert_eq!(entry.binding_version.as_ref(), Some(binding_version));
}

fn administrator() -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("principal scope"),
        subject_id: SubjectId::parse("root").expect("subject id"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse("prn_root").expect("principal id")
        ]))
        .expect("principals"),
    }
}

/// Commits `operation` as the administrator of an access-controlled
/// namespace, with `bytes` as the content of a put.
async fn administer(
    history: &History,
    operation: impl FnOnce(Option<loonfs_types::ContentRef>) -> FilesystemOperation,
    bytes: Option<&[u8]>,
) -> Commit {
    let namespace = namespace_store(&history.store, &history.namespace_id);
    let prepared = match bytes {
        Some(bytes) => {
            let stored = namespace
                .store_bytes_as_content(bytes)
                .await
                .expect("store content");
            let catalog = namespace.load_catalog_entry().await.expect("catalog");
            vec![crate::storage::content::prepare_stored_content(
                &catalog, stored,
            )]
        }
        None => Vec::new(),
    };
    let content_ref = prepared
        .first()
        .map(|prepared| prepared.content_ref().clone());
    let intent = CommitIntent::single(
        CommitId::generate(),
        loonfs_test_support::test_actor(),
        None,
        operation(content_ref),
    )
    .with_subject(administrator());
    let candidate = if prepared.is_empty() {
        CommitCandidate::new(intent)
    } else {
        CommitCandidate::prepared(intent, prepared)
    };
    history
        .fixture()
        .publish_candidates(vec![candidate])
        .await
        .pop()
        .expect("one result")
        .expect("commit as the administrator")
}

async fn administer_put(history: &History, path: &str) -> Commit {
    let path = AbsolutePath::parse(path).expect("path");
    administer(
        history,
        |content_ref| FilesystemOperation::PutFile {
            path,
            content: loonfs_types::FileContent::Ref(content_ref.expect("put content")),
            behavior: loonfs_types::DestinationBehavior::Replace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
        Some(b"bytes"),
    )
    .await
}

/// Checks that a later move, deletion, or access change of a file's
/// directory doesn't hide the file's own last touch.
#[tokio::test]
async fn an_ancestor_changed_after_a_files_last_touch_leaves_the_files_entry() {
    let history = History::new(NamespaceAccess::Acl {
        principal_scope: PrincipalScope::parse("org_demo").expect("principal scope"),
        root_grants: AccessGrants::new(BTreeMap::from([(
            PrincipalId::parse("prn_root").expect("principal id"),
            AccessRights::ADMIN,
        )]))
        .expect("root grants"),
    })
    .await;
    for path in ["/moved/a.txt", "/deleted/b.txt", "/shared/c.txt"] {
        administer_put(&history, path).await;
    }
    let from_seq = administer_put(&history, "/kept.txt").await.committed_seq;
    for path in ["/moved/a.txt", "/deleted/b.txt", "/shared/c.txt"] {
        administer_put(&history, path).await;
    }
    let path = |path: &str| AbsolutePath::parse(path).expect("path");
    administer(
        &history,
        |_| FilesystemOperation::MovePath {
            source_path: path("/moved"),
            destination_path: path("/renamed"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: loonfs_types::DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        None,
    )
    .await;
    administer(
        &history,
        |_| FilesystemOperation::DeletePath {
            path: path("/deleted"),
            behavior: loonfs_types::DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        },
        None,
    )
    .await;
    administer(
        &history,
        |_| FilesystemOperation::UpdateAccess {
            path: path("/shared"),
            boundary: false,
            grants: AccessGrants::new(BTreeMap::from([(
                PrincipalId::parse("prn_reader").expect("principal id"),
                AccessRight::Read.into(),
            )]))
            .expect("grants"),
            expected_inode_id: None,
            expected_access_revision_no: None,
        },
        None,
    )
    .await;

    let entries = history.diff(from_seq, "/", 1000).await;
    let summaries: Vec<_> = entries.iter().map(summary).collect();
    assert_eq!(
        summaries,
        [
            (DiffEntryKind::Changed, Some("/renamed/a.txt"), None),
            (DiffEntryKind::Deleted, None, Some("/deleted/b.txt")),
            (DiffEntryKind::Changed, Some("/shared/c.txt"), None),
            (DiffEntryKind::Moved, Some("/renamed"), Some("/moved")),
            (DiffEntryKind::Deleted, None, Some("/deleted")),
            (DiffEntryKind::Changed, Some("/shared"), None),
        ]
    );
    for file in &entries[..3] {
        assert_eq!(
            file.revision_no.is_some(),
            file.kind == DiffEntryKind::Changed
        );
    }
    assert_eq!(entries[5].access_revision_no, Some(AccessRevisionNo(1)));
}

#[tokio::test]
async fn a_floor_advance_between_two_pages_fails_the_later_page() {
    let history = History::unrestricted().await;
    let from_seq = history.write("/a.txt").await.committed_seq;
    history.write("/b.txt").await;
    history.write("/c.txt").await;
    let first = history
        .page(from_seq, None, None, 1)
        .await
        .expect("the first page")
        .response;
    let after_seq = first.next_after_seq.expect("a second page");

    history.fold().await;
    let advanced = NamespaceFixture::new(
        &history.store,
        &history.namespace_id,
        &writer_at(
            "maintainer",
            crate::test_support::grace_elapsed_after(COMMITTED_AT_MS),
        ),
    )
    .engine()
    .advance_retention_floor(crate::manifest::RetentionTarget::Head)
    .await
    .expect("advance the floor")
    .response
    .retention_floor_seq;
    assert!(advanced > from_seq);

    let error = history
        .page(from_seq, Some(first.to_seq), Some(after_seq), 1)
        .await
        .expect_err("the floor passed the diff's start");
    assert_matches!(
        error,
        CoreError::DiffRebootstrapRequired {
            from_seq: failed,
            retention_floor_seq,
        } if failed == from_seq && retention_floor_seq == advanced
    );
    let details = error.details().expect("details");
    assert_eq!(
        (details.from_seq, details.after_seq),
        (Some(from_seq), None)
    );
}

#[tokio::test]
async fn an_undeleted_directory_is_one_entry_and_its_children_are_not() {
    let history = History::unrestricted().await;
    history.write("/dir/x.txt").await;
    history.write("/dir/y.txt").await;
    let directory = history.inode_id("/dir").await;
    let deletion = history.delete("/dir").await;
    history.fold().await;
    history
        .commit(FilesystemOperation::Undelete {
            inode_id: directory,
            deletion_seq: deletion.committed_seq,
            destination_path: None,
            destination_parent_inode_id: None,
            destination_display_name: None,
        })
        .await;

    let entries = history.diff(deletion.committed_seq, "/", 1000).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].inode_id, directory);
    assert_eq!(entries[0].inode_kind, EntryInodeKind::Directory);
    assert_eq!(
        summary(&entries[0]),
        (DiffEntryKind::Undeleted, Some("/dir"), None)
    );
}

#[tokio::test]
async fn a_subtree_moved_in_from_outside_the_path_appears_once_as_moved() {
    let history = History::unrestricted().await;
    history.write("/outside/sub/f.txt").await;
    let from_seq = history.write("/inside/kept.txt").await.committed_seq;
    history.move_path("/outside/sub", "/inside/sub").await;

    let moved = (
        DiffEntryKind::Moved,
        Some("/inside/sub"),
        Some("/outside/sub"),
    );
    for path in ["/inside", "/outside", "/INSIDE/sub"] {
        let entries = history.diff(from_seq, path, 1000).await;
        let summaries: Vec<_> = entries.iter().map(summary).collect();
        assert_eq!(summaries, [moved], "under {path}");
    }
    assert!(history
        .diff(from_seq, "/inside/kept.txt", 1000)
        .await
        .is_empty());
}

/// Checks that a page ends after the commit that brings its touches to the
/// bound, without splitting that commit, and that the next page starts with
/// the commit after it.
#[tokio::test]
async fn a_page_ends_after_the_commit_that_reaches_the_touch_bound() {
    let history = History::unrestricted().await;
    let from_seq = history.write("/kept.txt").await.committed_seq;
    let mut commits = Vec::new();
    for number in 0..3 {
        let paths = [format!("/{number}-a.txt"), format!("/{number}-b.txt")];
        commits.push(
            history
                .fixture()
                .write_files_bytes(&paths, b"bytes")
                .await
                .expect("write two files in one commit"),
        );
    }
    let namespace = namespace_store(&history.store, &history.namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("load the current view");
    let limit = EffectiveLimit::new(NonZeroU32::new(1000).expect("a nonzero limit"));
    let root = AbsolutePath::root();

    // Each commit touches two files, so the second commit takes the page from
    // two touches to four, past the bound of three, and stays whole.
    let first = diff_page_within(&view, from_seq, None, None, limit, 3, &root)
        .await
        .expect("the first page");
    assert_eq!((first.commits_scanned, first.touches_examined), (2, 4));
    assert_eq!(first.response.entries.len(), 4);
    assert_eq!(
        first.response.next_after_seq,
        Some(commits[1].committed_seq)
    );

    let second = diff_page_within(
        &view,
        from_seq,
        Some(first.response.to_seq),
        first.response.next_after_seq,
        limit,
        3,
        &root,
    )
    .await
    .expect("the second page");
    let summaries: Vec<_> = second.response.entries.iter().map(summary).collect();
    assert_eq!(
        summaries,
        [
            (DiffEntryKind::Created, Some("/2-a.txt"), None),
            (DiffEntryKind::Created, Some("/2-b.txt"), None),
        ]
    );
    assert_eq!(second.response.next_after_seq, None);
}

#[tokio::test]
async fn a_create_followed_by_a_delete_in_the_range_yields_nothing() {
    let history = History::unrestricted().await;
    let from_seq = history.write("/kept.txt").await.committed_seq;
    history.write("/scratch.txt").await;
    history.delete("/scratch.txt").await;

    for limit in [1, 1000] {
        assert!(history.diff(from_seq, "/", limit).await.is_empty());
    }
}

/// Checks that an entry is the same whichever page reads the last touch
/// of its inode, over a history whose first half is folded.
#[tokio::test]
async fn an_inode_classifies_the_same_way_whichever_page_its_last_touch_is_on() {
    let history = History::unrestricted().await;
    history.write("/a/one.txt").await;
    history.write("/a/two.txt").await;
    history.write("/b/three.txt").await;
    let from_seq = history.write("/b/four.txt").await.committed_seq;
    history.append("/a/one.txt").await;
    history.move_path("/a/two.txt", "/b/two.txt").await;
    history.fold().await;
    history.write("/c/five.txt").await;
    history.delete("/b/three.txt").await;
    history.append("/a/one.txt").await;
    history.move_path("/b", "/d").await;

    let expected = history.diff(from_seq, "/", 1000).await;
    let summaries: Vec<_> = expected.iter().map(summary).collect();
    assert_eq!(
        summaries,
        [
            (DiffEntryKind::Moved, Some("/d/two.txt"), Some("/a/two.txt")),
            (DiffEntryKind::Created, Some("/c"), None),
            (DiffEntryKind::Created, Some("/c/five.txt"), None),
            (DiffEntryKind::Deleted, None, Some("/b/three.txt")),
            (DiffEntryKind::Changed, Some("/a/one.txt"), None),
            (DiffEntryKind::Moved, Some("/d"), Some("/b")),
        ]
    );
    for limit in [1, 2, 3] {
        assert_eq!(
            history.diff(from_seq, "/", limit).await,
            expected,
            "limit {limit}"
        );
    }
}

#[tokio::test]
async fn a_range_out_of_bounds_is_rejected_and_an_empty_range_is_an_empty_page() {
    let history = History::unrestricted().await;
    let first = history.write("/a.txt").await.committed_seq;
    let head = history.write("/b.txt").await.committed_seq;

    for (to_seq, after_seq) in [(Some(first), None), (None, Some(head))] {
        let from_seq = to_seq.unwrap_or(first);
        let page = history
            .page(from_seq, to_seq, after_seq, 1000)
            .await
            .expect("an empty range")
            .response;
        assert!(page.entries.is_empty());
        assert_eq!(page.next_after_seq, None);
    }
    for (from_seq, to_seq, after_seq, param) in [
        (first, Some(ChangeSeq(head.0 + 1)), None, "to_seq"),
        (head, Some(first), None, "from_seq"),
        (ChangeSeq(head.0 + 1), None, None, "from_seq"),
        (head, None, Some(first), "after_seq"),
        (first, Some(first), Some(head), "after_seq"),
    ] {
        let error = history
            .page(from_seq, to_seq, after_seq, 1000)
            .await
            .expect_err("a sequence out of bounds");
        assert_matches!(
            error,
            CoreError::InvalidDiffRange { param: failed, .. } if failed == param,
            "{from_seq} {to_seq:?} {after_seq:?}"
        );
        assert_eq!(error.code(), loonfs_types::ErrorCode::InvalidRequest);
    }
}
