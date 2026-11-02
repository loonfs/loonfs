//! The diff's rules, checked by name against histories written through
//! the commit engine.

use super::*;
use crate::commit::engine::CommitCandidate;
use crate::path::write::{CommitIntent, FilesystemOperation};
use crate::test_support::{namespace_store, writer_at, NamespaceFixture};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_types::{
    AccessGrants, AccessRight, AccessRights, AttributeKey, AttributeValue, Commit, CommitId,
    EffectiveLimit, EntryInodeKind, FilesystemChange, NamespaceAccess, PrincipalId, PrincipalScope,
    PrincipalSet, Subject, SubjectId,
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
        let options = DiffOptions {
            to_seq,
            after_seq,
            limit: EffectiveLimit::new(NonZeroU32::new(limit).expect("a nonzero limit")),
            ..DiffOptions::default()
        };
        diff_page(&view, from_seq, &options).await
    }

    /// Reads every page of the diff from `from_seq` to the head under
    /// `path`, `limit` commits at a time, and returns their entries.
    async fn diff(&self, from_seq: ChangeSeq, path: &str, limit: u32) -> Vec<DiffEntry> {
        let namespace = namespace_store(&self.store, &self.namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("load the current view");
        let mut options = DiffOptions {
            path: AbsolutePath::parse(path).expect("path"),
            limit: EffectiveLimit::new(NonZeroU32::new(limit).expect("a nonzero limit")),
            ..DiffOptions::default()
        };
        let mut entries = Vec::new();
        loop {
            let page = diff_page(&view, from_seq, &options)
                .await
                .expect("read a page")
                .response;
            assert_eq!(page.to_seq, options.to_seq.unwrap_or(view.head().seq));
            options.to_seq = Some(page.to_seq);
            entries.extend(page.entries);
            let Some(next) = page.next_after_seq else {
                return entries;
            };
            options.after_seq = Some(next);
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
            content: loonfs_types::FileContent::Ref {
                content_ref: content_ref.expect("put content"),
                source_namespace_id: None,
            },
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

/// Checks that a diff of a moved directory's path, or of a path below it
/// before the move or after it, reports the move.
#[tokio::test]
async fn a_move_of_a_directory_above_the_path_is_an_entry() {
    let history = History::unrestricted().await;
    history.write("/a/b/file.txt").await;
    let from_seq = history.write("/other/kept.txt").await.committed_seq;
    history.move_path("/a", "/x").await;
    let directory = history.inode_id("/x").await;

    for path in ["/a/b", "/x/b", "/X/B", "/a/b/file.txt", "/a", "/x"] {
        let entries = history.diff(from_seq, path, 1000).await;
        let summaries: Vec<_> = entries.iter().map(summary).collect();
        assert_eq!(
            summaries,
            [(DiffEntryKind::Moved, Some("/x"), Some("/a"))],
            "under {path}"
        );
        assert_eq!(entries[0].inode_id, directory, "under {path}");
    }
    assert!(history.diff(from_seq, "/other", 1000).await.is_empty());
}

/// Checks that a diff of a path below a directory reports the directory's
/// deletion, and its undelete in place.
#[tokio::test]
async fn a_deletion_or_undelete_of_a_directory_above_the_path_is_an_entry() {
    let history = History::unrestricted().await;
    let written = history.write("/a/b/file.txt").await.committed_seq;
    let directory = history.inode_id("/a").await;
    let deletion = history.delete("/a").await;
    let deleted = history.diff(written, "/a/b", 1000).await;
    let summaries: Vec<_> = deleted.iter().map(summary).collect();
    assert_eq!(summaries, [(DiffEntryKind::Deleted, None, Some("/a"))]);
    assert_eq!(deleted[0].inode_id, directory);

    history
        .commit(FilesystemOperation::Undelete {
            inode_id: directory,
            deletion_seq: deletion.committed_seq,
            destination_path: None,
            destination_parent_inode_id: None,
            destination_display_name: None,
        })
        .await;
    let undeleted = history.diff(deletion.committed_seq, "/a/b", 1000).await;
    let summaries: Vec<_> = undeleted.iter().map(summary).collect();
    assert_eq!(summaries, [(DiffEntryKind::Undeleted, Some("/a"), None)]);
    assert_eq!(undeleted[0].inode_id, directory);
}

/// Checks that a directory created above the path isn't an entry, because
/// each inode that the directory contains under the path is an entry of its
/// own.
#[tokio::test]
async fn a_directory_created_above_the_path_is_not_an_entry() {
    let history = History::unrestricted().await;
    let from_seq = history.write("/kept.txt").await.committed_seq;
    history.write("/a/b/f.txt").await;

    let entries = history.diff(from_seq, "/a/b", 1000).await;
    let summaries: Vec<_> = entries.iter().map(summary).collect();
    assert_eq!(
        summaries,
        [
            (DiffEntryKind::Created, Some("/a/b"), None),
            (DiffEntryKind::Created, Some("/a/b/f.txt"), None),
        ]
    );
}

/// Checks that a directory above the path is one entry when the diff reads
/// the directory's two moves on different pages.
#[tokio::test]
async fn a_directory_above_the_path_touched_on_two_pages_is_one_entry() {
    let history = History::unrestricted().await;
    let from_seq = history.write("/a/b/file.txt").await.committed_seq;
    history.move_path("/a", "/y").await;
    history.write("/other.txt").await;
    history.move_path("/y", "/x").await;

    for limit in [1, 1000] {
        let entries = history.diff(from_seq, "/a/b", limit).await;
        let summaries: Vec<_> = entries.iter().map(summary).collect();
        assert_eq!(
            summaries,
            [(DiffEntryKind::Moved, Some("/x"), Some("/a"))],
            "limit {limit}"
        );
    }
}

/// Checks that a directory whose attribute and access revisions changed is a
/// `changed` entry under its own path, and not an entry under a path below
/// it.
#[tokio::test]
async fn an_attribute_or_access_change_of_a_directory_above_the_path_is_not_an_entry() {
    let history = History::new(NamespaceAccess::Acl {
        principal_scope: PrincipalScope::parse("org_demo").expect("principal scope"),
        root_grants: AccessGrants::new(BTreeMap::from([(
            PrincipalId::parse("prn_root").expect("principal id"),
            AccessRights::ADMIN,
        )]))
        .expect("root grants"),
    })
    .await;
    let from_seq = administer_put(&history, "/a/b/file.txt")
        .await
        .committed_seq;
    let path = |path: &str| AbsolutePath::parse(path).expect("path");
    administer(
        &history,
        |_| FilesystemOperation::UpdateAttributes {
            path: path("/a"),
            set: BTreeMap::from([(
                AttributeKey::parse("owner").expect("attribute key"),
                AttributeValue::parse("reader").expect("attribute value"),
            )]),
            remove: Vec::new(),
            expected_inode_id: None,
            expected_attributes_revision_no: None,
        },
        None,
    )
    .await;
    administer(
        &history,
        |_| FilesystemOperation::UpdateAccess {
            path: path("/a"),
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

    assert!(history.diff(from_seq, "/a/b", 1000).await.is_empty());
    let entries = history.diff(from_seq, "/a", 1000).await;
    let summaries: Vec<_> = entries.iter().map(summary).collect();
    assert_eq!(summaries, [(DiffEntryKind::Changed, Some("/a"), None)]);
    assert_eq!(
        (
            entries[0].attributes_revision_no,
            entries[0].access_revision_no
        ),
        (Some(AttributesRevisionNo(1)), Some(AccessRevisionNo(1)))
    );
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
    // Each commit touches two files, so the second commit takes the page from
    // two touches to four, past the bound of three, and stays whole.
    let first = diff_page_within(&view, from_seq, &DiffOptions::default(), 3)
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
        &DiffOptions {
            to_seq: Some(first.response.to_seq),
            after_seq: first.response.next_after_seq,
            ..DiffOptions::default()
        },
        3,
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

/// Reads every page of a diff from `from_seq` with `options`, with
/// `max_touches` as the page's bound, and returns the pages.
async fn diff_pages(
    history: &History,
    from_seq: ChangeSeq,
    mut options: DiffOptions,
    max_touches: usize,
) -> Vec<DiffResponse> {
    let namespace = namespace_store(&history.store, &history.namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("load the current view");
    let mut pages = Vec::new();
    loop {
        let page = diff_page_within(&view, from_seq, &options, max_touches)
            .await
            .expect("read a page")
            .response;
        options.to_seq = Some(page.to_seq);
        options.after_seq = page.next_after_seq;
        options.event_index = page.next_event_index.unwrap_or(0);
        let more = page.next_after_seq.is_some();
        pages.push(page);
        if !more {
            return pages;
        }
    }
}

/// Checks that with `split_commits` a commit that touches more inodes than
/// one page examines is read across pages, that a page that ends inside the
/// commit reports the next event's index, and that the entries keyed by
/// inode ID, with the later entry kept, equal the diff read without
/// splitting. An inode that the commit's events touch on two pages is
/// reported on both.
#[tokio::test]
async fn a_commit_over_the_touch_bound_is_read_across_pages_with_split_commits() {
    let history = History::unrestricted().await;
    let from_seq = history.write("/kept.txt").await.committed_seq;
    let mut operations: Vec<_> = (0..40)
        .map(|index| FilesystemOperation::CreateDirectoryByInode {
            parent_inode_id: loonfs_types::ROOT_INODE_ID,
            display_name: loonfs_types::DisplayName::parse(format!("large-{index}")).expect("name"),
        })
        .collect();
    operations.push(FilesystemOperation::UpdateAttributes {
        path: AbsolutePath::parse("/large-0").expect("path"),
        set: BTreeMap::from([(
            AttributeKey::parse("touched").expect("key"),
            AttributeValue::parse("twice").expect("value"),
        )]),
        remove: Vec::new(),
        expected_inode_id: None,
        expected_attributes_revision_no: None,
    });
    let large = history
        .fixture()
        .publish_candidates(vec![CommitCandidate::new(CommitIntent {
            commit_id: CommitId::parse("large").expect("commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            operations,
            preconditions: Vec::new(),
        })])
        .await
        .pop()
        .expect("one result")
        .expect("publish the large commit");
    history.write("/after.txt").await;

    let split = diff_pages(
        &history,
        from_seq,
        DiffOptions {
            commit_splitting: CommitSplitting::AtPageBudget,
            ..DiffOptions::default()
        },
        10,
    )
    .await;
    let whole = diff_pages(&history, from_seq, DiffOptions::default(), 10).await;
    assert_eq!(
        whole.len(),
        2,
        "the large commit is one page without splitting"
    );
    assert!(split.len() > 4, "{} pages", split.len());
    for page in &split[1..split.len() - 1] {
        assert_eq!(page.next_after_seq, Some(large.committed_seq));
        assert!(page.next_event_index.is_some());
    }
    let keyed = |pages: &[DiffResponse]| {
        pages
            .iter()
            .flat_map(|page| page.entries.iter())
            .map(|entry| (entry.inode_id, entry.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(keyed(&split), keyed(&whole));
    let first_directory = history.inode_id("/large-0").await;
    let pages_with_it = split
        .iter()
        .filter(|page| {
            page.entries
                .iter()
                .any(|entry| entry.inode_id == first_directory)
        })
        .count();
    assert_eq!(pages_with_it, 2);
}

/// Checks that a diff refuses a position inside a commit without
/// `split_commits`, and one at `from_seq`, which is no commit of the range.
#[tokio::test]
async fn a_diff_refuses_an_event_index_without_split_commits_or_at_from_seq() {
    let history = History::unrestricted().await;
    let from_seq = history.write("/a.txt").await.committed_seq;
    let after = history.write("/b.txt").await.committed_seq;
    let namespace = namespace_store(&history.store, &history.namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("load the current view");
    for (after_seq, commit_splitting) in [
        (after, CommitSplitting::Whole),
        (from_seq, CommitSplitting::AtPageBudget),
    ] {
        let options = DiffOptions {
            after_seq: Some(after_seq),
            event_index: 1,
            commit_splitting,
            ..DiffOptions::default()
        };
        let error = diff_page(&view, from_seq, &options)
            .await
            .expect_err("an invalid position");
        assert_matches!(error, CoreError::InvalidEventIndex(_));
    }
}
