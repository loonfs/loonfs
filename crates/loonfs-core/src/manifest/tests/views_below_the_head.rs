//! Lookups through a view whose sequence is below its manifest's head, which
//! return the state at the view's sequence.

use super::*;
use crate::metadata::MetadataView;
use loonfs_types::Commit;
use std::assert_matches;
use tempfile::TempDir;

/// A namespace whose current manifest includes three commits made after its
/// first manifest.
struct History {
    _directory: TempDir,
    store: LocalFsStore,
    namespace_id: NamespaceId,
    /// The commit that creates `/first.txt` at revision 1.
    created: Commit,
    /// The commit that writes revision 2 of `/first.txt`.
    rewritten: Commit,
    /// The commit that creates `/later.txt`, at the manifest's head.
    later: Commit,
    first_inode: InodeId,
    later_inode: InodeId,
}

impl History {
    /// Loads the current manifest's segments.
    async fn segments(&self) -> scan::VerifiedMetadataSegments<'_, LocalFsStore> {
        let manifest_no = current_manifest_number(&self.store, &self.namespace_id).await;
        let segments = load_manifest_segments_for_inspection(
            &self.store,
            None,
            &self.namespace_id,
            &manifest_no,
        )
        .await
        .expect("load the manifest's segments");
        assert_eq!(
            segments.manifest().payload().head_seq,
            self.later.committed_seq,
            "the manifest should include every commit"
        );
        segments
    }
}

async fn history() -> History {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("views-below-the-head").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let fixture = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
    let created = fixture
        .write_file_bytes("/first.txt", b"one\n", None)
        .await
        .expect("create /first.txt");
    let rewritten = fixture
        .write_file_bytes("/first.txt", b"two\n", None)
        .await
        .expect("rewrite /first.txt");
    let later = fixture
        .write_file_bytes("/later.txt", b"later\n", None)
        .await
        .expect("create /later.txt");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the three commits");
    let first_inode =
        crate::test_support::visible_inode_id(&store, &namespace_id, "/first.txt").await;
    let later_inode =
        crate::test_support::visible_inode_id(&store, &namespace_id, "/later.txt").await;
    History {
        _directory: directory,
        store,
        namespace_id,
        created,
        rewritten,
        later,
        first_inode,
        later_inode,
    }
}

#[tokio::test]
async fn a_view_below_the_head_returns_the_revision_that_was_latest_at_its_sequence() {
    let history = history().await;
    let segments = history.segments().await;
    for (visible_seq, revision_no, committed_seq) in [
        (
            history.created.committed_seq,
            RevisionNo(1),
            history.created.committed_seq,
        ),
        (
            history.later.committed_seq,
            RevisionNo(2),
            history.rewritten.committed_seq,
        ),
    ] {
        let view = MetadataView::over_manifest_segments(&segments, visible_seq);
        let revision = view
            .latest_revision_head(history.first_inode)
            .await
            .expect("revision head")
            .expect("the file exists at the view's sequence");
        assert_eq!(
            (revision.revision_no, revision.committed_seq),
            (revision_no, committed_seq),
            "at {visible_seq:?}"
        );
    }
}

#[tokio::test]
async fn a_view_below_the_head_finds_no_inode_created_after_its_sequence() {
    let history = history().await;
    let segments = history.segments().await;
    let below = MetadataView::over_manifest_segments(&segments, history.rewritten.committed_seq);
    let first = below
        .inode_at_seq(history.first_inode)
        .await
        .expect("inode lookup")
        .expect("the first file exists before the view's sequence");
    assert_eq!(first.committed_seq, history.created.committed_seq);
    assert_eq!(
        below
            .inode_at_seq(history.later_inode)
            .await
            .expect("inode lookup"),
        None
    );

    let head = MetadataView::over_manifest_segments(&segments, history.later.committed_seq);
    let later = head
        .inode_at_seq(history.later_inode)
        .await
        .expect("inode lookup")
        .expect("the later file exists at the head");
    assert_eq!(later.committed_seq, history.later.committed_seq);
}

#[tokio::test]
async fn a_view_below_the_head_finds_no_revision_committed_after_its_sequence() {
    let history = history().await;
    let segments = history.segments().await;
    let below = MetadataView::over_manifest_segments(&segments, history.created.committed_seq);
    let first = below
        .revision_at_head(history.first_inode, RevisionNo(1))
        .await
        .expect("revision lookup")
        .expect("revision 1 exists at the view's sequence");
    assert_eq!(first.committed_seq, history.created.committed_seq);
    assert_eq!(
        below
            .revision_at_head(history.first_inode, RevisionNo(2))
            .await
            .expect("revision lookup"),
        None
    );
    assert_matches!(
        below
            .revision_by_inode(history.first_inode, RevisionNo(2))
            .await,
        Err(CoreError::RevisionNotFound {
            revision_no: RevisionNo(2),
            ..
        })
    );

    let head = MetadataView::over_manifest_segments(&segments, history.later.committed_seq);
    let second = head
        .revision_at_head(history.first_inode, RevisionNo(2))
        .await
        .expect("revision lookup")
        .expect("revision 2 exists at the head");
    assert_eq!(second.committed_seq, history.rewritten.committed_seq);
}

/// A commit ID can't be reused while its receipt is kept, so one manifest
/// has one receipt for each commit ID. This test checks the receipt of a
/// commit after the view's sequence instead of an earlier receipt of the
/// same commit ID.
#[tokio::test]
async fn a_view_below_the_head_finds_no_commit_receipt_committed_after_its_sequence() {
    let history = history().await;
    let segments = history.segments().await;
    let below = MetadataView::over_manifest_segments(&segments, history.created.committed_seq);
    let created = below
        .find_commit_receipt(&history.created.commit_id)
        .await
        .expect("receipt lookup")
        .expect("the first commit is at the view's sequence");
    assert_eq!(created.committed_seq, history.created.committed_seq);
    assert_eq!(
        below
            .find_commit_receipt(&history.rewritten.commit_id)
            .await
            .expect("receipt lookup"),
        None
    );

    let head = MetadataView::over_manifest_segments(&segments, history.later.committed_seq);
    let rewritten = head
        .find_commit_receipt(&history.rewritten.commit_id)
        .await
        .expect("receipt lookup")
        .expect("the rewrite is below the head");
    assert_eq!(rewritten.committed_seq, history.rewritten.committed_seq);
}
