//! The content revision index: one row for each file revision, keyed by the
//! content value that the revision refers to, written from the WAL tail and
//! by a fold, read a page at a time for each value, and kept by a base
//! compaction while the basis has the revision.

use super::expired_deletion_rows::{compact_group, family_rows};
use super::*;
use crate::metadata::{MetadataView, RevisionRecord};
use loonfs_types::format::manifest::ContentRevisionRecord;
use loonfs_types::{ContentId, DestinationPrecondition};

/// Reads revision `revision_no` of `inode_id` through the current read view.
async fn revision<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    revision_no: u64,
) -> RevisionRecord {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_metadata_view()
        .await
        .expect("read view")
        .metadata_view()
        .revision_at_head(inode_id, RevisionNo(revision_no))
        .await
        .expect("revision lookup")
        .expect("the revision exists")
}

/// Reads one page of a content value's rows through the current read view.
async fn content_revisions_page<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    value: (&ContentId, u64),
    start_after: Option<(InodeId, RevisionNo)>,
    limit: usize,
) -> Vec<ContentRevisionRecord> {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_metadata_view()
        .await
        .expect("read view")
        .metadata_view()
        .content_revisions_page(value.0, value.1, start_after, limit)
        .await
        .expect("content revision lookup")
}

fn indexed(revisions: &[&RevisionRecord]) -> Vec<ContentRevisionRecord> {
    revisions
        .iter()
        .map(|revision| ContentRevisionRecord::from_revision(revision))
        .collect()
}

/// Checks the page lookup for the two values that
/// [`revisions_are_indexed_by_content_value_in_the_tail_and_the_fold`]
/// writes, first with one page for each value and then with one page for
/// each row.
async fn assert_pages<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_id: &ContentId,
    expected: [&[ContentRevisionRecord]; 2],
) {
    for (size_bytes, rows) in [3, 6].into_iter().zip(expected) {
        assert_eq!(
            content_revisions_page(store, namespace_id, (content_id, size_bytes), None, 8).await,
            rows,
            "size {size_bytes}"
        );
        let mut start_after = None;
        for row in rows {
            let page = content_revisions_page(
                store,
                namespace_id,
                (content_id, size_bytes),
                start_after,
                1,
            )
            .await;
            assert_eq!(page, std::slice::from_ref(row), "size {size_bytes}");
            start_after = Some((row.inode_id, row.revision_no));
        }
        let last = content_revisions_page(
            store,
            namespace_id,
            (content_id, size_bytes),
            start_after,
            1,
        )
        .await;
        assert!(last.is_empty(), "{last:?}");
    }
}

/// Checks that the projected tail and a fold both index every revision by
/// its content value: a put, an append that extends the chain to a longer
/// value, and a copy by reference and a restore that refer to values that
/// earlier revisions refer to. A lookup returns a value's revisions by
/// inode, newest first, and a page that starts after a row continues where
/// the last page stopped.
#[tokio::test]
async fn revisions_are_indexed_by_content_value_in_the_tail_and_the_fold() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let fixture = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
    fixture
        .write_file_bytes("/a.txt", b"one", None)
        .await
        .expect("put /a.txt");
    for operation in [
        FilesystemOperation::AppendFile {
            path: AbsolutePath::parse("/a.txt").expect("path"),
            inline_content: b"two".to_vec(),
            expected_inode_id: None,
            expected_revision_no: None,
        },
        FilesystemOperation::CopyPath {
            source_path: AbsolutePath::parse("/a.txt").expect("path"),
            destination_path: AbsolutePath::parse("/b.txt").expect("path"),
            precondition: DestinationPrecondition::default(),
        },
        FilesystemOperation::RestoreRevision {
            path: AbsolutePath::parse("/a.txt").expect("path"),
            source_revision_no: RevisionNo(1),
        },
    ] {
        fixture
            .submit_operation(CommitId::generate(), operation, Vec::new())
            .await
            .expect("publish the operation");
    }
    let a = crate::test_support::visible_inode_id(&store, &namespace_id, "/a.txt").await;
    let b = crate::test_support::visible_inode_id(&store, &namespace_id, "/b.txt").await;
    assert!(a < b, "the file created first has the smaller inode ID");
    let put = revision(&store, &namespace_id, a, 1).await;
    let append = revision(&store, &namespace_id, a, 2).await;
    let copy = revision(&store, &namespace_id, b, 1).await;
    let restore = revision(&store, &namespace_id, a, 3).await;
    let content_id = put.content_ref.content_id.clone();
    assert_eq!(
        [&put, &append, &copy, &restore].map(|revision| (
            &revision.content_ref.content_id,
            revision.content_ref.size_bytes
        )),
        [3, 6, 6, 3].map(|size_bytes| (&content_id, size_bytes)),
        "the append extends the chain in place, and the copy and the restore \
         refer to the values of earlier revisions"
    );
    let three = indexed(&[&restore, &put]);
    let six = indexed(&[&append, &copy]);
    let expected: Vec<MetadataRow> = three
        .iter()
        .chain(&six)
        .cloned()
        .map(MetadataRow::ContentRevision)
        .collect();

    let tail_rows = crate::test_support::namespace_store(&store, &namespace_id)
        .load_manifest_projection(crate::test_support::observed_now())
        .await
        .expect("projection")
        .tail_state
        .merged_rows();
    assert_eq!(
        manifest_rows_for_family(&tail_rows, ApiMetadataRowFamily::ContentRevisions),
        expected
    );
    assert_pages(&store, &namespace_id, &content_id, [&three[..], &six[..]]).await;

    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the revisions");
    assert_eq!(
        family_rows(
            &store,
            &namespace_id,
            ApiMetadataRowFamily::ContentRevisions
        )
        .await,
        expected
    );
    assert_pages(&store, &namespace_id, &content_id, [&three[..], &six[..]]).await;
}

/// Checks that a view below its manifest's head returns only the rows at or
/// below its sequence, and still fills a page when the rows above its
/// sequence come first in key order.
#[tokio::test]
async fn a_view_below_the_head_reads_past_the_rows_above_its_sequence() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let fixture = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
    let put = fixture
        .write_file_bytes("/a.txt", b"one", None)
        .await
        .expect("put /a.txt");
    for _ in 0..2 {
        fixture
            .write_file_bytes("/a.txt", b"two", None)
            .await
            .expect("rewrite /a.txt");
        fixture
            .restore_file_revision("/a.txt", RevisionNo(1), None)
            .await
            .expect("restore revision 1");
    }
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the revisions");
    let a = crate::test_support::visible_inode_id(&store, &namespace_id, "/a.txt").await;
    let content_ref = revision(&store, &namespace_id, a, 1).await.content_ref;
    let manifest_no = current_manifest_number(&store, &namespace_id).await;
    let segments = load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest_no)
        .await
        .expect("load the manifest's segments");

    // Revisions 5 and 3 restore revision 1 above the put's sequence, and
    // they sort before it.
    let below = MetadataView::over_manifest_segments(&segments, put.committed_seq);
    let page = below
        .content_revisions_page(&content_ref.content_id, content_ref.size_bytes, None, 1)
        .await
        .expect("content revision lookup");
    assert_eq!(
        page.iter()
            .map(|row| (row.inode_id, row.revision_no))
            .collect::<Vec<_>>(),
        [(a, RevisionNo(1))]
    );
    let head =
        MetadataView::over_manifest_segments(&segments, segments.manifest().payload().head_seq);
    let page = head
        .content_revisions_page(&content_ref.content_id, content_ref.size_bytes, None, 8)
        .await
        .expect("content revision lookup");
    assert_eq!(
        page.iter().map(|row| row.revision_no).collect::<Vec<_>>(),
        [5, 3, 1].map(RevisionNo)
    );
}

/// Checks that a base compaction of the group keeps a row while the basis's
/// `revisions` family has its revision: before the revisions group drops a
/// revision below the floor, the row stays, and the next base compaction of
/// the group after the drop removes it through the revision filter.
#[tokio::test]
async fn a_base_compaction_keeps_the_rows_of_the_revisions_that_the_basis_has() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let fixture = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    for body in ["one", "two", "three"] {
        fixture
            .write_file_bytes("/a.txt", body.as_bytes(), None)
            .await
            .expect("write /a.txt");
        namespace.fold_wal().await.expect("fold the revision");
    }
    let a = crate::test_support::visible_inode_id(&store, &namespace_id, "/a.txt").await;
    let mut revisions = Vec::new();
    for revision_no in 1..=3 {
        revisions.push(revision(&store, &namespace_id, a, revision_no).await);
    }
    // A floor at the second revision supersedes the first.
    advance_floor_through(&store, &namespace_id, revisions[1].committed_seq, &context).await;
    let rows_of = |revisions: &[&RevisionRecord]| {
        let mut rows: Vec<MetadataRow> = indexed(revisions)
            .into_iter()
            .map(MetadataRow::ContentRevision)
            .collect();
        rows.sort_by_key(MetadataRow::row_key);
        rows
    };

    compact_group(
        &store,
        &namespace_id,
        &context,
        MetadataFamilyGroup::ContentRevisions,
    )
    .await;
    assert_eq!(
        family_rows(
            &store,
            &namespace_id,
            ApiMetadataRowFamily::ContentRevisions
        )
        .await,
        rows_of(&[&revisions[0], &revisions[1], &revisions[2]]),
        "the basis has every revision"
    );

    compact_group(
        &store,
        &namespace_id,
        &context,
        MetadataFamilyGroup::Revisions,
    )
    .await;
    let kept: Vec<_> = family_rows(&store, &namespace_id, ApiMetadataRowFamily::Revisions)
        .await
        .into_iter()
        .map(|row| match row {
            MetadataRow::FileRevision(revision) => revision.revision_no,
            other => panic!("the revisions family holds a foreign row: {other:?}"),
        })
        .collect();
    assert_eq!(kept, [RevisionNo(3), RevisionNo(2)]);
    // A second run of the group, so the next compaction is bottom-anchored
    // over two runs.
    fixture
        .write_file_bytes("/b.txt", b"four", None)
        .await
        .expect("write /b.txt");
    namespace.fold_wal().await.expect("fold /b.txt");
    let b = crate::test_support::visible_inode_id(&store, &namespace_id, "/b.txt").await;
    let other = revision(&store, &namespace_id, b, 1).await;
    compact_group(
        &store,
        &namespace_id,
        &context,
        MetadataFamilyGroup::ContentRevisions,
    )
    .await;
    assert_eq!(
        family_rows(
            &store,
            &namespace_id,
            ApiMetadataRowFamily::ContentRevisions
        )
        .await,
        rows_of(&[&revisions[1], &revisions[2], &other]),
        "the first revision's row goes with its revision"
    );
    let first = &revisions[0].content_ref;
    let page = content_revisions_page(
        &store,
        &namespace_id,
        (&first.content_id, first.size_bytes),
        None,
        8,
    )
    .await;
    assert!(page.is_empty(), "{page:?}");
}

/// Checks that a manifest written before the family existed, whose runs
/// have no `content_revisions` segments, reads as before: its files resolve
/// and read, the index has no rows for its revisions, and the revisions
/// committed after it are indexed.
#[tokio::test]
async fn a_manifest_without_the_family_reads_unchanged_and_indexes_later_revisions() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let fixture = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    fixture
        .write_file_bytes("/a.txt", b"one", None)
        .await
        .expect("put /a.txt");
    namespace.fold_wal().await.expect("fold the put");
    let mut payload = namespace
        .load_current_manifest()
        .await
        .expect("load the current manifest")
        .payload()
        .clone();
    for run in &mut payload.runs {
        run.segments
            .retain(|segments| segments.family != ApiMetadataRowFamily::ContentRevisions);
    }
    let manifest = encode_namespace_manifest_json(payload)
        .expect("encode the manifest without the family")
        .into_envelope();
    super::index_parity::overwrite_manifest(&store, &namespace_id, manifest).await;

    let a = crate::test_support::visible_inode_id(&store, &namespace_id, "/a.txt").await;
    let first = revision(&store, &namespace_id, a, 1).await;
    let value = (&first.content_ref.content_id, first.content_ref.size_bytes);
    let page = content_revisions_page(&store, &namespace_id, value, None, 8).await;
    assert!(page.is_empty(), "{page:?}");
    for body in ["two", "three"] {
        fixture
            .write_file_bytes("/a.txt", body.as_bytes(), None)
            .await
            .expect("rewrite /a.txt");
        namespace.fold_wal().await.expect("fold the rewrite");
    }
    let later = [
        revision(&store, &namespace_id, a, 2).await,
        revision(&store, &namespace_id, a, 3).await,
    ];
    compact_group(
        &store,
        &namespace_id,
        &context,
        MetadataFamilyGroup::ContentRevisions,
    )
    .await;
    let mut expected: Vec<MetadataRow> = indexed(&[&later[0], &later[1]])
        .into_iter()
        .map(MetadataRow::ContentRevision)
        .collect();
    expected.sort_by_key(MetadataRow::row_key);
    assert_eq!(
        family_rows(
            &store,
            &namespace_id,
            ApiMetadataRowFamily::ContentRevisions
        )
        .await,
        expected
    );
    let page = content_revisions_page(&store, &namespace_id, value, None, 8).await;
    assert!(page.is_empty(), "{page:?}");
}
