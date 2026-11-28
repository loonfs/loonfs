//! An extension's entry across the core publications: every publication
//! copies it, a namespace tombstone empties its runs, and an extension's
//! publication whose starting entry was replaced publishes nothing.

use super::*;
use crate::manifest::{ExtensionBasis, ExtensionPublication};
use crate::namespace::control::LoadedManifest;
use crate::test_support::{writer_at, NamespaceFixture};
use crate::DeleteNamespaceOptions;
use loonfs_types::format::manifest::{ExtensionEntry, ExtensionRunRef, ExtensionSegmentRef};
use loonfs_types::format::segment_blocks::BlockHandle;
use loonfs_types::{ExtensionName, IndexSegmentId};

fn grep() -> ExtensionName {
    ExtensionName::parse("grep").expect("extension name")
}

/// Returns a descriptor of a segment that `namespace_id` owns, whose ID
/// `number` determines, so two stores that publish the same entry write the
/// same bytes. Core never reads an extension segment, so the object needn't
/// exist.
pub(crate) fn extension_segment(namespace_id: &NamespaceId, number: u64) -> ExtensionSegmentRef {
    ExtensionSegmentRef {
        owner_namespace_id: namespace_id.clone(),
        segment_id: IndexSegmentId::parse(format!("idx_{number:026}")).expect("index segment id"),
        checksum: loonfs_types::Checksum::compute(
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
            &number.to_be_bytes(),
        ),
        encoding: loonfs_types::format::manifest::EXTENSION_SEGMENT_ENCODING,
        row_count: 1,
        min_row_key: "text-a".to_owned(),
        max_row_key: "text-a".to_owned(),
        index_block: BlockHandle {
            offset: 10,
            stored_bytes: 10,
            decoded_bytes: 10,
            crc32c: 0,
        },
        filter_block: BlockHandle {
            offset: 0,
            stored_bytes: 10,
            decoded_bytes: 10,
            crc32c: 0,
        },
        filter_inline: None,
    }
}

/// Returns run `run_no` at level 0 with `segments`.
pub(crate) fn extension_run(run_no: RunNo, segments: Vec<ExtensionSegmentRef>) -> ExtensionRunRef {
    ExtensionRunRef {
        run_no,
        level: 0,
        segments,
    }
}

/// Publishes an entry with `status` as its own member and one new run over
/// the current manifest, and returns the entry that the manifest lists.
pub(super) async fn publish_entry<S: ObjectStore + Clone>(
    fixture: &NamespaceFixture<S>,
    status: &str,
) -> ExtensionEntry {
    let engine = fixture.engine();
    let basis = engine.load_extension_basis().await.expect("basis");
    let published = engine
        .namespace
        .fixture_writer_session()
        .await
        .expect("the fixture's writer session")
        .publish_extension(&basis, &grep(), entry_after(&basis, status))
        .await
        .expect("publish");
    let ExtensionPublication::Published {
        basis: published, ..
    } = published.response
    else {
        panic!("no other publication races this one");
    };
    published.find_entry(&grep()).cloned().expect("the entry")
}

/// Returns the entry of `basis` with `status` as its own member and one more
/// run, whose number and segment ID are the basis entry's count of runs.
fn entry_after(basis: &ExtensionBasis, status: &str) -> ExtensionEntry {
    let mut runs = basis
        .find_entry(&grep())
        .map(|entry| entry.runs.clone())
        .unwrap_or_default();
    let number = runs.len() as u64;
    let segment = extension_segment(&basis.manifest().namespace_id, number);
    runs.push(extension_run(RunNo(number), vec![segment]));
    let state = serde_json::json!({ "status": status })
        .as_object()
        .cloned()
        .expect("an object");
    ExtensionEntry::new(1, runs, state)
}

async fn current_manifest(store: &LocalFsStore, namespace_id: &NamespaceId) -> LoadedManifest {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest")
}

async fn current_entry(store: &LocalFsStore, namespace_id: &NamespaceId) -> ExtensionEntry {
    current_manifest(store, namespace_id)
        .await
        .payload()
        .extensions[&grep()]
        .clone()
}

/// Checks that a fold, a compaction, a retention advance, a retention window
/// change, and another writer's claim each copy the entry, and that the
/// tombstone copies it with its runs emptied.
#[tokio::test]
async fn every_publication_copies_the_entry_and_a_tombstone_empties_its_runs() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("extension-copies").expect("namespace");
    let committed_at_ms = test_context().now_ms;
    let fixture = NamespaceFixture::new(&store, &namespace_id, &test_context());
    fixture.create().await.expect("create");
    fixture
        .write_file_bytes("/a", b"a", None)
        .await
        .expect("write");
    let entry = publish_entry(&fixture, "active").await;
    for path in ["/b", "/c", "/d"] {
        fixture
            .write_file_bytes(path, path.as_bytes(), None)
            .await
            .expect("write");
        crate::test_support::namespace_store(&store, &namespace_id)
            .fixture_writer_session()
            .await
            .expect("the writer's session")
            .fold()
            .await
            .expect("fold");
    }
    assert_eq!(
        current_entry(&store, &namespace_id).await,
        entry,
        "the entry after a fold"
    );
    let compacted = fixture
        .clone()
        .with_run_policy(crate::test_support::one_delta_run_policy())
        .engine()
        .compact_from_current()
        .await
        .expect("compact");
    assert!(matches!(compacted, CompactionUnitOutcome::Published { .. }));
    assert_eq!(
        current_entry(&store, &namespace_id).await,
        entry,
        "the entry after a compaction"
    );
    NamespaceFixture::new(
        &store,
        &namespace_id,
        &writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(committed_at_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance");
    assert_eq!(
        current_entry(&store, &namespace_id).await,
        entry,
        "the entry after a retention advance"
    );
    fixture
        .engine()
        .namespace
        .fixture_writer_session()
        .await
        .expect("the fixture's writer session")
        .set_retention_window(
            loonfs_types::RetentionWindow::new(2 * crate::limits::GC_GRACE_WINDOW_MS).ok(),
        )
        .await
        .expect("window");
    assert_eq!(
        current_entry(&store, &namespace_id).await,
        entry,
        "the entry after a retention window change"
    );
    let other = NamespaceFixture::new(&store, &namespace_id, &writer_at("other-writer", 0));
    other
        .write_file_bytes("/e", b"e", None)
        .await
        .expect("another writer's commit");
    assert_eq!(
        current_entry(&store, &namespace_id).await,
        entry,
        "the entry after another writer's claim"
    );

    other
        .engine()
        .delete_namespace(
            &crate::test_support::administrator(&other.engine()),
            DeleteNamespaceOptions::default(),
        )
        .await
        .expect("delete");
    let tombstone = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("tombstone");
    assert!(tombstone.payload().status.is_deleted());
    let kept = &tombstone.payload().extensions[&grep()];
    assert!(kept.runs.is_empty(), "{kept:?}");
    assert_eq!(kept.version, entry.version);
    assert_eq!(kept.state(), entry.state());
}

/// Folds the namespace's WAL tail through the fixture's writer session.
async fn fold(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let folded = crate::test_support::namespace_store(store, namespace_id)
        .fixture_writer_session()
        .await
        .expect("the writer's session")
        .fold()
        .await
        .expect("fold");
    assert_eq!(folded.response.outcome, crate::FoldWalOutcome::Published);
}

/// Returns the position of the namespace's writer session, which an
/// extension's change publishes from.
async fn session_position(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> crate::publish::WriterPosition {
    crate::test_support::namespace_store(store, namespace_id)
        .fixture_writer_session()
        .await
        .expect("the writer's session")
        .load_position_alone()
        .await
        .expect("the session's position")
}

/// Checks that an extension's publication whose entry the session's own
/// later change replaced, such as a disable, publishes nothing and returns
/// `Superseded`, that the next one builds on the winner, and that a
/// publication over a tombstone returns `namespace_deleted`.
#[tokio::test]
async fn an_extension_publication_whose_entry_the_session_replaced_publishes_nothing() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("extension-superseded").expect("namespace");
    let fixture = NamespaceFixture::new(&store, &namespace_id, &test_context());
    fixture.create().await.expect("create");
    fixture
        .write_file_bytes("/a", b"a", None)
        .await
        .expect("write");
    fold(&store, &namespace_id).await;
    let engine = fixture.engine();
    let stale = engine.load_extension_basis().await.expect("basis");
    let won = publish_entry(&fixture, "won").await;
    let winner = current_manifest(&store, &namespace_id).await;
    // The session's position follows its own publication of `won`.
    let position = session_position(&store, &namespace_id).await;

    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let lost = namespace
        .publish_extension_in_new_lane(&position, &stale, &grep(), entry_after(&stale, "lost"))
        .await
        .expect("publish");
    assert!(matches!(lost.response, ExtensionPublication::Superseded));
    assert!(lost.published.is_none());
    assert_eq!(
        current_manifest(&store, &namespace_id).await.manifest(),
        winner.manifest()
    );

    let next = publish_entry(&fixture, "next").await;
    assert_eq!(next.runs[..1], won.runs);

    let basis = engine.load_extension_basis().await.expect("basis");
    engine
        .delete_namespace(
            &crate::test_support::administrator(&engine),
            DeleteNamespaceOptions::default(),
        )
        .await
        .expect("delete");
    let tombstone = namespace
        .load_current_basis(crate::test_support::observed_now())
        .await
        .expect("the tombstone");
    let lane = crate::test_support::lane_at_manifest(tombstone);
    let deleted = namespace
        .publish_extension_from(
            lane.lock().await.acquired_turn(),
            &basis,
            &grep(),
            entry_after(&basis, "deleted"),
        )
        .await
        .expect_err("a tombstone has no successor");
    assert!(
        matches!(deleted, CoreError::NamespaceDeleted { .. }),
        "{deleted:?}"
    );
}
