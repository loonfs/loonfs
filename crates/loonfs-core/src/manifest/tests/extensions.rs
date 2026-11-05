//! An extension's entry across the core publications: every publication
//! copies it, a namespace tombstone empties its runs, and an extension's
//! publication is rebuilt over a manifest that copied the entry it built on.

use super::*;
use crate::manifest::{ExtensionBasis, ExtensionPublication};
use crate::namespace::control::LoadedManifest;
use crate::test_support::{writer_at, NamespaceFixture};
use crate::DeleteNamespaceOptions;
use loonfs_types::format::manifest::{ExtensionEntry, ExtensionRunRef, ExtensionSegmentRef};
use loonfs_types::format::segment_blocks::BlockHandle;
use loonfs_types::{CompactorEpoch, ExtensionName, IndexSegmentId};
use std::num::NonZeroU64;

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
        .publish_extension(&basis, &grep(), |base| entry_after(&basis, status, base))
        .await
        .expect("publish");
    let ExtensionPublication::Published(published) = published.response else {
        panic!("no other publication races this one");
    };
    published.find_entry(&grep()).cloned().expect("the entry")
}

/// Returns the entry of `basis` with `status` as its own member and one more
/// run, numbered `base`, and the next run number after it. The new run's
/// segment ID is determined by the basis's next run number, so the entry
/// doesn't depend on the manifest that it's published over.
fn entry_after(basis: &ExtensionBasis, status: &str, base: RunNo) -> (ExtensionEntry, RunNo) {
    let mut runs = basis
        .find_entry(&grep())
        .map(|entry| entry.runs.clone())
        .unwrap_or_default();
    let namespace_id = &basis.manifest().namespace_id;
    let segment = extension_segment(namespace_id, basis.manifest().next_run_no.0);
    runs.push(extension_run(base, vec![segment]));
    let state = serde_json::json!({ "status": status })
        .as_object()
        .cloned()
        .expect("an object");
    (
        ExtensionEntry::new(2, runs, state),
        base.successor().expect("run"),
    )
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

/// Checks that a fold, a compaction, a compactor claim, a retention advance,
/// a retention window change, and another writer's claim each copy the
/// entry, and that the tombstone copies it with its runs emptied.
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
            .fold_wal_tail(None, &fixture.namespace.start_clock())
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
        .compact_metadata_unit(
            None,
            CompactorEpoch(0),
            &MetadataCompactionCancellation::default(),
        )
        .await
        .expect("compact");
    assert!(matches!(compacted, CompactionUnitOutcome::Published { .. }));
    assert_eq!(
        current_entry(&store, &namespace_id).await,
        entry,
        "the entry after a compaction"
    );
    fixture.engine().claim_compactor().await.expect("claim");
    assert_eq!(
        current_entry(&store, &namespace_id).await,
        entry,
        "the entry after a compactor claim"
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
        .set_retention_window(NonZeroU64::new(60_000))
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
        .delete_namespace(DeleteNamespaceOptions::default())
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

/// A publication that copies an extension's entry, which another process
/// makes after an extension's change loaded its basis.
#[derive(Debug, Clone, Copy)]
enum EntryCopy {
    Fold,
    Compaction,
    RetentionAdvance,
    CompactorClaim,
}

impl EntryCopy {
    async fn publish(self, store: &LocalFsStore, namespace_id: &NamespaceId) {
        let fixture = NamespaceFixture::new(store, namespace_id, &test_context());
        match self {
            Self::Fold => {
                let folded = crate::test_support::namespace_store(store, namespace_id)
                    .fold_wal_tail(None, &fixture.namespace.start_clock())
                    .await
                    .expect("fold");
                assert_eq!(folded.response.outcome, crate::FoldWalOutcome::Published);
            }
            Self::Compaction => {
                let compacted = fixture
                    .clone()
                    .with_run_policy(crate::test_support::one_delta_run_policy())
                    .engine()
                    .compact_metadata_unit(
                        None,
                        CompactorEpoch(0),
                        &MetadataCompactionCancellation::default(),
                    )
                    .await
                    .expect("compact");
                assert!(matches!(compacted, CompactionUnitOutcome::Published { .. }));
            }
            Self::RetentionAdvance => {
                NamespaceFixture::new(
                    store,
                    namespace_id,
                    &writer_at(
                        "test-writer",
                        crate::test_support::grace_elapsed_after(test_context().now_ms),
                    ),
                )
                .engine()
                .advance_retention_floor(RetentionTarget::Head)
                .await
                .expect("advance");
            }
            Self::CompactorClaim => {
                fixture.engine().claim_compactor().await.expect("claim");
            }
        }
    }
}

/// Checks that an extension's publication that loses its put to a fold, a
/// compaction, a retention advance, or a compactor claim is published over
/// the manifest that won, which copied the entry, and that its new run's
/// number follows the winner's runs.
#[tokio::test]
async fn an_extension_publication_that_loses_to_a_copy_of_its_entry_is_rebuilt_over_it() {
    for copy in [
        EntryCopy::Fold,
        EntryCopy::Compaction,
        EntryCopy::RetentionAdvance,
        EntryCopy::CompactorClaim,
    ] {
        let directory = tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = NamespaceId::parse("extension-rebuilt").expect("namespace");
        let fixture = NamespaceFixture::new(&store, &namespace_id, &test_context());
        fixture.create().await.expect("create");
        for path in ["/a", "/b", "/c"] {
            fixture
                .write_file_bytes(path, path.as_bytes(), None)
                .await
                .expect("write");
            EntryCopy::Fold.publish(&store, &namespace_id).await;
        }
        publish_entry(&fixture, "active").await;
        fixture
            .write_file_bytes("/d", b"d", None)
            .await
            .expect("write a tail for the fold");
        let stale = fixture
            .engine()
            .load_extension_basis()
            .await
            .expect("basis");
        copy.publish(&store, &namespace_id).await;
        let winner = current_manifest(&store, &namespace_id).await;
        assert!(
            winner.payload().manifest_no > stale.manifest().manifest_no,
            "{copy:?}"
        );

        let rebuilt = fixture
            .engine()
            .publish_extension(&stale, &grep(), |base| entry_after(&stale, "rebuilt", base))
            .await
            .expect("publish");
        assert!(
            matches!(rebuilt.response, ExtensionPublication::Published(_)),
            "{copy:?}"
        );
        let current = current_manifest(&store, &namespace_id).await;
        let next_run_no = winner.payload().next_run_no;
        let entry = &current.payload().extensions[&grep()];
        assert_eq!(entry.runs.last().expect("the new run").run_no, next_run_no);
        assert_eq!(
            entry.runs[..entry.runs.len() - 1],
            winner.payload().extensions[&grep()].runs
        );
        assert_eq!(
            NamespaceManifestPayload {
                manifest_no: winner.payload().manifest_no,
                next_run_no,
                extensions: winner.payload().extensions.clone(),
                ..current.payload().clone()
            },
            *winner.payload(),
            "{copy:?}: the rebuilt manifest changes only the entry and the next run number"
        );
        assert_eq!(
            current.payload().manifest_no,
            winner.payload().manifest_no.successor().expect("next")
        );
        assert_eq!(
            current.payload().next_run_no,
            next_run_no.successor().expect("run")
        );
    }
}

/// Checks that an extension's publication that loses its put to another
/// change of its entry publishes nothing, that the next one builds on the
/// winner, and that a publication over a tombstone returns
/// `namespace_deleted`.
#[tokio::test]
async fn an_extension_publication_that_loses_to_another_change_of_its_entry_publishes_nothing() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("extension-superseded").expect("namespace");
    let fixture = NamespaceFixture::new(&store, &namespace_id, &test_context());
    fixture.create().await.expect("create");
    fixture
        .write_file_bytes("/a", b"a", None)
        .await
        .expect("write");
    EntryCopy::Fold.publish(&store, &namespace_id).await;
    let engine = fixture.engine();
    let stale = engine.load_extension_basis().await.expect("basis");
    let won = publish_entry(&fixture, "won").await;
    let winner = current_manifest(&store, &namespace_id).await;

    let lost = engine
        .publish_extension(&stale, &grep(), |base| entry_after(&stale, "lost", base))
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
    assert_eq!(next.runs[1].run_no, winner.payload().next_run_no);

    let basis = engine.load_extension_basis().await.expect("basis");
    engine
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete");
    let deleted = engine
        .publish_extension(&basis, &grep(), |base| entry_after(&basis, "deleted", base))
        .await
        .expect_err("a tombstone has no successor");
    assert!(
        matches!(deleted, CoreError::NamespaceDeleted { .. }),
        "{deleted:?}"
    );
}

/// Checks that the first fold after an extension allocated run numbers
/// numbers its run after the extension's.
#[tokio::test]
async fn a_first_fold_after_an_extension_numbers_its_run_after_the_extensions() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("extension-first").expect("namespace");
    let fixture = NamespaceFixture::new(&store, &namespace_id, &test_context());
    fixture.create().await.expect("create");
    fixture
        .write_file_bytes("/a", b"a", None)
        .await
        .expect("write");
    let entry = publish_entry(&fixture, "active").await;
    assert_eq!(entry.runs[0].run_no, RunNo(0));
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal_tail(None, &fixture.namespace.start_clock())
        .await
        .expect("fold");
    let folded = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("folded");
    assert_eq!(
        folded
            .payload()
            .runs
            .iter()
            .map(|run| run.run_no)
            .collect::<Vec<_>>(),
        vec![RunNo(1)]
    );
    assert_eq!(folded.payload().next_run_no, RunNo(2));
    assert_eq!(folded.payload().extensions[&grep()], entry);
}

/// Checks that no two runs of a manifest share a number, whether metadata
/// runs or extensions' runs, because one counter allocates the numbers of
/// the metadata runs and of every extension's runs, and that an extension's
/// run is below the next run number.
#[tokio::test]
async fn no_two_runs_of_a_manifest_share_a_number() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("extension-run-numbers").expect("namespace");
    let fixture = NamespaceFixture::new(&store, &namespace_id, &test_context());
    fixture.create().await.expect("create");
    fixture
        .write_file_bytes("/a", b"a", None)
        .await
        .expect("write");
    EntryCopy::Fold.publish(&store, &namespace_id).await;
    let folded = current_manifest(&store, &namespace_id).await;
    let metadata_run_no = folded.payload().runs[0].run_no;
    let run_no = folded.payload().next_run_no;
    let with_entries = |entries: Vec<(&str, Vec<ExtensionRunRef>)>| {
        let mut payload = folded.payload().clone();
        payload.next_run_no = run_no.successor().expect("run");
        for (name, runs) in entries {
            payload.extensions.insert(
                ExtensionName::parse(name).expect("extension name"),
                ExtensionEntry::new(2, runs, serde_json::Map::new()),
            );
        }
        crate::manifest::validate::validate_manifest(&folded.object_key, &payload)
    };
    let segment = |number| extension_segment(&namespace_id, number);

    with_entries(vec![(
        "grep",
        vec![extension_run(run_no, vec![segment(1), segment(2)])],
    )])
    .expect("two segments of one extension run");
    for (entries, rejected) in [
        (
            vec![(
                "grep",
                vec![extension_run(metadata_run_no, vec![segment(1)])],
            )],
            "an extension run at a metadata run's number",
        ),
        (
            vec![
                ("grep", vec![extension_run(run_no, vec![segment(1)])]),
                ("other", vec![extension_run(run_no, vec![segment(2)])]),
            ],
            "two extensions' runs at one number",
        ),
        (
            vec![(
                "grep",
                vec![
                    extension_run(run_no, vec![segment(1)]),
                    extension_run(run_no, vec![segment(2)]),
                ],
            )],
            "two runs of one extension at one number",
        ),
    ] {
        let error = with_entries(entries).expect_err(rejected);
        assert!(
            error.to_string().contains("duplicate run number"),
            "{error}"
        );
    }
    let error = with_entries(vec![(
        "grep",
        vec![extension_run(
            run_no.successor().expect("run"),
            vec![segment(1)],
        )],
    )])
    .expect_err("an extension run at the next run number");
    assert!(
        error.to_string().contains("is not below next run"),
        "{error}"
    );
}
