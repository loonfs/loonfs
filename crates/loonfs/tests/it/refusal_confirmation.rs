//! A writer session's results that it computes from its position without a
//! put, which the session's check confirms before the session returns them,
//! and reads during the session's own publication, whose check waits for the
//! publication's record instead of falling back to the read cache.

use crate::writer_position_reads::{fixture, peer, Fixture, Parked};
use loonfs::test_support::writer_position_manifest_no;
use loonfs::{DeleteNamespaceOptions, ErrorCode, NamespaceSessionStatus};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_manifest_prefix};
use loonfs_test_support::clock::bounded;
use loonfs_test_support::stores::{request_lines, RecordedOperation};
use loonfs_types::ManifestNo;
use std::time::Duration;

/// How long a step that waits for another task gets before the test fails.
const WAIT: Duration = Duration::from_secs(10);

/// Returns the manifest that the fixture's session's position is on.
fn position_manifest_no(fixture: &Fixture) -> ManifestNo {
    writer_position_manifest_no(&fixture.writer, &fixture.namespace_id)
        .expect("the session has a position")
}

/// Returns the number of manifest puts in `operations`.
fn manifest_puts(fixture: &Fixture, operations: &[RecordedOperation]) -> usize {
    let manifests = metadata_manifest_prefix(&fixture.namespace_id);
    operations
        .iter()
        .filter(|operation| {
            matches!(operation, RecordedOperation::Put { .. })
                && operation.key().starts_with(&manifests)
        })
        .count()
}

/// Checks that a commit whose precondition fails, and a deletion whose
/// expected head doesn't match, in a session that another writer displaced and
/// that hasn't put since, return `writer_fenced`, because the session's check
/// confirms the position before the session returns a refusal computed from
/// it, and settles the position, which finds the other writer's claim.
///
/// The peer deletes `/dir` and commits once more, so the old session's
/// position still has `/dir` and the old head. A `create_directory` of
/// `/dir` would be refused there as `path_conflict`, and a deletion that
/// expects the peer's head as `stale_head`, both without a put.
#[tokio::test]
async fn a_refusal_of_a_displaced_session_returns_writer_fenced() {
    for deletion in [false, true] {
        let name = if deletion {
            "displaced-refused-deletion"
        } else {
            "displaced-refused-commit"
        };
        let fixture = fixture(name, Parked::WalPuts).await;
        let actor = loonfs_test_support::test_actor();
        fixture
            .namespace
            .create_directory("/dir", &actor)
            .await
            .expect("the session's commit");
        let peer = peer(&fixture, &format!("{name}-peer")).await;
        let peer_namespace = peer
            .open_namespace(&fixture.namespace_id)
            .expect("open namespace");
        peer_namespace
            .delete_path("/dir", &actor)
            .await
            .expect("the peer claims the namespace and deletes the directory");
        let peer_head = peer_namespace
            .create_directory("/peer", &actor)
            .await
            .expect("the peer's commit")
            .committed_seq;
        let refused = if deletion {
            fixture
                .namespace
                .delete_with_options(&DeleteNamespaceOptions {
                    expected_head_seq: Some(peer_head),
                })
                .await
                .map(drop)
        } else {
            fixture
                .namespace
                .create_directory("/dir", &actor)
                .await
                .map(drop)
        };
        assert_eq!(
            refused.expect_err("the session is displaced").code(),
            ErrorCode::WriterFenced,
            "{name}"
        );
        assert_eq!(
            fixture.namespace.session_status(),
            NamespaceSessionStatus::Fenced,
            "{name}"
        );
        peer.shutdown().await.expect("shut down the peer");
    }
}

/// Checks the requests of a commit whose precondition fails in a session
/// that is still the namespace's writer: the commit sends no request of its
/// own, and the session's check confirms the position with one HEAD of the
/// manifest after the position's and one read of the manifest boundary
/// before the session returns the refusal. A commit that puts sends its WAL put and nothing else.
#[tokio::test]
async fn a_refused_commit_of_the_writer_sends_one_head_and_one_boundary_read() {
    let fixture = fixture("writer-refused-commit", Parked::WalPuts).await;
    let actor = loonfs_test_support::test_actor();
    fixture
        .namespace
        .create_directory("/dir", &actor)
        .await
        .expect("the session's commit");
    let manifest_no = position_manifest_no(&fixture);
    fixture.recording.reset();
    let refused = fixture
        .namespace
        .create_directory("/dir", &actor)
        .await
        .expect_err("the directory exists");
    assert_eq!(refused.code(), ErrorCode::PathConflict);
    let namespace_id = &fixture.namespace_id;
    assert_eq!(
        request_lines(&fixture.recording.take()),
        [
            format!(
                "head {namespace_id} MetadataManifest {:020}",
                manifest_no.0 + 1
            ),
            format!("get {namespace_id} Boundary"),
        ]
    );
    fixture
        .namespace
        .create_directory("/other", &actor)
        .await
        .expect("a commit that puts");
    assert!(
        matches!(
            fixture.recording.take().as_slice(),
            [RecordedOperation::Put { .. }]
        ),
        "the commit sends its WAL put and nothing else"
    );
}

/// Starts a fold of the fixture's tail, and returns it once the fold has put
/// its manifest and waits for the read of the manifest boundary after the
/// put, so the session hasn't recorded the manifest yet, with the number of
/// the fold's manifest. The recording starts at the fold. The caller releases
/// the read and awaits the fold.
async fn fold_parked_before_its_record(
    fixture: &Fixture,
) -> (
    impl std::future::Future<Output = loonfs::Result<loonfs::FoldWalResponse>> + '_,
    ManifestNo,
) {
    let manifest_no = position_manifest_no(fixture);
    fixture.recording.reset();
    fixture.recording.inner().block_next();
    let fold = tokio::spawn({
        let namespace = fixture.namespace.clone();
        async move { namespace.fold_wal().await }
    });
    bounded(WAIT, fixture.recording.inner().wait_until_blocked())
        .await
        .expect("the fold reads the boundary after its put");
    let folded = ManifestNo(manifest_no.0 + 1);
    assert!(
        fixture.recording.snapshot().iter().any(|operation| {
            matches!(operation, RecordedOperation::Put { .. })
                && operation.key() == metadata_manifest_object(&fixture.namespace_id, &folded)
        }),
        "the fold has put its manifest"
    );
    let fold = async move { fold.await.expect("join the fold") };
    (fold, folded)
}

/// Waits until `fixture`'s recording has a HEAD of manifest `manifest_no`.
async fn wait_for_head_of(fixture: &Fixture, manifest_no: ManifestNo) {
    let key = metadata_manifest_object(&fixture.namespace_id, &manifest_no);
    bounded(WAIT, async {
        while !fixture.recording.snapshot().iter().any(
            |operation| matches!(operation, RecordedOperation::Head { key: head } if *head == key),
        ) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the session's check finds the fold's manifest");
}

/// Lets the tasks that this test spawned run until each waits for the store
/// or a lock. The test's runtime has one thread and an in-memory store, so a
/// spawned task that isn't waiting for a lock runs to its end meanwhile.
async fn let_spawned_tasks_run() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}

/// Checks that a strong read while the session's own fold is between its
/// manifest put and its record waits for the record, and then reads from the
/// session's position on the fold's manifest, with a commit that the session
/// acknowledged meanwhile.
///
/// The read's check finds the fold's manifest, so the session settles its
/// position: it waits for the publication lane, finds that its position
/// follows the fold's manifest, and returns without the engine, so a commit
/// sent meanwhile doesn't wait for the read. The check then runs once more on
/// the fold's manifest. The read's requests are the HEAD that finds the
/// fold's manifest, then, once the fold has read the boundary after its put
/// and recorded its manifest, one HEAD and one boundary read, with no load of
/// the position.
#[tokio::test]
async fn a_strong_read_during_the_sessions_own_publication_reads_the_published_position() {
    let fixture = fixture("read-during-publication", Parked::BoundaryReads).await;
    let actor = loonfs_test_support::test_actor();
    fixture
        .namespace
        .create_directory("/dir", &actor)
        .await
        .expect("the session's commit");
    let (fold, folded) = fold_parked_before_its_record(&fixture).await;
    fixture.recording.reset();
    let read = tokio::spawn({
        let reader = fixture.reader.strong();
        async move { reader.stat("/during").await }
    });
    wait_for_head_of(&fixture, folded).await;
    let commit = bounded(WAIT, fixture.namespace.create_directory("/during", &actor))
        .await
        .expect("the commit doesn't wait for the read")
        .expect("the commit during the publication");
    let_spawned_tasks_run().await;
    assert!(!read.is_finished(), "the read waits for the fold's record");
    fixture.recording.inner().release();
    bounded(WAIT, fold)
        .await
        .expect("the fold ends")
        .expect("fold");
    let entry = bounded(WAIT, read)
        .await
        .expect("the read ends")
        .expect("join the read")
        .expect("stat");
    assert_eq!(entry.head_seq, commit.committed_seq);
    let namespace_id = &fixture.namespace_id;
    let requests: Vec<String> = request_lines(&fixture.recording.take())
        .into_iter()
        .filter(|line| !line.starts_with("put "))
        .collect();
    assert_eq!(
        requests,
        [
            format!("head {namespace_id} MetadataManifest {:020}", folded.0),
            // The fold's read of the boundary after its put, which the
            // recording logs once the store answers it.
            format!("get {namespace_id} Boundary"),
            format!("head {namespace_id} MetadataManifest {:020}", folded.0 + 1),
            format!("get {namespace_id} Boundary"),
        ],
        "the commit's WAL put is the only other request"
    );
    assert_eq!(position_manifest_no(&fixture), folded);
}

/// Checks that an `eventual` read during the session's own publication sees
/// a commit that the session acknowledged in that window.
///
/// The position's last check is past the maximum staleness, so each read
/// checks it, and the check finds the fold's manifest before the fold
/// records it. A read that fell back to the read cache's head would read the
/// head that a discovery loaded before the commit, inside its staleness, so
/// both reads wait for the fold's record and read from the session's
/// position instead.
#[tokio::test]
async fn an_eventual_read_during_the_sessions_own_publication_sees_an_acknowledged_commit() {
    let fixture = fixture("eventual-during-publication", Parked::BoundaryReads).await;
    let actor = loonfs_test_support::test_actor();
    fixture
        .namespace
        .create_directory("/dir", &actor)
        .await
        .expect("the session's commit");
    fixture
        .clock
        .advance_ms(crate::writer_position_reads::MAX_STALENESS_MS);
    let (fold, folded) = fold_parked_before_its_record(&fixture).await;
    let first = tokio::spawn({
        let reader = fixture.reader.clone();
        async move { reader.stat("/dir").await }
    });
    wait_for_head_of(&fixture, folded).await;
    let commit = bounded(
        WAIT,
        fixture.namespace.create_directory("/acknowledged", &actor),
    )
    .await
    .expect("the commit doesn't wait for the read")
    .expect("the commit during the publication");
    let second = tokio::spawn({
        let reader = fixture.reader.clone();
        async move { reader.stat("/acknowledged").await }
    });
    let_spawned_tasks_run().await;
    fixture.recording.inner().release();
    bounded(WAIT, fold)
        .await
        .expect("the fold ends")
        .expect("fold");
    bounded(WAIT, first)
        .await
        .expect("the first read ends")
        .expect("join the first read")
        .expect("the first read");
    let entry = bounded(WAIT, second)
        .await
        .expect("the second read ends")
        .expect("join the second read")
        .expect("the second read sees the acknowledged commit");
    assert_eq!(entry.head_seq, commit.committed_seq);
}

/// Checks that a commit whose precondition fails while the session's own
/// fold is between its manifest put and its record returns the refusal once
/// the fold records its manifest, without a load of the position or a claim.
///
/// The session's check of its position finds the fold's manifest after it,
/// so the session settles its position: once the fold leaves the publication
/// lane, the position follows the fold's manifest, which the check then
/// confirms under the session's epoch.
#[tokio::test]
async fn a_refusal_during_the_sessions_own_publication_is_confirmed_without_a_load() {
    let fixture = fixture("refused-during-publication", Parked::BoundaryReads).await;
    let actor = loonfs_test_support::test_actor();
    fixture
        .namespace
        .create_directory("/dir", &actor)
        .await
        .expect("the session's commit");
    let (fold, folded) = fold_parked_before_its_record(&fixture).await;
    let refused = tokio::spawn({
        let namespace = fixture.namespace.clone();
        let actor = actor.clone();
        async move { namespace.create_directory("/dir", &actor).await }
    });
    wait_for_head_of(&fixture, folded).await;
    fixture.recording.inner().release();
    bounded(WAIT, fold)
        .await
        .expect("the fold ends")
        .expect("fold");
    let refused = bounded(WAIT, refused)
        .await
        .expect("the refusal ends")
        .expect("join the refusal");
    assert_eq!(
        refused.expect_err("the directory exists").code(),
        ErrorCode::PathConflict
    );
    let operations = fixture.recording.take();
    assert_eq!(
        manifest_puts(&fixture, &operations),
        1,
        "only the fold puts a manifest, so the session doesn't claim: {operations:?}"
    );
    assert!(
        !operations.iter().any(|operation| matches!(
            operation,
            RecordedOperation::List { prefix } if *prefix == metadata_manifest_prefix(&fixture.namespace_id)
        )),
        "the session doesn't load its position: {operations:?}"
    );
    assert_eq!(position_manifest_no(&fixture), folded);
    assert_eq!(
        fixture.namespace.session_status(),
        NamespaceSessionStatus::Open
    );
}

/// A value that a session computes from its own state without a put.
#[derive(Debug, Clone, Copy)]
enum Unconfirmed {
    /// A commit that replays the receipt of an earlier commit.
    ReceiptReplay,
    /// A window change to the window that the manifest already has.
    SameWindow,
    /// An extension's change whose starting entry another change replaced.
    SupersededExtension,
    /// A load of an extension's basis from the session's position.
    ExtensionBasis,
}

/// Checks that each value that a session computes from its own state
/// without a put is confirmed before the session returns it, so a session
/// that another writer displaced, and that hasn't put since, returns
/// `writer_fenced` for each.
#[tokio::test]
async fn every_value_without_a_put_of_a_displaced_session_returns_writer_fenced() {
    let actor = loonfs_test_support::test_actor();
    let commit_id = loonfs_types::CommitId::parse("replayed").expect("commit id");
    let replayed = loonfs::CreateDirectoryOptions {
        commit: loonfs::CommitOptions {
            commit_id: Some(commit_id),
            ..loonfs::CommitOptions::default()
        },
        ..loonfs::CreateDirectoryOptions::default()
    };
    let window = loonfs::RetentionWindow::new(2 * loonfs::GC_GRACE_WINDOW_MS).expect("window");
    let grep = loonfs_types::ExtensionName::parse("grep").expect("extension name");
    let entry = |version| {
        loonfs_types::format::manifest::ExtensionEntry::new(
            version,
            Vec::new(),
            serde_json::Map::new(),
        )
    };
    for case in [
        Unconfirmed::ReceiptReplay,
        Unconfirmed::SameWindow,
        Unconfirmed::SupersededExtension,
        Unconfirmed::ExtensionBasis,
    ] {
        let name = format!("unconfirmed-{case:?}").to_lowercase();
        let fixture = fixture(&name, Parked::WalPuts).await;
        let namespace = &fixture.namespace;
        let basis = namespace.load_extension_basis().await.expect("basis");
        match case {
            Unconfirmed::ReceiptReplay => drop(
                namespace
                    .create_directory_with_options("/replayed", &actor, &replayed)
                    .await
                    .expect("the commit"),
            ),
            Unconfirmed::SameWindow => drop(
                namespace
                    .set_retention_window(Some(window))
                    .await
                    .expect("the window change"),
            ),
            Unconfirmed::SupersededExtension => drop(
                namespace
                    .publish_extension(&basis, &grep, entry(1))
                    .await
                    .expect("the extension's change"),
            ),
            Unconfirmed::ExtensionBasis => {}
        }
        peer(&fixture, &format!("{name}-peer"))
            .await
            .open_namespace(&fixture.namespace_id)
            .expect("open namespace")
            .put_file("/peer.txt", b"peer", &actor)
            .await
            .expect("the peer claims the namespace");
        let error = match case {
            Unconfirmed::ReceiptReplay => namespace
                .create_directory_with_options("/replayed", &actor, &replayed)
                .await
                .map(drop),
            Unconfirmed::SameWindow => namespace.set_retention_window(Some(window)).await.map(drop),
            Unconfirmed::SupersededExtension => namespace
                .publish_extension(&basis, &grep, entry(2))
                .await
                .map(drop),
            Unconfirmed::ExtensionBasis => namespace.load_extension_basis().await.map(drop),
        }
        .expect_err("the session is displaced");
        assert_eq!(error.code(), ErrorCode::WriterFenced, "{case:?}");
    }
}

/// Checks that a read in a runtime whose session of the namespace has no
/// writer epoch doesn't claim the namespace: the read's check has no
/// position to settle, and the store has no new manifest after the read.
#[tokio::test]
async fn a_read_never_causes_a_claim() {
    let fixture = fixture("read-never-claims", Parked::WalPuts).await;
    let other = peer(&fixture, "read-never-claims-other").await;
    let _session = other
        .open_namespace(&fixture.namespace_id)
        .expect("a session without an epoch");
    fixture.recording.reset();
    for reader in [
        other.read_only().namespace(&fixture.namespace_id),
        other.read_only().namespace(&fixture.namespace_id).strong(),
    ] {
        reader.stat("/small.txt").await.expect("stat");
    }
    let operations = fixture.recording.take();
    assert_eq!(manifest_puts(&fixture, &operations), 0, "{operations:?}");
}
