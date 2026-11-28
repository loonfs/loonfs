//! A window-based retention advance in a runtime with the namespace's writer
//! session, after another runtime changed the namespace's retention window.

use loonfs::{
    AdvanceRetentionFloorOptions, CommitSeq, CreateNamespaceOptions, LoonFs, MetadataCache,
    Namespace, RetentionTarget, RetentionWindow, SharedObjectStore, Writable, GC_GRACE_WINDOW_MS,
};
use loonfs_core::control::KeptBoundary;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::MemoryStore;
use loonfs_types::{ChecksumAlgorithm, ManifestNo, NamespaceId};
use std::sync::Arc;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;

/// How many commits the fixture makes, one file each.
const COMMITS: u64 = 3;

fn window(window_ms: u64) -> RetentionWindow {
    RetentionWindow::new(window_ms).expect("a nonzero window")
}

/// Two runtimes on one store and one wall clock that the test moves.
///
/// Runtime A created the namespace with a one-day retention window, made
/// [`COMMITS`] commits through its writer session, and folded them, so its
/// session's position is on the fold's manifest. The clock then moved two
/// days and a grace window, so every commit is older than the window and
/// the grace window. Runtime B has no session for the namespace.
struct Fixture {
    store: SharedObjectStore,
    clock: Arc<ManualClock>,
    namespace_id: NamespaceId,
    a: LoonFs<Writable>,
    /// A's handle on its writer session, which keeps the session open.
    a_session: Namespace<Writable>,
    b: LoonFs<Writable>,
    /// The manifest that A's position is on.
    position: ManifestNo,
}

async fn runtime(
    store: &SharedObjectStore,
    clock: &Arc<ManualClock>,
    name: &str,
    metadata_cache: MetadataCache,
) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(writer_id(name))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .wall_clock(clock.clone())
        .metadata_cache(metadata_cache)
        .build()
        .await
        .expect("runtime")
}

/// Builds the fixture. A's runtime keeps manifests in its segment cache
/// when `a_caches_manifests` is set, and keeps none otherwise, so each load
/// of the position's manifest goes to the store. With `snapshot`, A
/// creates a snapshot after its fold, which keeps the position's manifest.
async fn fixture(name: &str, a_caches_manifests: bool, snapshot: bool) -> Fixture {
    let store: SharedObjectStore = Arc::new(MemoryStore::new(ChecksumAlgorithm::Crc64nvme));
    let clock = Arc::new(ManualClock::at_system_time());
    let namespace_id = namespace_id(name);
    let a_name = format!("{name}-a");
    let b_name = format!("{name}-b");
    let a_cache = if a_caches_manifests {
        MetadataCache::default()
    } else {
        MetadataCache::builder()
            .max_segment_bytes(0)
            .max_manifest_bytes(0)
            .build()
    };
    let a = runtime(&store, &clock, &a_name, a_cache).await;
    let actor = loonfs_test_support::test_actor();
    a.create_namespace_with_options(
        &namespace_id,
        &actor,
        &CreateNamespaceOptions {
            retention_window_ms: Some(window(DAY_MS)),
            ..CreateNamespaceOptions::default()
        },
    )
    .await
    .expect("namespace");
    let namespace = a.open_namespace(&namespace_id).expect("open namespace");
    for index in 0..COMMITS {
        namespace
            .put_file(&format!("/{index}.txt"), b"file", &actor)
            .await
            .expect("put");
    }
    a.open_namespace(&namespace_id)
        .expect("open namespace")
        .fold_wal()
        .await
        .expect("fold A's tail");
    if snapshot {
        namespace.create_snapshot().await.expect("snapshot");
    }
    a.drain().await.expect("drain");
    let position = loonfs_core::test_support::namespace_store(store.clone(), &namespace_id)
        .load_current_manifest()
        .await
        .expect("the position's manifest");
    assert_eq!(
        position
            .payload()
            .retention_window_ms
            .map(std::num::NonZeroU64::get),
        Some(DAY_MS)
    );
    assert_eq!(position.payload().head_seq, CommitSeq(COMMITS));
    clock.advance_ms(2 * DAY_MS + GC_GRACE_WINDOW_MS);
    let b = runtime(&store, &clock, &b_name, MetadataCache::default()).await;
    Fixture {
        store,
        clock,
        namespace_id,
        a,
        a_session: namespace,
        b,
        position: position.manifest().manifest_no,
    }
}

impl Fixture {
    /// Sets the namespace's retention window through runtime B.
    async fn set_window_in_b(&self, window_ms: Option<u64>) {
        self.b
            .open_namespace(&self.namespace_id)
            .expect("open namespace")
            .set_retention_window(window_ms.map(window))
            .await
            .expect("set the window in B");
    }

    /// Runs a window-based retention advance through runtime A, the
    /// advance that a host's sweep makes.
    async fn advance_by_window_in_a(&self) -> loonfs::Result<loonfs::AdvanceRetentionResponse> {
        self.a
            .open_namespace(&self.namespace_id)
            .expect("open namespace")
            .advance_retention_floor_with_options(&AdvanceRetentionFloorOptions {
                target: RetentionTarget::Window,
            })
            .await
    }

    /// Returns the namespace's retention floor as runtime B loads it from
    /// the store.
    async fn floor(&self) -> CommitSeq {
        self.b
            .maintenance()
            .diagnostics(&self.namespace_id)
            .await
            .expect("diagnostics")
            .retention_floor_seq
    }

    /// Asserts that a window-based advance through A leaves the floor at 0.
    async fn assert_advance_leaves_the_floor(&self) {
        assert!(
            self.a_session.has_current_writer_position(),
            "A's session has a position when the advance starts"
        );
        let advanced = self.advance_by_window_in_a().await;
        let floor = self.floor().await;
        assert_eq!(
            floor,
            CommitSeq(0),
            "the advance through A returned {advanced:?}, and the floor moved"
        );
    }
}

/// Checks that a window-based advance through A, after B cleared the
/// window, leaves the floor where it is, while A's position is on the
/// manifest before the change and that manifest is in A's segment cache.
#[tokio::test]
async fn a_cleared_window_stops_an_advance_from_a_cached_stale_position() {
    let fixture = fixture("cleared-cached", true, false).await;
    fixture.set_window_in_b(None).await;
    fixture.assert_advance_leaves_the_floor().await;
}

/// Checks the same as
/// [`a_cleared_window_stops_an_advance_from_a_cached_stale_position`] when
/// A's runtime keeps no manifest in its segment cache, so the advance loads
/// the position's manifest from the store.
#[tokio::test]
async fn a_cleared_window_stops_an_advance_from_an_uncached_stale_position() {
    let fixture = fixture("cleared-uncached", false, false).await;
    fixture.set_window_in_b(None).await;
    fixture.assert_advance_leaves_the_floor().await;
}

/// Checks that a window-based advance through A leaves the floor where it
/// is when a snapshot keeps the manifest that A's position is on, B cleared
/// the window, and a collection past the grace window raised the manifest
/// boundary over the position's manifest, which A's runtime doesn't cache.
///
/// B first sets a two-day window and then clears it. A collection raises
/// the boundary only to a manifest that it deletes, and the snapshot keeps
/// the position's manifest, so the manifest of the two-day window is the
/// one that the collection deletes above the position's manifest.
#[tokio::test]
async fn a_cleared_window_stops_an_advance_from_a_snapshot_kept_position_after_a_collection() {
    let fixture = fixture("cleared-kept", false, true).await;
    fixture.set_window_in_b(Some(2 * DAY_MS)).await;
    fixture.set_window_in_b(None).await;
    fixture.clock.advance_ms(GC_GRACE_WINDOW_MS + 1);
    fixture
        .b
        .maintenance()
        .gc(&fixture.namespace_id)
        .await
        .expect("collect");
    let key = metadata_manifest_object(&fixture.namespace_id, &fixture.position);
    assert!(
        fixture.store.head(&key).await.expect("HEAD").is_some(),
        "the snapshot keeps the position's manifest"
    );
    let (above, _) =
        loonfs_core::test_support::namespace_store(fixture.store.clone(), &fixture.namespace_id)
            .above_boundary(fixture.position, KeptBoundary::default())
            .await
            .expect("read the boundary");
    assert!(
        !above,
        "the collection raises the boundary over the position's manifest"
    );
    fixture.assert_advance_leaves_the_floor().await;
}

/// Checks that a window-based advance through A, after B changed the
/// window from one day to thirty days, doesn't pass the thirty-day cutoff,
/// while A's position is on the manifest before the change and that
/// manifest is in A's segment cache. Every commit is younger than thirty
/// days, so the floor stays at 0.
#[tokio::test]
async fn a_longer_window_bounds_an_advance_from_a_cached_stale_position() {
    let fixture = fixture("longer-cached", true, false).await;
    fixture.set_window_in_b(Some(30 * DAY_MS)).await;
    fixture.assert_advance_leaves_the_floor().await;
}

/// Checks that a window-based advance through A, after B took the namespace
/// over, plans from the namespace's current head, and that A's publication
/// returns `writer_fenced` instead of moving the floor, with no new writer
/// epoch and no panicked task in A's runtime.
///
/// B opens the namespace, which claims the writer epoch, commits two files
/// dated as old as A's commits, and folds them. A's position is still on its
/// own fold's manifest. The advance's check of the position finds a manifest
/// of a higher writer epoch and drops the position, so the advance plans
/// from the current manifest, whose head is B's last commit. A's session
/// then finds itself fenced when it publishes the floor, and only the
/// namespace's writer, B, publishes manifests.
#[tokio::test]
async fn an_advance_after_a_takeover_returns_writer_fenced() {
    let fixture = fixture("takeover", true, false).await;
    fixture
        .clock
        .set_ms(fixture.clock.now_ms() - 2 * DAY_MS - GC_GRACE_WINDOW_MS);
    let actor = loonfs_test_support::test_actor();
    let b_session = fixture
        .b
        .open_namespace(&fixture.namespace_id)
        .expect("open the namespace in B");
    for index in 0..2 {
        b_session
            .put_file(&format!("/b{index}.txt"), b"file", &actor)
            .await
            .expect("put through B");
    }
    b_session.fold_wal().await.expect("fold B's tail");
    let b_epoch = crate::common::writer_epoch(&fixture.store, &fixture.namespace_id).await;
    let floor = fixture.floor().await;
    fixture.clock.advance_ms(2 * DAY_MS + GC_GRACE_WINDOW_MS);
    assert!(fixture.a_session.has_current_writer_position());
    let fenced = fixture
        .advance_by_window_in_a()
        .await
        .expect_err("A is no longer the namespace's writer");
    assert_eq!(fenced.code(), loonfs::ErrorCode::WriterFenced, "{fenced:?}");
    assert_eq!(fixture.floor().await, floor, "the floor doesn't move");
    assert_eq!(
        crate::common::writer_epoch(&fixture.store, &fixture.namespace_id).await,
        b_epoch,
        "the advance claims no writer epoch"
    );
    fixture.a.drain().await.expect("no task in A panicked");
}
