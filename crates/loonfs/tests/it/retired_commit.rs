//! A commit through a writer session that committed before another runtime
//! deleted the namespace, after a collection retired the namespace.

use loonfs::test_support::writer;
use loonfs::{ErrorCode, LoonFs, MetadataCache, SharedObjectStore, Writable};
use loonfs_objectstore::keys::namespace_prefix;
use loonfs_objectstore::{ObjectStore, ObjectStoreExtension};
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{request_lines, KeyPredicate, MemoryStore, RecordingStore};
use loonfs_types::{ChecksumAlgorithm, NamespaceId};
use std::sync::Arc;

/// How far the namespace's deletion has gone when the stale session
/// commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// The tombstone is the current manifest.
    Deleted,
    /// A collection raised the boundary to retired, and later collections
    /// deleted every object under the prefix except the boundary object and
    /// the fences.
    Retired,
}

/// Returns each object under the namespace's prefix with its stored size.
async fn prefix_objects(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> Vec<(String, u64)> {
    let mut objects = Vec::new();
    for key in store
        .list_prefix(&namespace_prefix(namespace_id))
        .await
        .expect("list the prefix")
    {
        let size = store
            .head(&key)
            .await
            .expect("HEAD")
            .map_or(0, |metadata| metadata.size_bytes);
        objects.push((key, size));
    }
    objects
}

/// Runs the sequence and returns the commit's result, the requests that the
/// commit sent, and the objects under the namespace's prefix before and
/// after the commit.
///
/// Runtime A opens a writer session and commits, so its position is on the
/// namespace's active manifest, which A's segment cache keeps when `cached`
/// is set. Runtime B deletes the namespace. For [`Stage::Retired`],
/// collections one grace window later retire the namespace and then delete
/// every object under its prefix that the reference collector deletes.
/// Nothing is deleted by hand. A client then commits through A.
async fn commit_through_the_stale_session(
    name: &str,
    stage: Stage,
    cached: bool,
) -> (
    loonfs::Result<loonfs::Commit>,
    Vec<String>,
    Vec<(String, u64)>,
    Vec<(String, u64)>,
) {
    let namespace_id = namespace_id(name);
    let recording = Arc::new(RecordingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let cache = if cached {
        MetadataCache::default()
    } else {
        MetadataCache::builder().max_manifest_bytes(0).build()
    };
    let runtime_a: LoonFs<Writable> =
        LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
            .writer_id(writer_id(&format!("{name}-a")))
            .config(loonfs::RuntimeConfig {
                min_publish_interval_ms: 0,
                ..loonfs::RuntimeConfig::default()
            })
            .metadata_cache(cache)
            .build()
            .await
            .expect("runtime A");
    let actor = loonfs_test_support::test_actor();
    runtime_a
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create");
    let session = runtime_a.open_namespace(&namespace_id).expect("open in A");
    session
        .put_file("/before.txt", b"before", &actor)
        .await
        .expect("commit before the deletion");

    let runtime_b = writer(store.clone(), &format!("{name}-b")).await;
    runtime_b
        .open_namespace(&namespace_id)
        .expect("open in B")
        .delete()
        .await
        .expect("B deletes the namespace");

    if stage == Stage::Retired {
        let maintenance =
            loonfs::test_support::maintenance_past_the_grace_window(store.clone()).await;
        let mut previous = prefix_objects(&store, &namespace_id).await;
        for _ in 0..4 {
            maintenance.gc(&namespace_id).await.expect("collect");
            let now = prefix_objects(&store, &namespace_id).await;
            if now == previous {
                break;
            }
            previous = now;
        }
    }
    let before = prefix_objects(&store, &namespace_id).await;
    recording.reset();
    let committed = session.put_file("/after.txt", b"after", &actor).await;
    let lines = request_lines(&recording.take());
    let after = prefix_objects(&store, &namespace_id).await;
    (committed, lines, before, after)
}

/// Checks that a commit through a writer session that committed before
/// another runtime deleted the namespace returns `namespace_deleted` and
/// writes nothing once a collection retired the namespace and deleted every
/// object under its prefix except the boundary object and the fences,
/// whether or not the session's runtime still caches the session's manifest. Before the retirement, while the tombstone is the current
/// manifest, the same commit returns `writer_fenced` and writes nothing.
///
/// The prefix keeps the boundary object and the fences, so the WAL number
/// after the session's tip has the fence that the deleting runtime wrote
/// when it acquired the writer epoch. With the manifest cached, the session
/// puts at that number, the fence refuses the put, and the next attempt
/// loads the namespace from the store. With the manifest evicted, the
/// session reads the manifest first. After the retirement the collection has
/// deleted the manifest, so the read finds nothing and the session reads the
/// boundary instead of putting. Before the retirement the manifest is there,
/// and the session puts as it does with the manifest cached.
#[tokio::test]
async fn a_commit_through_a_session_older_than_the_deletion_is_refused_after_retirement() {
    for stage in [Stage::Deleted, Stage::Retired] {
        for cached in [true, false] {
            let name = format!(
                "retired-commit-{}-{}",
                format!("{stage:?}").to_lowercase(),
                if cached { "hit" } else { "miss" }
            );
            let (committed, lines, before, after) =
                commit_through_the_stale_session(&name, stage, cached).await;
            assert_eq!(
                after, before,
                "the commit writes nothing ({stage:?}, cached={cached})"
            );
            let namespace = namespace_id(&name).to_string();
            let line = |request: &str| request.replace("{ns}", &namespace);
            let load = if cached {
                Vec::new()
            } else {
                vec![line("get {ns} MetadataManifest 00000000000000000003")]
            };
            // Before retirement, the tombstone records the deleting runtime's
            // writer epoch, so the session's writer anchor returns
            // `writer_fenced` before it reaches the tombstone.
            let (expected, rest) = match stage {
                Stage::Deleted => (
                    ErrorCode::WriterFenced,
                    vec![
                        line("put {ns} WalObject 00000000000000000003"),
                        line("get {ns} Boundary"),
                        line("list namespaces/{ns}/manifests/"),
                        line("get {ns} MetadataManifest 00000000000000000006"),
                        line("get {ns} Boundary"),
                    ],
                ),
                Stage::Retired if cached => (
                    ErrorCode::NamespaceDeleted,
                    vec![
                        line("put {ns} WalObject 00000000000000000003"),
                        line("get {ns} Boundary"),
                    ],
                ),
                Stage::Retired => (
                    ErrorCode::NamespaceDeleted,
                    vec![line("get {ns} Boundary"), line("get {ns} Boundary")],
                ),
            };
            assert_eq!(
                committed.map(|_| ()).map_err(|error| error.code()),
                Err(expected),
                "{stage:?}, cached={cached}"
            );
            assert_eq!(lines, [load, rest].concat(), "{stage:?}, cached={cached}");
        }
    }
}
