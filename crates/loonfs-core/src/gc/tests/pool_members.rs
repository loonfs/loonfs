//! The pool's collection, which protects content with the roots of every member
//! of the pool.

use super::*;
use crate::cache::CompletedUploadSessionCache;
use crate::limits::POOL_ROOTS_READ_BUDGET_MS;
use crate::manifest::tests::{
    build_namespace_manifest_from_metadata_state, write_namespace_manifest, ManifestMetadataSource,
};
use crate::metadata::MetadataStateBuilder;
use crate::namespace::state::NamespaceReadState;
use crate::segment::MetadataRunPolicy;
use crate::time::OperationClock;
use loonfs_objectstore::keys::{
    content_blob, pool_member, temporary_object, temporary_prefix, upload_outcome, upload_session,
};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::control::UploadSessionPayload;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{
    ChecksumAlgorithm, CommitSeq, ContentExtent, ContentId, ContentLayout, ExtentObject,
    GcSkippedWork,
};

/// Creates `names[0]`, then forks each later name from the one before it,
/// and returns the pool and the namespace IDs.
async fn pool<S: ObjectStore + Clone>(store: S, names: &[&str]) -> (PoolId, Vec<NamespaceId>) {
    let setup = context(1_000);
    let members = names
        .iter()
        .map(|name| NamespaceId::parse(*name).expect("namespace"))
        .collect::<Vec<_>>();
    let pool_id = crate::test_support::NamespaceFixture::new(store.clone(), &members[0], &setup)
        .create()
        .await
        .expect("create the namespace without a source")
        .pool_id;
    for pair in members.windows(2) {
        fork(store.clone(), &pair[0], &pair[1]).await;
    }
    (pool_id, members)
}

async fn fork<S: ObjectStore + Clone>(store: S, source: &NamespaceId, target: &NamespaceId) {
    let fixture = crate::test_support::NamespaceFixture::new(store, source, &context(1_000));
    fixture
        .engine()
        .fork_namespace(
            target,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
}

/// Writes a content object under the prefix of `pool_id` and returns its key.
async fn plant<S: ObjectStore>(store: &S, pool_id: &PoolId, content_id: &ContentId) -> String {
    let key = content_blob(pool_id, content_id);
    store
        .put_if_absent(&key, Bytes::from_static(b"x"))
        .await
        .expect("content object");
    key
}

/// Publishes the next manifest of `namespace_id` with one more delta run,
/// whose layout row, created by `namespace_id`, refers to the object of
/// `content_id` under the prefix of its pool.
///
/// The test writes the row directly, so a planted object is the only object
/// that the member's view refers to.
async fn publish_layout<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    content_id: &ContentId,
) {
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    let mut head = NamespaceReadState::from(current.payload());
    head.seq = CommitSeq(head.seq.0 + 1);
    let mut state = MetadataStateBuilder::default();
    state.push_content_layout(ContentLayoutRecord {
        content_id: content_id.clone(),
        size_bytes: 1,
        committed_seq: head.seq,
        layout: ContentLayout {
            creator_namespace_id: namespace_id.clone(),
            extents: vec![ContentExtent {
                content_id: content_id.clone(),
                object: ExtentObject::Whole,
                offset: 0,
                length: 1,
            }],
        },
    });
    let manifest_no = current.manifest().manifest_no;
    let manifest = build_namespace_manifest_from_metadata_state(
        store,
        namespace_id,
        ManifestMetadataSource {
            head: &head,
            basis_manifest_no: Some(manifest_no),
            retention_floor_seq: current.payload().retention_floor_seq,
            metadata_state: &state.build(),
        },
        MetadataRunPolicy::default(),
        ManifestNo(manifest_no.0 + 1),
    )
    .await
    .expect("manifest");
    write_namespace_manifest(store, manifest.payload().clone())
        .await
        .expect("write the manifest");
}

/// Returns a context that's one grace window past the newest object of
/// every namespace in `members`.
async fn aged_context<S: ObjectStore>(store: &S, members: &[NamespaceId]) -> WriterAt {
    let mut now_ms = 0;
    for namespace_id in members {
        now_ms =
            now_ms.max(now_after_newest_object(store, namespace_id, GC_GRACE_WINDOW_MS + 1).await);
    }
    context(now_ms)
}

async fn exists<S: ObjectStore>(store: &S, key: &str) -> bool {
    store.head(key).await.expect("head").is_some()
}

/// Runs the collection of `namespace_id` at `now_ms`.
async fn collect<S: ObjectStore>(store: &S, namespace_id: &NamespaceId, now_ms: u64) -> GcResponse {
    crate::test_support::namespace_store(store, namespace_id)
        .gc(&context(now_ms).clock())
        .await
        .expect("collection")
}

/// Runs the collection of `pool_id` at `now_ms`.
async fn collect_pool_at<S: ObjectStore>(
    store: &S,
    pool_id: &PoolId,
    now_ms: u64,
) -> PoolGcResponse {
    collect_pool(store, pool_id, &context(now_ms)).await
}

async fn delete_namespace<S: ObjectStore>(store: &S, namespace_id: &NamespaceId, now_ms: u64) {
    crate::test_support::NamespaceFixture::new(store, namespace_id, &context(now_ms))
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete the namespace");
}

/// Writes the records of a session that `namespace_id` created and that
/// expires at `expires_at_ms`, with `outcome`'s record when it's set, under
/// the prefix of `pool_id`.
async fn plant_session<S: ObjectStore>(
    store: &S,
    pool_id: &PoolId,
    namespace_id: &NamespaceId,
    expires_at_ms: u64,
    outcome: Option<UploadOutcome>,
) -> UploadSessionPayload {
    let session = crate::test_support::proxied_upload_session(namespace_id, expires_at_ms);
    crate::test_support::write_upload_records(store, pool_id, &session, outcome).await;
    session
}

/// Returns the outcome of a session completed at `completed_at_ms`.
fn completed(completed_at_ms: u64) -> UploadOutcome {
    UploadOutcome::Completed {
        completed_at_ms,
        content_ref: ContentRef::blob_v1(ContentId::generate(), b"x", ChecksumAlgorithm::Crc64nvme),
    }
}

/// Returns whether the session record of `session` exists under the prefix
/// of `pool_id`.
async fn session_exists<S: ObjectStore>(
    store: &S,
    pool_id: &PoolId,
    session: &UploadSessionPayload,
) -> bool {
    exists(store, &upload_session(pool_id, &session.upload_id)).await
}

/// Runs the collection of `pool_id` with a read working memory of
/// `limit_bytes`, and checks that the collection releases the memory it held.
async fn gc_within<S: ObjectStore>(
    store: &S,
    pool_id: &PoolId,
    limit_bytes: usize,
    clock: &OperationClock,
) -> crate::error::Result<PoolGcResponse> {
    let memory = Arc::new(crate::cache::ReadWorkingMemory::new(limit_bytes, None));
    let report = crate::PoolStore {
        segment_cache: Arc::new(root_scan_cache(Arc::clone(&memory))),
        ..crate::test_support::pool_store(store, pool_id)
    }
    .gc(None, clock)
    .await;
    assert_eq!(memory.in_use(), 0, "the collection released what it held");
    report
}

#[tokio::test]
async fn content_that_only_a_member_view_refers_to_stays_through_every_generation() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix("namespaces/fork/"),
    );
    let (pool_id, members) = pool(&store, &["first", "fork", "grandchild"]).await;
    let mut referenced = Vec::new();
    for (member, number) in members[1..].iter().zip(1..) {
        let content_id = numbered_content_id(number);
        referenced.push(plant(&store, &pool_id, &content_id).await);
        publish_layout(&store, member, &content_id).await;
    }
    let unreferenced = plant(&store, &pool_id, &numbered_content_id(3)).await;
    let aged = aged_context(&store, &members).await;
    store.reset();
    let report = collect_pool(&store, &pool_id, &aged).await;
    // A member's roots cost one listing of its manifests, to find its newest
    // manifest, and one snapshot listing, and the pool's collection lists
    // nothing else under the member's prefix.
    let listings = store
        .take()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::List { prefix } => Some(prefix),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        listings,
        [
            metadata_manifest_prefix(&members[1]),
            snapshot_prefix(&members[1])
        ]
    );
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
    for key in &referenced {
        assert!(exists(&store, key).await, "`{key}`");
    }
}

#[tokio::test]
async fn content_that_only_a_fork_tail_refers_to_stays() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (pool_id, members) = pool(&store, &["first", "fork"]).await;
    // The fork's upload is stored under the pool's prefix.
    let content_ref = put_file_content(&store, &members[1], "/tail", b"tail").await;
    let referenced = content_blob(&pool_id, &content_ref.content_id);
    assert!(exists(&store, &referenced).await);
    let unreferenced = plant(&store, &pool_id, &ContentId::generate()).await;
    let aged = aged_context(&store, &members).await;
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
    assert!(exists(&store, &referenced).await);
}

#[tokio::test]
async fn content_that_only_a_deleted_fork_refers_to_stays_until_the_fork_is_retired() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (pool_id, members) = pool(&store, &["first", "fork"]).await;
    let fork = &members[1];
    let content_id = ContentId::generate();
    let referenced = plant(&store, &pool_id, &content_id).await;
    publish_layout(&store, fork, &content_id).await;
    crate::test_support::NamespaceFixture::new(&store, fork, &context(1_000))
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete the fork");
    let aged = aged_context(&store, &members).await;
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 0);
    assert!(exists(&store, &referenced).await);
    let retired = crate::test_support::namespace_store(&store, fork)
        .gc(&aged.clock())
        .await
        .expect("the fork's retirement");
    assert_eq!(retired.deleted_snapshots_by_owner.fork, 1);
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &referenced).await);
}

/// Checks that the collection doesn't read a fork whose member entry isn't
/// listed yet, as for a fork installed during the collection: the fork's
/// content is the content of the source manifest that the source's snapshot
/// keeps.
#[tokio::test]
async fn a_fork_installed_during_the_collection_is_not_read() {
    let directory = tempdir().expect("directory");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let (pool_id, members) = pool(&store, &["first", "fork"]).await;
    let first = &members[0];
    let pending = NamespaceId::parse("pending").expect("namespace");
    let namespace = crate::test_support::namespace_store(&store, first);
    namespace
        .create_snapshot_record(
            SnapshotOwner::Fork {
                target_namespace_id: pending.clone(),
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context(1_000).clock(),
        )
        .await
        .expect("fork snapshot");
    let unreferenced = plant(&store, &pool_id, &ContentId::generate()).await;
    let aged = aged_context(&store, &members).await;
    let recording = RecordingStore::new(
        Arc::clone(&store),
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&pending)),
    );
    let report = collect_pool(&recording, &pool_id, &aged).await;
    assert_eq!(recording.count(OperationSelector::Any), 0);
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
}

#[tokio::test]
async fn an_unreadable_member_skips_the_content_sweep_and_the_collection_sweeps_temporary_objects()
{
    for corrupt in [0, 1, 2] {
        let directory = tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let (pool_id, members) = pool(&store, &["first", "fork", "grandchild"]).await;
        let current = crate::test_support::namespace_store(&store, &members[corrupt])
            .load_current_manifest()
            .await
            .expect("current manifest");
        replace_object(&store, &current.object_key, Bytes::from_static(b"not json"))
            .await
            .expect("corrupt the member's manifest");
        let unreferenced = plant(&store, &pool_id, &ContentId::generate()).await;
        let temporary = temporary_object(&pool_id);
        store
            .put_if_absent(&temporary, Bytes::from_static(b"x"))
            .await
            .expect("temporary object");
        let aged = aged_context(&store, &members).await;
        let report = collect_pool(&store, &pool_id, &aged).await;
        assert_eq!(report.deleted.content_objects, 0, "`{}`", members[corrupt]);
        assert_eq!(
            report.skipped,
            [GcSkippedWork::ContentDeletionUnreadableMember]
        );
        assert!(exists(&store, &unreferenced).await);
        assert_eq!(report.deleted.temporary_objects, 1);
        assert!(!exists(&store, &temporary).await);
    }
}

#[tokio::test]
async fn a_collection_past_the_pool_roots_budget_skips_the_content_sweep() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (pool_id, members) = pool(&store, &["first", "fork"]).await;
    let unreferenced = plant(&store, &pool_id, &ContentId::generate()).await;
    let aged = aged_context(&store, &members).await;
    let timer = Arc::new(ManualClock::new(0));
    let late = OperationClock::start(aged.now_ms, timer.clone());
    timer.advance_ms(POOL_ROOTS_READ_BUDGET_MS + 1);
    let report = crate::test_support::pool_store(&store, &pool_id)
        .gc(None, &late)
        .await
        .expect("the late collection");
    assert_eq!(report.deleted.content_objects, 0);
    assert_eq!(report.skipped, [GcSkippedWork::ContentDeletionPoolBudget]);
    assert!(exists(&store, &unreferenced).await);
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 1);
    assert_eq!(report.skipped, []);
    assert!(!exists(&store, &unreferenced).await);
}

/// Checks that the collection deletes stale member entries as the listing
/// yields them, so a member list that wouldn't fit the read memory never
/// has to: 256 entries of namespaces without a manifest, each with a 128-byte
/// ID, under 32 KiB.
#[tokio::test]
async fn stale_member_entries_are_deleted_as_they_are_listed_within_the_read_memory() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (pool_id, members) = pool(&store, &["first", "fork"]).await;
    let limit_bytes = 32 * 1024;
    let stale = (0..limit_bytes / 128)
        .map(|index| NamespaceId::parse(format!("pending-{index:0120}")).expect("namespace"))
        .collect::<Vec<_>>();
    for namespace_id in &stale {
        crate::test_support::write_member_entry(&store, &pool_id, namespace_id).await;
    }
    let unreferenced = plant(&store, &pool_id, &ContentId::generate()).await;
    let aged = aged_context(&store, &members).await;
    let report = gc_within(&store, &pool_id, limit_bytes, &aged.clock())
        .await
        .expect("the live members fit the read memory");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
    for namespace_id in &stale {
        assert!(!exists(&store, &pool_member(&pool_id, namespace_id)).await);
    }
    for namespace_id in &members {
        assert!(exists(&store, &pool_member(&pool_id, namespace_id)).await);
    }
}

/// Checks that a deleted member that isn't eligible for retirement still
/// protects its content, and that its fork's content stays too.
#[tokio::test]
async fn a_deleted_member_protects_its_content_until_it_is_retired() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (pool_id, members) = pool(&store, &["first"]).await;
    let first = &members[0];
    let shared =
        put_file_content_with_id(&store, first, "/shared", b"shared", numbered_content_id(1)).await;
    let fork_id = NamespaceId::parse("fork").expect("namespace");
    fork(&store, first, &fork_id).await;
    let own = put_file_content_with_id(&store, first, "/own", b"own", numbered_content_id(2)).await;
    let forked = put_file_content_with_id(
        &store,
        &fork_id,
        "/forked",
        b"forked",
        numbered_content_id(3),
    )
    .await;
    crate::test_support::NamespaceFixture::new(&store, first, &context(1_000))
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete the member");
    let unreferenced = plant(&store, &pool_id, &numbered_content_id(4)).await;
    let aged = aged_context(&store, &[first.clone(), fork_id.clone()]).await;
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 1);
    assert_eq!(report.skipped, []);
    assert!(!exists(&store, &unreferenced).await);
    for content in [&shared, &own, &forked] {
        assert!(exists(&store, &content_blob(&pool_id, &content.content_id)).await);
    }
    let bytes = crate::test_support::namespace_store(&store, &fork_id)
        .load_current_metadata_view()
        .await
        .expect("the fork's view")
        .read_file_bytes(
            "/shared",
            None,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("the fork reads inherited content");
    assert_eq!(bytes.bytes, b"shared");
}

#[tokio::test]
async fn a_forks_content_is_stored_in_its_sources_pool_and_only_the_pools_collection_sweeps_it() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix("pools/"),
    );
    let (pool_id, members) = pool(&store, &["first", "fork"]).await;
    let (first, fork) = (&members[0], &members[1]);
    let first_content = put_file_content(&store, first, "/root", b"first").await;
    let fork_content = put_file_content(&store, fork, "/upload", b"upload").await;
    crate::test_support::NamespaceFixture::new(&store, fork, &context(1_000))
        .engine()
        .begin_upload()
        .await
        .expect("open an upload in the fork");
    assert_eq!(
        crate::test_support::load_pool_id(&store, fork).await,
        pool_id
    );
    let mut listed = store
        .list_prefix(&loonfs_objectstore::keys::content_prefix(&pool_id))
        .await
        .expect("list the pool's content");
    listed.sort();
    let mut kept = [&first_content, &fork_content]
        .map(|content_ref| content_blob(&pool_id, &content_ref.content_id));
    kept.sort();
    assert_eq!(listed, kept);
    assert_eq!(
        store
            .list_prefix(&loonfs_objectstore::keys::upload_session_prefix(&pool_id))
            .await
            .expect("list the pool's uploads")
            .len(),
        1
    );
    let unreferenced = plant(&store, &pool_id, &numbered_content_id(1)).await;
    let aged = aged_context(&store, &members).await;
    // Neither the collection of a namespace with a fork nor the collection of
    // a fork makes a request under the pool's prefix.
    for namespace_id in [first, fork] {
        store.reset();
        collect(&store, namespace_id, aged.now_ms).await;
        assert_eq!(store.take(), [], "`{namespace_id}`");
    }
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
    for key in &kept {
        assert!(exists(&store, key).await, "`{key}`");
    }
}

#[tokio::test]
async fn a_pool_sweeps_a_live_members_sessions_by_the_members_status() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (pool_id, members) = pool(&store, &["first", "fork"]).await;
    let (first, fork) = (&members[0], &members[1]);
    let kept = put_file_content(&store, fork, "/kept", b"kept").await;
    let unreferenced = plant(&store, &pool_id, &ContentId::generate()).await;
    delete_namespace(&store, first, 1_000).await;
    let base_ms = aged_context(&store, &members).await.now_ms;
    let fork_open = plant_session(&store, &pool_id, fork, base_ms, None).await;
    let fork_completed = UploadSessionPayload {
        content_id: kept.content_id.clone(),
        ..crate::test_support::proxied_upload_session(fork, base_ms)
    };
    let outcome = UploadOutcome::Completed {
        completed_at_ms: base_ms,
        content_ref: kept.clone(),
    };
    crate::test_support::write_upload_records(&store, &pool_id, &fork_completed, Some(outcome))
        .await;
    let first_open = plant_session(&store, &pool_id, first, base_ms, None).await;
    let first_completed =
        plant_session(&store, &pool_id, first, base_ms, Some(completed(base_ms))).await;

    // The fork's snapshot keeps the deleted source from retirement, so the
    // source's sessions stay, and the fork's sessions follow the fork's rules.
    collect_pool_at(&store, &pool_id, base_ms + GC_GRACE_WINDOW_MS).await;
    assert!(exists(&store, &upload_outcome(&pool_id, &fork_open.upload_id)).await);
    assert!(!exists(&store, &upload_outcome(&pool_id, &first_open.upload_id)).await);
    let later_ms = base_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS;
    let report = collect_pool_at(&store, &pool_id, later_ms).await;
    assert_eq!(report.deleted.upload_sessions, 2);
    assert!(!session_exists(&store, &pool_id, &fork_open).await);
    assert!(!session_exists(&store, &pool_id, &fork_completed).await);
    assert!(session_exists(&store, &pool_id, &first_open).await);
    assert!(session_exists(&store, &pool_id, &first_completed).await);
    assert!(!exists(&store, &upload_outcome(&pool_id, &first_open.upload_id)).await);
    // Without its session, the fork's content stays because a fork revision
    // refers to it.
    collect_pool_at(&store, &pool_id, later_ms).await;
    assert!(exists(&store, &content_blob(&pool_id, &kept.content_id)).await);
    assert!(!exists(&store, &unreferenced).await);
}

#[tokio::test]
async fn a_deleted_forks_sessions_stay_until_the_fork_is_retired() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (pool_id, members) = pool(&store, &["first", "fork"]).await;
    let fork = &members[1];
    // The fork is deleted after the sessions' completed-content grace and
    // abort grace have passed, so only the fork's tombstone keeps them.
    let deleted_at_ms = 1_000 + CONTENT_RECLAMATION_GRACE_WINDOW_MS;
    let aborted = UploadOutcome::Aborted {
        aborted_at_ms: 1_000,
    };
    let mut sessions = vec![
        plant_session(&store, &pool_id, fork, 1_000, Some(completed(1_000))).await,
        plant_session(&store, &pool_id, fork, 1_000, Some(aborted)).await,
    ];
    delete_namespace(&store, fork, deleted_at_ms).await;
    let report = collect_pool_at(&store, &pool_id, deleted_at_ms + 1).await;
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.retained.upload_session_window, 2);

    let aged = aged_context(&store, &members).await;
    // A completion within the grace would keep the session in an active
    // namespace.
    sessions.push(
        plant_session(
            &store,
            &pool_id,
            fork,
            aged.now_ms,
            Some(completed(aged.now_ms)),
        )
        .await,
    );
    let retired = collect(&store, fork, aged.now_ms).await;
    assert_eq!(retired.deleted_snapshots_by_owner.fork, 1);
    let report = collect_pool_at(&store, &pool_id, aged.now_ms).await;
    assert_eq!(report.deleted.upload_sessions, 3);
    for session in &sessions {
        assert!(!session_exists(&store, &pool_id, session).await);
    }
}

#[tokio::test]
async fn an_unreadable_member_leaves_the_sessions_of_unread_namespaces_undecided() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    // The collection reads the members in the order of their entries' keys,
    // so it reads `a` and `b`, stops at `c`, and doesn't read `d`.
    let (pool_id, members) = pool(&store, &["a", "b", "c", "d"]).await;
    let c_manifest = crate::test_support::namespace_store(&store, &members[2])
        .load_current_manifest()
        .await
        .expect("current manifest");
    replace_object(
        &store,
        &c_manifest.object_key,
        Bytes::from_static(b"not json"),
    )
    .await
    .expect("corrupt the manifest of `c`");
    let mut sessions = Vec::new();
    for member in &members {
        sessions.push(plant_session(&store, &pool_id, member, 1_000, Some(completed(1_000))).await);
    }
    let aged = aged_context(&store, &members).await;
    let report = collect_pool_at(&store, &pool_id, aged.now_ms).await;
    assert_eq!(report.deleted.upload_sessions, 2);
    assert_eq!(report.retained.upload_session_undecided, 2);
    for (session, kept) in sessions.iter().zip([false, false, true, true]) {
        assert_eq!(
            session_exists(&store, &pool_id, session).await,
            kept,
            "`{}`",
            session.namespace_id
        );
    }
}

#[tokio::test]
async fn a_session_completed_in_a_fork_installed_during_the_collection_keeps_its_grace() {
    let directory = tempdir().expect("directory");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let (pool_id, members) = pool(&inner, &["first"]).await;
    let first = &members[0];
    let fork_id = NamespaceId::parse("fork").expect("namespace");
    // The collection lists the temporary objects after it lists the upload
    // sessions and reads the members.
    let store = BlockingStore::new(
        inner,
        KeyPredicate::exact(temporary_prefix(&pool_id)),
        OperationSelector::List,
    );
    store.block_next();
    let (report, upload_id) = tokio::join!(collect_pool_at(&store, &pool_id, 1_000), async {
        store.wait_until_blocked().await;
        fork(store.inner(), first, &fork_id).await;
        let (upload_id, _) =
            complete_upload_for_gc(store.inner(), &fork_id, b"late", &context(1_000)).await;
        store.release();
        upload_id
    });
    // The collection listed the sessions before the session existed, so it leaves
    // the session to the next collection.
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.retained.upload_session_window, 0);
    assert!(exists(store.inner(), &upload_session(&pool_id, &upload_id)).await);
    // The fork can still commit the session's content, so the next collection keeps
    // the session for its completed-content grace.
    let report = collect_pool_at(store.inner(), &pool_id, 1_000).await;
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.retained.upload_session_window, 1);
    assert!(exists(store.inner(), &upload_session(&pool_id, &upload_id)).await);
}

#[tokio::test]
async fn a_cached_completed_session_follows_its_members_status() {
    for retire_the_fork in [false, true] {
        let directory = tempdir().expect("directory");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        let (pool_id, members) = pool(&store, &["first", "fork"]).await;
        let (first, fork) = (&members[0], &members[1]);
        let now_ms = aged_context(&store, &members).await.now_ms;
        let session = plant_session(&store, &pool_id, fork, now_ms, Some(completed(now_ms))).await;
        let cache = CompletedUploadSessionCache::unshared(1024 * 1024);
        crate::test_support::pool_store(&store, &pool_id)
            .gc(Some(&cache), &context(now_ms).clock())
            .await
            .expect("the collection that caches the session");
        assert!(cache.get(&session.upload_id).is_some());
        // A retired fork's session is deleted at once. A live fork's session
        // is deleted after its completed-content grace, although the deleted
        // source is retained.
        let later_ms = if retire_the_fork {
            delete_namespace(&store, fork, 1_000).await;
            collect(&store, fork, now_ms).await;
            now_ms
        } else {
            delete_namespace(&store, first, 1_000).await;
            now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS
        };
        store.reset();
        let report = crate::test_support::pool_store(&store, &pool_id)
            .gc(Some(&cache), &context(later_ms).clock())
            .await
            .expect("the collection that decides the session");
        assert_eq!(
            report.deleted.upload_sessions, 1,
            "retired: {retire_the_fork}"
        );
        assert!(!store
            .take_get_keys()
            .contains(&upload_outcome(&pool_id, &session.upload_id)));
    }
}
