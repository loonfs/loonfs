//! The collection on a family root, which protects content with the roots of every
//! member of its fork family.

use super::*;
use crate::cache::CompletedUploadSessionCache;
use crate::limits::FAMILY_ROOTS_READ_BUDGET_MS;
use crate::manifest::tests::{
    build_namespace_manifest_from_metadata_state, write_namespace_manifest, ManifestMetadataSource,
};
use crate::metadata::MetadataStateBuilder;
use crate::namespace::state::NamespaceReadState;
use crate::segment::MetadataRunPolicy;
use crate::time::OperationClock;
use loonfs_objectstore::keys::{
    content_blob, temporary_object, temporary_prefix, upload_outcome, upload_session,
};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::control::UploadSessionPayload;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{
    ChangeSeq, ChecksumAlgorithm, ContentExtent, ContentId, ContentLayout, ExtentObject,
};

/// Creates `names[0]`, then forks each later name from the one before it,
/// and returns the namespace IDs.
async fn family<S: ObjectStore + Clone>(store: S, names: &[&str]) -> Vec<NamespaceId> {
    let setup = context(1_000);
    let members = names
        .iter()
        .map(|name| NamespaceId::parse(*name).expect("namespace"))
        .collect::<Vec<_>>();
    crate::test_support::NamespaceFixture::new(store.clone(), &members[0], &setup)
        .create()
        .await
        .expect("create the family root");
    for pair in members.windows(2) {
        fork(store.clone(), &pair[0], &pair[1]).await;
    }
    members
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

/// Writes an object under the prefix of `family_root` and returns its key.
async fn plant<S: ObjectStore>(
    store: &S,
    family_root: &NamespaceId,
    content_id: &ContentId,
) -> String {
    let key = content_blob(family_root, content_id);
    store
        .put_if_absent(&key, Bytes::from_static(b"x"))
        .await
        .expect("content object");
    key
}

/// Publishes the next manifest of `namespace_id` with one more delta run,
/// whose layout row, created by `namespace_id`, refers to the object of
/// `content_id` under the prefix of its family root.
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
    head.seq = ChangeSeq(head.seq.0 + 1);
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
/// every namespace in `family`.
async fn aged_context<S: ObjectStore>(store: &S, family: &[NamespaceId]) -> WriterAt {
    let mut now_ms = 0;
    for namespace_id in family {
        now_ms = now_ms.max(now_after_newest_object(store, namespace_id, GRACE_MS + 1).await);
    }
    context(now_ms)
}

async fn exists<S: ObjectStore>(store: &S, key: &str) -> bool {
    store.head(key).await.expect("head").is_some()
}

/// Runs the collection of `namespace_id` at `now_ms`.
async fn collect<S: ObjectStore>(store: &S, namespace_id: &NamespaceId, now_ms: u64) -> GcResponse {
    crate::test_support::namespace_store(store, namespace_id)
        .gc(None, &options(), &context(now_ms).clock())
        .await
        .expect("collection")
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
/// the prefix of `family_root`.
async fn plant_session<S: ObjectStore>(
    store: &S,
    family_root: &NamespaceId,
    namespace_id: &NamespaceId,
    expires_at_ms: u64,
    outcome: Option<UploadOutcome>,
) -> UploadSessionPayload {
    let session = crate::test_support::proxied_upload_session(namespace_id, expires_at_ms);
    crate::test_support::write_upload_records(store, family_root, &session, outcome).await;
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
/// of `family_root`.
async fn session_exists<S: ObjectStore>(
    store: &S,
    family_root: &NamespaceId,
    session: &UploadSessionPayload,
) -> bool {
    exists(store, &upload_session(family_root, &session.upload_id)).await
}

/// Runs the collection of `namespace_id` with a read working memory of
/// `limit_bytes`, and checks that the collection releases the memory it held.
async fn gc_within<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    limit_bytes: usize,
    clock: &OperationClock,
) -> crate::error::Result<GcResponse> {
    let memory = Arc::new(crate::cache::ReadWorkingMemory::new(limit_bytes, None));
    let report = crate::NamespaceStore {
        segment_cache: Arc::new(root_scan_cache(Arc::clone(&memory))),
        ..crate::test_support::namespace_store(store, namespace_id)
    }
    .gc(None, &options(), clock)
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
    let family = family(&store, &["root", "fork", "grandchild"]).await;
    let root = &family[0];
    let mut referenced = Vec::new();
    for member in &family[1..] {
        let content_id = ContentId::generate();
        referenced.push(plant(&store, root, &content_id).await);
        publish_layout(&store, member, &content_id).await;
    }
    let unreferenced = plant(&store, root, &ContentId::generate()).await;
    let aged = aged_context(&store, &family).await;
    store.reset();
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the root's collection");
    // A member's roots cost one pin listing, and the root's collection lists
    // nothing else under the member's prefix.
    let listings = store
        .take()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::List { prefix } => Some(prefix),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(listings, [pin_prefix(&family[1])]);
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
    let family = family(&store, &["root", "fork"]).await;
    let root = &family[0];
    // The fork's upload is stored under the family root's prefix.
    let content_ref = put_file_content(&store, &family[1], "/tail", b"tail").await;
    let referenced = content_blob(root, &content_ref.content_id);
    assert!(exists(&store, &referenced).await);
    let unreferenced = plant(&store, root, &ContentId::generate()).await;
    let aged = aged_context(&store, &family).await;
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the root's collection");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
    assert!(exists(&store, &referenced).await);
}

#[tokio::test]
async fn content_that_only_a_deleted_fork_refers_to_stays_until_the_fork_is_retired() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let family = family(&store, &["root", "fork"]).await;
    let (root, fork) = (&family[0], &family[1]);
    let content_id = ContentId::generate();
    let referenced = plant(&store, root, &content_id).await;
    publish_layout(&store, fork, &content_id).await;
    crate::test_support::NamespaceFixture::new(&store, fork, &context(1_000))
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete the fork");
    let aged = aged_context(&store, &family).await;
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the root's collection before the fork retires");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(exists(&store, &referenced).await);
    let retired = crate::test_support::namespace_store(&store, fork)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the fork's retirement");
    assert_eq!(retired.deleted_checkpoints_by_owner.fork, 1);
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the root's collection after the fork retires");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &referenced).await);
}

#[tokio::test]
async fn a_fork_pin_whose_target_has_no_manifest_is_skipped() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let family = family(&store, &["root", "fork"]).await;
    let root = &family[0];
    // An installation that hasn't finished has written its pin and nothing
    // else.
    let namespace = crate::test_support::namespace_store(&store, root);
    namespace
        .create_pin(
            PinOwner::Fork {
                target_namespace_id: NamespaceId::parse("pending").expect("namespace"),
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context(1_000).clock(),
        )
        .await
        .expect("fork pin");
    let unreferenced = plant(&store, root, &ContentId::generate()).await;
    let aged = aged_context(&store, &family).await;
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the root's collection");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
}

#[tokio::test]
async fn an_unreadable_member_skips_the_content_sweep_and_the_collection_sweeps_the_other_families()
{
    // The fork's pin is on the root, so the root's pin cleanup reads the
    // fork's manifest too. The grandchild's pin is on the fork.
    for corrupt in [1, 2] {
        let directory = tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let family = family(&store, &["root", "fork", "grandchild"]).await;
        let root = &family[0];
        let current = crate::test_support::namespace_store(&store, &family[corrupt])
            .load_current_manifest()
            .await
            .expect("current manifest");
        replace_object(&store, &current.object_key, Bytes::from_static(b"not json"))
            .await
            .expect("corrupt the member's manifest");
        let unreferenced = plant(&store, root, &ContentId::generate()).await;
        let temporary = temporary_object(root);
        store
            .put_if_absent(&temporary, Bytes::from_static(b"x"))
            .await
            .expect("temporary object");
        let aged = aged_context(&store, &family).await;
        let report = crate::test_support::namespace_store(&store, root)
            .gc(None, &options(), &aged.clock())
            .await
            .expect("the root's collection continues");
        assert_eq!(report.deleted.content_objects, 0, "`{}`", family[corrupt]);
        assert!(exists(&store, &unreferenced).await);
        assert_eq!(report.deleted.temporary_objects, 1);
        assert!(!exists(&store, &temporary).await);
        assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    }
}

#[tokio::test]
async fn a_collection_past_the_family_roots_budget_skips_the_content_sweep() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let family = family(&store, &["root", "fork"]).await;
    let root = &family[0];
    let unreferenced = plant(&store, root, &ContentId::generate()).await;
    let aged = aged_context(&store, &family).await;
    let timer = Arc::new(ManualClock::new(0));
    let late = OperationClock::start(aged.now_ms, timer.clone());
    timer.advance_ms(FAMILY_ROOTS_READ_BUDGET_MS + 1);
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &late)
        .await
        .expect("the late collection");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(exists(&store, &unreferenced).await);
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("a collection within the budget");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
}

#[tokio::test]
async fn a_collection_fails_when_a_members_fork_targets_do_not_fit_the_read_memory() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let family = family(&store, &["root", "fork"]).await;
    let (root, fork) = (&family[0], &family[1]);
    let limit_bytes = 32 * 1024;
    gc_within(
        &store,
        root,
        limit_bytes,
        &aged_context(&store, &family).await.clock(),
    )
    .await
    .expect("the family's roots fit the read memory");
    // Each target is an installation that hasn't finished. The walk keeps a
    // copy of each target's 128-byte ID in its members and another in its
    // queue, so the targets alone need more than the read memory.
    for index in 0..limit_bytes / 128 {
        let target_namespace_id =
            NamespaceId::parse(format!("pending-{index:0120}")).expect("namespace");
        let namespace = crate::test_support::namespace_store(&store, fork);
        namespace
            .create_pin(
                PinOwner::Fork {
                    target_namespace_id,
                },
                crate::test_support::folded_manifest(&namespace)
                    .await
                    .expect("fold the WAL tail"),
                &context(1_000).clock(),
            )
            .await
            .expect("fork pin");
    }
    let unreferenced = plant(&store, root, &ContentId::generate()).await;
    let aged = aged_context(&store, &family).await;
    let error = gc_within(&store, root, limit_bytes, &aged.clock())
        .await
        .expect_err("the fork's targets don't fit the read memory");
    assert_matches!(
        error,
        CoreError::LiveSetExceedsReadMemory { limit, .. } if limit == limit_bytes
    );
    assert!(exists(&store, &unreferenced).await);
    let report = gc_within(&store, root, 64 * 1024 * 1024, &aged.clock())
        .await
        .expect("a collection with enough read memory");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
}

#[tokio::test]
async fn a_deleted_root_with_a_live_fork_sweeps_unreferenced_content() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let family = family(&store, &["root"]).await;
    let root = &family[0];
    let shared = put_file_content(&store, root, "/shared", b"shared").await;
    let fork_id = NamespaceId::parse("fork").expect("namespace");
    fork(&store, root, &fork_id).await;
    crate::test_support::NamespaceFixture::new(&store, root, &context(1_000))
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete the root");
    let unreferenced = plant(&store, root, &ContentId::generate()).await;
    let aged = aged_context(&store, &[root.clone(), fork_id.clone()]).await;
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the deleted root's collection");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
    assert!(exists(&store, &content_blob(root, &shared.content_id)).await);
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
async fn a_fork_collection_lists_no_content_and_no_upload_sessions() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::new(|key| key.contains("/content/") || key.contains("/uploads/")),
    );
    let family = family(&store, &["root", "fork"]).await;
    let (root, fork) = (&family[0], &family[1]);
    let content_ref = put_file_content(&store, fork, "/upload", b"upload").await;
    crate::test_support::NamespaceFixture::new(&store, fork, &context(1_000))
        .engine()
        .begin_upload()
        .await
        .expect("open an upload in the fork");
    // The fork's content and upload records are stored under the family
    // root's prefix.
    assert!(exists(&store, &content_blob(root, &content_ref.content_id)).await);
    for prefix in [
        loonfs_objectstore::keys::content_prefix(fork),
        loonfs_objectstore::keys::upload_session_prefix(fork),
    ] {
        assert_eq!(
            store.list_prefix(&prefix).await.expect("list"),
            Vec::<String>::new()
        );
    }
    let aged = aged_context(&store, &family).await;
    store.reset();
    let report = crate::test_support::namespace_store(&store, fork)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the fork's collection");
    assert_eq!(store.take(), []);
    assert_eq!(report.deleted.content_objects, 0);
    assert_eq!(report.deleted.upload_sessions, 0);
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the root's collection");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(exists(&store, &content_blob(root, &content_ref.content_id)).await);
}

#[tokio::test]
async fn a_deleted_roots_collection_sweeps_a_live_forks_sessions_by_the_forks_status() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let family = family(&store, &["root", "fork"]).await;
    let (root, fork) = (&family[0], &family[1]);
    let kept = put_file_content(&store, fork, "/kept", b"kept").await;
    let unreferenced = plant(&store, root, &ContentId::generate()).await;
    delete_namespace(&store, root, 1_000).await;
    let base_ms = aged_context(&store, &family).await.now_ms;
    let fork_open = plant_session(&store, root, fork, base_ms, None).await;
    let fork_completed = UploadSessionPayload {
        content_id: kept.content_id.clone(),
        ..crate::test_support::proxied_upload_session(fork, base_ms)
    };
    let outcome = UploadOutcome::Completed {
        completed_at_ms: base_ms,
        content_ref: kept.clone(),
    };
    crate::test_support::write_upload_records(&store, root, &fork_completed, Some(outcome)).await;
    let root_open = plant_session(&store, root, root, base_ms, None).await;
    let root_completed = plant_session(&store, root, root, base_ms, Some(completed(base_ms))).await;

    // The fork's pin keeps the root from retirement, so the root's own
    // sessions stay, and the fork's sessions follow the fork's rules.
    collect(&store, root, base_ms + GRACE_MS).await;
    assert!(exists(&store, &upload_outcome(root, &fork_open.upload_id)).await);
    assert!(!exists(&store, &upload_outcome(root, &root_open.upload_id)).await);
    let later_ms = base_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS;
    let report = collect(&store, root, later_ms).await;
    assert_eq!(report.deleted.upload_sessions, 2);
    assert!(!session_exists(&store, root, &fork_open).await);
    assert!(!session_exists(&store, root, &fork_completed).await);
    assert!(session_exists(&store, root, &root_open).await);
    assert!(session_exists(&store, root, &root_completed).await);
    assert!(!exists(&store, &upload_outcome(root, &root_open.upload_id)).await);
    // Without its session, the fork's content stays because a fork revision
    // refers to it.
    collect(&store, root, later_ms).await;
    assert!(exists(&store, &content_blob(root, &kept.content_id)).await);
    assert!(!exists(&store, &unreferenced).await);
}

#[tokio::test]
async fn a_deleted_forks_sessions_stay_until_the_fork_is_retired() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let family = family(&store, &["root", "fork"]).await;
    let (root, fork) = (&family[0], &family[1]);
    // The fork is deleted after the sessions' completed-content grace and
    // abort grace have passed, so only the fork's tombstone keeps them.
    let deleted_at_ms = 1_000 + CONTENT_RECLAMATION_GRACE_WINDOW_MS;
    let aborted = UploadOutcome::Aborted {
        aborted_at_ms: 1_000,
    };
    let mut sessions = vec![
        plant_session(&store, root, fork, 1_000, Some(completed(1_000))).await,
        plant_session(&store, root, fork, 1_000, Some(aborted)).await,
    ];
    delete_namespace(&store, fork, deleted_at_ms).await;
    let report = collect(&store, root, deleted_at_ms + 1).await;
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.retained.upload_session_window, 2);

    let aged = aged_context(&store, &family).await;
    // A completion within the grace would keep the session in an active
    // namespace.
    sessions.push(
        plant_session(
            &store,
            root,
            fork,
            aged.now_ms,
            Some(completed(aged.now_ms)),
        )
        .await,
    );
    let retired = collect(&store, fork, aged.now_ms).await;
    assert_eq!(retired.deleted_checkpoints_by_owner.fork, 1);
    let report = collect(&store, root, aged.now_ms).await;
    assert_eq!(report.deleted.upload_sessions, 3);
    for session in &sessions {
        assert!(!session_exists(&store, root, session).await);
    }
}

#[tokio::test]
async fn an_unreadable_member_leaves_the_sessions_of_unread_namespaces_undecided() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    // Each name is forked from the one before it, so the walk finds `c`
    // only through the pins of `b`.
    let family = family(&store, &["root", "a", "b", "c"]).await;
    let root = &family[0];
    let b_manifest = crate::test_support::namespace_store(&store, &family[2])
        .load_current_manifest()
        .await
        .expect("current manifest");
    replace_object(
        &store,
        &b_manifest.object_key,
        Bytes::from_static(b"not json"),
    )
    .await
    .expect("corrupt the manifest of `b`");
    let mut sessions = Vec::new();
    for member in &family {
        sessions.push(plant_session(&store, root, member, 1_000, Some(completed(1_000))).await);
    }
    let aged = aged_context(&store, &family).await;
    let report = collect(&store, root, aged.now_ms).await;
    assert_eq!(report.deleted.upload_sessions, 2);
    assert_eq!(report.retained.upload_session_undecided, 2);
    for (session, kept) in sessions.iter().zip([false, false, true, true]) {
        assert_eq!(
            session_exists(&store, root, session).await,
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
    let root = family(&inner, &["root"]).await.remove(0);
    let fork_id = NamespaceId::parse("fork").expect("namespace");
    // The collection lists the temporary objects after it lists the upload sessions
    // and walks the fork family.
    let store = BlockingStore::new(
        inner,
        KeyPredicate::exact(temporary_prefix(&root)),
        OperationSelector::List,
    );
    store.block_next();
    let (report, upload_id) = tokio::join!(collect(&store, &root, 1_000), async {
        store.wait_until_blocked().await;
        fork(store.inner(), &root, &fork_id).await;
        let (upload_id, _) =
            complete_upload_for_gc(store.inner(), &fork_id, b"late", &context(1_000)).await;
        store.release();
        upload_id
    });
    // The collection listed the sessions before the session existed, so it leaves
    // the session to the next collection.
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.retained.upload_session_window, 0);
    assert!(exists(store.inner(), &upload_session(&root, &upload_id)).await);
    // The fork can still commit the session's content, so the next collection keeps
    // the session for its completed-content grace.
    let report = collect(store.inner(), &root, 1_000).await;
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.retained.upload_session_window, 1);
    assert!(exists(store.inner(), &upload_session(&root, &upload_id)).await);
}

#[tokio::test]
async fn a_cached_completed_session_follows_its_members_status() {
    for retire_the_fork in [false, true] {
        let directory = tempdir().expect("directory");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        let family = family(&store, &["root", "fork"]).await;
        let (root, fork) = (&family[0], &family[1]);
        let now_ms = aged_context(&store, &family).await.now_ms;
        let session = plant_session(&store, root, fork, now_ms, Some(completed(now_ms))).await;
        let cache = CompletedUploadSessionCache::unshared(1024 * 1024);
        crate::test_support::namespace_store(&store, root)
            .gc(Some(&cache), &options(), &context(now_ms).clock())
            .await
            .expect("the collection that caches the session");
        assert!(cache.get(&session.upload_id).is_some());
        // A retired fork's session is deleted at once. A live fork's session
        // is deleted after its completed-content grace, although the deleted
        // root is retained.
        let later_ms = if retire_the_fork {
            delete_namespace(&store, fork, 1_000).await;
            collect(&store, fork, now_ms).await;
            now_ms
        } else {
            delete_namespace(&store, root, 1_000).await;
            now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS
        };
        store.reset();
        let report = crate::test_support::namespace_store(&store, root)
            .gc(Some(&cache), &options(), &context(later_ms).clock())
            .await
            .expect("the collection that decides the session");
        assert_eq!(
            report.deleted.upload_sessions, 1,
            "retired: {retire_the_fork}"
        );
        assert!(!store
            .take_get_keys()
            .contains(&upload_outcome(root, &session.upload_id)));
    }
}
