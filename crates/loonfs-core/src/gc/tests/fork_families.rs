//! The pass on a family root, which protects content with the roots of every
//! member of its fork family.

use super::*;
use crate::limits::FAMILY_ROOTS_READ_BUDGET_MS;
use crate::manifest::tests::{
    build_namespace_manifest_from_metadata_state, write_namespace_manifest, ManifestMetadataSource,
};
use crate::metadata::MetadataStateBuilder;
use crate::namespace::state::NamespaceReadState;
use crate::segment::MetadataRunPolicy;
use crate::time::OperationClock;
use loonfs_objectstore::keys::{content_blob, temporary_object};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{ChangeSeq, ContentExtent, ContentId, ContentLayout, ExtentObject};

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
    crate::test_support::NamespaceFixture::new(store, source, &context(1_000))
        .engine()
        .fork_namespace(target, &loonfs_test_support::test_actor(), None)
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

/// Runs the pass of `namespace_id` with a read working memory of
/// `limit_bytes`, and checks that the pass releases the memory it held.
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
    assert_eq!(memory.in_use(), 0, "the pass released what it held");
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
        .expect("the root's pass");
    // A member's roots cost one pin listing and one manifest listing, and the
    // root's pass lists nothing else under the member's prefix.
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
        [pin_prefix(&family[1]), metadata_manifest_prefix(&family[1])]
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
        .expect("the root's pass");
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
        .expect("the root's pass before the fork retires");
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
        .expect("the root's pass after the fork retires");
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
    crate::test_support::namespace_store(&store, root)
        .create_pin(
            PinOwner::Fork {
                target_namespace_id: NamespaceId::parse("pending").expect("namespace"),
            },
            &context(1_000).clock(),
        )
        .await
        .expect("fork pin");
    let unreferenced = plant(&store, root, &ContentId::generate()).await;
    let aged = aged_context(&store, &family).await;
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the root's pass");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
}

#[tokio::test]
async fn an_unreadable_member_skips_the_content_sweep_and_the_pass_sweeps_the_other_families() {
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
            .expect("the root's pass continues");
        assert_eq!(report.deleted.content_objects, 0, "`{}`", family[corrupt]);
        assert!(exists(&store, &unreferenced).await);
        assert_eq!(report.deleted.temporary_objects, 1);
        assert!(!exists(&store, &temporary).await);
        assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    }
}

#[tokio::test]
async fn a_pass_past_the_family_roots_budget_skips_the_content_sweep() {
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
        .expect("the late pass");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(exists(&store, &unreferenced).await);
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("a pass within the budget");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(!exists(&store, &unreferenced).await);
}

#[tokio::test]
async fn a_pass_fails_when_a_members_fork_targets_do_not_fit_the_read_memory() {
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
        crate::test_support::namespace_store(&store, fork)
            .create_pin(
                PinOwner::Fork {
                    target_namespace_id,
                },
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
        .expect("a pass with enough read memory");
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
        .expect("the deleted root's pass");
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
async fn a_fork_pass_lists_no_content_and_no_upload_sessions() {
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
        .expect("the fork's pass");
    assert_eq!(store.take(), []);
    assert_eq!(report.deleted.content_objects, 0);
    assert_eq!(report.deleted.upload_sessions, 0);
    let report = crate::test_support::namespace_store(&store, root)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("the root's pass");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(exists(&store, &content_blob(root, &content_ref.content_id)).await);
}
