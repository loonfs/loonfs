//! Grep root object-store loading and publication behavior.

#![allow(clippy::panic)]

use bytes::Bytes;
use loonfs::StoreFailureClass;
use loonfs_api::{ChangeSeq, NamespaceId, RunNo};
use loonfs_grep::keyspace::{manifest_key, manifests_prefix, root_key};
use loonfs_grep::root::{
    advance_grep_root, encode_grep_manifest, encode_grep_root, load_grep_root, seed_grep_root,
    GrepIndexState, GrepIndexStatus, GrepManifestObjectId, GrepManifestState, GrepRootError,
    GrepRootPointer,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, MetadataMapStore};

#[tokio::test]
async fn load_rejects_namespace_identity_mismatch() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let requested = namespace_id("requested");
    let wrong = root(namespace_id("other"), ChangeSeq(0));
    let manifest_object_id = GrepManifestObjectId::generate();
    let (manifest, manifest_bytes) = encode_grep_manifest(wrong)
        .expect("encode manifest")
        .into_parts();
    store
        .put(
            &manifest_key(&requested, &manifest_object_id),
            Bytes::from(manifest_bytes),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write mismatched manifest");
    let bytes = encode_grep_root(GrepRootPointer::new(
        requested.clone(),
        manifest_object_id,
        manifest.payload_checksum().to_owned(),
    ))
    .expect("pointer envelope")
    .into_bytes();
    let key = root_key(&requested);
    store
        .put(&key, Bytes::from(bytes), PutMode::CreateIfAbsent)
        .await
        .expect("write mismatched root");

    assert!(matches!(
        load_grep_root(&store, &requested).await,
        Err(GrepRootError::IdentityMismatch { expected, actual, .. })
            if expected.as_str() == "requested" && actual.as_str() == "other"
    ));
}

#[tokio::test]
async fn load_requires_the_root_etag_as_a_store_contract() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = namespace_id("missing-etag");
    seed_grep_root(&inner, &root(namespace_id.clone(), ChangeSeq(0)))
        .await
        .expect("seed root");
    let store = MetadataMapStore::without_etag(inner, KeyPredicate::exact(root_key(&namespace_id)));

    let error = load_grep_root(&store, &namespace_id)
        .await
        .expect_err("a loaded grep root requires its etag");
    assert!(matches!(
        error,
        GrepRootError::Store {
            class: StoreFailureClass::Other,
            message,
            ..
        } if message.contains("required grep-root etag")
    ));
}

#[tokio::test]
async fn seed_succeeds_once_and_second_seed_conflicts() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let state = root(namespace_id("docs"), ChangeSeq(0));

    seed_grep_root(&store, &state)
        .await
        .expect("first seed succeeds");
    assert!(matches!(
        seed_grep_root(&store, &state).await,
        Err(GrepRootError::Conflict { .. })
    ));
}

#[tokio::test]
async fn identical_state_publishes_a_fresh_manifest_object() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = namespace_id("docs");
    let successor = root(namespace_id.clone(), ChangeSeq(1));
    let seeded = seed_grep_root(&store, &root(namespace_id.clone(), ChangeSeq(0)))
        .await
        .expect("seed root");

    let first = advance_grep_root(&store, &seeded, &successor)
        .await
        .expect("first advance");
    let second = advance_grep_root(&store, &first, &successor)
        .await
        .expect("second advance over identical state");

    assert_eq!(
        first.manifest_envelope().payload_checksum(),
        second.manifest_envelope().payload_checksum(),
        "the two candidates carry the very same bytes"
    );
    assert_ne!(
        first.manifest_object_id(),
        second.manifest_object_id(),
        "and still occupy different objects"
    );
    for published in [&first, &second] {
        let object_key = manifest_key(&namespace_id, published.manifest_object_id());
        let stored = store
            .get(&object_key, None)
            .await
            .expect("read a published manifest")
            .expect("a published manifest is durable at the key the pointer names");
        let expected = encode_grep_manifest(published.manifest_envelope().payload().clone())
            .expect("encode manifest")
            .into_bytes();
        assert_eq!(
            stored,
            Bytes::from(expected),
            "publication leaves `{object_key}` holding exactly the manifest it published"
        );
    }
    assert_eq!(
        store
            .list_prefix(&manifests_prefix(&namespace_id))
            .await
            .expect("list candidate manifests")
            .len(),
        3
    );
}

#[tokio::test]
async fn racing_advancers_have_one_winner_and_loser_can_reload_and_retry() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = namespace_id("docs");
    let seeded = seed_grep_root(&store, &root(namespace_id.clone(), ChangeSeq(0)))
        .await
        .expect("seed root");
    let first = root(namespace_id.clone(), ChangeSeq(1));
    let second = root(namespace_id.clone(), ChangeSeq(2));

    let (first_result, second_result) = tokio::join!(
        advance_grep_root(&store, &seeded, &first),
        advance_grep_root(&store, &seeded, &second)
    );
    let outcomes = [first_result, second_result];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(GrepRootError::Conflict { .. })))
            .count(),
        1
    );
    assert_eq!(
        store
            .list_prefix(&manifests_prefix(&namespace_id))
            .await
            .expect("list candidate manifests")
            .len(),
        3,
        "the seed, winner, and CAS loser's immutable manifests remain durable"
    );

    let reloaded = load_grep_root(&store, &namespace_id)
        .await
        .expect("reload succeeds")
        .expect("root exists");
    let retry = root(namespace_id.clone(), ChangeSeq(3));
    advance_grep_root(&store, &reloaded, &retry)
        .await
        .expect("reload-and-retry succeeds");
    let final_root = load_grep_root(&store, &namespace_id)
        .await
        .expect("final load succeeds")
        .expect("root exists");
    assert_eq!(
        final_root
            .manifest_state()
            .status()
            .active_watermark()
            .map(|resume| (resume.built_through_seq(), resume.next_event_index())),
        Some((ChangeSeq(3), 0))
    );
}

fn root(namespace_id: NamespaceId, built_through_seq: ChangeSeq) -> GrepManifestState {
    GrepManifestState::new(
        namespace_id,
        GrepIndexStatus::Active {
            built_through_seq,
            next_event_index: 0,
        },
        GrepIndexState {
            reorganize: None,
            next_run_no: RunNo(0),
        },
        Vec::new(),
    )
    .expect("valid root")
}
