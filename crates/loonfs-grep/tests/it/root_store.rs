//! Grep root object-store loading and publication behavior.

#![allow(clippy::panic)]

use bytes::Bytes;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_grep::keyspace::{manifest_key, manifests_prefix, root_key};
use loonfs_grep::root::{
    advance_grep_root, encode_grep_manifest, encode_grep_root, load_grep_root, seed_grep_root,
    GrepIndexState, GrepLifecycle, GrepManifestEnvelope, GrepRootEnvelope, GrepRootError,
    GrepRootPointer, GrepRootState,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::ids::namespace_id;

#[tokio::test]
async fn load_rejects_namespace_identity_mismatch() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let requested = namespace_id("requested");
    let wrong = root(namespace_id("other"), ChangeSeq(0));
    let manifest = GrepManifestEnvelope::from_state(wrong).expect("manifest envelope");
    let manifest_bytes = encode_grep_manifest(&manifest).expect("encode manifest");
    store
        .put(
            &manifest_key(&requested, manifest.manifest_id()),
            Bytes::from(manifest_bytes),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write mismatched manifest");
    let envelope = GrepRootEnvelope::from_pointer(GrepRootPointer::new(
        requested.clone(),
        manifest.manifest_id().clone(),
    ))
    .expect("pointer envelope");
    let bytes = encode_grep_root(&envelope).expect("encode pointer");
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

/// A manifest id is the digest of its payload, and the rest of the envelope
/// is family constants, so this writer cannot produce two different byte
/// strings for one id. A foreign writer or a damaged object still can, and
/// the immutable-write byte check is what catches it.
#[tokio::test]
async fn same_manifest_id_with_different_envelope_bytes_is_corrupt() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = namespace_id("docs");
    let state = root(namespace_id.clone(), ChangeSeq(0));

    // Occupy the manifest key this seed will claim with different bytes.
    let envelope = GrepManifestEnvelope::from_state(state.clone()).expect("manifest envelope");
    let key = manifest_key(&namespace_id, envelope.manifest_id());
    store
        .put(
            &key,
            Bytes::from_static(b"not the manifest these bytes claim to be"),
            PutMode::Overwrite,
        )
        .await
        .expect("plant divergent manifest bytes");

    assert!(matches!(
        seed_grep_root(&store, &state).await,
        Err(GrepRootError::Corrupt { .. })
    ));
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
    assert_eq!(final_root.state().index().built_through_seq, ChangeSeq(3));
}

#[tokio::test]
async fn stale_etag_advance_fails_after_concurrent_advance() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = namespace_id("docs");
    let stale = seed_grep_root(&store, &root(namespace_id.clone(), ChangeSeq(0)))
        .await
        .expect("seed root");
    advance_grep_root(&store, &stale, &root(namespace_id.clone(), ChangeSeq(1)))
        .await
        .expect("concurrent advance succeeds");

    assert!(matches!(
        advance_grep_root(&store, &stale, &root(namespace_id, ChangeSeq(2))).await,
        Err(GrepRootError::Conflict { .. })
    ));
}

fn root(namespace_id: NamespaceId, built_through_seq: ChangeSeq) -> GrepRootState {
    GrepRootState::new(
        namespace_id,
        GrepLifecycle::Steady,
        GrepIndexState::new(built_through_seq, 0, None, 0),
        Vec::new(),
    )
    .expect("valid root")
}
