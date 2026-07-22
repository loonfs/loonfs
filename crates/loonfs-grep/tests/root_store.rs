#![allow(clippy::panic)]

use bytes::Bytes;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_grep::keyspace::root_key;
use loonfs_grep::root::{
    advance_grep_root, encode_grep_root, load_grep_root, seed_grep_root, GrepIndexState,
    GrepLifecycle, GrepRootEnvelope, GrepRootError, GrepRootState,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, PutMode};

#[tokio::test]
async fn load_rejects_namespace_identity_mismatch() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let requested = namespace_id("requested");
    let wrong = root(namespace_id("other"), ChangeSeq(0));
    let envelope = GrepRootEnvelope::from_state("test", wrong).expect("envelope");
    let bytes = encode_grep_root(&envelope).expect("encode");
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

    seed_grep_root(&store, &state, "seed-one")
        .await
        .expect("first seed succeeds");
    assert!(matches!(
        seed_grep_root(&store, &state, "seed-two").await,
        Err(GrepRootError::Conflict { .. })
    ));
}

#[tokio::test]
async fn racing_advancers_have_one_winner_and_loser_can_reload_and_retry() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = namespace_id("docs");
    let seeded = seed_grep_root(&store, &root(namespace_id.clone(), ChangeSeq(0)), "seed")
        .await
        .expect("seed root");
    let first = root(namespace_id.clone(), ChangeSeq(1));
    let second = root(namespace_id.clone(), ChangeSeq(2));

    let (first_result, second_result) = tokio::join!(
        advance_grep_root(&store, &seeded, &first, "racer-one"),
        advance_grep_root(&store, &seeded, &second, "racer-two")
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

    let reloaded = load_grep_root(&store, &namespace_id)
        .await
        .expect("reload succeeds")
        .expect("root exists");
    let retry = root(namespace_id.clone(), ChangeSeq(3));
    advance_grep_root(&store, &reloaded, &retry, "retry")
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
    let stale = seed_grep_root(&store, &root(namespace_id.clone(), ChangeSeq(0)), "seed")
        .await
        .expect("seed root");
    advance_grep_root(
        &store,
        &stale,
        &root(namespace_id.clone(), ChangeSeq(1)),
        "winner",
    )
    .await
    .expect("concurrent advance succeeds");

    assert!(matches!(
        advance_grep_root(&store, &stale, &root(namespace_id, ChangeSeq(2)), "stale").await,
        Err(GrepRootError::Conflict { .. })
    ));
}

fn root(namespace_id: NamespaceId, built_through_seq: ChangeSeq) -> GrepRootState {
    GrepRootState::new(
        namespace_id,
        GrepLifecycle::Steady,
        GrepIndexState::new(built_through_seq, None, 0),
        Vec::new(),
    )
    .expect("valid root")
}

fn namespace_id(value: &str) -> NamespaceId {
    NamespaceId::parse(value).expect("valid namespace id")
}
