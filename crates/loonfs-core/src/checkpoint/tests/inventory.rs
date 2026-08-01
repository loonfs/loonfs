//! Listing the active checkpoint records a namespace still carries.
//!
//! The listing exists so a pin can be found again once its creation response
//! is gone, so what it must never do is hide a record that still roots a
//! basis. These tests pin exactly that: labels do not identify records,
//! release removes them from the answer, and a passed expiry does not.

use super::*;
use crate::checkpoint::list::list_checkpoints;
use loonfs_api::wire::control::CheckpointOwner;
use loonfs_api::{CheckpointOwnerSummary, ErrorCode};

async fn pin_named<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    name: &str,
    expires_at_ms: Option<u64>,
    context: &MutationContext,
) -> CheckpointId {
    create::create_checkpoint(
        store,
        namespace_id,
        CheckpointOwner::User {
            name: name.to_owned(),
        },
        expires_at_ms,
        context,
    )
    .await
    .expect("create checkpoint")
    .checkpoint_id
}

/// One label over two calls is two records, and the listing is where their
/// two ids can be read back — the whole reason it exists.
#[tokio::test]
async fn two_pins_under_one_label_list_as_two_records() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    let first = pin_named(&store, &namespace_id, "nightly", None, &context).await;
    let second = pin_named(&store, &namespace_id, "nightly", None, &context).await;
    assert_ne!(first, second, "each call mints its own record");

    let listed = list_checkpoints(&store, &namespace_id)
        .await
        .expect("list checkpoints");
    assert_eq!(listed.namespace_id, namespace_id);
    let ids: BTreeSet<_> = listed
        .checkpoints
        .iter()
        .map(|checkpoint| checkpoint.checkpoint_id.clone())
        .collect();
    assert_eq!(ids, BTreeSet::from([first.clone(), second]));
    for checkpoint in &listed.checkpoints {
        assert_eq!(
            checkpoint.owner,
            CheckpointOwnerSummary::User {
                name: "nightly".to_owned()
            }
        );
        assert_eq!(checkpoint.created_at_ms, context.now_ms);
        assert_eq!(checkpoint.expires_at_ms, None, "no ttl was asked for");
    }

    // Release is what takes a record out of the answer, and it takes out
    // exactly the one named.
    super::super::release::release_checkpoint(&store, &namespace_id, &first, &context)
        .await
        .expect("release checkpoint");
    let after_release = list_checkpoints(&store, &namespace_id)
        .await
        .expect("list checkpoints");
    assert_eq!(after_release.checkpoints.len(), 1);
    assert_ne!(after_release.checkpoints[0].checkpoint_id, first);
}

/// A record whose expiry has passed is still active, still roots its basis,
/// and is still listed. Garbage collection is what turns a passed expiry
/// into a release; until it does, hiding the record would hide a live root
/// from the operation whose job is to find live roots.
#[tokio::test]
async fn an_expired_record_is_listed_with_its_expiry_until_it_is_released() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    let expires_at_ms = context.now_ms - 1;
    let expired = pin_named(
        &store,
        &namespace_id,
        "yesterday",
        Some(expires_at_ms),
        &context,
    )
    .await;

    let listed = list_checkpoints(&store, &namespace_id)
        .await
        .expect("list checkpoints");
    assert_eq!(listed.checkpoints.len(), 1);
    assert_eq!(listed.checkpoints[0].checkpoint_id, expired);
    assert_eq!(listed.checkpoints[0].expires_at_ms, Some(expires_at_ms));
}

/// An inventory that answered "no checkpoints" for a namespace that does not
/// exist would be indistinguishable from one with none.
#[tokio::test]
async fn a_namespace_that_does_not_exist_is_not_a_namespace_without_checkpoints() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("absent").expect("namespace id");

    let error = list_checkpoints(&store, &namespace_id)
        .await
        .expect_err("listing an absent namespace fails");
    assert_eq!(error.code(), ErrorCode::NamespaceNotFound);
}
