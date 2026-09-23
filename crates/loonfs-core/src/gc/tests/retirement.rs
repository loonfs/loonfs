//! Derived retirement deadlines and outstanding multipart uploads.

use super::*;

#[tokio::test]
async fn deleted_namespace_uses_its_deletion_clock_without_publishing_a_manifest() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("retirement-clock").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let (deadline, keys) = retired_content_namespace(&store, &namespace_id).await;
    let before = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("tombstone");
    store.reset();
    let waiting = gc_namespace(
        &store,
        &namespace_id,
        &config(),
        &context(deadline.now_ms - 1),
    )
    .await
    .expect("before deadline");
    assert_eq!(waiting.reclaim_after_ms, Some(1_000 + GRACE_MS));
    assert_eq!(waiting.next_reclamation_at_ms, Some(deadline.now_ms));
    assert_eq!(waiting.deleted.retired_content_objects, 0);
    let reclaimed = gc_namespace(&store, &namespace_id, &config(), &deadline)
        .await
        .expect("at deadline");
    assert_eq!(reclaimed.deleted.retired_content_objects, keys.len() as u64);
    assert_eq!(reclaimed.next_reclamation_at_ms, None);
    let after = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("same tombstone");
    assert_eq!(
        before.state.manifest.manifest_no,
        after.state.manifest.manifest_no
    );
    assert_eq!(store.counts().puts, 0);
}
