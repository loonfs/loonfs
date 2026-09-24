//! Transitive fork protection for content initially published inline.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::protocol::PublishTailOptions;
use crate::storage::inline_content::InlineContent;
use loonfs_api::{AbsolutePath, CommitId, ContentId, DestinationBehavior};

async fn publish_inline(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    index: usize,
    setup: &MutationContext,
) -> String {
    let value = InlineContent::new(
        namespace_id.clone(),
        ContentId::generate(),
        Bytes::copy_from_slice(namespace_id.as_str().as_bytes()),
    );
    let key = loonfs_objectstore::keys::content_blob(namespace_id, &value.content_ref().content_id);
    let request = CommitRequest::single(
        CommitId::parse(format!("write-{index}")).expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse(format!("/owned-{index}")).expect("path"),
            content_ref: Some(value.content_ref().clone()),
            inline_content: None,
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            [CommitCandidate::with_inline_content(
                request,
                Vec::new(),
                vec![value],
            )],
            setup,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .pop()
        .expect("result")
        .expect("publish inline");
    key
}

async fn assert_leaf_reads_every_owner(store: &LocalFsStore, namespaces: &[NamespaceId; 3]) {
    let view = load_current_metadata_view(store, &namespaces[2])
        .await
        .expect("fresh leaf view");
    for (index, owner) in namespaces.iter().enumerate() {
        let file = view
            .get_file_bytes(
                store,
                &format!("/owned-{index}"),
                None,
                &ReadAccess::live(Authorizer::Unrestricted),
            )
            .await
            .expect("read inherited or local content");
        assert_eq!(file.bytes, owner.as_str().as_bytes());
    }
}

#[tokio::test]
async fn live_grandchild_keeps_deleted_ancestors_pinned_until_retirement_runs_leaf_first() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespaces =
        ["root", "middle", "leaf"].map(|name| NamespaceId::parse(name).expect("namespace"));
    let setup = context(1_000);
    let mut keys = Vec::new();
    for (index, namespace_id) in namespaces.iter().enumerate() {
        if index == 0 {
            create(&store, namespace_id, &setup)
                .await
                .expect("bootstrap root");
        } else {
            fork_namespace(
                &store,
                &namespaces[index - 1],
                namespace_id,
                &loonfs_test_support::test_actor(),
                None,
                &setup,
            )
            .await
            .expect("fork");
        }
        keys.push(publish_inline(&store, namespace_id, index, &setup).await);
    }
    let middle_pin = read_fork_record(&store, &namespaces[0]).await;
    let leaf_pin = read_fork_record(&store, &namespaces[1]).await;
    for namespace_id in &namespaces[..2] {
        delete_namespace(&store, namespace_id, Default::default(), &setup)
            .await
            .expect("delete ancestor");
    }
    let mut aged_now = 0;
    for namespace_id in &namespaces {
        aged_now = aged_now.max(
            now_after_newest_object(
                &store,
                namespace_id,
                UNREFERENCED_SEGMENT_MIN_AGE_MS + GRACE_MS + 1,
            )
            .await,
        );
    }
    let aged = context(aged_now);
    for index in [0, 1, 1, 0] {
        let report = gc_namespace(&store, &namespaces[index], &config(), &aged)
            .await
            .expect("collect ancestor while grandchild lives");
        assert_eq!(report.deleted.retired_content_objects, 0);
        assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
        assert!(checkpoint_exists(&store, &namespaces[0], &middle_pin.pin_id).await);
        assert!(checkpoint_exists(&store, &namespaces[1], &leaf_pin.pin_id).await);
        assert_leaf_reads_every_owner(&store, &namespaces).await;
    }

    delete_namespace(&store, &namespaces[2], Default::default(), &setup)
        .await
        .expect("delete leaf");
    // Deletion alone must not release either link in the protection chain.
    for index in [0, 1] {
        let report = gc_namespace(&store, &namespaces[index], &config(), &aged)
            .await
            .expect("ancestors still pinned by unretired descendants");
        assert_eq!(report.deleted.retired_content_objects, 0);
    }
    for index in [2, 1, 0] {
        gc_namespace(&store, &namespaces[index], &config(), &aged)
            .await
            .expect("retire from leaf to root");
        assert!(store
            .head(&keys[index])
            .await
            .expect("retired object")
            .is_none());
        // Retirement may delete only this owner's objects, not inherited ones.
        for key in &keys[..index] {
            assert!(store.head(key).await.expect("ancestor object").is_some());
        }
        if index > 0 {
            let pin = if index == 2 { &leaf_pin } else { &middle_pin };
            assert!(!checkpoint_exists(&store, &namespaces[index - 1], &pin.pin_id).await);
        }
    }
}
