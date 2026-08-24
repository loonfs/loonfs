//! Listing the active checkpoint records a namespace still carries.
//!
//! The listing exists so a pin can be found again once its creation response
//! is gone, so what it must never do is hide a record that still roots a
//! basis. These tests pin exactly that: labels do not identify records,
//! release removes them from the answer, and a passed expiry does not.

use super::*;
use crate::checkpoint::list::list_checkpoints_page;
use loonfs_api::wire::control::CheckpointOwner;
use loonfs_api::{
    CheckpointOwnerSummary, ErrorCode, ListCheckpointsResponse, NamespaceCursor, PageRequest,
};
use loonfs_test_support::ids::page_limit;

async fn list_all_checkpoints<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> crate::error::Result<ListCheckpointsResponse> {
    let limit = page_limit(2);
    let mut checkpoints = Vec::new();
    let mut cursor = None;
    loop {
        let page =
            list_checkpoints_page(store, namespace_id, PageRequest { limit, cursor }).await?;
        checkpoints.extend(page.items);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(ListCheckpointsResponse {
        namespace_id: namespace_id.clone(),
        checkpoints,
        next_cursor: None,
    })
}

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
            expires_at_ms,
        },
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

    let listed = list_all_checkpoints(&store, &namespace_id)
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
    let after_release = list_all_checkpoints(&store, &namespace_id)
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

    let listed = list_all_checkpoints(&store, &namespace_id)
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

    let error = list_all_checkpoints(&store, &namespace_id)
        .await
        .expect_err("listing an absent namespace fails");
    assert_eq!(error.code(), ErrorCode::NamespaceNotFound);
}

#[tokio::test]
async fn pages_concatenate_to_every_checkpoint_once_in_id_order() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    let mut expected = Vec::new();
    for index in 0..7 {
        expected.push(
            pin_named(
                &store,
                &namespace_id,
                &format!("pin-{index}"),
                None,
                &context,
            )
            .await,
        );
    }
    expected.sort();

    let mut actual = Vec::new();
    let mut cursor = None;
    let mut page_count = 0;
    loop {
        let page = list_checkpoints_page(
            &store,
            &namespace_id,
            PageRequest {
                limit: page_limit(2),
                cursor,
            },
        )
        .await
        .expect("list checkpoint page");
        page_count += 1;
        actual.extend(
            page.items
                .into_iter()
                .map(|checkpoint| checkpoint.checkpoint_id),
        );
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }

    assert!(page_count > 1);
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn released_runs_advance_the_last_inspected_key_cursor() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    let mut ids = Vec::new();
    for index in 0..8 {
        ids.push(
            pin_named(
                &store,
                &namespace_id,
                &format!("pin-{index}"),
                None,
                &context,
            )
            .await,
        );
    }
    ids.sort();
    for checkpoint_id in &ids[1..6] {
        super::super::release::release_checkpoint(&store, &namespace_id, checkpoint_id, &context)
            .await
            .expect("release checkpoint in filtered run");
    }

    let first = list_checkpoints_page(
        &store,
        &namespace_id,
        PageRequest {
            limit: page_limit(1),
            cursor: None,
        },
    )
    .await
    .expect("first page");
    assert_eq!(first.items[0].checkpoint_id, ids[0]);

    let second = list_checkpoints_page(
        &store,
        &namespace_id,
        PageRequest {
            limit: page_limit(1),
            cursor: first.next_cursor,
        },
    )
    .await
    .expect("second page across released run");
    assert_eq!(second.items[0].checkpoint_id, ids[6]);
    assert_eq!(
        second
            .next_cursor
            .as_ref()
            .and_then(NamespaceCursor::last_key),
        Some(loonfs_objectstore::keys::checkpoint_record(&namespace_id, &ids[6]).as_str())
    );

    let third = list_checkpoints_page(
        &store,
        &namespace_id,
        PageRequest {
            limit: page_limit(1),
            cursor: second.next_cursor,
        },
    )
    .await
    .expect("third page");
    assert_eq!(third.items[0].checkpoint_id, ids[7]);
    assert!(third.next_cursor.is_none());
}

#[derive(Debug)]
struct DeleteOnCheckpointLoadStore<S> {
    inner: S,
    target: String,
    deleted: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for DeleteOnCheckpointLoadStore<S> {
    async fn head(
        &self,
        key: &str,
    ) -> std::result::Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> std::result::Result<Option<ObjectBody>, ObjectStoreError> {
        if key == self.target && !self.deleted.swap(true, std::sync::atomic::Ordering::SeqCst) {
            self.inner.delete(key).await?;
        }
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> std::result::Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> std::result::Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> std::result::Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, std::result::Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

#[tokio::test]
async fn checkpoint_deleted_between_listing_and_load_is_skipped() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    let mut ids = Vec::new();
    for index in 0..3 {
        ids.push(
            pin_named(
                &inner,
                &namespace_id,
                &format!("pin-{index}"),
                None,
                &context,
            )
            .await,
        );
    }
    ids.sort();
    let deleted = ids[1].clone();
    let store = DeleteOnCheckpointLoadStore {
        inner,
        target: loonfs_objectstore::keys::checkpoint_record(&namespace_id, &deleted),
        deleted: std::sync::atomic::AtomicBool::new(false),
    };

    let listed = list_all_checkpoints(&store, &namespace_id)
        .await
        .expect("list across concurrent reap");
    let listed_ids = listed
        .checkpoints
        .into_iter()
        .map(|checkpoint| checkpoint.checkpoint_id)
        .collect::<Vec<_>>();
    assert_eq!(listed_ids, vec![ids[0].clone(), ids[2].clone()]);
}

#[tokio::test]
async fn first_page_loads_only_the_records_needed_to_fill_it() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");
    for index in 0..20 {
        pin_named(
            &inner,
            &namespace_id,
            &format!("pin-{index}"),
            None,
            &context,
        )
        .await;
    }
    let prefix = loonfs_objectstore::keys::checkpoint_prefix(&namespace_id);
    let store = CountingStore::new(inner, KeyPredicate::prefix(prefix));

    let page = list_checkpoints_page(
        &store,
        &namespace_id,
        PageRequest {
            limit: page_limit(2),
            cursor: None,
        },
    )
    .await
    .expect("first checkpoint page");

    assert_eq!(page.items.len(), 2);
    assert!(page.next_cursor.is_some());
    let counts = store.snapshot();
    assert_eq!(counts.lists, 1);
    assert_eq!(counts.gets_with_metadata, 2);
}

#[tokio::test]
async fn checkpoint_cursor_is_bound_to_its_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let target = NamespaceId::parse("target").expect("namespace id");
    let context = test_context();
    for namespace_id in [&source, &target] {
        bootstrap_namespace(&store, namespace_id, &context, false)
            .await
            .expect("bootstrap namespace");
    }
    for index in 0..2 {
        pin_named(&store, &source, &format!("pin-{index}"), None, &context).await;
    }
    let source_page = list_checkpoints_page(
        &store,
        &source,
        PageRequest {
            limit: page_limit(1),
            cursor: None,
        },
    )
    .await
    .expect("source page");

    let error = list_checkpoints_page(
        &store,
        &target,
        PageRequest {
            limit: page_limit(1),
            cursor: source_page.next_cursor,
        },
    )
    .await
    .expect_err("foreign cursor should fail");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}
