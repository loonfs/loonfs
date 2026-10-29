//! Listing the active pins a namespace still carries.
//!
//! The listing exists so a pin can be found again once its creation response
//! is gone, so what it must never do is hide a record that still roots a
//! basis. These tests pin exactly that: labels do not identify records,
//! delete removes them from the answer, and a passed expiry does not.

use super::*;
use loonfs_objectstore::ListedObject;
use loonfs_test_support::ids::page_limit;
use loonfs_types::format::control::PinOwner;
use loonfs_types::{CheckpointOwner, ErrorCode, ListCheckpointsResponse, PageRequest};

async fn list_all_checkpoints<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> crate::error::Result<ListCheckpointsResponse> {
    let limit = page_limit(2);
    let mut checkpoints = Vec::new();
    let mut cursor = None;
    loop {
        let page = crate::NamespaceEngine::reader(crate::test_support::namespace_store(
            store,
            namespace_id,
        ))
        .list_checkpoints_page(PageRequest { limit, cursor })
        .await?;
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
    context: &WriterAt,
) -> PinId {
    crate::test_support::namespace_store(store, namespace_id)
        .create_pin(
            PinOwner::User {
                name: name.to_owned(),
                expires_at_ms,
            },
            &context.clock(),
        )
        .await
        .map(|report| crate::pin::checkpoint_summary(report.response))
        .expect("create checkpoint")
        .checkpoint_id
}

#[tokio::test]
async fn two_pins_under_one_label_list_as_two_records() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
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
            CheckpointOwner::User {
                name: "nightly".to_owned()
            }
        );
        assert_eq!(checkpoint.created_at_ms, context.now_ms);
        assert_eq!(checkpoint.expires_at_ms, None, "no ttl was asked for");
    }

    // Delete is what takes a record out of the answer, and it takes out
    // exactly the one named.
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&first)
    .await
    .expect("delete checkpoint");
    let after_delete = list_all_checkpoints(&store, &namespace_id)
        .await
        .expect("list checkpoints");
    assert_eq!(after_delete.checkpoints.len(), 1);
    assert_ne!(after_delete.checkpoints[0].checkpoint_id, first);
}

#[tokio::test]
async fn an_expired_record_is_listed_with_its_expiry_until_it_is_deleted() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
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
    bootstrap_namespace(&store, &namespace_id, &context)
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
        let page = crate::NamespaceEngine::reader(crate::test_support::namespace_store(
            &store,
            &namespace_id,
        ))
        .list_checkpoints_page(PageRequest {
            limit: page_limit(2),
            cursor,
        })
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
async fn deleted_pins_are_absent_from_later_pages() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
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
        crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine()
        .delete_snapshot(checkpoint_id)
        .await
        .expect("delete checkpoint in filtered run");
    }

    let first =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .list_checkpoints_page(PageRequest {
                limit: page_limit(1),
                cursor: None,
            })
            .await
            .expect("first page");
    assert_eq!(first.items[0].checkpoint_id, ids[0]);

    let second =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .list_checkpoints_page(PageRequest {
                limit: page_limit(1),
                cursor: first.next_cursor,
            })
            .await
            .expect("second page across deleted run");
    assert_eq!(second.items[0].checkpoint_id, ids[6]);
    assert_eq!(
        second
            .next_cursor
            .as_ref()
            .map(|cursor| cursor.last_key.as_str()),
        Some(loonfs_objectstore::keys::pin(&namespace_id, &ids[6]).as_str())
    );

    let third =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .list_checkpoints_page(PageRequest {
                limit: page_limit(1),
                cursor: second.next_cursor,
            })
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
    loonfs_test_support::delegate_object_store!(self; buffered);

    fn checksum_algorithm(&self) -> loonfs_types::ChecksumAlgorithm {
        self.inner.checksum_algorithm()
    }

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

    fn list_entries_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, std::result::Result<ListedObject, ObjectStoreError>> {
        self.inner.list_entries_from_stream(prefix, start_after)
    }

    async fn list_child_prefixes(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: loonfs_types::EffectiveLimit,
    ) -> std::result::Result<loonfs_types::Page<String, String>, ObjectStoreError> {
        self.inner
            .list_child_prefixes(prefix, start_after, limit)
            .await
    }
}

#[tokio::test]
async fn checkpoint_deleted_between_listing_and_load_is_skipped() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context)
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
        target: loonfs_objectstore::keys::pin(&namespace_id, &deleted),
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
    bootstrap_namespace(&inner, &namespace_id, &context)
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
    let prefix = loonfs_objectstore::keys::pin_prefix(&namespace_id);
    let store = RecordingStore::new(inner, KeyPredicate::prefix(prefix));

    let page =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .list_checkpoints_page(PageRequest {
                limit: page_limit(2),
                cursor: None,
            })
            .await
            .expect("first checkpoint page");

    assert_eq!(page.items.len(), 2);
    assert!(page.next_cursor.is_some());
    let counts = store.counts();
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
        bootstrap_namespace(&store, namespace_id, &context)
            .await
            .expect("bootstrap namespace");
    }
    for index in 0..2 {
        pin_named(&store, &source, &format!("pin-{index}"), None, &context).await;
    }
    let source_page =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &source))
            .list_checkpoints_page(PageRequest {
                limit: page_limit(1),
                cursor: None,
            })
            .await
            .expect("source page");

    let error =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &target))
            .list_checkpoints_page(PageRequest {
                limit: page_limit(1),
                cursor: source_page.next_cursor.clone(),
            })
            .await
            .expect_err("foreign cursor should fail");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}

/// Checks that the snapshot listing returns user pins, expired ones
/// included, and leaves out fork pins, while the checkpoint listing returns
/// every pin.
#[tokio::test]
async fn the_snapshot_listing_returns_user_pins_and_leaves_out_fork_pins() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap namespace");
    let fork_owner = PinOwner::Fork {
        target_namespace_id: NamespaceId::parse("clone").expect("namespace id"),
    };
    let mut snapshots = BTreeSet::new();
    for index in 0..3 {
        snapshots.insert(
            pin_named(
                &store,
                &namespace_id,
                &format!("pin-{index}"),
                Some(context.now_ms - 1),
                &context,
            )
            .await,
        );
        crate::test_support::namespace_store(&store, &namespace_id)
            .create_pin(fork_owner.clone(), &context.clock())
            .await
            .expect("create fork pin");
    }

    let mut listed = BTreeSet::new();
    let mut cursor = None;
    loop {
        let page = crate::NamespaceEngine::reader(crate::test_support::namespace_store(
            &store,
            &namespace_id,
        ))
        .list_snapshots_page(PageRequest {
            limit: page_limit(2),
            cursor,
        })
        .await
        .expect("list snapshots");
        for snapshot in page.items {
            assert_eq!(snapshot.expires_at_ms, Some(context.now_ms - 1));
            listed.insert(snapshot.snapshot_id);
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    assert_eq!(listed, snapshots);
    let checkpoints = list_all_checkpoints(&store, &namespace_id)
        .await
        .expect("list checkpoints");
    assert_eq!(checkpoints.checkpoints.len(), 6);
}

#[tokio::test]
async fn a_refused_owner_writes_no_record_to_find() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap namespace");

    let refused = [
        PinOwner::User {
            name: String::new(),
            expires_at_ms: None,
        },
        PinOwner::User {
            name: String::new(),
            expires_at_ms: Some(context.now_ms + 60_000),
        },
        PinOwner::User {
            name: "report-run".to_owned(),
            expires_at_ms: Some(0),
        },
    ];
    for owner in refused {
        let error = crate::test_support::namespace_store(&store, &namespace_id)
            .create_pin(owner.clone(), &context.clock())
            .await
            .map(|report| crate::pin::checkpoint_summary(report.response))
            .expect_err("the owner is not creatable");
        assert_eq!(error.code(), ErrorCode::InvalidRequest, "owner: {owner:?}");
    }

    let listed = list_all_checkpoints(&store, &namespace_id)
        .await
        .expect("list checkpoints");
    assert!(listed.checkpoints.is_empty());
}

/// Checks that a pin read by ID matches its listed row, that a snapshot
/// read by ID returns an expired snapshot, and that a read through an
/// expired snapshot or a fork pin returns `snapshot_not_found`.
#[tokio::test]
async fn a_pin_read_by_id_matches_its_listed_row_and_a_read_through_it_checks_expiry() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap namespace");
    let unexpiring = pin_named(&store, &namespace_id, "nightly", None, &context).await;
    let expires_at_ms = context.now_ms + 10;
    let expiring = pin_named(&store, &namespace_id, "view", Some(expires_at_ms), &context).await;
    let fork = crate::test_support::namespace_store(&store, &namespace_id)
        .create_pin(
            PinOwner::Fork {
                target_namespace_id: NamespaceId::parse("clone").expect("namespace id"),
            },
            &context.clock(),
        )
        .await
        .expect("create fork pin")
        .response
        .pin_id;

    let listed = list_all_checkpoints(&store, &namespace_id)
        .await
        .expect("list checkpoints");
    assert_eq!(listed.checkpoints.len(), 3);
    for row in &listed.checkpoints {
        let read = crate::NamespaceEngine::reader(crate::test_support::namespace_store(
            &store,
            &namespace_id,
        ))
        .get_checkpoint(&row.checkpoint_id)
        .await
        .expect("read checkpoint");
        assert_eq!(&read, row);
    }
    let expired =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .get_snapshot(&expiring)
            .await
            .expect("read an expired snapshot by its ID");
    assert_eq!(expired.expires_at_ms, Some(expires_at_ms));

    fn code<T: std::fmt::Debug>(result: crate::error::Result<T>) -> ErrorCode {
        result.expect_err("refused").code()
    }
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let namespace = &namespace;
    let read_at = |pin_id, now_ms| async move {
        crate::pin::ensure_unexpired(namespace.load_user_pin(pin_id).await?, now_ms)
    };
    read_at(&expiring, expires_at_ms - 1)
        .await
        .expect("an unexpired snapshot is readable");
    read_at(&unexpiring, u64::MAX)
        .await
        .expect("a snapshot without an expiry is readable");
    assert_eq!(
        code(read_at(&expiring, expires_at_ms).await),
        ErrorCode::SnapshotNotFound
    );
    assert_eq!(
        code(read_at(&fork, context.now_ms).await),
        ErrorCode::SnapshotNotFound
    );
    assert_eq!(
        code(
            crate::NamespaceEngine::reader(crate::test_support::namespace_store(
                &store,
                &namespace_id
            ))
            .get_snapshot(&fork)
            .await
        ),
        ErrorCode::SnapshotNotFound
    );
    let missing = PinId::parse("pin_00000000000000000009-0000000000000009").expect("pin id");
    assert_eq!(
        code(
            crate::NamespaceEngine::reader(crate::test_support::namespace_store(
                &store,
                &namespace_id
            ))
            .get_checkpoint(&missing)
            .await
        ),
        ErrorCode::SnapshotNotFound
    );
}

/// Checks that deleting a missing snapshot succeeds without a write, and
/// that deleting a fork pin returns `snapshot_not_found` and keeps it.
#[tokio::test]
async fn deleting_a_missing_snapshot_succeeds_and_a_fork_pin_is_not_a_snapshot() {
    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap namespace");
    let snapshot = pin_named(&store, &namespace_id, "nightly", None, &context).await;
    let fork = crate::test_support::namespace_store(&store, &namespace_id)
        .create_pin(
            PinOwner::Fork {
                target_namespace_id: NamespaceId::parse("clone").expect("namespace id"),
            },
            &context.clock(),
        )
        .await
        .expect("create fork pin")
        .response
        .pin_id;

    for attempt in 0..2 {
        store.reset();
        let deleted = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine()
        .delete_snapshot(&snapshot)
        .await
        .expect("delete succeeds whether or not the snapshot exists");
        assert_eq!(deleted.snapshot_id, snapshot);
        assert_eq!(
            store.counts().deletes,
            usize::from(attempt == 0),
            "attempt {attempt}"
        );
    }
    store.reset();
    let error = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&fork)
    .await
    .expect_err("a fork pin isn't a snapshot");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);
    assert_eq!(store.counts().deletes, 0);
    assert!(crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&fork)
        .await
        .expect("load")
        .is_some());
}
