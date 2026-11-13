//! Listing the active snapshots a namespace still carries.
//!
//! The listing exists so a snapshot can be found again once its creation response
//! is gone, so what it must never do is hide a record that still roots a
//! basis. These tests pin exactly that: labels do not identify records, and
//! a delete or a passed expiry hides a snapshot from the caller's listing but
//! not from the inventory.

use super::*;
use loonfs_objectstore::ListedObject;
use loonfs_test_support::ids::page_limit;
use loonfs_types::format::control::SnapshotOwner;
use loonfs_types::{ErrorCode, ListSnapshotRecordsResponse, PageRequest, SnapshotRecordOwner};

async fn list_all_snapshot_records<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> crate::error::Result<ListSnapshotRecordsResponse> {
    let limit = page_limit(2);
    let mut snapshots = Vec::new();
    let mut cursor = None;
    loop {
        let page = crate::NamespaceEngine::reader(crate::test_support::namespace_store(
            store,
            namespace_id,
        ))
        .list_snapshot_records_page(PageRequest { limit, cursor })
        .await?;
        snapshots.extend(page.items);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(ListSnapshotRecordsResponse {
        namespace_id: namespace_id.clone(),
        snapshots,
        next_cursor: None,
    })
}

async fn snapshot_named<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    name: &str,
    expires_at_ms: Option<u64>,
    context: &WriterAt,
) -> SnapshotId {
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    namespace
        .create_snapshot_record(
            SnapshotOwner::User {
                name: Some(name.to_owned()),
                expires_at_ms,
                deleted_at_ms: None,
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context.clock(),
        )
        .await
        .map(crate::snapshot::snapshot_record_summary)
        .expect("create snapshot")
        .snapshot_id
}

#[tokio::test]
async fn two_snapshots_under_one_label_list_as_two_records() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap namespace");

    let first = snapshot_named(&store, &namespace_id, "nightly", None, &context).await;
    let second = snapshot_named(&store, &namespace_id, "nightly", None, &context).await;
    assert_ne!(first, second, "each call mints its own record");

    let listed = list_all_snapshot_records(&store, &namespace_id)
        .await
        .expect("list snapshots");
    assert_eq!(listed.namespace_id, namespace_id);
    let ids: BTreeSet<_> = listed
        .snapshots
        .iter()
        .map(|snapshot| snapshot.snapshot_id.clone())
        .collect();
    assert_eq!(ids, BTreeSet::from([first.clone(), second]));
    for snapshot in &listed.snapshots {
        assert_eq!(
            snapshot.owner,
            SnapshotRecordOwner::User {
                name: Some("nightly".to_owned())
            }
        );
        assert_eq!(snapshot.created_at_ms, context.now_ms);
        assert_eq!(snapshot.expires_at_ms, None, "no ttl was asked for");
    }

    // A delete marks exactly the one snapshot it names, and both stay in the
    // inventory.
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&first)
    .await
    .expect("delete snapshot");
    let after_delete = list_all_snapshot_records(&store, &namespace_id)
        .await
        .expect("list snapshots");
    assert_eq!(after_delete.snapshots.len(), 2);
    for snapshot in &after_delete.snapshots {
        assert_eq!(
            snapshot.deleted_at_ms.is_some(),
            snapshot.snapshot_id == first
        );
    }
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
    let expired = snapshot_named(
        &store,
        &namespace_id,
        "yesterday",
        Some(expires_at_ms),
        &context,
    )
    .await;

    let listed = list_all_snapshot_records(&store, &namespace_id)
        .await
        .expect("list snapshots");
    assert_eq!(listed.snapshots.len(), 1);
    assert_eq!(listed.snapshots[0].snapshot_id, expired);
    assert_eq!(listed.snapshots[0].expires_at_ms, Some(expires_at_ms));
}

#[tokio::test]
async fn a_namespace_that_does_not_exist_is_not_a_namespace_without_snapshots() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("absent").expect("namespace id");

    let error = list_all_snapshot_records(&store, &namespace_id)
        .await
        .expect_err("listing an absent namespace fails");
    assert_eq!(error.code(), ErrorCode::NamespaceNotFound);
}

#[tokio::test]
async fn pages_concatenate_to_every_snapshot_once_in_id_order() {
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
            snapshot_named(
                &store,
                &namespace_id,
                &format!("snapshot-{index}"),
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
        .list_snapshot_records_page(PageRequest {
            limit: page_limit(2),
            cursor,
        })
        .await
        .expect("list snapshot page");
        page_count += 1;
        actual.extend(page.items.into_iter().map(|snapshot| snapshot.snapshot_id));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }

    assert!(page_count > 1);
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn deleted_snapshots_are_absent_from_later_pages() {
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
            snapshot_named(
                &store,
                &namespace_id,
                &format!("snapshot-{index}"),
                None,
                &context,
            )
            .await,
        );
    }
    ids.sort();
    for snapshot_id in &ids[1..6] {
        crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine()
        .delete_snapshot(snapshot_id)
        .await
        .expect("delete snapshot in filtered run");
    }

    let first =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .list_snapshots_page(PageRequest {
                limit: page_limit(1),
                cursor: None,
            })
            .await
            .expect("first page");
    assert_eq!(first.items[0].snapshot_id, ids[0]);

    let second =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .list_snapshots_page(PageRequest {
                limit: page_limit(1),
                cursor: first.next_cursor,
            })
            .await
            .expect("second page across deleted run");
    assert_eq!(second.items[0].snapshot_id, ids[6]);
    assert_eq!(
        second
            .next_cursor
            .clone()
            .map(|cursor| crate::snapshot::SnapshotRecordPageCursor::from(cursor).last_key),
        Some(loonfs_objectstore::keys::snapshot_record(
            &namespace_id,
            &ids[6]
        ))
    );

    let third =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .list_snapshots_page(PageRequest {
                limit: page_limit(1),
                cursor: second.next_cursor,
            })
            .await
            .expect("third page");
    assert_eq!(third.items[0].snapshot_id, ids[7]);
    assert!(third.next_cursor.is_none());
}

#[derive(Debug)]
struct DeleteOnSnapshotLoadStore<S> {
    inner: S,
    target: String,
    deleted: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for DeleteOnSnapshotLoadStore<S> {
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
async fn snapshot_deleted_between_listing_and_load_is_skipped() {
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
            snapshot_named(
                &inner,
                &namespace_id,
                &format!("snapshot-{index}"),
                None,
                &context,
            )
            .await,
        );
    }
    ids.sort();
    let deleted = ids[1].clone();
    let store = DeleteOnSnapshotLoadStore {
        inner,
        target: loonfs_objectstore::keys::snapshot_record(&namespace_id, &deleted),
        deleted: std::sync::atomic::AtomicBool::new(false),
    };

    let listed = list_all_snapshot_records(&store, &namespace_id)
        .await
        .expect("list across concurrent reap");
    let listed_ids = listed
        .snapshots
        .into_iter()
        .map(|snapshot| snapshot.snapshot_id)
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
        snapshot_named(
            &inner,
            &namespace_id,
            &format!("snapshot-{index}"),
            None,
            &context,
        )
        .await;
    }
    let prefix = loonfs_objectstore::keys::snapshot_prefix(&namespace_id);
    let store = RecordingStore::new(inner, KeyPredicate::prefix(prefix));

    let page =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .list_snapshot_records_page(PageRequest {
                limit: page_limit(2),
                cursor: None,
            })
            .await
            .expect("first snapshot page");

    assert_eq!(page.items.len(), 2);
    assert!(page.next_cursor.is_some());
    let counts = store.counts();
    assert_eq!(counts.lists, 1);
    assert_eq!(counts.gets_with_metadata, 2);
}

#[tokio::test]
async fn snapshot_record_cursor_is_bound_to_its_namespace() {
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
        snapshot_named(
            &store,
            &source,
            &format!("snapshot-{index}"),
            None,
            &context,
        )
        .await;
    }
    let source_page =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &source))
            .list_snapshot_records_page(PageRequest {
                limit: page_limit(1),
                cursor: None,
            })
            .await
            .expect("source page");

    let error =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &target))
            .list_snapshot_records_page(PageRequest {
                limit: page_limit(1),
                cursor: source_page.next_cursor.clone(),
            })
            .await
            .expect_err("foreign cursor should fail");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}

/// Checks that the snapshot listing returns user snapshots and leaves out fork
/// snapshots, while the snapshot inventory returns every snapshot.
#[tokio::test]
async fn the_snapshot_listing_returns_user_snapshots_and_leaves_out_fork_snapshots() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap namespace");
    let fork_owner = SnapshotOwner::Fork {
        target_namespace_id: NamespaceId::parse("clone").expect("namespace id"),
    };
    let mut snapshots = BTreeSet::new();
    for index in 0..3 {
        snapshots.insert(
            snapshot_named(
                &store,
                &namespace_id,
                &format!("snapshot-{index}"),
                None,
                &context,
            )
            .await,
        );
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        namespace
            .create_snapshot_record(
                fork_owner.clone(),
                crate::test_support::folded_manifest(&namespace)
                    .await
                    .expect("fold the WAL tail"),
                &context.clock(),
            )
            .await
            .expect("create fork snapshot");
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
            assert_eq!(snapshot.expires_at_ms, None);
            listed.insert(snapshot.snapshot_id);
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    assert_eq!(listed, snapshots);
    let records = list_all_snapshot_records(&store, &namespace_id)
        .await
        .expect("list snapshot records");
    assert_eq!(records.snapshots.len(), 6);
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
        SnapshotOwner::User {
            name: Some(String::new()),
            expires_at_ms: None,
            deleted_at_ms: None,
        },
        SnapshotOwner::User {
            name: Some(String::new()),
            expires_at_ms: Some(context.now_ms + 60_000),
            deleted_at_ms: None,
        },
        SnapshotOwner::User {
            name: Some("report-run".to_owned()),
            expires_at_ms: Some(0),
            deleted_at_ms: None,
        },
    ];
    for owner in refused {
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let error = namespace
            .create_snapshot_record(
                owner.clone(),
                crate::test_support::folded_manifest(&namespace)
                    .await
                    .expect("fold the WAL tail"),
                &context.clock(),
            )
            .await
            .map(crate::snapshot::snapshot_record_summary)
            .expect_err("the owner is not creatable");
        assert_eq!(error.code(), ErrorCode::InvalidRequest, "owner: {owner:?}");
    }

    let listed = list_all_snapshot_records(&store, &namespace_id)
        .await
        .expect("list snapshots");
    assert!(listed.snapshots.is_empty());
}

/// Checks that a snapshot read by ID matches its listed row, that a snapshot
/// read by ID and the snapshot listing leave out an expired snapshot that the
/// inventory still returns, and that a read through an expired snapshot or a
/// fork snapshot returns `snapshot_not_found`.
#[tokio::test]
async fn a_snapshot_read_by_id_matches_its_listed_row_and_a_read_through_it_checks_expiry() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap namespace");
    let unexpiring = snapshot_named(&store, &namespace_id, "nightly", None, &context).await;
    let expires_at_ms = context.now_ms + 10;
    let expiring =
        snapshot_named(&store, &namespace_id, "view", Some(expires_at_ms), &context).await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let fork = namespace
        .create_snapshot_record(
            SnapshotOwner::Fork {
                target_namespace_id: NamespaceId::parse("clone").expect("namespace id"),
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context.clock(),
        )
        .await
        .expect("create fork snapshot")
        .snapshot_id;

    let listed = list_all_snapshot_records(&store, &namespace_id)
        .await
        .expect("list snapshots");
    assert_eq!(listed.snapshots.len(), 3);
    for row in &listed.snapshots {
        let read = crate::NamespaceEngine::reader(crate::test_support::namespace_store(
            &store,
            &namespace_id,
        ))
        .get_snapshot_record(&row.snapshot_id)
        .await
        .expect("read snapshot");
        assert_eq!(&read, row);
    }
    let reader =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id));
    assert_eq!(
        code(reader.get_snapshot(&expiring).await),
        ErrorCode::SnapshotNotFound
    );
    let live = reader
        .list_snapshots_page(PageRequest {
            limit: page_limit(2),
            cursor: None,
        })
        .await
        .expect("list snapshots");
    let live: Vec<_> = live.items.into_iter().map(|row| row.snapshot_id).collect();
    assert_eq!(live, vec![unexpiring.clone()]);

    fn code<T: std::fmt::Debug>(result: crate::error::Result<T>) -> ErrorCode {
        result.expect_err("refused").code()
    }
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let namespace = &namespace;
    let read_at = |snapshot_id, now_ms| async move {
        crate::snapshot::ensure_live(namespace.load_user_snapshot(snapshot_id).await?, now_ms)
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
    let missing = SnapshotId::parse("snp_00000000000000000009-00000000000000000000000009")
        .expect("snapshot id");
    assert_eq!(
        code(
            crate::NamespaceEngine::reader(crate::test_support::namespace_store(
                &store,
                &namespace_id
            ))
            .get_snapshot_record(&missing)
            .await
        ),
        ErrorCode::SnapshotNotFound
    );
}

/// Checks that deleting a deleted snapshot succeeds without a write, and
/// that deleting a fork snapshot returns `snapshot_not_found` and keeps it.
#[tokio::test]
async fn a_repeated_delete_writes_nothing_and_a_fork_snapshot_is_not_a_user_snapshot() {
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
    let snapshot = snapshot_named(&store, &namespace_id, "nightly", None, &context).await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let fork = namespace
        .create_snapshot_record(
            SnapshotOwner::Fork {
                target_namespace_id: NamespaceId::parse("clone").expect("namespace id"),
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context.clock(),
        )
        .await
        .expect("create fork snapshot")
        .snapshot_id;

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
        .expect("delete succeeds whether or not the snapshot is deleted");
        assert_eq!(deleted.snapshot_id, snapshot);
        assert_eq!(
            store.counts().compare_and_swaps,
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
    .expect_err("a fork snapshot isn't a user snapshot");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert!(crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&fork)
        .await
        .expect("load")
        .is_some());
}

/// Checks that a host whose clock is behind the deleting host's clock can't
/// read through, extend, or fork a deleted snapshot.
#[tokio::test]
async fn a_deleted_snapshot_is_refused_by_a_host_whose_clock_is_behind() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let behind = test_context();
    bootstrap_namespace(&store, &namespace_id, &behind)
        .await
        .expect("bootstrap namespace");
    let snapshot = snapshot_named(&store, &namespace_id, "nightly", None, &behind).await;
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", behind.now_ms + 1_000),
    )
    .engine()
    .delete_snapshot(&snapshot)
    .await
    .expect("delete snapshot");

    let fixture = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &behind);
    let head = fixture.namespace.load_read_state().await.expect("head");
    let engine = fixture.engine();
    assert_eq!(
        fixture
            .namespace
            .load_snapshot_read_basis(&head, &snapshot)
            .await
            .expect_err("read through a deleted snapshot")
            .code(),
        ErrorCode::SnapshotNotFound
    );
    assert_eq!(
        engine
            .extend_snapshot(&snapshot, behind.now_ms + 60_000)
            .await
            .expect_err("extend a deleted snapshot")
            .code(),
        ErrorCode::SnapshotNotFound
    );
    assert_eq!(
        engine
            .fork_namespace_at_snapshot(
                &NamespaceId::parse("clone").expect("namespace id"),
                &loonfs_test_support::test_actor(),
                &snapshot,
            )
            .await
            .expect_err("fork a deleted snapshot")
            .code(),
        ErrorCode::SnapshotNotFound
    );
}
