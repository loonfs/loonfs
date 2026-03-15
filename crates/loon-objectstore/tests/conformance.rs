mod provider_env;

use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{
    derived_progress, namespace_head, namespace_lease, queue_shard, snapshot_manifest,
    snapshot_table, wal_commit, SnapshotTableFamily,
};
use loon_objectstore::provider::{Expectation, AWS_S3, CLOUDFLARE_R2, LOCAL_FS};
use loon_objectstore::r2::{R2Store, R2StoreConfig};
use loon_objectstore::s3::{AwsS3Store, AwsS3StoreConfig};
use loon_objectstore::{ByteRange, ObjectStore};
use provider_env::{
    provider_env_example_contents, AwsS3ConformanceConfig, CloudflareR2ConformanceConfig,
    AWS_S3_OPTIONAL_VARS, AWS_S3_REQUIRED_VARS, CLOUDFLARE_R2_OPTIONAL_VARS,
    CLOUDFLARE_R2_REQUIRED_VARS,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn provider_profiles_exist() {
    assert_eq!(LOCAL_FS.name, "local-fs");
    assert_eq!(AWS_S3.name, "aws-s3");
    assert_eq!(CLOUDFLARE_R2.name, "cloudflare-r2");
    assert_eq!(
        LOCAL_FS.provider_etag_is_canonical_identity,
        Expectation::ExpectedNo
    );
    assert_eq!(
        AWS_S3.provider_etag_is_canonical_identity,
        Expectation::ExpectedNo
    );
    assert_eq!(
        CLOUDFLARE_R2.provider_etag_is_canonical_identity,
        Expectation::ExpectedNo
    );
}

#[test]
fn key_builders_cover_locked_object_families() {
    assert_eq!(namespace_head("ns-1"), "namespaces/ns-1/head.json");
    assert_eq!(namespace_lease("ns-1"), "namespaces/ns-1/lease.json");
    assert_eq!(
        wal_commit("ns-1", 420, "commit-1"),
        "namespaces/ns-1/wal/00000000000000000420-commit-1.cbor.zst"
    );
    assert_eq!(
        snapshot_manifest("ns-1", 420),
        "namespaces/ns-1/snapshots/00000000000000000420/manifest.json"
    );
    assert_eq!(
        snapshot_table("ns-1", 420, SnapshotTableFamily::Inodes, 3),
        "namespaces/ns-1/snapshots/00000000000000000420/tables/inodes-00003.sst.zst"
    );
    assert_eq!(
        derived_progress("ns-1", "BuildSnapshot"),
        "namespaces/ns-1/derived/BuildSnapshot/progress.json"
    );
    assert_eq!(queue_shard(12), "queue/shards/00012.json");
}

#[test]
fn provider_env_example_covers_real_provider_contract() {
    let example = provider_env_example_contents().expect("read provider env example");
    for name in AWS_S3_REQUIRED_VARS
        .iter()
        .chain(AWS_S3_OPTIONAL_VARS.iter())
        .chain(CLOUDFLARE_R2_REQUIRED_VARS.iter())
        .chain(CLOUDFLARE_R2_OPTIONAL_VARS.iter())
    {
        assert!(
            example.contains(name),
            "provider env example should contain {name}"
        );
    }
}

#[test]
fn local_fs_create_if_absent_is_enforced() {
    let temp_dir = TestDir::new("create-if-absent");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_create_if_absent_is_enforced(&store);
}

#[test]
fn local_fs_compare_and_swap_rejects_stale_writer() {
    let temp_dir = TestDir::new("cas");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_compare_and_swap_rejects_stale_writer(&store);
}

#[test]
fn local_fs_lists_immediately_after_write_and_delete() {
    let temp_dir = TestDir::new("listing");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_lists_immediately_after_write_and_delete(&store);
}

#[test]
fn local_fs_supports_range_reads() {
    let temp_dir = TestDir::new("ranges");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_supports_range_reads(&store);
}

#[test]
fn local_fs_rejects_path_traversal_keys() {
    let temp_dir = TestDir::new("invalid-key");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_rejects_path_traversal_keys(&store);
}

#[test]
#[ignore = "requires real AWS S3 credentials"]
fn aws_s3_real_provider_conformance() {
    let config = AwsS3ConformanceConfig::from_env()
        .expect("load AWS S3 real-provider conformance environment");
    let store = AwsS3Store::new(AwsS3StoreConfig {
        bucket: config.bucket,
        region: config.region,
        endpoint_url: config.endpoint,
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
        session_token: config.session_token,
        key_prefix: Some(config.prefix),
        force_path_style: false,
    })
    .expect("create AWS S3 object store");

    assert_provider_conformance(&store);
}

#[test]
#[ignore = "requires real Cloudflare R2 credentials"]
fn cloudflare_r2_real_provider_conformance() {
    let config = CloudflareR2ConformanceConfig::from_env()
        .expect("load Cloudflare R2 real-provider conformance environment");
    let store = R2Store::new(R2StoreConfig {
        bucket: config.bucket,
        account_id: config.account_id,
        endpoint_url: config.endpoint,
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
        key_prefix: Some(config.prefix),
    })
    .expect("create Cloudflare R2 object store");

    assert_provider_conformance(&store);
}

fn assert_provider_conformance<S: ObjectStore>(store: &S) {
    assert_create_if_absent_is_enforced(store);
    assert_compare_and_swap_rejects_stale_writer(store);
    assert_lists_immediately_after_write_and_delete(store);
    assert_supports_range_reads(store);
    assert_rejects_path_traversal_keys(store);
}

fn assert_create_if_absent_is_enforced<S: ObjectStore>(store: &S) {
    let key = namespace_head("ns-1");
    let _ = store.delete(&key);

    store
        .put_if_absent(&key, br#"{"seq":41}"#)
        .expect("initial create should succeed");

    let second = store.put_if_absent(&key, br#"{"seq":42}"#);
    assert_precondition_failed(second);

    assert_eq!(
        store
            .get(&key, None)
            .expect("read body")
            .expect("body exists"),
        br#"{"seq":41}"#
    );

    store.delete(&key).expect("cleanup create-if-absent object");
}

fn assert_compare_and_swap_rejects_stale_writer<S: ObjectStore>(store: &S) {
    let key = namespace_head("ns-1");
    let _ = store.delete(&key);

    store
        .put_if_absent(&key, br#"{"seq":41,"fence_token":8}"#)
        .expect("seed initial head");
    let first_read = store
        .head(&key)
        .expect("head read")
        .expect("head should exist")
        .etag
        .expect("etag should exist");

    store
        .compare_and_swap(&key, &first_read, br#"{"seq":42,"fence_token":8}"#)
        .expect("first CAS should succeed");

    let stale = store.compare_and_swap(&key, &first_read, br#"{"seq":42,"fence_token":9}"#);
    assert_precondition_failed(stale);

    assert_eq!(
        store
            .get(&key, None)
            .expect("read body")
            .expect("body exists"),
        br#"{"seq":42,"fence_token":8}"#
    );

    store.delete(&key).expect("cleanup CAS object");
}

fn assert_lists_immediately_after_write_and_delete<S: ObjectStore>(store: &S) {
    let key = derived_progress("ns-1", "BuildSnapshot");
    let _ = store.delete(&key);

    store
        .put_if_absent(&key, br#"{"built_through_seq":420}"#)
        .expect("create progress object");
    assert_eq!(
        store
            .list_prefix("namespaces/ns-1/derived/")
            .expect("list after write"),
        vec![key.clone()]
    );

    store.delete(&key).expect("delete progress object");
    assert!(store
        .list_prefix("namespaces/ns-1/derived/")
        .expect("list after delete")
        .is_empty());
}

fn assert_supports_range_reads<S: ObjectStore>(store: &S) {
    let key = wal_commit("ns-1", 420, "commit-1");
    let _ = store.delete(&key);

    store
        .put_if_absent(&key, b"abcdef")
        .expect("create wal object");

    let range = store
        .get(
            &key,
            Some(ByteRange {
                start_inclusive: 1,
                end_exclusive: 4,
            }),
        )
        .expect("range read")
        .expect("range body should exist");

    assert_eq!(range, b"bcd");

    store.delete(&key).expect("cleanup range object");
}

fn assert_rejects_path_traversal_keys<S: ObjectStore>(store: &S) {
    let result = store.put_if_absent("../escape", b"oops");
    assert!(matches!(result, Err(ObjectStoreError::InvalidKey(_))));
}

fn assert_precondition_failed<T>(result: Result<T, ObjectStoreError>) {
    assert!(matches!(result, Err(ObjectStoreError::PreconditionFailed)));
}

#[derive(Debug)]
struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(label: &str) -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "loondb-objectstore-{label}-{}-{stamp}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
