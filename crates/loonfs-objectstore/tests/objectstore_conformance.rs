mod provider_env;

use bytes::Bytes;
use loonfs_api::ManifestId;
use loonfs_objectstore::abs::{AzureAbsStore, AzureAbsStoreConfig};
use loonfs_objectstore::fs::LocalFsStore;
use loonfs_objectstore::gcs::{GcpGcsStore, GcpGcsStoreConfig};
use loonfs_objectstore::keys::{
    content_blob, content_store_descriptor, derived_progress, metadata_sst, namespace_descriptor,
    namespace_head, namespace_lease, namespace_manifest, wal_segment, DerivedWorkClass,
};
use loonfs_objectstore::probes::run_contract_probes;
use loonfs_objectstore::provider::{
    Expectation, AWS_S3, AZURE_ABS, CLOUDFLARE_R2, GCP_GCS, LOCAL_FS,
};
use loonfs_objectstore::r2::{CloudflareR2Store, CloudflareR2StoreConfig};
use loonfs_objectstore::s3::{AwsS3Store, AwsS3StoreConfig};
use loonfs_objectstore::ObjectStoreError;
use loonfs_objectstore::{ByteRange, ObjectStore};
use provider_env::{
    provider_env_example_contents, AwsS3ConformanceConfig, AzureAbsConformanceConfig,
    CloudflareR2ConformanceConfig, GcpGcsConformanceConfig, AWS_S3_OPTIONAL_VARS,
    AWS_S3_REQUIRED_VARS, AZURE_ABS_OPTIONAL_VARS, AZURE_ABS_REQUIRED_VARS,
    CLOUDFLARE_R2_OPTIONAL_VARS, CLOUDFLARE_R2_REQUIRED_VARS, GCP_GCS_OPTIONAL_VARS,
    GCP_GCS_REQUIRED_VARS,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn provider_profiles_exist() {
    assert_eq!(LOCAL_FS.name, "local-fs");
    assert_eq!(AWS_S3.name, "aws-s3");
    assert_eq!(CLOUDFLARE_R2.name, "cloudflare-r2");
    assert_eq!(GCP_GCS.name, "gcp-gcs");
    assert_eq!(AZURE_ABS.name, "azure-abs");
    assert_eq!(
        LOCAL_FS.active_contract.opaque_compare_token_for_cas,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        LOCAL_FS.active_contract.full_object_read_identity,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        LOCAL_FS.active_contract.overwrite,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        LOCAL_FS.active_contract.delete_idempotent,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        LOCAL_FS
            .active_contract
            .head_reflects_latest_write_and_delete,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        LOCAL_FS.active_contract.scoped_key_prefixing,
        Expectation::ExpectedNo
    );
    assert_eq!(
        LOCAL_FS.active_contract.traversal_rejection,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        LOCAL_FS.active_contract.sorted_list_prefix,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        AWS_S3.active_contract.opaque_compare_token_for_cas,
        Expectation::ExpectedYes
    );
    assert_eq!(
        AWS_S3.active_contract.full_object_read_identity,
        Expectation::ExpectedYes
    );
    assert_eq!(AWS_S3.active_contract.overwrite, Expectation::ExpectedYes);
    assert_eq!(
        AWS_S3.active_contract.delete_idempotent,
        Expectation::ExpectedYes
    );
    assert_eq!(
        AWS_S3.active_contract.head_reflects_latest_write_and_delete,
        Expectation::ExpectedYes
    );
    assert_eq!(
        AWS_S3.active_contract.scoped_key_prefixing,
        Expectation::ExpectedYes
    );
    assert_eq!(
        AWS_S3.active_contract.traversal_rejection,
        Expectation::ExpectedYes
    );
    assert_eq!(
        AWS_S3.active_contract.sorted_list_prefix,
        Expectation::ExpectedYes
    );
    assert_eq!(
        CLOUDFLARE_R2.active_contract.opaque_compare_token_for_cas,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        CLOUDFLARE_R2.active_contract.full_object_read_identity,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        CLOUDFLARE_R2.active_contract.overwrite,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        CLOUDFLARE_R2.active_contract.delete_idempotent,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        CLOUDFLARE_R2
            .active_contract
            .head_reflects_latest_write_and_delete,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        CLOUDFLARE_R2.active_contract.scoped_key_prefixing,
        Expectation::ExpectedYes
    );
    assert_eq!(
        CLOUDFLARE_R2.active_contract.traversal_rejection,
        Expectation::ExpectedYes
    );
    assert_eq!(
        CLOUDFLARE_R2.active_contract.sorted_list_prefix,
        Expectation::ExpectedYes
    );
    assert_eq!(
        GCP_GCS.active_contract.opaque_compare_token_for_cas,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        GCP_GCS.active_contract.full_object_read_identity,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        GCP_GCS.active_contract.overwrite,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        GCP_GCS.active_contract.delete_idempotent,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        GCP_GCS
            .active_contract
            .head_reflects_latest_write_and_delete,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        GCP_GCS.active_contract.scoped_key_prefixing,
        Expectation::ExpectedYes
    );
    assert_eq!(
        GCP_GCS.active_contract.traversal_rejection,
        Expectation::ExpectedYes
    );
    assert_eq!(
        GCP_GCS.active_contract.sorted_list_prefix,
        Expectation::ExpectedYes
    );
    assert_eq!(
        LOCAL_FS.future_capabilities.multipart_upload,
        Expectation::ExpectedNo
    );
    assert_eq!(
        AWS_S3.future_capabilities.multipart_upload,
        Expectation::ExpectedYes
    );
    assert_eq!(
        CLOUDFLARE_R2.future_capabilities.multipart_upload,
        Expectation::ExpectedYes
    );
    assert_eq!(
        GCP_GCS.future_capabilities.multipart_upload,
        Expectation::ExpectedYes
    );
    assert_eq!(
        AZURE_ABS.active_contract.compare_and_swap_small_object,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        AZURE_ABS.active_contract.full_object_read_identity,
        Expectation::VerifyByConformance
    );
    assert_eq!(
        AZURE_ABS.future_capabilities.multipart_upload,
        Expectation::ExpectedYes
    );
}

#[test]
fn key_builders_cover_locked_object_families() {
    assert_eq!(
        namespace_descriptor("ns-1"),
        "namespaces/ns-1/descriptor.json"
    );
    assert_eq!(namespace_head("ns-1"), "namespaces/ns-1/control/head.json");
    assert_eq!(
        namespace_lease("ns-1"),
        "namespaces/ns-1/control/lease.json"
    );
    assert_eq!(
        content_store_descriptor("cs_00000000000000000000000000000001"),
        "content-stores/cs_00000000000000000000000000000001/descriptor.json"
    );
    assert_eq!(
        content_blob(
            "cs_00000000000000000000000000000001",
            "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        )
        .expect("content blob key"),
        "content-stores/cs_00000000000000000000000000000001/blobs/sha256/ab/cd/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    );
    assert_eq!(
        wal_segment("ns-1", "seg_00000000000000000000000000000001"),
        "namespaces/ns-1/wal/seg_00000000000000000000000000000001.wal.zst"
    );
    assert_eq!(
        namespace_manifest("ns-1", ManifestId(420)),
        "namespaces/ns-1/manifest/00000000000000000420.manifest.json"
    );
    assert_eq!(
        metadata_sst("ns-1", "tbl_00000000000000000000000000000001"),
        "namespaces/ns-1/tables/metadata/tbl_00000000000000000000000000000001.sst.zst"
    );
    assert_eq!(
        metadata_sst("source-ns", "tbl_00000000000000000000000000000002"),
        "namespaces/source-ns/tables/metadata/tbl_00000000000000000000000000000002.sst.zst"
    );
    assert_eq!(
        metadata_sst("ns-1", "tbl_ffffffffffffffffffffffffffffffff"),
        "namespaces/ns-1/tables/metadata/tbl_ffffffffffffffffffffffffffffffff.sst.zst"
    );
    assert_eq!(
        derived_progress("ns-1", DerivedWorkClass::ManifestBuilder),
        "namespaces/ns-1/derived/manifest-builder/progress.json"
    );
}

#[test]
fn provider_env_example_covers_real_provider_contract() {
    let example = provider_env_example_contents().expect("read provider env example");
    for name in AWS_S3_REQUIRED_VARS
        .iter()
        .chain(AWS_S3_OPTIONAL_VARS.iter())
        .chain(CLOUDFLARE_R2_REQUIRED_VARS.iter())
        .chain(CLOUDFLARE_R2_OPTIONAL_VARS.iter())
        .chain(GCP_GCS_REQUIRED_VARS.iter())
        .chain(GCP_GCS_OPTIONAL_VARS.iter())
        .chain(AZURE_ABS_REQUIRED_VARS.iter())
        .chain(AZURE_ABS_OPTIONAL_VARS.iter())
    {
        assert!(
            example.contains(name),
            "provider env example should contain {name}"
        );
    }
}

#[tokio::test]
async fn local_fs_create_if_absent_is_enforced() {
    let temp_dir = TestDir::new("create-if-absent");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_create_if_absent_is_enforced(&store).await;
}

#[tokio::test]
async fn local_fs_compare_and_swap_rejects_stale_writer() {
    let temp_dir = TestDir::new("cas");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_compare_and_swap_rejects_stale_writer(&store).await;
}

#[tokio::test]
async fn local_fs_overwrite_visibility_and_delete_idempotence() {
    let temp_dir = TestDir::new("overwrite-delete");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_overwrite_updates_head_and_body(&store).await;
    assert_get_with_metadata_returns_body_and_identity(&store).await;
    assert_delete_missing_is_idempotent(&store).await;
}

#[tokio::test]
async fn local_fs_lists_immediately_after_write_and_delete() {
    let temp_dir = TestDir::new("listing");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_lists_immediately_after_write_and_delete(&store).await;
    assert_sorted_list_prefix(&store).await;
}

#[tokio::test]
async fn local_fs_supports_range_reads() {
    let temp_dir = TestDir::new("ranges");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_supports_range_reads(&store).await;
}

#[tokio::test]
async fn local_fs_rejects_path_traversal_keys() {
    let temp_dir = TestDir::new("invalid-key");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_rejects_invalid_keys_consistently(&store).await;
}

#[tokio::test]
async fn local_fs_compare_and_swap_missing_object_rejects_writer() {
    let temp_dir = TestDir::new("cas-missing");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_compare_and_swap_missing_object_rejects_writer(&store).await;
}

#[tokio::test]
async fn local_fs_contract_probes_match_doctor_surface() {
    let temp_dir = TestDir::new("doctor-probes");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    let report = run_contract_probes(&store, "local-fs-doctor")
        .await
        .expect("run doctor probes");
    assert_eq!(
        report.checks,
        vec![
            "create_if_absent",
            "compare_and_swap",
            "get_with_metadata",
            "visibility_after_write",
            "visibility_after_delete",
            "sorted_listing",
            "scoped_prefix_behavior",
        ]
    );
}

#[tokio::test]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_real_provider_conformance() {
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
    assert_provider_conformance(&store).await;
}

#[tokio::test]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_real_provider_conformance() {
    let config = CloudflareR2ConformanceConfig::from_env()
        .expect("load Cloudflare R2 real-provider conformance environment");
    let store = CloudflareR2Store::new(CloudflareR2StoreConfig {
        bucket: config.bucket,
        account_id: config.account_id,
        endpoint_url: config.endpoint,
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
        key_prefix: Some(config.prefix),
    })
    .expect("create Cloudflare R2 object store");
    assert_provider_conformance(&store).await;
}

#[tokio::test]
#[ignore = "requires real GCP GCS credentials"]
async fn gcp_gcs_real_provider_conformance() {
    let config = GcpGcsConformanceConfig::from_env()
        .expect("load GCP GCS real-provider conformance environment");
    let store = GcpGcsStore::new(GcpGcsStoreConfig {
        bucket: config.bucket,
        service_account_key_path: config.service_account_key_path,
        key_prefix: Some(config.prefix),
    })
    .expect("create GCP GCS object store");
    assert_provider_conformance(&store).await;
}

#[tokio::test]
#[ignore = "requires real Azure Blob Storage credentials"]
async fn azure_abs_real_provider_conformance() {
    let config = AzureAbsConformanceConfig::from_env()
        .expect("load Azure Blob Storage real-provider conformance environment");
    let store = AzureAbsStore::new(AzureAbsStoreConfig {
        account_name: config.account_name,
        container_name: config.container_name,
        access_key: config.access_key,
        endpoint_url: config.endpoint,
        key_prefix: Some(config.prefix),
    })
    .expect("create Azure Blob Storage object store");
    assert_provider_conformance(&store).await;
}

async fn assert_provider_conformance<S: ObjectStore>(store: &S) {
    run_contract_probes(store, "provider-conformance")
        .await
        .expect("run shared contract probes");
    assert_create_if_absent_is_enforced(store).await;
    assert_compare_and_swap_rejects_stale_writer(store).await;
    assert_compare_and_swap_missing_object_rejects_writer(store).await;
    assert_overwrite_updates_head_and_body(store).await;
    assert_get_with_metadata_returns_body_and_identity(store).await;
    assert_delete_missing_is_idempotent(store).await;
    assert_lists_immediately_after_write_and_delete(store).await;
    assert_sorted_list_prefix(store).await;
    assert_supports_range_reads(store).await;
    assert_rejects_invalid_keys_consistently(store).await;
}

async fn assert_get_with_metadata_returns_body_and_identity<S: ObjectStore>(store: &S) {
    let key = namespace_head("ns-1");
    let _ = store.delete(&key).await;

    let written = store
        .put_overwrite(
            &key,
            Bytes::from_static(br#"{"seq":41,"source":"get-with-metadata"}"#),
        )
        .await
        .expect("write object for get_with_metadata");
    let loaded = store
        .get_with_metadata(&key)
        .await
        .expect("get_with_metadata")
        .expect("object should exist");

    assert_eq!(loaded.bytes, br#"{"seq":41,"source":"get-with-metadata"}"#);
    assert_eq!(loaded.metadata.size_bytes, loaded.bytes.len() as u64);
    assert_eq!(loaded.metadata.etag, written.etag);

    store
        .delete(&key)
        .await
        .expect("cleanup get_with_metadata object");
}

async fn assert_create_if_absent_is_enforced<S: ObjectStore>(store: &S) {
    let key = namespace_head("ns-1");
    let _ = store.delete(&key).await;

    store
        .put_if_absent(&key, Bytes::from_static(br#"{"seq":41}"#))
        .await
        .expect("initial create should succeed");

    let second = store
        .put_if_absent(&key, Bytes::from_static(br#"{"seq":42}"#))
        .await;
    assert_precondition_failed(second);

    assert_eq!(
        store
            .get(&key, None)
            .await
            .expect("read body")
            .expect("body exists"),
        Bytes::from_static(br#"{"seq":41}"#)
    );

    store
        .delete(&key)
        .await
        .expect("cleanup create-if-absent object");
}

async fn assert_compare_and_swap_rejects_stale_writer<S: ObjectStore>(store: &S) {
    let key = namespace_head("ns-1");
    let _ = store.delete(&key).await;

    store
        .put_if_absent(&key, Bytes::from_static(br#"{"seq":41,"fence_token":8}"#))
        .await
        .expect("seed initial head");
    let first_read = store
        .head(&key)
        .await
        .expect("head read")
        .expect("head should exist")
        .etag
        .expect("etag should exist");

    store
        .compare_and_swap(
            &key,
            &first_read,
            Bytes::from_static(br#"{"seq":42,"fence_token":8}"#),
        )
        .await
        .expect("first CAS should succeed");

    let stale = store
        .compare_and_swap(
            &key,
            &first_read,
            Bytes::from_static(br#"{"seq":42,"fence_token":9}"#),
        )
        .await;
    assert_precondition_failed(stale);

    assert_eq!(
        store
            .get(&key, None)
            .await
            .expect("read body")
            .expect("body exists"),
        Bytes::from_static(br#"{"seq":42,"fence_token":8}"#)
    );

    store.delete(&key).await.expect("cleanup CAS object");
}

async fn assert_compare_and_swap_missing_object_rejects_writer<S: ObjectStore>(store: &S) {
    let key = namespace_head("ns-cas-missing");
    let _ = store.delete(&key).await;
    assert_precondition_failed(
        store
            .compare_and_swap(&key, "missing-etag", Bytes::from_static(br#"{"seq":1}"#))
            .await,
    );
}

async fn assert_overwrite_updates_head_and_body<S: ObjectStore>(store: &S) {
    let key = namespace_head("ns-overwrite");
    let _ = store.delete(&key).await;

    let first = store
        .put_overwrite(&key, Bytes::from_static(br#"{"seq":41}"#))
        .await
        .expect("initial overwrite should succeed");
    let second = store
        .put_overwrite(&key, Bytes::from_static(br#"{"seq":42}"#))
        .await
        .expect("second overwrite should succeed");

    assert_eq!(
        store
            .get(&key, None)
            .await
            .expect("read overwritten body")
            .expect("overwritten body exists"),
        Bytes::from_static(br#"{"seq":42}"#)
    );
    let head = store
        .head(&key)
        .await
        .expect("head after overwrite")
        .expect("overwritten object exists");
    assert_eq!(head.etag, second.etag);
    assert_eq!(head.size_bytes, second.size_bytes);
    assert_ne!(first, second, "overwrite should refresh visible metadata");

    store
        .delete(&key)
        .await
        .expect("cleanup overwritten object");
    assert_eq!(store.head(&key).await.expect("head after delete"), None);
}

async fn assert_delete_missing_is_idempotent<S: ObjectStore>(store: &S) {
    let key = namespace_head("ns-delete-missing");
    let _ = store.delete(&key).await;
    store
        .delete(&key)
        .await
        .expect("delete missing object should succeed");
    assert_eq!(store.head(&key).await.expect("head missing object"), None);
}

async fn assert_lists_immediately_after_write_and_delete<S: ObjectStore>(store: &S) {
    let key = derived_progress("ns-1", DerivedWorkClass::ManifestBuilder);
    let _ = store.delete(&key).await;

    store
        .put_if_absent(&key, Bytes::from_static(br#"{"built_through_seq":420}"#))
        .await
        .expect("create progress object");
    assert_eq!(
        store
            .list_prefix("namespaces/ns-1/derived/")
            .await
            .expect("list after write"),
        vec![key.clone()]
    );

    store.delete(&key).await.expect("delete progress object");
    assert!(store
        .list_prefix("namespaces/ns-1/derived/")
        .await
        .expect("list after delete")
        .is_empty());
}

async fn assert_sorted_list_prefix<S: ObjectStore>(store: &S) {
    let keys = vec![
        derived_progress("ns-sort", DerivedWorkClass::ManifestBuilder),
        namespace_head("ns-sort"),
        namespace_lease("ns-sort"),
    ];
    for key in &keys {
        let _ = store.delete(key).await;
    }

    store
        .put_if_absent(&keys[1], Bytes::from_static(br#"{"seq":1}"#))
        .await
        .expect("seed second sort key");
    store
        .put_if_absent(&keys[2], Bytes::from_static(br#"{"lease":1}"#))
        .await
        .expect("seed third sort key");
    store
        .put_if_absent(&keys[0], Bytes::from_static(br#"{"through_seq":1}"#))
        .await
        .expect("seed first sort key");

    let listed = store
        .list_prefix("namespaces/ns-sort/")
        .await
        .expect("list sorted keys");
    let mut expected = keys.clone();
    expected.sort();
    assert_eq!(listed, expected);

    for key in &keys {
        store.delete(key).await.expect("cleanup sort key");
    }
}

async fn assert_supports_range_reads<S: ObjectStore>(store: &S) {
    let key = wal_segment("ns-1", "seg_00000000000000000000000000000001");
    let _ = store.delete(&key).await;

    store
        .put_if_absent(&key, Bytes::from_static(b"abcdef"))
        .await
        .expect("create wal object");

    let range = store
        .get(
            &key,
            Some(ByteRange {
                start_inclusive: 1,
                end_exclusive: 4,
            }),
        )
        .await
        .expect("range read")
        .expect("range body should exist");

    assert_eq!(range, Bytes::from_static(b"bcd"));

    store.delete(&key).await.expect("cleanup range object");
}

async fn assert_rejects_invalid_keys_consistently<S: ObjectStore>(store: &S) {
    for key in ["../escape", "namespaces//bad", "./escape"] {
        assert!(matches!(
            store.head(key).await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
        assert!(matches!(
            store.get(key, None).await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
        assert!(matches!(
            store.put_if_absent(key, Bytes::from_static(b"oops")).await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
        assert!(matches!(
            store.put_overwrite(key, Bytes::from_static(b"oops")).await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
        assert!(matches!(
            store
                .compare_and_swap(key, "etag", Bytes::from_static(b"oops"))
                .await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
        assert!(matches!(
            store.delete(key).await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
        assert!(matches!(
            store.list_prefix(key).await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
    }
}

fn assert_precondition_failed<T>(result: Result<T, ObjectStoreError>) {
    assert!(matches!(result, Err(ObjectStoreError::PreconditionFailed)));
}

#[derive(Debug)]
struct TestDir {
    path: PathBuf,
}

impl TestDir {
    #[allow(clippy::disallowed_methods)]
    fn new(label: &str) -> Self {
        // Test-only unique paths are an entropy boundary, not protocol time.
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "loonfs-objectstore-{label}-{}-{stamp}",
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
