use crate::provider_env::{
    provider_env_example_contents, AwsS3ConformanceConfig, AzureAbsConformanceConfig,
    CloudflareR2ConformanceConfig, GcpGcsConformanceConfig, AWS_S3_OPTIONAL_VARS,
    AWS_S3_REQUIRED_VARS, AZURE_ABS_OPTIONAL_VARS, AZURE_ABS_REQUIRED_VARS,
    CLOUDFLARE_R2_OPTIONAL_VARS, CLOUDFLARE_R2_REQUIRED_VARS, GCP_GCS_OPTIONAL_VARS,
    GCP_GCS_REQUIRED_VARS,
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use loonfs_api::{Checksum, ContentId, ManifestObjectId};
use loonfs_objectstore::abs::{AzureAbsStore, AzureAbsStoreConfig};
use loonfs_objectstore::gcs::{GcpGcsStore, GcpGcsStoreConfig};
use loonfs_objectstore::keys::{
    content_blob, metadata_manifest_object, metadata_segment, wal_head, wal_segment,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::probe::{run_store_contract_probe, StoreProbeOutcome, StoreProbeReport};
use loonfs_objectstore::s3_compatible::{
    AwsS3StoreConfig, CloudflareR2StoreConfig, S3CompatibleStore,
};
use loonfs_objectstore::ObjectStoreError;
use loonfs_objectstore::{AwsS3Credentials, ObjectStore};
use tempfile::TempDir;

#[test]
fn key_builders_cover_locked_object_families() {
    assert_eq!(
        wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id")),
        "namespaces/ns-1/wal/head.json"
    );
    assert_eq!(
        content_blob(
            &loonfs_api::ContentStoreId::parse("cs_00000000000000000000000000000001").expect("valid content store id"),
            &ContentId::parse("con_abcdef0123456789abcdef0123456789").expect("valid content id"),
        ),
        "content-stores/cs_00000000000000000000000000000001/objects/ab/cd/con_abcdef0123456789abcdef0123456789"
    );
    assert_eq!(
        wal_segment(
            &loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"),
            &loonfs_api::WalSegmentId::parse("wal_00000000000000000001-644e4d336fd4ee33")
                .expect("valid WAL segment id")
        ),
        "namespaces/ns-1/wal/segments/wal_00000000000000000001-644e4d336fd4ee33.wal.zst"
    );
    assert_eq!(
        metadata_manifest_object(
            &loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"),
            &ManifestObjectId::parse("man_00000000000000000420-0123456789abcdef")
                .expect("valid manifest object id"),
        ),
        "namespaces/ns-1/metadata/manifests/man_00000000000000000420-0123456789abcdef.manifest.json"
    );
    assert_eq!(
        metadata_segment(
            &loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"),
            &loonfs_api::MetadataSegmentId::parse("seg_00000000000000000000000000000001")
                .expect("valid metadata segment id")
        ),
        "namespaces/ns-1/metadata/segments/seg_00000000000000000000000000000001.sst.zst"
    );
    assert_eq!(
        metadata_segment(
            &loonfs_api::NamespaceId::parse("source-ns").expect("valid namespace id"),
            &loonfs_api::MetadataSegmentId::parse("seg_00000000000000000000000000000002")
                .expect("valid metadata segment id")
        ),
        "namespaces/source-ns/metadata/segments/seg_00000000000000000000000000000002.sst.zst"
    );
    assert_eq!(
        metadata_segment(
            &loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"),
            &loonfs_api::MetadataSegmentId::parse("seg_ffffffffffffffffffffffffffffffff")
                .expect("valid metadata segment id")
        ),
        "namespaces/ns-1/metadata/segments/seg_ffffffffffffffffffffffffffffffff.sst.zst"
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
async fn local_fs_passes_the_store_contract_probe() {
    let temp_dir = test_dir("contract-probe");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_store_contract_probe_passes(&store, false).await;
    assert_start_after_contract(&store).await;
}

#[tokio::test]
async fn local_fs_rejects_path_traversal_keys() {
    let temp_dir = test_dir("invalid-key");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_rejects_invalid_keys_consistently(&store).await;
}

#[tokio::test]
async fn local_fs_streamed_write_round_trips() {
    let temp_dir = test_dir("streamed-write");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_streamed_write_round_trips(&store).await;
}

#[tokio::test]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_real_provider_conformance() {
    let config = AwsS3ConformanceConfig::from_env()
        .expect("load AWS S3 real-provider conformance environment");
    let store = S3CompatibleStore::aws_s3(AwsS3StoreConfig {
        bucket: config.bucket,
        region: config.region,
        endpoint_url: config.endpoint,
        credentials: AwsS3Credentials::Static {
            access_key_id: config.access_key_id,
            secret_access_key: config.secret_access_key,
            session_token: config.session_token,
        },
        key_prefix: Some(config.prefix),
        force_path_style: false,
    })
    .expect("create AWS S3 object store");
    assert_provider_conformance(&store, true).await;
}

#[tokio::test]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_streamed_write_round_trips() {
    let config = AwsS3ConformanceConfig::from_env()
        .expect("load AWS S3 real-provider conformance environment");
    let store = S3CompatibleStore::aws_s3(AwsS3StoreConfig {
        bucket: config.bucket,
        region: config.region,
        endpoint_url: config.endpoint,
        credentials: AwsS3Credentials::Static {
            access_key_id: config.access_key_id,
            secret_access_key: config.secret_access_key,
            session_token: config.session_token,
        },
        key_prefix: Some(config.prefix),
        force_path_style: false,
    })
    .expect("create AWS S3 object store");
    assert_streamed_write_round_trips(&store).await;
}

#[tokio::test]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_streamed_write_round_trips() {
    let config = CloudflareR2ConformanceConfig::from_env()
        .expect("load Cloudflare R2 real-provider conformance environment");
    let store = S3CompatibleStore::cloudflare_r2(CloudflareR2StoreConfig {
        bucket: config.bucket,
        account_id: config.account_id,
        endpoint_url: config.endpoint,
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
        key_prefix: Some(config.prefix),
    })
    .expect("create Cloudflare R2 object store");
    assert_streamed_write_round_trips(&store).await;
}

#[tokio::test]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_put_stores_a_trustworthy_checksum() {
    let config = AwsS3ConformanceConfig::from_env()
        .expect("load AWS S3 real-provider conformance environment");
    let store = S3CompatibleStore::aws_s3(AwsS3StoreConfig {
        bucket: config.bucket,
        region: config.region,
        endpoint_url: config.endpoint,
        credentials: AwsS3Credentials::Static {
            access_key_id: config.access_key_id,
            secret_access_key: config.secret_access_key,
            session_token: config.session_token,
        },
        key_prefix: Some(config.prefix),
        force_path_style: false,
    })
    .expect("create AWS S3 object store");
    assert_put_stores_a_trustworthy_checksum(&store, "aws-s3").await;
}

#[tokio::test]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_checksumless_put_stores_a_trustworthy_checksum() {
    let config = CloudflareR2ConformanceConfig::from_env()
        .expect("load Cloudflare R2 real-provider conformance environment");
    let store = S3CompatibleStore::cloudflare_r2(CloudflareR2StoreConfig {
        bucket: config.bucket,
        account_id: config.account_id,
        endpoint_url: config.endpoint,
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
        key_prefix: Some(config.prefix),
    })
    .expect("create Cloudflare R2 object store");
    assert_put_stores_a_trustworthy_checksum(&store, "cloudflare-r2").await;
}

#[tokio::test]
#[ignore = "requires real GCP GCS credentials"]
async fn gcp_gcs_checksumless_put_stores_a_trustworthy_checksum() {
    let config = GcpGcsConformanceConfig::from_env()
        .expect("load GCP GCS real-provider conformance environment");
    let store = GcpGcsStore::new(GcpGcsStoreConfig {
        bucket: config.bucket,
        service_account_key_path: config.service_account_key_path,
        key_prefix: Some(config.prefix),
    })
    .expect("create GCP GCS object store");
    assert_put_stores_a_trustworthy_checksum(&store, "gcp-gcs").await;
}

#[test]
#[ignore = "requires real AWS S3 credentials"]
fn aws_s3_store_survives_alternating_current_thread_runtimes() {
    // Regression probe for the 30s stall: a provider client driven from two
    // current-thread runtimes parked pooled connections until the client
    // timeout fired. The store-owned IO runtime decouples HTTP driving from
    // caller runtime topology, so alternating runtimes stays fast.
    let config = AwsS3ConformanceConfig::from_env()
        .expect("load AWS S3 real-provider conformance environment");
    let store = S3CompatibleStore::aws_s3(AwsS3StoreConfig {
        bucket: config.bucket,
        region: config.region,
        endpoint_url: config.endpoint,
        credentials: AwsS3Credentials::Static {
            access_key_id: config.access_key_id,
            secret_access_key: config.secret_access_key,
            session_token: config.session_token,
        },
        key_prefix: Some(config.prefix),
        force_path_style: false,
    })
    .expect("create AWS S3 object store");

    let runtime_a = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime a");
    let runtime_b = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime b");

    #[allow(clippy::disallowed_methods)]
    // Wall-clock bounds a live-provider stall check; no protocol time depends on it.
    let started = std::time::Instant::now();
    for round in 0..5u32 {
        let key = format!("runtime-affinity-probe/round-{round}.bin");
        let bytes = Bytes::from(format!("round {round}"));
        runtime_a
            .block_on(store.put_overwrite(&key, bytes))
            .expect("put on runtime a");
        let head = runtime_b
            .block_on(store.head(&key))
            .expect("head on runtime b");
        assert!(head.is_some(), "object should exist after put");
        let body = runtime_a
            .block_on(store.get(&key, None))
            .expect("get on runtime a");
        assert!(body.is_some(), "object body should read back");
        runtime_b
            .block_on(store.delete(&key))
            .expect("delete on runtime b");
    }
    #[allow(clippy::disallowed_methods)]
    // Same wall-clock boundary as above; 20 rounds of small ops complete in
    // seconds unless a request parks until the 30s client timeout.
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(25),
        "alternating-runtime ops should never park until the client timeout; took {elapsed:?}"
    );
}

#[tokio::test]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_real_provider_conformance() {
    let config = CloudflareR2ConformanceConfig::from_env()
        .expect("load Cloudflare R2 real-provider conformance environment");
    let store = S3CompatibleStore::cloudflare_r2(CloudflareR2StoreConfig {
        bucket: config.bucket,
        account_id: config.account_id,
        endpoint_url: config.endpoint,
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
        key_prefix: Some(config.prefix),
    })
    .expect("create Cloudflare R2 object store");
    assert_provider_conformance(&store, true).await;
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
    assert_provider_conformance(&store, true).await;
}

#[tokio::test]
#[ignore = "requires real GCP GCS credentials"]
async fn gcp_gcs_streamed_write_round_trips() {
    let config = GcpGcsConformanceConfig::from_env()
        .expect("load GCP GCS real-provider conformance environment");
    let store = GcpGcsStore::new(GcpGcsStoreConfig {
        bucket: config.bucket,
        service_account_key_path: config.service_account_key_path,
        key_prefix: Some(config.prefix),
    })
    .expect("create GCP GCS object store");
    assert_streamed_write_round_trips(&store).await;
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
    assert_provider_conformance(&store, false).await;
}

#[tokio::test]
#[ignore = "requires real Azure Blob Storage credentials"]
async fn azure_abs_streamed_write_round_trips() {
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
    assert_streamed_write_round_trips(&store).await;
}

/// The live provider sweep: the store contract probe, plus the key
/// rejection the probe deliberately leaves to tests.
///
/// The probe is the production surface an operator runs, and running it
/// here is what stops the two from drifting: a contract check changes for
/// production and for this sweep in one edit, or not at all.
async fn assert_provider_conformance(store: &dyn ObjectStore, direct_put_proven: bool) {
    assert_store_contract_probe_passes(store, direct_put_proven).await;
    assert_start_after_contract(store).await;
    assert_rejects_invalid_keys_consistently(store).await;
}

/// Checks that every provider resumes after the given key in sorted order.
async fn assert_start_after_contract(store: &dyn ObjectStore) {
    let run_id = loonfs_api::generated_id("list");
    let prefix = format!("start-after/{run_id}/");
    let keys = [
        format!("{prefix}a"),
        format!("{prefix}b"),
        format!("{prefix}c"),
    ];
    for key in &keys {
        store
            .put_overwrite(key, Bytes::from_static(b"listed"))
            .await
            .expect("write start-after fixture");
    }

    let all = store
        .list_prefix_from_stream(&prefix, None)
        .try_collect::<Vec<_>>()
        .await
        .expect("list from prefix start");
    assert_eq!(all, keys);

    let after_exact = store
        .list_prefix_from_stream(&prefix, Some(&keys[0]))
        .try_collect::<Vec<_>>()
        .await
        .expect("list after exact key");
    assert_eq!(after_exact, keys[1..]);

    let between = format!("{prefix}bb");
    let after_gap = store
        .list_prefix_from_stream(&prefix, Some(&between))
        .try_collect::<Vec<_>>()
        .await
        .expect("list after absent key");
    assert_eq!(after_gap, keys[2..]);

    let after_end = format!("{prefix}z");
    let complete = store
        .list_prefix_from_stream(&prefix, Some(&after_end))
        .try_collect::<Vec<_>>()
        .await
        .expect("list after prefix end");
    assert!(complete.is_empty());

    for key in &keys {
        store.delete(key).await.expect("delete start-after fixture");
    }
}

/// Requires every probe check to pass, and prints the whole report when one
/// does not.
///
/// The report is the point of a live run. Cloudflare R2's 501 answer to
/// `GetObjectAttributes` was read off exactly this output, so a failure
/// shows every check's verdict rather than the first one that broke.
///
/// `stored_checksum_readback` may return `unsupported` only for providers that
/// do not offer direct PUT. AWS S3, Cloudflare R2, and GCS are tested with
/// direct PUT enabled, so they must return stored checksums. Every other probe
/// check must pass for every provider.
async fn assert_store_contract_probe_passes(store: &dyn ObjectStore, direct_put_proven: bool) {
    let run_id = loonfs_api::generated_id("probe");
    let report = run_store_contract_probe(store, &run_id).await;
    let acceptable = report.checks.iter().all(|check| match check.outcome {
        StoreProbeOutcome::Passed => true,
        StoreProbeOutcome::Unsupported => {
            check.name == "stored_checksum_readback" && !direct_put_proven
        }
        StoreProbeOutcome::Failed { .. } => false,
    });
    assert!(
        acceptable,
        "store contract probe {run_id} did not pass:\n{}",
        probe_report_lines(&report)
    );
}

fn probe_report_lines(report: &StoreProbeReport) -> String {
    report
        .checks
        .iter()
        .map(|check| format!("  {}", check.check_line()))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Returns the object key used by the stored-checksum test.
fn stored_checksum_test_key() -> String {
    content_blob(
        &loonfs_api::ContentStoreId::parse("cs_00000000000000000000000000000001")
            .expect("valid content store id"),
        &ContentId::parse("con_9a41c07d55e2410fb3c6d8e1f2a3b4c5").expect("valid content id"),
    )
}

/// Verifies that provider metadata matches the uploaded bytes.
async fn assert_put_stores_a_trustworthy_checksum<S: ObjectStore>(store: &S, provider: &str) {
    let key = stored_checksum_test_key();
    let _ = store.delete(&key).await;

    let payload: Vec<u8> = (0..1_000_003usize)
        .map(|index| (index % 241) as u8)
        .collect();
    store
        .put(
            &key,
            Bytes::from(payload.clone()),
            loonfs_objectstore::PutMode::CreateIfAbsent,
        )
        .await
        .expect("upload object");

    let stored = store
        .head_stored_checksum(&key)
        .await
        .expect("head the stored checksum");
    assert!(
        stored.is_some(),
        "{provider}: no stored checksum reported after PUT"
    );
    let stored = stored.expect("stored checksum present");
    assert_eq!(
        stored.size_bytes,
        payload.len() as u64,
        "{provider}: stored size does not match the uploaded bytes"
    );
    let local = Checksum::compute(stored.checksum.algorithm, &payload);
    assert_eq!(
        stored.checksum, local,
        "{provider}: stored checksum does not match the uploaded bytes"
    );
    store.delete(&key).await.expect("delete the test object");
}

/// The content key a streamed-write exercise writes to and cleans up.
fn streamed_write_key() -> String {
    content_blob(
        &loonfs_api::ContentStoreId::parse("cs_00000000000000000000000000000001")
            .expect("valid content store id"),
        &ContentId::parse("con_5723ea9d1c4b48f0a1d2e3f4a5b6c7d8").expect("valid content id"),
    )
}

/// Cuts a payload into stream chunks whose boundaries have nothing to do
/// with the store's part size, exactly as an HTTP body's do not.
fn streamed_chunks(payload: &[u8], chunk_bytes: usize) -> loonfs_objectstore::ByteStream {
    let chunks: Vec<Bytes> = payload
        .chunks(chunk_bytes)
        .map(Bytes::copy_from_slice)
        .collect();
    futures::stream::iter(chunks.into_iter().map(Ok)).boxed()
}

/// A proxied write's shape against a real provider: a payload larger than
/// the store's part size, delivered as a stream, must land byte-identical
/// and leave the prefix it borrowed empty afterwards.
///
/// Three internal parts is the smallest payload with a middle part, which
/// is where a provider's own rules about non-final part sizes bite.
async fn assert_streamed_write_round_trips<S: ObjectStore>(store: &S) {
    let key = streamed_write_key();
    let _ = store.delete(&key).await;

    let payload_len = 3 * loonfs_objectstore::PROVIDER_MULTIPART_PART_BYTES as usize;
    let payload: Vec<u8> = (0..payload_len).map(|index| (index % 251) as u8).collect();
    let expected = Checksum::sha256(&payload);

    let size_bytes = store
        .put_streamed(
            &key,
            streamed_chunks(&payload, 64 * 1024),
            loonfs_objectstore::PutMode::CreateIfAbsent,
        )
        .await
        .expect("streamed write of a multi-part payload");
    assert_eq!(size_bytes, payload_len as u64);

    let read_back = store
        .get(&key, None)
        .await
        .expect("read the streamed object back")
        .expect("streamed object exists");
    assert_eq!(read_back.len(), payload_len);
    assert_eq!(
        Checksum::sha256(&read_back),
        expected,
        "the assembled object must hash to what was streamed into it"
    );

    store.delete(&key).await.expect("delete streamed object");
    let prefix = key.rsplit_once('/').expect("content key has a shard").0;
    assert!(
        store
            .list_prefix(&format!("{prefix}/"))
            .await
            .expect("list the streamed object's shard")
            .is_empty(),
        "a streamed-write exercise leaves nothing behind"
    );
}

async fn assert_rejects_invalid_keys_consistently(store: &dyn ObjectStore) {
    fn assert_invalid_key<T: std::fmt::Debug>(key: &str, result: Result<T, ObjectStoreError>) {
        let carries_rejected_key = matches!(
            &result,
            Err(ObjectStoreError::InvalidKey { object_key, .. }) if object_key == key
        );
        assert!(
            carries_rejected_key,
            "expected invalid key error for `{key}`, got {result:?}"
        );
    }

    for key in ["../escape", "namespaces//bad", "./escape"] {
        assert_invalid_key(key, store.head(key).await);
        assert_invalid_key(key, store.get(key, None).await);
        assert_invalid_key(
            key,
            store.put_if_absent(key, Bytes::from_static(b"oops")).await,
        );
        assert_invalid_key(
            key,
            store.put_overwrite(key, Bytes::from_static(b"oops")).await,
        );
        assert_invalid_key(
            key,
            store
                .compare_and_swap(key, "etag", Bytes::from_static(b"oops"))
                .await,
        );
        assert_invalid_key(key, store.delete(key).await);
        assert_invalid_key(key, store.list_prefix(key).await);
        assert_invalid_key(
            key,
            store
                .list_prefix_from_stream("valid/", Some(key))
                .try_collect::<Vec<_>>()
                .await,
        );
    }
}

fn test_dir(label: &str) -> TempDir {
    tempfile::Builder::new()
        .prefix(&format!("loonfs-objectstore-{label}-"))
        .tempdir()
        .expect("create temp dir")
}
