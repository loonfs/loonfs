use crate::provider_env::{
    provider_env_example_contents, AwsS3ConformanceConfig, AzureAbsConformanceConfig,
    CloudflareR2ConformanceConfig, GcpGcsConformanceConfig, AWS_S3_OPTIONAL_VARS,
    AWS_S3_REQUIRED_VARS, AZURE_ABS_OPTIONAL_VARS, AZURE_ABS_REQUIRED_VARS,
    CLOUDFLARE_R2_OPTIONAL_VARS, CLOUDFLARE_R2_REQUIRED_VARS, GCP_GCS_OPTIONAL_VARS,
    GCP_GCS_REQUIRED_VARS,
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use loonfs_objectstore::abs::{azure_abs, AzureAbsStoreConfig};
use loonfs_objectstore::gcs::{gcp_gcs, GcpGcsStoreConfig};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::probe::{run_store_contract_probe, StoreProbeOutcome, StoreProbeReport};
use loonfs_objectstore::s3_compatible::{
    aws_s3, cloudflare_r2, AwsS3StoreConfig, CloudflareR2StoreConfig,
};
use loonfs_objectstore::{AwsS3Credentials, ObjectStore};
use loonfs_objectstore::{ExtendBase, ExtendedObject, ImmutableWriteError, ObjectStoreError};
use loonfs_test_support::ids::page_limit;
use loonfs_types::{Checksum, ChecksumAlgorithm, ContentId, NamespaceId, Page};
use tempfile::TempDir;

const MIB: usize = 1024 * 1024;

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
    assert_child_prefix_contract(&store).await;
}

#[tokio::test]
async fn local_fs_lists_child_prefixes_inside_its_key_prefix() {
    let temp_dir = test_dir("child-prefixes");
    let store = LocalFsStore::with_key_prefix(temp_dir.path(), Some("tenant-a"))
        .expect("create scoped local object store");
    let neighbour = LocalFsStore::with_key_prefix(temp_dir.path(), Some("tenant-b"))
        .expect("create neighbouring local object store");
    neighbour
        .put_overwrite("outside/1", Bytes::from_static(b"outside"))
        .await
        .expect("write outside the key prefix");

    assert_child_prefix_contract(&store).await;
    store
        .put_overwrite("inside/1", Bytes::from_static(b"inside"))
        .await
        .expect("write inside the key prefix");
    let root = child_page(&store, "", None, 10).await;
    assert_eq!(root.items, ["inside/"]);
    assert_eq!(root.next_cursor, None);
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
async fn local_fs_honours_attested_writes_and_extension() {
    let temp_dir = test_dir("attested-extension");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_attested_writes_and_extension(&store, &[16, 1024], ChecksumAlgorithm::Crc64nvme).await;
}

#[tokio::test]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_attested_writes_and_extension() {
    let config = AwsS3ConformanceConfig::from_env()
        .expect("load AWS S3 real-provider conformance environment");
    let store = aws_s3(AwsS3StoreConfig {
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
    assert_attested_writes_and_extension(&store, &[1024, 9 * MIB], ChecksumAlgorithm::Crc64nvme)
        .await;
}

#[tokio::test]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_attested_writes_and_extension() {
    let config = CloudflareR2ConformanceConfig::from_env()
        .expect("load Cloudflare R2 real-provider conformance environment");
    let store = cloudflare_r2(CloudflareR2StoreConfig {
        bucket: config.bucket,
        account_id: config.account_id,
        endpoint_url: config.endpoint,
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
        key_prefix: Some(config.prefix),
    })
    .expect("create Cloudflare R2 object store");
    assert_attested_writes_and_extension(&store, &[1024, 65 * MIB], ChecksumAlgorithm::Crc64nvme)
        .await;
}

#[tokio::test]
#[ignore = "requires real GCP GCS credentials"]
async fn gcp_gcs_attested_writes_and_extension() {
    let config = GcpGcsConformanceConfig::from_env()
        .expect("load GCP GCS real-provider conformance environment");
    let store = gcp_gcs(GcpGcsStoreConfig {
        bucket: config.bucket,
        service_account_key_path: config.service_account_key_path,
        key_prefix: Some(config.prefix),
    })
    .expect("create GCP GCS object store");
    assert_attested_writes_and_extension(&store, &[1024, 9 * MIB], ChecksumAlgorithm::Crc32c).await;
}

#[tokio::test]
#[ignore = "requires real Azure Blob Storage credentials"]
async fn azure_abs_attested_writes_and_extension() {
    let config = AzureAbsConformanceConfig::from_env()
        .expect("load Azure Blob Storage real-provider conformance environment");
    let store = azure_abs(AzureAbsStoreConfig {
        account_name: config.account_name,
        container_name: config.container_name,
        access_key: config.access_key,
        endpoint_url: config.endpoint,
        key_prefix: Some(config.prefix),
    })
    .expect("create Azure Blob Storage object store");
    assert_attested_writes_and_extension(&store, &[1024, 9 * MIB], ChecksumAlgorithm::Crc64nvme)
        .await;
}

#[tokio::test]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_real_provider_conformance() {
    let config = AwsS3ConformanceConfig::from_env()
        .expect("load AWS S3 real-provider conformance environment");
    let store = aws_s3(AwsS3StoreConfig {
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
    let store = aws_s3(AwsS3StoreConfig {
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
    let store = cloudflare_r2(CloudflareR2StoreConfig {
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
    let store = aws_s3(AwsS3StoreConfig {
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
    let store = cloudflare_r2(CloudflareR2StoreConfig {
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
    let store = gcp_gcs(GcpGcsStoreConfig {
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
    let store = aws_s3(AwsS3StoreConfig {
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
    let store = cloudflare_r2(CloudflareR2StoreConfig {
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
    let store = gcp_gcs(GcpGcsStoreConfig {
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
    let store = gcp_gcs(GcpGcsStoreConfig {
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
    let store = azure_abs(AzureAbsStoreConfig {
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
    let store = azure_abs(AzureAbsStoreConfig {
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
    assert_child_prefix_contract(store).await;
    assert_rejects_invalid_keys_consistently(store).await;
}

/// Checks that a store lists the child prefixes under a prefix in order, one
/// page at a time, resumes after a given child, and leaves out the objects
/// directly under the prefix.
async fn assert_child_prefix_contract(store: &dyn ObjectStore) {
    let run_id = loonfs_types::generated_id("children");
    let prefix = format!("child-prefixes/{run_id}/");
    let keys = [
        "a/1", "a/2/3", "b/1", "b-1/1", "b0/1", "c/1", "c/2", "c/3/4", "c/5/6", "d/1", "direct",
    ]
    .map(|key| format!("{prefix}{key}"));
    for key in &keys {
        store
            .put_overwrite(key, Bytes::from_static(b"listed"))
            .await
            .expect("write child-prefix fixture");
    }
    let children = ["a/", "b-1/", "b/", "b0/", "c/", "d/"].map(|child| format!("{prefix}{child}"));

    let first = child_page(store, &prefix, None, 2).await;
    assert_eq!(first.items, children[..2]);
    let second = child_page(store, &prefix, first.next_cursor.as_deref(), 2).await;
    assert_eq!(second.items, children[2..4]);
    let mut listed = [first.items, second.items].concat();
    let mut cursor = second.next_cursor;
    while let Some(start_after) = cursor {
        let page = child_page(store, &prefix, Some(&start_after), 2).await;
        assert!(page.items.len() <= 2, "{page:?}");
        listed.extend(page.items);
        cursor = page.next_cursor;
    }
    assert_eq!(listed, children);

    let after_child = child_page(store, &prefix, Some(&children[2]), 2).await;
    assert_eq!(after_child.items, children[3..5]);
    let whole = child_page(store, &prefix, None, 10).await;
    assert_eq!(whole.items, children);
    assert_eq!(whole.next_cursor, None);
    let empty = child_page(store, &format!("{prefix}none/"), None, 10).await;
    assert!(empty.items.is_empty());
    assert_eq!(empty.next_cursor, None);
    let unterminated = store
        .list_child_prefixes(prefix.trim_end_matches('/'), None, page_limit(10))
        .await;
    assert!(
        matches!(unterminated, Err(ObjectStoreError::InvalidKey { .. })),
        "{unterminated:?}"
    );

    for key in &keys {
        store
            .delete(key)
            .await
            .expect("delete child-prefix fixture");
    }
}

async fn child_page(
    store: &dyn ObjectStore,
    prefix: &str,
    start_after: Option<&str>,
    limit: u32,
) -> Page<String, String> {
    store
        .list_child_prefixes(prefix, start_after, page_limit(limit))
        .await
        .expect("list child prefixes")
}

/// Checks that every provider resumes after the given key in sorted order.
async fn assert_start_after_contract(store: &dyn ObjectStore) {
    let run_id = loonfs_types::generated_id("list");
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
    let run_id = loonfs_types::generated_id("probe");
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
        &loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
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
        &loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
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
    let prefix = key
        .rsplit_once('/')
        .expect("content key has a content prefix")
        .0;
    assert!(
        store
            .list_prefix(&format!("{prefix}/"))
            .await
            .expect("list the streamed object's content prefix")
            .is_empty(),
        "a streamed-write exercise leaves nothing behind"
    );
}

/// Pins attested immutable writes and extension on one store, for each base
/// length: a write attests its bytes, an occupied key is decided by that
/// attestation, and an extension appends only under the version it names.
async fn assert_attested_writes_and_extension<S: ObjectStore>(
    store: &S,
    base_lengths: &[usize],
    crc: ChecksumAlgorithm,
) {
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let pieces = Bytes::from_static(b" and the pieces appended to it");
    for &base_length in base_lengths {
        let key = content_blob(&namespace_id, &ContentId::generate());
        let base: Vec<u8> = (0..base_length).map(|index| (index % 251) as u8).collect();
        let written = store
            .put_immutable_verified(&key, Bytes::from(base.clone()))
            .await
            .expect("immutable write");
        assert_eq!(written.sha256, Some(Checksum::sha256(&base)));
        store
            .put_immutable_verified(&key, Bytes::from(base.clone()))
            .await
            .expect("the same bytes are the same object");
        assert!(matches!(
            store
                .put_immutable_verified(&key, Bytes::from_static(b"other bytes"))
                .await,
            Err(ImmutableWriteError::DifferentObject { .. })
        ));

        let version = current_version(store, &key).await;
        let extended = [base.as_slice(), &pieces].concat();
        let result = ExtendedObject {
            sha256: Checksum::sha256(&extended),
            crc: Some(Checksum::compute(crc, &extended)),
        };
        store
            .extend_object(&key, &version, pieces.clone(), &result)
            .await
            .expect("extend");
        assert_eq!(
            store.get(&key, None).await.expect("get").as_deref(),
            Some(extended.as_slice())
        );
        let head = store.head(&key).await.expect("head").expect("extended");
        assert_eq!(head.sha256, Some(result.sha256.clone()));
        assert!(matches!(
            store
                .extend_object(&key, &version, pieces.clone(), &result)
                .await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        ));

        let twice = [extended.as_slice(), &pieces].concat();
        let wrong_crc = ExtendedObject {
            sha256: Checksum::sha256(&twice),
            crc: Some(Checksum::compute(crc, b"other bytes")),
        };
        let refused = store
            .extend_object(
                &key,
                &current_version(store, &key).await,
                pieces.clone(),
                &wrong_crc,
            )
            .await;
        assert!(
            matches!(refused, Err(ObjectStoreError::ChecksumMismatch { .. })),
            "{refused:?}"
        );
        store.delete(&key).await.expect("delete extended object");
    }
    assert!(
        store
            .list_prefix("namespaces/demo/scratch/")
            .await
            .expect("list scratch objects")
            .is_empty(),
        "an extension leaves no scratch object behind"
    );
}

async fn current_version<S: ObjectStore>(store: &S, key: &str) -> ExtendBase {
    let head = store.head(key).await.expect("head").expect("present");
    ExtendBase {
        length: head.size_bytes,
        etag: head.etag.expect("etag"),
    }
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
