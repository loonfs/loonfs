use crate::provider_env::{
    provider_env_example_contents, AwsS3ConformanceConfig, CloudflareR2ConformanceConfig,
    GcpGcsConformanceConfig, AWS_S3_OPTIONAL_VARS, AWS_S3_REQUIRED_VARS,
    CLOUDFLARE_R2_OPTIONAL_VARS, CLOUDFLARE_R2_REQUIRED_VARS, GCP_GCS_OPTIONAL_VARS,
    GCP_GCS_REQUIRED_VARS,
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use loonfs_objectstore::gcs::{gcp_gcs, GcpGcsStoreConfig};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::probe::{run_store_contract_probe, StoreProbeReport};
use loonfs_objectstore::s3_compatible::{
    aws_s3, cloudflare_r2, AwsS3StoreConfig, CloudflareR2StoreConfig,
};
use loonfs_objectstore::{AssemblySource, ImmutableWriteError, ObjectStoreError};
use loonfs_objectstore::{AwsS3Credentials, ByteRange, ObjectStore};
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
    assert_store_contract_probe_passes(&store).await;
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
async fn local_fs_honours_assembly() {
    let temp_dir = test_dir("assembly");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_assembly(
        &store,
        &[(16, 30), (1024, 30)],
        ChecksumAlgorithm::Crc64nvme,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_assembly() {
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
    assert_assembly(
        &store,
        &[(1024, 30), (1024, 9 * MIB), (9 * MIB, 9 * MIB)],
        ChecksumAlgorithm::Crc64nvme,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_assembly() {
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
    assert_assembly(
        &store,
        &[(1024, 30), (64 * MIB, 64 * MIB), (40 * MIB, 90 * MIB)],
        ChecksumAlgorithm::Crc64nvme,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires real GCP GCS credentials"]
async fn gcp_gcs_assembly() {
    let config = GcpGcsConformanceConfig::from_env()
        .expect("load GCP GCS real-provider conformance environment");
    let store = gcp_gcs(GcpGcsStoreConfig {
        bucket: config.bucket,
        service_account_key_path: config.service_account_key_path,
        key_prefix: Some(config.prefix),
    })
    .expect("create GCP GCS object store");
    assert_assembly(
        &store,
        &[(1024, 30), (9 * MIB, 30)],
        ChecksumAlgorithm::Crc32c,
    )
    .await;
    assert_chained_compose(&store).await;
}

#[tokio::test]
#[ignore = "Azure Blob Storage needs a native checksum adapter"]
async fn azure_abs_assembly() {
    let error = toml::from_str::<loonfs_objectstore::StoreConfig>(r#"kind = "azure-abs""#)
        .expect_err("Azure requires a native adapter");
    assert!(error
        .to_string()
        .contains("Azure Blob Storage is not supported yet: it stores no full-object checksum"));
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
    assert_provider_conformance(&store).await;
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
async fn cloudflare_r2_put_stores_a_trustworthy_checksum() {
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
async fn gcp_gcs_put_stores_a_trustworthy_checksum() {
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
    assert_provider_conformance(&store).await;
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
    assert_provider_conformance(&store).await;
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
#[ignore = "Azure Blob Storage needs a native checksum adapter"]
async fn azure_abs_real_provider_conformance() {
    let error = toml::from_str::<loonfs_objectstore::StoreConfig>(r#"kind = "azure-abs""#)
        .expect_err("Azure requires a native adapter");
    assert!(error
        .to_string()
        .contains("Azure Blob Storage is not supported yet: it stores no full-object checksum"));
}

#[tokio::test]
#[ignore = "Azure Blob Storage needs a native checksum adapter"]
async fn azure_abs_streamed_write_round_trips() {
    let error = toml::from_str::<loonfs_objectstore::StoreConfig>(r#"kind = "azure-abs""#)
        .expect_err("Azure requires a native adapter");
    assert!(error
        .to_string()
        .contains("Azure Blob Storage is not supported yet: it stores no full-object checksum"));
}

/// The live provider sweep: the store contract probe, plus the key
/// rejection the probe deliberately leaves to tests.
///
/// The probe is the production surface an operator runs, and running it
/// here is what stops the two from drifting: a contract check changes for
/// production and for this sweep in one edit, or not at all.
async fn assert_provider_conformance(store: &dyn ObjectStore) {
    assert_store_contract_probe_passes(store).await;
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
    let prefix = format!("start-after/{run_id}/entry-");
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

    let entries = store
        .list_entries_from_stream(&prefix, None)
        .try_collect::<Vec<_>>()
        .await
        .expect("list entries with timestamps");
    assert_eq!(
        entries.iter().map(|entry| &entry.key).collect::<Vec<_>>(),
        keys.iter().collect::<Vec<_>>()
    );
    for entry in entries {
        let metadata = store
            .head(&entry.key)
            .await
            .expect("head listed object")
            .expect("listed object exists");
        assert_eq!(
            entry.last_modified_ms, metadata.last_modified_ms,
            "{}",
            entry.key
        );
    }

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

async fn assert_store_contract_probe_passes(store: &dyn ObjectStore) {
    let run_id = loonfs_types::generated_id("probe");
    let report = run_store_contract_probe(store, &run_id).await;
    assert!(
        report
            .checks
            .iter()
            .all(|check| matches!(check.outcome, loonfs_objectstore::StoreProbeOutcome::Passed)),
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

    let stored = store.head(&key).await.expect("head the stored checksum");
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
    let checksum = stored.checksum.expect("stored checksum");
    let local = Checksum::compute(store.checksum_algorithm(), &payload);
    assert_eq!(
        checksum, local,
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
    let expected = Checksum::crc64nvme(&payload);

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
        Checksum::crc64nvme(&read_back),
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

async fn assert_assembly<S: ObjectStore>(
    store: &S,
    cases: &[(usize, usize)],
    crc: ChecksumAlgorithm,
) {
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    for &(first_length, second_length) in cases {
        let first = Bytes::from(vec![b'a'; first_length]);
        let second = Bytes::from(vec![b'b'; second_length]);
        let tail = Bytes::from_static(b"tail");
        let payload = [first.as_ref(), second.as_ref(), tail.as_ref()].concat();
        let mut sources = Vec::new();
        for bytes in [first, second] {
            let key = content_blob(&namespace_id, &ContentId::generate());
            store
                .put_immutable_verified(&key, bytes.clone())
                .await
                .expect("source");
            sources.push(AssemblySource {
                key,
                range: None,
                checksum: Checksum::compute(crc, &bytes),
            });
        }
        let key = content_blob(&namespace_id, &ContentId::generate());
        let expected = Checksum::compute(crc, &payload);
        let written = store
            .assemble(
                &key,
                &sources,
                vec![tail.slice(..2), tail.slice(2..)],
                &expected,
            )
            .await
            .expect("assembly");
        assert_eq!(
            store.get(&key, None).await.expect("get").expect("object"),
            payload
        );
        assert_eq!(written.checksum, Some(expected.clone()));
        assert_eq!(
            store
                .assemble(
                    &key,
                    &sources,
                    vec![tail.slice(..2), tail.slice(2..)],
                    &expected
                )
                .await
                .expect("retry")
                .size_bytes,
            payload.len() as u64
        );
        let wrong_key = content_blob(&namespace_id, &ContentId::generate());
        assert!(matches!(
            store
                .assemble(
                    &wrong_key,
                    &sources,
                    vec![tail],
                    &Checksum::compute(crc, b"wrong")
                )
                .await,
            Err(ImmutableWriteError::Transport {
                source: ObjectStoreError::ChecksumMismatch { .. },
                ..
            })
        ));
        assert!(store.head(&wrong_key).await.expect("absent").is_none());
        let ranged = content_blob(&namespace_id, &ContentId::generate());
        sources[0].range = Some(ByteRange {
            start_inclusive: 1,
            end_exclusive: first_length as u64 - 1,
        });
        sources[0].checksum = Checksum::compute(crc, &payload[1..first_length - 1]);
        let ranged_bytes = [&payload[1..first_length - 1], &payload[first_length..]].concat();
        store
            .assemble(
                &ranged,
                &sources,
                vec![Bytes::from_static(b"ta"), Bytes::from_static(b"il")],
                &Checksum::compute(crc, &ranged_bytes),
            )
            .await
            .expect("ranged assembly");
        assert_eq!(
            store
                .get(&ranged, None)
                .await
                .expect("get range")
                .expect("range"),
            ranged_bytes
        );
        store.delete(&ranged).await.expect("delete range");
        sources[0].range = Some(ByteRange {
            start_inclusive: 0,
            end_exclusive: first_length as u64 + 1,
        });
        assert!(matches!(
            store
                .assemble(&ranged, &sources, Vec::new(), &expected)
                .await,
            Err(ImmutableWriteError::Transport {
                source: ObjectStoreError::PreconditionFailed { .. },
                ..
            })
        ));
        assert!(store
            .head(&ranged)
            .await
            .expect("short source creates nothing")
            .is_none());
        store.delete(&sources[0].key).await.expect("delete source");
        assert!(matches!(
            store
                .assemble(&ranged, &sources, Vec::new(), &expected)
                .await,
            Err(ImmutableWriteError::Transport {
                source: ObjectStoreError::PreconditionFailed { .. },
                ..
            })
        ));
        assert!(store
            .head(&ranged)
            .await
            .expect("missing source creates nothing")
            .is_none());
        for source in sources {
            store.delete(&source.key).await.expect("delete source");
        }
        store.delete(&key).await.expect("delete assembly");
    }
    assert!(store
        .list_prefix("namespaces/demo/temporary/")
        .await
        .expect("temporaries")
        .is_empty());
}

async fn assert_chained_compose<S: ObjectStore>(store: &S) {
    let owner = NamespaceId::parse("demo").expect("owner");
    let mut sources = Vec::new();
    let mut bytes = Vec::new();
    for index in 0..40 {
        let source = Bytes::from(vec![index; MIB / 4]);
        let key = content_blob(&owner, &ContentId::generate());
        store
            .put_immutable_verified(&key, source.clone())
            .await
            .expect("source");
        sources.push(AssemblySource {
            key,
            range: None,
            checksum: Checksum::crc32c(&source),
        });
        bytes.extend_from_slice(&source);
    }
    let key = content_blob(&owner, &ContentId::generate());
    let expected = Checksum::crc32c(&bytes);
    let result = store
        .assemble(&key, &sources, Vec::new(), &expected)
        .await
        .expect("chained compose");
    assert_eq!(result.checksum, Some(expected));
    assert_eq!(
        store.get(&key, None).await.expect("get").expect("object"),
        bytes
    );
    assert!(store
        .list_prefix("namespaces/demo/temporary/")
        .await
        .expect("temporaries")
        .is_empty());
    store.delete(&key).await.expect("delete result");
    for source in sources {
        store.delete(&source.key).await.expect("delete source");
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
