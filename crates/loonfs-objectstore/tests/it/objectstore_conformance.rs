use crate::provider_env::{
    provider_env_example_contents, AwsS3ConformanceConfig, AzureAbsConformanceConfig,
    CloudflareR2ConformanceConfig, GcpGcsConformanceConfig, AWS_S3_OPTIONAL_VARS,
    AWS_S3_REQUIRED_VARS, AZURE_ABS_OPTIONAL_VARS, AZURE_ABS_REQUIRED_VARS,
    CLOUDFLARE_R2_OPTIONAL_VARS, CLOUDFLARE_R2_REQUIRED_VARS, GCP_GCS_OPTIONAL_VARS,
    GCP_GCS_REQUIRED_VARS,
};
use bytes::Bytes;
use futures::StreamExt;
use loonfs_api::{ContentId, ManifestObjectId, StorageChecksum};
use loonfs_objectstore::abs::{AzureAbsStore, AzureAbsStoreConfig};
use loonfs_objectstore::gcs::{GcpGcsStore, GcpGcsStoreConfig};
use loonfs_objectstore::keys::{
    content_blob, metadata_manifest_object, metadata_table, wal_head, wal_segment,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::probe::{run_store_contract_probe, StoreProbeOutcome, StoreProbeReport};
use loonfs_objectstore::s3_compatible::{
    AwsS3StoreConfig, CloudflareR2StoreConfig, S3CompatibleStore,
};
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreError;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn key_builders_cover_locked_object_families() {
    assert_eq!(wal_head("ns-1"), "namespaces/ns-1/wal/head.json");
    assert_eq!(
        content_blob(
            "cs_00000000000000000000000000000001",
            &ContentId::parse("con_abcdef0123456789abcdef0123456789").expect("valid content id"),
        ),
        "content-stores/cs_00000000000000000000000000000001/objects/ab/cd/con_abcdef0123456789abcdef0123456789"
    );
    assert_eq!(
        wal_segment("ns-1", "seg_00000000000000000000000000000001"),
        "namespaces/ns-1/wal/segments/seg_00000000000000000000000000000001.wal.zst"
    );
    assert_eq!(
        metadata_manifest_object(
            "ns-1",
            &ManifestObjectId::parse("00000000000000000420-0123456789abcdef")
                .expect("valid manifest object id"),
        ),
        "namespaces/ns-1/metadata/manifests/00000000000000000420-0123456789abcdef.manifest.json"
    );
    assert_eq!(
        metadata_table("ns-1", "tbl_00000000000000000000000000000001"),
        "namespaces/ns-1/metadata/tables/tbl_00000000000000000000000000000001.sst.zst"
    );
    assert_eq!(
        metadata_table("source-ns", "tbl_00000000000000000000000000000002"),
        "namespaces/source-ns/metadata/tables/tbl_00000000000000000000000000000002.sst.zst"
    );
    assert_eq!(
        metadata_table("ns-1", "tbl_ffffffffffffffffffffffffffffffff"),
        "namespaces/ns-1/metadata/tables/tbl_ffffffffffffffffffffffffffffffff.sst.zst"
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
    let temp_dir = TestDir::new("contract-probe");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_store_contract_probe_passes(&store).await;
}

#[tokio::test]
async fn local_fs_rejects_path_traversal_keys() {
    let temp_dir = TestDir::new("invalid-key");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    assert_rejects_invalid_keys_consistently(&store).await;
}

#[tokio::test]
async fn local_fs_streamed_write_round_trips() {
    let temp_dir = TestDir::new("streamed-write");
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
        access_key_id: config.access_key_id.into(),
        secret_access_key: config.secret_access_key.into(),
        session_token: config.session_token.map(Into::into),
        key_prefix: Some(config.prefix),
        force_path_style: false,
    })
    .expect("create AWS S3 object store");
    assert_provider_conformance(&store).await;
}

/// The streamed write against live AWS S3. Separate from the conformance
/// sweep because it moves 24 MiB: it is the one exercise where the
/// provider's real multipart rules — non-final part sizes, completion,
/// assembly — meet a payload the server never held.
#[tokio::test]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_streamed_write_round_trips() {
    let config = AwsS3ConformanceConfig::from_env()
        .expect("load AWS S3 real-provider conformance environment");
    let store = S3CompatibleStore::aws_s3(AwsS3StoreConfig {
        bucket: config.bucket,
        region: config.region,
        endpoint_url: config.endpoint,
        access_key_id: config.access_key_id.into(),
        secret_access_key: config.secret_access_key.into(),
        session_token: config.session_token.map(Into::into),
        key_prefix: Some(config.prefix),
        force_path_style: false,
    })
    .expect("create AWS S3 object store");
    assert_streamed_write_round_trips(&store).await;
}

/// The same streamed write against live Cloudflare R2, whose fixed
/// non-final part size rule is the one this geometry has to satisfy.
#[tokio::test]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_streamed_write_round_trips() {
    let config = CloudflareR2ConformanceConfig::from_env()
        .expect("load Cloudflare R2 real-provider conformance environment");
    let store = S3CompatibleStore::cloudflare_r2(CloudflareR2StoreConfig {
        bucket: config.bucket,
        account_id: config.account_id,
        endpoint_url: config.endpoint,
        access_key_id: config.access_key_id.into(),
        secret_access_key: config.secret_access_key.into(),
        key_prefix: Some(config.prefix),
    })
    .expect("create Cloudflare R2 object store");
    assert_streamed_write_round_trips(&store).await;
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
        access_key_id: config.access_key_id.into(),
        secret_access_key: config.secret_access_key.into(),
        session_token: config.session_token.map(Into::into),
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
        access_key_id: config.access_key_id.into(),
        secret_access_key: config.secret_access_key.into(),
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
        access_key: config.access_key.into(),
        endpoint_url: config.endpoint,
        key_prefix: Some(config.prefix),
    })
    .expect("create Azure Blob Storage object store");
    assert_provider_conformance(&store).await;
}

/// The live provider sweep: the store contract probe, plus the key
/// rejection the probe deliberately leaves to tests.
///
/// The probe is the production surface an operator runs, and running it
/// here is what stops the two from drifting: a contract check changes for
/// production and for this sweep in one edit, or not at all.
async fn assert_provider_conformance(store: &dyn ObjectStore) {
    assert_store_contract_probe_passes(store).await;
    assert_rejects_invalid_keys_consistently(store).await;
}

/// Requires every probe check to pass, and prints the whole report when one
/// does not.
///
/// The report is the point of a live run. Cloudflare R2's 501 answer to
/// `GetObjectAttributes` was read off exactly this output, so a failure
/// shows every check's verdict rather than the first one that broke.
///
/// `stored_checksum_readback` may answer `unsupported`: that is the
/// capability line for presigned direct uploads, and GCS and Azure Blob
/// Storage both sit on the far side of it. Every other check must pass
/// outright on every provider this suite covers, multipart included.
async fn assert_store_contract_probe_passes(store: &dyn ObjectStore) {
    let run_id = loonfs_api::generated_id("probe");
    let report = run_store_contract_probe(store, &run_id).await;
    let acceptable = report.checks.iter().all(|check| match check.outcome {
        StoreProbeOutcome::Passed => true,
        StoreProbeOutcome::Unsupported => check.name == "stored_checksum_readback",
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

/// The content key a streamed-write exercise writes to and cleans up.
fn streamed_write_key() -> String {
    content_blob(
        "cs_00000000000000000000000000000001",
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
    let expected = StorageChecksum::sha256(&payload);

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
        StorageChecksum::sha256(&read_back),
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
    }
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
