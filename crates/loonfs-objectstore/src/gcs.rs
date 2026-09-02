//! Google Cloud Storage provider.

use crate::configured::ConfiguredObjectStoreKind;
use crate::object_store::Result;
use crate::presign::{
    stored_crc32c, DirectTransferIssuers, GcsPresignerConfig, GcsV4Presigner, CHECKSUM_HEAD_TTL,
};
use crate::provider_object_store::{CompareToken, StoredChecksumReader};
use crate::signed_request::{send_signed, stored_checksum_from_signed_head};
use crate::store_io_runtime::StoreIoRuntime;
use crate::{
    ObjectStoreError, ProviderObjectStore, ProviderObjectStoreConfig, StoredObjectChecksum,
};
use async_trait::async_trait;
use loonfs_api::Checksum;
use object_store::client::{HttpClient, HttpConnector, HttpRequestBody};
use object_store::gcp::GoogleCloudStorageBuilder;
use std::sync::Arc;
use std::time::SystemTime;

/// Supplies explicit credentials and key scoping for the native Google Cloud Storage adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcpGcsStoreConfig {
    /// Bucket that acts as the LoonFS object-store root.
    pub bucket: String,
    /// Filesystem path to the service-account JSON loaded by the provider client.
    pub service_account_key_path: String,
    /// Logical prefix prepended to every key, or `None` to use the bucket root.
    pub key_prefix: Option<String>,
}

struct GcsRequestSigner {
    request_signer: Arc<GcsV4Presigner>,
    http: HttpClient,
}

/// Builds a native GCS adapter whose compare tokens are object generations.
pub fn gcp_gcs(config: GcpGcsStoreConfig) -> Result<ProviderObjectStore> {
    gcp_gcs_with_issuers(config).map(|(store, _)| store)
}

/// Returns the store and the issuers that sign direct transfers against it.
pub(crate) fn gcp_gcs_with_issuers(
    config: GcpGcsStoreConfig,
) -> Result<(ProviderObjectStore, DirectTransferIssuers)> {
    let request_signer = Arc::new(GcsV4Presigner::new(GcsPresignerConfig {
        bucket: config.bucket.clone(),
        service_account_key_path: config.service_account_key_path.clone(),
        key_prefix: config.key_prefix.clone(),
    })?);

    let io_runtime = StoreIoRuntime::new()?;
    let http = io_runtime
        .connector()
        .connect(&crate::provider_object_store::provider_client_options())
        .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?;
    let builder = GoogleCloudStorageBuilder::new()
        .with_http_connector(io_runtime.connector())
        .with_client_options(crate::provider_object_store::provider_client_options())
        .with_retry(crate::provider_object_store::provider_retry_config())
        .with_bucket_name(config.bucket)
        .with_service_account_path(config.service_account_key_path);

    let provider = Arc::new(
        builder
            .build()
            .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?,
    );
    let store = ProviderObjectStore::new(
        Arc::clone(&provider) as Arc<dyn object_store::ObjectStore>,
        provider,
        ProviderObjectStoreConfig {
            key_prefix: config.key_prefix,
        },
        ConfiguredObjectStoreKind::GcpGcs,
        io_runtime,
    )?;
    let signer = Arc::new(GcsRequestSigner {
        request_signer: Arc::clone(&request_signer),
        http,
    });
    let direct_transfers = DirectTransferIssuers {
        get: request_signer.clone(),
        put: Some(request_signer),
        multipart: None,
    };

    let store = store
        .compare_token(CompareToken::Generation)
        .checksum_reader(signer);
    Ok((store, direct_transfers))
}

impl GcsRequestSigner {
    #[allow(clippy::disallowed_methods)]
    fn signing_time() -> SystemTime {
        // A V4 signature is dated, so this internally issued request enters
        // wall time here. Nothing durable is derived from it.
        SystemTime::now()
    }
}

#[async_trait]
impl StoredChecksumReader for GcsRequestSigner {
    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>> {
        let signed = self.request_signer.presign_head_stored_checksum(
            key,
            CHECKSUM_HEAD_TTL,
            Self::signing_time(),
        )?;
        let response = send_signed(&self.http, key, signed, HttpRequestBody::empty()).await?;
        stored_checksum_from_signed_head(key, &response, stored_crc32c_from_headers)
    }
}

/// Finds the stored CRC-32C among a metadata response's hash headers.
///
/// GCS reports its hashes either as one comma-joined `x-goog-hash` value or
/// as a header line per algorithm, and promises no order between them. Every
/// value is searched, so which spelling arrives cannot decide whether the
/// checksum is found — reading only the first header line would miss a
/// crc32c that happened to follow an md5.
fn stored_crc32c_from_headers(headers: &http::HeaderMap) -> Option<Checksum> {
    headers
        .get_all("x-goog-hash")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find_map(stored_crc32c)
}

#[cfg(test)]
mod tests {
    use super::{gcp_gcs, stored_crc32c_from_headers, GcpGcsStoreConfig};
    use crate::test_support::gcs_fixture_service_account_key_file;
    use crate::{ObjectStore, ObjectStoreError};
    use bytes::Bytes;

    #[tokio::test]
    async fn invalid_keys_are_rejected_before_generation_tokens() {
        let (_key_dir, service_account_key_path) =
            gcs_fixture_service_account_key_file("gcs-invalid-key");
        let store = gcp_gcs(GcpGcsStoreConfig {
            bucket: "bucket".to_owned(),
            service_account_key_path: service_account_key_path.display().to_string(),
            key_prefix: None,
        })
        .expect("construct gcs store");

        assert!(matches!(
            store
                .compare_and_swap("../escape", "not-a-generation", Bytes::from_static(b"oops"))
                .await,
            Err(ObjectStoreError::InvalidKey { .. })
        ));
    }

    #[tokio::test]
    async fn compare_and_swap_rejects_a_non_generation_token() {
        let (_key_dir, service_account_key_path) =
            gcs_fixture_service_account_key_file("gcs-streamed-cas");
        let store = gcp_gcs(GcpGcsStoreConfig {
            bucket: "bucket".to_owned(),
            service_account_key_path: service_account_key_path.display().to_string(),
            key_prefix: None,
        })
        .expect("construct gcs store");
        let error = store
            .compare_and_swap(
                "namespaces/demo/wal/head.json",
                "not-a-generation",
                Bytes::from_static(b"payload"),
            )
            .await
            .expect_err("non-generation compare token should fail");

        assert!(matches!(error, ObjectStoreError::PreconditionFailed { .. }));
    }

    #[test]
    fn service_account_key_path_is_required() {
        assert!(matches!(
            gcp_gcs(GcpGcsStoreConfig {
                bucket: "bucket".to_owned(),
                service_account_key_path: " ".to_owned(),
                key_prefix: None,
            }),
            Err(ObjectStoreError::Configuration(_))
        ));
    }

    #[test]
    fn a_service_account_key_that_cannot_sign_stops_the_store_from_being_built() {
        let (key_dir, _key_path) = gcs_fixture_service_account_key_file("gcs-unsignable");
        let unsignable = key_dir.path().join("unsignable.json");
        std::fs::write(
            &unsignable,
            br#"{"client_email":"a@b.iam.gserviceaccount.com","private_key":"private_key","disable_oauth":true}"#,
        )
        .expect("write unsignable service account key");

        assert!(matches!(
            gcp_gcs(GcpGcsStoreConfig {
                bucket: "bucket".to_owned(),
                service_account_key_path: unsignable.display().to_string(),
                key_prefix: None,
            }),
            Err(ObjectStoreError::Configuration(_))
        ));
    }

    #[test]
    fn the_stored_crc32c_is_found_however_gcs_spells_its_hash_header() {
        let crc32c_of_hello = "9a71bb4c";
        let md5 = "md5=XUFAKrxLKna5cZ2REBfFkg==";
        let crc32c = "crc32c=mnG7TA==";

        for values in [
            vec![crc32c],
            vec![&format!("{md5},{crc32c}")],
            vec![&format!("{crc32c},{md5}")],
            // A header line per algorithm, with the crc32c second.
            vec![md5, crc32c],
            vec![crc32c, md5],
        ] {
            let mut headers = http::HeaderMap::new();
            for value in &values {
                headers.append(
                    "x-goog-hash",
                    http::HeaderValue::from_str(value).expect("header value"),
                );
            }
            assert_eq!(
                stored_crc32c_from_headers(&headers).map(|checksum| checksum.value),
                Some(crc32c_of_hello.to_owned()),
                "crc32c not found in {values:?}"
            );
        }

        // An object GCS describes without a crc32c is described without one.
        let mut md5_only = http::HeaderMap::new();
        md5_only.append(
            "x-goog-hash",
            http::HeaderValue::from_static("md5=XUFAKrxLKna5cZ2REBfFkg=="),
        );
        assert_eq!(stored_crc32c_from_headers(&md5_only), None);
        assert_eq!(stored_crc32c_from_headers(&http::HeaderMap::new()), None);
    }
}
