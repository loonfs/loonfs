//! Google Cloud Storage provider.

use super::{ByteRange, ByteStream, ObjectBody, ObjectMetadata, ObjectStore, PutMode};
use crate::object_store::Result;
use crate::presign::{stored_crc32c, GcsPresignerConfig, GcsV4Presigner, CHECKSUM_HEAD_TTL};
use crate::signed_request::{execute_signed, stored_checksum_from_signed_head};
use crate::store_io_runtime::StoreIoRuntime;
use crate::{
    ObjectStoreError, ProviderObjectStore, ProviderObjectStoreConfig, StoredObjectChecksum,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
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

/// Google Cloud Storage through its native API.
///
/// GCS conditions writes on object generations, not ETags, and silently
/// ignores HTTP `If-Match`/`If-None-Match` on its S3-interoperability
/// surface — conformance proved interop overwrites instead of failing
/// preconditions. The native API is therefore the only correct backend, and
/// this adapter hands out the generation as the opaque compare token.
#[derive(Debug)]
pub struct GcpGcsStore {
    inner: ProviderObjectStore,
    /// Signs the one request the provider client cannot express: the metadata
    /// read that reports an object's stored CRC-32C back. The V4 signer this
    /// crate already owns for direct-transfer URLs expresses it, so the store
    /// needs no second credential path.
    request_signer: GcsV4Presigner,
    /// Sends that signed request over the store's own IO runtime and timeout
    /// scheme, exactly like every provider-client request.
    http: HttpClient,
    /// Keeps the HTTP IO runtime alive for the provider client's lifetime;
    /// the connector inside the client holds only a handle onto it.
    _io_runtime: StoreIoRuntime,
}

impl GcpGcsStore {
    /// Builds a native GCS adapter whose compare tokens are object generations.
    ///
    /// Construction fails for a blank bucket or credential path, an invalid
    /// key prefix, runtime initialization, or provider-client configuration.
    pub fn new(config: GcpGcsStoreConfig) -> Result<Self> {
        if config.bucket.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "bucket must not be empty".to_owned(),
            ));
        }
        if config.service_account_key_path.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "service account key path must not be empty".to_owned(),
            ));
        }

        let request_signer = GcsV4Presigner::new(GcsPresignerConfig {
            bucket: config.bucket.clone(),
            service_account_key_path: config.service_account_key_path.clone(),
            key_prefix: config.key_prefix.clone(),
        })?;

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
        let inner = ProviderObjectStore::new(
            Arc::clone(&provider) as Arc<dyn object_store::ObjectStore>,
            Some(provider),
            ProviderObjectStoreConfig {
                key_prefix: config.key_prefix,
            },
        )?;

        Ok(Self {
            inner,
            request_signer,
            http,
            _io_runtime: io_runtime,
        })
    }

    #[allow(clippy::disallowed_methods)]
    fn signing_time() -> SystemTime {
        // A V4 signature is dated, so this internally issued request enters
        // wall time here. Nothing durable is derived from it.
        SystemTime::now()
    }

    fn generation_as_compare_token(metadata: ObjectMetadata) -> ObjectMetadata {
        ObjectMetadata {
            etag: metadata.version.clone(),
            ..metadata
        }
    }

    fn require_generation_compare_token(key: &str, expected_etag: &str) -> Result<()> {
        expected_etag
            .parse::<u64>()
            .map(|_| ())
            .map_err(|_| ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            })
    }
}

#[async_trait]
impl ObjectStore for GcpGcsStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        Ok(self
            .inner
            .head(key)
            .await?
            .map(Self::generation_as_compare_token))
    }

    /// Reads size and the stored CRC-32C back from one signed metadata
    /// request.
    ///
    /// GCS reports the stored checksum in `x-goog-hash` on an ordinary object
    /// request, so this is a `HEAD` and nothing more. The provider client
    /// surfaces neither that header nor any other checksum, which is why the
    /// request is signed here rather than asked of the client.
    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>> {
        let signed = self.request_signer.presign_head_stored_checksum(
            key,
            CHECKSUM_HEAD_TTL,
            Self::signing_time(),
        )?;
        let response = execute_signed(&self.http, key, signed, HttpRequestBody::empty()).await?;
        stored_checksum_from_signed_head(key, &response, stored_crc32c_from_headers)
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        Ok(self
            .inner
            .get_with_metadata(key)
            .await?
            .map(|body| ObjectBody {
                metadata: Self::generation_as_compare_token(body.metadata),
                bytes: body.bytes,
            }))
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        self.inner.get(key, range).await
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        self.inner.validate_key(key)?;
        if let PutMode::CompareAndSwap { expected_etag } = &mode {
            Self::require_generation_compare_token(key, expected_etag)?;
        }
        Ok(Self::generation_as_compare_token(
            self.inner.put(key, bytes, mode).await?,
        ))
    }

    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        self.inner.validate_key(key)?;
        if matches!(mode, PutMode::CompareAndSwap { .. }) {
            // The public GCS compare token is an object generation, while
            // `ProviderObjectStore` can only compare the raw e-tag returned
            // by its provider HEAD before multipart completion. Forwarding
            // streamed CAS would therefore enforce a different condition
            // from `head`; reject it instead of claiming false CAS safety.
            return Err(ObjectStoreError::Unsupported(
                "streamed compare-and-swap with GCS generation tokens",
            ));
        }
        self.inner.put_streamed(key, body, mode).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String>> {
        // GCS offsets are inclusive. The provider adapter removes the offset
        // key so this method remains strictly after it.
        self.inner.list_prefix_from_stream(prefix, start_after)
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
    use super::{stored_crc32c_from_headers, GcpGcsStore, GcpGcsStoreConfig};
    use crate::test_support::gcs_fixture_service_account_key_file;
    use crate::{ObjectStore, ObjectStoreError, PutMode};
    use bytes::Bytes;
    use futures::StreamExt;

    #[tokio::test]
    async fn invalid_keys_are_rejected_before_generation_tokens() {
        let (_key_dir, service_account_key_path) =
            gcs_fixture_service_account_key_file("gcs-invalid-key");
        let store = GcpGcsStore::new(GcpGcsStoreConfig {
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
    async fn streamed_compare_and_swap_rejects_the_mismatched_token_semantics() {
        let (_key_dir, service_account_key_path) =
            gcs_fixture_service_account_key_file("gcs-streamed-cas");
        let store = GcpGcsStore::new(GcpGcsStoreConfig {
            bucket: "bucket".to_owned(),
            service_account_key_path: service_account_key_path.display().to_string(),
            key_prefix: None,
        })
        .expect("construct gcs store");
        let body = futures::stream::iter([Ok(Bytes::from_static(b"payload"))]).boxed();

        let error = store
            .put_streamed(
                "namespaces/demo/wal/head.json",
                body,
                PutMode::CompareAndSwap {
                    expected_etag: "123".to_owned(),
                },
            )
            .await
            .expect_err("streamed GCS CAS must not compare an e-tag to a generation");

        assert!(matches!(error, ObjectStoreError::Unsupported(_)));
    }

    #[test]
    fn service_account_key_path_is_required() {
        assert!(matches!(
            GcpGcsStore::new(GcpGcsStoreConfig {
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
            GcpGcsStore::new(GcpGcsStoreConfig {
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
