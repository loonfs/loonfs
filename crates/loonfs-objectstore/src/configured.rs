//! [`ConfiguredObjectStore`]: one provider client built from configuration,
//! paired with the direct transfers that provider can authorize.

use crate::assembly::PartSizing;
use crate::aws_credentials::{aws_credentials_source, cloudflare_r2_credentials_source};
use crate::gcs::{gcp_gcs_with_issuers, GcpGcsStoreConfig};
use crate::local_fs_store::{LocalFsStore, LocalFsStoreConfig};
use crate::object_store::{Result, SharedObjectStore};
use crate::presign::{
    DirectTransferIssuers, AWS_S3_MAX_DIRECT_PUT_BYTES, CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
    GCP_GCS_MAX_DIRECT_PUT_BYTES,
};
use crate::provider_object_store::{CompareToken, CreatePath};
use crate::s3_compatible::{
    aws_s3_with_credentials, cloudflare_r2_with_credentials, AwsS3StoreConfig,
    CloudflareR2StoreConfig, CopySourcePrecondition,
};
use http::Uri;
use loonfs_types::ChecksumAlgorithm;
use std::sync::Arc;

/// Provider domains covered by live direct-transfer tests.
pub(crate) const AWS_S3_PROVEN_DOMAINS: &[&str] = &["amazonaws.com", "amazonaws.com.cn"];
pub(crate) const CLOUDFLARE_R2_PROVEN_DOMAINS: &[&str] = &["r2.cloudflarestorage.com"];

/// Identifies the concrete provider backing a [`ConfiguredObjectStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfiguredObjectStoreKind {
    /// Stores objects beneath a Unix-family local filesystem root.
    LocalFs,
    /// Uses AWS S3 through its SigV4 API.
    AwsS3,
    /// Uses Cloudflare R2 through its S3-compatible API.
    CloudflareR2,
    /// Uses Google Cloud Storage's native generation-aware API.
    GcpGcs,
}

impl ConfiguredObjectStoreKind {
    /// Returns the stable snake_case label, matching the `kind` tag used in
    /// config files.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalFs => "local_fs",
            Self::AwsS3 => "aws_s3",
            Self::CloudflareR2 => "cloudflare_r2",
            Self::GcpGcs => "gcp_gcs",
        }
    }

    /// Returns the most bytes that one assembly on this store keeps in
    /// memory at once, which is the part it builds and the source read it
    /// copies into that part.
    pub const fn max_assembly_buffer_bytes(self) -> u64 {
        let part_bytes = match self.part_sizing() {
            PartSizing::Equal => crate::assembly::R2_PART_BYTES,
            PartSizing::Varied => crate::assembly::READ_BYTES,
        };
        part_bytes + crate::assembly::READ_BYTES
    }

    /// Returns the checksum algorithm that this store verifies on upload and
    /// reports from `head`.
    pub(crate) const fn checksum_algorithm(self) -> ChecksumAlgorithm {
        match self {
            Self::GcpGcs => ChecksumAlgorithm::Crc32c,
            Self::LocalFs | Self::AwsS3 | Self::CloudflareR2 => ChecksumAlgorithm::Crc64nvme,
        }
    }

    /// Returns the version field that this store's conditional writes compare.
    pub(crate) const fn compare_token(self) -> CompareToken {
        match self {
            Self::GcpGcs => CompareToken::Generation,
            Self::LocalFs | Self::AwsS3 | Self::CloudflareR2 => CompareToken::Etag,
        }
    }

    /// Returns which requests create an object while its key is absent.
    pub(crate) const fn create_path(self) -> CreatePath {
        match self {
            Self::GcpGcs => CreatePath::Signer,
            Self::LocalFs | Self::AwsS3 | Self::CloudflareR2 => CreatePath::ProviderClient,
        }
    }

    /// Returns the largest object that one presigned PUT to this store
    /// creates, or `None` for a store that signs no direct transfers.
    pub(crate) const fn direct_put_max_content_bytes(self) -> Option<u64> {
        match self {
            Self::LocalFs => None,
            Self::AwsS3 => Some(AWS_S3_MAX_DIRECT_PUT_BYTES),
            Self::CloudflareR2 => Some(CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES),
            Self::GcpGcs => Some(GCP_GCS_MAX_DIRECT_PUT_BYTES),
        }
    }

    /// Returns how this store sizes the parts of one multipart upload.
    pub(crate) const fn part_sizing(self) -> PartSizing {
        match self {
            Self::CloudflareR2 => PartSizing::Equal,
            Self::LocalFs | Self::AwsS3 | Self::GcpGcs => PartSizing::Varied,
        }
    }

    /// Returns the precondition that a part copy puts on its source object.
    pub(crate) const fn copy_source_precondition(self) -> CopySourcePrecondition {
        match self {
            Self::CloudflareR2 => CopySourcePrecondition::Unconditional,
            Self::LocalFs | Self::AwsS3 | Self::GcpGcs => CopySourcePrecondition::IfMatch,
        }
    }

    /// Returns whether a part copy's response reports the CRC-64/NVME of the
    /// copied part.
    pub(crate) const fn copy_part_reports_checksum(self) -> bool {
        !matches!(self, Self::CloudflareR2)
    }

    /// Returns whether a multipart completion is checked by reading the
    /// object's stored checksum back, because the completion accepts a wrong
    /// checksum claim.
    pub(crate) const fn completion_checked_by_head(self) -> bool {
        matches!(self, Self::CloudflareR2)
    }
}

/// Validated provider client and the direct-transfer issuers it supports.
///
/// Construction decides whether direct transfers are allowed. Unsupported
/// signing behavior or an unvalidated endpoint produces no direct-transfer
/// issuers, so higher layers do not need to interpret provider configuration.
#[derive(Debug)]
pub struct ConfiguredObjectStore {
    inner: SharedObjectStore,
    direct_transfers: Option<DirectTransferIssuers>,
}

impl ConfiguredObjectStore {
    /// Opens a local-filesystem provider with optional logical key scoping.
    ///
    /// Construction fails outside Unix-family platforms, when the root cannot
    /// be created, or when the key prefix is invalid.
    pub fn local_fs(config: LocalFsStoreConfig) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(LocalFsStore::with_key_prefix(
                config.root,
                config.key_prefix.as_deref(),
            )?),
            direct_transfers: None,
        })
    }

    /// Builds an AWS S3 store, and the direct transfers it can authorize on
    /// a proven endpoint.
    ///
    /// Construction fails when signing, provider, runtime, or key-prefix configuration is invalid.
    pub fn aws_s3(config: AwsS3StoreConfig) -> Result<Self> {
        let credentials = aws_credentials_source(&config.credentials, &config.region)?;
        let direct_transfers_enabled =
            endpoint_is_proven(config.endpoint_url.as_deref(), AWS_S3_PROVEN_DOMAINS);
        let provider = aws_s3_with_credentials(config, credentials)?;
        let direct_transfers = direct_transfers_enabled.then_some(provider.direct_transfers);
        Ok(Self {
            inner: Arc::new(provider.store),
            direct_transfers,
        })
    }

    /// Builds a Cloudflare R2 store, and the direct transfers it can
    /// authorize on a proven endpoint.
    ///
    /// Construction fails when ambient credentials are missing from the
    /// environment, or when signing, provider, runtime, or key-prefix
    /// configuration is invalid.
    pub fn cloudflare_r2(config: CloudflareR2StoreConfig) -> Result<Self> {
        let credentials = cloudflare_r2_credentials_source(&config.credentials)?;
        let direct_transfers_enabled = endpoint_is_proven(
            Some(config.endpoint_url.as_str()),
            CLOUDFLARE_R2_PROVEN_DOMAINS,
        );
        let provider = cloudflare_r2_with_credentials(config, credentials)?;
        let direct_transfers = direct_transfers_enabled.then_some(provider.direct_transfers);
        Ok(Self {
            inner: Arc::new(provider.store),
            direct_transfers,
        })
    }

    /// Builds a native GCS store, and the direct transfers it can authorize.
    ///
    /// This adapter implements no multipart signing for GCS, so the bundle's
    /// multipart slot stays empty instead of containing a signer that would
    /// fail. It needs no endpoint-trust decision either. The configuration
    /// has no endpoint override, so every capability is `https` to Google's
    /// own host.
    ///
    /// Construction fails when the key file, provider, runtime, or key prefix is invalid.
    pub fn gcp_gcs(config: GcpGcsStoreConfig) -> Result<Self> {
        let provider = gcp_gcs_with_issuers(config)?;
        Ok(Self {
            inner: Arc::new(provider.store),
            direct_transfers: Some(provider.direct_transfers),
        })
    }

    /// Returns the direct transfers this deployment can authorize, or `None`
    /// when every transfer has to be proxied.
    ///
    /// This is the whole answer: a caller reads the bundle's fields to learn
    /// which directions are offered and never asks configuration a second
    /// question about them.
    pub fn direct_transfers(&self) -> Option<DirectTransferIssuers> {
        self.direct_transfers.clone()
    }

    /// Hands out the provider client every handle and helper reads through.
    ///
    /// Take [`direct_transfers`](Self::direct_transfers) first: this consumes
    /// the configured store.
    pub fn into_shared(self) -> SharedObjectStore {
        self.inner
    }
}

/// Returns whether an endpoint uses TLS and belongs to a provider domain
/// family validated by the live conformance suite.
///
/// A missing override uses the provider's validated default endpoint.
/// Presigned URLs are bearer capabilities, so explicit `http` endpoints are
/// rejected even when their host belongs to a validated provider.
fn endpoint_is_proven(endpoint_url: Option<&str>, domain_families: &[&str]) -> bool {
    let Some(endpoint_url) = endpoint_url else {
        return true;
    };
    let Ok(uri) = endpoint_url.parse::<Uri>() else {
        return false;
    };
    uri.scheme_str() == Some("https") && endpoint_host_is_proven(&uri, domain_families)
}

/// Whether a parsed endpoint's host is one of the proven families, ignoring
/// the scheme. Config validation asks this separately so it can tell an
/// unproven provider apart from a proven one addressed insecurely.
pub(crate) fn endpoint_host_is_proven(uri: &Uri, domain_families: &[&str]) -> bool {
    let Some(host) = uri.host() else {
        return false;
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    domain_families.iter().any(|domain| {
        host == *domain
            || host
                .strip_suffix(domain)
                .is_some_and(|prefix| prefix.ends_with('.'))
    })
}

#[cfg(test)]
mod tests {
    use super::ConfiguredObjectStore;
    use crate::gcs::GcpGcsStoreConfig;
    use crate::keys::hint;
    use crate::local_fs_store::{LocalFsStore, LocalFsStoreConfig};
    use crate::presign::{
        PresignedPutRequest, AWS_S3_MAX_DIRECT_PUT_BYTES, CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
        GCP_GCS_MAX_DIRECT_PUT_BYTES,
    };
    use crate::s3_compatible::{AwsS3StoreConfig, CloudflareR2StoreConfig};
    use crate::test_support::gcs_fixture_service_account_key_file;
    use crate::ObjectStoreError;
    use crate::{AwsS3Credentials, CloudflareR2Credentials, GcpGcsCredentials, ObjectStore};
    use bytes::Bytes;
    use std::assert_matches;
    use std::time::{Duration, UNIX_EPOCH};

    const FIXTURE_ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
    const FIXTURE_SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    #[tokio::test]
    async fn configured_local_fs_scopes_optional_key_prefix() {
        let temp_dir = unique_temp_dir("configured-store-local");
        let store = ConfiguredObjectStore::local_fs(local_fs_config(&temp_dir, Some("tenant-a")))
            .expect("construct configured local fs store")
            .into_shared();
        let hint_key = hint(&loonfs_types::NamespaceId::parse("ns-1").expect("valid namespace id"));

        store
            .put_if_absent(&hint_key, Bytes::from_static(br#"{"ok":true}"#))
            .await
            .expect("write scoped object");

        let raw_store = LocalFsStore::new(temp_dir.path()).expect("open raw store");
        assert!(raw_store
            .head(&format!("tenant-a/{hint_key}"))
            .await
            .expect("head raw scoped object")
            .is_some());
        assert_eq!(
            store
                .list_prefix("namespaces/ns-1/")
                .await
                .expect("list scoped prefix"),
            vec![hint_key]
        );
    }

    #[test]
    fn a_gcs_store_signs_puts_and_gets_and_offers_no_multipart() {
        let transfers = gcs_store()
            .direct_transfers()
            .expect("gcs signs its native scheme");
        let put = transfers.put.expect("gcs signs whole-object writes");

        assert!(
            transfers.multipart.is_none(),
            "this adapter signs no multipart for GCS, which is spelled as an absent signer"
        );
        assert_eq!(put.max_content_bytes(), GCP_GCS_MAX_DIRECT_PUT_BYTES);
    }

    #[tokio::test]
    async fn the_gcs_bundle_signs_native_generation_preconditions() {
        let issuer = gcs_store()
            .direct_transfers()
            .and_then(|transfers| transfers.put)
            .expect("gcs presigner");

        let signed = issuer
            .presign_put(
                PresignedPutRequest {
                    object_key: "namespaces/demo/content/con_0123456789abcdef0123456789abcdef",
                    ttl: Duration::from_secs(900),
                },
                UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            )
            .await
            .expect("presign");

        assert!(signed
            .url
            .starts_with("https://storage.googleapis.com/bucket/tenant-a/namespaces/"));
        assert_eq!(
            signed
                .headers
                .get("x-goog-if-generation-match")
                .map(String::as_str),
            Some("0")
        );
        assert!(!signed.headers.contains_key("x-goog-hash"));
        // The S3 family's spellings never appear on a native GCS capability.
        assert!(!signed.headers.contains_key("if-none-match"));
        assert!(!signed.url.contains("X-Amz-"));
    }

    #[test]
    fn configured_object_store_offers_direct_transfers_only_on_proven_endpoints() {
        let temp_dir = unique_temp_dir("configured-store-kind");
        let local = ConfiguredObjectStore::local_fs(local_fs_config(&temp_dir, None))
            .expect("construct local store");
        assert!(local.direct_transfers().is_none());

        assert!(aws_s3_store(None).direct_transfers().is_some());
        assert!(
            aws_s3_store(Some("https://bucket.s3.cn-north-1.amazonaws.com.cn"))
                .direct_transfers()
                .is_some()
        );
        // Some other implementation of the S3 API, not covered by those runs.
        assert!(aws_s3_store(Some("https://gateway.example"))
            .direct_transfers()
            .is_none());
        assert!(aws_s3_store(Some("http://127.0.0.1:9000"))
            .direct_transfers()
            .is_none());

        assert!(r2_store("https://account.r2.cloudflarestorage.com")
            .direct_transfers()
            .is_some());
        assert!(r2_store("https://gateway.example")
            .direct_transfers()
            .is_none());

        // GCS has no endpoint override to judge, so every GCS store offers
        // direct transfers.
        assert!(gcs_store().direct_transfers().is_some());
    }

    #[test]
    fn a_proven_domain_without_tls_offers_no_direct_transfers() {
        assert!(aws_s3_store(Some("http://bucket.s3.amazonaws.com"))
            .direct_transfers()
            .is_none());
        assert!(aws_s3_store(Some("https://bucket.s3.amazonaws.com"))
            .direct_transfers()
            .is_some());

        assert!(r2_store("http://account.r2.cloudflarestorage.com")
            .direct_transfers()
            .is_none());
        assert!(r2_store("https://account.r2.cloudflarestorage.com")
            .direct_transfers()
            .is_some());
    }

    #[test]
    fn s3_compatible_stores_offer_all_three_directions() {
        let s3 = aws_s3_store(None).direct_transfers().expect("s3 transfers");
        let s3_put = s3.put.expect("s3 signs whole-object writes");
        assert!(s3.multipart.is_some());
        assert_eq!(s3_put.max_content_bytes(), AWS_S3_MAX_DIRECT_PUT_BYTES);

        let r2 = r2_store("https://account.r2.cloudflarestorage.com")
            .direct_transfers()
            .expect("r2 transfers");
        let r2_put = r2.put.expect("r2 signs whole-object writes");
        assert!(r2.multipart.is_some());
        assert_eq!(
            r2_put.max_content_bytes(),
            CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES
        );
        assert!(
            r2_put.max_content_bytes() < s3_put.max_content_bytes(),
            "R2 documents the smaller single-part ceiling of the two"
        );
    }

    fn aws_s3_store(endpoint_url: Option<&str>) -> ConfiguredObjectStore {
        ConfiguredObjectStore::aws_s3(AwsS3StoreConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: endpoint_url.map(ToOwned::to_owned),
            credentials: AwsS3Credentials::Static {
                access_key_id: "access".into(),
                secret_access_key: "secret".into(),
                session_token: None,
            },
            key_prefix: Some("tenant-a".to_owned()),
            force_path_style: true,
        })
        .expect("construct s3 store")
    }

    fn gcs_store() -> ConfiguredObjectStore {
        let (_key_dir, config) = gcs_config();
        ConfiguredObjectStore::gcp_gcs(config).expect("construct gcs store")
    }

    fn gcs_config() -> (tempfile::TempDir, GcpGcsStoreConfig) {
        let (key_dir, service_account_key_path) =
            gcs_fixture_service_account_key_file("configured-store-gcs");
        (
            key_dir,
            GcpGcsStoreConfig {
                bucket: "bucket".to_owned(),
                credentials: GcpGcsCredentials::ServiceAccountFile {
                    path: service_account_key_path.display().to_string(),
                },
                key_prefix: Some("tenant-a".to_owned()),
            },
        )
    }

    fn r2_store(endpoint_url: &str) -> ConfiguredObjectStore {
        ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
            bucket: "bucket".to_owned(),
            account_id: "account".to_owned(),
            endpoint_url: endpoint_url.to_owned(),
            credentials: CloudflareR2Credentials::Static {
                access_key_id: "access".into(),
                secret_access_key: "secret".into(),
            },
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct r2 store")
    }

    #[tokio::test]
    async fn cloudflare_r2_presigner_uses_path_style_account_endpoint() {
        let store = ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
            bucket: "bucket".to_owned(),
            account_id: "account".to_owned(),
            endpoint_url: "https://account.r2.cloudflarestorage.com".to_owned(),
            credentials: CloudflareR2Credentials::Static {
                access_key_id: "access".into(),
                secret_access_key: "secret".into(),
            },
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct r2 store");
        let issuer = store
            .direct_transfers()
            .and_then(|transfers| transfers.put)
            .expect("r2 presigner");

        let signed = issuer
            .presign_put(
                PresignedPutRequest {
                    object_key: "namespaces/demo/content/con_0123456789abcdef0123456789abcdef",
                    ttl: Duration::from_secs(900),
                },
                UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            )
            .await
            .expect("presign");

        assert!(signed
            .url
            .starts_with("https://account.r2.cloudflarestorage.com/bucket/tenant-a/namespaces/"));
        assert!(!signed.url.starts_with("https://bucket.account."));
    }

    #[test]
    fn configured_object_store_debug_redacts_presigner_credentials() {
        let store = ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
            bucket: "bucket".to_owned(),
            account_id: "account".to_owned(),
            endpoint_url: "https://account.r2.cloudflarestorage.com".to_owned(),
            credentials: CloudflareR2Credentials::Static {
                access_key_id: FIXTURE_ACCESS_KEY_ID.into(),
                secret_access_key: FIXTURE_SECRET_ACCESS_KEY.into(),
            },
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct r2 store");

        let rendered = format!("{store:?}");

        assert!(!rendered.contains(FIXTURE_ACCESS_KEY_ID));
        assert!(!rendered.contains(FIXTURE_SECRET_ACCESS_KEY));
    }

    #[tokio::test]
    async fn configured_local_fs_preserves_invalid_key_errors() {
        let temp_dir = unique_temp_dir("configured-store-invalid-key");
        let store = ConfiguredObjectStore::local_fs(local_fs_config(&temp_dir, Some("tenant-a")))
            .expect("construct configured local fs store")
            .into_shared();

        let error = store
            .put_if_absent("../hint.json", Bytes::from_static(br#"{"ok":true}"#))
            .await
            .expect_err("traversal key should be rejected");
        assert_matches!(
            error,
            ObjectStoreError::InvalidKey { object_key, .. } if object_key == "../hint.json"
        );
    }

    fn local_fs_config(root: &tempfile::TempDir, key_prefix: Option<&str>) -> LocalFsStoreConfig {
        LocalFsStoreConfig {
            root: root.path().display().to_string(),
            key_prefix: key_prefix.map(ToOwned::to_owned),
        }
    }

    fn unique_temp_dir(label: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("loonfs-objectstore-{label}-"))
            .tempdir()
            .expect("create temp dir")
    }
}
