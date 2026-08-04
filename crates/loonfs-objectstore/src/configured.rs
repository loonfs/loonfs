//! [`ConfiguredObjectStore`]: one provider client built from configuration,
//! paired with the direct transfers that provider can authorize.

use crate::abs::{AzureAbsStore, AzureAbsStoreConfig};
use crate::gcs::{GcpGcsStore, GcpGcsStoreConfig};
use crate::local_fs_store::LocalFsStore;
use crate::object_store::{Result, SharedObjectStore};
use crate::presign::{
    DirectTransferIssuers, S3CompatiblePresigner, S3PresignerConfig, AWS_S3_MAX_DIRECT_PUT_BYTES,
    CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
};
use crate::s3_compatible::{AwsS3StoreConfig, CloudflareR2StoreConfig, S3CompatibleStore};
use http::Uri;
use std::path::PathBuf;
use std::sync::Arc;

/// Endpoint families whose direct-transfer preconditions the live
/// conformance suite has actually run against.
///
/// A presigned write hands a client a capability and then trusts the
/// provider to have enforced the signed checksum and create-only
/// precondition — completion never reads the bytes back. Only these
/// endpoints have been run against that suite, so only they earn the trust.
/// An endpoint override outside the provider's own domain family is some
/// other implementation of the S3 API and is not covered by those runs.
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
    /// Uses Azure Blob Storage's native shared-key API.
    AzureAbs,
}

impl ConfiguredObjectStoreKind {
    /// Returns the stable kebab-case label, matching the `kind` tag used in
    /// config files.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalFs => "local-fs",
            Self::AwsS3 => "aws-s3",
            Self::CloudflareR2 => "cloudflare-r2",
            Self::GcpGcs => "gcp-gcs",
            Self::AzureAbs => "azure-abs",
        }
    }
}

/// One validated runtime provider, plus the direct transfers it can
/// authorize.
///
/// Construction is the only thing this type adds: it holds the provider
/// client as the same shared trait object every handle takes, so callers
/// dispatch through the provider's own [`ObjectStore`](crate::ObjectStore)
/// implementation rather than through a copy of it here.
///
/// Building the bundle *is* the trust decision. A provider that cannot sign
/// the preconditions direct transfers rest on, or an endpoint outside the
/// families the live conformance suite has run against, produces no bundle
/// at all — so nothing above this crate re-derives that judgement from
/// configuration.
#[derive(Debug)]
pub struct ConfiguredObjectStore {
    inner: SharedObjectStore,
    direct_transfers: Option<DirectTransferIssuers>,
}

impl ConfiguredObjectStore {
    /// Opens a local-filesystem provider with optional logical key scoping.
    ///
    /// Construction fails outside Unix-family platforms, when the root cannot
    /// be created, or when `key_prefix` is invalid.
    pub fn local_fs(root: impl Into<PathBuf>, key_prefix: Option<&str>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(LocalFsStore::with_key_prefix(root, key_prefix)?),
            direct_transfers: None,
        })
    }

    /// Builds an AWS S3 store, and the direct transfers it can authorize on
    /// a proven endpoint.
    ///
    /// Construction fails when signing, provider, runtime, or key-prefix configuration is invalid.
    pub fn aws_s3(config: AwsS3StoreConfig) -> Result<Self> {
        let direct_transfers =
            endpoint_is_proven(config.endpoint_url.as_deref(), AWS_S3_PROVEN_DOMAINS)
                .then(|| {
                    S3CompatiblePresigner::new(S3PresignerConfig {
                        bucket: config.bucket.clone(),
                        region: config.region.clone(),
                        endpoint_url: config.endpoint_url.clone(),
                        access_key_id: config.access_key_id.clone(),
                        secret_access_key: config.secret_access_key.clone(),
                        session_token: config.session_token.clone(),
                        key_prefix: config.key_prefix.clone(),
                        force_path_style: config.force_path_style,
                        direct_put_max_content_bytes: AWS_S3_MAX_DIRECT_PUT_BYTES,
                    })
                    .map(s3_compatible_transfers)
                })
                .transpose()?;
        let store = S3CompatibleStore::aws_s3(config)?;
        Ok(Self {
            inner: Arc::new(store),
            direct_transfers,
        })
    }

    /// Builds a Cloudflare R2 store, and the direct transfers it can
    /// authorize on a proven endpoint.
    ///
    /// Construction fails when signing, provider, runtime, or key-prefix configuration is invalid.
    pub fn cloudflare_r2(config: CloudflareR2StoreConfig) -> Result<Self> {
        let direct_transfers = endpoint_is_proven(
            Some(config.endpoint_url.as_str()),
            CLOUDFLARE_R2_PROVEN_DOMAINS,
        )
        .then(|| {
            S3CompatiblePresigner::new(S3PresignerConfig {
                bucket: config.bucket.clone(),
                region: "auto".to_owned(),
                endpoint_url: Some(config.endpoint_url.clone()),
                access_key_id: config.access_key_id.clone(),
                secret_access_key: config.secret_access_key.clone(),
                session_token: None,
                key_prefix: config.key_prefix.clone(),
                force_path_style: true,
                direct_put_max_content_bytes: CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
            })
            .map(s3_compatible_transfers)
        })
        .transpose()?;
        let store = S3CompatibleStore::cloudflare_r2(config)?;
        Ok(Self {
            inner: Arc::new(store),
            direct_transfers,
        })
    }

    /// Builds a native GCS store without direct-transfer issuance.
    ///
    /// Construction fails when credentials, provider runtime, or key-prefix configuration is invalid.
    pub fn gcp_gcs(config: GcpGcsStoreConfig) -> Result<Self> {
        let store = GcpGcsStore::new(config)?;
        Ok(Self {
            inner: Arc::new(store),
            direct_transfers: None,
        })
    }

    /// Builds a native Azure Blob store without direct-transfer issuance.
    ///
    /// Construction fails when credentials, provider runtime, or key-prefix configuration is invalid.
    pub fn azure_abs(config: AzureAbsStoreConfig) -> Result<Self> {
        let store = AzureAbsStore::new(config)?;
        Ok(Self {
            inner: Arc::new(store),
            direct_transfers: None,
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

/// The S3-compatible providers sign all three directions with one presigner.
fn s3_compatible_transfers(presigner: S3CompatiblePresigner) -> DirectTransferIssuers {
    let presigner = Arc::new(presigner);
    DirectTransferIssuers::read_only(presigner.clone())
        .with_put(presigner.clone())
        .with_multipart(presigner)
}

/// Whether an endpoint sits in one of the domain families whose signed
/// preconditions the live conformance suite has exercised, *and* is reached
/// over TLS.
///
/// An absent endpoint is the provider's own default, which qualifies on both
/// counts. TLS is not incidental here: a presigned URL is a bearer
/// capability to write or read one object, so issuing one over `http` would
/// put it, and the signed headers with it, on the wire in cleartext for
/// anyone on the path to replay. An `http` override of a provider's own
/// domain is a misconfiguration rather than a different provider, and
/// [`StoreConfig::validate`](crate::StoreConfig::validate) rejects it by
/// name; this is the second gate, for a configuration built in Rust that
/// never passed through that validation.
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
    use crate::abs::AzureAbsStoreConfig;
    use crate::gcs::GcpGcsStoreConfig;
    use crate::keys::wal_head;
    use crate::local_fs_store::LocalFsStore;
    use crate::presign::{
        DirectTransferIssuers, PresignedPutRequest, S3CompatiblePresigner, S3PresignerConfig,
        AWS_S3_MAX_DIRECT_PUT_BYTES, CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
    };
    use crate::s3_compatible::{AwsS3StoreConfig, CloudflareR2StoreConfig};
    use crate::ObjectStore;
    use crate::ObjectStoreError;
    use bytes::Bytes;
    use loonfs_api::ChecksumAlgorithm;
    use loonfs_api::ContentId;
    use loonfs_api::ContentRef;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const AZURITE_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    const FAKE_GCS_SERVICE_ACCOUNT_KEY: &str = r#"{"private_key":"private_key","private_key_id":"private_key_id","client_email":"client_email","disable_oauth":true}"#;

    #[tokio::test]
    async fn configured_local_fs_scopes_optional_key_prefix() {
        let temp_dir = unique_temp_dir("configured-store-local");
        let store = ConfiguredObjectStore::local_fs(&temp_dir, Some("tenant-a"))
            .expect("construct configured local fs store")
            .into_shared();
        let head_key = wal_head("ns-1");

        store
            .put_overwrite(&head_key, Bytes::from_static(br#"{"ok":true}"#))
            .await
            .expect("write scoped object");

        let raw_store = LocalFsStore::new(&temp_dir).expect("open raw store");
        assert!(raw_store
            .head(&format!("tenant-a/{head_key}"))
            .await
            .expect("head raw scoped object")
            .is_some());
        assert_eq!(
            store
                .list_prefix("namespaces/ns-1/")
                .await
                .expect("list scoped prefix"),
            vec![head_key]
        );
    }

    /// Only a provider that can presign, on an endpoint the live conformance
    /// suite has run against, offers direct transfers at all.
    #[test]
    fn configured_object_store_offers_direct_transfers_only_on_proven_endpoints() {
        let local = ConfiguredObjectStore::local_fs(unique_temp_dir("configured-store-kind"), None)
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

        let gcs_service_account_key_path =
            fake_gcs_service_account_key_file("configured-store-gcs-kind");
        let gcs = ConfiguredObjectStore::gcp_gcs(GcpGcsStoreConfig {
            bucket: "bucket".to_owned(),
            service_account_key_path: gcs_service_account_key_path.display().to_string(),
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct gcs store");
        assert!(gcs.direct_transfers().is_none());

        let azure = ConfiguredObjectStore::azure_abs(AzureAbsStoreConfig {
            account_name: "devstoreaccount1".to_owned(),
            container_name: "container".to_owned(),
            access_key: AZURITE_ACCOUNT_KEY.into(),
            endpoint_url: None,
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct azure store");
        assert!(azure.direct_transfers().is_none());
    }

    /// A presigned URL is a bearer capability, so a proven domain reached
    /// over plain `http` earns no bundle: the capability and its signed
    /// headers would go out in cleartext.
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

    /// The S3-compatible providers sign every direction, and each names its
    /// own documented single-request ceiling.
    #[test]
    fn s3_compatible_stores_offer_all_three_directions() {
        let s3 = aws_s3_store(None).direct_transfers().expect("s3 transfers");
        let s3_put = s3.put.expect("s3 signs whole-object writes");
        assert!(s3.multipart.is_some());
        assert_eq!(s3_put.checksum_algorithm(), ChecksumAlgorithm::Sha256);
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

    /// A store that signs reads and whole-object writes but has no multipart
    /// API is expressible: absence is a missing field, not a failing call.
    #[test]
    fn a_bundle_can_offer_put_and_get_without_multipart() {
        let presigner = Arc::new(
            S3CompatiblePresigner::new(S3PresignerConfig {
                bucket: "bucket".to_owned(),
                region: "us-east-1".to_owned(),
                endpoint_url: None,
                access_key_id: "access".into(),
                secret_access_key: "secret".into(),
                session_token: None,
                key_prefix: None,
                force_path_style: false,
                direct_put_max_content_bytes: AWS_S3_MAX_DIRECT_PUT_BYTES,
            })
            .expect("presigner"),
        );
        let transfers = DirectTransferIssuers::read_only(presigner.clone()).with_put(presigner);

        assert!(transfers.put.is_some());
        assert!(transfers.multipart.is_none());
    }

    fn aws_s3_store(endpoint_url: Option<&str>) -> ConfiguredObjectStore {
        ConfiguredObjectStore::aws_s3(AwsS3StoreConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: endpoint_url.map(ToOwned::to_owned),
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
            session_token: None,
            key_prefix: Some("tenant-a".to_owned()),
            force_path_style: true,
        })
        .expect("construct s3 store")
    }

    fn r2_store(endpoint_url: &str) -> ConfiguredObjectStore {
        ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
            bucket: "bucket".to_owned(),
            account_id: "account".to_owned(),
            endpoint_url: endpoint_url.to_owned(),
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct r2 store")
    }

    #[test]
    fn cloudflare_r2_presigner_uses_path_style_account_endpoint() {
        let store = ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
            bucket: "bucket".to_owned(),
            account_id: "account".to_owned(),
            endpoint_url: "https://account.r2.cloudflarestorage.com".to_owned(),
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
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
                    object_key:
                        "content-stores/cs/objects/01/23/con_0123456789abcdef0123456789abcdef",
                    content_ref: &ContentRef::blob_v1(ContentId::generate(), b"hello"),
                    expires_in: Duration::from_secs(900),
                },
                UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            )
            .expect("presign");

        assert!(signed.url.starts_with(
            "https://account.r2.cloudflarestorage.com/bucket/tenant-a/content-stores/"
        ));
        assert!(!signed.url.starts_with("https://bucket.account."));
    }

    #[test]
    fn configured_object_store_debug_redacts_presigner_credentials() {
        let store = ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
            bucket: "bucket".to_owned(),
            account_id: "account".to_owned(),
            endpoint_url: "https://account.r2.cloudflarestorage.com".to_owned(),
            access_key_id: "access".into(),
            secret_access_key: "debug-secret".into(),
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct r2 store");

        let rendered = format!("{store:?}");

        assert!(!rendered.contains("debug-secret"));
        assert!(!rendered.contains("debug-access-key"));
    }

    #[tokio::test]
    async fn configured_local_fs_preserves_invalid_key_errors() {
        let temp_dir = unique_temp_dir("configured-store-invalid-key");
        let store = ConfiguredObjectStore::local_fs(&temp_dir, Some("tenant-a"))
            .expect("construct configured local fs store")
            .into_shared();

        let error = store
            .put_overwrite("../head.json", Bytes::from_static(br#"{"ok":true}"#))
            .await
            .expect_err("traversal key should be rejected");
        assert!(matches!(
            error,
            ObjectStoreError::InvalidKey { object_key, .. } if object_key == "../head.json"
        ));
    }

    #[allow(clippy::disallowed_methods)]
    fn unique_temp_dir(label: &str) -> PathBuf {
        // Test-only unique paths are an entropy boundary, not protocol time.
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("loonfs-objectstore-{label}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn fake_gcs_service_account_key_file(label: &str) -> PathBuf {
        let path = unique_temp_dir(label).join("service-account.json");
        fs::write(&path, FAKE_GCS_SERVICE_ACCOUNT_KEY).expect("write fake service account key");
        path
    }
}
