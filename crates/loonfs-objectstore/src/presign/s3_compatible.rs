//! Presigner for S3-compatible providers (AWS S3, Cloudflare R2).

use super::{ObjectTransferIssuer, PresignedPutRequest, PresignedUrl};
use crate::keyspace::{parse_endpoint_url, scope_object_key};
use crate::object_store::Result;
use crate::presign::aws_sigv4::{
    aws_dates, canonical_query_string, hex_lower, hmac_sha256, normalize_header_value,
    percent_encode_path, percent_encode_segment,
};
use crate::secret::SecretString;
use crate::ObjectStoreError;
use base64::Engine as _;
use loonfs_api::wire::hex::hex_decode_bytes;
use loonfs_api::{ChecksumAlgorithm, ContentRef, ContentRefKind};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{Duration, SystemTime};

const S3_CREATE_ONLY_HEADER: &str = "if-none-match";
const S3_SHA256_CHECKSUM_HEADER: &str = "x-amz-checksum-sha256";
/// Asks S3-family `HeadObject` to report the object's stored checksum.
pub(crate) const S3_CHECKSUM_MODE_HEADER: &str = "x-amz-checksum-mode";
const MAX_PRESIGN_EXPIRY: u64 = 7 * 24 * 60 * 60;

/// Supplies explicit SigV4 credentials and endpoint addressing for direct-put URLs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3PresignerConfig {
    /// Bucket incorporated into the signed request target.
    pub bucket: String,
    /// SigV4 region incorporated into the credential scope.
    pub region: String,
    /// S3-compatible endpoint override, or `None` for the regional AWS endpoint.
    pub endpoint_url: Option<String>,
    /// Access-key id exposed in the signed credential parameter.
    pub access_key_id: SecretString,
    /// Secret access key used to derive the request signature.
    pub secret_access_key: SecretString,
    /// Temporary credential token signed into the request, or `None` for long-lived credentials.
    pub session_token: Option<SecretString>,
    /// Logical prefix prepended before the object key is encoded and signed.
    pub key_prefix: Option<String>,
    /// Selects path-style bucket addressing instead of virtual-hosted style.
    pub force_path_style: bool,
}

/// Issues checksum-bound, create-only SigV4 PUT capabilities for S3-compatible providers.
#[derive(Debug, Clone)]
pub struct S3CompatiblePresigner {
    config: S3PresignerConfig,
}

impl S3CompatiblePresigner {
    /// Creates a presigner after validating required bucket, region, and credential values.
    ///
    /// Blank required values fail immediately; endpoint, key-prefix, content,
    /// expiry, and signing-time failures surface when [`ObjectTransferIssuer::presign_put`] runs.
    pub fn new(config: S3PresignerConfig) -> Result<Self> {
        if config.bucket.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "bucket must not be empty".to_owned(),
            ));
        }
        if config.region.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "region must not be empty".to_owned(),
            ));
        }
        if config.access_key_id.expose().trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "access key id must not be empty".to_owned(),
            ));
        }
        if config.secret_access_key.expose().trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "secret access key must not be empty".to_owned(),
            ));
        }
        Ok(Self { config })
    }

    /// Signs a `HeadObject` that asks the provider to report the object's
    /// stored full-object checksum.
    ///
    /// `GetObjectAttributes` would answer the same question on AWS S3 and
    /// return 501 on Cloudflare R2, so the head is the only portable surface
    /// and the only one this crate signs.
    pub(crate) fn presign_head_stored_checksum(
        &self,
        object_key: &str,
        expires_in: Duration,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        self.presign(
            "HEAD",
            object_key,
            BTreeMap::from([(S3_CHECKSUM_MODE_HEADER.to_owned(), "ENABLED".to_owned())]),
            expires_in,
            now,
        )
    }

    fn presign(
        &self,
        method: &str,
        object_key: &str,
        required_headers: BTreeMap<String, String>,
        expires_in: Duration,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        // Expiry comes from deployment configuration, not the transport:
        // a bad value is a config bug and must not look like network
        // weather.
        if expires_in.is_zero() {
            return Err(ObjectStoreError::Configuration(
                "presigned URL expiry must be greater than zero".to_owned(),
            ));
        }
        if expires_in.as_secs() > MAX_PRESIGN_EXPIRY {
            return Err(ObjectStoreError::Configuration(format!(
                "presigned URL expiry must not exceed {MAX_PRESIGN_EXPIRY} seconds"
            )));
        }

        let endpoint = self.endpoint(object_key)?;
        let dates = aws_dates(object_key, now)?;
        let credential_scope = format!(
            "{}/{}/s3/aws4_request",
            dates.short_date, self.config.region
        );
        let credential = format!(
            "{}/{}",
            self.config.access_key_id.expose(),
            credential_scope
        );

        let mut headers_to_sign = BTreeMap::from([("host".to_owned(), endpoint.host.clone())]);
        for (name, value) in &required_headers {
            headers_to_sign.insert(name.to_ascii_lowercase(), normalize_header_value(value));
        }
        let signed_headers = headers_to_sign
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(";");
        let mut canonical_headers = String::new();
        for (name, value) in &headers_to_sign {
            writeln!(&mut canonical_headers, "{name}:{value}")
                .expect("writing to String cannot fail");
        }

        let mut query = BTreeMap::from([
            ("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
            ("X-Amz-Credential".to_owned(), credential),
            ("X-Amz-Date".to_owned(), dates.amz_date.clone()),
            ("X-Amz-Expires".to_owned(), expires_in.as_secs().to_string()),
            ("X-Amz-SignedHeaders".to_owned(), signed_headers.clone()),
        ]);
        if let Some(token) = &self.config.session_token {
            query.insert("X-Amz-Security-Token".to_owned(), token.expose().to_owned());
        }
        let canonical_query = canonical_query_string(&query);
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
            method, endpoint.canonical_uri, canonical_query, canonical_headers, signed_headers
        );
        let hashed_request = hex_lower(&Sha256::digest(canonical_request.as_bytes()));
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            dates.amz_date, credential_scope, hashed_request
        );
        let signing_key = signing_key(
            self.config.secret_access_key.expose(),
            &dates.short_date,
            &self.config.region,
        );
        let signature = hex_lower(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let url = format!(
            "{}://{}{}?{}&X-Amz-Signature={}",
            endpoint.scheme, endpoint.host, endpoint.canonical_uri, canonical_query, signature
        );
        let expires_at_ms = unix_ms(object_key, now)? + expires_in.as_millis() as u64;

        Ok(PresignedUrl {
            method: method.to_owned(),
            url,
            headers: required_headers,
            expires_at_ms,
        })
    }

    fn endpoint(&self, object_key: &str) -> Result<S3Endpoint> {
        let scoped_key = scope_object_key(self.config.key_prefix.as_deref(), object_key)?;
        let encoded_key = percent_encode_path(&scoped_key);

        match self.config.endpoint_url.as_deref() {
            Some(endpoint_url) => {
                let parsed = parse_endpoint_url(endpoint_url)?;
                let base_path = if parsed.path.is_empty() {
                    String::new()
                } else {
                    format!("/{}", parsed.path)
                };
                if self.config.force_path_style {
                    Ok(S3Endpoint {
                        scheme: parsed.scheme.to_owned(),
                        host: parsed.authority.to_owned(),
                        canonical_uri: format!(
                            "{}/{}/{}",
                            base_path,
                            percent_encode_segment(&self.config.bucket),
                            encoded_key
                        ),
                    })
                } else {
                    let bucket_prefix = format!("{}.", self.config.bucket);
                    let host = if parsed.authority.starts_with(&bucket_prefix) {
                        parsed.authority.to_owned()
                    } else {
                        format!("{}.{}", self.config.bucket, parsed.authority)
                    };
                    Ok(S3Endpoint {
                        scheme: parsed.scheme.to_owned(),
                        host,
                        canonical_uri: format!("{}/{}", base_path, encoded_key),
                    })
                }
            }
            None => {
                let scheme = "https".to_owned();
                if self.config.force_path_style {
                    Ok(S3Endpoint {
                        scheme,
                        host: format!("s3.{}.amazonaws.com", self.config.region),
                        canonical_uri: format!(
                            "/{}/{}",
                            percent_encode_segment(&self.config.bucket),
                            encoded_key
                        ),
                    })
                } else {
                    Ok(S3Endpoint {
                        scheme,
                        host: format!(
                            "{}.s3.{}.amazonaws.com",
                            self.config.bucket, self.config.region
                        ),
                        canonical_uri: format!("/{encoded_key}"),
                    })
                }
            }
        }
    }
}

impl ObjectTransferIssuer for S3CompatiblePresigner {
    fn presign_put(
        &self,
        request: PresignedPutRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        let required_headers = s3_direct_put_required_headers(request.content_ref)?;
        self.presign(
            "PUT",
            request.object_key,
            required_headers,
            request.expires_in,
            now,
        )
    }
}

fn s3_direct_put_required_headers(content_ref: &ContentRef) -> Result<BTreeMap<String, String>> {
    Ok(BTreeMap::from([
        (S3_CREATE_ONLY_HEADER.to_owned(), "*".to_owned()),
        (
            S3_SHA256_CHECKSUM_HEADER.to_owned(),
            s3_sha256_checksum_header(content_ref)?,
        ),
    ]))
}

/// Converts the reference's stored checksum into the base64 spelling the
/// S3 family signs, so the provider refuses any body that does not hash to it.
fn s3_sha256_checksum_header(content_ref: &ContentRef) -> Result<String> {
    if content_ref.kind != ContentRefKind::BlobV1 {
        return Err(invalid_direct_put_content(
            "direct_put only supports blob_v1 content refs",
        ));
    }
    // Single PUT is the only direct producer today, and it signs a SHA-256.
    // The CRC algorithms exist in the format for direct multipart, which
    // presigns nothing.
    if content_ref.storage_checksum.algorithm != ChecksumAlgorithm::Sha256 {
        return Err(invalid_direct_put_content(
            "direct_put requires a sha256 storage checksum",
        ));
    }
    if content_ref.whole_file_sha256.as_deref() != Some(content_ref.storage_checksum.value.as_str())
    {
        return Err(invalid_direct_put_content(
            "direct_put content ref must carry the same sha256 as its storage checksum",
        ));
    }

    let digest = hex_decode_bytes(&content_ref.storage_checksum.value)
        .map_err(|_| invalid_direct_put_content("direct_put sha256 must be lowercase hex"))?;
    if digest.len() != 32 {
        return Err(invalid_direct_put_content(
            "direct_put sha256 must be 64 hex characters",
        ));
    }

    Ok(base64::engine::general_purpose::STANDARD.encode(digest))
}

fn invalid_direct_put_content(message: &str) -> ObjectStoreError {
    ObjectStoreError::InvalidContentRef(message.to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3Endpoint {
    scheme: String,
    host: String,
    canonical_uri: String,
}

fn unix_ms(object_key: &str, time: SystemTime) -> Result<u64> {
    let duration = time.duration_since(std::time::UNIX_EPOCH).map_err(|err| {
        ObjectStoreError::transport(
            object_key,
            format!("system time is before unix epoch: {err}"),
        )
    })?;
    Ok(duration.as_millis() as u64)
}

fn signing_key(secret: &str, date: &str, region: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}

#[cfg(test)]
mod tests {
    use super::{S3CompatiblePresigner, S3PresignerConfig};
    use crate::presign::{ObjectTransferIssuer, PresignedPutRequest};
    use crate::ObjectStoreError;
    use loonfs_api::{ChecksumAlgorithm, ContentId, ContentRef, ContentRefKind, StorageChecksum};
    use std::time::{Duration, UNIX_EPOCH};

    const CONTENT_KEY: &str = "content-stores/cs/objects/01/cnt_0123456789abcdef0123456789abcdef";

    fn content_ref() -> ContentRef {
        ContentRef::blob_v1(
            ContentId::parse("cnt_0123456789abcdef0123456789abcdef").expect("valid content id"),
            b"hello",
        )
    }

    fn presigner(key_prefix: Option<&str>, endpoint_url: Option<&str>) -> S3CompatiblePresigner {
        S3CompatiblePresigner::new(S3PresignerConfig {
            bucket: "bucket".to_owned(),
            region: endpoint_url.map_or("us-east-1", |_| "us-east-2").to_owned(),
            endpoint_url: endpoint_url.map(ToOwned::to_owned),
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
            session_token: None,
            key_prefix: key_prefix.map(ToOwned::to_owned),
            force_path_style: false,
        })
        .expect("signer")
    }

    #[test]
    fn presigned_put_scopes_key_and_signs_s3_compatible_required_headers() {
        let signed = presigner(Some("tenant-a"), None)
            .presign_put(
                PresignedPutRequest {
                    object_key: CONTENT_KEY,
                    content_ref: &content_ref(),
                    expires_in: Duration::from_secs(900),
                },
                UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            )
            .expect("presign");

        assert_eq!(signed.method, "PUT");
        assert_eq!(
            signed.headers.get("if-none-match").map(String::as_str),
            Some("*")
        );
        assert_eq!(
            signed
                .headers
                .get("x-amz-checksum-sha256")
                .map(String::as_str),
            Some("LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=")
        );
        assert!(signed
            .url
            .starts_with("https://bucket.s3.us-east-1.amazonaws.com/tenant-a/content-stores/"));
        assert!(signed
            .url
            .contains("X-Amz-SignedHeaders=host%3Bif-none-match%3Bx-amz-checksum-sha256"));
        assert!(!signed.url.contains("secret"));
    }

    /// The checksum readback rides the signature the same way the write's
    /// checksum does, so an operator cannot strip it in flight.
    #[test]
    fn presigned_head_signs_the_checksum_mode_header() {
        let signed = presigner(Some("tenant-a"), None)
            .presign_head_stored_checksum(
                CONTENT_KEY,
                Duration::from_secs(60),
                UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            )
            .expect("presign head");

        assert_eq!(signed.method, "HEAD");
        assert_eq!(
            signed
                .headers
                .get("x-amz-checksum-mode")
                .map(String::as_str),
            Some("ENABLED")
        );
        assert!(signed
            .url
            .contains("X-Amz-SignedHeaders=host%3Bx-amz-checksum-mode"));
        assert!(signed
            .url
            .starts_with("https://bucket.s3.us-east-1.amazonaws.com/tenant-a/content-stores/"));
    }

    #[test]
    fn presigned_put_rejects_content_refs_it_cannot_bind_to_the_write() {
        let signer = presigner(None, None);
        let unsupported_kind = ContentRef {
            kind: ContentRefKind::Unsupported("future_kind".to_owned()),
            ..content_ref()
        };
        let crc_only = ContentRef {
            storage_checksum: StorageChecksum {
                algorithm: ChecksumAlgorithm::Crc64nvme,
                value: "bbb7305bdf118bcb".to_owned(),
            },
            whole_file_sha256: None,
            ..content_ref()
        };
        let disagreeing_digests = ContentRef {
            whole_file_sha256: Some(
                "0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            ),
            ..content_ref()
        };

        for content_ref in [unsupported_kind, crc_only, disagreeing_digests] {
            let error = signer
                .presign_put(
                    PresignedPutRequest {
                        object_key: CONTENT_KEY,
                        content_ref: &content_ref,
                        expires_in: Duration::from_secs(900),
                    },
                    UNIX_EPOCH + Duration::from_secs(1_700_000_000),
                )
                .expect_err("unsignable content ref");
            assert!(matches!(error, ObjectStoreError::InvalidContentRef(_)));
        }
    }

    #[test]
    fn presigned_put_accepts_bucket_specific_custom_endpoint() {
        let signed = presigner(
            Some("tenant-a"),
            Some("https://bucket.s3.us-east-2.amazonaws.com"),
        )
        .presign_put(
            PresignedPutRequest {
                object_key: CONTENT_KEY,
                content_ref: &content_ref(),
                expires_in: Duration::from_secs(900),
            },
            UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        )
        .expect("presign");

        assert!(signed
            .url
            .starts_with("https://bucket.s3.us-east-2.amazonaws.com/tenant-a/content-stores/"));
        assert!(!signed.url.contains("bucket.bucket"));
    }

    #[test]
    fn presigner_debug_redacts_credentials() {
        let config = S3PresignerConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: None,
            access_key_id: "access".into(),
            secret_access_key: "debug-secret".into(),
            session_token: Some("debug-session-token".into()),
            key_prefix: Some("tenant-a".to_owned()),
            force_path_style: false,
        };

        let config_debug = format!("{config:?}");
        let signer = S3CompatiblePresigner::new(config).expect("signer");
        let signer_debug = format!("{signer:?}");

        for rendered in [config_debug, signer_debug] {
            assert!(!rendered.contains("debug-secret"));
            assert!(!rendered.contains("debug-access-key"));
            assert!(!rendered.contains("debug-session-token"));
            assert!(rendered.contains("<redacted>"));
        }
    }
}
