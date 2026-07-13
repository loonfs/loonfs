//! Presigner for S3-compatible providers (AWS S3, Cloudflare R2).

use super::{ObjectTransferIssuer, PresignedPutRequest, PresignedUrl};
use crate::presign::aws_sigv4::{
    aws_dates, canonical_query_string, hex_lower, hmac_sha256, normalize_header_value,
    percent_encode_path, percent_encode_segment,
};
use crate::secret::SecretString;
use crate::ObjectStoreError;
use base64::Engine as _;
use loonfs_api::{ContentRef, ContentRefKind};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::SystemTime;

const S3_CREATE_ONLY_HEADER: &str = "if-none-match";
const S3_SHA256_CHECKSUM_HEADER: &str = "x-amz-checksum-sha256";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3PresignerConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub access_key_id: SecretString,
    pub secret_access_key: SecretString,
    pub session_token: Option<SecretString>,
    pub key_prefix: Option<String>,
    pub force_path_style: bool,
}

#[derive(Debug, Clone)]
pub struct S3CompatiblePresigner {
    config: S3PresignerConfig,
}

impl S3CompatiblePresigner {
    pub fn new(config: S3PresignerConfig) -> Result<Self, ObjectStoreError> {
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

    fn endpoint(&self, object_key: &str) -> Result<S3Endpoint, ObjectStoreError> {
        let scoped_key = scope_object_key(self.config.key_prefix.as_deref(), object_key)?;
        let encoded_key = percent_encode_path(&scoped_key);

        match self.config.endpoint_url.as_deref() {
            Some(endpoint_url) => {
                let parsed = parse_endpoint_url(endpoint_url)?;
                if self.config.force_path_style {
                    Ok(S3Endpoint {
                        scheme: parsed.scheme,
                        host: parsed.authority,
                        canonical_uri: format!(
                            "{}/{}/{}",
                            parsed.base_path,
                            percent_encode_segment(&self.config.bucket),
                            encoded_key
                        ),
                    })
                } else {
                    let bucket_prefix = format!("{}.", self.config.bucket);
                    let host = if parsed.authority.starts_with(&bucket_prefix) {
                        parsed.authority
                    } else {
                        format!("{}.{}", self.config.bucket, parsed.authority)
                    };
                    Ok(S3Endpoint {
                        scheme: parsed.scheme,
                        host,
                        canonical_uri: format!("{}/{}", parsed.base_path, encoded_key),
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
    ) -> Result<PresignedUrl, ObjectStoreError> {
        if request.expires_in.is_zero() {
            return Err(ObjectStoreError::transport(
                request.object_key,
                "presigned URL expiry must be greater than zero",
            ));
        }
        if request.expires_in.as_secs() > 604_800 {
            return Err(ObjectStoreError::transport(
                request.object_key,
                "presigned URL expiry must not exceed seven days",
            ));
        }

        let endpoint = self.endpoint(request.object_key)?;
        let required_headers = s3_direct_put_required_headers(request.content_ref)?;
        let dates = aws_dates(request.object_key, now)?;
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
            (
                "X-Amz-Expires".to_owned(),
                request.expires_in.as_secs().to_string(),
            ),
            ("X-Amz-SignedHeaders".to_owned(), signed_headers.clone()),
        ]);
        if let Some(token) = &self.config.session_token {
            query.insert("X-Amz-Security-Token".to_owned(), token.expose().to_owned());
        }
        let canonical_query = canonical_query_string(&query);
        let canonical_request = format!(
            "PUT\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
            endpoint.canonical_uri, canonical_query, canonical_headers, signed_headers
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
        let expires_at_ms =
            unix_ms(request.object_key, now)? + request.expires_in.as_millis() as u64;

        Ok(PresignedUrl {
            method: "PUT".to_owned(),
            url,
            headers: required_headers,
            expires_at_ms,
        })
    }
}

fn s3_direct_put_required_headers(
    content_ref: &ContentRef,
) -> Result<BTreeMap<String, String>, ObjectStoreError> {
    Ok(BTreeMap::from([
        (S3_CREATE_ONLY_HEADER.to_owned(), "*".to_owned()),
        (
            S3_SHA256_CHECKSUM_HEADER.to_owned(),
            s3_sha256_checksum_header(content_ref)?,
        ),
    ]))
}

fn s3_sha256_checksum_header(content_ref: &ContentRef) -> Result<String, ObjectStoreError> {
    if content_ref.kind != ContentRefKind::WholeFileV0 {
        return Err(invalid_direct_put_content(
            "direct_put only supports whole_file_v0 content refs",
        ));
    }

    let digest_hex = content_ref.digest.strip_prefix("sha256:").ok_or_else(|| {
        invalid_direct_put_content("direct_put content_ref digest must use sha256")
    })?;
    if digest_hex.len() != 64 {
        return Err(invalid_direct_put_content(
            "direct_put content_ref sha256 digest must be 64 hex characters",
        ));
    }
    if !digest_hex
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid_direct_put_content(
            "direct_put content_ref sha256 digest must be lowercase hex",
        ));
    }

    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let start = index * 2;
        *byte = u8::from_str_radix(&digest_hex[start..start + 2], 16).map_err(|_| {
            invalid_direct_put_content("direct_put content_ref sha256 digest must be lowercase hex")
        })?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedEndpoint {
    scheme: String,
    authority: String,
    base_path: String,
}

fn parse_endpoint_url(value: &str) -> Result<ParsedEndpoint, ObjectStoreError> {
    let (scheme, rest) = value
        .strip_prefix("https://")
        .map(|rest| ("https", rest))
        .or_else(|| value.strip_prefix("http://").map(|rest| ("http", rest)))
        .ok_or_else(|| {
            ObjectStoreError::Configuration(
                "endpoint url must start with http:// or https://".to_owned(),
            )
        })?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() {
        return Err(ObjectStoreError::Configuration(
            "endpoint url must include authority".to_owned(),
        ));
    }
    let path = path.trim_end_matches('/');
    Ok(ParsedEndpoint {
        scheme: scheme.to_owned(),
        authority: authority.to_owned(),
        base_path: if path.is_empty() {
            String::new()
        } else {
            format!("/{path}")
        },
    })
}

fn scope_object_key(prefix: Option<&str>, key: &str) -> Result<String, ObjectStoreError> {
    let key = key.trim_start_matches('/');
    if key.is_empty() {
        return Err(ObjectStoreError::InvalidKey {
            object_key: key.to_owned(),
            message: "object key must not be empty".to_owned(),
        });
    }
    Ok(match prefix {
        Some(prefix) if !prefix.trim_matches('/').is_empty() => {
            format!("{}/{}", prefix.trim_matches('/'), key)
        }
        _ => key.to_owned(),
    })
}

fn unix_ms(object_key: &str, time: SystemTime) -> Result<u64, ObjectStoreError> {
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
    use loonfs_api::{ContentRef, ContentRefKind};
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn presigned_put_scopes_key_and_signs_s3_compatible_required_headers() {
        let signer = S3CompatiblePresigner::new(S3PresignerConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: None,
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
            session_token: None,
            key_prefix: Some("tenant-a".to_owned()),
            force_path_style: false,
        })
        .expect("signer");

        let signed = signer
            .presign_put(
                PresignedPutRequest {
                    object_key: "content-stores/cs/blobs/sha256/ab/cd/digest",
                    content_ref: &ContentRef::whole_file_v0(b"hello"),
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

    #[test]
    fn presigned_put_rejects_content_ref_without_s3_checksum_header() {
        let signer = S3CompatiblePresigner::new(S3PresignerConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: None,
            access_key_id: "debug-access-key".into(),
            secret_access_key: "secret".into(),
            session_token: None,
            key_prefix: None,
            force_path_style: false,
        })
        .expect("signer");
        let content_ref = ContentRef {
            kind: ContentRefKind::Unsupported("future_kind".to_owned()),
            digest: "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                .to_owned(),
            size_bytes: 5,
        };

        let error = signer
            .presign_put(
                PresignedPutRequest {
                    object_key: "content-stores/cs/blobs/sha256/2c/f2/digest",
                    content_ref: &content_ref,
                    expires_in: Duration::from_secs(900),
                },
                UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            )
            .expect_err("unsupported content ref");

        assert!(matches!(error, ObjectStoreError::InvalidContentRef(_)));
    }

    #[test]
    fn presigned_put_accepts_bucket_specific_custom_endpoint() {
        let signer = S3CompatiblePresigner::new(S3PresignerConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-2".to_owned(),
            endpoint_url: Some("https://bucket.s3.us-east-2.amazonaws.com".to_owned()),
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
            session_token: None,
            key_prefix: Some("tenant-a".to_owned()),
            force_path_style: false,
        })
        .expect("signer");

        let signed = signer
            .presign_put(
                PresignedPutRequest {
                    object_key: "content-stores/cs/blobs/sha256/ab/cd/digest",
                    content_ref: &ContentRef::whole_file_v0(b"hello"),
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
