//! Presigner for S3-compatible providers (AWS S3, Cloudflare R2).

use super::{
    DirectGetIssuer, DirectMultipartIssuer, DirectPutIssuer, PresignedGetRequest,
    PresignedPartRequest, PresignedPutRequest, PresignedUrl,
};
use crate::keyspace::{parse_endpoint_url, scope_object_key};
use crate::object_store::Result;
use crate::presign::v4::{
    canonical_query_string, hex_lower, hmac_sha256, normalize_header_value, percent_encode_path,
    percent_encode_segment, signing_dates, unix_ms,
};
use crate::secret::SecretString;
use crate::ObjectStoreError;
use base64::Engine as _;
use loonfs_api::wire::hex::hex_decode_bytes;
use loonfs_api::{ChecksumAlgorithm, ContentRef, ContentRefKind, StorageChecksum};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{Duration, SystemTime};

const S3_CREATE_ONLY_HEADER: &str = "if-none-match";
const S3_SHA256_CHECKSUM_HEADER: &str = "x-amz-checksum-sha256";
const S3_CRC64NVME_CHECKSUM_HEADER: &str = "x-amz-checksum-crc64nvme";
const S3_CHECKSUM_ALGORITHM_HEADER: &str = "x-amz-checksum-algorithm";
/// Pins every multipart upload LoonFS opens to a whole-object checksum.
///
/// Full-object coverage is established here, at creation, rather than read
/// back later: Cloudflare R2 never reports `x-amz-checksum-type`, so a
/// reader could not tell a full-object checksum from a composite one.
const S3_CHECKSUM_TYPE_HEADER: &str = "x-amz-checksum-type";
const S3_FULL_OBJECT_CHECKSUM_TYPE: &str = "FULL_OBJECT";
const S3_CRC64NVME_ALGORITHM: &str = "CRC64NVME";
/// Asks S3-family `HeadObject` to report the object's stored checksum.
pub(crate) const S3_CHECKSUM_MODE_HEADER: &str = "x-amz-checksum-mode";
const MAX_PRESIGN_EXPIRY: u64 = 7 * 24 * 60 * 60;

/// AWS S3's documented maximum for a single `PutObject`: 5 GiB. A larger
/// object has to be uploaded in parts.
///
/// Amazon S3 user guide, "Uploading objects": the largest object that can be
/// uploaded in a single PUT is 5 GB, and the provider answers
/// `EntityTooLarge` above it.
pub const AWS_S3_MAX_DIRECT_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Cloudflare R2's documented maximum for a single-part upload: 5 MiB less
/// than 5 GiB.
///
/// Cloudflare R2 limits, "Maximum upload size — 5 GiB (single-part)", whose
/// footnote gives the exact figure as 5 MiB below 5 GiB (4.995 GiB). R2's
/// ceiling is the smaller of the two S3-compatible providers, so it is
/// stated separately rather than assumed equal to AWS's.
pub const CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024 - 5 * 1024 * 1024;

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
    /// This endpoint's documented maximum for a single PUT, reported by
    /// [`DirectPutIssuer::max_content_bytes`]. The S3-compatible providers
    /// do not agree on it, so each names its own
    /// ([`AWS_S3_MAX_DIRECT_PUT_BYTES`], [`CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES`]).
    pub direct_put_max_content_bytes: u64,
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
    /// expiry, and signing-time failures surface when [`DirectPutIssuer::presign_put`] runs.
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

    /// Signs `CreateMultipartUpload` for an object whose checksum will cover
    /// the whole assembly.
    pub(crate) fn presign_create_multipart(
        &self,
        object_key: &str,
        expires_in: Duration,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        self.presign_with_query(
            "POST",
            object_key,
            BTreeMap::from([("uploads".to_owned(), String::new())]),
            BTreeMap::from([
                (
                    S3_CHECKSUM_ALGORITHM_HEADER.to_owned(),
                    S3_CRC64NVME_ALGORITHM.to_owned(),
                ),
                (
                    S3_CHECKSUM_TYPE_HEADER.to_owned(),
                    S3_FULL_OBJECT_CHECKSUM_TYPE.to_owned(),
                ),
            ]),
            expires_in,
            now,
        )
    }

    /// Signs `AbortMultipartUpload`, the cleanup a terminated session runs.
    pub(crate) fn presign_abort_multipart(
        &self,
        object_key: &str,
        provider_upload_id: &str,
        expires_in: Duration,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        self.presign_with_query(
            "DELETE",
            object_key,
            BTreeMap::from([("uploadId".to_owned(), provider_upload_id.to_owned())]),
            BTreeMap::new(),
            expires_in,
            now,
        )
    }

    /// Signs `CompleteMultipartUpload` carrying the whole-object checksum.
    ///
    /// AWS S3 treats that checksum as a precondition and refuses to assemble
    /// an object that does not match it. Cloudflare R2 accepts the request
    /// and stores the true checksum instead, which is why completion still
    /// reads the object back rather than trusting this call's success.
    pub(crate) fn presign_complete_multipart(
        &self,
        object_key: &str,
        provider_upload_id: &str,
        full_object_checksum: &StorageChecksum,
        expires_in: Duration,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        self.presign_with_query(
            "POST",
            object_key,
            BTreeMap::from([("uploadId".to_owned(), provider_upload_id.to_owned())]),
            BTreeMap::from([(
                S3_CRC64NVME_CHECKSUM_HEADER.to_owned(),
                base64_crc64nvme(full_object_checksum)?,
            )]),
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
        self.presign_with_query(
            method,
            object_key,
            BTreeMap::new(),
            required_headers,
            expires_in,
            now,
        )
    }

    /// Signs one request, with any operation-selecting query parameters
    /// folded into the canonical query alongside the credential ones.
    ///
    /// The multipart operations are addressed by query parameter rather than
    /// by path (`?uploads`, `?uploadId=`, `?partNumber=`), so they have to
    /// participate in the signature or the provider computes a different one.
    fn presign_with_query(
        &self,
        method: &str,
        object_key: &str,
        operation_query: BTreeMap<String, String>,
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
        let dates = signing_dates(object_key, now)?;
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

        let mut query = operation_query;
        query.extend([
            ("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
            ("X-Amz-Credential".to_owned(), credential),
            ("X-Amz-Date".to_owned(), dates.timestamp.clone()),
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
            dates.timestamp, credential_scope, hashed_request
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

impl DirectPutIssuer for S3CompatiblePresigner {
    fn checksum_algorithm(&self) -> ChecksumAlgorithm {
        // The signed header this presigner binds a body to.
        ChecksumAlgorithm::Sha256
    }

    fn max_content_bytes(&self) -> u64 {
        self.config.direct_put_max_content_bytes
    }

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

impl DirectMultipartIssuer for S3CompatiblePresigner {
    fn presign_multipart_part(
        &self,
        request: PresignedPartRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        if request.part_number == 0 {
            return Err(invalid_direct_put_content("part numbers start at one"));
        }
        // No create-only header here, deliberately. A part is not the object:
        // re-uploading one is how a client retries a failed transfer, and both
        // providers take the last write and follow it with the checksum.
        self.presign_with_query(
            "PUT",
            request.object_key,
            BTreeMap::from([
                ("partNumber".to_owned(), request.part_number.to_string()),
                ("uploadId".to_owned(), request.provider_upload_id.to_owned()),
            ]),
            BTreeMap::from([(
                S3_CRC64NVME_CHECKSUM_HEADER.to_owned(),
                base64_crc64nvme(request.part_checksum)?,
            )]),
            request.expires_in,
            now,
        )
    }
}

impl DirectGetIssuer for S3CompatiblePresigner {
    fn presign_get(
        &self,
        request: PresignedGetRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        // No required headers, so `host` is the only name in
        // `X-Amz-SignedHeaders` and the only line in the canonical headers.
        // A `Range` the client adds is therefore outside the signature
        // entirely, and one issued URL serves ranged, resumed, and parallel
        // reads of the object without another round trip to the server.
        // Adding a required header here would silently cost that.
        self.presign(
            "GET",
            request.object_key,
            BTreeMap::new(),
            request.expires_in,
            now,
        )
    }
}

/// Converts a CRC-64/NVME into the base64 spelling the S3 family signs.
fn base64_crc64nvme(checksum: &StorageChecksum) -> Result<String> {
    if checksum.algorithm != ChecksumAlgorithm::Crc64nvme {
        return Err(invalid_direct_put_content(
            "multipart uploads are checksummed with crc64nvme",
        ));
    }
    let raw = hex_decode_bytes(&checksum.value)
        .map_err(|_| invalid_direct_put_content("crc64nvme must be lowercase hex"))?;
    if raw.len() != ChecksumAlgorithm::Crc64nvme.value_bytes() {
        return Err(invalid_direct_put_content(
            "crc64nvme must be 16 hex characters",
        ));
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(raw))
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

fn signing_key(secret: &str, date: &str, region: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}

#[cfg(test)]
mod tests {
    use super::{S3CompatiblePresigner, S3PresignerConfig, AWS_S3_MAX_DIRECT_PUT_BYTES};
    use crate::presign::{
        DirectGetIssuer, DirectPutIssuer, PresignedGetRequest, PresignedPutRequest,
    };
    use crate::ObjectStoreError;
    use loonfs_api::{ChecksumAlgorithm, ContentId, ContentRef, ContentRefKind, StorageChecksum};
    use std::time::{Duration, UNIX_EPOCH};

    const CONTENT_KEY: &str =
        "content-stores/cs/objects/01/23/con_0123456789abcdef0123456789abcdef";

    fn content_ref() -> ContentRef {
        ContentRef::blob_v1(
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("valid content id"),
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
            direct_put_max_content_bytes: AWS_S3_MAX_DIRECT_PUT_BYTES,
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

    /// The read capability signs `host` and nothing else, which is what
    /// leaves `Range` outside the signature: one issued URL serves ranged,
    /// resumed, and parallel reads without another round trip to the
    /// server. The live suite proves the provider agrees.
    #[test]
    fn presigned_get_signs_only_the_host_so_range_stays_unsigned() {
        let signed = presigner(Some("tenant-a"), None)
            .presign_get(
                PresignedGetRequest {
                    object_key: CONTENT_KEY,
                    expires_in: Duration::from_secs(900),
                },
                UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            )
            .expect("presign get");

        assert_eq!(signed.method, "GET");
        assert!(
            signed.headers.is_empty(),
            "a read capability requires the client to send nothing"
        );
        assert!(signed.url.contains("X-Amz-SignedHeaders=host"));
        assert!(!signed.url.to_ascii_lowercase().contains("range"));
        assert!(signed
            .url
            .starts_with("https://bucket.s3.us-east-1.amazonaws.com/tenant-a/content-stores/"));
        assert!(!signed.url.contains("secret"));
    }

    /// Reads and writes of the same object address the same URL: whatever
    /// the key prefix and endpoint style resolve to for one, they resolve
    /// to for the other, so a deployment cannot sign a write it is unable
    /// to sign a read of.
    #[test]
    fn presigned_get_addresses_the_same_object_the_write_did() {
        let signer = presigner(
            Some("tenant-a"),
            Some("https://bucket.s3.us-east-2.amazonaws.com"),
        );
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let written = signer
            .presign_put(
                PresignedPutRequest {
                    object_key: CONTENT_KEY,
                    content_ref: &content_ref(),
                    expires_in: Duration::from_secs(900),
                },
                now,
            )
            .expect("presign put");
        let read = signer
            .presign_get(
                PresignedGetRequest {
                    object_key: CONTENT_KEY,
                    expires_in: Duration::from_secs(900),
                },
                now,
            )
            .expect("presign get");

        let object_of = |url: &str| url.split('?').next().expect("url path").to_owned();
        assert_eq!(object_of(&read.url), object_of(&written.url));
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
            direct_put_max_content_bytes: AWS_S3_MAX_DIRECT_PUT_BYTES,
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
