//! Presigner for Google Cloud Storage's native V4 signed URLs.
//!
//! This uses the native GCS XML API because the S3-compatible API did not
//! enforce write preconditions in live tests. The native API supports object
//! generation checks and stores CRC-32C checksums.
//!
use super::{
    DirectGetIssuer, DirectPutIssuer, PresignedGetRequest, PresignedPutRequest, PresignedUrl,
};
use crate::keyspace::{normalize_key_prefix, scope_object_key};
use crate::object_store::Result;
use crate::presign::v4::{
    hex_lower, percent_encode_path, percent_encode_segment, presign_v4, signing_dates, V4Endpoint,
    V4RequestParts, V4Scheme,
};
use crate::ObjectStoreError;
use async_trait::async_trait;
use base64::Engine as _;
use loonfs_api::wire::hex::hex_encode_bytes;
use loonfs_api::{Checksum, ChecksumAlgorithm};
use ring::rand::SystemRandom;
use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};
use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime};

/// Host used for every GCS signed URL.
const GCS_HOST: &str = "storage.googleapis.com";

/// A generation of zero makes the request create-only.
const GCS_GENERATION_MATCH_HEADER: &str = "x-goog-if-generation-match";
const GCS_CREATE_ONLY_GENERATION: &str = "0";

/// The signing scheme, written into both the algorithm query parameter and
/// the first line of the string to sign.
const GCS_SIGNING_ALGORITHM: &str = "GOOG4-RSA-SHA256";

/// The credential scope's location and terminator. GCS accepts `auto` for the
/// location rather than requiring the bucket's region, which is what Google's
/// own client libraries write, so a deployment never has to configure one.
const GCS_CREDENTIAL_SCOPE_SUFFIX: &str = "auto/storage/goog4_request";

/// Maximum GCS object size. GCS does not document a smaller limit for a
/// single-request upload.
pub const GCP_GCS_MAX_DIRECT_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;

/// Configuration for native GCS signed URLs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsPresignerConfig {
    /// Bucket incorporated into the signed request target.
    pub bucket: String,
    /// Filesystem path to the service-account JSON whose private key signs.
    pub service_account_key_path: String,
    /// Logical prefix prepended before the object key is encoded and signed.
    pub key_prefix: Option<String>,
}

/// Creates native GCS V4 signed URLs.
pub struct GcsV4Presigner {
    bucket: String,
    key_prefix: Option<String>,
    /// The service account named in the credential parameter, which is also
    /// the identity GCS resolves the signing key from.
    client_email: String,
    signing_key: RsaKeyPair,
}

/// The two fields of a service-account JSON this module needs. Every other
/// field is ignored rather than rejected: the file is Google's, and its shape
/// is theirs to extend.
#[derive(serde::Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
}

impl fmt::Debug for GcsV4Presigner {
    /// Renders no key material and no service-account identity. A signing key
    /// is a bearer credential for the whole bucket, so nothing about it
    /// reaches a log through a `Debug` of the store that holds it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcsV4Presigner")
            .field("bucket", &self.bucket)
            .field("key_prefix", &self.key_prefix)
            .field("client_email", &"<redacted>")
            .field("signing_key", &"<redacted>")
            .finish()
    }
}

impl GcsV4Presigner {
    /// Creates a presigner, reading and parsing the service-account key once.
    ///
    /// The key prefix is normalized here, through the same helper the
    /// provider client uses. That is not tidiness: the two have to resolve
    /// an object key to the same string or a signed write lands somewhere
    /// the store's own reads, listings, and collection never look — an
    /// object committed, invisible, and unreclaimable.
    ///
    /// Construction fails for an unusable key prefix, an unreadable or
    /// malformed key file, or a private key that is not an RSA key in PKCS#8
    /// form.
    pub fn new(config: GcsPresignerConfig) -> Result<Self> {
        let raw = std::fs::read(&config.service_account_key_path).map_err(|err| {
            ObjectStoreError::Configuration(format!("service account key is unreadable: {err}"))
        })?;
        // The parse error is not quoted: a malformed key file's contents are
        // key material as often as not.
        let key: ServiceAccountKey = serde_json::from_slice(&raw).map_err(|_| {
            ObjectStoreError::Configuration(
                "service account key must be JSON carrying `client_email` and `private_key`"
                    .to_owned(),
            )
        })?;
        if key.client_email.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "service account key must name a client_email".to_owned(),
            ));
        }

        let der = pkcs8_der(&key.private_key)?;
        let signing_key = RsaKeyPair::from_pkcs8(&der).map_err(|_| {
            ObjectStoreError::Configuration(
                "service account private key must be an RSA key in PKCS#8 form".to_owned(),
            )
        })?;

        Ok(Self {
            bucket: config.bucket,
            key_prefix: normalize_key_prefix(config.key_prefix.as_deref())?,
            client_email: key.client_email,
            signing_key,
        })
    }

    /// Signs a `HEAD` that reads an object's size and stored checksum back.
    ///
    /// GCS reports the stored CRC-32C in the `x-goog-hash` response header of
    /// an ordinary object request, so no special request header is needed to
    /// ask for it and the capability signs `host` alone.
    pub(crate) fn presign_head_stored_checksum(
        &self,
        object_key: &str,
        expires_in: Duration,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        self.presign("HEAD", object_key, BTreeMap::new(), expires_in, now)
    }

    fn presign(
        &self,
        method: &str,
        object_key: &str,
        required_headers: BTreeMap<String, String>,
        expires_in: Duration,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        let scoped_key = scope_object_key(self.key_prefix.as_deref(), object_key)?;
        let canonical_uri = format!(
            "/{}/{}",
            percent_encode_segment(&self.bucket),
            percent_encode_path(&scoped_key)
        );
        let dates = signing_dates(object_key, now)?;
        let credential_scope = format!("{}/{GCS_CREDENTIAL_SCOPE_SUFFIX}", dates.short_date);
        presign_v4(
            V4Scheme {
                algorithm: GCS_SIGNING_ALGORITHM,
                query_prefix: "X-Goog",
                credential: format!("{}/{credential_scope}", self.client_email),
                credential_scope,
                extra_query: BTreeMap::new(),
                signing_dates: dates,
            },
            V4Endpoint {
                scheme: "https".to_owned(),
                host: GCS_HOST.to_owned(),
                canonical_uri,
            },
            method,
            V4RequestParts {
                object_key,
                operation_query: BTreeMap::new(),
                required_headers,
            },
            expires_in,
            now,
            |message| self.sign(object_key, message),
        )
    }

    /// Produces the hex-encoded RSASSA-PKCS1-v1_5 SHA-256 signature GCS
    /// verifies against the service account's public key.
    fn sign(&self, object_key: &str, message: &[u8]) -> Result<String> {
        let mut signature = vec![0u8; self.signing_key.public().modulus_len()];
        self.signing_key
            .sign(
                &RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                message,
                &mut signature,
            )
            .map_err(|_| {
                ObjectStoreError::transport(
                    object_key,
                    "service account key could not sign the request".to_owned(),
                )
            })?;
        Ok(hex_lower(&signature))
    }
}

#[async_trait]
impl DirectPutIssuer for GcsV4Presigner {
    fn stored_checksum_algorithm(&self) -> ChecksumAlgorithm {
        ChecksumAlgorithm::Crc32c
    }

    fn max_content_bytes(&self) -> u64 {
        GCP_GCS_MAX_DIRECT_PUT_BYTES
    }

    async fn presign_put(
        &self,
        request: PresignedPutRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        let required_headers = BTreeMap::from([(
            GCS_GENERATION_MATCH_HEADER.to_owned(),
            GCS_CREATE_ONLY_GENERATION.to_owned(),
        )]);
        self.presign(
            "PUT",
            request.object_key,
            required_headers,
            request.expires_in,
            now,
        )
    }
}

#[async_trait]
impl DirectGetIssuer for GcsV4Presigner {
    async fn presign_get(
        &self,
        request: PresignedGetRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        // No required headers, so `host` is the only name in
        // `X-Goog-SignedHeaders` and the only line in the canonical headers.
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

/// Reads the stored CRC-32C out of a GCS `x-goog-hash` header value.
///
/// The header lists one or more `<algorithm>=<base64>` pairs in an
/// unspecified order and may carry algorithms this format does not name, so
/// the CRC-32C is selected rather than positioned. An absent or unusable
/// crc32c answers `None`, and the caller treats that as a failure: an object
/// GCS will not describe is never completed on its size alone.
pub(crate) fn stored_crc32c(header_value: &str) -> Option<Checksum> {
    for pair in header_value.split(',') {
        let Some(encoded) = pair.trim().strip_prefix("crc32c=") else {
            continue;
        };
        let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
            continue;
        };
        if raw.len() != ChecksumAlgorithm::Crc32c.value_bytes() {
            continue;
        }
        return Some(Checksum {
            algorithm: ChecksumAlgorithm::Crc32c,
            value: hex_encode_bytes(&raw),
        });
    }
    None
}

/// Decodes a PEM-wrapped PKCS#8 private key into DER.
///
/// Service-account JSON carries the key as a PEM block with escaped
/// newlines, which JSON decoding has already turned back into real ones.
fn pkcs8_der(private_key_pem: &str) -> Result<Vec<u8>> {
    const BEGIN: &str = "-----BEGIN PRIVATE KEY-----";
    const END: &str = "-----END PRIVATE KEY-----";

    let body = private_key_pem
        .trim()
        .strip_prefix(BEGIN)
        .and_then(|rest| rest.trim_end().strip_suffix(END))
        .ok_or_else(|| {
            ObjectStoreError::Configuration(
                "service account private key must be a PKCS#8 PEM block".to_owned(),
            )
        })?;
    let base64_body: String = body.split_whitespace().collect();
    base64::engine::general_purpose::STANDARD
        .decode(base64_body)
        .map_err(|_| {
            ObjectStoreError::Configuration(
                "service account private key is not valid base64".to_owned(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::{stored_crc32c, GcsPresignerConfig, GcsV4Presigner, GCP_GCS_MAX_DIRECT_PUT_BYTES};
    use crate::keyspace::{normalize_key_prefix, scope_object_key};
    use crate::presign::{
        DirectGetIssuer, DirectPutIssuer, PresignedGetRequest, PresignedPutRequest,
    };
    use crate::test_support::{gcs_fixture_service_account_key_file, GCS_FIXTURE_CLIENT_EMAIL};
    use crate::ObjectStoreError;
    use loonfs_api::ChecksumAlgorithm;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const CONTENT_KEY: &str = "namespaces/demo/content/con_0123456789abcdef0123456789abcdef";
    /// 2023-11-14T22:13:20Z, the instant every expected signature below was
    /// produced at.
    const SIGNING_EPOCH_SECS: u64 = 1_700_000_000;
    const EXPIRES_IN: Duration = Duration::from_secs(900);
    /// CRC-32C of `b"hello"`, in the lowercase hex this format stores and in
    /// the big-endian base64 GCS reads.
    const HELLO_CRC32C_HEX: &str = "9a71bb4c";

    /// The query prefix every expected URL shares: the algorithm, the
    /// credential scope, and the request timestamp.
    const EXPECTED_CREDENTIAL: &str = "X-Goog-Algorithm=GOOG4-RSA-SHA256\
         &X-Goog-Credential=loonfs-presign-fixture%40loonfs-tests.iam.gserviceaccount.com\
         %2F20231114%2Fauto%2Fstorage%2Fgoog4_request\
         &X-Goog-Date=20231114T221320Z&X-Goog-Expires=900";

    // Independent Python canonicalization and OpenSSL signing pin the exact request.
    const PUT_PREFIXED_SIGNATURE: &str = "a6e71b5a0046de4486b7a16341153ba9c499fefd27a122248580d0bfc7e3abe453d9dae811a479f7c48467d706cfcc5cc9a0b8b9195129a1d0f1fd21da2828141f4c305b09b1e5277d52adc683810a65bb4612ad71033004723c1971f017aec0a4b6147c872380b94468a8355887ab56cd69906c4ab208015b68b9af5ac84702cedc1459911d96c2cb72dad19e89006119e2f355af281ff48b3a4ef5a5a597f72582829f51fc8d4705b087b22a0efbc8c0374db177aea60276553d8b635ed0d40c7f3aa380a34052741837b4477804e06c6dbb554b6fff2f86557a4fbd5ca3c58d7807756fcc6ce418d42bd4026c01e4473500c305441731aa8be68cf975e00e";
    const GET_PREFIXED_SIGNATURE: &str = "58232d0a69c6b381284f09344e217259a400875d39b2e0e7d4d52dbe148e2db9f952f994f2ff0a20a46ed5c438000931b7c9ec412f725d438f1dcd7a4bfa422bb3f1b69b88081afb16c353fa294c076aa9aaa51ee279bc7d312391ee33cf4a070cc100c97816fb9f19fc807c3213ba2000341a6a886fd4687bb97ba6a4f27dd7266dbcb9cc31a184a3e996312810115377235413bf292a0e708e9a47d3f06404658d6e38145c5f06497d8d5bcaf944a0b6be88d6e9bae713d9ef80bba74849a1b4e82e71682d9e813d27f5ba915b3a351d2da72fa8ffa92d5a8b83e24f5a01446e797155de1d74d9ebddd0fa8a136716d8d441516dfa6da0e23699a274a9ed84";
    const HEAD_PREFIXED_SIGNATURE: &str = "68bc280612724b108e0692aff38037c8c7604e3fa3bf3cfe97b4e58e5f5ff8de8d8743b3319399c072d2ba5a3c80320f0744fdabe989388238d45810bf0e87d1f5809e49da2cba2874b3a4cbdb4382854f243d11997172b9799398d11f3a55ee5a0daab7e9f116be9b302ea17bf047ab27ddbcb326c33fca46fa99fe3e8231e465b9642b34710164f68bdbcaa73bd1f5750fea1d14fbf942210d64d44310ddfb81549263a9a28ae5c5e0a84922ef5cedbc4ee4f1b197609a16ad63e5c3c3951ea1d3eaeb7962f0300916564a8a10601961ce80542b6cf9eab6fcf525cf14a67a5a0672813678cbf80840f07b2e959e6415b214cdc2596c5ebbeaf0ea12104985";
    const PUT_UNPREFIXED_SIGNATURE: &str = "201e2951edf8d3d2da08d8dcb9bea6318dda0a2927480717202ce6294c694aeb98fb8f128f73470b790decc1e38e5c5fdb454e3c5fb438c10bbdc3f1e0a2b5f73cb8f160477ae507ea7ccf6cee93d00420b936558c94673d932c8854b5864466035c6fa5e8a4c17c9e6be5908f1659c13a1720b24da96537fe641816074391d752d8d0feb8307faa95003601d05918ae9602e23b40abf4afb734065ed528404981ff9d0f5d8e46cac190dbe69b63c96d302d4eecf8c01e041015817a8287621e8e32ac3b2060f84e0827ccf0f86871c57dc6180d68ef72b9be42ee6573be53ca4621f38eccb32077a2869d7906f6ea7064eb2a03d1ac72a0ff0a40f8d2bdb741";
    const GET_ESCAPED_SIGNATURE: &str = "48f24d8ec39422d1cf01b7b1051bfb4745edfbe4b3214b6d62ba8ddb740fb7545792e63cc1b7dcc2224f9d13cb4c8d038b6e413771c4fed5c7486a7e903686819cc0912147bb42ebf5ae1d7aae8e08c9d3861e7705e55907fb0e779c8415f1c9545d83feee4a9391daa4b06af47ec5adf42a207da7cfd0c5d7df69d6a11f79c4854f8bb1e8db10e2334f894d1b62a40fe66b68a8b9a738ae9c458eaa7003da710377dd0865cb11cdf14dda05419ab4506f23c813d8ab572f306eee16c7a6de67f72ed6c45dd224cb4843d8caa07f768d2d471062782a4d352434dd553650acea94b73c19295c204cd5c3172b8e29db11bf626bd80148553ce774d0a82c7f83ac";

    fn signing_time() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(SIGNING_EPOCH_SECS)
    }

    fn presigner(key_prefix: Option<&str>) -> GcsV4Presigner {
        let (_key_dir, service_account_key_path) =
            gcs_fixture_service_account_key_file("gcs-presign");
        GcsV4Presigner::new(GcsPresignerConfig {
            bucket: "bucket".to_owned(),
            service_account_key_path: service_account_key_path.display().to_string(),
            key_prefix: key_prefix.map(ToOwned::to_owned),
        })
        .expect("signer")
    }

    async fn presign_put(signer: &GcsV4Presigner) -> crate::presign::PresignedUrl {
        signer
            .presign_put(
                PresignedPutRequest {
                    object_key: CONTENT_KEY,
                    expires_in: EXPIRES_IN,
                },
                signing_time(),
            )
            .await
            .expect("presign put")
    }

    #[tokio::test]
    async fn presigned_put_binds_the_scoped_path_and_create_only_precondition() {
        let signed = presign_put(&presigner(Some("tenant-a"))).await;

        assert_eq!(signed.method, "PUT");
        assert_eq!(
            signed
                .headers
                .get("x-goog-if-generation-match")
                .map(String::as_str),
            Some("0")
        );
        assert!(!signed.headers.contains_key("x-goog-hash"));
        assert_eq!(
            signed.url,
            format!(
                "https://storage.googleapis.com/bucket/tenant-a/{CONTENT_KEY}?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host%3Bx-goog-if-generation-match\
                 &X-Goog-Signature={PUT_PREFIXED_SIGNATURE}"
            )
        );
        assert_eq!(
            signed.expires_at_ms,
            SIGNING_EPOCH_SECS * 1_000 + EXPIRES_IN.as_millis() as u64
        );
    }

    #[tokio::test]
    async fn presigned_get_signs_only_the_host_so_range_stays_unsigned() {
        let signed = presigner(Some("tenant-a"))
            .presign_get(
                PresignedGetRequest {
                    object_key: CONTENT_KEY,
                    expires_in: EXPIRES_IN,
                },
                signing_time(),
            )
            .await
            .expect("presign get");

        assert_eq!(signed.method, "GET");
        assert!(
            signed.headers.is_empty(),
            "a read capability requires the client to send nothing"
        );
        assert!(!signed.url.to_ascii_lowercase().contains("range"));
        assert_eq!(
            signed.url,
            format!(
                "https://storage.googleapis.com/bucket/tenant-a/{CONTENT_KEY}?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host&X-Goog-Signature={GET_PREFIXED_SIGNATURE}"
            )
        );
    }

    #[test]
    fn presigned_head_reads_the_object_back_with_host_signed_alone() {
        let signed = presigner(Some("tenant-a"))
            .presign_head_stored_checksum(CONTENT_KEY, EXPIRES_IN, signing_time())
            .expect("presign head");

        assert_eq!(signed.method, "HEAD");
        assert!(signed.headers.is_empty());
        assert_eq!(
            signed.url,
            format!(
                "https://storage.googleapis.com/bucket/tenant-a/{CONTENT_KEY}?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host&X-Goog-Signature={HEAD_PREFIXED_SIGNATURE}"
            )
        );
    }

    #[tokio::test]
    async fn the_key_prefix_is_inside_the_signature() {
        let unprefixed = presign_put(&presigner(None)).await;

        assert_eq!(
            unprefixed.url,
            format!(
                "https://storage.googleapis.com/bucket/{CONTENT_KEY}?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host%3Bx-goog-if-generation-match\
                 &X-Goog-Signature={PUT_UNPREFIXED_SIGNATURE}"
            )
        );
        assert_ne!(
            PUT_UNPREFIXED_SIGNATURE, PUT_PREFIXED_SIGNATURE,
            "the same object key under two prefixes must not sign alike"
        );
    }

    #[tokio::test]
    async fn the_signer_and_the_store_resolve_a_key_to_the_same_string() {
        for raw_prefix in [None, Some("   "), Some(""), Some("tenant-a")] {
            let signed = presigner(raw_prefix)
                .presign_get(
                    PresignedGetRequest {
                        object_key: CONTENT_KEY,
                        expires_in: EXPIRES_IN,
                    },
                    signing_time(),
                )
                .await
                .expect("presign get");

            // Exactly what `ProviderObjectStore` does with the same value.
            let store_key = scope_object_key(
                normalize_key_prefix(raw_prefix)
                    .expect("store normalizes the prefix")
                    .as_deref(),
                CONTENT_KEY,
            )
            .expect("store scopes the key");

            let signed_path = signed.url.split('?').next().expect("url path");
            assert_eq!(
                signed_path,
                format!("https://storage.googleapis.com/bucket/{store_key}"),
                "signer and store disagree about the key under prefix {raw_prefix:?}"
            );
        }
    }

    #[test]
    fn an_unusable_key_prefix_fails_construction() {
        for raw_prefix in ["tenant-a//bad", "../escape"] {
            let (_key_dir, service_account_key_path) =
                gcs_fixture_service_account_key_file("gcs-presign-prefix");
            assert!(matches!(
                GcsV4Presigner::new(GcsPresignerConfig {
                    bucket: "bucket".to_owned(),
                    service_account_key_path: service_account_key_path.display().to_string(),
                    key_prefix: Some(raw_prefix.to_owned()),
                }),
                Err(ObjectStoreError::InvalidKey { .. })
            ));
        }
    }

    #[tokio::test]
    async fn path_segments_are_percent_encoded_and_separators_are_not() {
        let signed = presigner(Some("tenant-a"))
            .presign_get(
                PresignedGetRequest {
                    object_key:
                        "namespaces/a b/c+d/e~f/content/con_0123456789abcdef0123456789abcdef",
                    expires_in: EXPIRES_IN,
                },
                signing_time(),
            )
            .await
            .expect("presign get");

        assert_eq!(
            signed.url,
            format!(
                "https://storage.googleapis.com/bucket/tenant-a/namespaces\
                 /a%20b/c%2Bd/e~f/content/con_0123456789abcdef0123456789abcdef?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host&X-Goog-Signature={GET_ESCAPED_SIGNATURE}"
            )
        );
    }

    #[tokio::test]
    async fn presigned_get_addresses_the_same_object_the_write_did() {
        let signer = presigner(Some("tenant-a"));
        let written = presign_put(&signer).await;
        let read = signer
            .presign_get(
                PresignedGetRequest {
                    object_key: CONTENT_KEY,
                    expires_in: EXPIRES_IN,
                },
                signing_time(),
            )
            .await
            .expect("presign get");

        let object_of = |url: &str| url.split('?').next().expect("url path").to_owned();
        assert_eq!(object_of(&read.url), object_of(&written.url));
    }

    #[test]
    fn gcs_advertises_crc32c_and_googles_documented_single_request_ceiling() {
        let signer = presigner(None);
        assert_eq!(
            signer.stored_checksum_algorithm(),
            ChecksumAlgorithm::Crc32c
        );
        assert_eq!(signer.max_content_bytes(), GCP_GCS_MAX_DIRECT_PUT_BYTES);
        assert_eq!(GCP_GCS_MAX_DIRECT_PUT_BYTES, 5 * 1024 * 1024 * 1024 * 1024);
    }

    #[tokio::test]
    async fn expiry_outside_googles_documented_window_is_a_configuration_error() {
        let signer = presigner(None);
        for expires_in in [Duration::ZERO, Duration::from_secs(7 * 24 * 60 * 60 + 1)] {
            let result = signer
                .presign_get(
                    PresignedGetRequest {
                        object_key: CONTENT_KEY,
                        expires_in,
                    },
                    signing_time(),
                )
                .await;
            assert!(matches!(result, Err(ObjectStoreError::Configuration(_))));
        }
    }

    #[test]
    fn a_key_file_that_cannot_sign_fails_construction() {
        let (key_dir, _key_path) = gcs_fixture_service_account_key_file("gcs-presign-bad");

        let not_json = key_dir.path().join("not-json.json");
        std::fs::write(&not_json, b"this is not a service account").expect("write");
        let no_pem = key_dir.path().join("no-pem.json");
        std::fs::write(
            &no_pem,
            br#"{"client_email":"a@b.iam.gserviceaccount.com","private_key":"private_key"}"#,
        )
        .expect("write");
        let missing = key_dir.path().join("absent.json");

        for path in [not_json, no_pem, missing] {
            assert!(
                matches!(
                    GcsV4Presigner::new(GcsPresignerConfig {
                        bucket: "bucket".to_owned(),
                        service_account_key_path: path.display().to_string(),
                        key_prefix: None,
                    }),
                    Err(ObjectStoreError::Configuration(_))
                ),
                "{} should not have produced a signer",
                path.display()
            );
        }
    }

    #[test]
    fn presigner_debug_redacts_the_service_account_and_its_key() {
        let signer = presigner(Some("tenant-a"));
        let rendered = format!("{signer:?}");

        assert!(!rendered.contains(GCS_FIXTURE_CLIENT_EMAIL));
        assert!(!rendered.contains("BEGIN PRIVATE KEY"));
        assert!(!rendered.contains("MIIEv"));
        assert!(rendered.contains("<redacted>"));
        assert!(rendered.contains("bucket"));
    }

    #[test]
    fn stored_crc32c_selects_its_algorithm_out_of_the_hash_header() {
        assert_eq!(
            stored_crc32c("crc32c=mnG7TA==").map(|checksum| checksum.value),
            Some(HELLO_CRC32C_HEX.to_owned())
        );
        assert_eq!(
            stored_crc32c("md5=XUFAKrxLKna5cZ2REBfFkg==,crc32c=mnG7TA==")
                .map(|checksum| checksum.value),
            Some(HELLO_CRC32C_HEX.to_owned())
        );
        assert_eq!(
            stored_crc32c("crc32c=mnG7TA==, md5=XUFAKrxLKna5cZ2REBfFkg==")
                .map(|checksum| checksum.algorithm),
            Some(ChecksumAlgorithm::Crc32c)
        );

        // An object described without a usable crc32c is described without
        // one; the caller fails rather than completing on size alone.
        assert_eq!(stored_crc32c("md5=XUFAKrxLKna5cZ2REBfFkg=="), None);
        assert_eq!(stored_crc32c(""), None);
        assert_eq!(stored_crc32c("crc32c=not-base64!"), None);
        assert_eq!(stored_crc32c("crc32c=bW5HN1RBPT0="), None);
    }
}
