//! Presigner for Google Cloud Storage's native V4 signed URLs.
//!
//! This uses the native GCS XML API because the S3-compatible API did not
//! enforce write preconditions in live tests. The native API supports object
//! generation checks and stores CRC-32C checksums.
//!
use super::{
    DirectGetIssuer, DirectPutIssuer, PresignedGetRequest, PresignedPutRequest, PresignedUrl,
};
use crate::checksum_header::decode_base64_checksum;
use crate::keyspace::KeyScope;
use crate::object_store::Result;
use crate::presign::v4::{
    percent_encode_path, percent_encode_segment, presign_v4, signing_dates, V4Endpoint,
    V4RequestParts, V4Scheme,
};
use crate::provider_error::{map_provider_error, ProviderClient};
use crate::retry::TransportRetry;
use crate::signed_request::send;
use crate::ObjectStoreError;
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use loonfs_types::format::hex::hex_encode_bytes;
use loonfs_types::{Checksum, ChecksumAlgorithm};
use object_store::client::HttpClient;
use object_store::gcp::{GcpCredentialProvider, GcpSigningCredentialProvider};
use ring::rand::SystemRandom;
use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Host used for every GCS signed URL.
const GCS_HOST: &str = "storage.googleapis.com";

/// The IAM Credentials API resource under which `signBlob` signs as a service
/// account, for a caller with the Service Account Token Creator role on it.
const IAM_SERVICE_ACCOUNTS: &str =
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts";

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

/// Maximum GCS object size.
///
/// GCS does not document a smaller limit for a single-request upload.
pub const GCP_GCS_MAX_DIRECT_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;

/// Creates native GCS V4 signed URLs.
pub struct GcsV4Presigner {
    bucket: String,
    key_scope: KeyScope,
    signer: GcsSigner,
}

/// What produces the RSA signature: a service-account key in this process,
/// or the IAM Credentials API signing as the service account that ambient
/// credentials resolve to.
enum GcsSigner {
    ServiceAccountKey {
        /// The service account written into the credential parameter, which
        /// is also the identity GCS resolves the signing key from.
        client_email: String,
        signing_key: Box<RsaKeyPair>,
    },
    SignBlob {
        http: HttpClient,
        /// The OAuth token each `signBlob` request is sent under.
        credentials: GcpCredentialProvider,
        /// The service account to sign as, resolved from the metadata server
        /// or the application default credentials file.
        signing_credentials: GcpSigningCredentialProvider,
        retry: Arc<TransportRetry>,
    },
}

/// The `signBlob` request body: the bytes to sign, base64-encoded.
#[derive(serde::Serialize)]
struct SignBlobRequest {
    payload: String,
}

/// The field of a `signBlob` response this module reads.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignBlobResponse {
    signed_blob: String,
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
    /// reaches a log through a `Debug` of the store that contains it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let signer = match &self.signer {
            GcsSigner::ServiceAccountKey { .. } => "service_account_key",
            GcsSigner::SignBlob { .. } => "sign_blob",
        };
        f.debug_struct("GcsV4Presigner")
            .field("bucket", &self.bucket)
            .field("key_prefix", &self.key_scope.prefix())
            .field("signer", &signer)
            .finish()
    }
}

impl GcsV4Presigner {
    /// Creates a presigner that signs as the service account in `key_json`
    /// and scopes keys with `key_scope`, the scope of the store it signs for.
    ///
    /// The store and the presigner have to resolve an object key to the same
    /// string, or a signed write is stored where the store's own reads,
    /// listings, and collection never look.
    ///
    /// Construction fails for a malformed key, or a private key that is not
    /// an RSA key in PKCS#8 form.
    pub(crate) fn with_service_account_key(
        bucket: String,
        key_scope: KeyScope,
        key_json: &str,
    ) -> Result<Self> {
        // The parse error is not quoted: a malformed key file's contents are
        // key material as often as not.
        let key: ServiceAccountKey = serde_json::from_str(key_json).map_err(|_| {
            ObjectStoreError::Configuration(
                "service account key must be JSON carrying `client_email` and `private_key`"
                    .to_owned(),
            )
        })?;
        if key.client_email.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "service account key must include a `client_email`".to_owned(),
            ));
        }

        let der = pkcs8_der(&key.private_key)?;
        let signing_key = RsaKeyPair::from_pkcs8(&der).map_err(|_| {
            ObjectStoreError::Configuration(
                "service account private key must be an RSA key in PKCS#8 form".to_owned(),
            )
        })?;

        Ok(Self {
            bucket,
            key_scope,
            signer: GcsSigner::ServiceAccountKey {
                client_email: key.client_email,
                signing_key: Box::new(signing_key),
            },
        })
    }

    /// Creates a presigner that signs through the IAM Credentials API as the
    /// service account `signing_credentials` resolves to, under the OAuth
    /// token from `credentials`. Each signed URL costs one `signBlob` request,
    /// sent with the store's transport retry.
    pub(crate) fn with_sign_blob(
        bucket: String,
        key_scope: KeyScope,
        http: HttpClient,
        credentials: GcpCredentialProvider,
        signing_credentials: GcpSigningCredentialProvider,
        retry: Arc<TransportRetry>,
    ) -> Self {
        Self {
            bucket,
            key_scope,
            signer: GcsSigner::SignBlob {
                http,
                credentials,
                signing_credentials,
                retry,
            },
        }
    }

    async fn presign(
        &self,
        method: &str,
        object_key: &str,
        required_headers: BTreeMap<String, String>,
        ttl: Duration,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        let scoped_key = self.key_scope.object_key(object_key)?;
        let canonical_uri = format!(
            "/{}/{}",
            percent_encode_segment(&self.bucket),
            percent_encode_path(&scoped_key)
        );
        let dates = signing_dates(object_key, now)?;
        let credential_scope = format!("{}/{GCS_CREDENTIAL_SCOPE_SUFFIX}", dates.short_date);
        let client_email = self.signer.client_email(object_key).await?;
        presign_v4(
            V4Scheme {
                algorithm: GCS_SIGNING_ALGORITHM,
                query_prefix: "X-Goog",
                credential: format!("{client_email}/{credential_scope}"),
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
            ttl,
            now,
            async |message| self.signer.sign(object_key, &client_email, message).await,
        )
        .await
    }
}

impl GcsSigner {
    /// The service account whose signature GCS verifies, written into the
    /// credential parameter.
    async fn client_email(&self, object_key: &str) -> Result<String> {
        match self {
            Self::ServiceAccountKey { client_email, .. } => Ok(client_email.clone()),
            Self::SignBlob {
                signing_credentials,
                ..
            } => {
                let credential = signing_credentials
                    .get_credential()
                    .await
                    .map_err(|error| {
                        map_provider_error(object_key, error, ProviderClient::Retrying)
                    })?;
                Ok(credential.email.clone())
            }
        }
    }

    /// Produces the hex-encoded RSASSA-PKCS1-v1_5 SHA-256 signature GCS
    /// verifies against the service account's public key.
    async fn sign(&self, object_key: &str, client_email: &str, message: &[u8]) -> Result<String> {
        match self {
            Self::ServiceAccountKey { signing_key, .. } => {
                sign_with_key(signing_key, object_key, message)
            }
            Self::SignBlob {
                http,
                credentials,
                retry,
                ..
            } => {
                let url = format!("{IAM_SERVICE_ACCOUNTS}/{client_email}:signBlob");
                let body = Bytes::from(
                    serde_json::to_vec(&SignBlobRequest {
                        payload: BASE64.encode(message),
                    })
                    .expect("a signBlob request body should serialize"),
                );
                retry
                    .run(object_key, "sign_blob", 0, || async {
                        sign_blob(http, credentials, object_key, &url, body.clone()).await
                    })
                    .await
            }
        }
    }
}

fn sign_with_key(signing_key: &RsaKeyPair, object_key: &str, message: &[u8]) -> Result<String> {
    let mut signature = vec![0u8; signing_key.public().modulus_len()];
    signing_key
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
    Ok(hex_encode_bytes(&signature))
}

/// Sends one `signBlob` request under the credential source's OAuth token and
/// reads the signature back. A refusal is reported as a permission error,
/// which means the token's identity lacks the role or is a user account and
/// not a service account. A server error is retryable.
async fn sign_blob(
    http: &HttpClient,
    credentials: &GcpCredentialProvider,
    object_key: &str,
    url: &str,
    body: Bytes,
) -> Result<String> {
    let credential = credentials
        .get_credential()
        .await
        .map_err(|error| map_provider_error(object_key, error, ProviderClient::Retrying))?;
    let request = http::Request::post(url)
        .header(AUTHORIZATION, format!("Bearer {}", credential.bearer))
        .header(CONTENT_TYPE, "application/json");
    let response = send(http, object_key, request, body.into()).await?;
    if response.status.is_server_error() {
        return Err(ObjectStoreError::retryable_transport(
            object_key,
            format!("signBlob failed with {}", response.status),
        ));
    }
    if !response.status.is_success() {
        return Err(ObjectStoreError::PermissionDenied {
            object_key: object_key.to_owned(),
            message: format!("signBlob refused with {}", response.status),
        });
    }
    let signed: SignBlobResponse = serde_json::from_slice(&response.body)
        .map_err(|_| ObjectStoreError::transport(object_key, "signBlob returned no signature"))?;
    let signature = BASE64.decode(signed.signed_blob).map_err(|_| {
        ObjectStoreError::transport(
            object_key,
            "signBlob returned a signature that is not base64",
        )
    })?;
    Ok(hex_encode_bytes(&signature))
}

#[async_trait]
impl DirectPutIssuer for GcsV4Presigner {
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
            request.ttl,
            now,
        )
        .await
    }
}

#[async_trait]
impl DirectGetIssuer for GcsV4Presigner {
    async fn presign_get(
        &self,
        request: PresignedGetRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        self.presign(
            "GET",
            request.object_key,
            request.signed_headers(),
            request.ttl,
            now,
        )
        .await
    }
}

/// Reads the service-account JSON that a GCS configuration points to.
pub(crate) fn read_service_account_key(path: &str) -> Result<String> {
    std::fs::read_to_string(path).map_err(|err| {
        ObjectStoreError::Configuration(format!("service account key is unreadable: {err}"))
    })
}

/// Reads the stored CRC-32C out of a GCS `x-goog-hash` header value.
///
/// The header lists one or more `<algorithm>=<base64>` pairs in an
/// unspecified order and may include algorithms this format does not define, so
/// the CRC-32C is selected rather than positioned. An absent or unusable
/// crc32c returns `None`, and the caller treats that as a failure: an object
/// GCS will not describe is never completed on its size alone.
pub(crate) fn stored_crc32c(header_value: &str) -> Option<Checksum> {
    for pair in header_value.split(',') {
        let Some(encoded) = pair.trim().strip_prefix("crc32c=") else {
            continue;
        };
        if let Some(checksum) = decode_base64_checksum(ChecksumAlgorithm::Crc32c, encoded) {
            return Some(checksum);
        }
    }
    None
}

/// Decodes a PEM-wrapped PKCS#8 private key into DER.
///
/// Service-account JSON contains the key as a PEM block with escaped
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
    BASE64.decode(base64_body).map_err(|_| {
        ObjectStoreError::Configuration(
            "service account private key is not valid base64".to_owned(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{
        pkcs8_der, read_service_account_key, stored_crc32c, GcsV4Presigner, ServiceAccountKey,
        BASE64,
    };
    use crate::keyspace::KeyScope;
    use crate::presign::{
        DirectGetIssuer, DirectPutIssuer, PresignedGetRequest, PresignedPutRequest,
    };
    use crate::retry::TransportRetry;
    use crate::test_support::{GCS_FIXTURE_CLIENT_EMAIL, GCS_FIXTURE_SERVICE_ACCOUNT_KEY};
    use crate::{ByteRange, ObjectStoreError};
    use base64::Engine as _;
    use bytes::Bytes;
    use loonfs_types::{ChecksumAlgorithm, StdMonotonicTimer};
    use object_store::client::{
        HttpClient, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
    };
    use object_store::gcp::{GcpCredential, GcpSigningCredential};
    use object_store::StaticCredentialProvider;
    use ring::rand::SystemRandom;
    use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};
    use std::assert_matches;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const CONTENT_KEY: &str = "namespaces/demo/content/con_0123456789abcdef0123456789abcdef";
    /// The bytes a read grant names: a resume from byte 10 of a 39-byte
    /// reference.
    const GRANT: ByteRange = ByteRange {
        start_inclusive: 10,
        end_exclusive: 39,
    };
    /// 2023-11-14T22:13:20Z, the instant every expected signature below was
    /// produced at.
    const SIGNING_EPOCH_SECS: u64 = 1_700_000_000;
    const TTL: Duration = Duration::from_secs(900);
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
    const GET_PREFIXED_SIGNATURE: &str = "8ca0497a06d4bce1ae1c08c5d6162de145018ca165aee0f629722f78bb98ddc944819be439cf6bbde60c64abcf814137debe2b16c3819202573a444313f94e6f74c058f34922c698f6c849af142f15b04b24123e1b651970fa23e3ded67c670f5b9d26234cc8a6248cc0eebccbd4165cb10bc7504e0826c85a31a97b08a50b4b74f72847dda604e7e75438dfcc19fe99bcb0881b805c37157fcfdf630227e01c44251231761d5c194c0173254e3f2e99ec1f6b72f1ea94936b017034ef4aca923804c4b479014c1a8d0aee8f21218c4a8ee1d67ba0e97ebb4117200921c0b7d11effd8bf81d37476764ed8b4318788a1baab657e72fa6e266a148797d423975c";
    const PUT_UNPREFIXED_SIGNATURE: &str = "201e2951edf8d3d2da08d8dcb9bea6318dda0a2927480717202ce6294c694aeb98fb8f128f73470b790decc1e38e5c5fdb454e3c5fb438c10bbdc3f1e0a2b5f73cb8f160477ae507ea7ccf6cee93d00420b936558c94673d932c8854b5864466035c6fa5e8a4c17c9e6be5908f1659c13a1720b24da96537fe641816074391d752d8d0feb8307faa95003601d05918ae9602e23b40abf4afb734065ed528404981ff9d0f5d8e46cac190dbe69b63c96d302d4eecf8c01e041015817a8287621e8e32ac3b2060f84e0827ccf0f86871c57dc6180d68ef72b9be42ee6573be53ca4621f38eccb32077a2869d7906f6ea7064eb2a03d1ac72a0ff0a40f8d2bdb741";
    const GET_ESCAPED_SIGNATURE: &str = "69f413dc31744f07063ec59fba35a7eb2e7853f65173ee5fe889d34994c046cc51f8d4517db671e90a64720e5aeb2d2583195043bcb5ef8b9c60b3f32af19e2fc3bcbb5cfbc0ad808236920b106987237a85bc45dbc22ddd8c2592330b4869b3885380376345fcd279252945a95139466e50b6d650dcff4e364c7705ea73514a73a045ad4245cb1a5f2debc5e04832a87c214b8ba302e8500f53ded590b24e92ad0d790f00afc0d1530b9605021532cf6eab6da0b79bd675843fe10e816fa5152922983741797f7bbaa722b023d14986f0f922b8c9165d38ece5d343b33d306a9cb6615f10e16dc277b269ce2dc0f8e80c3a4237b00f0dc51347e35f08761e01";

    fn signing_time() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(SIGNING_EPOCH_SECS)
    }

    fn presigner(key_prefix: Option<&str>) -> GcsV4Presigner {
        GcsV4Presigner::with_service_account_key(
            "bucket".to_owned(),
            KeyScope::new(key_prefix).expect("prefix"),
            GCS_FIXTURE_SERVICE_ACCOUNT_KEY,
        )
        .expect("signer")
    }

    /// The IAM Credentials API as a double: records each `signBlob` request,
    /// and signs its payload with the fixture key or refuses with `status`.
    #[derive(Clone, Debug)]
    struct SignBlobService {
        requests: Arc<Mutex<Vec<(String, http::HeaderMap, Bytes)>>>,
        status: http::StatusCode,
    }

    #[async_trait::async_trait]
    impl HttpService for SignBlobService {
        async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
            use http_body_util::BodyExt as _;
            let (parts, body) = request.into_parts();
            let bytes = body.collect().await?.to_bytes();
            self.requests.lock().expect("requests").push((
                parts.uri.to_string(),
                parts.headers,
                bytes.clone(),
            ));
            let body = if self.status.is_success() {
                let request: serde_json::Value =
                    serde_json::from_slice(&bytes).expect("a JSON body");
                let message = BASE64
                    .decode(request["payload"].as_str().expect("a payload"))
                    .expect("a base64 payload");
                Bytes::from(format!(
                    r#"{{"keyId":"fixture","signedBlob":"{}"}}"#,
                    BASE64.encode(fixture_signature(&message))
                ))
            } else {
                Bytes::new()
            };
            Ok(http::Response::builder()
                .status(self.status)
                .body(body)
                .expect("response")
                .map(HttpResponseBody::from))
        }
    }

    /// Signs `message` with the fixture key, as signBlob signs with the
    /// service account's own key.
    fn fixture_signature(message: &[u8]) -> Vec<u8> {
        let key: ServiceAccountKey =
            serde_json::from_str(GCS_FIXTURE_SERVICE_ACCOUNT_KEY).expect("fixture key");
        let key =
            RsaKeyPair::from_pkcs8(&pkcs8_der(&key.private_key).expect("pkcs8")).expect("rsa key");
        let mut signature = vec![0u8; key.public().modulus_len()];
        key.sign(
            &RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            message,
            &mut signature,
        )
        .expect("sign");
        signature
    }

    fn sign_blob_presigner(service: SignBlobService) -> GcsV4Presigner {
        GcsV4Presigner::with_sign_blob(
            "bucket".to_owned(),
            KeyScope::new(Some("tenant-a")).expect("prefix"),
            HttpClient::new(service),
            Arc::new(StaticCredentialProvider::new(GcpCredential {
                bearer: "test".to_owned(),
            })),
            Arc::new(StaticCredentialProvider::new(GcpSigningCredential {
                email: GCS_FIXTURE_CLIENT_EMAIL.to_owned(),
                private_key: None,
            })),
            Arc::new(TransportRetry::new(Arc::new(StdMonotonicTimer::default()))),
        )
    }

    async fn presign_put(signer: &GcsV4Presigner) -> crate::presign::PresignedUrl {
        signer
            .presign_put(
                PresignedPutRequest {
                    object_key: CONTENT_KEY,
                    ttl: TTL,
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
            (SIGNING_EPOCH_SECS + TTL.as_secs()) * 1_000
        );
    }

    #[tokio::test]
    async fn presigned_get_signs_the_range_it_grants_and_returns_it_as_a_header() {
        let signed = presigner(Some("tenant-a"))
            .presign_get(
                PresignedGetRequest {
                    object_key: CONTENT_KEY,
                    range: GRANT,
                    ttl: TTL,
                },
                signing_time(),
            )
            .await
            .expect("presign get");

        assert_eq!(signed.method, "GET");
        assert_eq!(
            signed.headers,
            std::collections::BTreeMap::from([("range".to_owned(), "bytes=10-38".to_owned())])
        );
        assert_eq!(
            signed.url,
            format!(
                "https://storage.googleapis.com/bucket/tenant-a/{CONTENT_KEY}?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host%3Brange&X-Goog-Signature={GET_PREFIXED_SIGNATURE}"
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
    }

    #[tokio::test]
    async fn a_sign_blob_signer_sends_the_string_to_sign_under_the_token_and_signs_like_the_key() {
        let service = SignBlobService {
            requests: Arc::default(),
            status: http::StatusCode::OK,
        };
        let signer = sign_blob_presigner(service.clone());

        let put = presign_put(&signer).await;
        let get = signer
            .presign_get(
                PresignedGetRequest {
                    object_key: CONTENT_KEY,
                    range: GRANT,
                    ttl: TTL,
                },
                signing_time(),
            )
            .await
            .expect("presign get");

        // The URLs the key file produces, so the bytes sent to signBlob are the
        // string the key signs.
        assert_eq!(
            put.url,
            format!(
                "https://storage.googleapis.com/bucket/tenant-a/{CONTENT_KEY}?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host%3Bx-goog-if-generation-match\
                 &X-Goog-Signature={PUT_PREFIXED_SIGNATURE}"
            )
        );
        assert_eq!(
            get.url,
            format!(
                "https://storage.googleapis.com/bucket/tenant-a/{CONTENT_KEY}?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host%3Brange&X-Goog-Signature={GET_PREFIXED_SIGNATURE}"
            )
        );

        let requests = service.requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        let (url, headers, body) = &requests[0];
        assert_eq!(
            url,
            &format!(
                "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts\
                 /{GCS_FIXTURE_CLIENT_EMAIL}:signBlob"
            )
        );
        assert_eq!(headers["authorization"], "Bearer test");
        assert_eq!(headers["content-type"], "application/json");
        let request: serde_json::Value = serde_json::from_slice(body).expect("a JSON body");
        let message = BASE64
            .decode(request["payload"].as_str().expect("a payload"))
            .expect("a base64 payload");
        assert!(String::from_utf8(message).expect("utf-8").starts_with(
            "GOOG4-RSA-SHA256\n20231114T221320Z\n20231114/auto/storage/goog4_request\n"
        ));
    }

    #[tokio::test]
    async fn a_refused_sign_blob_fails_the_presign_as_permission_denied() {
        let service = SignBlobService {
            requests: Arc::default(),
            status: http::StatusCode::FORBIDDEN,
        };
        let error = sign_blob_presigner(service)
            .presign_put(
                PresignedPutRequest {
                    object_key: CONTENT_KEY,
                    ttl: TTL,
                },
                signing_time(),
            )
            .await
            .expect_err("signBlob refused");
        assert_matches!(error, ObjectStoreError::PermissionDenied { .. });
    }

    #[tokio::test]
    async fn the_signer_and_the_store_resolve_a_key_to_the_same_string() {
        for raw_prefix in [None, Some("   "), Some(""), Some("tenant-a")] {
            let signed = presigner(raw_prefix)
                .presign_get(
                    PresignedGetRequest {
                        object_key: CONTENT_KEY,
                        range: GRANT,
                        ttl: TTL,
                    },
                    signing_time(),
                )
                .await
                .expect("presign get");

            // Exactly what `ProviderObjectStore` does with the same value.
            let store_key = KeyScope::new(raw_prefix)
                .expect("store normalizes the prefix")
                .object_key(CONTENT_KEY)
                .expect("store scopes the key");

            let signed_path = signed.url.split('?').next().expect("url path");
            assert_eq!(
                signed_path,
                format!("https://storage.googleapis.com/bucket/{store_key}"),
                "signer and store disagree about the key under prefix {raw_prefix:?}"
            );
        }
    }

    #[tokio::test]
    async fn path_segments_are_percent_encoded_and_separators_are_not() {
        let signed = presigner(Some("tenant-a"))
            .presign_get(
                PresignedGetRequest {
                    object_key:
                        "namespaces/a b/c+d/e~f/content/con_0123456789abcdef0123456789abcdef",
                    range: GRANT,
                    ttl: TTL,
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
                 &X-Goog-SignedHeaders=host%3Brange&X-Goog-Signature={GET_ESCAPED_SIGNATURE}"
            )
        );
    }

    #[tokio::test]
    async fn expiry_outside_googles_documented_window_is_a_configuration_error() {
        let signer = presigner(None);
        for ttl in [Duration::ZERO, Duration::from_secs(7 * 24 * 60 * 60 + 1)] {
            let result = signer
                .presign_get(
                    PresignedGetRequest {
                        object_key: CONTENT_KEY,
                        range: GRANT,
                        ttl,
                    },
                    signing_time(),
                )
                .await;
            assert_matches!(result, Err(ObjectStoreError::Configuration(_)));
        }
    }

    #[test]
    fn a_key_that_cannot_sign_fails_construction() {
        for key_json in [
            "this is not a service account",
            r#"{"client_email":"a@b.iam.gserviceaccount.com","private_key":"private_key"}"#,
        ] {
            assert_matches!(
                GcsV4Presigner::with_service_account_key(
                    "bucket".to_owned(),
                    KeyScope::default(),
                    key_json
                ),
                Err(ObjectStoreError::Configuration(_)),
                "{key_json} should not have produced a signer"
            );
        }
        assert_matches!(
            read_service_account_key("/nonexistent/absent.json"),
            Err(ObjectStoreError::Configuration(_))
        );
    }

    #[test]
    fn presigner_debug_shows_the_signer_kind_and_no_service_account_or_key() {
        let signer = presigner(Some("tenant-a"));
        let rendered = format!("{signer:?}");

        assert!(!rendered.contains(GCS_FIXTURE_CLIENT_EMAIL));
        assert!(!rendered.contains("BEGIN PRIVATE KEY"));
        assert!(!rendered.contains("MIIEv"));
        assert!(rendered.contains("service_account_key"));
        assert!(rendered.contains("bucket"));
    }

    #[test]
    fn stored_crc32c_selects_its_algorithm_out_of_the_hash_header() {
        assert_eq!(
            stored_crc32c("crc32c=mnG7TA==").map(|checksum| checksum.value().to_string()),
            Some(HELLO_CRC32C_HEX.to_owned())
        );
        assert_eq!(
            stored_crc32c("md5=XUFAKrxLKna5cZ2REBfFkg==,crc32c=mnG7TA==")
                .map(|checksum| checksum.value().to_string()),
            Some(HELLO_CRC32C_HEX.to_owned())
        );
        assert_eq!(
            stored_crc32c("crc32c=mnG7TA==, md5=XUFAKrxLKna5cZ2REBfFkg==")
                .map(|checksum| checksum.algorithm()),
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
