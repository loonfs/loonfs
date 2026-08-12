//! Presigner for Google Cloud Storage's native V4 signed URLs.
//!
//! GCS's S3-interoperability surface is not an option here: live conformance
//! proved it silently ignores preconditions (see [`GcpGcsStore`]). Its native
//! XML API conditions on object generations and validates CRC-32C, and this
//! module signs that API directly with `GOOG4-RSA-SHA256`.
//!
//! [`GcpGcsStore`]: crate::gcs::GcpGcsStore

use super::{
    DirectGetIssuer, DirectPutIssuer, PresignedGetRequest, PresignedPutRequest, PresignedUrl,
};
use crate::keyspace::{normalize_key_prefix, scope_object_key};
use crate::object_store::Result;
use crate::presign::v4::{
    canonical_query_string, hex_lower, normalize_header_value, percent_encode_path,
    percent_encode_segment, signing_dates, unix_ms,
};
use crate::ObjectStoreError;
use base64::Engine as _;
use loonfs_api::wire::hex::{hex_decode_bytes, hex_encode_bytes};
use loonfs_api::{ChecksumAlgorithm, ContentRef, ContentRefKind, StorageChecksum};
use ring::rand::SystemRandom;
use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write as _;
use std::time::{Duration, SystemTime};

/// Google's own signed-URL host, and the only one this module addresses.
///
/// The GCS store configuration carries no endpoint override, so the endpoint
/// trust rule that gates the S3-compatible providers has nothing to decide
/// here: every capability this module issues is `https` to Google.
const GCS_HOST: &str = "storage.googleapis.com";

/// Create-only precondition. A generation of zero means "only if the object
/// does not currently exist", which is GCS's spelling of the guarantee the
/// S3 family gets from `if-none-match: *`.
const GCS_GENERATION_MATCH_HEADER: &str = "x-goog-if-generation-match";
const GCS_CREATE_ONLY_GENERATION: &str = "0";

/// Carries a checksum GCS validates the uploaded body against on a write, and
/// reports the stored one back on a read.
const GCS_HASH_HEADER: &str = "x-goog-hash";

/// The signing scheme, written into both the algorithm query parameter and
/// the first line of the string to sign.
const GCS_SIGNING_ALGORITHM: &str = "GOOG4-RSA-SHA256";

/// The credential scope's location and terminator. GCS accepts `auto` for the
/// location rather than requiring the bucket's region, which is what Google's
/// own client libraries write, so a deployment never has to configure one.
const GCS_CREDENTIAL_SCOPE_SUFFIX: &str = "auto/storage/goog4_request";

/// Google's documented ceiling for a signed URL's lifetime: seven days.
const MAX_PRESIGN_EXPIRY: u64 = 7 * 24 * 60 * 60;

/// Google Cloud Storage's documented maximum for a single-request upload:
/// 5 TiB.
///
/// Cloud Storage documents one object-size maximum and no separate
/// single-request one -- Cloud Storage, "Object uploads": you can upload and
/// store any MIME type of data up to 5 TiB in size, and a single-request
/// upload is described there as a PUT whose body is the whole object. The
/// object ceiling is therefore the request ceiling.
///
/// This is three orders of magnitude above the S3 family's 5 GiB single-PUT
/// ceiling, which is what lets this adapter carry large objects without
/// signing multipart at all: there is no size at which the whole-object
/// write stops being expressible. Google does document an XML API multipart
/// upload, and documents it as S3-compatible; this adapter does not
/// implement it, because it belongs to the same interoperability surface
/// whose precondition handling conformance found unsound, and because the
/// large-object path GCS is headed for is the native resumable upload.
pub const GCP_GCS_MAX_DIRECT_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;

/// Supplies the service-account key and key scoping for native GCS signed URLs.
///
/// The key path is the one the GCS store already loads to authenticate its
/// provider client; signing reads the same file rather than asking an
/// operator for a second credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsPresignerConfig {
    /// Bucket incorporated into the signed request target.
    pub bucket: String,
    /// Filesystem path to the service-account JSON whose private key signs.
    pub service_account_key_path: String,
    /// Logical prefix prepended before the object key is encoded and signed.
    pub key_prefix: Option<String>,
}

/// Issues checksum-bound, create-only `GOOG4-RSA-SHA256` capabilities for
/// Google Cloud Storage.
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
    /// Construction fails for a blank bucket or key path, an unusable key
    /// prefix, an unreadable or malformed key file, or a private key that is
    /// not an RSA key in PKCS#8 form. Failing here is deliberate: a GCS
    /// deployment whose key cannot sign also cannot authenticate its
    /// provider client, so the fault belongs at startup rather than at the
    /// first transfer.
    pub fn new(config: GcsPresignerConfig) -> Result<Self> {
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
        // Expiry comes from deployment configuration, not the transport: a
        // bad value is a config bug and must not look like network weather.
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

        let scoped_key = scope_object_key(self.key_prefix.as_deref(), object_key)?;
        let canonical_uri = format!(
            "/{}/{}",
            percent_encode_segment(&self.bucket),
            percent_encode_path(&scoped_key)
        );
        let dates = signing_dates(object_key, now)?;
        let credential_scope = format!("{}/{GCS_CREDENTIAL_SCOPE_SUFFIX}", dates.short_date);

        let mut headers_to_sign = BTreeMap::from([("host".to_owned(), GCS_HOST.to_owned())]);
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

        let query = BTreeMap::from([
            (
                "X-Goog-Algorithm".to_owned(),
                GCS_SIGNING_ALGORITHM.to_owned(),
            ),
            (
                "X-Goog-Credential".to_owned(),
                format!("{}/{credential_scope}", self.client_email),
            ),
            ("X-Goog-Date".to_owned(), dates.timestamp.clone()),
            (
                "X-Goog-Expires".to_owned(),
                expires_in.as_secs().to_string(),
            ),
            ("X-Goog-SignedHeaders".to_owned(), signed_headers.clone()),
        ]);
        let canonical_query = canonical_query_string(&query);
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD"
        );
        let string_to_sign = format!(
            "{GCS_SIGNING_ALGORITHM}\n{}\n{credential_scope}\n{}",
            dates.timestamp,
            hex_lower(&Sha256::digest(canonical_request.as_bytes()))
        );
        let signature = self.sign(object_key, string_to_sign.as_bytes())?;
        let url = format!(
            "https://{GCS_HOST}{canonical_uri}?{canonical_query}&X-Goog-Signature={signature}"
        );
        let expires_at_ms = unix_ms(object_key, now)? + expires_in.as_millis() as u64;

        Ok(PresignedUrl {
            method: method.to_owned(),
            url,
            headers: required_headers,
            expires_at_ms,
        })
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

impl DirectPutIssuer for GcsV4Presigner {
    fn checksum_algorithm(&self) -> ChecksumAlgorithm {
        // The digest the signed `x-goog-hash` header binds a body to. GCS
        // validates CRC-32C and MD5 and nothing else, and MD5 is not a
        // checksum this format names.
        ChecksumAlgorithm::Crc32c
    }

    fn max_content_bytes(&self) -> u64 {
        GCP_GCS_MAX_DIRECT_PUT_BYTES
    }

    fn presign_put(
        &self,
        request: PresignedPutRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl> {
        let required_headers = BTreeMap::from([
            (
                GCS_GENERATION_MATCH_HEADER.to_owned(),
                GCS_CREATE_ONLY_GENERATION.to_owned(),
            ),
            (
                GCS_HASH_HEADER.to_owned(),
                gcs_hash_header(request.content_ref)?,
            ),
        ]);
        self.presign(
            "PUT",
            request.object_key,
            required_headers,
            request.expires_in,
            now,
        )
    }
}

impl DirectGetIssuer for GcsV4Presigner {
    fn presign_get(
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

/// Builds the complete `x-goog-hash` value the write is signed against.
///
/// The whole header rides the signature, not just the digest inside it, so a
/// client can neither drop the checksum nor swap the algorithm it names.
fn gcs_hash_header(content_ref: &ContentRef) -> Result<String> {
    if content_ref.kind != ContentRefKind::BlobV1 {
        return Err(invalid_direct_put_content(
            "direct_put only supports blob_v1 content refs",
        ));
    }
    if content_ref.storage_checksum.algorithm != ChecksumAlgorithm::Crc32c {
        return Err(invalid_direct_put_content(
            "direct_put on GCS requires a crc32c storage checksum",
        ));
    }
    Ok(format!(
        "crc32c={}",
        base64_crc32c(&content_ref.storage_checksum)?
    ))
}

/// Converts a CRC-32C from the lowercase hex this format stores into the
/// big-endian base64 GCS reads and writes.
///
/// The two spellings meet here and nowhere else: every layer above holds the
/// hex form, and the provider's is confined to this adapter.
fn base64_crc32c(checksum: &StorageChecksum) -> Result<String> {
    let raw = hex_decode_bytes(&checksum.value)
        .map_err(|_| invalid_direct_put_content("crc32c must be lowercase hex"))?;
    if raw.len() != ChecksumAlgorithm::Crc32c.value_bytes() {
        return Err(invalid_direct_put_content(
            "crc32c must be 8 hex characters",
        ));
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(raw))
}

/// Reads the stored CRC-32C out of a GCS `x-goog-hash` header value.
///
/// The header lists one or more `<algorithm>=<base64>` pairs in an
/// unspecified order and may carry algorithms this format does not name, so
/// the CRC-32C is selected rather than positioned. An absent or unusable
/// crc32c answers `None`, and the caller treats that as a failure: an object
/// GCS will not describe is never completed on its size alone.
pub(crate) fn stored_crc32c(header_value: &str) -> Option<StorageChecksum> {
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
        return Some(StorageChecksum {
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

fn invalid_direct_put_content(message: &str) -> ObjectStoreError {
    ObjectStoreError::InvalidContentRef(message.to_owned())
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
    use loonfs_api::{ChecksumAlgorithm, ContentId, ContentRef, ContentRefKind, StorageChecksum};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const CONTENT_KEY: &str =
        "content-stores/cs/objects/01/23/con_0123456789abcdef0123456789abcdef";
    /// 2023-11-14T22:13:20Z, the instant every expected signature below was
    /// produced at.
    const SIGNING_EPOCH_SECS: u64 = 1_700_000_000;
    const EXPIRES_IN: Duration = Duration::from_secs(900);
    /// CRC-32C of `b"hello"`, in the lowercase hex this format stores and in
    /// the big-endian base64 GCS reads.
    const HELLO_CRC32C_HEX: &str = "9a71bb4c";
    const HELLO_CRC32C_BASE64: &str = "mnG7TA==";

    /// The query prefix every expected URL shares: the algorithm, the
    /// credential scope, and the request timestamp.
    const EXPECTED_CREDENTIAL: &str = "X-Goog-Algorithm=GOOG4-RSA-SHA256\
         &X-Goog-Credential=loonfs-presign-fixture%40loonfs-tests.iam.gserviceaccount.com\
         %2F20231114%2Fauto%2Fstorage%2Fgoog4_request\
         &X-Goog-Date=20231114T221320Z&X-Goog-Expires=900";

    // The expected signatures below were produced by Google's own
    // `google-cloud-storage` Python client (`generate_signed_url_v4`) against
    // this fixture key, at a pinned timestamp. They are therefore an
    // independent implementation's answer, not this module's own output
    // recorded back: a construction bug here -- a wrong credential scope, a
    // dropped signed header, a mis-encoded path -- changes the signature and
    // fails these tests.
    const PUT_PREFIXED_SIGNATURE: &str = "966829ea3141ec468614f5472381de24f864cd92be0f69d929d53e163dec9d2146f87acfa957f9b9e7e2e598f7ad9b1aaff7a1f456bea1451b487925dbcda72415d38cb40541e964ccd719184d7400b40c268b50ce92ac0c183ade437e0ba4009cff02f683c325f95cccb83ee8836683ce6b7dc473f02e54b928fe63c9b723da8835e414a8d1d14aa235432b1827056f49abf2fde1c927920fd90026b0dc344b0f68198e86acfafa4744708cec4f9899540b7900bfb9e0fb188bedcf8a85f43a7b88d067c2d48cc7f95f8744e84397cc23ec46433fb5e8e9a0bc249f248b92b7a1c6b7797bbccbbd9a4fcb9ee978bef3dd15d215aabf2f8ddc95818a64396f8e";
    const GET_PREFIXED_SIGNATURE: &str = "0a3a4a24c04a97eafee5fc2038c2a5d774f8246d8a69a1cca83a9c0c3585cd4a0516f2727d4c270112b1a8fd9f7d9c274ba42a27d81898266752233c877a61b7c1e4d47a9126033347382242a8b10e29656e28188492bede3f5108da056d77e572193633e7d28a075282c0b99f96437d13f674532b9a078114130b45789d427d8f5d108efacf07c27ebcbe1a460af18470c8f8289929c4fb60049b6ff0c7ed7cfa95f2b4063980faf342a75a2cead80d4e21d9cd9ee152779c0b549ad16d650a211c938e1febbbbb77d943e77344eb9ed89e0a3971d2ef89075971dae7a3be163fcec135f2036f3d31b15121773fb1f9f2307d0a6b5b4bbdf18d236d7742e89a";
    const HEAD_PREFIXED_SIGNATURE: &str = "19357d0b1acda281239c83d61e360fad813d2ae5323cc1803d6669f6665062cf6dd639382d1ba37e9ae46f53c166908da32314a1b68c3af9adae8e2bd405af49f5b7e9ea6c096a32a7def051f925bcde9f972470e50398eb4787ec7fef560926c3bf5a1baf7efbf57201a0d2cfb593654680e3122f807e4e7452adc04e142fb8ccd9aa126a28400acca5dbb2f08de129edd1a4608fb116f9ae11efbfed83f762d4d1b9765a8c8c4ebbbe3de3bc8ffffcc370aaaba40ee6f01e6bc59bd053a41e1714f1bbb3ae061847ff6b4c3ece7532cf3b6568024c3705cd568876e3a1c2547958e188146be944915657923614257cb857db1691718efaf16d2b88a0afebdd";
    const PUT_UNPREFIXED_SIGNATURE: &str = "0f43f56a0e2bd7e41d91240d2d401dd783cd882af218f263fd7083b4b2288e5bbeba3bcb587df6271d91d7e724efa851ee64de8b94d45252c7c2321a54d2c022f8eb493d2b62b8677c631b8507cb8f91fde5f0d51be1773dce672a3102308aa9268cab57ca8862f07ff7aac44b640286f3c839c3ee850b9a8b790577bd0c489d8992a36f4426b04f643bda515b7fedd900ef78f4d60980cf0ac5fea12fa56921beb356c3b153b0f8a437c0444ff3d463dee360e914407302bc813477bd9fc6cecfb340bab4f66e054f1a9d53e0266607b8a0a2b93c4e918587e6c3b88c8a6b554273ace7e82f0d14fac90a748496d2ab8b1ee62c33c9332bb1ffaf7042efbf3c";
    const GET_ESCAPED_SIGNATURE: &str = "32a9b708f6681f2725b500fd65776c471170a8c52bf912473f69c029264303d07c8e33619300384a175c5390ba89b12aeaa127dac514a19d0d6c53a4d39794b65dd85c0842662be728b9437454767352969a6f588c70525fb5306fc5463663ab364bdcd1a85469e9a7bb8fa5d87073b97f028809836cbac7ebd056104d847cd4c59bf8ea27b7116b8b3116ad93b3ff8b2ef73e0c5b5afc8a19312c5a042b2659fbe991f5ee0b4c7d36af73ed428266c8be2c6766370332afadbfc342a5c4dde67805fa2517cd2ae1eab4e77579fb4e8df3f53b38788de706e28253d55ed47c873c4556691f377c84a613de67ac8f977f743bfdaa7ff61e3ae4227254b9e7d536";

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

    /// A reference whose storage checksum is the CRC-32C GCS enforces, which
    /// is what a `direct_put` claim carries on this provider.
    fn crc32c_content_ref() -> ContentRef {
        ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::parse("con_0123456789abcdef0123456789abcdef")
                .expect("valid content id"),
            size_bytes: 5,
            storage_checksum: StorageChecksum {
                algorithm: ChecksumAlgorithm::Crc32c,
                value: HELLO_CRC32C_HEX.to_owned(),
            },
            whole_file_sha256: None,
        }
    }

    fn presign_put(signer: &GcsV4Presigner) -> crate::presign::PresignedUrl {
        signer
            .presign_put(
                PresignedPutRequest {
                    object_key: CONTENT_KEY,
                    content_ref: &crc32c_content_ref(),
                    expires_in: EXPIRES_IN,
                },
                signing_time(),
            )
            .expect("presign put")
    }

    /// The whole write contract in one signature: the scoped object path, the
    /// create-only precondition, the complete `x-goog-hash` value, and the
    /// signed-header set that binds both headers to it.
    #[test]
    fn presigned_put_binds_the_scoped_path_checksum_and_create_only_precondition() {
        let signed = presign_put(&presigner(Some("tenant-a")));

        assert_eq!(signed.method, "PUT");
        assert_eq!(
            signed
                .headers
                .get("x-goog-if-generation-match")
                .map(String::as_str),
            Some("0")
        );
        assert_eq!(
            signed.headers.get("x-goog-hash").map(String::as_str),
            Some(format!("crc32c={HELLO_CRC32C_BASE64}").as_str())
        );
        assert_eq!(
            signed.url,
            format!(
                "https://storage.googleapis.com/bucket/tenant-a/{CONTENT_KEY}?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host%3Bx-goog-hash%3Bx-goog-if-generation-match\
                 &X-Goog-Signature={PUT_PREFIXED_SIGNATURE}"
            )
        );
        assert_eq!(
            signed.expires_at_ms,
            SIGNING_EPOCH_SECS * 1_000 + EXPIRES_IN.as_millis() as u64
        );
    }

    /// The read capability signs `host` and nothing else, which is what
    /// leaves `Range` outside the signature: one issued URL serves ranged,
    /// resumed, and parallel reads without another round trip to the server.
    #[test]
    fn presigned_get_signs_only_the_host_so_range_stays_unsigned() {
        let signed = presigner(Some("tenant-a"))
            .presign_get(
                PresignedGetRequest {
                    object_key: CONTENT_KEY,
                    expires_in: EXPIRES_IN,
                },
                signing_time(),
            )
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

    /// The checksum readback needs no request header, so it signs the same
    /// header set a read does and differs only in method.
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

    /// The key prefix is part of what is signed, not decoration on the URL:
    /// dropping it produces a different signature, so a capability issued for
    /// one tenant's prefix cannot be replayed against another's.
    #[test]
    fn the_key_prefix_is_inside_the_signature() {
        let unprefixed = presign_put(&presigner(None));

        assert_eq!(
            unprefixed.url,
            format!(
                "https://storage.googleapis.com/bucket/{CONTENT_KEY}?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host%3Bx-goog-hash%3Bx-goog-if-generation-match\
                 &X-Goog-Signature={PUT_UNPREFIXED_SIGNATURE}"
            )
        );
        assert_ne!(
            PUT_UNPREFIXED_SIGNATURE, PUT_PREFIXED_SIGNATURE,
            "the same object key under two prefixes must not sign alike"
        );
    }

    /// The signer and the provider client resolve an object key to the same
    /// string, whatever spelling of a prefix the deployment configured.
    ///
    /// A whitespace-only prefix is the case that used to split them: the
    /// store normalizes it away and works in the unprefixed keyspace, while
    /// the signer kept it raw and addressed `%20%20%20/...`. An object
    /// written through such a capability is committed and then invisible to
    /// every read, listing, and collection the store performs — durable
    /// nowhere anything looks.
    #[test]
    fn the_signer_and_the_store_resolve_a_key_to_the_same_string() {
        for raw_prefix in [None, Some("   "), Some(""), Some("tenant-a")] {
            let signed = presigner(raw_prefix)
                .presign_get(
                    PresignedGetRequest {
                        object_key: CONTENT_KEY,
                        expires_in: EXPIRES_IN,
                    },
                    signing_time(),
                )
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

    /// A prefix the store would refuse is refused here too, at construction,
    /// rather than producing capabilities the store cannot address.
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

    /// Path segments are percent-encoded, and the separators between them are
    /// not. Getting either wrong signs a different object than the one the
    /// URL addresses.
    #[test]
    fn path_segments_are_percent_encoded_and_separators_are_not() {
        let signed = presigner(Some("tenant-a"))
            .presign_get(
                PresignedGetRequest {
                    object_key: "content-stores/cs/objects/a b/c+d/e~f/con_0123456789abcdef0123456789abcdef",
                    expires_in: EXPIRES_IN,
                },
                signing_time(),
            )
            .expect("presign get");

        assert_eq!(
            signed.url,
            format!(
                "https://storage.googleapis.com/bucket/tenant-a/content-stores/cs/objects\
                 /a%20b/c%2Bd/e~f/con_0123456789abcdef0123456789abcdef?{EXPECTED_CREDENTIAL}\
                 &X-Goog-SignedHeaders=host&X-Goog-Signature={GET_ESCAPED_SIGNATURE}"
            )
        );
    }

    /// Reads and writes of the same object address the same URL, so a
    /// deployment cannot sign a write it is unable to sign a read of.
    #[test]
    fn presigned_get_addresses_the_same_object_the_write_did() {
        let signer = presigner(Some("tenant-a"));
        let written = presign_put(&signer);
        let read = signer
            .presign_get(
                PresignedGetRequest {
                    object_key: CONTENT_KEY,
                    expires_in: EXPIRES_IN,
                },
                signing_time(),
            )
            .expect("presign get");

        let object_of = |url: &str| url.split('?').next().expect("url path").to_owned();
        assert_eq!(object_of(&read.url), object_of(&written.url));
    }

    #[test]
    fn gcs_advertises_crc32c_and_googles_documented_single_request_ceiling() {
        let signer = presigner(None);
        assert_eq!(signer.checksum_algorithm(), ChecksumAlgorithm::Crc32c);
        assert_eq!(signer.max_content_bytes(), GCP_GCS_MAX_DIRECT_PUT_BYTES);
        assert_eq!(GCP_GCS_MAX_DIRECT_PUT_BYTES, 5 * 1024 * 1024 * 1024 * 1024);
    }

    /// A checksum GCS cannot enforce is refused at issuance rather than
    /// signed into a write the provider would accept without checking.
    #[test]
    fn presigned_put_rejects_content_refs_it_cannot_bind_to_the_write() {
        let signer = presigner(None);
        let unsupported_kind = ContentRef {
            kind: ContentRefKind::Unsupported("future_kind".to_owned()),
            ..crc32c_content_ref()
        };
        let sha256_only = ContentRef {
            storage_checksum: StorageChecksum::sha256(b"hello"),
            whole_file_sha256: Some(StorageChecksum::sha256(b"hello").value),
            ..crc32c_content_ref()
        };
        let malformed_crc = ContentRef {
            storage_checksum: StorageChecksum {
                algorithm: ChecksumAlgorithm::Crc32c,
                value: "nothex!!".to_owned(),
            },
            ..crc32c_content_ref()
        };

        for content_ref in [unsupported_kind, sha256_only, malformed_crc] {
            let error = signer
                .presign_put(
                    PresignedPutRequest {
                        object_key: CONTENT_KEY,
                        content_ref: &content_ref,
                        expires_in: EXPIRES_IN,
                    },
                    signing_time(),
                )
                .expect_err("unsignable content ref");
            assert!(matches!(error, ObjectStoreError::InvalidContentRef(_)));
        }
    }

    #[test]
    fn expiry_outside_googles_documented_window_is_a_configuration_error() {
        let signer = presigner(None);
        for expires_in in [Duration::ZERO, Duration::from_secs(7 * 24 * 60 * 60 + 1)] {
            assert!(matches!(
                signer.presign_get(
                    PresignedGetRequest {
                        object_key: CONTENT_KEY,
                        expires_in,
                    },
                    signing_time(),
                ),
                Err(ObjectStoreError::Configuration(_))
            ));
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

    /// A signing key is a bearer credential for the whole bucket, so nothing
    /// about it reaches a log through the store that holds it.
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

    /// GCS lists hashes in an unspecified order and may name algorithms this
    /// format does not, so the CRC-32C is selected rather than positioned.
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
