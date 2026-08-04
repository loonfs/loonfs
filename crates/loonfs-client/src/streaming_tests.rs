//! What a one-pass put costs in memory, and that it still reconciles a
//! retried commit id.
//!
//! The measurement here is retention, not resident size: the source hands
//! out chunks whose owners report when they are released, so the peak is
//! how much of the payload the uploader was holding at once — a number that
//! is exact, deterministic, and says nothing about the allocator.
#![allow(clippy::panic)]
// Transport-choice tests panic in unexpected match arms for precise
// diagnostics.

use super::*;
use crate::transport::test_transport::{self, Outcome};
use futures::stream::StreamExt;
use loonfs_api::v0::{DirectMultipartUpload, DirectPutUpload, UploadMode};
use loonfs_api::{
    direct_put_checksum_feature, CapabilityDocument, ContentId, ContentRef,
    FEATURE_UPLOADS_DIRECT_PUT, PROFILE_CORE_V0, PROTOCOL_VERSION,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Part geometry the scripted server hands back. Smaller than the default
/// so the test's payload stays cheap, and a server is free to choose it.
const TEST_PART_BYTES: u64 = 1024 * 1024;
/// A payload just past the size at which a put stops holding its bytes
/// whole — which is what puts it on the streaming path at all — cut into
/// eight full parts and a short ninth, so the source's end is discovered
/// rather than computed.
const TEST_PAYLOAD_BYTES: usize = STREAMING_PUT_MIN_BYTES as usize + 1_000;
/// Parts [`TEST_PAYLOAD_BYTES`] is cut into at [`TEST_PART_BYTES`].
const TEST_PAYLOAD_PARTS: u32 = 9;

/// How much of a payload its consumer is holding, tracked by the payload
/// itself.
#[derive(Debug, Default)]
struct Retention {
    live_bytes: AtomicU64,
    peak_live_bytes: AtomicU64,
    live_chunks: AtomicUsize,
    peak_live_chunks: AtomicUsize,
    total_bytes: AtomicU64,
}

impl Retention {
    fn handed_out(&self, len: usize) {
        let live = self.live_bytes.fetch_add(len as u64, Ordering::SeqCst) + len as u64;
        self.peak_live_bytes.fetch_max(live, Ordering::SeqCst);
        let chunks = self.live_chunks.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_live_chunks.fetch_max(chunks, Ordering::SeqCst);
        self.total_bytes.fetch_add(len as u64, Ordering::SeqCst);
    }

    fn released(&self, len: usize) {
        self.live_bytes.fetch_sub(len as u64, Ordering::SeqCst);
        self.live_chunks.fetch_sub(1, Ordering::SeqCst);
    }

    fn peak_live_bytes(&self) -> u64 {
        self.peak_live_bytes.load(Ordering::SeqCst)
    }

    fn peak_live_chunks(&self) -> usize {
        self.peak_live_chunks.load(Ordering::SeqCst)
    }

    fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::SeqCst)
    }
}

/// One chunk of a watched payload. Its `Drop` is what marks the bytes
/// released, so the live count falls exactly when the consumer lets go —
/// including when a part is a view of this chunk rather than a copy of it.
struct WatchedChunk {
    bytes: Vec<u8>,
    retention: Arc<Retention>,
}

impl AsRef<[u8]> for WatchedChunk {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for WatchedChunk {
    fn drop(&mut self) {
        self.retention.released(self.bytes.len());
    }
}

/// A payload that reports how much of itself is being held.
///
/// Chunks are cut at the part size on purpose: the part reader hands a
/// part-filling chunk straight through instead of copying it, so a part
/// keeps its chunk alive for exactly as long as the uploader keeps the
/// part. A source chunked more finely would be copied into part buffers and
/// released early, which would measure the source rather than the uploader.
fn watched_source(payload: &[u8], chunk_bytes: usize) -> (PayloadSource, Arc<Retention>) {
    let retention = Arc::new(Retention::default());
    let chunks: Vec<Vec<u8>> = payload
        .chunks(chunk_bytes)
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    let handed = Arc::clone(&retention);
    let stream = futures::stream::iter(chunks.into_iter().map(move |bytes| {
        handed.handed_out(bytes.len());
        Ok(Bytes::from_owner(WatchedChunk {
            bytes,
            retention: Arc::clone(&handed),
        }))
    }))
    .boxed();
    let size_bytes = payload.len() as u64;
    (PayloadSource::sized_stream(stream, size_bytes), retention)
}

/// A source that declares a length it does not deliver.
///
/// `PayloadSource::sized_stream` documents its length as a hint, and this is
/// what a wrong one looks like: the bytes are what they are, and only they
/// may decide what is claimed or refused.
fn source_declaring(payload: &[u8], declared: u64, chunk_bytes: usize) -> PayloadSource {
    let chunks: Vec<Vec<u8>> = payload.chunks(chunk_bytes).map(<[u8]>::to_vec).collect();
    let stream =
        futures::stream::iter(chunks.into_iter().map(|bytes| Ok(Bytes::from(bytes)))).boxed();
    PayloadSource::sized_stream(stream, declared)
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|offset| (offset % 251) as u8).collect()
}

fn namespace_id() -> NamespaceId {
    NamespaceId::parse("demo").expect("valid namespace id")
}

fn spec() -> NamespacePath {
    NamespacePath::parse("demo", "/big.bin").expect("valid namespace path")
}

fn upload_id() -> UploadId {
    UploadId::parse("upl_00000000000000000000000000000001").expect("valid upload id")
}

fn client() -> Client {
    Client::new(ClientConfig {
        server_url: "http://example.invalid".to_owned(),
        auth_token: None,
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid client config")
}

/// A client whose failures are the test's own, not the retry policy's, so a
/// scripted conversation is exactly as long as it reads.
fn client_without_retry() -> Client {
    Client::new(ClientConfig {
        server_url: "http://example.invalid".to_owned(),
        auth_token: None,
        request_timeout_ms: None,
        disable_transient_retry: true,
        ca_cert_path: None,
    })
    .expect("valid client config")
}

fn json(value: &impl serde::Serialize) -> Outcome {
    Outcome::Success(serde_json::to_vec(value).expect("serialize scripted response"))
}

/// A capability document advertising whichever upload transports the test
/// wants the deployment to offer.
fn capabilities(direct_multipart: bool) -> Outcome {
    capabilities_for(Advertised {
        direct_multipart,
        ..Advertised::default()
    })
}

/// What a scripted deployment says about its upload transports.
#[derive(Default, Clone, Copy)]
struct Advertised {
    direct_multipart: bool,
    /// The whole-object checksum this deployment says its provider
    /// enforces, or `None` to advertise no `direct_put` at all.
    ///
    /// Providers do not agree on one -- the S3 family enforces SHA-256 and
    /// GCS enforces CRC-32C -- so the shape is a parameter here for the same
    /// reason it is a capability key on the wire: the client folds whichever
    /// the deployment names.
    direct_put: Option<ChecksumAlgorithm>,
    /// The service's own buffering cap, when the deployment advertises one.
    proxy_max_bytes: Option<u64>,
    /// The provider's single-request ceiling, when it advertises one.
    direct_put_max_bytes: Option<u64>,
}

fn capabilities_for(advertised: Advertised) -> Outcome {
    let mut features = std::collections::BTreeMap::from([(
        FEATURE_UPLOADS_DIRECT_MULTIPART.to_owned(),
        advertised.direct_multipart,
    )]);
    if let Some(algorithm) = advertised.direct_put {
        features.insert(FEATURE_UPLOADS_DIRECT_PUT.to_owned(), true);
        features.insert(direct_put_checksum_feature(algorithm).to_owned(), true);
    }
    let mut limits = std::collections::BTreeMap::new();
    if let Some(cap) = advertised.proxy_max_bytes {
        limits.insert(LIMIT_UPLOAD_MAX_CONTENT_BYTES.to_owned(), cap);
    }
    if let Some(cap) = advertised.direct_put_max_bytes {
        limits.insert(LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES.to_owned(), cap);
    }
    json(&CapabilityDocument {
        protocol_version: PROTOCOL_VERSION.to_owned(),
        profiles: vec![PROFILE_CORE_V0.to_owned()],
        features,
        limits,
    })
}

fn begin_direct_put(content_ref: ContentRef) -> Outcome {
    json(&BeginUploadResponse {
        namespace_id: namespace_id(),
        upload_id: upload_id(),
        mode: UploadMode::DirectPut,
        direct_put: Some(DirectPutUpload {
            content_ref,
            access: ObjectTransferAccess::PresignedUrl {
                method: "PUT".to_owned(),
                url: "http://object.invalid/content".to_owned(),
                headers: std::collections::BTreeMap::new(),
                expires_at_ms: u64::MAX,
            },
        }),
        direct_multipart: None,
    })
}

fn begin_multipart() -> Outcome {
    json(&BeginUploadResponse {
        namespace_id: namespace_id(),
        upload_id: upload_id(),
        mode: UploadMode::DirectMultipart,
        direct_put: None,
        direct_multipart: Some(DirectMultipartUpload {
            part_size_bytes: TEST_PART_BYTES,
        }),
    })
}

fn begin_proxied() -> Outcome {
    json(&BeginUploadResponse {
        namespace_id: namespace_id(),
        upload_id: upload_id(),
        mode: UploadMode::ServiceProxied,
        direct_put: None,
        direct_multipart: None,
    })
}

/// Authorizes `count` parts starting at `first`, as one wave's signing
/// response would.
fn signed_parts(first: u32, count: u32) -> Outcome {
    json(&SignUploadPartsResponse {
        namespace_id: namespace_id(),
        upload_id: upload_id(),
        parts: (first..first + count)
            .map(|part_number| SignedUploadPart {
                part_number,
                access: ObjectTransferAccess::PresignedUrl {
                    method: "PUT".to_owned(),
                    url: format!("http://provider.invalid/part/{part_number}"),
                    headers: std::collections::BTreeMap::new(),
                    expires_at_ms: 1,
                },
            })
            .collect(),
    })
}

fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef::blob_v1(ContentId::generate(), bytes)
}

fn completed(content_ref: ContentRef) -> Outcome {
    json(&CompleteUploadResponse {
        namespace_id: namespace_id(),
        upload_id: upload_id(),
        content_ref,
        validated_content_token: None,
    })
}

fn commit_landed() -> Outcome {
    json(&ApiCommitResponse {
        namespace_id: namespace_id(),
        commit_id: CommitId::parse("c_00000000000000000000000000000001").expect("valid commit id"),
        committed_seq: ChangeSeq(1),
    })
}

/// The scripted conversation a direct-multipart put has: capabilities,
/// begin, then per wave one signing request and one upload per part, then
/// completion and the commit.
fn multipart_script(parts: u32, uploaded: ContentRef) -> Vec<Outcome> {
    let mut script = vec![capabilities(true), begin_multipart()];
    let window = DIRECT_MULTIPART_PARTS_IN_FLIGHT as u32;
    let mut next = 1;
    while next <= parts {
        let wave = window.min(parts + 1 - next);
        script.push(signed_parts(next, wave));
        for part_number in next..next + wave {
            script.push(Outcome::PartAccepted(format!("\"etag-{part_number}\"")));
        }
        next += wave;
    }
    // A payload that ends on a wave boundary needs one more wave to be told
    // the source is done; this one does not.
    script.push(completed(uploaded));
    script.push(commit_landed());
    script
}

/// A journal that keeps what it is told, so a test can hand it back as the
/// record of an interrupted run.
#[derive(Debug, Default)]
struct RecordingJournal {
    began: Mutex<Option<(UploadId, u64)>>,
    parts: Mutex<Vec<CompletedUploadPart>>,
}

impl RecordingJournal {
    fn resume(&self) -> MultipartUploadResume {
        let began = self.began.lock().expect("journal lock").clone();
        let (upload_id, part_size_bytes) = began.expect("the session was opened");
        MultipartUploadResume {
            upload_id,
            part_size_bytes,
            parts: self.parts.lock().expect("journal lock").clone(),
        }
    }

    fn part_numbers(&self) -> Vec<u32> {
        self.parts
            .lock()
            .expect("journal lock")
            .iter()
            .map(|part| part.part_number)
            .collect()
    }
}

impl MultipartUploadJournal for RecordingJournal {
    fn began(&self, upload_id: &UploadId, part_size_bytes: u64) {
        *self.began.lock().expect("journal lock") = Some((upload_id.clone(), part_size_bytes));
    }

    fn part_completed(&self, part: &CompletedUploadPart) {
        self.parts.lock().expect("journal lock").push(part.clone());
    }
}

/// The conversation a resumed upload has: no `begin` — it rejoins the
/// session it was given — and signing plus uploads only for the parts still
/// missing.
fn resumed_script(missing: &[u32], uploaded: ContentRef) -> Vec<Outcome> {
    let mut script = vec![capabilities(true)];
    let window = DIRECT_MULTIPART_PARTS_IN_FLIGHT;
    for wave in missing.chunks(window) {
        script.push(signed_parts(wave[0], wave.len() as u32));
        for part_number in wave {
            script.push(Outcome::PartAccepted(format!("\"etag-{part_number}\"")));
        }
    }
    script.push(completed(uploaded));
    script.push(commit_landed());
    script
}

/// An upload interrupted after some parts landed picks the session back up
/// and sends only what is missing. Every byte is still read — the assembly
/// is verified against a checksum over the whole object — but the parts
/// already in object storage are not sent a second time.
#[tokio::test]
async fn a_resumed_multipart_put_uploads_only_the_parts_that_are_missing() {
    let payload = payload(TEST_PAYLOAD_BYTES);
    let uploaded = content_ref(&payload);
    let journal = RecordingJournal::default();

    // The first run gets through two waves and is cut off before the third
    // is signed: the script runs out where the interruption did.
    let landed = DIRECT_MULTIPART_PARTS_IN_FLIGHT as u32 * 2;
    let mut first = vec![capabilities(true), begin_multipart()];
    for wave in 0..2u32 {
        let first_part = wave * DIRECT_MULTIPART_PARTS_IN_FLIGHT as u32 + 1;
        first.push(signed_parts(
            first_part,
            DIRECT_MULTIPART_PARTS_IN_FLIGHT as u32,
        ));
        for part_number in first_part..first_part + DIRECT_MULTIPART_PARTS_IN_FLIGHT as u32 {
            first.push(Outcome::PartAccepted(format!("\"etag-{part_number}\"")));
        }
    }
    // The signing request for the third wave never lands, and the abort
    // that follows a failed session does not either. Retry is off so each
    // is one attempt and the script stays exactly this long.
    first.push(Outcome::TransportFailure);
    first.push(Outcome::TransportFailure);
    let transport = test_transport::script(first);
    let interrupted = client_without_retry()
        .put_file_stream_resumable(
            &spec(),
            PayloadSource::sized_stream(
                futures::stream::once({
                    let payload = payload.clone();
                    async move { Ok(Bytes::from(payload)) }
                })
                .boxed(),
                payload.len() as u64,
            ),
            &PutFileOptions::default(),
            &journal,
            None,
        )
        .await;
    assert!(interrupted.is_err(), "the third wave never got signed");
    assert_eq!(
        journal.part_numbers(),
        (1..=landed).collect::<Vec<_>>(),
        "the journal holds exactly the parts that landed"
    );
    let resume = journal.resume();
    assert_eq!(resume.part_size_bytes, TEST_PART_BYTES);
    drop(transport);

    // The rerun sends only parts 9 onward.
    let missing: Vec<u32> = (landed + 1..=TEST_PAYLOAD_PARTS).collect();
    let (source, retention) = watched_source(&payload, TEST_PART_BYTES as usize);
    let transport = test_transport::script(resumed_script(&missing, uploaded));
    let resumed_journal = RecordingJournal::default();
    client()
        .put_file_stream_resumable(
            &spec(),
            source,
            &PutFileOptions::default(),
            &resumed_journal,
            Some(&resume),
        )
        .await
        .expect("a resumed multipart put should land");

    assert_eq!(
        resumed_journal.part_numbers(),
        missing,
        "only the missing parts were uploaded"
    );
    assert_eq!(
        retention.total_bytes(),
        TEST_PAYLOAD_BYTES as u64,
        "every byte is still folded into the whole-object checksum"
    );
    // capabilities + one signing request + one PUT per missing part +
    // completion + commit, and no `begin`: the session was rejoined.
    assert_eq!(transport.attempts(), 1 + 1 + missing.len() + 2);
}

/// A large put reads its source once and never holds more of it than the
/// window it uploads in.
#[tokio::test]
async fn a_direct_multipart_put_holds_only_its_window() {
    let payload = payload(TEST_PAYLOAD_BYTES);
    let (source, retention) = watched_source(&payload, TEST_PART_BYTES as usize);
    let _transport =
        test_transport::script(multipart_script(TEST_PAYLOAD_PARTS, content_ref(&payload)));

    client()
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect("a scripted multipart put should land");

    let window_bound = DIRECT_MULTIPART_PARTS_IN_FLIGHT as u64 * TEST_PART_BYTES;
    assert_eq!(
        retention.total_bytes(),
        TEST_PAYLOAD_BYTES as u64,
        "every payload byte crossed the source boundary exactly once"
    );
    assert!(
        retention.peak_live_bytes() <= window_bound,
        "the put held {} bytes at once, past its {window_bound}-byte window",
        retention.peak_live_bytes()
    );
    assert!(
        retention.peak_live_chunks() <= DIRECT_MULTIPART_PARTS_IN_FLIGHT,
        "the put held {} parts at once",
        retention.peak_live_chunks()
    );
}

/// Nothing about the flow depends on knowing the length first: a source
/// that cannot say how long it is takes the same path and the same window.
#[tokio::test]
async fn an_unknown_length_source_uploads_the_same_way() {
    let payload = payload(TEST_PAYLOAD_BYTES);
    let (sized, retention) = watched_source(&payload, TEST_PART_BYTES as usize);
    let (stream, size_bytes) = sized.into_stream();
    assert_eq!(size_bytes, Some(TEST_PAYLOAD_BYTES as u64));
    // Throw the length away: this is the pipe's view of the same bytes.
    let source = PayloadSource::stream(stream);
    assert_eq!(source.size_bytes(), None);
    let _transport =
        test_transport::script(multipart_script(TEST_PAYLOAD_PARTS, content_ref(&payload)));

    client()
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect("a length-less source should upload");

    assert_eq!(retention.total_bytes(), TEST_PAYLOAD_BYTES as u64);
    assert!(
        retention.peak_live_bytes() <= DIRECT_MULTIPART_PARTS_IN_FLIGHT as u64 * TEST_PART_BYTES,
        "peak was {}",
        retention.peak_live_bytes()
    );
}

/// A deployment that cannot authorize part uploads gets the same source as
/// a request body, and the client accumulates none of it.
#[tokio::test]
async fn a_proxied_put_streams_its_body() {
    let payload = payload(TEST_PAYLOAD_BYTES);
    let chunk_bytes = 16 * 1024;
    let (source, retention) = watched_source(&payload, chunk_bytes);
    let uploaded = content_ref(&payload);
    let _transport = test_transport::script(vec![
        capabilities(false),
        begin_proxied(),
        json(&UploadContentResponse {
            namespace_id: namespace_id(),
            upload_id: upload_id(),
            content_ref: uploaded.clone(),
        }),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect("a scripted proxied put should land");

    assert_eq!(
        retention.total_bytes(),
        TEST_PAYLOAD_BYTES as u64,
        "the whole payload was forwarded"
    );
    assert!(
        retention.peak_live_bytes() <= (2 * chunk_bytes) as u64,
        "a forwarded body should never accumulate; peak was {}",
        retention.peak_live_bytes()
    );
}

/// A small source goes through the server — but because the deployment's
/// advertised cap says it fits, not because the client assumed it would.
/// The document is read once and cached, so the round trip is paid at most
/// once per client however many puts follow.
#[tokio::test]
async fn a_small_sized_source_proxies_against_the_advertised_cap() {
    let payload = payload(1_000);
    let uploaded = content_ref(&payload);
    let (source, _) = watched_source(&payload, 512);
    let transport = test_transport::script(vec![
        capabilities_for(Advertised {
            proxy_max_bytes: Some(4_096),
            ..Advertised::default()
        }),
        begin_proxied(),
        json(&UploadContentResponse {
            namespace_id: namespace_id(),
            upload_id: upload_id(),
            content_ref: uploaded.clone(),
        }),
        completed(uploaded),
        commit_landed(),
    ]);

    let client = client();
    client
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect("a small streamed put should land");

    assert_eq!(
        transport
            .sent()
            .iter()
            .filter(|sent| sent.url.ends_with("/v0/capabilities"))
            .count(),
        1,
        "the capability document is fetched once and cached on the client"
    );
}

/// The reviewer's case: a payload the service will not buffer, on a
/// deployment with no multipart API. Being under one part says nothing
/// about whether the service would take it, so the whole-object write is
/// what carries it — the fast path may not assume a cap it never read.
#[tokio::test]
async fn a_small_payload_past_the_proxy_cap_takes_direct_put() {
    let payload = payload(1_025);
    let uploaded = content_ref(&payload);
    let (source, _) = watched_source(&payload, 512);
    let transport = test_transport::script(vec![
        capabilities_for(Advertised {
            direct_put: Some(ChecksumAlgorithm::Sha256),
            proxy_max_bytes: Some(1_024),
            direct_put_max_bytes: Some(5 * 1024 * 1024 * 1024),
            ..Advertised::default()
        }),
        begin_direct_put(uploaded.clone()),
        Outcome::Success(Vec::new()),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect("a payload past the proxy cap should take the direct write");

    let object_write = transport
        .sent()
        .into_iter()
        .find(|sent| sent.url.starts_with("http://object.invalid/"))
        .expect("the payload went straight to object storage");
    assert_eq!(object_write.body_bytes(), payload.len());
}

/// The reviewer's case: a payload nobody measured, on a deployment that
/// signs no multipart — which is what the GCS adapter is.
///
/// A length nobody knows is not a reason to fall to the service. The
/// whole-object write's own first pass measures the payload without sending
/// a byte, and that measurement is what routes it. Falling straight to the
/// proxy here would stream an unmeasured payload — a pipe, a `tar` on
/// stdin — at a cap it does not fit, and fail at the far end after moving
/// every byte, with the one transport that could have carried it never
/// asked.
#[tokio::test]
async fn an_unknown_length_payload_past_the_proxy_cap_takes_direct_put() {
    let payload = payload(64 * 1024);
    let uploaded = content_ref(&payload);
    let (sized, retention) = watched_source(&payload, 4 * 1024);
    let (stream, _) = sized.into_stream();
    // The pipe's view of the same bytes: no length to route on.
    let source = PayloadSource::stream(stream);
    assert_eq!(source.size_bytes(), None);

    let transport = test_transport::script(vec![
        // A GCS-shaped deployment: crc32c whole-object writes, no multipart
        // API, and a proxy that would refuse this payload.
        capabilities_for(Advertised {
            direct_put: Some(ChecksumAlgorithm::Crc32c),
            direct_multipart: false,
            proxy_max_bytes: Some(1_024),
            direct_put_max_bytes: Some(5 * 1024 * 1024 * 1024 * 1024),
        }),
        begin_direct_put(uploaded.clone()),
        Outcome::Success(Vec::new()),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect("an unmeasured payload past the proxy cap should take the direct write");

    let object_write = transport
        .sent()
        .into_iter()
        .find(|sent| sent.url.starts_with("http://object.invalid/"))
        .expect("the payload went straight to object storage");
    assert_eq!(object_write.body_bytes(), payload.len());
    // And the measuring pass still never held it: it folded and spooled.
    assert_eq!(retention.total_bytes(), payload.len() as u64);
    assert!(
        retention.peak_live_bytes() <= 8 * 1024,
        "the measuring pass accumulated the payload; peak was {}",
        retention.peak_live_bytes()
    );
}

/// The other half of the same rule: measuring first does not push small
/// payloads onto the direct write.
///
/// The pass decides nothing by itself — it produces a length, and the
/// ordinary ladder routes on it. A payload the service will take goes
/// through the service, exactly as it would have with its length known up
/// front.
#[tokio::test]
async fn a_small_unknown_length_payload_still_proxies() {
    let payload = payload(1_000);
    let uploaded = content_ref(&payload);
    let (sized, _) = watched_source(&payload, 512);
    let (stream, _) = sized.into_stream();
    let source = PayloadSource::stream(stream);
    assert_eq!(source.size_bytes(), None);

    let transport = test_transport::script(vec![
        capabilities_for(Advertised {
            direct_put: Some(ChecksumAlgorithm::Crc32c),
            direct_multipart: false,
            proxy_max_bytes: Some(4_096),
            direct_put_max_bytes: Some(5 * 1024 * 1024 * 1024 * 1024),
        }),
        begin_proxied(),
        json(&UploadContentResponse {
            namespace_id: namespace_id(),
            upload_id: upload_id(),
            content_ref: uploaded.clone(),
        }),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect("a small unmeasured payload should still proxy");

    assert!(
        transport
            .sent()
            .into_iter()
            .all(|sent| !sent.url.starts_with("http://object.invalid/")),
        "a payload the service would take was sent straight to object storage"
    );
}

/// A direct-put payload crosses the network in bounded pieces: the first
/// pass measures it without holding it, and the second streams it from
/// where that pass left it. Nothing on either side of the digest ever holds
/// the payload whole.
#[tokio::test]
async fn a_direct_put_streams_its_payload_without_ever_holding_it() {
    let payload = payload(TEST_PAYLOAD_BYTES);
    let uploaded = content_ref(&payload);
    let (source, retention) = watched_source(&payload, TEST_PART_BYTES as usize);
    let transport = test_transport::script(vec![
        capabilities_for(Advertised {
            direct_put: Some(ChecksumAlgorithm::Sha256),
            proxy_max_bytes: Some(1_024),
            direct_put_max_bytes: Some(5 * 1024 * 1024 * 1024),
            ..Advertised::default()
        }),
        begin_direct_put(uploaded.clone()),
        Outcome::Success(Vec::new()),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect("a large streamed direct put should land");

    // Pass one: the source's own chunks were folded and spooled, never
    // accumulated.
    assert_eq!(retention.total_bytes(), TEST_PAYLOAD_BYTES as u64);
    assert!(
        retention.peak_live_bytes() <= 2 * TEST_PART_BYTES,
        "the measuring pass accumulated the payload; peak was {}",
        retention.peak_live_bytes()
    );

    // Pass two: the request body arrived in many bounded pieces, so no
    // single allocation ever held the payload.
    let object_write = transport
        .sent()
        .into_iter()
        .find(|sent| sent.url.starts_with("http://object.invalid/"))
        .expect("the payload went straight to object storage");
    assert_eq!(object_write.body_bytes(), TEST_PAYLOAD_BYTES);
    assert!(
        object_write.body_chunks.len() > 1,
        "a payload sent in one piece was assembled whole somewhere"
    );
    assert!(
        object_write.largest_body_chunk() <= crate::payload::SOURCE_CHUNK_BYTES,
        "one body piece was larger than a source read: {} bytes",
        object_write.largest_body_chunk()
    );
}

/// A declared length is a hint, so a wrong one may not end an upload. Here
/// it says nothing can carry the payload; the measuring pass finds that
/// something can, and the upload lands.
#[tokio::test]
async fn a_wrong_size_hint_does_not_refuse_an_upload_that_actually_fits() {
    let payload = payload(2_000);
    let uploaded = content_ref(&payload);
    // Declares far more than the largest transport would take.
    let source = source_declaring(&payload, 100_000, 512);
    let _transport = test_transport::script(vec![
        capabilities_for(Advertised {
            direct_put: Some(ChecksumAlgorithm::Sha256),
            proxy_max_bytes: Some(1_024),
            direct_put_max_bytes: Some(4_096),
            ..Advertised::default()
        }),
        begin_direct_put(uploaded.clone()),
        Outcome::Success(Vec::new()),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect("a payload that actually fits must not be refused on a hint");
}

/// When nothing really can carry the payload, the refusal reports what the
/// client measured — never the length the source declared.
#[tokio::test]
async fn a_refusal_reports_the_measured_length_not_the_declared_one() {
    let payload = payload(50_000);
    let source = source_declaring(&payload, 100_000, 512);
    let _transport = test_transport::script(vec![capabilities_for(Advertised {
        direct_put: Some(ChecksumAlgorithm::Sha256),
        proxy_max_bytes: Some(1_024),
        direct_put_max_bytes: Some(4_096),
        ..Advertised::default()
    })]);

    let error = client()
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect_err("no transport can carry this payload");
    match error {
        ClientError::UploadTooLarge { size_bytes, reason } => {
            assert_eq!(
                size_bytes,
                payload.len() as u64,
                "the refusal must name the measured length, not the declared one"
            );
            assert!(reason.contains("4096"), "{reason}");
        }
        other => panic!("expected an upload-too-large refusal, got {other:?}"),
    }
}

/// A file-backed source is the dominant case, and it pays nothing extra:
/// the second pass re-opens the file the caller named, so the payload is
/// never copied anywhere — not into memory and not into a spool.
#[tokio::test]
async fn a_file_backed_direct_put_re_reads_the_file_rather_than_spooling_it() {
    let payload = payload(TEST_PAYLOAD_BYTES);
    let uploaded = content_ref(&payload);
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("payload.bin");
    std::fs::write(&path, &payload).expect("write the payload");
    let source = PayloadSource::open_file(&path).await.expect("open payload");

    let transport = test_transport::script(vec![
        capabilities_for(Advertised {
            direct_put: Some(ChecksumAlgorithm::Sha256),
            proxy_max_bytes: Some(1_024),
            direct_put_max_bytes: Some(5 * 1024 * 1024 * 1024),
            ..Advertised::default()
        }),
        begin_direct_put(uploaded.clone()),
        Outcome::Success(Vec::new()),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(&spec(), source, &PutFileOptions::default())
        .await
        .expect("a file-backed direct put should land");

    // Only the caller's own file was ever written; the upload added none.
    let left_behind: Vec<_> = std::fs::read_dir(directory.path())
        .expect("read the directory back")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.file_name())
        .collect();
    assert_eq!(
        left_behind,
        vec![std::ffi::OsString::from("payload.bin")],
        "a file-backed payload should be re-read, not copied"
    );

    let object_write = transport
        .sent()
        .into_iter()
        .find(|sent| sent.url.starts_with("http://object.invalid/"))
        .expect("the payload went straight to object storage");
    assert_eq!(object_write.body_bytes(), TEST_PAYLOAD_BYTES);
    assert!(object_write.largest_body_chunk() <= crate::payload::SOURCE_CHUNK_BYTES);
}

/// Sends one streamed body to a socket that only reads and reports the
/// request head, so the framing the client chose is observable.
async fn request_head_for(source: PayloadSource) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a probe socket");
    let address = listener.local_addr().expect("probe address");
    let served = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept the request");
        let mut buffer = vec![0u8; 4096];
        let read = tokio::io::AsyncReadExt::read(&mut socket, &mut buffer)
            .await
            .expect("read the request head");
        let _ = tokio::io::AsyncWriteExt::write_all(
            &mut socket,
            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}",
        )
        .await;
        String::from_utf8_lossy(&buffer[..read]).to_lowercase()
    });

    let client = Client::new(ClientConfig {
        server_url: format!("http://{address}"),
        auth_token: None,
        request_timeout_ms: None,
        disable_transient_retry: true,
        ca_cert_path: None,
    })
    .expect("valid client config");
    // The response is not the point; the request head is.
    let _ = client
        .upload_streamed_content(&namespace_id(), &upload_id(), source)
        .await;
    served.await.expect("probe task")
}

/// A source that knows its length declares it, so the server can refuse an
/// oversized body before reading it.
#[tokio::test]
async fn a_sized_source_frames_its_body_with_a_content_length() {
    let stream = futures::stream::iter(vec![Ok(Bytes::from_static(b"0123456789"))]).boxed();
    let head = request_head_for(PayloadSource::sized_stream(stream, 10)).await;

    assert!(head.contains("content-length: 10"), "{head}");
    assert!(
        !head.contains("chunked"),
        "a body of known length is not chunked: {head}"
    );
}

/// A source that cannot know its length is framed chunked, which is the
/// only honest framing for it — and the case the server's incremental cap
/// exists to bound.
#[tokio::test]
async fn an_unsized_source_frames_its_body_chunked() {
    let stream = futures::stream::iter(vec![Ok(Bytes::from_static(b"0123456789"))]).boxed();
    let head = request_head_for(PayloadSource::stream(stream)).await;

    assert!(head.contains("transfer-encoding: chunked"), "{head}");
    assert!(
        !head.contains("content-length"),
        "a body of unknown length cannot declare one: {head}"
    );
}

/// The evidence a streamed put keeps is the reference the server minted for
/// it, and it answers only what it actually knows.
#[test]
fn streamed_evidence_answers_only_the_digests_it_has() {
    let bytes = b"the same bytes twice";
    let sha256 = ContentRef::blob_v1(ContentId::generate(), bytes);
    let streamed = UploadedContent::Streamed(&sha256);
    assert_eq!(
        streamed.matches(&StorageChecksum::sha256(bytes)),
        Some(true)
    );
    assert_eq!(
        streamed.matches(&StorageChecksum::sha256(b"other bytes entirely")),
        Some(false)
    );
    // A provider-assembled object is described by a CRC, and a SHA-256
    // reference cannot be compared against it. That is a refusal, not
    // agreement.
    assert_eq!(
        streamed.matches(&StorageChecksum::crc64nvme(bytes)),
        None,
        "a digest nobody computed over this payload must not be answered"
    );

    let assembled = ContentRef {
        kind: sha256.kind,
        content_id: sha256.content_id.clone(),
        size_bytes: sha256.size_bytes,
        storage_checksum: StorageChecksum::crc64nvme(bytes),
        whole_file_sha256: None,
    };
    let streamed = UploadedContent::Streamed(&assembled);
    assert_eq!(
        streamed.matches(&StorageChecksum::crc64nvme(bytes)),
        Some(true),
        "the digest one pass folded is what a multipart retry compares"
    );
    assert_eq!(
        streamed.matches(&StorageChecksum::crc64nvme(b"different bytes here")),
        Some(false)
    );
    assert_eq!(streamed.matches(&StorageChecksum::sha256(bytes)), None);
}

/// Held bytes can answer any digest in the vocabulary, which is why the
/// buffered path never has to refuse.
#[test]
fn held_evidence_recomputes_whatever_it_is_asked() {
    let bytes = b"held in memory";
    let held = UploadedContent::Bytes(bytes);
    assert_eq!(held.matches(&StorageChecksum::sha256(bytes)), Some(true));
    assert_eq!(held.matches(&StorageChecksum::crc64nvme(bytes)), Some(true));
    assert_eq!(held.matches(&StorageChecksum::crc32c(bytes)), Some(true));
    assert_eq!(
        held.matches(&StorageChecksum::sha256(b"something else")),
        Some(false)
    );
    assert_eq!(
        held.matches(&StorageChecksum::crc32c(b"something else")),
        Some(false)
    );
}
