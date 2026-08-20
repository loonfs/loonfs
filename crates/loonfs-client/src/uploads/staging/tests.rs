//! Upload memory bounds, transport selection, and retry behavior.
//!
//! Test chunks record when they are created and dropped. This measures the
//! payload bytes retained by the uploader without depending on allocator
//! behavior.
#![allow(clippy::panic)]
// Transport-choice tests panic in unexpected match arms for precise
// diagnostics.

use super::*;
use crate::transport::test_transport::{self, Outcome};
use futures::stream::StreamExt;
use loonfs_api::v0::{DirectMultipartUpload, DirectPutUpload, UploadMode};
use loonfs_api::{
    CapabilityDocument, ContentId, ContentRef, ContentRefKind, FEATURE_UPLOADS_DIRECT_PUT,
    PROFILE_CORE_V0, PROTOCOL_VERSION,
};
use loonfs_test_support::ids::content_ref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Small part size used to keep multipart tests inexpensive.
const TEST_PART_BYTES: u64 = 1024 * 1024;
/// Payload just above the streaming threshold, split into eight full parts
/// and one partial part.
const TEST_PAYLOAD_BYTES: usize = STREAMING_PUT_MIN_BYTES as usize + 1_000;
/// Parts [`TEST_PAYLOAD_BYTES`] is cut into at [`TEST_PART_BYTES`].
const TEST_PAYLOAD_PARTS: u32 = 9;

/// Tracks payload chunks retained by the uploader.
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

/// A chunk that updates retention counters when dropped.
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

/// Builds a source that records retained payload bytes.
///
/// Source chunks match the part size so the uploader can reuse each buffer.
/// This keeps the counters aligned with buffers retained by in-flight parts.
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
    (PayloadSource::stream(stream), retention)
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
    /// The algorithm returned at begin, or `None` to advertise no
    /// `direct_put` at all.
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
    if advertised.direct_put.is_some() {
        features.insert(FEATURE_UPLOADS_DIRECT_PUT.to_owned(), true);
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

fn begin_direct_put(checksum_algorithm: ChecksumAlgorithm) -> Outcome {
    json(&BeginUploadResponse::DirectPut {
        namespace_id: namespace_id(),
        upload_id: upload_id(),
        direct_put: DirectPutUpload {
            checksum_algorithm,
            access: ObjectTransferAccess::PresignedUrl {
                method: "PUT".to_owned(),
                url: "http://object.invalid/content".to_owned(),
                headers: std::collections::BTreeMap::new(),
                expires_at_ms: u64::MAX,
            },
        },
    })
}

fn begin_multipart() -> Outcome {
    json(&BeginUploadResponse::DirectMultipart {
        namespace_id: namespace_id(),
        upload_id: upload_id(),
        direct_multipart: DirectMultipartUpload {
            part_size_bytes: TEST_PART_BYTES,
            checksum_algorithm: ChecksumAlgorithm::Crc64nvme,
        },
    })
}

fn begin_proxied() -> Outcome {
    json(&BeginUploadResponse::ServiceProxied {
        namespace_id: namespace_id(),
        upload_id: upload_id(),
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

fn completed(content_ref: ContentRef) -> Outcome {
    json(&UploadSessionResponse {
        namespace_id: namespace_id(),
        upload_id: upload_id(),
        mode: UploadMode::DirectMultipart,
        status: UploadSessionStatus::Completed {
            completed_at_ms: 1,
            content_ref,
            content_token: None,
        },
    })
}

#[tokio::test]
async fn a_direct_put_uses_the_checksum_algorithm_returned_at_begin() {
    let first = Bytes::from_static(b"one ");
    let second = Bytes::from_static(b"pass");
    let source = PayloadSource::stream(
        futures::stream::iter([Ok(first.clone()), Ok(second.clone())]).boxed(),
    );
    let (source, observation) = observed_direct_put_source(source, ChecksumAlgorithm::Crc32c);
    let (mut stream, _) = source.into_stream();
    let mut sent = Vec::new();
    while let Some(chunk) = stream.next().await {
        sent.extend_from_slice(&chunk.expect("source chunk"));
    }

    let claim = observation
        .lock()
        .expect("direct PUT observation lock")
        .finish();
    assert_eq!(sent, b"one pass");
    assert_eq!(claim.size_bytes, sent.len() as u64);
    assert_eq!(claim.checksum, Checksum::crc32c(&sent));
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
    began: Mutex<Option<(UploadId, u64, ChecksumAlgorithm)>>,
    parts: Mutex<Vec<CompletedUploadPart>>,
}

impl RecordingJournal {
    fn resume(&self) -> MultipartUploadResume {
        let began = self.began.lock().expect("journal lock").clone();
        let (upload_id, part_size_bytes, checksum_algorithm) =
            began.expect("the session was opened");
        MultipartUploadResume {
            upload_id,
            part_size_bytes,
            checksum_algorithm,
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

    fn part_algorithms(&self) -> Vec<ChecksumAlgorithm> {
        self.parts
            .lock()
            .expect("journal lock")
            .iter()
            .map(|part| part.checksum.algorithm)
            .collect()
    }
}

impl MultipartUploadJournal for RecordingJournal {
    fn began(
        &self,
        upload_id: &UploadId,
        part_size_bytes: u64,
        checksum_algorithm: ChecksumAlgorithm,
    ) {
        *self.began.lock().expect("journal lock") =
            Some((upload_id.clone(), part_size_bytes, checksum_algorithm));
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
            PayloadSource::stream(
                futures::stream::once({
                    let payload = payload.clone();
                    async move { Ok(Bytes::from(payload)) }
                })
                .boxed(),
            ),
            &PutFileOptions::new(loonfs_test_support::test_actor()),
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
            &PutFileOptions::new(loonfs_test_support::test_actor()),
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

/// A resumed upload obeys the algorithm recorded beside its durable session,
/// even when it differs from the algorithm a new multipart session receives
/// today. The payload is still read once while that recorded digest is folded.
#[tokio::test]
async fn a_resumed_multipart_put_uses_the_recorded_checksum_algorithm() {
    let payload = payload(TEST_PAYLOAD_BYTES);
    let uploaded = ContentRef {
        kind: ContentRefKind::BlobV1,
        content_id: ContentId::generate(),
        size_bytes: payload.len() as u64,
        checksum: Checksum::crc32c(&payload),
    };
    let missing: Vec<u32> = (1..=TEST_PAYLOAD_PARTS).collect();
    let transport = test_transport::script(resumed_script(&missing, uploaded));
    let resume = MultipartUploadResume {
        upload_id: upload_id(),
        part_size_bytes: TEST_PART_BYTES,
        checksum_algorithm: ChecksumAlgorithm::Crc32c,
        parts: Vec::new(),
    };
    let (source, retention) = watched_source(&payload, TEST_PART_BYTES as usize);
    let journal = RecordingJournal::default();

    client()
        .put_file_stream_resumable(
            &spec(),
            source,
            &PutFileOptions::new(loonfs_test_support::test_actor()),
            &journal,
            Some(&resume),
        )
        .await
        .expect("the resumed multipart put should land");

    assert_eq!(journal.part_numbers(), missing);
    assert!(journal
        .part_algorithms()
        .into_iter()
        .all(|algorithm| algorithm == ChecksumAlgorithm::Crc32c));
    assert_eq!(retention.total_bytes(), TEST_PAYLOAD_BYTES as u64);
    let signing_waves = missing.len().div_ceil(DIRECT_MULTIPART_PARTS_IN_FLIGHT);
    assert_eq!(transport.attempts(), 1 + signing_waves + missing.len() + 2);
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
        .put_file_stream(
            &spec(),
            source,
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
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
        .put_file_stream(
            &spec(),
            source,
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
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
async fn a_small_streamed_source_proxies_against_the_advertised_cap() {
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
        .put_file_stream(
            &spec(),
            source,
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
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

/// A payload above the proxy limit uses direct PUT when multipart is absent.
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
        begin_direct_put(ChecksumAlgorithm::Sha256),
        Outcome::Success(Vec::new()),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(
            &spec(),
            source,
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("a payload past the proxy cap should take the direct write");

    let object_write = transport
        .sent()
        .into_iter()
        .find(|sent| sent.url.starts_with("http://object.invalid/"))
        .expect("the payload went straight to object storage");
    assert_eq!(object_write.body_bytes(), payload.len());
}

/// An unknown-length payload can use direct PUT without multipart support.
#[tokio::test]
async fn an_unknown_length_payload_past_the_proxy_cap_takes_direct_put() {
    let payload = payload(64 * 1024);
    let uploaded = content_ref(&payload);
    let (source, retention) = watched_source(&payload, 4 * 1024);
    assert_eq!(source.size_bytes(), None);

    let transport = test_transport::script(vec![
        // GCS supports CRC-32C direct PUTs but not multipart uploads.
        capabilities_for(Advertised {
            direct_put: Some(ChecksumAlgorithm::Crc32c),
            direct_multipart: false,
            proxy_max_bytes: Some(1_024),
            direct_put_max_bytes: Some(5 * 1024 * 1024 * 1024 * 1024),
        }),
        begin_direct_put(ChecksumAlgorithm::Crc32c),
        Outcome::Success(Vec::new()),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(
            &spec(),
            source,
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("an unmeasured payload past the proxy cap should take the direct write");

    let object_write = transport
        .sent()
        .into_iter()
        .find(|sent| sent.url.starts_with("http://object.invalid/"))
        .expect("the payload went straight to object storage");
    assert_eq!(object_write.body_bytes(), payload.len());
    assert_eq!(retention.total_bytes(), payload.len() as u64);
    assert!(
        retention.peak_live_bytes() <= 8 * 1024,
        "the upload accumulated the payload; peak was {}",
        retention.peak_live_bytes()
    );
}

/// An unknown length cannot be routed by its eventual size. Direct PUT is
/// selected without a preflight read and observes the size during transfer.
#[tokio::test]
async fn an_unknown_length_payload_takes_direct_put_without_a_preflight_read() {
    let payload = payload(1_000);
    let uploaded = content_ref(&payload);
    let (source, _) = watched_source(&payload, 512);
    assert_eq!(source.size_bytes(), None);

    let transport = test_transport::script(vec![
        capabilities_for(Advertised {
            direct_put: Some(ChecksumAlgorithm::Crc32c),
            direct_multipart: false,
            proxy_max_bytes: Some(4_096),
            direct_put_max_bytes: Some(5 * 1024 * 1024 * 1024 * 1024),
        }),
        begin_direct_put(ChecksumAlgorithm::Crc32c),
        Outcome::Success(Vec::new()),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(
            &spec(),
            source,
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("an unmeasured payload should take the direct write");

    let object_write = transport
        .sent()
        .into_iter()
        .find(|sent| sent.url.starts_with("http://object.invalid/"))
        .expect("the payload went straight to object storage");
    assert_eq!(object_write.body_bytes(), payload.len());
}

/// Direct PUT streams, counts, and hashes a payload in one pass.
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
        begin_direct_put(ChecksumAlgorithm::Sha256),
        Outcome::Success(Vec::new()),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(
            &spec(),
            source,
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("a large streamed direct put should land");

    assert_eq!(retention.total_bytes(), TEST_PAYLOAD_BYTES as u64);
    assert!(
        retention.peak_live_bytes() <= 2 * TEST_PART_BYTES,
        "the upload accumulated the payload; peak was {}",
        retention.peak_live_bytes()
    );

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
        object_write.largest_body_chunk() <= TEST_PART_BYTES as usize,
        "one body piece was larger than a source read: {} bytes",
        object_write.largest_body_chunk()
    );
}

#[tokio::test]
async fn a_capability_failure_does_not_downgrade_a_measured_upload_to_the_proxy() {
    let transport = test_transport::script([Outcome::Success(b"not json".to_vec())]);

    let error = client()
        .put_file_bytes(
            &spec(),
            b"payload",
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect_err("capability discovery failure must remain visible");

    assert!(matches!(error, ClientError::Json(_)), "{error:?}");
    assert_eq!(transport.attempts(), 1);
}

/// A file-backed direct PUT reads the source once without copying it to a
/// spool file.
#[tokio::test]
async fn a_file_backed_direct_put_reads_the_file_once_without_spooling_it() {
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
        begin_direct_put(ChecksumAlgorithm::Sha256),
        Outcome::Success(Vec::new()),
        completed(uploaded),
        commit_landed(),
    ]);

    client()
        .put_file_stream(
            &spec(),
            source,
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
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
        "a file-backed payload should not be copied"
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
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("payload.bin");
    std::fs::write(&path, b"0123456789").expect("write payload");
    let source = PayloadSource::open_file(path).await.expect("open payload");
    let head = request_head_for(source).await;

    assert!(head.contains("content-length: 10"), "{head}");
    assert!(
        !head.contains("chunked"),
        "a body of known length is not chunked: {head}"
    );
}

/// A source with unknown length uses chunked transfer encoding so the server
/// can enforce its incremental size limit.
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
