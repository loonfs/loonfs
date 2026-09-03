//! Direct-download selection and verification tests.
//!
//! A file uses direct object-store access only when it exceeds the server's
//! proxy limit and the deployment advertises direct downloads.

use super::*;
use crate::transport::test_transport::{self, Outcome};
use loonfs_api::v0::ObjectTransferAccess;
use loonfs_api::{
    CapabilityDocument, ContentId, ContentRef, API_GROUP_FILESYSTEM_V0, PROTOCOL_VERSION,
};
use std::collections::BTreeMap;

/// Default maximum size of a proxied read response.
const DEFAULT_PROXY_CAP_BYTES: u64 = 256 * 1024 * 1024;
/// Test file larger than the default proxy limit.
const AUDIT_FILE_BYTES: u64 = 300 * 1024 * 1024;

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

/// Builds a capability response for the requested direct-read support.
fn capabilities(direct_get: bool, proxy_cap_bytes: Option<u64>) -> Outcome {
    let document = CapabilityDocument {
        protocol_version: PROTOCOL_VERSION.to_owned(),
        api_groups: vec![API_GROUP_FILESYSTEM_V0.to_owned()],
        features: BTreeMap::from([(FEATURE_DOWNLOADS_DIRECT_GET.to_owned(), direct_get)]),
        limits: proxy_cap_bytes
            .map(|cap| BTreeMap::from([(LIMIT_DOWNLOAD_MAX_CONTENT_BYTES.to_owned(), cap)]))
            .unwrap_or_default(),
    };
    Outcome::Success(serde_json::to_vec(&document).expect("serialize capability document"))
}

#[tokio::test]
async fn a_file_past_the_default_proxy_cap_takes_the_grant() {
    let client = client();
    let _guard = test_transport::script([capabilities(true, Some(DEFAULT_PROXY_CAP_BYTES))]);

    assert!(client
        .offers_direct_download(AUDIT_FILE_BYTES)
        .await
        .expect("capabilities"));
    // Cached document, so no second scripted response is needed.
    assert!(!client
        .offers_direct_download(DEFAULT_PROXY_CAP_BYTES)
        .await
        .expect("cached capabilities"));
    assert!(!client
        .offers_direct_download(1)
        .await
        .expect("cached capabilities"));
}

#[tokio::test]
async fn a_deployment_without_the_capability_never_takes_the_grant() {
    let client = client();
    let _guard = test_transport::script([capabilities(false, Some(DEFAULT_PROXY_CAP_BYTES))]);

    assert!(!client
        .offers_direct_download(AUDIT_FILE_BYTES)
        .await
        .expect("capabilities"));
}

#[tokio::test]
async fn a_deployment_that_advertises_no_cap_stays_proxied() {
    let client = client();
    let _guard = test_transport::script([capabilities(true, None)]);

    assert!(!client
        .offers_direct_download(AUDIT_FILE_BYTES)
        .await
        .expect("capabilities"));
}

#[tokio::test]
async fn a_capability_failure_is_not_reported_as_no_direct_download() {
    let client = client();
    let transport = test_transport::script([Outcome::Success(b"not json".to_vec())]);

    let result = client.offers_direct_download(AUDIT_FILE_BYTES).await;

    assert!(matches!(result, Err(ClientError::Json(_))), "{result:?}");
    assert_eq!(transport.attempts(), 1);
}

fn grant(content_ref: ContentRef, url: &str) -> BeginDownloadResponse {
    BeginDownloadResponse {
        namespace_id: NamespaceId::parse("demo").expect("namespace id"),
        path: AbsolutePath::parse("/big.bin").expect("absolute path"),
        revision_no: RevisionNo(1),
        content_ref,
        access: ObjectTransferAccess::PresignedUrl {
            method: "GET".to_owned(),
            url: url.to_owned(),
            headers: BTreeMap::new(),
            expires_at_ms: 0,
        },
    }
}

#[tokio::test]
async fn a_streamed_read_is_refused_when_the_bytes_are_not_what_the_grant_named() {
    let payload = b"the bytes the grant described".to_vec();
    let served = b"something else entirely, and a different length".to_vec();
    let content_ref = ContentRef::blob_v1(ContentId::generate(), &payload);
    let client = client();

    let _guard = test_transport::script([Outcome::Success(served)]);
    let mut sink = Vec::new();
    let error = client
        .download_via_presigned_url(
            &grant(content_ref, "http://example.invalid/object"),
            &mut sink,
        )
        .await
        .expect_err("bytes that are not the granted object");
    assert!(
        matches!(&error, ClientError::Protocol(message) if message.contains("grant named")),
        "unexpected error: {error}"
    );
}

/// A reference whose only full-object evidence is a CRC-32C, as a direct
/// transfer to Google Cloud Storage leaves behind.
fn crc32c_content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        kind: loonfs_api::ContentRefKind::BlobV1,
        content_id: ContentId::generate(),
        size_bytes: bytes.len() as u64,
        checksum: loonfs_api::Checksum::crc32c(bytes),
    }
}

#[tokio::test]
async fn a_crc32c_only_grant_verifies_the_bytes_it_receives() {
    let payload = b"transferred straight to the provider".to_vec();
    let content_ref = crc32c_content_ref(&payload);
    let client = client();

    // Same length, different bytes: only the CRC can tell the two apart.
    let served = b"transferred straight to the PROVIDER".to_vec();
    assert_eq!(served.len(), payload.len());
    let _guard =
        test_transport::script([Outcome::Success(payload.clone()), Outcome::Success(served)]);

    let mut sink = Vec::new();
    let written = client
        .download_via_presigned_url(
            &grant(content_ref.clone(), "http://example.invalid/object"),
            &mut sink,
        )
        .await
        .expect("granted object");
    assert_eq!(written, payload.len() as u64);
    assert_eq!(sink, payload);

    let mut sink = Vec::new();
    let error = client
        .download_via_presigned_url(
            &grant(content_ref, "http://example.invalid/object"),
            &mut sink,
        )
        .await
        .expect_err("bytes that are not the granted object");
    assert!(
        matches!(&error, ClientError::Protocol(message) if message.contains("grant named")),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_resumed_crc32c_download_folds_the_prefix_into_the_same_verdict() {
    let payload = b"the first half and then the second half".to_vec();
    let held = 10;
    let content_ref = crc32c_content_ref(&payload);
    let client = client();

    let _guard = test_transport::script([
        Outcome::Success(payload[held..].to_vec()),
        Outcome::Success(payload[held..].to_vec()),
    ]);
    let mut download = client
        .open_direct_download_at(
            &grant(content_ref.clone(), "http://example.invalid/object"),
            held as u64,
        )
        .await
        .expect("resumed grant");
    download.fold_resumed_prefix(&payload[..held]);
    while download.next_chunk().await.expect("chunk").is_some() {}

    // A prefix that is not the object's fails the whole download.
    let mut download = client
        .open_direct_download_at(
            &grant(content_ref, "http://example.invalid/object"),
            held as u64,
        )
        .await
        .expect("resumed grant");
    download.fold_resumed_prefix(&vec![0u8; held]);
    let error = loop {
        match download.next_chunk().await {
            Ok(Some(_)) => continue,
            #[allow(clippy::panic, reason = "the failure this test exists to catch")]
            Ok(None) => panic!("a prefix that is not the object's verified"),
            Err(error) => break error,
        }
    };
    assert!(
        matches!(&error, ClientError::Protocol(message) if message.contains("grant named")),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_streamed_read_writes_the_granted_object_and_reports_its_length() {
    let payload = b"exactly the bytes the grant described".to_vec();
    let content_ref = ContentRef::blob_v1(ContentId::generate(), &payload);
    let client = client();

    let _guard = test_transport::script([Outcome::Success(payload.clone())]);
    let mut sink = Vec::new();
    let written = client
        .download_via_presigned_url(
            &grant(content_ref, "http://example.invalid/object"),
            &mut sink,
        )
        .await
        .expect("granted object");

    assert_eq!(written, payload.len() as u64);
    assert_eq!(sink, payload);
}

#[tokio::test]
async fn a_resumed_download_asks_for_the_rest_and_verifies_the_whole_file() {
    let payload = b"the first half and then the second half".to_vec();
    let held = 10;
    let content_ref = ContentRef::blob_v1(ContentId::generate(), &payload);
    let client = client();

    let guard = test_transport::script([Outcome::Success(payload[held..].to_vec())]);
    let mut download = client
        .open_direct_download_at(
            &grant(content_ref, "http://example.invalid/object"),
            held as u64,
        )
        .await
        .expect("resumed grant");
    download.fold_resumed_prefix(&payload[..held]);
    let mut received = Vec::new();
    while let Some(chunk) = download.next_chunk().await.expect("chunk") {
        received.extend_from_slice(&chunk);
    }

    assert_eq!(
        received,
        payload[held..],
        "only the bytes past the resume point arrive"
    );
    let sent = guard.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].header("range"),
        Some("bytes=10-"),
        "the rest is asked for by range: {sent:?}"
    );
}

#[tokio::test]
async fn a_resume_is_refused_until_it_accounts_for_what_it_holds() {
    let payload = b"a whole object".to_vec();
    let content_ref = ContentRef::blob_v1(ContentId::generate(), &payload);
    let client = client();

    let guard = test_transport::script([Outcome::Success(payload.clone())]);
    let mut whole = client
        .open_direct_download(&grant(content_ref.clone(), "http://example.invalid/object"))
        .await
        .expect("grant");
    while whole.next_chunk().await.expect("chunk").is_some() {}
    assert_eq!(
        guard.sent()[0].header("range"),
        None,
        "a download of the whole object names no range"
    );
    drop(guard);

    let _guard = test_transport::script([Outcome::Success(payload[4..].to_vec())]);
    let mut resumed = client
        .open_direct_download_at(&grant(content_ref, "http://example.invalid/object"), 4)
        .await
        .expect("resumed grant");
    let error = resumed
        .next_chunk()
        .await
        .expect_err("the skipped bytes are still owed");
    assert!(
        matches!(&error, ClientError::Http(message) if message.contains("resumed at offset 4")),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_grant_that_does_not_authorize_a_read_is_refused_before_any_request() {
    let payload = b"unused".to_vec();
    let content_ref = ContentRef::blob_v1(ContentId::generate(), &payload);
    let mut grant = grant(content_ref, "http://example.invalid/object");
    let ObjectTransferAccess::PresignedUrl { method, .. } = &mut grant.access;
    *method = "PUT".to_owned();

    let mut sink = Vec::new();
    let error = client()
        .download_via_presigned_url(&grant, &mut sink)
        .await
        .expect_err("a write capability cannot serve a read");
    assert!(
        matches!(&error, ClientError::Protocol(message) if message.contains("presigned download method")),
        "unexpected error: {error}"
    );
    assert!(sink.is_empty());
}
