//! Direct-download selection and verification tests.
//!
//! Direct access is preferred whenever the deployment advertises it.

use super::*;
use crate::scripted_transport::{self, Outcome};
use loonfs_types::api::v0::ObjectTransferAccess;
use loonfs_types::{
    CapabilityDocument, ContentId, ContentRef, API_GROUP_FILESYSTEM_V0, PROTOCOL_VERSION,
};
use std::assert_matches;
use std::collections::BTreeMap;

fn client_for(transport: &crate::scripted_transport::ScriptedTransport) -> Client {
    Client::with_transport(
        ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        },
        transport.clone(),
    )
    .expect("valid client config")
}

/// Builds a capability response for the requested direct-read support.
fn capabilities(direct_get: bool) -> Outcome {
    let document = CapabilityDocument {
        protocol_version: PROTOCOL_VERSION.to_owned(),
        api_groups: vec![API_GROUP_FILESYSTEM_V0.to_owned()],
        features: BTreeMap::from([(FEATURE_DOWNLOADS_DIRECT_GET.to_owned(), direct_get)]),
        limits: BTreeMap::new(),
    };
    Outcome::Success(serde_json::to_vec(&document).expect("serialize capability document"))
}

#[tokio::test]
async fn direct_download_selection_uses_the_cached_capability() {
    let transport = scripted_transport::script([capabilities(true)]);
    let client = client_for(&transport);
    assert!(client.offers_direct_download().await.expect("capabilities"));
    assert!(client
        .offers_direct_download()
        .await
        .expect("cached capabilities"));
}

#[tokio::test]
async fn a_deployment_without_the_capability_never_takes_the_grant() {
    let transport = scripted_transport::script([capabilities(false)]);
    let client = client_for(&transport);

    assert!(!client.offers_direct_download().await.expect("capabilities"));
}

#[tokio::test]
async fn a_capability_failure_is_not_reported_as_no_direct_download() {
    let transport = scripted_transport::script([Outcome::Success(b"not json".to_vec())]);
    let client = client_for(&transport);

    let result = client.offers_direct_download().await;

    assert_matches!(result, Err(ClientError::Json(_)));
    assert_eq!(transport.attempts(), 1);
}

fn grant(content_ref: ContentRef, url: &str) -> CreateDownloadResponse {
    grant_from(content_ref, url, 0)
}

/// A grant of `[start_offset, size_bytes)`, signing that range as a server
/// does.
fn grant_from(content_ref: ContentRef, url: &str, start_offset: u64) -> CreateDownloadResponse {
    let headers = match content_ref.size_bytes {
        0 => BTreeMap::new(),
        size_bytes => BTreeMap::from([(
            "range".to_owned(),
            format!("bytes={start_offset}-{}", size_bytes - 1),
        )]),
    };
    CreateDownloadResponse {
        namespace_id: NamespaceId::parse("demo").expect("namespace id"),
        path: AbsolutePath::parse("/big.bin").expect("absolute path"),
        revision_no: RevisionNo(1),
        ranges: vec![DownloadRange {
            start_offset,
            length: content_ref.size_bytes - start_offset,
            access: ObjectTransferAccess::PresignedUrl {
                method: "GET".to_owned(),
                url: url.to_owned(),
                headers,
                expires_at_ms: 0,
            },
        }],
        content_ref,
    }
}

/// A grant from offset 0 with one range per length, each signing its own
/// span of one object.
fn grant_of_ranges(content_ref: ContentRef, url: &str, lengths: &[u64]) -> CreateDownloadResponse {
    let mut start_offset = 0;
    let ranges = lengths
        .iter()
        .enumerate()
        .map(|(index, &length)| {
            let range = DownloadRange {
                start_offset,
                length,
                access: ObjectTransferAccess::PresignedUrl {
                    method: "GET".to_owned(),
                    url: format!("{url}?signed={index}"),
                    headers: BTreeMap::from([(
                        "range".to_owned(),
                        format!("bytes={start_offset}-{}", start_offset + length - 1),
                    )]),
                    expires_at_ms: 0,
                },
            };
            start_offset += length;
            range
        })
        .collect();
    CreateDownloadResponse {
        namespace_id: NamespaceId::parse("demo").expect("namespace id"),
        path: AbsolutePath::parse("/big.bin").expect("absolute path"),
        revision_no: RevisionNo(1),
        ranges,
        content_ref,
    }
}

#[tokio::test]
async fn a_streamed_read_is_refused_when_the_bytes_are_not_what_the_grant_named() {
    let payload = b"the bytes the grant described".to_vec();
    let served = b"something else entirely, and a different length".to_vec();
    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        &payload,
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );

    let transport = scripted_transport::script([Outcome::Success(served)]);
    let client = client_for(&transport);
    let mut sink = Vec::new();
    let error = client
        .download_via_presigned_url(
            &grant(content_ref, "http://example.invalid/object"),
            &mut sink,
        )
        .await
        .expect_err("bytes that are not the granted object");
    assert_matches!(
        &error,
        ClientError::Protocol(message) if message.contains("grant named"),
        "unexpected error: {error}"
    );
}

/// A reference whose only full-object evidence is a CRC-32C, as a direct
/// transfer to Google Cloud Storage leaves behind.
fn crc32c_content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        kind: loonfs_types::ContentRefKind::BlobV1,
        owner_namespace_id: loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        content_id: ContentId::generate(),
        size_bytes: bytes.len() as u64,
        checksum: loonfs_types::Checksum::crc32c(bytes),
    }
}

#[tokio::test]
async fn a_crc32c_only_grant_verifies_the_bytes_it_receives() {
    let payload = b"transferred straight to the provider".to_vec();
    let content_ref = crc32c_content_ref(&payload);

    // Same length, different bytes: only the CRC can tell the two apart.
    let served = b"transferred straight to the PROVIDER".to_vec();
    assert_eq!(served.len(), payload.len());
    let transport =
        scripted_transport::script([Outcome::Success(payload.clone()), Outcome::Success(served)]);
    let client = client_for(&transport);

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
    assert_matches!(
        &error,
        ClientError::Protocol(message) if message.contains("grant named"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_resumed_crc32c_download_folds_the_prefix_into_the_same_verdict() {
    let payload = b"the first half and then the second half".to_vec();
    let held = 10;
    let content_ref = crc32c_content_ref(&payload);

    let transport = scripted_transport::script([
        Outcome::Success(payload[held..].to_vec()),
        Outcome::Success(payload[held..].to_vec()),
    ]);
    let client = client_for(&transport);
    let resumed = grant_from(content_ref, "http://example.invalid/object", held as u64);
    let mut download = client
        .open_direct_download(&resumed)
        .await
        .expect("resumed grant");
    download
        .fold_resumed_prefix(&payload[..held])
        .expect("fold the held prefix");
    while download.next_chunk().await.expect("chunk").is_some() {}

    // A prefix that is not the object's fails the whole download.
    let mut download = client
        .open_direct_download(&resumed)
        .await
        .expect("resumed grant");
    download
        .fold_resumed_prefix(&vec![0u8; held])
        .expect("a prefix of the right length is accepted");
    let error = loop {
        match download.next_chunk().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("a prefix that is not the object's verified"),
            Err(error) => break error,
        }
    };
    assert_matches!(
        &error,
        ClientError::Protocol(message) if message.contains("grant named"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_resumed_download_that_fails_its_checksum_never_reports_completion() {
    let payload = b"the first half and then the second half".to_vec();
    let held = 10;
    let transport = scripted_transport::script([Outcome::Success(payload[held..].to_vec())]);
    let client = client_for(&transport);
    let resumed = grant_from(
        crc32c_content_ref(&payload),
        "http://example.invalid/object",
        held as u64,
    );
    let mut download = client
        .open_direct_download(&resumed)
        .await
        .expect("resumed grant");
    download
        .fold_resumed_prefix(&vec![0u8; held])
        .expect("a prefix of the right length is accepted");
    download
        .next_chunk()
        .await
        .expect("the fetched bytes")
        .expect("a chunk before the end");
    for _ in 0..2 {
        let result = download.next_chunk().await;
        let Err(ClientError::Protocol(message)) = &result else {
            panic!("expected the checksum failure, got {result:?}");
        };
        assert!(message.contains("grant named"), "{message}");
    }
}

#[tokio::test]
async fn a_failed_range_open_is_returned_by_every_later_read() {
    let payload = b"aaaabbbbcccc".to_vec();
    let grant = grant_of_ranges(
        crc32c_content_ref(&payload),
        "http://example.invalid/object",
        &[4, 4, 4],
    );
    let transport = scripted_transport::script([
        Outcome::Success(b"aaaa".to_vec()),
        Outcome::TransportFailure,
        Outcome::Success(b"cccc".to_vec()),
    ]);
    let client = Client::with_transport(
        ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: true,
            ca_cert_path: None,
        },
        transport.clone(),
    )
    .expect("valid client config");
    let mut download = client
        .open_direct_download(&grant)
        .await
        .expect("open the grant");
    let first = download.next_chunk().await.expect("the first range");
    assert_eq!(first.as_deref(), Some(&b"aaaa"[..]));

    let failed = download
        .next_chunk()
        .await
        .expect_err("the second range fails to open")
        .to_string();
    for _ in 0..2 {
        let later = download
            .next_chunk()
            .await
            .expect_err("the stream stays failed instead of reading the third range");
        assert_eq!(later.to_string(), failed);
    }
}

#[tokio::test]
async fn a_streamed_read_writes_the_granted_object_and_reports_its_length() {
    let payload = b"exactly the bytes the grant described".to_vec();
    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        &payload,
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );

    let transport = scripted_transport::script([Outcome::Success(payload.clone())]);
    let client = client_for(&transport);
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
async fn ranges_up_to_the_window_are_requested_while_the_current_one_is_read() {
    let payload: Vec<u8> = (0u8..50).collect();
    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        &payload,
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let grant = grant_of_ranges(content_ref, "http://example.invalid/object", &[10; 5]);
    let transport = scripted_transport::script(grant.ranges.iter().map(|range| {
        let start = loonfs_types::usize_from_u64(range.start_offset);
        Outcome::Success(
            payload[start..start + loonfs_types::usize_from_u64(range.length)].to_vec(),
        )
    }));
    let client = client_for(&transport);

    let mut download = client
        .open_direct_download(&grant)
        .await
        .expect("open the grant");
    assert_eq!(transport.attempts(), 1);
    let first = download.next_chunk().await.expect("first range");
    assert_eq!(first.as_deref(), Some(&payload[..10]));
    assert_eq!(transport.attempts(), RANGE_RESPONSES_IN_FLIGHT);
    let second = download.next_chunk().await.expect("second range");
    assert_eq!(second.as_deref(), Some(&payload[10..20]));
    assert_eq!(transport.attempts(), RANGE_RESPONSES_IN_FLIGHT + 1);
    let mut rest = Vec::new();
    while let Some(chunk) = download.next_chunk().await.expect("verified download") {
        rest.extend_from_slice(&chunk);
    }

    assert_eq!(rest, payload[20..]);
    let urls: Vec<String> = transport.sent().into_iter().map(|sent| sent.url).collect();
    let expected: Vec<String> = (0..5)
        .map(|index| format!("http://example.invalid/object?signed={index}"))
        .collect();
    assert_eq!(urls, expected);
}

#[tokio::test(start_paused = true)]
async fn a_held_next_range_does_not_time_out_while_the_current_one_is_read() {
    let payload = b"abcde".to_vec();
    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        &payload,
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let grant = grant_of_ranges(content_ref, "http://example.invalid/object", &[3, 2]);
    let clock = std::sync::Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let service = {
        let clock = clock.clone();
        tower::service_fn(move |request: http::Request<crate::Body>| {
            let clock = clock.clone();
            async move {
                if request.uri().query() != Some("signed=0") {
                    return Ok(http::Response::new(crate::Body::from(Bytes::from_static(
                        b"de",
                    ))));
                }
                let chunks = futures::stream::iter(*b"abc").then(move |byte| {
                    let clock = clock.clone();
                    async move {
                        clock.advance_ms(30_000);
                        tokio::time::advance(std::time::Duration::from_secs(30)).await;
                        Ok::<_, std::io::Error>(Bytes::from(vec![byte]))
                    }
                });
                Ok::<_, crate::TransportError>(http::Response::new(crate::Body::from_stream(
                    chunks,
                )))
            }
        })
    };
    let mut client = Client::with_transport(
        ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        },
        service,
    )
    .expect("valid client config");
    client.timer = clock.clone();

    let mut download = client
        .open_direct_download(&grant)
        .await
        .expect("open the grant");
    let mut received = Vec::new();
    while let Some(chunk) = download.next_chunk().await.expect("verified download") {
        received.extend_from_slice(&chunk);
    }

    assert_eq!(received, payload);
    assert_eq!(clock.now_ms(), 90_000);
}

#[tokio::test]
async fn a_resume_takes_a_new_grant_and_sends_the_headers_it_returns() {
    let payload = b"the first half and then the second half".to_vec();
    let held: usize = 10;
    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        &payload,
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let issued = grant_from(
        content_ref,
        "http://example.invalid/object?signed",
        held as u64,
    );

    let transport = scripted_transport::script([
        Outcome::Success(serde_json::to_vec(&issued).expect("encode grant")),
        Outcome::Success(payload[held..].to_vec()),
    ]);
    let client = client_for(&transport);
    let spec = NamespacePath::parse("demo", "/big.bin").expect("namespace path");
    let resumed = client
        .create_download_with_options(
            &spec,
            &DownloadOptions {
                start_offset: held as u64,
                ..DownloadOptions::default()
            },
        )
        .await
        .expect("a grant from the resume point");
    let mut download = client
        .open_direct_download(&resumed)
        .await
        .expect("open the new grant");
    download
        .fold_resumed_prefix(&payload[..held])
        .expect("fold the held prefix");
    let mut received = Vec::new();
    while let Some(chunk) = download.next_chunk().await.expect("chunk") {
        received.extend_from_slice(&chunk);
    }

    assert_eq!(
        received,
        payload[held..],
        "only the bytes past the resume point arrive"
    );
    let sent = transport.sent();
    assert_eq!(sent.len(), 2);
    let request: serde_json::Value =
        serde_json::from_slice(&sent[0].body).expect("grant request body");
    assert_eq!(request["start_offset"], held, "{request}");
    assert_eq!(sent[1].url, "http://example.invalid/object?signed");
    assert_eq!(
        sent[1].header("range"),
        Some("bytes=10-38"),
        "the grant's signed range is sent as given: {sent:?}"
    );
}

#[tokio::test]
async fn a_resumed_inode_download_takes_a_new_grant_from_its_offset() {
    let payload = b"the first half and then the second half".to_vec();
    let held: usize = 10;
    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        &payload,
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let path_grant = grant_from(content_ref, "http://example.invalid/object", held as u64);
    let inode_grant = CreateDownloadByInodeResponse {
        namespace_id: path_grant.namespace_id,
        inode_id: loonfs_types::InodeId(1),
        revision_no: path_grant.revision_no,
        content_ref: path_grant.content_ref,
        ranges: path_grant.ranges,
    };

    let transport = scripted_transport::script([
        Outcome::Success(serde_json::to_vec(&inode_grant).expect("encode grant")),
        Outcome::Success(payload[held..].to_vec()),
    ]);
    let client = client_for(&transport);
    let resumed = client
        .create_revision_download_by_inode_with_options(
            &NamespaceId::parse("demo").expect("namespace id"),
            loonfs_types::InodeId(1),
            RevisionNo(1),
            &DownloadOptions {
                start_offset: held as u64,
                ..DownloadOptions::default()
            },
        )
        .await
        .expect("a grant from the resume point");
    let mut download = client
        .open_direct_download_by_inode(&resumed)
        .await
        .expect("open the new grant");
    download
        .fold_resumed_prefix(&payload[..held])
        .expect("fold the held prefix");
    let mut received = Vec::new();
    while let Some(chunk) = download.next_chunk().await.expect("chunk") {
        received.extend_from_slice(&chunk);
    }

    assert_eq!(
        received,
        payload[held..],
        "only the bytes past the resume point arrive"
    );
    let sent = transport.sent();
    assert_eq!(sent.len(), 2);
    assert!(sent[0].url.ends_with("/revisions/1/downloads"), "{sent:?}");
    let request: serde_json::Value =
        serde_json::from_slice(&sent[0].body).expect("grant request body");
    assert_eq!(request, serde_json::json!({"start_offset": held}));
    assert_eq!(
        sent[1].header("range"),
        Some("bytes=10-38"),
        "the grant's signed range is sent as given: {sent:?}"
    );
}

/// Checks that each grant request sends `snapshot_id` for a snapshot, the
/// consistency fields of a live read that sets a consistency, and none of
/// them for a live read that sets none, so the server's default applies.
#[tokio::test]
async fn grants_send_only_the_fields_of_what_they_read_at() {
    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        b"report",
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let path_grant = grant(content_ref, "http://example.invalid/object");
    let inode_grant = CreateDownloadByInodeResponse {
        namespace_id: path_grant.namespace_id.clone(),
        inode_id: loonfs_types::InodeId(2),
        revision_no: path_grant.revision_no,
        content_ref: path_grant.content_ref.clone(),
        ranges: path_grant.ranges.clone(),
    };
    let snapshot_id =
        PinId::parse("pin_00000000000000000001-0000000000000002").expect("snapshot ID");
    let cases = [
        (ReadAt::Live(None), serde_json::json!({})),
        (
            ReadAt::Live(Some(Consistency::Strong)),
            serde_json::json!({"consistency": "strong"}),
        ),
        (
            ReadAt::Live(Some(Consistency::Eventual {
                max_staleness_ms: Some(2_500),
            })),
            serde_json::json!({"consistency": "eventual", "max_staleness_ms": 2500}),
        ),
        (
            ReadAt::Snapshot(snapshot_id.clone()),
            serde_json::json!({"snapshot_id": snapshot_id}),
        ),
    ];
    let transport = scripted_transport::script(cases.iter().flat_map(|_| {
        [
            Outcome::Success(serde_json::to_vec(&path_grant).expect("encode grant")),
            Outcome::Success(serde_json::to_vec(&inode_grant).expect("encode grant")),
            Outcome::Success(serde_json::to_vec(&inode_grant).expect("encode grant")),
        ]
    }));
    let client = client_for(&transport);
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let spec = NamespacePath::parse("demo", "/report.txt").expect("namespace path");
    let mut expected = Vec::new();
    for (at, fields) in cases {
        let options = DownloadOptions {
            at,
            ..DownloadOptions::default()
        };
        client
            .create_download_with_options(&spec, &options)
            .await
            .expect("path grant");
        client
            .create_download_by_inode_with_options(&namespace_id, InodeId(2), &options)
            .await
            .expect("inode grant");
        client
            .create_revision_download_by_inode_with_options(
                &namespace_id,
                InodeId(2),
                RevisionNo(1),
                &options,
            )
            .await
            .expect("revision grant");
        let mut path_fields = fields.clone();
        path_fields["path"] = serde_json::json!("/report.txt");
        expected.extend([path_fields, fields.clone(), fields]);
    }
    let bodies: Vec<serde_json::Value> = transport
        .sent()
        .iter()
        .map(|request| serde_json::from_slice(&request.body).expect("grant request body"))
        .collect();
    assert_eq!(bodies, expected);
}

#[tokio::test]
async fn a_resume_is_refused_until_it_accounts_for_what_it_holds() {
    let payload = b"a whole object".to_vec();
    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        &payload,
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );

    let transport = scripted_transport::script([Outcome::Success(payload[4..].to_vec())]);
    let client = client_for(&transport);
    let mut resumed = client
        .open_direct_download(&grant_from(content_ref, "http://example.invalid/object", 4))
        .await
        .expect("resumed grant");
    let error = resumed
        .next_chunk()
        .await
        .expect_err("the skipped bytes are still owed");
    assert_matches!(
        &error,
        ClientError::Http(message) if message.contains("resumed at offset 4"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_grant_that_does_not_authorize_a_read_is_refused_before_any_request() {
    let payload = b"unused".to_vec();
    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        &payload,
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let mut grant = grant(content_ref, "http://example.invalid/object");
    let ObjectTransferAccess::PresignedUrl { method, .. } = &mut grant.ranges[0].access;
    *method = "PUT".to_owned();

    let mut sink = Vec::new();
    let error = client_for(&scripted_transport::script([]))
        .download_via_presigned_url(&grant, &mut sink)
        .await
        .expect_err("a write capability cannot serve a read");
    assert_matches!(
        &error,
        ClientError::Protocol(message) if message.contains("presigned download method"),
        "unexpected error: {error}"
    );
    assert!(sink.is_empty());
}

#[tokio::test]
async fn a_grant_from_past_the_start_is_refused_before_any_request() {
    let payload = b"ten bytes!".to_vec();
    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        &payload,
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    // The grant signs `range: bytes=5-9`.
    let resumed = grant_from(content_ref, "http://example.invalid/object", 5);

    let transport = scripted_transport::script([Outcome::Success(payload[5..].to_vec())]);
    let mut sink = Vec::new();
    let error = client_for(&transport)
        .download_via_presigned_url(&resumed, &mut sink)
        .await
        .expect_err("the helper takes a grant from offset 0");
    assert_matches!(
        &error,
        ClientError::Protocol(message)
            if message.contains("offset 5") && message.contains("open_direct_download"),
        "unexpected error: {error}"
    );
    assert_eq!(transport.attempts(), 0);
    assert!(sink.is_empty());
}

#[tokio::test]
async fn ranges_verify_the_whole_revision_and_resume_inside_the_second_object() {
    let payload = b"firstsecond";
    let reference = crc32c_content_ref(payload);
    let mut whole = grant(reference.clone(), "http://example.invalid/first");
    whole.ranges[0].length = 5;
    let ObjectTransferAccess::PresignedUrl { headers, .. } = &mut whole.ranges[0].access;
    headers.insert("range".to_owned(), "bytes=7-11".to_owned());
    let mut second = grant_from(reference, "http://example.invalid/second", 5)
        .ranges
        .remove(0);
    let ObjectTransferAccess::PresignedUrl { headers, .. } = &mut second.access;
    headers.insert("range".to_owned(), "bytes=0-5".to_owned());
    whole.ranges.push(second);
    let mut resumed = whole.clone();
    resumed.ranges.remove(0);
    resumed.ranges[0].start_offset = 7;
    resumed.ranges[0].length = 4;
    let ObjectTransferAccess::PresignedUrl { headers, .. } = &mut resumed.ranges[0].access;
    headers.insert("range".to_owned(), "bytes=2-5".to_owned());
    let transport = scripted_transport::script([
        Outcome::Success(b"first".to_vec()),
        Outcome::Success(b"second".to_vec()),
        Outcome::Success(b"cond".to_vec()),
        Outcome::Success(b"firs".to_vec()),
        Outcome::Success(b"second".to_vec()),
        Outcome::Success(b"firsts".to_vec()),
        Outcome::Success(b"second".to_vec()),
        Outcome::Success(b"first".to_vec()),
        Outcome::Success(b"SECOND".to_vec()),
    ]);
    let client = client_for(&transport);
    let mut bytes = Vec::new();
    client
        .download_via_presigned_url(&whole, &mut bytes)
        .await
        .expect("whole revision");
    assert_eq!(bytes, payload);
    let mut stream = client.open_direct_download(&resumed).await.expect("resume");
    stream
        .fold_resumed_prefix(&payload[..7])
        .expect("fold the held prefix");
    let mut suffix = Vec::new();
    while let Some(chunk) = stream.next_chunk().await.expect("verified suffix") {
        suffix.extend_from_slice(&chunk);
    }
    assert_eq!(suffix, b"cond");
    let sent = transport.sent();
    assert_eq!(sent[0].header("range"), Some("bytes=7-11"));
    assert_eq!(sent[1].header("range"), Some("bytes=0-5"));
    assert_eq!(sent[2].header("range"), Some("bytes=2-5"));
    for expected_attempts in [5, 7, 9] {
        let error = client
            .download_via_presigned_url(&whole, &mut Vec::new())
            .await
            .expect_err("invalid range bytes");
        assert_matches!(error, ClientError::Protocol(_));
        assert_eq!(transport.attempts(), expected_attempts);
    }
}
