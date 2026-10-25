//! Ranged reads: the `Range` header they send, and the responses they accept.

use super::*;
use crate::scripted_transport::{self, Outcome, ScriptedTransport};
use std::assert_matches;
use std::num::NonZeroU64;

fn client_for(transport: &ScriptedTransport) -> Client {
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

fn range(start_offset: u64, length: Option<u64>) -> ReadRange {
    ReadRange {
        start_offset,
        length: length.map(|length| NonZeroU64::new(length).expect("nonzero length")),
    }
}

async fn body_bytes(body: PayloadStream) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut body = body;
    while let Some(chunk) = body.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

/// Checks that a ranged read by path and by inode sends the `Range` header
/// beside the read's usual query, and returns the range that the server's
/// `Content-Range` header describes.
#[tokio::test]
async fn a_ranged_read_sends_one_range_and_returns_what_the_server_sent() {
    let transport = scripted_transport::script([
        Outcome::PartialContent("bytes 2-5/10".to_owned(), b"cdef".to_vec()),
        Outcome::PartialContent("bytes 7-9/10".to_owned(), b"hij".to_vec()),
    ]);
    let client = client_for(&transport);
    let spec = NamespacePath::parse("demo", "/file.txt").expect("namespace path");
    let snapshot_id =
        PinId::parse("pin_00000000000000000001-0000000000000002").expect("snapshot ID");

    let by_path = client
        .read_file_range_stream(&spec, range(2, Some(4)))
        .await
        .expect("ranged read by path");
    assert_eq!(
        (by_path.start_offset, by_path.length, by_path.size_bytes),
        (2, 4, 10)
    );
    assert_eq!(body_bytes(by_path.body).await.expect("body"), b"cdef");
    let by_inode = client
        .read_file_range_stream_by_inode_with_options(
            spec.namespace(),
            InodeId(42),
            range(7, None),
            &ReadFileOptions {
                at: ReadAt::Snapshot(snapshot_id.clone()),
            },
        )
        .await
        .expect("ranged read by inode");
    assert_eq!(
        (by_inode.start_offset, by_inode.length, by_inode.size_bytes),
        (7, 3, 10)
    );
    assert_eq!(body_bytes(by_inode.body).await.expect("body"), b"hij");

    let sent = transport.sent();
    assert_eq!(
        sent[0].url,
        "http://example.invalid/v0/namespaces/demo/filesystem/content?path=%2Ffile.txt"
    );
    assert_eq!(sent[0].header("range"), Some("bytes=2-5"));
    assert_eq!(
        sent[1].url,
        format!(
            "http://example.invalid/v0/namespaces/demo/inodes/ino_42/content?snapshot_id={snapshot_id}"
        )
    );
    assert_eq!(sent[1].header("range"), Some("bytes=7-"));
}

/// Checks that a ranged read refuses a response that isn't the requested
/// range (a whole file, a different range, or a body of the wrong length),
/// instead of returning bytes from the wrong offset.
#[tokio::test]
async fn a_ranged_read_refuses_a_response_other_than_its_range() {
    let spec = NamespacePath::parse("demo", "/file.txt").expect("namespace path");
    for (outcome, requested) in [
        (Outcome::Success(b"abcdefghij".to_vec()), range(2, Some(4))),
        (
            Outcome::PartialContent("bytes 0-3/10".to_owned(), b"abcd".to_vec()),
            range(2, Some(4)),
        ),
        (
            Outcome::PartialContent("bytes 2-4/10".to_owned(), b"cde".to_vec()),
            range(2, Some(4)),
        ),
        (
            Outcome::PartialContent("bytes 2-5/*".to_owned(), b"cdef".to_vec()),
            range(2, Some(4)),
        ),
    ] {
        let transport = scripted_transport::script([outcome]);
        let error = client_for(&transport)
            .read_file_range_stream(&spec, requested)
            .await
            .expect_err("a response that isn't the range");
        assert_matches!(error, ClientError::Protocol(_));
    }

    for body in [&b"cde"[..], &b"cdefg"[..]] {
        let transport = scripted_transport::script([Outcome::PartialContent(
            "bytes 2-5/10".to_owned(),
            body.to_vec(),
        )]);
        let read = client_for(&transport)
            .read_file_range_stream(&spec, range(2, Some(4)))
            .await
            .expect("the headers describe the range");
        body_bytes(read.body)
            .await
            .expect_err("a body that isn't the range's length");
    }
}
