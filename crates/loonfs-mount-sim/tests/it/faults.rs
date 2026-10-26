//! Fault hooks: a dropped commit response, the order of a commit's response and its echo, held
//! feed pages, and failed content reads.

use crate::support::{client_for, fixture, put, request, Fixture};
use bytes::Bytes;
use futures::TryStreamExt as _;
use loonfs_client::{Client, NamespacePath, ReadRange};
use loonfs_mount_sim::{ContentReadFault, EchoOrder, InjectedFault, Operation};
use loonfs_types::api::v0::ListChangesResponse;
use loonfs_types::ChangeSeq;
use std::num::NonZeroU64;

/// Returns a client that makes one attempt per request, so a test sees each fault.
fn single_attempt_client(fixture: &Fixture) -> Client {
    let mut config = fixture.server.client_config();
    config.disable_transient_retry = true;
    client_for(&fixture.server, config)
}

async fn feed(client: &Client, after_seq: u64) -> ListChangesResponse {
    client
        .list_changes(&"demo".parse().expect("namespace ID"), ChangeSeq(after_seq))
        .next()
        .await
        .expect("a page")
        .expect("a feed page")
}

#[tokio::test]
async fn a_dropped_commit_response_leaves_the_commit_standing() {
    let fixture = fixture();
    let client = single_attempt_client(&fixture);
    let commit = request(vec![put("/f", b"f")]);
    fixture.server.drop_next_commit_response();
    client
        .commit(&fixture.namespace_id, &fixture.actor, &commit)
        .await
        .expect_err("the client sees a transport failure");
    let page = feed(&client, 0).await;
    assert_eq!(
        page.changes[0].commit_id, commit.commit_id,
        "the commit stands"
    );

    let retried = client
        .commit(&fixture.namespace_id, &fixture.actor, &commit)
        .await
        .expect("the retry replays");
    assert_eq!(retried.committed_seq, ChangeSeq(1));
    let faults: Vec<_> = fixture
        .server
        .trace()
        .iter()
        .map(|record| record.fault)
        .collect();
    assert_eq!(faults, [Some(InjectedFault::ResponseDropped), None, None]);
}

#[tokio::test]
async fn a_commits_echo_arrives_before_or_after_its_response_as_the_test_orders() {
    let fixture = fixture();
    let hold = fixture.server.order_next_commit_echo(EchoOrder::EchoFirst);
    let client = fixture.client.clone();
    let (namespace_id, actor) = (fixture.namespace_id.clone(), fixture.actor.clone());
    let pending = tokio::spawn(async move {
        client
            .commit(&namespace_id, &actor, &request(vec![put("/first", b"1")]))
            .await
    });
    hold.wait_for_held_responses(1).await;
    assert_eq!(
        feed(&fixture.client, 0).await.changes.len(),
        1,
        "the echo comes first"
    );
    hold.release();
    let first = pending
        .await
        .expect("the task")
        .expect("the held response arrives");

    let hold = fixture
        .server
        .order_next_commit_echo(EchoOrder::ResponseFirst);
    let second = fixture
        .client
        .commit(
            &fixture.namespace_id,
            &fixture.actor,
            &request(vec![put("/second", b"2")]),
        )
        .await
        .expect("the response comes first");
    let hidden = feed(&fixture.client, first.committed_seq.0).await;
    assert_eq!(
        (hidden.changes.len(), hidden.head_seq),
        (0, first.committed_seq)
    );
    hold.release();
    let shown = feed(&fixture.client, first.committed_seq.0).await;
    assert_eq!(shown.changes, [second]);
}

#[tokio::test]
async fn a_held_feed_page_reflects_the_head_when_the_server_read_it() {
    let fixture = fixture();
    let hold = fixture.server.hold_feed_pages();
    let client = fixture.client.clone();
    let pending = tokio::spawn(async move { feed(&client, 0).await });
    hold.wait_for_held_responses(1).await;
    fixture.commit(vec![put("/during", b"d")]);
    drop(hold);
    let page = pending.await.expect("the task");
    assert_eq!((page.head_seq, page.changes.len()), (ChangeSeq(0), 0));
    assert_eq!(
        feed(&fixture.client, 0).await.changes.len(),
        1,
        "the hold is over"
    );
}

#[tokio::test]
async fn a_failed_content_read_fails_the_way_the_fault_describes() {
    let fixture = fixture();
    fixture.commit(vec![put("/f", b"0123456789")]);
    let spec = NamespacePath::parse("demo", "/f").expect("namespace path");
    let client = single_attempt_client(&fixture);
    for fault in [
        ContentReadFault::TransportFailure,
        ContentReadFault::InterruptedBody { after_bytes: 4 },
    ] {
        fixture.server.fail_next_content_read(fault);
        client.read_file(&spec).await.expect_err("the read fails");
    }
    fixture
        .server
        .fail_next_content_read(ContentReadFault::CorruptedByte);
    let range = ReadRange {
        start_offset: 2,
        length: NonZeroU64::new(3),
    };
    let stream = client
        .read_file_range_stream(&spec, range)
        .await
        .expect("a range");
    let chunks: Vec<Bytes> = stream.body.try_collect().await.expect("the body");
    let bytes = chunks.concat();
    assert_eq!(
        bytes,
        [b'2' ^ 0xff, b'3', b'4'],
        "a range isn't verified, so the client sees the change"
    );

    // A client that retries reads the file whole after an interrupted body.
    fixture
        .server
        .fail_next_content_read(ContentReadFault::InterruptedBody { after_bytes: 4 });
    assert_eq!(
        fixture
            .client
            .read_file(&spec)
            .await
            .expect("a retried read"),
        b"0123456789"
    );
    let reads: Vec<_> = fixture
        .server
        .trace()
        .iter()
        .filter(|record| record.operation == Operation::GetFileContent)
        .map(|record| record.fault)
        .collect();
    assert_eq!(
        reads,
        [
            Some(InjectedFault::ContentRead(
                ContentReadFault::TransportFailure
            )),
            Some(InjectedFault::ContentRead(
                ContentReadFault::InterruptedBody { after_bytes: 4 }
            )),
            Some(InjectedFault::ContentRead(ContentReadFault::CorruptedByte)),
            Some(InjectedFault::ContentRead(
                ContentReadFault::InterruptedBody { after_bytes: 4 }
            )),
            None,
        ]
    );
}

#[tokio::test]
async fn hidden_commits_stay_out_of_the_feed_until_each_hold_is_released() {
    let fixture = fixture();
    let first = fixture
        .server
        .order_next_commit_echo(EchoOrder::ResponseFirst);
    let second = fixture
        .server
        .order_next_commit_echo(EchoOrder::ResponseFirst);
    for file in ["/a", "/b"] {
        fixture
            .client
            .commit(
                &fixture.namespace_id,
                &fixture.actor,
                &request(vec![put(file, b"x")]),
            )
            .await
            .expect("the response comes first");
    }
    second.release();
    let page = feed(&fixture.client, 0).await;
    assert_eq!(
        (page.changes.len(), page.head_seq),
        (0, ChangeSeq(0)),
        "`/a` is still hidden"
    );
    first.release();
    assert_eq!(feed(&fixture.client, 0).await.changes.len(), 2);
}
