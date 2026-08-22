//! The served attribute surface: what `include_attributes` projects, what the
//! server does with the parameter, and that the client round-trips it.

#![allow(clippy::panic)]
// A transport failure is not an outcome under test; it panics with what it saw.

use crate::common::http_split_support::*;
use crate::common::{collect_path_entries, start_server};
use loonfs_api::AttributeRevisionNo;
use loonfs_client::{
    ListPathEntriesOptions, NamespacePath, PutFileOptions, StatPathOptions, UpdateAttributesOptions,
};
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::{attribute_key, attribute_text, namespace_id};
use std::collections::BTreeMap;
use tempfile::tempdir;

fn path(absolute_path: &str) -> NamespacePath {
    NamespacePath::parse("demo", absolute_path).expect("namespace path")
}

/// A namespace holding `/docs/report.txt` with one attribute, and
/// `/docs/notes.txt` with none.
async fn served_namespace(harness: &crate::common::TestServer) {
    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    for absolute_path in ["/docs/report.txt", "/docs/notes.txt"] {
        harness
            .client
            .put_file_bytes(
                &path(absolute_path),
                b"body",
                &PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put file");
    }
    harness
        .client
        .update_attributes(
            &path("/docs/report.txt"),
            &UpdateAttributesOptions {
                set: BTreeMap::from([(attribute_key("owner"), attribute_text("platform"))]),
                ..UpdateAttributesOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("annotate");
}

fn get_json(harness: &crate::common::TestServer, query: &str) -> serde_json::Value {
    let response = raw_agent()
        .get(&format!("{}/v0/namespaces/demo{query}", harness.server_url))
        .set("authorization", "Bearer test-token")
        .call()
        .expect("read request");
    serde_json::from_reader(response.into_reader()).expect("response JSON")
}

fn get_status(harness: &crate::common::TestServer, query: &str) -> u16 {
    match raw_agent()
        .get(&format!("{}/v0/namespaces/demo{query}", harness.server_url))
        .set("authorization", "Bearer test-token")
        .call()
    {
        Ok(response) => response.status(),
        Err(ureq::Error::Status(status, _)) => status,
        Err(error) => panic!("read request failed: {error}"),
    }
}

/// The required attribute projection siblings are either both present or absent.
fn assert_projection(entry: &serde_json::Value, projected: bool) {
    assert_eq!(
        entry.get("attributes").is_some(),
        projected,
        "wrong `attributes` projection: {entry}"
    );
    assert_eq!(
        entry.get("attributes_revision_no").is_some(),
        projected,
        "wrong `attributes_revision_no` projection: {entry}"
    );
}

/// Stat projects attributes by default and drops the siblings on request;
/// listing does the opposite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_reads_project_flat_attribute_siblings_together() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-attributes",
        "http-attributes",
    ))
    .await;
    served_namespace(&harness).await;

    // Stat's default is on: the annotated inode carries its map, and the
    // bare one carries the cleared state at revision 0 rather than nothing.
    let annotated = get_json(&harness, "/filesystem/entry?path=%2Fdocs%2Freport.txt");
    assert_projection(&annotated, true);
    assert_eq!(annotated["attributes"]["owner"], "platform");
    assert_eq!(annotated["attributes_revision_no"], 1);
    assert!(annotated.pointer("/attributes/attributes").is_none());

    let bare = get_json(&harness, "/filesystem/entry?path=%2Fdocs%2Fnotes.txt");
    assert_projection(&bare, true);
    assert_eq!(bare["attributes"], serde_json::json!({}));
    assert_eq!(bare["attributes_revision_no"], 0);

    let opted_out = get_json(
        &harness,
        "/filesystem/entry?path=%2Fdocs%2Freport.txt&include_attributes=false",
    );
    assert_projection(&opted_out, false);

    // Listing's default is off.
    let listing = get_json(&harness, "/filesystem/entries?path=%2Fdocs");
    let entries = listing["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 2);
    for entry in entries {
        assert_projection(entry, false);
    }

    let projected = get_json(
        &harness,
        "/filesystem/entries?path=%2Fdocs&include_attributes=true",
    );
    let entries = projected["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 2);
    for entry in entries {
        assert_projection(entry, true);
        match entry["path"].as_str().expect("path") {
            "/docs/report.txt" => {
                assert_eq!(entry["attributes"]["owner"], "platform");
                assert_eq!(entry["attributes_revision_no"], 1);
            }
            _ => {
                assert_eq!(entry["attributes"], serde_json::json!({}));
                assert_eq!(entry["attributes_revision_no"], 0);
            }
        }
        assert!(entry.pointer("/attributes/attributes").is_none());
    }
}

/// The parameter accepts the two spellings it documents and rejects the rest,
/// on both read endpoints.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_include_attributes_accepts_true_and_false_and_nothing_else() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-attributes-parse",
        "http-attributes-parse",
    ))
    .await;
    served_namespace(&harness).await;

    for (endpoint, target) in [
        ("/filesystem/entry", "%2Fdocs%2Freport.txt"),
        ("/filesystem/entries", "%2Fdocs"),
    ] {
        for value in ["true", "false"] {
            assert_eq!(
                get_status(
                    &harness,
                    &format!("{endpoint}?path={target}&include_attributes={value}")
                ),
                200
            );
        }
        for value in ["yes", "1", "TRUE", ""] {
            assert_eq!(
                get_status(
                    &harness,
                    &format!("{endpoint}?path={target}&include_attributes={value}")
                ),
                400,
                "{endpoint} accepted include_attributes={value}"
            );
        }
    }

    // An explicit value equal to the endpoint's default answers exactly what
    // omitting the parameter answers.
    assert_eq!(
        get_json(
            &harness,
            "/filesystem/entry?path=%2Fdocs%2Freport.txt&include_attributes=true"
        ),
        get_json(&harness, "/filesystem/entry?path=%2Fdocs%2Freport.txt")
    );
    assert_eq!(
        get_json(
            &harness,
            "/filesystem/entries?path=%2Fdocs&include_attributes=false"
        ),
        get_json(&harness, "/filesystem/entries?path=%2Fdocs")
    );
}

/// The client sends the options it was given, and its defaults are the
/// server's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_client_round_trips_the_read_options() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-attributes-client",
        "http-attributes-client",
    ))
    .await;
    served_namespace(&harness).await;

    // The plain methods carry each endpoint's default, and the raw-HTTP test
    // above already pins what those defaults answer. What only the client can
    // prove is that a non-default option reaches the wire, so that is what
    // this checks: each surface, once, against its own default.
    let stat = harness
        .client
        .stat_path(&path("/docs/report.txt"), &Default::default())
        .await
        .expect("stat");
    assert_eq!(
        stat.attributes
            .as_ref()
            .and_then(|projection| projection.attributes.get(&attribute_key("owner")))
            .cloned(),
        Some(attribute_text("platform"))
    );
    assert_eq!(
        stat.attributes
            .as_ref()
            .map(|projection| projection.attributes_revision_no),
        Some(AttributeRevisionNo(1))
    );

    let without = harness
        .client
        .stat_path(
            &path("/docs/report.txt"),
            &StatPathOptions {
                include_attributes: false,
            },
        )
        .await
        .expect("stat without attributes");
    assert!(without.attributes.is_none());

    let listing = collect_path_entries(&harness.client, &path("/docs"), &Default::default())
        .await
        .expect("list");
    assert!(listing
        .entries
        .iter()
        .all(|entry| entry.attributes.is_none()));

    let projected = collect_path_entries(
        &harness.client,
        &path("/docs"),
        &ListPathEntriesOptions {
            include_attributes: true,
        },
    )
    .await
    .expect("list with attributes");
    assert!(projected
        .entries
        .iter()
        .all(|entry| entry.attributes.is_some()));
}

/// A served deployment advertises attributes as a core feature, the way the
/// embedded runtime does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_served_capability_document_advertises_attributes() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-attributes-capabilities",
        "http-attributes-capabilities",
    ))
    .await;

    let document = harness
        .client
        .capabilities()
        .await
        .expect("read capabilities");
    assert_eq!(
        document.features.get(loonfs_api::FEATURE_ATTRIBUTES),
        Some(&true)
    );
    document.validate().expect("the document is well formed");
}
