//! Bounded HTTP metric labels.

use super::*;

#[test]
fn route_labels_come_from_the_contract_route_table() {
    for (matched_route, label) in [
        (
            Some("/v0/namespaces/{namespace_id}/commits"),
            "/v0/namespaces/{namespace_id}/commits",
        ),
        (Some("/health"), OTHER_ROUTE),
        (None, UNMATCHED_ROUTE),
    ] {
        assert_eq!(route_label(matched_route), label);
    }
}

#[test]
fn status_classes_collapse_to_their_leading_digit() {
    assert_eq!(status_class_label(StatusCode::OK), "2xx");
    assert_eq!(status_class_label(StatusCode::UNAUTHORIZED), "4xx");
    assert_eq!(status_class_label(StatusCode::SERVICE_UNAVAILABLE), "5xx");
}
