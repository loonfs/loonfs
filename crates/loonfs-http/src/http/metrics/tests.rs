//! Bounded HTTP metric labels.

use super::*;

#[test]
fn route_labels_intern_once_and_refuse_to_grow_without_bound() {
    let mut routes = RouteLabels::default();
    let first = routes.intern("/v0/namespaces/{namespace_id}/commits");
    let again = routes.intern("/v0/namespaces/{namespace_id}/commits");
    assert_eq!(first, again);
    assert_eq!(first.as_ptr(), again.as_ptr());

    for index in 0..MAX_ROUTE_LABELS {
        routes.intern(&format!("/synthetic/{index}"));
    }
    assert_eq!(routes.intern("/one/too/many"), UNMATCHED_ROUTE);
}

#[test]
fn status_classes_collapse_to_their_leading_digit() {
    assert_eq!(status_class_label(StatusCode::OK), "2xx");
    assert_eq!(status_class_label(StatusCode::UNAUTHORIZED), "4xx");
    assert_eq!(status_class_label(StatusCode::SERVICE_UNAVAILABLE), "5xx");
}
