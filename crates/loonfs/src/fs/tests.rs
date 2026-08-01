//! Behavior tests for the runtime core.

use crate::publish::{parse_mutation_path, CommitRequest, FilesystemOperation};
use crate::{CommitId, DestinationBehavior, RevisionNo};

#[test]
fn mutation_facade_exports_constructor_types() {
    // An embedded caller builds a whole multi-operation request from the
    // crate's own facade: nothing here reaches into loonfs-core.
    let request = CommitRequest {
        commit_id: CommitId::generate(),
        message: None,
        operations: vec![
            FilesystemOperation::RestoreRevision {
                path: parse_mutation_path("/docs/Report.txt").expect("valid mutation path"),
                source_revision_no: RevisionNo(1),
            },
            FilesystemOperation::MovePath {
                from_path: parse_mutation_path("/docs/Report.txt").expect("valid mutation path"),
                to_path: parse_mutation_path("/docs/report.txt").expect("valid mutation path"),
                behavior: DestinationBehavior::NoReplace,
            },
        ],
    };

    assert_eq!(request.operations.len(), 2);
}

#[test]
fn single_operation_request_carries_exactly_one_operation() {
    let operation = FilesystemOperation::CreateDirectory {
        path: parse_mutation_path("/docs").expect("valid mutation path"),
        parents: false,
    };
    let request = CommitRequest::single(
        CommitId::generate(),
        Some("create docs".to_owned()),
        operation.clone(),
    );

    assert_eq!(request.operations, vec![operation]);
    assert_eq!(request.message.as_deref(), Some("create docs"));
}
