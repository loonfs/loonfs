//! The method and send policy of each server operation that the client calls.
//!
//! Each constant's name is an OpenAPI operation ID in upper case. The client sends an operation once when
//! its `x-loonfs-retry` class is `not_idempotent`, and retries every other operation. A content
//! transfer retries without the operation deadline, because a large body can take longer than
//! the deadline to send. A body that the client streams is sent once whatever the policy, because
//! a stream can't be replayed. A streamed response is retried until its head arrives, because no
//! byte of its body has been read before then.

use crate::transport::SendPolicy;
use http::Method;

/// The HTTP method of one server operation, and how the client sends it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Operation {
    pub(crate) method: Method,
    pub(crate) policy: SendPolicy,
}

impl Operation {
    /// An operation that the client sends once.
    pub(crate) const fn once(method: Method) -> Self {
        Self {
            method,
            policy: SendPolicy::Once,
        }
    }

    /// An operation that the client retries within the operation deadline.
    pub(crate) const fn retry(method: Method) -> Self {
        Self {
            method,
            policy: SendPolicy::Retry,
        }
    }

    /// An operation that the client retries without the operation deadline.
    pub(crate) const fn retry_without_deadline(method: Method) -> Self {
        Self {
            method,
            policy: SendPolicy::RetryWithoutDeadline,
        }
    }
}

pub(crate) const GET_HEALTH: Operation = Operation::retry(Method::GET);
pub(crate) const GET_CAPABILITIES: Operation = Operation::retry(Method::GET);
pub(crate) const CREATE_NAMESPACE: Operation = Operation::once(Method::POST);
pub(crate) const GET_NAMESPACE: Operation = Operation::retry(Method::GET);
pub(crate) const GET_PATH_ENTRY: Operation = Operation::retry(Method::GET);
pub(crate) const GET_INODE: Operation = Operation::retry(Method::GET);
pub(crate) const LIST_PATH_ENTRIES: Operation = Operation::retry(Method::GET);
pub(crate) const LIST_INODE_CHILDREN: Operation = Operation::retry(Method::GET);
pub(crate) const LIST_FILE_REVISIONS: Operation = Operation::retry(Method::GET);
pub(crate) const LIST_FILE_REVISIONS_BY_INODE: Operation = Operation::retry(Method::GET);
pub(crate) const GET_FILE_CONTENT: Operation = Operation::retry_without_deadline(Method::GET);
pub(crate) const GET_FILE_CONTENT_BY_INODE: Operation =
    Operation::retry_without_deadline(Method::GET);
pub(crate) const GET_FILE_REVISION_CONTENT_BY_INODE: Operation =
    Operation::retry_without_deadline(Method::GET);
pub(crate) const CREATE_DOWNLOAD: Operation = Operation::retry(Method::POST);
pub(crate) const CREATE_DOWNLOAD_BY_INODE: Operation = Operation::retry(Method::POST);
pub(crate) const CREATE_REVISION_DOWNLOAD_BY_INODE: Operation = Operation::retry(Method::POST);
pub(crate) const LIST_TRASH: Operation = Operation::retry(Method::GET);
pub(crate) const CREATE_COMMIT: Operation = Operation::retry(Method::POST);
pub(crate) const CREATE_UPLOAD: Operation = Operation::once(Method::POST);
pub(crate) const PUT_UPLOAD_CONTENT: Operation = Operation::retry_without_deadline(Method::PUT);
pub(crate) const SIGN_UPLOAD_PARTS: Operation = Operation::retry(Method::POST);
pub(crate) const COMPLETE_UPLOAD: Operation = Operation::retry(Method::POST);
pub(crate) const GET_UPLOAD: Operation = Operation::retry(Method::GET);
pub(crate) const ABORT_UPLOAD: Operation = Operation::retry(Method::POST);
pub(crate) const LIST_CHANGES: Operation = Operation::retry(Method::GET);
pub(crate) const CREATE_SNAPSHOT: Operation = Operation::once(Method::POST);
pub(crate) const LIST_SNAPSHOTS: Operation = Operation::retry(Method::GET);
pub(crate) const GET_SNAPSHOT: Operation = Operation::retry(Method::GET);
pub(crate) const EXTEND_SNAPSHOT: Operation = Operation::retry(Method::POST);
pub(crate) const DELETE_SNAPSHOT: Operation = Operation::retry(Method::DELETE);
pub(crate) const FORK_NAMESPACE: Operation = Operation::once(Method::POST);
pub(crate) const DELETE_NAMESPACE: Operation = Operation::once(Method::DELETE);
pub(crate) const GET_NAMESPACE_DIAGNOSTICS: Operation = Operation::retry(Method::GET);
pub(crate) const LIST_CHECKPOINTS: Operation = Operation::retry(Method::GET);
pub(crate) const GET_CHECKPOINT: Operation = Operation::retry(Method::GET);
pub(crate) const RUN_MAINTENANCE: Operation = Operation::once(Method::POST);
pub(crate) const PROBE_STORE: Operation = Operation::once(Method::POST);
pub(crate) const GREP: Operation = Operation::retry(Method::GET);
pub(crate) const GET_GREP_INDEX: Operation = Operation::retry(Method::GET);
pub(crate) const ENABLE_GREP_INDEX: Operation = Operation::retry(Method::POST);
pub(crate) const DISABLE_GREP_INDEX: Operation = Operation::retry(Method::POST);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::BTreeMap;

    /// Every constant above, paired with its operation ID.
    const OPERATIONS: &[(&str, Operation)] = &[
        ("get_health", GET_HEALTH),
        ("get_capabilities", GET_CAPABILITIES),
        ("create_namespace", CREATE_NAMESPACE),
        ("get_namespace", GET_NAMESPACE),
        ("get_path_entry", GET_PATH_ENTRY),
        ("get_inode", GET_INODE),
        ("list_path_entries", LIST_PATH_ENTRIES),
        ("list_inode_children", LIST_INODE_CHILDREN),
        ("list_file_revisions", LIST_FILE_REVISIONS),
        ("list_file_revisions_by_inode", LIST_FILE_REVISIONS_BY_INODE),
        ("get_file_content", GET_FILE_CONTENT),
        ("get_file_content_by_inode", GET_FILE_CONTENT_BY_INODE),
        (
            "get_file_revision_content_by_inode",
            GET_FILE_REVISION_CONTENT_BY_INODE,
        ),
        ("create_download", CREATE_DOWNLOAD),
        ("create_download_by_inode", CREATE_DOWNLOAD_BY_INODE),
        (
            "create_revision_download_by_inode",
            CREATE_REVISION_DOWNLOAD_BY_INODE,
        ),
        ("list_trash", LIST_TRASH),
        ("create_commit", CREATE_COMMIT),
        ("create_upload", CREATE_UPLOAD),
        ("put_upload_content", PUT_UPLOAD_CONTENT),
        ("sign_upload_parts", SIGN_UPLOAD_PARTS),
        ("complete_upload", COMPLETE_UPLOAD),
        ("get_upload", GET_UPLOAD),
        ("abort_upload", ABORT_UPLOAD),
        ("list_changes", LIST_CHANGES),
        ("create_snapshot", CREATE_SNAPSHOT),
        ("list_snapshots", LIST_SNAPSHOTS),
        ("get_snapshot", GET_SNAPSHOT),
        ("extend_snapshot", EXTEND_SNAPSHOT),
        ("delete_snapshot", DELETE_SNAPSHOT),
        ("fork_namespace", FORK_NAMESPACE),
        ("delete_namespace", DELETE_NAMESPACE),
        ("get_namespace_diagnostics", GET_NAMESPACE_DIAGNOSTICS),
        ("list_checkpoints", LIST_CHECKPOINTS),
        ("get_checkpoint", GET_CHECKPOINT),
        ("run_maintenance", RUN_MAINTENANCE),
        ("probe_store", PROBE_STORE),
        ("grep", GREP),
        ("get_grep_index", GET_GREP_INDEX),
        ("enable_grep_index", ENABLE_GREP_INDEX),
        ("disable_grep_index", DISABLE_GREP_INDEX),
    ];

    /// Checks that the client sends exactly the `not_idempotent` operations of the generated
    /// OpenAPI document once, sends each operation with its documented method, and has an
    /// operation for every one it can call.
    #[test]
    fn operations_agree_with_the_documented_methods_and_retry_classes() {
        let document: Value = serde_json::from_str(
            &std::fs::read_to_string(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../docs/specs/openapi.json"
            ))
            .expect("read docs/specs/openapi.json"),
        )
        .expect("parse docs/specs/openapi.json");
        let mut classes = BTreeMap::new();
        for path in document["paths"]
            .as_object()
            .expect("paths object")
            .values()
        {
            for (method, operation) in path.as_object().expect("path item object") {
                if let Some(operation_id) = operation["operationId"].as_str() {
                    let class = operation["x-loonfs-retry"]
                        .as_str()
                        .unwrap_or_else(|| panic!("`{operation_id}` has no retry class"));
                    classes.insert(operation_id, (method.as_str(), class));
                }
            }
        }

        for (operation_id, operation) in OPERATIONS {
            let (method, class) = classes
                .remove(operation_id)
                .unwrap_or_else(|| panic!("`{operation_id}` isn't in the OpenAPI document"));
            assert_eq!(
                operation.method.as_str().to_ascii_lowercase(),
                method,
                "the client sends `{operation_id}` with the wrong method"
            );
            let policy = operation.policy;
            let once = match class {
                "not_idempotent" => true,
                "idempotent" | "replayable" => false,
                class => panic!("`{operation_id}` has an unknown retry class `{class}`"),
            };
            assert_eq!(
                policy == SendPolicy::Once,
                once,
                "the client sends `{class}` operation `{operation_id}` with {policy:?}"
            );
        }
        assert_eq!(
            classes.into_keys().collect::<Vec<_>>(),
            ["get_metrics", "get_readiness"],
            "the client has no send policy for these operations"
        );
    }
}
