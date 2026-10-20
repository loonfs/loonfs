//! The send policy of each server operation that the client calls.
//!
//! Each constant's name is an OpenAPI operation ID in upper case. The client sends an operation once when
//! its `x-loonfs-retry` class is `not_idempotent`, and retries every other operation. A content
//! transfer retries without the operation deadline, because a large body can take longer than
//! the deadline to send. A body that the client streams is sent once whatever the policy, because
//! a stream can't be replayed.

use crate::transport::SendPolicy;

pub(crate) const GET_HEALTH: SendPolicy = SendPolicy::Retry;
pub(crate) const GET_CAPABILITIES: SendPolicy = SendPolicy::Retry;
pub(crate) const CREATE_NAMESPACE: SendPolicy = SendPolicy::Once;
pub(crate) const GET_NAMESPACE: SendPolicy = SendPolicy::Retry;
pub(crate) const GET_PATH_ENTRY: SendPolicy = SendPolicy::Retry;
pub(crate) const GET_INODE: SendPolicy = SendPolicy::Retry;
pub(crate) const LIST_PATH_ENTRIES: SendPolicy = SendPolicy::Retry;
pub(crate) const LIST_INODE_CHILDREN: SendPolicy = SendPolicy::Retry;
pub(crate) const LIST_FILE_REVISIONS: SendPolicy = SendPolicy::Retry;
pub(crate) const LIST_FILE_REVISIONS_BY_INODE: SendPolicy = SendPolicy::Retry;
pub(crate) const GET_FILE_CONTENT: SendPolicy = SendPolicy::RetryWithoutDeadline;
pub(crate) const GET_FILE_CONTENT_BY_INODE: SendPolicy = SendPolicy::RetryWithoutDeadline;
pub(crate) const GET_FILE_REVISION_CONTENT_BY_INODE: SendPolicy = SendPolicy::RetryWithoutDeadline;
pub(crate) const CREATE_DOWNLOAD: SendPolicy = SendPolicy::Retry;
pub(crate) const CREATE_DOWNLOAD_BY_INODE: SendPolicy = SendPolicy::Retry;
pub(crate) const CREATE_REVISION_DOWNLOAD_BY_INODE: SendPolicy = SendPolicy::Retry;
pub(crate) const LIST_TRASH: SendPolicy = SendPolicy::Retry;
pub(crate) const CREATE_COMMIT: SendPolicy = SendPolicy::Retry;
pub(crate) const CREATE_UPLOAD: SendPolicy = SendPolicy::Once;
pub(crate) const PUT_UPLOAD_CONTENT: SendPolicy = SendPolicy::RetryWithoutDeadline;
pub(crate) const SIGN_UPLOAD_PARTS: SendPolicy = SendPolicy::Retry;
pub(crate) const COMPLETE_UPLOAD: SendPolicy = SendPolicy::Retry;
pub(crate) const GET_UPLOAD: SendPolicy = SendPolicy::Retry;
pub(crate) const ABORT_UPLOAD: SendPolicy = SendPolicy::Retry;
pub(crate) const LIST_CHANGES: SendPolicy = SendPolicy::Retry;
pub(crate) const CREATE_SNAPSHOT: SendPolicy = SendPolicy::Once;
pub(crate) const LIST_SNAPSHOTS: SendPolicy = SendPolicy::Retry;
pub(crate) const GET_SNAPSHOT: SendPolicy = SendPolicy::Retry;
pub(crate) const EXTEND_SNAPSHOT: SendPolicy = SendPolicy::Retry;
pub(crate) const DELETE_SNAPSHOT: SendPolicy = SendPolicy::Once;
pub(crate) const FORK_NAMESPACE: SendPolicy = SendPolicy::Once;
pub(crate) const DELETE_NAMESPACE: SendPolicy = SendPolicy::Once;
pub(crate) const GET_NAMESPACE_DIAGNOSTICS: SendPolicy = SendPolicy::Retry;
pub(crate) const CREATE_CHECKPOINT: SendPolicy = SendPolicy::Once;
pub(crate) const LIST_CHECKPOINTS: SendPolicy = SendPolicy::Retry;
pub(crate) const GET_CHECKPOINT: SendPolicy = SendPolicy::Retry;
pub(crate) const DELETE_CHECKPOINT: SendPolicy = SendPolicy::Once;
pub(crate) const RUN_MAINTENANCE: SendPolicy = SendPolicy::Once;
pub(crate) const PROBE_STORE: SendPolicy = SendPolicy::Once;
pub(crate) const GREP: SendPolicy = SendPolicy::Retry;
pub(crate) const GET_GREP_INDEX: SendPolicy = SendPolicy::Retry;
pub(crate) const ENABLE_GREP_INDEX: SendPolicy = SendPolicy::Retry;
pub(crate) const DISABLE_GREP_INDEX: SendPolicy = SendPolicy::Retry;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::BTreeMap;

    /// Every constant above, paired with its operation ID.
    const OPERATIONS: &[(&str, SendPolicy)] = &[
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
        ("create_checkpoint", CREATE_CHECKPOINT),
        ("list_checkpoints", LIST_CHECKPOINTS),
        ("get_checkpoint", GET_CHECKPOINT),
        ("delete_checkpoint", DELETE_CHECKPOINT),
        ("run_maintenance", RUN_MAINTENANCE),
        ("probe_store", PROBE_STORE),
        ("grep", GREP),
        ("get_grep_index", GET_GREP_INDEX),
        ("enable_grep_index", ENABLE_GREP_INDEX),
        ("disable_grep_index", DISABLE_GREP_INDEX),
    ];

    /// Checks that the client sends exactly the `not_idempotent` operations of the generated
    /// OpenAPI document once, and has a send policy for every operation it can call.
    #[test]
    fn send_policies_agree_with_the_documented_retry_classes() {
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
            for operation in path.as_object().expect("path item object").values() {
                if let Some(operation_id) = operation["operationId"].as_str() {
                    let class = operation["x-loonfs-retry"]
                        .as_str()
                        .unwrap_or_else(|| panic!("`{operation_id}` has no retry class"));
                    classes.insert(operation_id, class);
                }
            }
        }

        for &(operation_id, policy) in OPERATIONS {
            let class = classes
                .remove(operation_id)
                .unwrap_or_else(|| panic!("`{operation_id}` isn't in the OpenAPI document"));
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
