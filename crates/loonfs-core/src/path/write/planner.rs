//! Path-intent fingerprinting and dispatch to responsibility-specific planners.

use super::intent::PathMutationIntent;
use super::plan_create::{
    plan_publish_create_directory, plan_publish_put_file_content_ref, plan_publish_undelete,
};
use super::plan_delete::plan_publish_delete_path;
use super::plan_restore::plan_publish_restore_revision;
use super::plan_transfer::{plan_publish_copy_file_path, plan_publish_move_path};
use super::planning_helpers::PublishPathPlanningView;
use crate::commit::{fingerprint_digest, PathIntentFingerprint, PATH_INTENT_FINGERPRINT_DOMAIN};
use crate::error::{CoreError, Result};
use crate::metadata::MetadataView;
use loonfs_api::wire::control::HeadState;
use loonfs_api::ChangeSeq;
use loonfs_api::{
    v0::CommitRequest as ApiCommitRequest, CommitId, ContentRef, DeleteDirectoryBehavior,
    DestinationBehavior, InodeId, NamespaceId, RevisionNo,
};
use loonfs_objectstore::ObjectStore;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedPathMutation {
    pub commit_id: CommitId,
    pub path_intent_fingerprint: PathIntentFingerprint,
    pub commit_request: ApiCommitRequest,
}

/// Canonical preimage for path-intent fingerprints.
///
/// The serde representation is durable contract (format spec, "Commit
/// identity fingerprints"): the same
/// normalized intent must fingerprint identically across releases. A
/// pinned-value test below fails if the encoding drifts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PathFingerprintInput {
    CreateDir {
        namespace_id: NamespaceId,
        absolute_path: String,
        parents: bool,
    },
    PutFile {
        namespace_id: NamespaceId,
        absolute_path: String,
        behavior: DestinationBehavior,
        content_ref: ContentRef,
    },
    DeletePath {
        namespace_id: NamespaceId,
        absolute_path: String,
        behavior: DeleteDirectoryBehavior,
        expected_inode_id: Option<InodeId>,
    },
    MovePath {
        namespace_id: NamespaceId,
        from_path: String,
        to_path: String,
        behavior: DestinationBehavior,
    },
    CopyFilePath {
        namespace_id: NamespaceId,
        from_path: String,
        to_path: String,
        behavior: DestinationBehavior,
    },
    RestoreRevision {
        namespace_id: NamespaceId,
        absolute_path: String,
        source_revision_no: RevisionNo,
    },
    Undelete {
        namespace_id: NamespaceId,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        absolute_path: String,
    },
}

fn fingerprint_path_input(identity: &PathFingerprintInput) -> Result<PathIntentFingerprint> {
    #[derive(Serialize)]
    struct CanonicalPathIntent<'a> {
        domain: &'static str,
        intent: &'a PathFingerprintInput,
    }

    fingerprint_digest(&CanonicalPathIntent {
        domain: PATH_INTENT_FINGERPRINT_DOMAIN,
        intent: identity,
    })
    .map(PathIntentFingerprint::new_unchecked)
    .map_err(|err| CoreError::Internal(format!("failed to fingerprint path intent: {err}")))
}

pub(crate) fn path_intent_fingerprint(
    namespace_id: &NamespaceId,
    intent: &PathMutationIntent,
) -> Result<PathIntentFingerprint> {
    let identity = match intent {
        PathMutationIntent::CreateDir {
            absolute_path,
            parents,
            ..
        } => PathFingerprintInput::CreateDir {
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.as_str().to_owned(),
            parents: *parents,
        },
        PathMutationIntent::PutFile {
            absolute_path,
            behavior,
            content_ref,
            ..
        } => PathFingerprintInput::PutFile {
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.as_str().to_owned(),
            behavior: *behavior,
            content_ref: content_ref.clone(),
        },
        // Path-intent identity covers the complete caller-visible logical
        // request. A changed delete guard must conflict instead of replaying
        // the old receipt without checking the new guard, and including it
        // matches explicit-commit identity, which already covers request
        // preconditions.
        PathMutationIntent::DeletePath {
            absolute_path,
            behavior,
            expected_inode_id,
            ..
        } => PathFingerprintInput::DeletePath {
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.as_str().to_owned(),
            behavior: *behavior,
            expected_inode_id: *expected_inode_id,
        },
        PathMutationIntent::MovePath {
            from_path,
            to_path,
            behavior,
            ..
        } => PathFingerprintInput::MovePath {
            namespace_id: namespace_id.clone(),
            from_path: from_path.as_str().to_owned(),
            to_path: to_path.as_str().to_owned(),
            behavior: *behavior,
        },
        PathMutationIntent::CopyFilePath {
            from_path,
            to_path,
            behavior,
            ..
        } => PathFingerprintInput::CopyFilePath {
            namespace_id: namespace_id.clone(),
            from_path: from_path.as_str().to_owned(),
            to_path: to_path.as_str().to_owned(),
            behavior: *behavior,
        },
        PathMutationIntent::RestoreRevision {
            absolute_path,
            source_revision_no,
            ..
        } => PathFingerprintInput::RestoreRevision {
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.as_str().to_owned(),
            source_revision_no: *source_revision_no,
        },
        PathMutationIntent::Undelete {
            inode_id,
            deleted_at_seq,
            absolute_path,
            ..
        } => PathFingerprintInput::Undelete {
            namespace_id: namespace_id.clone(),
            inode_id: *inode_id,
            deleted_at_seq: *deleted_at_seq,
            absolute_path: absolute_path.as_str().to_owned(),
        },
    };
    fingerprint_path_input(&identity)
}

pub(crate) async fn plan_path_mutation_against_publish_view<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    intent: &PathMutationIntent,
    head: &HeadState,
    metadata_state: &MetadataView<'_, '_, S>,
) -> Result<PlannedPathMutation> {
    let commit_id = intent.commit_id().clone();
    let path_intent_fingerprint = path_intent_fingerprint(namespace_id, intent)?;
    let view = PublishPathPlanningView {
        head,
        metadata_state,
    };
    let commit_request = match intent {
        PathMutationIntent::CreateDir {
            absolute_path,
            parents,
            ..
        } => plan_publish_create_directory(absolute_path, *parents, &commit_id, &view).await?,
        PathMutationIntent::PutFile {
            absolute_path,
            content_ref,
            behavior,
            ..
        } => {
            plan_publish_put_file_content_ref(
                absolute_path,
                content_ref.clone(),
                *behavior,
                &commit_id,
                &view,
            )
            .await?
        }
        PathMutationIntent::DeletePath {
            absolute_path,
            behavior,
            expected_inode_id,
            ..
        } => {
            plan_publish_delete_path(
                absolute_path,
                *behavior,
                *expected_inode_id,
                &commit_id,
                &view,
            )
            .await?
        }
        PathMutationIntent::MovePath {
            from_path,
            to_path,
            behavior,
            ..
        } => plan_publish_move_path(from_path, to_path, *behavior, &commit_id, &view).await?,
        PathMutationIntent::CopyFilePath {
            from_path,
            to_path,
            behavior,
            ..
        } => plan_publish_copy_file_path(from_path, to_path, *behavior, &commit_id, &view).await?,
        PathMutationIntent::RestoreRevision {
            absolute_path,
            source_revision_no,
            ..
        } => {
            plan_publish_restore_revision(absolute_path, *source_revision_no, &commit_id, &view)
                .await?
        }
        PathMutationIntent::Undelete {
            inode_id,
            deleted_at_seq,
            absolute_path,
            ..
        } => {
            plan_publish_undelete(*inode_id, *deleted_at_seq, absolute_path, &commit_id, &view)
                .await?
        }
    };
    Ok(PlannedPathMutation {
        commit_id,
        path_intent_fingerprint,
        commit_request,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::core_commit_fingerprint_for_v0_request;
    use crate::context::MutationContext;
    use crate::metadata::MetadataState;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::path::write::ops::{delete_path, put_file_bytes};
    use crate::protocol::{load_publish_metadata_view, PublishTailOptions};
    use crate::storage::content::store_bytes_as_content;
    use loonfs_api::v0::{CommitOp, CommitPrecondition, CommitRequest as ApiCommitRequest};
    use loonfs_api::{AbsolutePath, RevisionNo};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    fn test_context() -> MutationContext {
        MutationContext {
            writer_id: "writer".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_version: "test".to_owned(),
            now_ms: 1,
        }
    }

    /// Pins the exact stored fingerprint for a fixed path intent.
    ///
    /// If this fails, the canonical preimage changed (format spec, "Commit
    /// identity fingerprints") and every
    /// persisted fingerprint would disagree with recomputed ones, breaking
    /// retry idempotency across versions. Do not update the literal without
    /// bumping the fingerprint scheme tag.
    #[test]
    fn path_intent_fingerprint_value_is_pinned() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let intent = PathMutationIntent::CreateDir {
            commit_id: CommitId::parse("c_00000000000000000000000000000042").expect("commit id"),
            absolute_path: AbsolutePath::parse("/docs").expect("path"),
            parents: false,
        };

        let fingerprint = path_intent_fingerprint(&namespace_id, &intent).expect("fingerprint");

        assert_eq!(
            fingerprint.as_str(),
            // Updated pre-release when `CreateDir` gained the `parents`
            // semantic parameter (no deployed namespaces hold the prior
            // value); post-release this literal only moves with a scheme
            // tag bump.
            "v0:sha256:06414b716b076c98e7a61e465ae729b2340045c133437a557c76902d73a5f33b"
        );
    }

    /// Pins the exact stored fingerprint encoding for a guarded delete.
    #[test]
    fn guarded_delete_path_fingerprint_value_is_pinned() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let intent = PathMutationIntent::DeletePath {
            commit_id: CommitId::parse("c_00000000000000000000000000000043").expect("commit id"),
            absolute_path: AbsolutePath::parse("/docs").expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: Some(InodeId(42)),
        };

        let fingerprint = path_intent_fingerprint(&namespace_id, &intent).expect("fingerprint");

        assert_eq!(
            fingerprint.as_str(),
            // Added pre-release when the delete guard became semantic
            // request content; no deployed receipts use the prior preimage.
            "v0:sha256:6b233b1fa124c36c09104446f8a1de60b6bec76fccf6fc41e72e064c60f47715"
        );
    }

    async fn setup_namespace() -> (
        tempfile::TempDir,
        LocalFsStore,
        NamespaceId,
        MutationContext,
    ) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");
        (temp_dir, store, namespace_id, context)
    }

    async fn try_plan_against_current_state(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
    ) -> Result<PlannedPathMutation> {
        let (view, _projection) = load_publish_metadata_view(
            store,
            None,
            None,
            namespace_id,
            None,
            None,
            &PublishTailOptions::default(),
        )
        .await
        .expect("publish view");
        let empty_overlay = MetadataState::default();
        let base_view = view.metadata_view();
        let metadata_view = base_view.with_overlay(&empty_overlay, view.head().seq);
        plan_path_mutation_against_publish_view(namespace_id, intent, view.head(), &metadata_view)
            .await
    }

    async fn plan_against_current_state(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
    ) -> PlannedPathMutation {
        try_plan_against_current_state(store, namespace_id, intent)
            .await
            .expect("plan")
    }

    #[tokio::test]
    async fn path_intent_fingerprint_is_stable_for_canonical_paths() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let left = path_intent_fingerprint(
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-docs-a").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs/a").expect("path"),
                parents: false,
            },
        )
        .expect("left fingerprint");
        let right = path_intent_fingerprint(
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-docs-b").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs/a").expect("path"),
                parents: false,
            },
        )
        .expect("right fingerprint");

        assert_eq!(left, right);
    }

    #[tokio::test]
    async fn path_intent_fingerprint_changes_when_logical_inputs_change() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let baseline = path_intent_fingerprint(
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-docs").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs").expect("path"),
                parents: false,
            },
        )
        .expect("baseline fingerprint");
        let changed = path_intent_fingerprint(
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-drafts").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/drafts").expect("path"),
                parents: false,
            },
        )
        .expect("changed fingerprint");

        assert_ne!(baseline, changed);
    }

    #[tokio::test]
    async fn path_intent_and_core_commit_fingerprints_use_distinct_domains() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let path_fingerprint = path_intent_fingerprint(
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-docs").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs").expect("path"),
                parents: false,
            },
        )
        .expect("path fingerprint");
        let core_fingerprint = core_commit_fingerprint_for_v0_request(
            &namespace_id,
            &ApiCommitRequest {
                commit_id: CommitId::parse("mkdir-docs").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("docs")
                        .expect("valid display name"),
                }],
                message: None,
            },
        )
        .expect("core fingerprint");

        assert_ne!(path_fingerprint.as_str(), core_fingerprint.as_str());
    }

    #[tokio::test]
    async fn create_directory_plan_contains_semantic_op_and_target_absence_precondition() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace().await;
        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-docs").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs").expect("path"),
                parents: false,
            },
        )
        .await;

        assert_eq!(
            planned.commit_request.ops,
            vec![CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
            }]
        );
        assert!(planned
            .commit_request
            .preconditions
            .iter()
            .any(|precondition| matches!(
                precondition,
                CommitPrecondition::ChildNameAbsent {
                    parent_inode_id: InodeId(1),
                    name_key,
                } if name_key.as_str() == "docs"
            )));
    }

    #[tokio::test]
    async fn put_file_plan_auto_creates_missing_parent_directories() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace().await;
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello")
            .await
            .expect("stage");
        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-nested").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs/nested/a.txt").expect("path"),
                content_ref: staged.content_ref.clone(),
                behavior: DestinationBehavior::NoReplace,
            },
        )
        .await;

        assert_eq!(planned.commit_request.ops.len(), 3);
        assert!(matches!(
            &planned.commit_request.ops[0],
            CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name,
            } if display_name.as_str() == "docs"
        ));
        assert!(matches!(
            &planned.commit_request.ops[1],
            CommitOp::CreateDirectory { display_name, .. } if display_name.as_str() == "nested"
        ));
        assert!(matches!(
            &planned.commit_request.ops[2],
            CommitOp::CreateFile {
                display_name,
                content_ref,
                ..
            } if display_name.as_str() == "a.txt" && content_ref == &staged.content_ref
        ));
    }

    #[tokio::test]
    async fn move_path_plan_contains_binding_and_target_absence_preconditions() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let seed_commit_id = CommitId::parse("seed-file").expect("valid commit id");
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/a.txt",
            b"hello",
            DestinationBehavior::NoReplace,
            &context,
            Some(&seed_commit_id),
        )
        .await
        .expect("seed file");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::MovePath {
                commit_id: CommitId::parse("move-file").expect("valid commit id"),
                from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
                behavior: DestinationBehavior::NoReplace,
            },
        )
        .await;

        assert!(matches!(
            planned.commit_request.ops.as_slice(),
            [CommitOp::Rename {
                new_display_name,
                ..
            }] if new_display_name.as_str() == "b.txt"
        ));
        assert!(planned
            .commit_request
            .preconditions
            .iter()
            .any(|precondition| matches!(precondition, CommitPrecondition::BindingIs { .. })));
        assert!(planned
            .commit_request
            .preconditions
            .iter()
            .any(|precondition| matches!(
                precondition,
                CommitPrecondition::ChildNameAbsent { name_key, .. } if name_key.as_str() == "b.txt"
            )));
    }

    #[tokio::test]
    async fn copy_file_plan_validates_source_revision_and_target_absence() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let seed_commit_id = CommitId::parse("seed-copy-source").expect("valid commit id");
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/a.txt",
            b"hello",
            DestinationBehavior::NoReplace,
            &context,
            Some(&seed_commit_id),
        )
        .await
        .expect("seed file");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::CopyFilePath {
                commit_id: CommitId::parse("copy-file").expect("valid commit id"),
                from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                to_path: AbsolutePath::parse("/docs/copy.txt").expect("path"),
                behavior: DestinationBehavior::NoReplace,
            },
        )
        .await;

        assert!(matches!(
            planned.commit_request.ops.as_slice(),
            [CommitOp::CreateFile { display_name, .. }] if display_name.as_str() == "copy.txt"
        ));
        assert!(planned
            .commit_request
            .preconditions
            .iter()
            .any(|precondition| matches!(
                precondition,
                CommitPrecondition::InodeRevisionIs {
                    revision_no: RevisionNo(1),
                    ..
                }
            )));
        assert!(planned
            .commit_request
            .preconditions
            .iter()
            .any(|precondition| matches!(
                precondition,
                CommitPrecondition::ChildNameAbsent { name_key, .. } if name_key.as_str() == "copy.txt"
            )));
    }

    #[tokio::test]
    async fn recreate_after_delete_succeeds_at_the_same_path() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/tmp.txt",
            b"first",
            DestinationBehavior::NoReplace,
            &context,
            Some(&CommitId::parse("recreate-seed").expect("valid commit id")),
        )
        .await
        .expect("seed file");
        delete_path(
            &store,
            &namespace_id,
            "/docs/tmp.txt",
            &context,
            Some(&CommitId::parse("recreate-delete").expect("valid commit id")),
        )
        .await
        .expect("delete file");

        // The tombstone covers the dead inode, not the name: the name is
        // reusable immediately, with or without an intervening rebuild.
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/tmp.txt",
            b"second",
            DestinationBehavior::NoReplace,
            &context,
            Some(&CommitId::parse("recreate-put").expect("valid commit id")),
        )
        .await
        .expect("recreate at the deleted path");
    }

    #[tokio::test]
    async fn deleted_subtree_names_replan_as_fresh_state() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let seed_commit_id = CommitId::parse("seed-dead-tree").expect("valid commit id");
        put_file_bytes(
            &store,
            &namespace_id,
            "/dead/file.txt",
            b"hello",
            DestinationBehavior::NoReplace,
            &context,
            Some(&seed_commit_id),
        )
        .await
        .expect("seed file");
        let delete_commit_id = CommitId::parse("delete-dead-tree").expect("valid commit id");
        delete_path(
            &store,
            &namespace_id,
            "/dead",
            &context,
            Some(&delete_commit_id),
        )
        .await
        .expect("delete tree");
        let staged = store_bytes_as_content(&store, &namespace_id, b"new")
            .await
            .expect("stage");
        // The deleted name is invisible, so planning under it starts a
        // fresh subtree instead of conflicting with the dead one — the same
        // answer callers get after compaction drops the dead rows.
        try_plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-under-dead").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/dead/new.txt").expect("path"),
                content_ref: staged.content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
        )
        .await
        .expect("recreating a deleted subtree plans as fresh state");
    }
}
