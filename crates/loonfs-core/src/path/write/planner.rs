//! Plans one path mutation intent against namespace state, producing the
//! commit plan and semantic fingerprint the publish path needs.

use super::intent::PathMutationIntent;
use crate::commit::{fingerprint_digest, PathIntentFingerprint, PATH_INTENT_FINGERPRINT_DOMAIN};
use crate::error::CoreError;
#[cfg(test)]
use crate::metadata::MetadataState;
use crate::metadata::{MetadataView, ResolvedVisiblePath, VisiblePathError};
use crate::path::helpers::{ensure_mutation_path, final_component};
use loonfs_api::wire::control::HeadState;
use loonfs_api::ChangeSeq;
use loonfs_api::{
    v0::{
        CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition,
        CommitRequest as ApiCommitRequest,
    },
    AbsolutePath, CommitId, ContentRef, DeleteDirectoryBehavior, DestinationBehavior, DisplayName,
    InodeId, InodeKind, NameKey, NamespaceId, RevisionNo,
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

fn path_intent_fingerprint(
    identity: &PathFingerprintInput,
) -> Result<PathIntentFingerprint, CoreError> {
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

pub(crate) fn path_intent_fingerprint_for_path_intent(
    namespace_id: &NamespaceId,
    intent: &PathMutationIntent,
) -> Result<PathIntentFingerprint, CoreError> {
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
        // `expected_inode_id` is deliberately outside the preimage: it is
        // a precondition on current state, not part of the mutation's
        // semantic identity (same stance as explicit commit preconditions
        // vs the path-op vocabulary).
        PathMutationIntent::DeletePath {
            absolute_path,
            behavior,
            ..
        } => PathFingerprintInput::DeletePath {
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.as_str().to_owned(),
            behavior: *behavior,
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
    path_intent_fingerprint(&identity)
}

fn is_missing_visible_path(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::PathNotFound(_) | CoreError::VisiblePath(VisiblePathError::PathNotFound { .. })
    )
}

pub(crate) async fn plan_path_mutation_against_publish_view<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    intent: &PathMutationIntent,
    head: &HeadState,
    metadata_state: &MetadataView<'_, '_, S>,
) -> Result<PlannedPathMutation, CoreError> {
    let commit_id = intent.commit_id().clone();
    let path_intent_fingerprint = path_intent_fingerprint_for_path_intent(namespace_id, intent)?;
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

struct PublishPathPlanningView<'a, 'b, 'store, S: ObjectStore + ?Sized> {
    head: &'a HeadState,
    metadata_state: &'a MetadataView<'b, 'store, S>,
}

async fn publish_binding_is_precondition<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    resolved: &ResolvedVisiblePath,
) -> Result<ApiCommitPrecondition, CoreError> {
    let parent_inode_id = resolved
        .parent_inode_id
        .ok_or(CoreError::RootMutationForbidden)?;
    let binding = view
        .metadata_state
        .current_parent_binding_for_child(resolved.inode_id)
        .await?
        .ok_or_else(|| CoreError::PathNotFound(resolved.absolute_path.clone()))?;
    if binding.parent_inode_id != parent_inode_id {
        return Err(CoreError::PathNotFound(resolved.absolute_path.clone()));
    }
    Ok(ApiCommitPrecondition::BindingIs {
        parent_inode_id,
        name_key: binding.name_key.clone(),
        child_inode_id: binding.child_inode_id,
        bind_seq: binding.bind_seq,
        bind_delta_index: binding.bind_delta_index,
    })
}

fn publish_child_name_absent_precondition<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    parent_inode_id: InodeId,
    display_name: &str,
) -> ApiCommitPrecondition {
    let display_name =
        DisplayName::parse(display_name).expect("path planner should provide valid display name");
    let name_key = NameKey::for_display_name(view.metadata_state.name_policy(), &display_name);
    ApiCommitPrecondition::ChildNameAbsent {
        parent_inode_id,
        name_key,
    }
}

/// Rejects planning through a *visible* path component covered by a subtree
/// tombstone. The walk observes only visible bindings, so its answer cannot
/// change when compaction drops rows no retained sequence observes: a deleted
/// (unbound) name simply ends the walk, and recreating it plans as a fresh
/// subtree. A visible-but-covered component cannot arise from legal writer
/// histories; hitting one means corrupt state, and failing the plan is the
/// safe answer.
async fn publish_reject_tombstoned_path_ancestor<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    absolute_path: &AbsolutePath,
) -> Result<(), CoreError> {
    let mut current_inode = InodeId(1);
    let mut current_path = AbsolutePath::root();

    for component in absolute_path.components() {
        let display_name = component.to_display_name();
        let name_key = NameKey::for_display_name(view.metadata_state.name_policy(), &display_name);
        let Some(bound_child) = view
            .metadata_state
            .visible_child(current_inode, name_key.as_str())
            .await?
        else {
            return Ok(());
        };
        let visible_component = DisplayName::parse(&bound_child.display_name)
            .map_err(crate::path::helpers::map_path_error_to_core)?;
        let visible_path = current_path.join(&visible_component);
        if let Some(tombstone) = view
            .metadata_state
            .covering_subtree_tombstone(bound_child.child_inode_id)
            .await?
        {
            return Err(CoreError::TombstoneConflict {
                path: visible_path.as_str().to_owned(),
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            });
        }
        current_inode = bound_child.child_inode_id;
        current_path = visible_path;
    }
    Ok(())
}

async fn plan_publish_create_directory<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    parents: bool,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    ensure_mutation_path(absolute_path)?;
    publish_reject_tombstoned_path_ancestor(view, absolute_path).await?;
    match view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await
    {
        Ok(_) => {
            return Err(CoreError::DestinationExists(
                absolute_path.as_str().to_owned(),
            ));
        }
        Err(error) if is_missing_visible_path(&error) => {}
        Err(error) => return Err(error),
    }
    let mut ops = Vec::new();
    let parent_inode_id = if parents {
        // The same ancestor auto-create the put-file plan performs.
        let mut next_inode_id = view.head.next_inode_id;
        publish_ensure_parent_directories(absolute_path, view, &mut ops, &mut next_inode_id).await?
    } else {
        publish_resolve_parent_directory(view, absolute_path).await?
    };
    let display_name = final_component(absolute_path)?;
    ops.push(ApiCommitOp::CreateDirectory {
        parent_inode_id,
        display_name: display_name.clone(),
    });
    // A parent allocated by this same commit cannot have conflicting
    // children yet, so the name and ancestor preconditions only apply when
    // the parent already exists — mirroring the put-file plan.
    let mut preconditions = Vec::new();
    if view
        .metadata_state
        .visible_inode(parent_inode_id)
        .await?
        .is_some()
    {
        preconditions.push(publish_child_name_absent_precondition(
            view,
            parent_inode_id,
            &display_name,
        ));
        preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
            inode_id: parent_inode_id,
        });
    }
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops,
        preconditions,
        message: None,
    })
}

async fn plan_publish_undelete<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    deleted_at_seq: ChangeSeq,
    absolute_path: &AbsolutePath,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    ensure_mutation_path(absolute_path)?;
    publish_reject_tombstoned_path_ancestor(view, absolute_path).await?;
    match view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await
    {
        Ok(_) => {
            return Err(CoreError::DestinationExists(
                absolute_path.as_str().to_owned(),
            ));
        }
        Err(error) if is_missing_visible_path(&error) => {}
        Err(error) => return Err(error),
    }
    // The destination parent must already exist: recovery targets a place
    // the caller can see, and commit validation re-checks the tombstone
    // root, the parent, and the name under the publish lock.
    let parent_inode_id = publish_resolve_parent_directory(view, absolute_path).await?;
    let display_name = final_component(absolute_path)?;
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::Undelete {
            inode_id,
            deleted_at_seq,
            parent_inode_id,
            display_name: display_name.clone(),
        }],
        preconditions: vec![
            publish_child_name_absent_precondition(view, parent_inode_id, &display_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: parent_inode_id,
            },
        ],
        message: None,
    })
}

async fn plan_publish_put_file_content_ref<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    content_ref: ContentRef,
    behavior: DestinationBehavior,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    ensure_mutation_path(absolute_path)?;
    publish_reject_tombstoned_path_ancestor(view, absolute_path).await?;
    let target = view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await;

    let mut ops = Vec::new();
    let mut next_inode_id = view.head.next_inode_id;
    let final_parent_inode =
        publish_ensure_parent_directories(absolute_path, view, &mut ops, &mut next_inode_id)
            .await?;
    let final_name = final_component(absolute_path)?;
    let mut preconditions = Vec::new();

    match target {
        Ok(existing) => {
            if behavior == DestinationBehavior::NoReplace {
                return Err(CoreError::DestinationExists(
                    absolute_path.as_str().to_owned(),
                ));
            }
            if existing.inode_kind != InodeKind::File {
                return Err(CoreError::ExpectedFile {
                    path: absolute_path.as_str().to_owned(),
                    kind: existing.inode_kind,
                });
            }
            let revision = view
                .metadata_state
                .latest_revision_head(existing.inode_id)
                .await?
                .ok_or_else(|| CoreError::PathNotFound(absolute_path.as_str().to_owned()))?;
            preconditions.push(publish_binding_is_precondition(view, &existing).await?);
            ops.push(ApiCommitOp::ReplaceFile {
                inode_id: existing.inode_id,
                base_revision_no: revision.revision_no,
                content_ref: content_ref.clone(),
            });
            preconditions.push(ApiCommitPrecondition::InodeRevisionIs {
                inode_id: existing.inode_id,
                revision_no: revision.revision_no,
            });
            preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: existing.inode_id,
            });
        }
        Err(error) if is_missing_visible_path(&error) => {
            ops.push(ApiCommitOp::CreateFile {
                parent_inode_id: final_parent_inode,
                display_name: final_name.clone(),
                content_ref,
            });
            if view
                .metadata_state
                .visible_inode(final_parent_inode)
                .await?
                .is_some()
            {
                preconditions.push(publish_child_name_absent_precondition(
                    view,
                    final_parent_inode,
                    &final_name,
                ));
                preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                    inode_id: final_parent_inode,
                });
            }
        }
        Err(other) => return Err(other),
    }

    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops,
        preconditions,
        message: None,
    })
}

async fn plan_publish_delete_path<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    behavior: DeleteDirectoryBehavior,
    expected_inode_id: Option<InodeId>,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    ensure_mutation_path(absolute_path)?;
    let resolved = view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await?;
    // Planning happens under the publish lock, so this check is race-free:
    // a caller that resolved the path earlier (a stat) either deletes that
    // exact inode or fails, never a raced rebinding.
    if let Some(expected) = expected_inode_id {
        if resolved.inode_id != expected {
            return Err(
                crate::commit::CommitValidationError::BindingPreconditionMismatch {
                    // Root cannot be deleted, so a resolved delete target
                    // always has a parent.
                    parent_inode_id: resolved.parent_inode_id.unwrap_or(InodeId(1)),
                    name_key: loonfs_api::name_key_for_display_name(
                        view.metadata_state.name_policy(),
                        &resolved.display_name,
                    ),
                    expected_child_inode_id: expected,
                    actual_child_inode_id: Some(resolved.inode_id),
                }
                .into(),
            );
        }
    }
    let recursive = behavior == DeleteDirectoryBehavior::Recursive;
    let op = match resolved.inode_kind {
        InodeKind::File => ApiCommitOp::DeleteFile {
            inode_id: resolved.inode_id,
        },
        InodeKind::Directory if recursive => ApiCommitOp::DeleteSubtree {
            root_inode_id: resolved.inode_id,
        },
        InodeKind::Directory => {
            if view
                .metadata_state
                .has_visible_children(resolved.inode_id)
                .await?
            {
                return Err(CoreError::DirectoryNotEmpty(
                    absolute_path.as_str().to_owned(),
                ));
            }
            ApiCommitOp::DeleteSubtree {
                root_inode_id: resolved.inode_id,
            }
        }
    };
    let mut preconditions = vec![publish_binding_is_precondition(view, &resolved).await?];
    if !recursive && resolved.inode_kind == InodeKind::Directory {
        preconditions.push(ApiCommitPrecondition::DirectoryEmpty {
            inode_id: resolved.inode_id,
        });
    }
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![op],
        preconditions,
        message: None,
    })
}

async fn plan_publish_move_path<S: ObjectStore + ?Sized>(
    from_path: &AbsolutePath,
    to_path: &AbsolutePath,
    behavior: DestinationBehavior,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    ensure_mutation_path(from_path)?;
    ensure_mutation_path(to_path)?;
    publish_reject_tombstoned_path_ancestor(view, from_path).await?;
    publish_reject_tombstoned_path_ancestor(view, to_path).await?;
    let source = view.metadata_state.resolve_visible_path(from_path).await?;
    let target_parent = publish_resolve_parent_directory(view, to_path).await?;
    let target_name = final_component(to_path)?;
    // Replace compiles to an atomic delete-plus-rename: the destination
    // file's delete and the source's rebind land in one commit, and the
    // rename's target-name check observes the in-commit unbind. Mirrors
    // put: only a file destination can be replaced, and a path never
    // replaces itself.
    let replaced = match view.metadata_state.resolve_visible_path(to_path).await {
        Ok(existing)
            if behavior == DestinationBehavior::Replace && existing.inode_id != source.inode_id =>
        {
            if existing.inode_kind != InodeKind::File {
                return Err(CoreError::ExpectedFile {
                    path: to_path.as_str().to_owned(),
                    kind: existing.inode_kind,
                });
            }
            Some(existing)
        }
        Ok(_) => return Err(CoreError::DestinationExists(to_path.as_str().to_owned())),
        Err(error) if is_missing_visible_path(&error) => None,
        Err(error) => return Err(error),
    };
    let mut ops = Vec::new();
    let mut preconditions = vec![publish_binding_is_precondition(view, &source).await?];
    match &replaced {
        Some(existing) => {
            ops.push(ApiCommitOp::DeleteFile {
                inode_id: existing.inode_id,
            });
            preconditions.push(publish_binding_is_precondition(view, existing).await?);
            preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: existing.inode_id,
            });
        }
        None => {
            preconditions.push(publish_child_name_absent_precondition(
                view,
                target_parent,
                &target_name,
            ));
        }
    }
    ops.push(ApiCommitOp::Rename {
        inode_id: source.inode_id,
        new_parent_inode_id: target_parent,
        new_display_name: target_name.clone(),
    });
    preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
        inode_id: source.inode_id,
    });
    preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
        inode_id: target_parent,
    });
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops,
        preconditions,
        message: None,
    })
}

async fn plan_publish_copy_file_path<S: ObjectStore + ?Sized>(
    from_path: &AbsolutePath,
    to_path: &AbsolutePath,
    behavior: DestinationBehavior,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    ensure_mutation_path(from_path)?;
    ensure_mutation_path(to_path)?;
    publish_reject_tombstoned_path_ancestor(view, from_path).await?;
    publish_reject_tombstoned_path_ancestor(view, to_path).await?;

    let source = view.metadata_state.resolve_visible_path(from_path).await?;
    if source.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: from_path.as_str().to_owned(),
            kind: source.inode_kind,
        });
    }

    // Replace mirrors put onto an existing file: the copy appends a new
    // revision to the destination inode, keeping its identity and revision
    // history. Only a file destination can be replaced, and a path never
    // replaces itself.
    let replaced = match view.metadata_state.resolve_visible_path(to_path).await {
        Ok(existing)
            if behavior == DestinationBehavior::Replace && existing.inode_id != source.inode_id =>
        {
            if existing.inode_kind != InodeKind::File {
                return Err(CoreError::ExpectedFile {
                    path: to_path.as_str().to_owned(),
                    kind: existing.inode_kind,
                });
            }
            Some(existing)
        }
        Ok(_) => return Err(CoreError::DestinationExists(to_path.as_str().to_owned())),
        Err(error) if is_missing_visible_path(&error) => None,
        Err(error) => return Err(error),
    };

    let revision = view
        .metadata_state
        .latest_revision_head(source.inode_id)
        .await?
        .ok_or_else(|| CoreError::PathNotFound(from_path.as_str().to_owned()))?;

    let target_parent = publish_resolve_parent_directory(view, to_path).await?;
    let target_name = final_component(to_path)?;
    let mut ops = Vec::new();
    let mut preconditions = vec![
        publish_binding_is_precondition(view, &source).await?,
        ApiCommitPrecondition::InodeRevisionIs {
            inode_id: source.inode_id,
            revision_no: revision.revision_no,
        },
        ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
            inode_id: source.inode_id,
        },
    ];
    match &replaced {
        Some(existing) => {
            let existing_revision = view
                .metadata_state
                .latest_revision_head(existing.inode_id)
                .await?
                .ok_or_else(|| CoreError::PathNotFound(to_path.as_str().to_owned()))?;
            ops.push(ApiCommitOp::ReplaceFile {
                inode_id: existing.inode_id,
                base_revision_no: existing_revision.revision_no,
                content_ref: revision.content_ref,
            });
            preconditions.push(publish_binding_is_precondition(view, existing).await?);
            preconditions.push(ApiCommitPrecondition::InodeRevisionIs {
                inode_id: existing.inode_id,
                revision_no: existing_revision.revision_no,
            });
            preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: existing.inode_id,
            });
        }
        None => {
            ops.push(ApiCommitOp::CreateFile {
                parent_inode_id: target_parent,
                display_name: target_name.clone(),
                content_ref: revision.content_ref,
            });
            preconditions.push(publish_child_name_absent_precondition(
                view,
                target_parent,
                &target_name,
            ));
        }
    }
    preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
        inode_id: target_parent,
    });
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops,
        preconditions,
        message: None,
    })
}

async fn plan_publish_restore_revision<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    source_revision_no: RevisionNo,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    ensure_mutation_path(absolute_path)?;
    publish_reject_tombstoned_path_ancestor(view, absolute_path).await?;
    let target = view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await?;
    if target.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: absolute_path.as_str().to_owned(),
            kind: target.inode_kind,
        });
    }
    let revision = view
        .metadata_state
        .latest_revision_head(target.inode_id)
        .await?
        .ok_or_else(|| CoreError::PathNotFound(absolute_path.as_str().to_owned()))?;

    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::RestoreRevision {
            inode_id: target.inode_id,
            source_revision_no,
            base_revision_no: revision.revision_no,
        }],
        preconditions: vec![
            publish_binding_is_precondition(view, &target).await?,
            ApiCommitPrecondition::InodeRevisionIs {
                inode_id: target.inode_id,
                revision_no: revision.revision_no,
            },
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: target.inode_id,
            },
        ],
        message: None,
    })
}

async fn publish_ensure_parent_directories<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    ops: &mut Vec<ApiCommitOp>,
    next_inode_id: &mut InodeId,
) -> Result<InodeId, CoreError> {
    let components = absolute_path.components();
    if components.len() <= 1 {
        return Ok(InodeId(1));
    }

    let mut current_inode = InodeId(1);
    let mut creating_missing_ancestors = false;
    for component in &components[..components.len() - 1] {
        let display_name = component.to_display_name();
        let name_key = NameKey::for_display_name(view.metadata_state.name_policy(), &display_name);
        if !creating_missing_ancestors {
            if let Some(child) = view
                .metadata_state
                .visible_child(current_inode, name_key.as_str())
                .await?
            {
                let inode = view
                    .metadata_state
                    .visible_inode(child.child_inode_id)
                    .await?
                    .ok_or_else(|| CoreError::PathNotFound(component.as_str().to_owned()))?;
                if inode.inode_kind != InodeKind::Directory {
                    return Err(CoreError::NonDirectoryPathComponent(
                        component.as_str().to_owned(),
                    ));
                }
                current_inode = child.child_inode_id;
                continue;
            }
            creating_missing_ancestors = true;
        }

        ops.push(ApiCommitOp::CreateDirectory {
            parent_inode_id: current_inode,
            display_name: display_name.as_str().to_owned(),
        });
        let allocated = *next_inode_id;
        *next_inode_id = InodeId(next_inode_id.0.saturating_add(1));
        current_inode = allocated;
    }
    Ok(current_inode)
}

async fn publish_resolve_parent_directory<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    absolute_path: &AbsolutePath,
) -> Result<InodeId, CoreError> {
    let Some(parent_path) = absolute_path.parent() else {
        return Ok(InodeId(1));
    };
    if parent_path.is_root() {
        return Ok(InodeId(1));
    }
    let resolved = view
        .metadata_state
        .resolve_visible_path(&parent_path)
        .await?;
    if resolved.inode_kind != InodeKind::Directory {
        return Err(CoreError::ExpectedDirectory {
            path: parent_path.as_str().to_owned(),
            kind: resolved.inode_kind,
        });
    }
    Ok(resolved.inode_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::core_commit_fingerprint_for_v0_request;
    use crate::context::MutationContext;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::path::write::ops::{delete_path, put_file_bytes};
    use crate::protocol::{load_publish_metadata_view, PublishTailOptions};
    use crate::storage::content::store_bytes_as_content;
    use loonfs_api::v0::{CommitOp, CommitPrecondition, CommitRequest as ApiCommitRequest};
    use loonfs_api::RevisionNo;
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

        let fingerprint =
            path_intent_fingerprint_for_path_intent(&namespace_id, &intent).expect("fingerprint");

        assert_eq!(
            fingerprint.as_str(),
            // Updated pre-release when `CreateDir` gained the `parents`
            // semantic parameter (no deployed namespaces hold the prior
            // value); post-release this literal only moves with a scheme
            // tag bump.
            "v0:sha256:06414b716b076c98e7a61e465ae729b2340045c133437a557c76902d73a5f33b"
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
    ) -> Result<PlannedPathMutation, CoreError> {
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
    async fn path_intent_fingerprint_normalizes_paths() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let left = path_intent_fingerprint_for_path_intent(
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-docs-a").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs//a/").expect("path"),
                parents: false,
            },
        )
        .expect("left fingerprint");
        let right = path_intent_fingerprint_for_path_intent(
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
        let baseline = path_intent_fingerprint_for_path_intent(
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-docs").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs").expect("path"),
                parents: false,
            },
        )
        .expect("baseline fingerprint");
        let changed = path_intent_fingerprint_for_path_intent(
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
        let path_fingerprint = path_intent_fingerprint_for_path_intent(
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
                    display_name: "docs".to_owned(),
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
                display_name: "docs".to_owned(),
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
            } if display_name == "docs"
        ));
        assert!(matches!(
            &planned.commit_request.ops[1],
            CommitOp::CreateDirectory { display_name, .. } if display_name == "nested"
        ));
        assert!(matches!(
            &planned.commit_request.ops[2],
            CommitOp::CreateFile {
                display_name,
                content_ref,
                ..
            } if display_name == "a.txt" && content_ref == &staged.content_ref
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
            }] if new_display_name == "b.txt"
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
            [CommitOp::CreateFile { display_name, .. }] if display_name == "copy.txt"
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
