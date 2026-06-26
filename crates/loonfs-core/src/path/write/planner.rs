use super::intent::PathMutationIntent;
use crate::commit::{
    fingerprint_digest, PathIntentFingerprint, PublishMetadataPreview,
    PATH_INTENT_FINGERPRINT_DOMAIN,
};
use crate::error::CoreError;
#[cfg(test)]
use crate::metadata::MetadataState;
use crate::metadata::{ResolvedVisiblePath, VisiblePathError};
use crate::path::helpers::{final_component, parse_absolute_path_for_core, parse_mutation_path};
use loonfs_api::wire::control::HeadState;
#[cfg(test)]
use loonfs_api::ChangeSeq;
use loonfs_api::{
    v0::{
        CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition,
        CommitRequest as ApiCommitRequest, MoveBehavior,
    },
    AbsolutePath, CommitId, ContentRef, DeleteDirectoryBehavior, DisplayName, InodeId, InodeKind,
    NameKey, NamespaceId, PutBehavior, RevisionNo,
};
use loonfs_objectstore::ObjectStore;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedPathMutation {
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
#[serde(tag = "type", rename_all = "snake_case")]
enum PathFingerprintInput {
    CreateDir {
        namespace_id: NamespaceId,
        absolute_path: String,
    },
    PutFile {
        namespace_id: NamespaceId,
        absolute_path: String,
        behavior: PutBehavior,
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
        behavior: MoveBehavior,
    },
    CopyFilePath {
        namespace_id: NamespaceId,
        from_path: String,
        to_path: String,
    },
    RestoreRevision {
        namespace_id: NamespaceId,
        absolute_path: String,
        source_revision_no: RevisionNo,
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
    .map_err(|err| CoreError::Store(err.to_string()))
}

pub(crate) fn path_intent_fingerprint_for_path_intent(
    namespace_id: &NamespaceId,
    intent: &PathMutationIntent,
) -> Result<PathIntentFingerprint, CoreError> {
    let identity = match intent {
        PathMutationIntent::CreateDir { absolute_path, .. } => PathFingerprintInput::CreateDir {
            namespace_id: namespace_id.clone(),
            absolute_path: normalized_path_for_fingerprint(absolute_path)?,
        },
        PathMutationIntent::PutFile {
            absolute_path,
            behavior,
            content_ref,
            ..
        } => PathFingerprintInput::PutFile {
            namespace_id: namespace_id.clone(),
            absolute_path: normalized_path_for_fingerprint(absolute_path)?,
            behavior: *behavior,
            content_ref: content_ref.clone(),
        },
        PathMutationIntent::DeletePath {
            absolute_path,
            behavior,
            ..
        } => PathFingerprintInput::DeletePath {
            namespace_id: namespace_id.clone(),
            absolute_path: normalized_path_for_fingerprint(absolute_path)?,
            behavior: *behavior,
        },
        PathMutationIntent::MovePath {
            from_path,
            to_path,
            behavior,
            ..
        } => PathFingerprintInput::MovePath {
            namespace_id: namespace_id.clone(),
            from_path: normalized_path_for_fingerprint(from_path)?,
            to_path: normalized_path_for_fingerprint(to_path)?,
            behavior: *behavior,
        },
        PathMutationIntent::CopyFilePath {
            from_path, to_path, ..
        } => PathFingerprintInput::CopyFilePath {
            namespace_id: namespace_id.clone(),
            from_path: normalized_path_for_fingerprint(from_path)?,
            to_path: normalized_path_for_fingerprint(to_path)?,
        },
        PathMutationIntent::RestoreRevision {
            absolute_path,
            source_revision_no,
            ..
        } => PathFingerprintInput::RestoreRevision {
            namespace_id: namespace_id.clone(),
            absolute_path: normalized_path_for_fingerprint(absolute_path)?,
            source_revision_no: *source_revision_no,
        },
    };
    path_intent_fingerprint(&identity)
}

fn normalized_path_for_fingerprint(absolute_path: &str) -> Result<String, CoreError> {
    Ok(parse_absolute_path_for_core(absolute_path)?
        .as_str()
        .to_owned())
}

fn is_missing_visible_path(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::MissingPath(_) | CoreError::VisiblePath(VisiblePathError::PathNotFound { .. })
    )
}

pub(crate) async fn plan_path_mutation_against_publish_view<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    intent: &PathMutationIntent,
    head: &HeadState,
    metadata_state: &PublishMetadataPreview<'_, S>,
) -> Result<PlannedPathMutation, CoreError> {
    let commit_id = intent.commit_id().clone();
    let path_intent_fingerprint = path_intent_fingerprint_for_path_intent(namespace_id, intent)?;
    let view = PublishPathPlanningView {
        head,
        metadata_state,
    };
    let commit_request = match intent {
        PathMutationIntent::CreateDir { absolute_path, .. } => {
            plan_publish_create_dir(absolute_path, &commit_id, &view).await?
        }
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
            ..
        } => plan_publish_delete_path(absolute_path, *behavior, &commit_id, &view).await?,
        PathMutationIntent::MovePath {
            from_path,
            to_path,
            behavior,
            ..
        } => plan_publish_move_path(from_path, to_path, *behavior, &commit_id, &view).await?,
        PathMutationIntent::CopyFilePath {
            from_path, to_path, ..
        } => plan_publish_copy_file_path(from_path, to_path, &commit_id, &view).await?,
        PathMutationIntent::RestoreRevision {
            absolute_path,
            source_revision_no,
            ..
        } => {
            plan_publish_restore_revision(absolute_path, *source_revision_no, &commit_id, &view)
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
struct PathPlanningView<'a> {
    head: &'a HeadState,
    metadata_state: &'a MetadataState,
}

struct PublishPathPlanningView<'a, 'b, S: ObjectStore + ?Sized> {
    head: &'a HeadState,
    metadata_state: &'a PublishMetadataPreview<'b, S>,
}

async fn publish_binding_is_precondition<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, S>,
    resolved: &ResolvedVisiblePath,
) -> Result<ApiCommitPrecondition, CoreError> {
    let parent_inode = resolved
        .parent_inode_id
        .ok_or(CoreError::RootMutationForbidden)?;
    let binding = view
        .metadata_state
        .current_parent_binding_for_child(resolved.inode_id, view.head.seq)
        .await?
        .ok_or_else(|| CoreError::MissingPath(resolved.absolute_path.clone()))?;
    if binding.parent_inode_id != parent_inode {
        return Err(CoreError::MissingPath(resolved.absolute_path.clone()));
    }
    Ok(ApiCommitPrecondition::BindingIs {
        parent_inode,
        name_key: NameKey::try_new(binding.name_key).map_err(|err| {
            CoreError::NamespaceCorrupt(format!("invalid metadata name_key: {err}"))
        })?,
        child_inode: binding.child_inode_id,
        bind_seq: binding.bind_seq,
        bind_delta_index: binding.bind_delta_index,
    })
}

fn publish_child_name_absent_precondition<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, S>,
    parent_inode: InodeId,
    display_name: &str,
) -> ApiCommitPrecondition {
    let display_name =
        DisplayName::parse(display_name).expect("path planner should provide valid display name");
    let name_key = NameKey::for_display_name(view.head.name_policy, &display_name);
    ApiCommitPrecondition::ChildNameAbsent {
        parent_inode,
        name_key,
    }
}

async fn publish_reject_tombstoned_path_ancestor<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, S>,
    absolute_path: &AbsolutePath,
) -> Result<(), CoreError> {
    let mut current_inode = InodeId(1);
    let mut current_path = AbsolutePath::root();

    for component in absolute_path.components() {
        let display_name = component.to_display_name();
        let name_key = NameKey::for_display_name(view.head.name_policy, &display_name);
        let Some(bound_child) = view
            .metadata_state
            .bound_child_at_seq(current_inode, name_key.as_str(), view.head.seq)
            .await?
        else {
            return Ok(());
        };
        let visible_component = DisplayName::parse(&bound_child.display_name)
            .map_err(crate::path::helpers::map_path_error_to_core)?;
        let visible_path = current_path.join(&visible_component);
        if let Some(tombstone) = view
            .metadata_state
            .covering_subtree_tombstone(bound_child.child_inode_id, view.head.seq)
            .await?
        {
            return Err(CoreError::TombstoneConflict {
                path: visible_path.as_str().to_owned(),
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            });
        }
        let Some(latest_binding) = view
            .metadata_state
            .current_parent_binding_for_child(bound_child.child_inode_id, view.head.seq)
            .await?
        else {
            return Ok(());
        };
        if latest_binding.parent_inode_id != bound_child.parent_inode_id
            || latest_binding.name_key != bound_child.name_key
            || latest_binding.bind_seq != bound_child.bind_seq
            || latest_binding.bind_delta_index != bound_child.bind_delta_index
        {
            return Ok(());
        }
        current_inode = bound_child.child_inode_id;
        current_path = visible_path;
    }
    Ok(())
}

async fn plan_publish_create_dir<S: ObjectStore + ?Sized>(
    absolute_path: &str,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    let absolute_path = parse_mutation_path(absolute_path)?;
    publish_reject_tombstoned_path_ancestor(view, &absolute_path).await?;
    match view
        .metadata_state
        .resolve_visible_path(&absolute_path, view.head.name_policy, view.head.seq)
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
    let parent_inode = publish_resolve_parent_directory(view, &absolute_path).await?;
    let display_name = final_component(&absolute_path)?;
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::CreateDir {
            parent_inode,
            display_name: display_name.clone(),
        }],
        preconditions: vec![
            publish_child_name_absent_precondition(view, parent_inode, &display_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: parent_inode,
            },
        ],
        message: None,
        annotations: None,
    })
}

async fn plan_publish_put_file_content_ref<S: ObjectStore + ?Sized>(
    absolute_path: &str,
    content_ref: ContentRef,
    behavior: PutBehavior,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    let absolute_path = parse_mutation_path(absolute_path)?;
    publish_reject_tombstoned_path_ancestor(view, &absolute_path).await?;
    let target = view
        .metadata_state
        .resolve_visible_path(&absolute_path, view.head.name_policy, view.head.seq)
        .await;

    let mut ops = Vec::new();
    let mut next_inode_id = view.head.next_inode_id;
    let final_parent_inode =
        publish_ensure_parent_directories(&absolute_path, view, &mut ops, &mut next_inode_id)
            .await?;
    let final_name = final_component(&absolute_path)?;
    let mut preconditions = Vec::new();

    match target {
        Ok(existing) => {
            if behavior == PutBehavior::NoReplace {
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
                .latest_revision_head_at_seq(existing.inode_id, view.head.seq)
                .await?
                .ok_or_else(|| CoreError::MissingPath(absolute_path.as_str().to_owned()))?;
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
                parent_inode: final_parent_inode,
                display_name: final_name.clone(),
                content_ref,
            });
            if view
                .metadata_state
                .visible_inode(final_parent_inode, view.head.seq)
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
        annotations: None,
    })
}

async fn plan_publish_delete_path<S: ObjectStore + ?Sized>(
    absolute_path: &str,
    behavior: DeleteDirectoryBehavior,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    let absolute_path = parse_mutation_path(absolute_path)?;
    let resolved = view
        .metadata_state
        .resolve_visible_path(&absolute_path, view.head.name_policy, view.head.seq)
        .await?;
    let recursive = behavior == DeleteDirectoryBehavior::Recursive;
    let op = match resolved.inode_kind {
        InodeKind::File => ApiCommitOp::DeleteFile {
            inode_id: resolved.inode_id,
        },
        InodeKind::Dir if recursive => ApiCommitOp::DeleteSubtree {
            root_inode: resolved.inode_id,
        },
        InodeKind::Dir => {
            let children = view
                .metadata_state
                .visible_children(resolved.inode_id, view.head.seq)
                .await?;
            if !children.is_empty() {
                return Err(CoreError::DirectoryNotEmpty(
                    absolute_path.as_str().to_owned(),
                ));
            }
            ApiCommitOp::DeleteSubtree {
                root_inode: resolved.inode_id,
            }
        }
    };
    let mut preconditions = vec![publish_binding_is_precondition(view, &resolved).await?];
    if !recursive && resolved.inode_kind == InodeKind::Dir {
        preconditions.push(ApiCommitPrecondition::DirectoryEmpty {
            inode_id: resolved.inode_id,
        });
    }
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![op],
        preconditions,
        message: None,
        annotations: None,
    })
}

async fn plan_publish_move_path<S: ObjectStore + ?Sized>(
    from_path: &str,
    to_path: &str,
    behavior: MoveBehavior,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    if behavior != MoveBehavior::NoReplace {
        return Err(CoreError::CommitValidation(
            crate::commit::CommitValidationError::UnsupportedMoveBehavior { behavior },
        ));
    }
    let from_path = parse_mutation_path(from_path)?;
    let to_path = parse_mutation_path(to_path)?;
    publish_reject_tombstoned_path_ancestor(view, &from_path).await?;
    publish_reject_tombstoned_path_ancestor(view, &to_path).await?;
    let source = view
        .metadata_state
        .resolve_visible_path(&from_path, view.head.name_policy, view.head.seq)
        .await?;
    let target_parent = publish_resolve_parent_directory(view, &to_path).await?;
    let target_name = final_component(&to_path)?;
    match view
        .metadata_state
        .resolve_visible_path(&to_path, view.head.name_policy, view.head.seq)
        .await
    {
        Ok(_) => return Err(CoreError::DestinationExists(to_path.as_str().to_owned())),
        Err(error) if is_missing_visible_path(&error) => {}
        Err(error) => return Err(error),
    }
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::Rename {
            inode_id: source.inode_id,
            new_parent_inode: target_parent,
            new_display_name: target_name.clone(),
            behavior,
        }],
        preconditions: vec![
            publish_binding_is_precondition(view, &source).await?,
            publish_child_name_absent_precondition(view, target_parent, &target_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: source.inode_id,
            },
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: target_parent,
            },
        ],
        message: None,
        annotations: None,
    })
}

async fn plan_publish_copy_file_path<S: ObjectStore + ?Sized>(
    from_path: &str,
    to_path: &str,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    let from_path = parse_mutation_path(from_path)?;
    let to_path = parse_mutation_path(to_path)?;
    publish_reject_tombstoned_path_ancestor(view, &from_path).await?;
    publish_reject_tombstoned_path_ancestor(view, &to_path).await?;

    let source = view
        .metadata_state
        .resolve_visible_path(&from_path, view.head.name_policy, view.head.seq)
        .await?;
    if source.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: from_path.as_str().to_owned(),
            kind: source.inode_kind,
        });
    }

    match view
        .metadata_state
        .resolve_visible_path(&to_path, view.head.name_policy, view.head.seq)
        .await
    {
        Ok(_) => return Err(CoreError::DestinationExists(to_path.as_str().to_owned())),
        Err(error) if is_missing_visible_path(&error) => {}
        Err(error) => return Err(error),
    }

    let revision = view
        .metadata_state
        .latest_revision_head_at_seq(source.inode_id, view.head.seq)
        .await?
        .ok_or_else(|| CoreError::MissingPath(from_path.as_str().to_owned()))?;

    let target_parent = publish_resolve_parent_directory(view, &to_path).await?;
    let target_name = final_component(&to_path)?;
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::CreateFile {
            parent_inode: target_parent,
            display_name: target_name.clone(),
            content_ref: revision.content_ref,
        }],
        preconditions: vec![
            publish_binding_is_precondition(view, &source).await?,
            ApiCommitPrecondition::InodeRevisionIs {
                inode_id: source.inode_id,
                revision_no: revision.revision_no,
            },
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: source.inode_id,
            },
            publish_child_name_absent_precondition(view, target_parent, &target_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: target_parent,
            },
        ],
        message: None,
        annotations: None,
    })
}

async fn plan_publish_restore_revision<S: ObjectStore + ?Sized>(
    absolute_path: &str,
    source_revision_no: RevisionNo,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, S>,
) -> Result<ApiCommitRequest, CoreError> {
    let absolute_path = parse_mutation_path(absolute_path)?;
    publish_reject_tombstoned_path_ancestor(view, &absolute_path).await?;
    let target = view
        .metadata_state
        .resolve_visible_path(&absolute_path, view.head.name_policy, view.head.seq)
        .await?;
    if target.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: absolute_path.as_str().to_owned(),
            kind: target.inode_kind,
        });
    }
    let revision = view
        .metadata_state
        .latest_revision_head_at_seq(target.inode_id, view.head.seq)
        .await?
        .ok_or_else(|| CoreError::MissingPath(absolute_path.as_str().to_owned()))?;

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
        annotations: None,
    })
}

async fn publish_ensure_parent_directories<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    view: &PublishPathPlanningView<'_, '_, S>,
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
        let name_key = NameKey::for_display_name(view.head.name_policy, &display_name);
        if !creating_missing_ancestors {
            if let Some(child) = view
                .metadata_state
                .visible_child(current_inode, name_key.as_str(), view.head.seq)
                .await?
            {
                let inode = view
                    .metadata_state
                    .visible_inode(child.child_inode_id, view.head.seq)
                    .await?
                    .ok_or_else(|| CoreError::MissingPath(component.as_str().to_owned()))?;
                if inode.inode_kind != InodeKind::Dir {
                    return Err(CoreError::NonDirectoryPathComponent(
                        component.as_str().to_owned(),
                    ));
                }
                current_inode = child.child_inode_id;
                continue;
            }
            creating_missing_ancestors = true;
        }

        ops.push(ApiCommitOp::CreateDir {
            parent_inode: current_inode,
            display_name: display_name.as_str().to_owned(),
        });
        let allocated = *next_inode_id;
        *next_inode_id = InodeId(next_inode_id.0.saturating_add(1));
        current_inode = allocated;
    }
    Ok(current_inode)
}

async fn publish_resolve_parent_directory<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, S>,
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
        .resolve_visible_path(&parent_path, view.head.name_policy, view.head.seq)
        .await?;
    if resolved.inode_kind != InodeKind::Dir {
        return Err(CoreError::ExpectedDirectory {
            path: parent_path.as_str().to_owned(),
            kind: resolved.inode_kind,
        });
    }
    Ok(resolved.inode_id)
}

#[cfg(test)]
fn binding_is_precondition(
    view: &PathPlanningView<'_>,
    resolved: &ResolvedVisiblePath,
) -> Result<ApiCommitPrecondition, CoreError> {
    let parent_inode = resolved
        .parent_inode_id
        .ok_or(CoreError::RootMutationForbidden)?;
    let binding = view
        .metadata_state
        .current_parent_binding_for_child(resolved.inode_id, view.head.seq)
        .ok_or_else(|| CoreError::MissingPath(resolved.absolute_path.clone()))?;
    if binding.parent_inode_id != parent_inode {
        return Err(CoreError::MissingPath(resolved.absolute_path.clone()));
    }
    Ok(ApiCommitPrecondition::BindingIs {
        parent_inode,
        name_key: NameKey::try_new(binding.name_key).map_err(|err| {
            CoreError::NamespaceCorrupt(format!("invalid metadata name_key: {err}"))
        })?,
        child_inode: binding.child_inode_id,
        bind_seq: binding.bind_seq,
        bind_delta_index: binding.bind_delta_index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::core_commit_fingerprint_for_v0_request;
    use crate::context::MutationContext;
    use crate::error::ErrorCode;
    use crate::metadata::{DirentryBindRecord, InodeRecord};
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::path::write::ops::{delete_path, put_file_bytes};
    use crate::protocol::{load_publish_manifest_plus_tail_view, PublishTailOptions};
    use crate::storage::content::store_bytes_as_content;
    use loonfs_api::v0::{CommitOp, CommitPrecondition, CommitRequest as ApiCommitRequest};
    use loonfs_api::RevisionNo;
    use loonfs_objectstore::fs::LocalFsStore;
    use tempfile::tempdir;

    fn test_context() -> MutationContext {
        MutationContext {
            writer_id: "writer".to_owned(),
            writer_version: "test".to_owned(),
            now_ms: 1,
            lease_duration_ms: 60_000,
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
            absolute_path: "/docs".to_owned(),
        };

        let fingerprint =
            path_intent_fingerprint_for_path_intent(&namespace_id, &intent).expect("fingerprint");

        assert_eq!(
            fingerprint.as_str(),
            "v0:sha256:85be04e569ee0e20adb4b3da3d095a6c27bd4fc2b5cb90785cc71980a48fddad"
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
        let (view, _projection) = load_publish_manifest_plus_tail_view(
            store,
            namespace_id,
            None,
            &PublishTailOptions::default(),
        )
        .await
        .expect("publish view");
        let preview = PublishMetadataPreview::new(view.metadata_view(), &MetadataState::default());
        plan_path_mutation_against_publish_view(namespace_id, intent, view.head(), &preview).await
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
                absolute_path: "/docs//a/".to_owned(),
            },
        )
        .expect("left fingerprint");
        let right = path_intent_fingerprint_for_path_intent(
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-docs-b").expect("valid commit id"),
                absolute_path: "/docs/a".to_owned(),
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
                absolute_path: "/docs".to_owned(),
            },
        )
        .expect("baseline fingerprint");
        let changed = path_intent_fingerprint_for_path_intent(
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-drafts").expect("valid commit id"),
                absolute_path: "/drafts".to_owned(),
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
                absolute_path: "/docs".to_owned(),
            },
        )
        .expect("path fingerprint");
        let core_fingerprint = core_commit_fingerprint_for_v0_request(
            &namespace_id,
            &ApiCommitRequest {
                commit_id: CommitId::parse("mkdir-docs").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "docs".to_owned(),
                }],
                message: None,
                annotations: None,
            },
        )
        .expect("core fingerprint");

        assert_ne!(path_fingerprint.as_str(), core_fingerprint.as_str());
    }

    #[tokio::test]
    async fn create_dir_plan_contains_semantic_op_and_target_absence_precondition() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace().await;
        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-docs").expect("valid commit id"),
                absolute_path: "/docs".to_owned(),
            },
        )
        .await;

        assert_eq!(
            planned.commit_request.ops,
            vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
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
                    parent_inode: InodeId(1),
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
                absolute_path: "/docs/nested/a.txt".to_owned(),
                content_ref: staged.content_ref.clone(),
                behavior: PutBehavior::NoReplace,
            },
        )
        .await;

        assert_eq!(planned.commit_request.ops.len(), 3);
        assert!(matches!(
            &planned.commit_request.ops[0],
            CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name,
            } if display_name == "docs"
        ));
        assert!(matches!(
            &planned.commit_request.ops[1],
            CommitOp::CreateDir { display_name, .. } if display_name == "nested"
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
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/a.txt",
            b"hello",
            PutBehavior::NoReplace,
            &context,
            Some("seed-file"),
        )
        .await
        .expect("seed file");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::MovePath {
                commit_id: CommitId::parse("move-file").expect("valid commit id"),
                from_path: "/docs/a.txt".to_owned(),
                to_path: "/docs/b.txt".to_owned(),
                behavior: MoveBehavior::NoReplace,
            },
        )
        .await;

        assert!(matches!(
            planned.commit_request.ops.as_slice(),
            [CommitOp::Rename {
                new_display_name,
                behavior: MoveBehavior::NoReplace,
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
    async fn binding_is_precondition_reports_invalid_durable_name_key_as_namespace_corrupt() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let mut head = HeadState::initial(namespace_id);
        head.seq = ChangeSeq(1);
        let metadata_state = MetadataState::from_rows(
            vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(0),
                },
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
            ],
            vec![DirentryBindRecord {
                parent_inode_id: InodeId(1),
                name_key: "bad/key".to_owned(),
                display_name: "file.txt".to_owned(),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let view = PathPlanningView {
            head: &head,
            metadata_state: &metadata_state,
        };
        let resolved = ResolvedVisiblePath {
            absolute_path: "/file.txt".to_owned(),
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            parent_inode_id: Some(InodeId(1)),
            display_name: "file.txt".to_owned(),
        };

        let error =
            binding_is_precondition(&view, &resolved).expect_err("invalid durable name key");

        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    }

    #[tokio::test]
    async fn copy_file_plan_validates_source_revision_and_target_absence() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/a.txt",
            b"hello",
            PutBehavior::NoReplace,
            &context,
            Some("seed-copy-source"),
        )
        .await
        .expect("seed file");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::CopyFilePath {
                commit_id: CommitId::parse("copy-file").expect("valid commit id"),
                from_path: "/docs/a.txt".to_owned(),
                to_path: "/docs/copy.txt".to_owned(),
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
    async fn tombstoned_ancestor_blocks_descendant_planning() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        put_file_bytes(
            &store,
            &namespace_id,
            "/dead/file.txt",
            b"hello",
            PutBehavior::NoReplace,
            &context,
            Some("seed-dead-tree"),
        )
        .await
        .expect("seed file");
        delete_path(
            &store,
            &namespace_id,
            "/dead",
            &context,
            Some("delete-dead-tree"),
        )
        .await
        .expect("delete tree");
        let staged = store_bytes_as_content(&store, &namespace_id, b"new")
            .await
            .expect("stage");
        let error = try_plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-under-dead").expect("valid commit id"),
                absolute_path: "/dead/new.txt".to_owned(),
                content_ref: staged.content_ref,
                behavior: PutBehavior::NoReplace,
            },
        )
        .await
        .expect_err("tombstoned ancestor");

        assert_eq!(error.code(), ErrorCode::TombstoneConflict);
    }
}
