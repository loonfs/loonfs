use super::helpers::{
    final_component, lookup_path, parse_absolute_path_for_core, parse_mutation_path,
};
use super::intent::{PathMutationIntent, PutFileBehavior};
use super::tombstone::reject_tombstoned_path_ancestor;
use crate::basis::{load_verified_namespace_basis, VerifiedNamespaceBasis};
use crate::commit::{PathIntentFingerprint, PATH_INTENT_FINGERPRINT_DOMAIN};
use crate::error::CoreError;
use crate::metadata::{MetadataState, ResolvedVisiblePath, VisiblePathError};
use loon_api::wire::control::payload_checksum_sha256;
use loon_api::wire::control::HeadState;
use loon_api::wire::wal::WalDelta;
use loon_api::{
    v0::{
        CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition,
        CommitRequest as ApiCommitRequest, RenameMode,
    },
    AbsolutePath, ChangeSeq, CommitId, ContentRef, DisplayName, InodeId, InodeKind, NameKey,
    NamespaceId, RevisionNo,
};
use loon_objectstore::ObjectStore;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedPathMutation {
    pub commit_id: CommitId,
    pub path_intent_fingerprint: PathIntentFingerprint,
    pub commit_request: ApiCommitRequest,
}

pub(crate) struct PathPlanner<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
}

impl<'a, S: ObjectStore + ?Sized> PathPlanner<'a, S> {
    pub(crate) fn new(store: &'a S) -> Self {
        Self { store }
    }

    pub(crate) fn plan_against_basis(
        &self,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
    ) -> Result<PlannedPathMutation, CoreError> {
        let basis = load_verified_namespace_basis(self.store, namespace_id)?;
        self.plan_against_verified_basis(namespace_id, intent, &basis)
    }

    pub(crate) fn plan_against_verified_basis(
        &self,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
        basis: &VerifiedNamespaceBasis,
    ) -> Result<PlannedPathMutation, CoreError> {
        self.plan_against_state(namespace_id, intent, &basis.head, &basis.metadata_state)
    }

    pub(crate) fn plan_against_state(
        &self,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
        head: &HeadState,
        metadata_state: &MetadataState,
    ) -> Result<PlannedPathMutation, CoreError> {
        plan_path_mutation_against_state(namespace_id, intent, head, metadata_state)
    }
}

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
        behavior: PutFileBehavior,
        content_ref: ContentRef,
    },
    DeletePath {
        namespace_id: NamespaceId,
        absolute_path: String,
        recursive: bool,
    },
    MovePath {
        namespace_id: NamespaceId,
        from_path: String,
        to_path: String,
        mode: RenameMode,
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

    payload_checksum_sha256(&CanonicalPathIntent {
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
            recursive,
            ..
        } => PathFingerprintInput::DeletePath {
            namespace_id: namespace_id.clone(),
            absolute_path: normalized_path_for_fingerprint(absolute_path)?,
            recursive: *recursive,
        },
        PathMutationIntent::MovePath {
            from_path,
            to_path,
            mode,
            ..
        } => PathFingerprintInput::MovePath {
            namespace_id: namespace_id.clone(),
            from_path: normalized_path_for_fingerprint(from_path)?,
            to_path: normalized_path_for_fingerprint(to_path)?,
            mode: *mode,
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

pub(crate) fn plan_path_mutation_against_state(
    namespace_id: &NamespaceId,
    intent: &PathMutationIntent,
    head: &HeadState,
    metadata_state: &MetadataState,
) -> Result<PlannedPathMutation, CoreError> {
    let commit_id = intent.commit_id().clone();
    let path_intent_fingerprint = path_intent_fingerprint_for_path_intent(namespace_id, intent)?;
    let view = PathPlanningView {
        head,
        metadata_state,
    };
    let commit_request = match intent {
        PathMutationIntent::CreateDir { absolute_path, .. } => {
            plan_create_dir(absolute_path, &commit_id, &view)?
        }
        PathMutationIntent::PutFile {
            absolute_path,
            content_ref,
            behavior,
            ..
        } => plan_put_file_content_ref(
            absolute_path,
            content_ref.clone(),
            *behavior,
            &commit_id,
            &view,
        )?,
        PathMutationIntent::DeletePath {
            absolute_path,
            recursive,
            ..
        } => plan_delete_path(absolute_path, *recursive, &commit_id, &view)?,
        PathMutationIntent::MovePath {
            from_path,
            to_path,
            mode,
            ..
        } => plan_move_path(from_path, to_path, *mode, &commit_id, &view)?,
        PathMutationIntent::CopyFilePath {
            from_path, to_path, ..
        } => plan_copy_file_path(from_path, to_path, &commit_id, &view)?,
        PathMutationIntent::RestoreRevision {
            absolute_path,
            source_revision_no,
            ..
        } => plan_restore_revision(absolute_path, *source_revision_no, &commit_id, &view)?,
    };
    Ok(PlannedPathMutation {
        commit_id,
        path_intent_fingerprint,
        commit_request,
    })
}

struct PathPlanningView<'a> {
    head: &'a HeadState,
    metadata_state: &'a MetadataState,
}

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

fn child_name_absent_precondition(
    view: &PathPlanningView<'_>,
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

fn plan_create_dir(
    absolute_path: &str,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    let absolute_path = parse_mutation_path(absolute_path)?;
    reject_tombstoned_path_ancestor(
        view.metadata_state,
        &absolute_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    if lookup_path(
        view.metadata_state,
        &absolute_path,
        view.head.name_policy,
        view.head.seq,
    )
    .is_ok()
    {
        return Err(CoreError::DestinationExists(
            absolute_path.as_str().to_owned(),
        ));
    }
    let parent_inode = resolve_parent_directory(
        view.metadata_state,
        &absolute_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    let display_name = final_component(&absolute_path)?;
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::CreateDir {
            parent_inode,
            display_name: display_name.clone(),
        }],
        preconditions: vec![
            child_name_absent_precondition(view, parent_inode, &display_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: parent_inode,
            },
        ],
        message: None,
        annotations: None,
    })
}

fn plan_put_file_content_ref(
    absolute_path: &str,
    content_ref: ContentRef,
    behavior: PutFileBehavior,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    let absolute_path = parse_mutation_path(absolute_path)?;
    reject_tombstoned_path_ancestor(
        view.metadata_state,
        &absolute_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    let target = lookup_path(
        view.metadata_state,
        &absolute_path,
        view.head.name_policy,
        view.head.seq,
    );

    let mut ops = Vec::new();
    let mut working = view.metadata_state.clone();
    let mut next_inode_id = view.head.next_inode_id;
    let mut op_index = 0u32;
    let final_parent_inode = ensure_parent_directories(
        &absolute_path,
        view.head.seq,
        view.head.name_policy,
        &mut working,
        &mut ops,
        &mut next_inode_id,
        &mut op_index,
    )?;
    let final_name = final_component(&absolute_path)?;
    let mut preconditions = Vec::new();

    match target {
        Ok(existing) => {
            if behavior == PutFileBehavior::CreateOnly {
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
                .ok_or_else(|| CoreError::MissingPath(absolute_path.as_str().to_owned()))?;
            preconditions.push(binding_is_precondition(view, &existing)?);
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
        Err(VisiblePathError::PathNotFound { .. }) => {
            ops.push(ApiCommitOp::CreateFile {
                parent_inode: final_parent_inode,
                display_name: final_name.clone(),
                content_ref,
            });
            if view
                .metadata_state
                .visible_inode(final_parent_inode, view.head.seq)
                .is_some()
            {
                preconditions.push(child_name_absent_precondition(
                    view,
                    final_parent_inode,
                    &final_name,
                ));
                preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                    inode_id: final_parent_inode,
                });
            }
        }
        Err(other) => return Err(other.into()),
    }

    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops,
        preconditions,
        message: None,
        annotations: None,
    })
}

fn plan_delete_path(
    absolute_path: &str,
    recursive: bool,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    let absolute_path = parse_mutation_path(absolute_path)?;
    let resolved = view.metadata_state.resolve_visible_path(
        &absolute_path,
        view.head.name_policy,
        view.head.seq,
    )?;
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
                .visible_children(resolved.inode_id, view.head.seq);
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
    let mut preconditions = vec![binding_is_precondition(view, &resolved)?];
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

fn plan_move_path(
    from_path: &str,
    to_path: &str,
    mode: RenameMode,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    if mode != RenameMode::NoReplace {
        return Err(CoreError::CommitValidation(
            crate::commit::CommitValidationError::UnsupportedRenameMode { mode },
        ));
    }
    let from_path = parse_mutation_path(from_path)?;
    let to_path = parse_mutation_path(to_path)?;
    reject_tombstoned_path_ancestor(
        view.metadata_state,
        &from_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    reject_tombstoned_path_ancestor(
        view.metadata_state,
        &to_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    let source = view.metadata_state.resolve_visible_path(
        &from_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    let target_parent = resolve_parent_directory(
        view.metadata_state,
        &to_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    let target_name = final_component(&to_path)?;
    if lookup_path(
        view.metadata_state,
        &to_path,
        view.head.name_policy,
        view.head.seq,
    )
    .is_ok()
    {
        return Err(CoreError::DestinationExists(to_path.as_str().to_owned()));
    }
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::Rename {
            inode_id: source.inode_id,
            new_parent_inode: target_parent,
            new_display_name: target_name.clone(),
            mode,
        }],
        preconditions: vec![
            binding_is_precondition(view, &source)?,
            child_name_absent_precondition(view, target_parent, &target_name),
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

fn plan_copy_file_path(
    from_path: &str,
    to_path: &str,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    let from_path = parse_mutation_path(from_path)?;
    let to_path = parse_mutation_path(to_path)?;
    reject_tombstoned_path_ancestor(
        view.metadata_state,
        &from_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    reject_tombstoned_path_ancestor(
        view.metadata_state,
        &to_path,
        view.head.name_policy,
        view.head.seq,
    )?;

    let source = view.metadata_state.resolve_visible_path(
        &from_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    if source.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: from_path.as_str().to_owned(),
            kind: source.inode_kind,
        });
    }

    if lookup_path(
        view.metadata_state,
        &to_path,
        view.head.name_policy,
        view.head.seq,
    )
    .is_ok()
    {
        return Err(CoreError::DestinationExists(to_path.as_str().to_owned()));
    }

    let revision = view
        .metadata_state
        .latest_revision_head_at_seq(source.inode_id, view.head.seq)
        .ok_or_else(|| CoreError::MissingPath(from_path.as_str().to_owned()))?;

    let target_parent = resolve_parent_directory(
        view.metadata_state,
        &to_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    let target_name = final_component(&to_path)?;
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::CreateFile {
            parent_inode: target_parent,
            display_name: target_name.clone(),
            content_ref: revision.content_ref,
        }],
        preconditions: vec![
            binding_is_precondition(view, &source)?,
            ApiCommitPrecondition::InodeRevisionIs {
                inode_id: source.inode_id,
                revision_no: revision.revision_no,
            },
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: source.inode_id,
            },
            child_name_absent_precondition(view, target_parent, &target_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: target_parent,
            },
        ],
        message: None,
        annotations: None,
    })
}

fn plan_restore_revision(
    absolute_path: &str,
    source_revision_no: RevisionNo,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    let absolute_path = parse_mutation_path(absolute_path)?;
    reject_tombstoned_path_ancestor(
        view.metadata_state,
        &absolute_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    let target = view.metadata_state.resolve_visible_path(
        &absolute_path,
        view.head.name_policy,
        view.head.seq,
    )?;
    if target.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: absolute_path.as_str().to_owned(),
            kind: target.inode_kind,
        });
    }
    let revision = view
        .metadata_state
        .latest_revision_head_at_seq(target.inode_id, view.head.seq)
        .ok_or_else(|| CoreError::MissingPath(absolute_path.as_str().to_owned()))?;

    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::RestoreRevision {
            inode_id: target.inode_id,
            source_revision_no,
            base_revision_no: revision.revision_no,
        }],
        preconditions: vec![
            binding_is_precondition(view, &target)?,
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

fn ensure_parent_directories(
    absolute_path: &AbsolutePath,
    committed_seq: ChangeSeq,
    name_policy: loon_api::NamePolicy,
    working: &mut MetadataState,
    ops: &mut Vec<ApiCommitOp>,
    next_inode_id: &mut InodeId,
    op_index: &mut u32,
) -> Result<InodeId, CoreError> {
    let components = absolute_path.components();
    if components.len() <= 1 {
        return Ok(InodeId(1));
    }

    let mut current_inode = InodeId(1);
    for component in &components[..components.len() - 1] {
        let display_name = component.to_display_name();
        let name_key = NameKey::for_display_name(name_policy, &display_name);
        if let Some(child) = working.visible_child(current_inode, name_key.as_str(), committed_seq)
        {
            let inode = working
                .visible_inode(child.child_inode_id, committed_seq)
                .ok_or_else(|| CoreError::MissingPath(component.as_str().to_owned()))?;
            if inode.inode_kind != InodeKind::Dir {
                return Err(CoreError::NonDirectoryPathComponent(
                    component.as_str().to_owned(),
                ));
            }
            current_inode = child.child_inode_id;
            continue;
        }

        ops.push(ApiCommitOp::CreateDir {
            parent_inode: current_inode,
            display_name: display_name.as_str().to_owned(),
        });
        let allocated = *next_inode_id;
        *next_inode_id = InodeId(next_inode_id.0.saturating_add(1));
        let delta_index = op_index.saturating_mul(2);
        let applied = working.apply_committed_wal_deltas(
            committed_seq,
            &[
                WalDelta::CreateInode {
                    delta_index,
                    inode_id: allocated,
                    inode_kind: InodeKind::Dir,
                },
                WalDelta::BindDirentry {
                    delta_index: delta_index.saturating_add(1),
                    parent_inode: current_inode,
                    name_key: name_key.as_str().to_owned(),
                    display_name: display_name.as_str().to_owned(),
                    child_inode: allocated,
                },
            ],
        )?;
        *working = applied.metadata_state;
        *op_index = op_index.saturating_add(1);
        current_inode = allocated;
    }
    Ok(current_inode)
}

fn resolve_parent_directory(
    metadata_state: &MetadataState,
    absolute_path: &AbsolutePath,
    name_policy: loon_api::NamePolicy,
    seq: ChangeSeq,
) -> Result<InodeId, CoreError> {
    let Some(parent_path) = absolute_path.parent() else {
        return Ok(InodeId(1));
    };
    if parent_path.is_root() {
        return Ok(InodeId(1));
    }
    let resolved = metadata_state.resolve_visible_path(&parent_path, name_policy, seq)?;
    if resolved.inode_kind != InodeKind::Dir {
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
    use crate::content::store_bytes_as_content;
    use crate::context::MutationContext;
    use crate::error::ErrorCode;
    use crate::metadata::{DirentryBindRecord, InodeRecord};
    use crate::namespace::lifecycle::bootstrap_namespace;
    use crate::path::mutation::{delete_path, put_file_bytes};
    use loon_api::v0::{CommitOp, CommitPrecondition, CommitRequest as ApiCommitRequest};
    use loon_api::RevisionNo;
    use loon_objectstore::fs::LocalFsStore;
    use tempfile::tempdir;

    fn test_context() -> MutationContext {
        MutationContext {
            writer_id: "writer".to_owned(),
            writer_version: "test".to_owned(),
            now_ms: 1,
            lease_duration_ms: 60_000,
        }
    }

    fn setup_namespace() -> (
        tempfile::TempDir,
        LocalFsStore,
        NamespaceId,
        MutationContext,
    ) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        (temp_dir, store, namespace_id, context)
    }

    fn plan_against_current_state(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
    ) -> PlannedPathMutation {
        let basis = load_verified_namespace_basis(store, namespace_id).expect("basis");
        PathPlanner::new(store)
            .plan_against_state(namespace_id, intent, &basis.head, &basis.metadata_state)
            .expect("plan")
    }

    #[test]
    fn path_intent_fingerprint_normalizes_paths() {
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

    #[test]
    fn path_intent_fingerprint_changes_when_logical_inputs_change() {
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

    #[test]
    fn path_intent_and_core_commit_fingerprints_use_distinct_domains() {
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

    #[test]
    fn create_dir_plan_contains_semantic_op_and_target_absence_precondition() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace();
        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::CreateDir {
                commit_id: CommitId::parse("mkdir-docs").expect("valid commit id"),
                absolute_path: "/docs".to_owned(),
            },
        );

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

    #[test]
    fn put_file_plan_auto_creates_missing_parent_directories() {
        let (_temp_dir, store, namespace_id, _context) = setup_namespace();
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello").expect("stage");
        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-nested").expect("valid commit id"),
                absolute_path: "/docs/nested/a.txt".to_owned(),
                content_ref: staged.content_ref.clone(),
                behavior: PutFileBehavior::CreateOnly,
            },
        );

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

    #[test]
    fn move_path_plan_contains_binding_and_target_absence_preconditions() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace();
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/a.txt",
            b"hello",
            PutFileBehavior::CreateOnly,
            &context,
            Some("seed-file"),
        )
        .expect("seed file");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::MovePath {
                commit_id: CommitId::parse("move-file").expect("valid commit id"),
                from_path: "/docs/a.txt".to_owned(),
                to_path: "/docs/b.txt".to_owned(),
                mode: RenameMode::NoReplace,
            },
        );

        assert!(matches!(
            planned.commit_request.ops.as_slice(),
            [CommitOp::Rename {
                new_display_name,
                mode: RenameMode::NoReplace,
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

    #[test]
    fn binding_is_precondition_reports_invalid_durable_name_key_as_namespace_corrupt() {
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

    #[test]
    fn copy_file_plan_validates_source_revision_and_target_absence() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace();
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/a.txt",
            b"hello",
            PutFileBehavior::CreateOnly,
            &context,
            Some("seed-copy-source"),
        )
        .expect("seed file");

        let planned = plan_against_current_state(
            &store,
            &namespace_id,
            &PathMutationIntent::CopyFilePath {
                commit_id: CommitId::parse("copy-file").expect("valid commit id"),
                from_path: "/docs/a.txt".to_owned(),
                to_path: "/docs/copy.txt".to_owned(),
            },
        );

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

    #[test]
    fn tombstoned_ancestor_blocks_descendant_planning() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace();
        put_file_bytes(
            &store,
            &namespace_id,
            "/dead/file.txt",
            b"hello",
            PutFileBehavior::CreateOnly,
            &context,
            Some("seed-dead-tree"),
        )
        .expect("seed file");
        delete_path(
            &store,
            &namespace_id,
            "/dead",
            &context,
            Some("delete-dead-tree"),
        )
        .expect("delete tree");
        let staged = store_bytes_as_content(&store, &namespace_id, b"new").expect("stage");
        let basis = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        let error = PathPlanner::new(&store)
            .plan_against_state(
                &namespace_id,
                &PathMutationIntent::PutFile {
                    commit_id: CommitId::parse("put-under-dead").expect("valid commit id"),
                    absolute_path: "/dead/new.txt".to_owned(),
                    content_ref: staged.content_ref,
                    behavior: PutFileBehavior::CreateOnly,
                },
                &basis.head,
                &basis.metadata_state,
            )
            .expect_err("tombstoned ancestor");

        assert_eq!(error.code(), ErrorCode::TombstoneConflict);
    }
}
