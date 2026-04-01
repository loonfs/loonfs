use super::paths::relative_path_is_under_prefix;
use loon_types::InodeKind;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NativeFsObjectId(String);

impl NativeFsObjectId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedFsEventBatch {
    pub events: Vec<NormalizedFsEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NormalizedFsEvent {
    Created {
        relative_path: String,
        inode_kind: Option<InodeKind>,
        native_object_id: Option<NativeFsObjectId>,
    },
    Modified {
        relative_path: String,
        inode_kind: Option<InodeKind>,
        native_object_id: Option<NativeFsObjectId>,
    },
    Removed {
        relative_path: String,
        inode_kind: Option<InodeKind>,
        native_object_id: Option<NativeFsObjectId>,
    },
    Renamed {
        from_relative_path: String,
        to_relative_path: String,
        inode_kind: Option<InodeKind>,
        native_object_id: Option<NativeFsObjectId>,
    },
    RescanSubtree {
        relative_path: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReducedLocalObservationIntent {
    ObserveLocal {
        relative_path: String,
    },
    ObserveDelete {
        relative_path: String,
    },
    ObserveMove {
        from_relative_path: String,
        to_relative_path: String,
    },
    ObserveSubtree {
        relative_path: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(tag = "reason_code", rename_all = "snake_case")]
pub enum FsEventReductionError {
    #[error("filesystem event batch contains an invalid relative path")]
    InvalidRelativePath {
        affected_relative_paths: Vec<String>,
        suggested_subtree_roots: Vec<String>,
    },
    #[error("filesystem event batch contains multiple rename candidates for one source path")]
    AmbiguousRenameSource {
        affected_relative_paths: Vec<String>,
        suggested_subtree_roots: Vec<String>,
    },
    #[error("filesystem event batch contains multiple rename candidates for one target path")]
    AmbiguousRenameTarget {
        affected_relative_paths: Vec<String>,
        suggested_subtree_roots: Vec<String>,
    },
    #[error(
        "filesystem event batch reuses one native object id across multiple rename candidates"
    )]
    AmbiguousNativeObjectId {
        affected_relative_paths: Vec<String>,
        suggested_subtree_roots: Vec<String>,
    },
    #[error("filesystem event batch contains contradictory path events")]
    ContradictoryPathEvents {
        affected_relative_paths: Vec<String>,
        suggested_subtree_roots: Vec<String>,
    },
    #[error("filesystem event batch contains incompatible kind hints for one rename candidate")]
    InvalidCrossKindRenameHint {
        affected_relative_paths: Vec<String>,
        suggested_subtree_roots: Vec<String>,
    },
}

impl FsEventReductionError {
    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::InvalidRelativePath { .. } => "invalid_relative_path",
            Self::AmbiguousRenameSource { .. } => "ambiguous_rename_source",
            Self::AmbiguousRenameTarget { .. } => "ambiguous_rename_target",
            Self::AmbiguousNativeObjectId { .. } => "ambiguous_native_object_id",
            Self::ContradictoryPathEvents { .. } => "contradictory_path_events",
            Self::InvalidCrossKindRenameHint { .. } => "invalid_cross_kind_rename_hint",
        }
    }
}

#[derive(Debug, Clone)]
struct EditEvent {
    order_index: usize,
    relative_path: String,
    inode_kind: Option<InodeKind>,
    native_object_id: Option<NativeFsObjectId>,
}

#[derive(Debug, Clone)]
struct RemoveEvent {
    order_index: usize,
    relative_path: String,
    inode_kind: Option<InodeKind>,
    native_object_id: Option<NativeFsObjectId>,
}

#[derive(Debug, Clone)]
struct RenameEvent {
    order_index: usize,
    from_relative_path: String,
    to_relative_path: String,
}

#[derive(Debug, Clone)]
struct RescanEvent {
    order_index: usize,
    relative_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimpleKind {
    File,
    Dir,
}

#[derive(Debug, Clone)]
struct OrderedIntent {
    order_index: usize,
    sort_key: String,
    intent: ReducedLocalObservationIntent,
}

pub fn reduce_fs_event_batch(
    batch: &NormalizedFsEventBatch,
) -> Result<Vec<ReducedLocalObservationIntent>, FsEventReductionError> {
    let mut edit_events = Vec::new();
    let mut remove_events = Vec::new();
    let mut rename_events = Vec::new();
    let mut rescan_events = Vec::new();

    for (order_index, event) in batch.events.iter().enumerate() {
        match event {
            NormalizedFsEvent::Created {
                relative_path,
                inode_kind,
                native_object_id,
            } => {
                validate_relative_path(relative_path, false)?;
                edit_events.push(EditEvent {
                    order_index,
                    relative_path: relative_path.clone(),
                    inode_kind: inode_kind.clone(),
                    native_object_id: native_object_id.clone(),
                });
            }
            NormalizedFsEvent::Modified {
                relative_path,
                inode_kind,
                native_object_id,
            } => {
                validate_relative_path(relative_path, false)?;
                edit_events.push(EditEvent {
                    order_index,
                    relative_path: relative_path.clone(),
                    inode_kind: inode_kind.clone(),
                    native_object_id: native_object_id.clone(),
                });
            }
            NormalizedFsEvent::Removed {
                relative_path,
                inode_kind,
                native_object_id,
            } => {
                validate_relative_path(relative_path, false)?;
                remove_events.push(RemoveEvent {
                    order_index,
                    relative_path: relative_path.clone(),
                    inode_kind: inode_kind.clone(),
                    native_object_id: native_object_id.clone(),
                });
            }
            NormalizedFsEvent::Renamed {
                from_relative_path,
                to_relative_path,
                inode_kind: _,
                native_object_id: _,
            } => {
                validate_relative_path(from_relative_path, false)?;
                validate_relative_path(to_relative_path, false)?;
                rename_events.push(RenameEvent {
                    order_index,
                    from_relative_path: from_relative_path.clone(),
                    to_relative_path: to_relative_path.clone(),
                });
            }
            NormalizedFsEvent::RescanSubtree { relative_path } => {
                validate_relative_path(relative_path, true)?;
                rescan_events.push(RescanEvent {
                    order_index,
                    relative_path: relative_path.clone(),
                });
            }
        }
    }

    let mut intents = Vec::new();
    let mut absorbed_roots = BTreeSet::new();

    reduce_explicit_renames(&rename_events, &mut intents, &mut absorbed_roots)?;

    reduce_native_id_moves(
        &edit_events,
        &remove_events,
        &mut intents,
        &mut absorbed_roots,
    )?;

    let remaining_edits = edit_events
        .into_iter()
        .filter(|event| !path_is_at_or_under_any_root(&event.relative_path, &absorbed_roots))
        .collect::<Vec<_>>();
    let remaining_removes = remove_events
        .into_iter()
        .filter(|event| !path_is_at_or_under_any_root(&event.relative_path, &absorbed_roots))
        .collect::<Vec<_>>();
    let remaining_rescans = rescan_events
        .into_iter()
        .filter(|event| !path_is_at_or_under_any_root(&event.relative_path, &absorbed_roots))
        .collect::<Vec<_>>();

    validate_remaining_path_consistency(&remaining_edits, &remaining_removes, &remaining_rescans)?;

    let delete_roots = select_delete_roots(&remaining_removes);
    for (relative_path, order_index) in &delete_roots {
        intents.push(OrderedIntent {
            order_index: *order_index,
            sort_key: format!("delete:{relative_path}"),
            intent: ReducedLocalObservationIntent::ObserveDelete {
                relative_path: relative_path.clone(),
            },
        });
        absorbed_roots.insert(relative_path.clone());
    }

    let remaining_edits = remaining_edits
        .into_iter()
        .filter(|event| !path_is_at_or_under_any_root(&event.relative_path, &absorbed_roots))
        .collect::<Vec<_>>();
    let remaining_rescans = remaining_rescans
        .into_iter()
        .filter(|event| !path_is_at_or_under_any_root(&event.relative_path, &absorbed_roots))
        .collect::<Vec<_>>();

    let subtree_roots = select_subtree_roots(&remaining_edits, &remaining_rescans)?;
    for (relative_path, order_index) in &subtree_roots {
        intents.push(OrderedIntent {
            order_index: *order_index,
            sort_key: format!("subtree:{relative_path}"),
            intent: ReducedLocalObservationIntent::ObserveSubtree {
                relative_path: relative_path.clone(),
            },
        });
        absorbed_roots.insert(relative_path.clone());
    }

    let mut local_observe_by_path = BTreeMap::<String, usize>::new();
    for event in remaining_edits {
        if path_is_at_or_under_any_root(&event.relative_path, &absorbed_roots) {
            continue;
        }
        local_observe_by_path
            .entry(event.relative_path.clone())
            .and_modify(|earliest| *earliest = (*earliest).min(event.order_index))
            .or_insert(event.order_index);
    }

    for (relative_path, order_index) in local_observe_by_path {
        intents.push(OrderedIntent {
            order_index,
            sort_key: format!("local:{relative_path}"),
            intent: ReducedLocalObservationIntent::ObserveLocal { relative_path },
        });
    }

    intents.sort_by(|left, right| {
        left.order_index
            .cmp(&right.order_index)
            .then_with(|| left.sort_key.cmp(&right.sort_key))
    });

    Ok(intents.into_iter().map(|entry| entry.intent).collect())
}

fn reduce_explicit_renames(
    rename_events: &[RenameEvent],
    intents: &mut Vec<OrderedIntent>,
    absorbed_roots: &mut BTreeSet<String>,
) -> Result<(), FsEventReductionError> {
    let mut source_counts = BTreeMap::<String, Vec<String>>::new();
    let mut target_counts = BTreeMap::<String, Vec<String>>::new();
    for event in rename_events {
        source_counts
            .entry(event.from_relative_path.clone())
            .or_default()
            .push(event.to_relative_path.clone());
        target_counts
            .entry(event.to_relative_path.clone())
            .or_default()
            .push(event.from_relative_path.clone());
    }

    for (source, targets) in &source_counts {
        if targets.len() > 1 {
            return Err(FsEventReductionError::AmbiguousRenameSource {
                affected_relative_paths: vec![source.clone()],
                suggested_subtree_roots: suggested_subtree_roots(std::slice::from_ref(source)),
            });
        }
    }
    for (target, sources) in &target_counts {
        if sources.len() > 1 {
            return Err(FsEventReductionError::AmbiguousRenameTarget {
                affected_relative_paths: vec![target.clone()],
                suggested_subtree_roots: suggested_subtree_roots(std::slice::from_ref(target)),
            });
        }
    }

    let mut sorted = rename_events.to_vec();
    sorted.sort_by(|left, right| {
        left.order_index
            .cmp(&right.order_index)
            .then_with(|| left.from_relative_path.cmp(&right.from_relative_path))
            .then_with(|| left.to_relative_path.cmp(&right.to_relative_path))
    });

    for event in sorted {
        intents.push(OrderedIntent {
            order_index: event.order_index,
            sort_key: format!(
                "move:{}->{}",
                event.from_relative_path, event.to_relative_path
            ),
            intent: ReducedLocalObservationIntent::ObserveMove {
                from_relative_path: event.from_relative_path.clone(),
                to_relative_path: event.to_relative_path.clone(),
            },
        });
        absorbed_roots.insert(event.from_relative_path);
        absorbed_roots.insert(event.to_relative_path);
    }

    Ok(())
}

fn reduce_native_id_moves(
    edit_events: &[EditEvent],
    remove_events: &[RemoveEvent],
    intents: &mut Vec<OrderedIntent>,
    absorbed_roots: &mut BTreeSet<String>,
) -> Result<(), FsEventReductionError> {
    let remaining_edits = edit_events
        .iter()
        .filter(|event| !path_is_at_or_under_any_root(&event.relative_path, absorbed_roots))
        .collect::<Vec<_>>();
    let remaining_removes = remove_events
        .iter()
        .filter(|event| !path_is_at_or_under_any_root(&event.relative_path, absorbed_roots))
        .collect::<Vec<_>>();

    let mut native_created = BTreeMap::<String, Vec<&EditEvent>>::new();
    let mut native_removed = BTreeMap::<String, Vec<&RemoveEvent>>::new();
    for event in remaining_edits {
        let Some(native_object_id) = &event.native_object_id else {
            continue;
        };
        native_created
            .entry(native_object_id.as_str().to_owned())
            .or_default()
            .push(event);
    }
    for event in remaining_removes {
        let Some(native_object_id) = &event.native_object_id else {
            continue;
        };
        native_removed
            .entry(native_object_id.as_str().to_owned())
            .or_default()
            .push(event);
    }

    let all_native_ids = native_created
        .keys()
        .chain(native_removed.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let all_paths = edit_events
        .iter()
        .map(|event| event.relative_path.clone())
        .chain(
            remove_events
                .iter()
                .map(|event| event.relative_path.clone()),
        )
        .collect::<BTreeSet<_>>();

    for native_id in all_native_ids {
        let creates = native_created.get(&native_id).cloned().unwrap_or_default();
        let removes = native_removed.get(&native_id).cloned().unwrap_or_default();
        if creates.len() > 1 || removes.len() > 1 {
            let affected_paths = creates
                .iter()
                .map(|event| event.relative_path.clone())
                .chain(removes.iter().map(|event| event.relative_path.clone()))
                .collect::<Vec<_>>();
            return Err(FsEventReductionError::AmbiguousNativeObjectId {
                affected_relative_paths: affected_paths.clone(),
                suggested_subtree_roots: suggested_subtree_roots(&affected_paths),
            });
        }
        if creates.len() != 1 || removes.len() != 1 {
            continue;
        }

        let create = creates[0];
        let remove = removes[0];
        let created_kind = infer_simple_kind(
            &create.relative_path,
            create.inode_kind.as_ref(),
            &all_paths,
        )?;
        let removed_kind = infer_simple_kind(
            &remove.relative_path,
            remove.inode_kind.as_ref(),
            &all_paths,
        )?;
        if created_kind != removed_kind {
            let affected_paths = vec![remove.relative_path.clone(), create.relative_path.clone()];
            return Err(FsEventReductionError::InvalidCrossKindRenameHint {
                affected_relative_paths: affected_paths.clone(),
                suggested_subtree_roots: suggested_subtree_roots(&affected_paths),
            });
        }

        let order_index = remove.order_index.min(create.order_index);
        intents.push(OrderedIntent {
            order_index,
            sort_key: format!("move:{}->{}", remove.relative_path, create.relative_path),
            intent: ReducedLocalObservationIntent::ObserveMove {
                from_relative_path: remove.relative_path.clone(),
                to_relative_path: create.relative_path.clone(),
            },
        });
        absorbed_roots.insert(remove.relative_path.clone());
        absorbed_roots.insert(create.relative_path.clone());
    }

    Ok(())
}

fn validate_remaining_path_consistency(
    edit_events: &[EditEvent],
    remove_events: &[RemoveEvent],
    rescan_events: &[RescanEvent],
) -> Result<(), FsEventReductionError> {
    let mut removed_by_path = BTreeMap::<String, usize>::new();
    let mut rescans_by_path = BTreeMap::<String, usize>::new();
    let mut edits_by_path = BTreeMap::<String, Vec<&EditEvent>>::new();

    for event in remove_events {
        removed_by_path
            .entry(event.relative_path.clone())
            .and_modify(|earliest| *earliest = (*earliest).min(event.order_index))
            .or_insert(event.order_index);
    }
    for event in rescan_events {
        rescans_by_path
            .entry(event.relative_path.clone())
            .and_modify(|earliest| *earliest = (*earliest).min(event.order_index))
            .or_insert(event.order_index);
    }
    for event in edit_events {
        edits_by_path
            .entry(event.relative_path.clone())
            .or_default()
            .push(event);
    }

    for relative_path in removed_by_path.keys() {
        if edits_by_path.contains_key(relative_path) || rescans_by_path.contains_key(relative_path)
        {
            let affected_paths = vec![relative_path.clone()];
            return Err(FsEventReductionError::ContradictoryPathEvents {
                affected_relative_paths: affected_paths.clone(),
                suggested_subtree_roots: suggested_subtree_roots(&affected_paths),
            });
        }
    }

    let all_paths = edit_events
        .iter()
        .map(|event| event.relative_path.clone())
        .chain(
            remove_events
                .iter()
                .map(|event| event.relative_path.clone()),
        )
        .chain(
            rescan_events
                .iter()
                .map(|event| event.relative_path.clone()),
        )
        .collect::<BTreeSet<_>>();

    for (relative_path, edits) in edits_by_path {
        let mut saw_file = false;
        let mut saw_dir = false;
        for event in edits {
            match infer_simple_kind(&relative_path, event.inode_kind.as_ref(), &all_paths)? {
                SimpleKind::File => saw_file = true,
                SimpleKind::Dir => saw_dir = true,
            }
        }
        if rescans_by_path.contains_key(&relative_path) && saw_file {
            let affected_paths = vec![relative_path.clone()];
            return Err(FsEventReductionError::ContradictoryPathEvents {
                affected_relative_paths: affected_paths.clone(),
                suggested_subtree_roots: suggested_subtree_roots(&affected_paths),
            });
        }
        if saw_file && saw_dir {
            let affected_paths = vec![relative_path.clone()];
            return Err(FsEventReductionError::ContradictoryPathEvents {
                affected_relative_paths: affected_paths.clone(),
                suggested_subtree_roots: suggested_subtree_roots(&affected_paths),
            });
        }
    }

    Ok(())
}

fn select_delete_roots(remove_events: &[RemoveEvent]) -> BTreeMap<String, usize> {
    let mut paths_by_length = remove_events
        .iter()
        .map(|event| event.relative_path.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    paths_by_length.sort_by(|left, right| {
        left.matches('/')
            .count()
            .cmp(&right.matches('/').count())
            .then_with(|| left.cmp(right))
    });

    let earliest_by_path =
        remove_events
            .iter()
            .fold(BTreeMap::<String, usize>::new(), |mut acc, event| {
                acc.entry(event.relative_path.clone())
                    .and_modify(|earliest| *earliest = (*earliest).min(event.order_index))
                    .or_insert(event.order_index);
                acc
            });

    let mut selected = BTreeMap::new();
    let mut selected_roots = BTreeSet::new();
    for relative_path in paths_by_length {
        if path_is_at_or_under_any_root(&relative_path, &selected_roots) {
            continue;
        }
        selected_roots.insert(relative_path.clone());
        selected.insert(
            relative_path.clone(),
            *earliest_by_path
                .get(&relative_path)
                .expect("delete root should have order index"),
        );
    }
    selected
}

fn select_subtree_roots(
    edit_events: &[EditEvent],
    rescan_events: &[RescanEvent],
) -> Result<BTreeMap<String, usize>, FsEventReductionError> {
    let all_paths = edit_events
        .iter()
        .map(|event| event.relative_path.clone())
        .chain(
            rescan_events
                .iter()
                .map(|event| event.relative_path.clone()),
        )
        .collect::<BTreeSet<_>>();

    let mut candidate_roots = BTreeMap::<String, usize>::new();
    for event in rescan_events {
        candidate_roots
            .entry(event.relative_path.clone())
            .and_modify(|earliest| *earliest = (*earliest).min(event.order_index))
            .or_insert(event.order_index);
    }

    let file_edit_events = edit_events
        .iter()
        .filter_map(|event| {
            match infer_simple_kind(&event.relative_path, event.inode_kind.as_ref(), &all_paths) {
                Ok(SimpleKind::Dir) => {
                    candidate_roots
                        .entry(event.relative_path.clone())
                        .and_modify(|earliest| *earliest = (*earliest).min(event.order_index))
                        .or_insert(event.order_index);
                    None
                }
                Ok(SimpleKind::File) => Some(Ok(event)),
                Err(err) => Some(Err(err)),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    let unique_file_event_paths = file_edit_events
        .iter()
        .map(|event| event.relative_path.clone())
        .collect::<BTreeSet<_>>();
    let mut file_count_by_ancestor = BTreeMap::<String, usize>::new();
    for relative_path in &unique_file_event_paths {
        for ancestor in ancestor_directories(relative_path) {
            *file_count_by_ancestor.entry(ancestor).or_insert(0) += 1;
        }
    }

    for event in &file_edit_events {
        let Some(grouped_root) = ancestor_directories(&event.relative_path)
            .into_iter()
            .find(|ancestor| file_count_by_ancestor.get(ancestor).copied().unwrap_or(0) >= 2)
        else {
            continue;
        };
        candidate_roots
            .entry(grouped_root)
            .and_modify(|earliest| *earliest = (*earliest).min(event.order_index))
            .or_insert(event.order_index);
    }

    let mut roots = candidate_roots.keys().cloned().collect::<Vec<_>>();
    roots.sort_by(|left, right| {
        left.matches('/')
            .count()
            .cmp(&right.matches('/').count())
            .then_with(|| left.cmp(right))
    });

    let mut selected_roots = BTreeSet::new();
    let mut selected = BTreeMap::new();
    for relative_path in roots {
        if path_is_at_or_under_any_root(&relative_path, &selected_roots) {
            continue;
        }
        selected_roots.insert(relative_path.clone());
        selected.insert(
            relative_path.clone(),
            *candidate_roots
                .get(&relative_path)
                .expect("candidate subtree root should keep order index"),
        );
    }

    Ok(selected)
}

fn validate_relative_path(
    relative_path: &str,
    allow_empty: bool,
) -> Result<(), FsEventReductionError> {
    if relative_path.is_empty() {
        if allow_empty {
            return Ok(());
        }
        return Err(FsEventReductionError::InvalidRelativePath {
            affected_relative_paths: vec![relative_path.to_owned()],
            suggested_subtree_roots: Vec::new(),
        });
    }
    if relative_path.contains('\\') {
        return Err(FsEventReductionError::InvalidRelativePath {
            affected_relative_paths: vec![relative_path.to_owned()],
            suggested_subtree_roots: Vec::new(),
        });
    }
    let path = Path::new(relative_path);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(FsEventReductionError::InvalidRelativePath {
            affected_relative_paths: vec![relative_path.to_owned()],
            suggested_subtree_roots: Vec::new(),
        });
    }
    Ok(())
}

fn infer_simple_kind(
    relative_path: &str,
    hint: Option<&InodeKind>,
    all_paths: &BTreeSet<String>,
) -> Result<SimpleKind, FsEventReductionError> {
    if let Some(kind) = hint {
        return match kind {
            InodeKind::File => Ok(SimpleKind::File),
            InodeKind::Dir => Ok(SimpleKind::Dir),
            InodeKind::Symlink | InodeKind::Mount => Ok(SimpleKind::File),
        };
    }

    if all_paths
        .iter()
        .any(|other| other != relative_path && relative_path_is_under_prefix(other, relative_path))
    {
        return Ok(SimpleKind::Dir);
    }

    Ok(SimpleKind::File)
}

fn ancestor_directories(relative_path: &str) -> Vec<String> {
    let mut ancestors = Vec::new();
    let mut current = Path::new(relative_path)
        .parent()
        .map(normalize_relative_path);
    while let Some(parent) = current {
        ancestors.push(parent.clone());
        if parent.is_empty() {
            break;
        }
        current = Path::new(&parent).parent().map(normalize_relative_path);
    }
    ancestors
}

fn normalize_relative_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn path_is_at_or_under_any_root(relative_path: &str, roots: &BTreeSet<String>) -> bool {
    roots
        .iter()
        .any(|root| root == relative_path || relative_path_is_under_prefix(relative_path, root))
}

fn suggested_subtree_roots(paths: &[String]) -> Vec<String> {
    let mut roots = paths
        .iter()
        .map(|path| {
            Path::new(path)
                .parent()
                .map(normalize_relative_path)
                .unwrap_or_default()
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    roots.sort();
    roots
}
