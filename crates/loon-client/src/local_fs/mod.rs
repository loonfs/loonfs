mod events;
mod observe;
mod paths;

pub use events::{
    reduce_fs_event_batch, FsEventReductionError, NativeFsObjectId, NormalizedFsEvent,
    NormalizedFsEventBatch, ReducedLocalObservationIntent,
};
pub use observe::{
    observe_delete_path, observe_local_path, observe_move_path, observe_subtree_path,
    ObserveDeleteError, ObserveDeleteReport, ObserveLocalError, ObserveLocalReport,
    ObserveMoveError, ObserveMoveReport, ObserveSubtreeError, ObserveSubtreeReport,
    ObservedPathKind,
};
pub use paths::{join_under_mirror_root, relative_path_is_under_prefix, NamespacePathIndex};
