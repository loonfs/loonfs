use crate::conflict::{
    archive_conflict_artifact_with_hooks,
    discover_conflict_artifact_archives_for_namespace_with_hooks,
    discover_conflict_artifacts_for_namespace_with_hooks,
    restore_conflict_artifact_to_path_with_hooks, unarchive_conflict_artifact_with_hooks,
    ConflictArtifactError, RestoredConflictArtifact,
};
use crate::executor::{
    execute_next_client_action_with_hooks, ExecuteNextClientActionError, NextClientAction,
};
use crate::local_apply::LocalApplyError;
use crate::state_db::{
    ClientFileId, ConflictArtifactArchiveRow, ConflictArtifactRow, SqliteStateDb,
};
use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, PutMode};
use loon_sim::faults::{
    FaultPlan, InjectedClientFault, InjectedStoreErrorKind, InjectedStoreOperation,
};
use loon_types::{ClientMutationRequest, ClientMutationResponse, InodeId, NamespaceId};
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct FaultController {
    inner: Arc<Mutex<FaultState>>,
}

#[derive(Debug, Default)]
struct FaultState {
    remaining: Vec<InjectedClientFault>,
    trace: Vec<String>,
}

impl FaultController {
    pub fn new(plan: FaultPlan) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FaultState {
                remaining: plan.client_faults,
                trace: Vec::new(),
            })),
        }
    }

    pub fn trace_lines(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .trace
            .clone()
    }

    fn push_trace(&self, line: String) {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .trace
            .push(line);
    }

    fn take_matching<F>(&self, predicate: F) -> Option<InjectedClientFault>
    where
        F: Fn(&InjectedClientFault) -> bool,
    {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let index = guard.remaining.iter().position(predicate)?;
        Some(guard.remaining.remove(index))
    }
}

pub trait ClientExecutionHooks {
    fn checkpoint(&self, _checkpoint_id: &'static str) {}

    fn dispatch_error(&self, _checkpoint_id: &'static str) -> Option<String> {
        None
    }

    fn local_apply_error(
        &self,
        _operation: &'static str,
        _path: &Path,
    ) -> Result<(), LocalApplyError> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopClientExecutionHooks;

impl ClientExecutionHooks for NoopClientExecutionHooks {}

impl ClientExecutionHooks for FaultController {
    fn checkpoint(&self, checkpoint_id: &'static str) {
        if let Some(InjectedClientFault::CrashAfterStepOnce {
            checkpoint_id: matched,
        }) = self.take_matching(|fault| {
            matches!(
                fault,
                InjectedClientFault::CrashAfterStepOnce { checkpoint_id: candidate }
                    if candidate == checkpoint_id
            )
        }) {
            self.push_trace(format!(
                "fault crash_after_step_once checkpoint_id={matched}"
            ));
            panic!("injected_crash checkpoint_id={matched}");
        }
    }

    fn dispatch_error(&self, checkpoint_id: &'static str) -> Option<String> {
        let fault = self.take_matching(|fault| {
            matches!(
                fault,
                InjectedClientFault::DispatchErrorOnce {
                    checkpoint_id: candidate,
                    ..
                } if candidate == checkpoint_id
            )
        })?;
        let InjectedClientFault::DispatchErrorOnce {
            checkpoint_id,
            message,
        } = fault
        else {
            unreachable!("matched dispatch fault should have dispatch shape");
        };
        self.push_trace(format!(
            "fault dispatch_error_once checkpoint_id={} message={}",
            checkpoint_id, message
        ));
        Some(message)
    }

    fn local_apply_error(
        &self,
        operation: &'static str,
        path: &Path,
    ) -> Result<(), LocalApplyError> {
        let path_display = path.display().to_string();
        let fault = self.take_matching(|fault| {
            matches!(
                fault,
                InjectedClientFault::LocalApplyErrorOnce {
                    operation: candidate,
                    path_contains,
                } if candidate == operation && path_display.contains(path_contains)
            )
        });
        if fault.is_none() {
            return Ok(());
        }
        self.push_trace(format!(
            "fault local_apply_error_once operation={} path={}",
            operation, path_display
        ));
        Err(LocalApplyError {
            operation,
            path: path_display,
            source: std::io::Error::other("injected local apply failure"),
        })
    }
}

#[derive(Debug)]
pub struct FaultInjectingObjectStore<S> {
    inner: S,
    controller: FaultController,
}

impl<S> FaultInjectingObjectStore<S> {
    pub fn new(inner: S, controller: FaultController) -> Self {
        Self { inner, controller }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    pub fn controller(&self) -> &FaultController {
        &self.controller
    }
}

impl<S: ObjectStore> FaultInjectingObjectStore<S> {
    fn injected_store_error(
        &self,
        operation: InjectedStoreOperation,
        key: &str,
    ) -> Option<ObjectStoreError> {
        let fault = self.controller.take_matching(|fault| {
            matches!(
                fault,
                InjectedClientFault::StoreErrorOnce {
                    operation: candidate_operation,
                    key_contains,
                    ..
                } if *candidate_operation == operation && key.contains(key_contains)
            )
        })?;
        let InjectedClientFault::StoreErrorOnce {
            operation: matched_operation,
            key_contains,
            error_kind,
        } = fault
        else {
            unreachable!("matched store fault should have store shape");
        };
        self.controller.push_trace(format!(
            "fault store_error_once operation={:?} key_contains={} error_kind={:?}",
            matched_operation, key_contains, error_kind
        ));
        Some(map_store_error_kind(error_kind, key))
    }
}

impl<S: ObjectStore> ObjectStore for FaultInjectingObjectStore<S> {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        if let Some(error) = self.injected_store_error(InjectedStoreOperation::Head, key) {
            return Err(error);
        }
        self.inner.head(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        if let Some(error) = self.injected_store_error(InjectedStoreOperation::Get, key) {
            return Err(error);
        }
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if let Some(error) = self.injected_store_error(InjectedStoreOperation::Put, key) {
            return Err(error);
        }
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        if let Some(error) = self.injected_store_error(InjectedStoreOperation::Delete, key) {
            return Err(error);
        }
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        if let Some(error) = self.injected_store_error(InjectedStoreOperation::ListPrefix, prefix) {
            return Err(error);
        }
        self.inner.list_prefix(prefix)
    }
}

fn map_store_error_kind(kind: InjectedStoreErrorKind, key: &str) -> ObjectStoreError {
    match kind {
        InjectedStoreErrorKind::NotFound => ObjectStoreError::NotFound,
        InjectedStoreErrorKind::PreconditionFailed => ObjectStoreError::PreconditionFailed,
        InjectedStoreErrorKind::Conflict => ObjectStoreError::Conflict,
        InjectedStoreErrorKind::InvalidRange => ObjectStoreError::InvalidRange,
        InjectedStoreErrorKind::Transport => {
            ObjectStoreError::Transport(format!("injected transport failure for `{key}`"))
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::result_large_err)]
pub fn execute_next_client_action_with_faults<S, LP, IP, ITP, F>(
    db: &mut SqliteStateDb,
    store: &S,
    resolve_local_only_source_path: LP,
    resolve_inode_source_path: IP,
    resolve_inode_target_path: ITP,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    controller: &FaultController,
    dispatch: F,
) -> Result<Option<NextClientAction>, ExecuteNextClientActionError>
where
    S: ObjectStore,
    LP: FnOnce(&ClientFileId) -> Option<PathBuf>,
    IP: FnOnce(&NamespaceId, InodeId) -> Option<PathBuf>,
    ITP: FnOnce(&NamespaceId, InodeId, Option<InodeId>, &str) -> Option<PathBuf>,
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    execute_next_client_action_with_hooks(
        db,
        store,
        resolve_local_only_source_path,
        resolve_inode_source_path,
        resolve_inode_target_path,
        uploaded_at_ms,
        created_at_ms,
        controller,
        dispatch,
    )
}

pub fn discover_conflict_artifacts_for_namespace_with_faults<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    controller: &FaultController,
) -> Result<Vec<ConflictArtifactRow>, ConflictArtifactError> {
    discover_conflict_artifacts_for_namespace_with_hooks(db, store, namespace_id, controller)
}

pub fn discover_conflict_artifact_archives_for_namespace_with_faults<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    controller: &FaultController,
) -> Result<Vec<ConflictArtifactArchiveRow>, ConflictArtifactError> {
    discover_conflict_artifact_archives_for_namespace_with_hooks(
        db,
        store,
        namespace_id,
        controller,
    )
}

pub fn archive_conflict_artifact_with_faults<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    archived_at_ms: u64,
    controller: &FaultController,
) -> Result<ConflictArtifactArchiveRow, ConflictArtifactError> {
    archive_conflict_artifact_with_hooks(
        db,
        store,
        namespace_id,
        conflict_id,
        archived_at_ms,
        controller,
    )
}

pub fn unarchive_conflict_artifact_with_faults<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    controller: &FaultController,
) -> Result<(), ConflictArtifactError> {
    unarchive_conflict_artifact_with_hooks(db, store, namespace_id, conflict_id, controller)
}

pub fn restore_conflict_artifact_to_path_with_faults<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    destination_path: &Path,
    controller: &FaultController,
) -> Result<RestoredConflictArtifact, ConflictArtifactError> {
    restore_conflict_artifact_to_path_with_hooks(
        db,
        store,
        namespace_id,
        conflict_id,
        destination_path,
        controller,
    )
}

pub fn decode_fault_plan<T>(faults: Vec<T>) -> FaultPlan
where
    T: Into<InjectedClientFault>,
{
    FaultPlan::new(faults.into_iter().map(Into::into).collect())
}
