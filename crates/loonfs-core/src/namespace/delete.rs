//! Terminal namespace status published as successive manifests.

use crate::checkpoint::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use crate::error::{CoreError, Result};
use crate::limits::RETIREMENT_PUBLICATION_BUDGET_MS;
use crate::namespace::control::load_current_manifest;
use crate::namespace::control_snapshot::load_control_snapshot;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::options::DeleteNamespaceOptions;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::control::{AcquiredWriter, NamespaceStatus};
use loonfs_api::{DeleteNamespaceResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    acquired_writer: AcquiredWriter,
) -> Result<DeleteNamespaceResponse> {
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    loop {
        let snapshot = load_control_snapshot(store, namespace_id).await?;
        let head = &snapshot.head;
        super::control::ensure_namespace_live(head)?;
        ensure_writer_not_fenced(head, &acquired_writer)?;
        if let Some(expected) = options.expected_head_seq {
            if head.seq != expected {
                return Err(CoreError::StaleHeadPrecondition {
                    assertion_index: None,
                    expected,
                    actual: head.seq,
                });
            }
        }
        let mut payload = snapshot.root.envelope.payload().clone();
        payload.manifest_no = payload
            .manifest_no
            .successor()
            .map_err(|error| CoreError::Internal(format!("manifest number {error}")))?;
        payload.head_seq = head.seq;
        payload.head_commit_id = head.head_commit_id.clone();
        payload.next_inode_id = head.next_inode_id;
        payload.status = NamespaceStatus::Deleted {
            reclaim_after_ms: None,
        };
        let manifest = encode_manifest(payload)?;
        if matches!(
            publish_manifest(
                store,
                namespace_id,
                &manifest,
                Some(snapshot.root.state.manifest.manifest_no),
                &timer,
                started_ms
            )
            .await?,
            ManifestPublicationOutcome::Published(_)
        ) {
            return Ok(DeleteNamespaceResponse {
                namespace_id: namespace_id.clone(),
                head_seq: head.seq,
            });
        }
    }
}

pub(crate) async fn retire_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    grace_window_ms: u64,
    call_now_ms: u64,
    timer: &dyn MonotonicTimer,
    started_ms: u64,
) -> Result<u64> {
    let deadline = call_now_ms
        .checked_add(grace_window_ms.max(crate::limits::NAMESPACE_RETIREMENT_GRACE_MS))
        .ok_or_else(|| CoreError::Internal("namespace retirement deadline overflow".to_owned()))?;
    loop {
        let current = load_current_manifest(store, namespace_id).await?;
        let mut payload = current.envelope.payload().clone();
        if !payload.status.is_deleted() {
            return Err(CoreError::NamespaceCorrupt(
                "a deleted namespace became active".to_owned(),
            ));
        }
        if let Some(deadline) = payload.status.reclaim_after_ms() {
            return Ok(deadline);
        }
        payload.manifest_no = payload
            .manifest_no
            .successor()
            .map_err(|error| CoreError::Internal(format!("manifest number {error}")))?;
        payload.status = NamespaceStatus::Deleted {
            reclaim_after_ms: Some(deadline),
        };
        let manifest = encode_manifest(payload)?;
        let elapsed_ms = timer.monotonic_now_ms().saturating_sub(started_ms);
        if elapsed_ms > RETIREMENT_PUBLICATION_BUDGET_MS {
            return Err(CoreError::MetadataPublicationBudgetExceeded {
                elapsed_ms,
                budget_ms: RETIREMENT_PUBLICATION_BUDGET_MS,
            });
        }
        if matches!(
            publish_manifest(
                store,
                namespace_id,
                &manifest,
                Some(current.state.manifest.manifest_no),
                timer,
                started_ms
            )
            .await?,
            ManifestPublicationOutcome::Published(_)
        ) {
            return Ok(deadline);
        }
    }
}
