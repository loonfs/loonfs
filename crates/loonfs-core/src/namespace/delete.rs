//! Namespace deletion status published as successive manifests.

use crate::checkpoint::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use crate::error::{CoreError, Result};
use crate::namespace::read_anchor::load_read_anchor;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::options::DeleteNamespaceOptions;
use crate::time::MonotonicTimer;
use loonfs_api::wire::control::{AcquiredWriter, NamespaceStatus};
use loonfs_api::{DeleteNamespaceResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    acquired_writer: AcquiredWriter,
    context: &crate::context::MutationContext,
    timer: &dyn MonotonicTimer,
    started_ms: u64,
) -> Result<DeleteNamespaceResponse> {
    loop {
        let anchor = load_read_anchor(store, namespace_id).await?;
        let head = &anchor.read_state;
        super::control::ensure_namespace_live(head)?;
        ensure_writer_not_fenced(head, &acquired_writer)?;
        if let Some(expected) = options.expected_head_seq {
            if head.seq != expected {
                return Err(CoreError::StaleHeadPrecondition {
                    precondition_index: None,
                    expected,
                    actual: head.seq,
                });
            }
        }
        // The delete barrier has drained admitted commits. Fold the final tail
        // before publishing a terminal manifest, so its rows and totals cover
        // the same head. A failed fold leaves the namespace active.
        if anchor.manifest.envelope.payload().last_folded_wal_no != head.wal_no {
            crate::checkpoint::ensure_metadata_publication_budget(timer, started_ms, namespace_id)?;
            crate::checkpoint::flush_wal(store, namespace_id).await?;
            continue;
        }
        let mut payload = anchor.manifest.envelope.payload().clone();
        payload.manifest_no = payload
            .manifest_no
            .successor()
            .map_err(|error| CoreError::Internal(format!("manifest number {error}")))?;
        payload.status = NamespaceStatus::Deleted {
            deleted_at_ms: context.now_ms,
        };
        let manifest = encode_manifest(payload)?;
        if matches!(
            publish_manifest(
                store,
                namespace_id,
                manifest,
                Some(anchor.manifest.state.manifest.manifest_no),
                timer,
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
