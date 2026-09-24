//! Namespace deletion status published as successive manifests.

use crate::checkpoint::publish::{update_manifest, ManifestChange};
use crate::error::{CoreError, Result};
use crate::namespace::read_anchor::load_read_anchor;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::options::DeleteNamespaceOptions;
use crate::time::Deadline;
use loonfs_api::wire::control::{AcquiredWriter, NamespaceStatus};
use loonfs_api::{DeleteNamespaceResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    acquired_writer: AcquiredWriter,
    context: &crate::context::MutationContext,
    deadline: &Deadline,
) -> Result<DeleteNamespaceResponse> {
    update_manifest(store, namespace_id, deadline, |mut payload| {
        let acquired_writer = &acquired_writer;
        async move {
            let anchor = load_read_anchor(store, namespace_id).await?;
            let head = &anchor.read_state;
            super::control::ensure_namespace_live(head)?;
            ensure_writer_not_fenced(head, acquired_writer)?;
            if let Some(expected) = options.expected_head_seq {
                if head.seq != expected {
                    return Err(CoreError::StaleHeadPrecondition {
                        precondition_index: None,
                        expected,
                        actual: head.seq,
                    });
                }
            }
            if payload.folded_wal_no < head.wal_no {
                deadline.ensure_metadata_publication_budget(namespace_id)?;
                crate::checkpoint::flush_wal_with_deadline(store, namespace_id, deadline).await?;
                return Ok(ManifestChange::Again);
            }
            payload.status = NamespaceStatus::Deleted {
                deleted_at_ms: context.now_ms,
            };
            let response = DeleteNamespaceResponse {
                namespace_id: namespace_id.clone(),
                head_seq: payload.head_seq,
            };
            Ok(ManifestChange::Next(Box::new(payload), response))
        }
    })
    .await
}
