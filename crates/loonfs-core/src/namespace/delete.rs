//! Namespace deletion status published as successive manifests.

use crate::error::{CoreError, Result};
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::manifest::publish::{update_manifest, ManifestChange};
use crate::manifest::MetadataRunPolicy;
use crate::namespace::control::LoadedManifest;
use crate::namespace::read_anchor::load_read_anchor;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::options::DeleteNamespaceOptions;
use crate::time::Deadline;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::NamespaceStatus;
use loonfs_types::{DeleteNamespaceResponse, NamespaceId};

#[allow(
    clippy::too_many_arguments,
    reason = "deletion passes the fold policy, the segment cache, and the content write permits to its fold"
)]
pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    acquired_writer: AcquiredWriter,
    context: &crate::context::MutationContext,
    deadline: &Deadline,
    fold_policy: MetadataRunPolicy,
    segment_cache: Option<&crate::cache::MetadataSegmentCache>,
    content_writes: &crate::PermitPool,
) -> Result<DeleteNamespaceResponse> {
    update_manifest(
        store,
        namespace_id,
        deadline,
        MAX_CONTENTION_ATTEMPTS,
        |current: LoadedManifest| {
            let acquired_writer = &acquired_writer;
            async move {
                let mut payload = current.state.envelope.payload().clone();
                let anchor = load_read_anchor(store, namespace_id, deadline.observe()).await?;
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
                    crate::manifest::fold_wal_tail(
                        store,
                        segment_cache,
                        namespace_id,
                        None,
                        deadline,
                        fold_policy,
                        content_writes,
                    )
                    .await?;
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
        },
    )
    .await
    .map(|(result, _, _)| result)
}
