//! The next-range query: where a fork's next merge into a namespace starts,
//! read without planning the merge
//! ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/merge.md#the-next-range-query)).

use super::{ensure_range_start_retained, fork_point, merge_start, rebase_basis};
use crate::engine::{NamespaceEngine, RuntimeReadContext};
use crate::error::{CoreError, HistoryPosition, Result};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{Merge, NextMergeRangeOutcome, NextMergeRangeResponse};

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Returns where the next merge of `fork`, read at `fork_context`, into
    /// this namespace, read at `context`, starts, without planning the merge
    /// ([API section 6.15](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#615-get-merges)).
    ///
    /// The range starts where [`Self::plan_merge`] would start it, or where
    /// [`Self::plan_replay`] would when this namespace is a rebase's new fork
    /// for `fork`. When a retention floor would refuse the merge with
    /// `rebootstrap_required`, the response's outcome says which floor
    /// instead. When the fork point is below this namespace's floor, the
    /// outcome has no start, because the merge index might no longer list
    /// the fork's newest merge. A namespace that isn't a fork of this
    /// namespace, or of the namespace that a rebase made this one from,
    /// returns `invalid_request`.
    pub async fn next_merge_range<F>(
        &self,
        context: &RuntimeReadContext,
        fork: &NamespaceEngine<S, F>,
        fork_context: &RuntimeReadContext,
    ) -> Result<NextMergeRangeResponse> {
        let main = self.namespace.load_metadata_view(context).await?;
        let fork_view = fork.namespace.load_metadata_view(fork_context).await?;
        let fork_head = fork_view.head();
        let fork_id = &fork_head.namespace_id;
        let source = rebase_basis(main.head(), fork_id)
            .map_or(&main.head().namespace_id, |(basis, _)| {
                &basis.manifest.owner_namespace_id
            });
        let fork_point_seq = fork_point(fork_head, source)?;
        let outcome =
            match merge_start(&main.metadata_view(), main.head(), fork_id, fork_point_seq).await {
                Ok(start) => {
                    let merge = start.previous.map(|previous| Merge {
                        source_namespace_id: previous.source_namespace_id,
                        source_after_seq: previous.source_after_seq,
                        source_through_seq: previous.source_through_seq,
                    });
                    let next_after_seq = start.after_seq;
                    match ensure_range_start_retained(&fork_view, next_after_seq) {
                        Ok(()) => NextMergeRangeOutcome::Open {
                            merge,
                            next_after_seq,
                        },
                        Err(CoreError::RebootstrapRequired {
                            below: HistoryPosition::RangeStart { .. },
                            retention_floor_seq,
                        }) => NextMergeRangeOutcome::RangeStartBelowFloor {
                            merge,
                            next_after_seq,
                            retention_floor_seq,
                        },
                        Err(error) => return Err(error),
                    }
                }
                Err(CoreError::RebootstrapRequired {
                    below: HistoryPosition::ForkPoint { .. },
                    retention_floor_seq,
                }) => NextMergeRangeOutcome::ForkPointBelowFloor {
                    retention_floor_seq,
                },
                Err(error) => return Err(error),
            };
        Ok(NextMergeRangeResponse {
            fork_point_seq,
            source_head_seq: fork_head.seq,
            outcome,
        })
    }
}
