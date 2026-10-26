//! Listings of a directory's subtree at a snapshot, which admission, a
//! rebuild, and a directory that enters the mount make, and the snapshots
//! they list at.

use crate::database::Ancestor;
use crate::error::{MountError, Result};
use loonfs_client::{Client, ClientError, InodeChildrenPager, ListOptions, ReadAt, StatOptions};
use loonfs_types::api::v0::{PathEntry, PathEntryKind, Snapshot};
use loonfs_types::{
    AttributeInclusion, ChangeSeq, EffectiveLimit, InodeId, MonotonicTimer, NamespaceId, PinId,
};
use std::collections::VecDeque;
use std::num::NonZeroU32;

/// The lifetime of a snapshot that a listing reads at. It bounds how long the
/// snapshot outlives a daemon that dies during the listing.
pub(crate) const SNAPSHOT_TTL_MS: u64 = 600_000;
/// The entries that each page of a listing asks for.
const LISTING_PAGE_ENTRIES: NonZeroU32 = NonZeroU32::new(1_000).expect("a page has entries");
/// How many levels a recursive listing descends below the listed directory.
/// A directory at this depth is returned without its entries.
const RECURSIVE_LISTING_DEPTH: usize = 128;
/// The name that a mount's snapshots have, for an operator who lists them.
const SNAPSHOT_NAME: &str = "loonfs-mount";
/// The most ancestors that a read of the root's ancestors follows. A derived
/// path can be deeper than a request path, so the bound is larger than the
/// 128 components that a request path can have.
const MAX_ANCESTORS: usize = 4_096;

/// Returns the options of a stat, with attributes, at a snapshot.
pub(crate) fn snapshot_stat(snapshot_id: &PinId) -> StatOptions {
    StatOptions {
        include_attributes: AttributeInclusion::Include,
        at: ReadAt::Snapshot(snapshot_id.clone()),
    }
}

/// Reads each ancestor by inode, from `parent` (the root's parent) up to the
/// namespace's root.
pub(crate) async fn read_ancestors(
    client: &Client,
    namespace_id: &NamespaceId,
    parent: Option<InodeId>,
    options: &StatOptions,
) -> Result<Vec<Ancestor>> {
    let mut ancestors = Vec::new();
    let mut parent = parent;
    while let Some(inode_id) = parent {
        if ancestors.len() == MAX_ANCESTORS {
            return Err(MountError::from(ClientError::Protocol(format!(
                "the mount's root has more than {MAX_ANCESTORS} ancestors"
            ))));
        }
        let entry = client
            .stat_by_inode_with_options(namespace_id, inode_id, options)
            .await?;
        parent = entry.parent_inode_id;
        ancestors.push(Ancestor {
            inode_id,
            path: entry.path,
        });
    }
    Ok(ancestors)
}

/// A snapshot that a listing reads at, which the listing extends while it
/// runs.
#[derive(Debug)]
pub(crate) struct SnapshotLease {
    snapshot: Snapshot,
    extended_at_ms: u64,
}

impl SnapshotLease {
    /// Creates a snapshot with a ten-minute lifetime.
    ///
    /// A forbidden snapshot means that the subject isn't an administrator of
    /// an ACL namespace.
    pub(crate) async fn create(
        client: &Client,
        namespace_id: &NamespaceId,
        timer: &dyn MonotonicTimer,
    ) -> Result<Self> {
        let snapshot = client
            .create_snapshot(namespace_id, SNAPSHOT_NAME, Some(SNAPSHOT_TTL_MS))
            .await
            .map_err(|error| match error.code() {
                Some(loonfs_types::ErrorCode::Forbidden) => MountError::AdministratorRequired {
                    message: error.to_string(),
                },
                _ => MountError::from(error),
            })?;
        Ok(Self {
            snapshot,
            extended_at_ms: timer.monotonic_now_ms(),
        })
    }

    /// Returns the snapshot's ID.
    pub(crate) fn snapshot_id(&self) -> &PinId {
        &self.snapshot.snapshot_id
    }

    /// Returns the sequence that the snapshot captured.
    pub(crate) fn captured_seq(&self) -> ChangeSeq {
        self.snapshot.captured_seq
    }

    /// Extends the snapshot by another ten minutes when less than half of
    /// its lifetime is left.
    pub(crate) async fn keep_alive(
        &mut self,
        client: &Client,
        timer: &dyn MonotonicTimer,
    ) -> Result<()> {
        let now_ms = timer.monotonic_now_ms();
        if now_ms.saturating_sub(self.extended_at_ms) < SNAPSHOT_TTL_MS / 2 {
            return Ok(());
        }
        self.snapshot = client
            .extend_snapshot(
                &self.snapshot.namespace_id,
                &self.snapshot.snapshot_id,
                SNAPSHOT_TTL_MS,
            )
            .await?;
        self.extended_at_ms = now_ms;
        Ok(())
    }

    /// Deletes the snapshot.
    pub(crate) async fn delete(self, client: &Client) -> Result<()> {
        client
            .delete_snapshot(&self.snapshot.namespace_id, &self.snapshot.snapshot_id)
            .await?;
        Ok(())
    }
}

/// A recursive listing of one directory's subtree at a snapshot, read page by
/// page.
///
/// The listing walks 128 levels below each directory that it lists, so it
/// lists a directory at that depth by inode afterward. Its queue of such
/// directories has at most one entry per directory 128 levels down, which
/// only a move can make.
pub(crate) struct SubtreeScan {
    client: Client,
    namespace_id: NamespaceId,
    snapshot_id: PinId,
    /// Directories to list, with the depth of their paths.
    queue: VecDeque<(InodeId, usize)>,
    current: Option<(InodeChildrenPager, usize)>,
    /// The entries the listing can still return, or `None` for no limit.
    remaining_entries: Option<u64>,
}

impl SubtreeScan {
    /// Starts a listing of the subtree of `root`, a directory entry read at
    /// the snapshot, that returns at most `max_entries` entries.
    pub(crate) fn new(
        client: &Client,
        namespace_id: &NamespaceId,
        snapshot_id: &PinId,
        root: &PathEntry,
        max_entries: Option<u64>,
    ) -> Self {
        Self {
            client: client.clone(),
            namespace_id: namespace_id.clone(),
            snapshot_id: snapshot_id.clone(),
            queue: VecDeque::from([(root.inode_id, root.path.components().len())]),
            current: None,
            remaining_entries: max_entries,
        }
    }

    /// Returns the next page of entries, or `None` once the subtree is
    /// listed or the listing returned `max_entries` entries.
    ///
    /// The entries come in depth-first order, so each directory's entry
    /// comes before the entries below it.
    pub(crate) async fn next_page(&mut self) -> Result<Option<Vec<PathEntry>>> {
        loop {
            if self.remaining_entries == Some(0) {
                return Ok(None);
            }
            if self.current.is_none() {
                let Some((inode_id, depth)) = self.queue.pop_front() else {
                    return Ok(None);
                };
                self.current = Some((self.pager(inode_id), depth));
            }
            let (pager, depth) = self.current.as_mut().expect("a pager was just set");
            let depth = *depth;
            let Some(page) = pager.next().await else {
                self.current = None;
                continue;
            };
            let entries = page?.entries;
            for entry in &entries {
                if matches!(entry.kind, PathEntryKind::Directory {})
                    && entry.path.components().len().saturating_sub(depth)
                        >= RECURSIVE_LISTING_DEPTH
                {
                    self.queue
                        .push_back((entry.inode_id, entry.path.components().len()));
                }
            }
            if let Some(remaining) = &mut self.remaining_entries {
                *remaining = remaining.saturating_sub(entries.len() as u64);
            }
            return Ok(Some(entries));
        }
    }

    fn pager(&self, inode_id: InodeId) -> InodeChildrenPager {
        let pager = self
            .client
            .list_by_inode_with_options(
                &self.namespace_id,
                inode_id,
                &ListOptions {
                    include_attributes: AttributeInclusion::Include,
                    recursive: true,
                    at: ReadAt::Snapshot(self.snapshot_id.clone()),
                },
            )
            .page_limit(EffectiveLimit::new(LISTING_PAGE_ENTRIES));
        match self.remaining_entries {
            Some(remaining) => pager.max_items(usize::try_from(remaining).unwrap_or(usize::MAX)),
            None => pager,
        }
    }
}
