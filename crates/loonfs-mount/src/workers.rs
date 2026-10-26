//! The work that the control task hands to workers, which make network
//! requests and file reads and report each result to the control task as a
//! message.

mod fetching;

use crate::cache_file::CacheFile;
use crate::control::{ControlMessage, ControlSender};
use crate::database::Ancestor;
use crate::error::{MountError, Result};
use crate::fetch::{FetchId, FetchJob};
use crate::read_handle::HandleId;
use crate::scan::{read_ancestors, snapshot_stat, SnapshotLease, SubtreeScan};
use crate::verify::{verify, Verification, VerifyJob};
use fetching::ParkedStream;
use loonfs_client::{Client, ClientError, ListChangesOptions, ReadAt, StatOptions};
use loonfs_types::api::v0::{ListChangesResponse, PathEntry, PathEntryKind};
use loonfs_types::options::Consistency;
use loonfs_types::{
    AttributeInclusion, ChangeSeq, ContentId, ContentRef, ErrorCode, InodeId, MonotonicTimer,
    NamespaceId, PinId,
};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// A snapshot that a listing read at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotRef {
    pub(crate) snapshot_id: PinId,
    pub(crate) captured_seq: ChangeSeq,
}

/// Which listing a page belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Listing {
    /// The listing of the root's subtree after `rebootstrap_required`.
    Rebuild,
    /// The listing of a directory that a move or an undelete brought into
    /// the mount.
    Entering(InodeId),
}

/// One piece of work for a worker.
#[derive(Debug)]
pub(crate) enum Work {
    /// Reads one page of the change feed after `after_seq`.
    PollFeed { after_seq: ChangeSeq },
    /// Reads an item that a move or an undelete brought into the mount, at
    /// `snapshot` when the feed is between a listing's commit and its
    /// snapshot, and at the live head otherwise. A directory is listed at a
    /// new snapshot, or at `snapshot`.
    ReadEnteringItem {
        inode_id: InodeId,
        snapshot: Option<SnapshotRef>,
    },
    /// Reads the root's path and its ancestors at the live head, after the
    /// root or an ancestor moved.
    ReadAncestors { root_inode_id: InodeId },
    /// Lists the root's subtree at a new snapshot.
    Rebuild { root_inode_id: InodeId },
    /// Extends a snapshot that a directory entering the mount was listed at.
    ExtendSnapshot { snapshot_id: PinId },
    /// Deletes a snapshot that nothing reads at anymore.
    DeleteSnapshot { snapshot_id: PinId },
    /// Fetches one range into a cache file.
    Fetch(FetchJob),
    /// Verifies a cache file.
    Verify(VerifyJob),
    /// Drops the grant stream that a closed read handle kept.
    DropStream { handle: HandleId },
}

/// What a worker read for an item that entered the mount.
#[derive(Debug)]
pub(crate) enum EnteringItem {
    /// The item isn't visible where the worker read it.
    Gone,
    /// A file, with its entry.
    File(Box<PathEntry>),
    /// A directory, whose entry and subtree the worker sent as
    /// [`WorkOutcome::EnteringDirectory`] and listing pages.
    Listed,
}

/// The root's entry and its ancestors, read at the live head.
#[derive(Debug, Clone)]
pub(crate) struct RootAncestry {
    pub(crate) root: PathEntry,
    pub(crate) ancestors: Vec<Ancestor>,
}

/// The snapshot and root entry of a rebuild's listing.
#[derive(Debug, Clone)]
pub(crate) struct RebuildBasis {
    pub(crate) snapshot: SnapshotRef,
    /// The root at the snapshot, or `None` when the root isn't visible.
    pub(crate) root: Option<PathEntry>,
}

/// What a fetch wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Fetched {
    pub(crate) written: Range<u64>,
}

/// The result of one piece of work.
#[derive(Debug)]
pub(crate) enum WorkOutcome {
    FeedPage(std::result::Result<ListChangesResponse, ClientError>),
    EnteringItemRead {
        inode_id: InodeId,
        /// The snapshot of a waiting directory that the read was at.
        snapshot: Option<SnapshotRef>,
        result: Result<EnteringItem>,
    },
    /// The entry of a directory that entered the mount, at the snapshot that
    /// its listing reads.
    EnteringDirectory {
        inode_id: InodeId,
        entry: PathEntry,
        snapshot: SnapshotRef,
        /// Whether the worker created the snapshot for this directory.
        created: bool,
    },
    AncestorsRead(Result<RootAncestry>),
    ListingPage {
        listing: Listing,
        entries: Vec<PathEntry>,
        /// The control task replies on this channel after it writes the
        /// page, or with the error that stopped it, so a listing keeps one
        /// page in memory at a time and stops at a page that isn't written.
        processed: oneshot::Sender<Result<()>>,
    },
    RebuildDone(Result<RebuildBasis>),
    Fetched {
        fetch_id: FetchId,
        result: Result<Fetched>,
    },
    Verified {
        content_id: ContentId,
        file: Arc<CacheFile>,
        /// The revision that the file was verified against.
        target: ContentRef,
        result: Result<Verification>,
    },
}

/// Starts work. The control task's only way to make a request or a slow
/// file read, so a test can replace the workers with a recorder and drive
/// the control task as a deterministic state machine.
pub(crate) trait Workers: Send {
    /// Starts `work`, whose outcome arrives later as a message.
    fn start(&mut self, work: Work);
}

struct WorkerContext {
    client: Client,
    namespace_id: NamespaceId,
    control: ControlSender,
    timer: Arc<dyn MonotonicTimer>,
    streams: Mutex<HashMap<HandleId, ParkedStream>>,
}

/// Workers that run each piece of work as a task on the engine's runtime.
pub(crate) struct ClientWorkers {
    context: Arc<WorkerContext>,
    runtime: tokio::runtime::Handle,
}

impl ClientWorkers {
    pub(crate) fn new(
        client: Client,
        namespace_id: NamespaceId,
        control: ControlSender,
        timer: Arc<dyn MonotonicTimer>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            context: Arc::new(WorkerContext {
                client,
                namespace_id,
                control,
                timer,
                streams: Mutex::new(HashMap::new()),
            }),
            runtime,
        }
    }
}

impl Workers for ClientWorkers {
    fn start(&mut self, work: Work) {
        let context = self.context.clone();
        self.runtime.spawn(async move { context.run(work).await });
    }
}

fn strong_stat() -> StatOptions {
    StatOptions {
        include_attributes: AttributeInclusion::Include,
        at: ReadAt::Live(Some(Consistency::Strong)),
    }
}

impl WorkerContext {
    fn send(&self, outcome: WorkOutcome) {
        // A stopped control task has no use for the outcome.
        let _ = self
            .control
            .send(ControlMessage::WorkDone(Box::new(outcome)));
    }

    async fn run(&self, work: Work) {
        match work {
            Work::PollFeed { after_seq } => {
                let options = ListChangesOptions {
                    at: ReadAt::Live(Some(Consistency::Strong)),
                };
                let page = self
                    .client
                    .list_changes_with_options(&self.namespace_id, after_seq, &options)
                    .next()
                    .await
                    .expect("the first page of a feed listing always loads");
                self.send(WorkOutcome::FeedPage(page));
            }
            Work::ReadEnteringItem { inode_id, snapshot } => {
                let result = self.read_entering_item(inode_id, snapshot.clone()).await;
                self.send(WorkOutcome::EnteringItemRead {
                    inode_id,
                    snapshot,
                    result,
                });
            }
            Work::ReadAncestors { root_inode_id } => {
                let result = self.read_ancestors(root_inode_id).await;
                self.send(WorkOutcome::AncestorsRead(result));
            }
            Work::Rebuild { root_inode_id } => {
                let result = self.rebuild(root_inode_id).await;
                self.send(WorkOutcome::RebuildDone(result));
            }
            Work::ExtendSnapshot { snapshot_id } => {
                if let Err(error) = self
                    .client
                    .extend_snapshot(
                        &self.namespace_id,
                        &snapshot_id,
                        crate::scan::SNAPSHOT_TTL_MS,
                    )
                    .await
                {
                    tracing::warn!(%snapshot_id, %error, "extending a listing's snapshot failed");
                }
            }
            Work::DeleteSnapshot { snapshot_id } => {
                if let Err(error) = self
                    .client
                    .delete_snapshot(&self.namespace_id, &snapshot_id)
                    .await
                {
                    tracing::warn!(%snapshot_id, %error, "deleting a listing's snapshot failed");
                }
            }
            Work::Fetch(job) => {
                let fetch_id = job.fetch_id;
                let result = self.fetch_with_retry(&job).await;
                self.send(WorkOutcome::Fetched { fetch_id, result });
            }
            Work::Verify(job) => {
                let content_id = job.content_id.clone();
                let file = job.file.clone();
                let target = job.target.clone();
                // Verification reads the whole file, so it runs on a thread
                // that may block.
                let result = tokio::task::spawn_blocking(move || verify(&job))
                    .await
                    .expect("a verification should not panic")
                    .map_err(|error| MountError::Io {
                        path: content_id.to_string(),
                        message: error.to_string(),
                    });
                self.send(WorkOutcome::Verified {
                    content_id,
                    file,
                    target,
                    result,
                });
            }
            Work::DropStream { handle } => {
                self.streams
                    .lock()
                    .expect("the stream lock should not be poisoned")
                    .remove(&handle);
            }
        }
    }

    async fn read_entering_item(
        &self,
        inode_id: InodeId,
        snapshot: Option<SnapshotRef>,
    ) -> Result<EnteringItem> {
        let options = snapshot
            .as_ref()
            .map_or_else(strong_stat, |snapshot| snapshot_stat(&snapshot.snapshot_id));
        let entry = match self
            .client
            .stat_by_inode_with_options(&self.namespace_id, inode_id, &options)
            .await
        {
            Ok(entry) => entry,
            Err(error) if error.code() == Some(ErrorCode::InodeNotFound) => {
                return Ok(EnteringItem::Gone)
            }
            Err(error) => return Err(error.into()),
        };
        match entry.kind {
            PathEntryKind::File { .. } => return Ok(EnteringItem::File(Box::new(entry))),
            PathEntryKind::Unknown => return Ok(EnteringItem::Gone),
            PathEntryKind::Directory {} => {}
        }
        let (lease, snapshot) = match snapshot {
            Some(snapshot) => (None, snapshot),
            None => {
                let lease =
                    SnapshotLease::create(&self.client, &self.namespace_id, self.timer.as_ref())
                        .await?;
                let snapshot = SnapshotRef {
                    snapshot_id: lease.snapshot_id().clone(),
                    captured_seq: lease.captured_seq(),
                };
                (Some(lease), snapshot)
            }
        };
        let entry = match self
            .client
            .stat_by_inode_with_options(
                &self.namespace_id,
                inode_id,
                &snapshot_stat(&snapshot.snapshot_id),
            )
            .await
        {
            Ok(entry) => entry,
            Err(error) => {
                if let Some(lease) = lease {
                    let _ = lease.delete(&self.client).await;
                }
                return match error.code() {
                    Some(ErrorCode::InodeNotFound) => Ok(EnteringItem::Gone),
                    _ => Err(error.into()),
                };
            }
        };
        self.send(WorkOutcome::EnteringDirectory {
            inode_id,
            entry: entry.clone(),
            snapshot: snapshot.clone(),
            created: lease.is_some(),
        });
        let mut lease = lease;
        let mut scan = SubtreeScan::new(
            &self.client,
            &self.namespace_id,
            &snapshot.snapshot_id,
            &entry,
            None,
        );
        loop {
            if let Some(lease) = &mut lease {
                lease.keep_alive(&self.client, self.timer.as_ref()).await?;
            }
            let Some(entries) = scan.next_page().await? else {
                return Ok(EnteringItem::Listed);
            };
            self.send_page(Listing::Entering(inode_id), entries).await?;
        }
    }

    async fn send_page(&self, listing: Listing, entries: Vec<PathEntry>) -> Result<()> {
        let (processed, written) = oneshot::channel();
        self.send(WorkOutcome::ListingPage {
            listing,
            entries,
            processed,
        });
        written.await.map_err(|_| MountError::Stopped)?
    }

    async fn read_ancestors(&self, root_inode_id: InodeId) -> Result<RootAncestry> {
        let root = self
            .client
            .stat_by_inode_with_options(&self.namespace_id, root_inode_id, &strong_stat())
            .await?;
        let ancestors = read_ancestors(
            &self.client,
            &self.namespace_id,
            root.parent_inode_id,
            &strong_stat(),
        )
        .await?;
        Ok(RootAncestry { root, ancestors })
    }

    /// Lists the root's subtree at a new snapshot. The control task deletes
    /// the snapshot once the rebuild is done, and the rebuild deletes it
    /// itself when it fails, so a failing rebuild that starts again doesn't
    /// leave a snapshot for each attempt.
    async fn rebuild(&self, root_inode_id: InodeId) -> Result<RebuildBasis> {
        let mut lease =
            SnapshotLease::create(&self.client, &self.namespace_id, self.timer.as_ref()).await?;
        let listed = self.list_for_rebuild(&mut lease, root_inode_id).await;
        if listed.is_err() {
            if let Err(error) = lease.delete(&self.client).await {
                tracing::warn!(%error, "deleting a failed rebuild's snapshot failed");
            }
        }
        listed
    }

    async fn list_for_rebuild(
        &self,
        lease: &mut SnapshotLease,
        root_inode_id: InodeId,
    ) -> Result<RebuildBasis> {
        let snapshot = SnapshotRef {
            snapshot_id: lease.snapshot_id().clone(),
            captured_seq: lease.captured_seq(),
        };
        let root = match self
            .client
            .stat_by_inode_with_options(
                &self.namespace_id,
                root_inode_id,
                &snapshot_stat(&snapshot.snapshot_id),
            )
            .await
        {
            Ok(root) => root,
            Err(error) if error.code() == Some(ErrorCode::InodeNotFound) => {
                return Ok(RebuildBasis {
                    snapshot,
                    root: None,
                })
            }
            Err(error) => return Err(error.into()),
        };
        let mut scan = SubtreeScan::new(
            &self.client,
            &self.namespace_id,
            &snapshot.snapshot_id,
            &root,
            None,
        );
        loop {
            lease.keep_alive(&self.client, self.timer.as_ref()).await?;
            let Some(entries) = scan.next_page().await? else {
                return Ok(RebuildBasis {
                    snapshot,
                    root: Some(root),
                });
            };
            self.send_page(Listing::Rebuild, entries).await?;
        }
    }
}
