//! Admitting a mount: the checks that it passes, and the first scan of its
//! tree at a snapshot, before anything is mounted.

use crate::config::{MountAccess, MountConfig, CONTROL_DIRECTORY_NAME};
use crate::control::write_listing;
use crate::database::{self, Database, MountRow};
use crate::error::{MountError, Result};
use crate::host::{check_host, DiskSpace};
use crate::item::{ItemId, ItemKind};
use crate::item_table::write_item;
use crate::progress::{MountProgress, Progress};
use crate::records::{ItemRecord, RemoteState, Visibility};
use crate::scan::{read_ancestors, snapshot_stat, SnapshotLease, SubtreeScan};
use crate::status::{ReflinkSupport, Transports};
use loonfs_client::{Client, NamespacePath};
use loonfs_types::api::v0::{PathEntry, PathEntryKind};
use loonfs_types::{
    MonotonicTimer, NamespaceNaming, FEATURE_COMMIT_INLINE_CONTENT, FEATURE_DOWNLOADS_DIRECT_GET,
    FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION,
};

/// What admission produced.
pub(crate) struct Admission {
    pub(crate) database: Database,
    pub(crate) reflinks: ReflinkSupport,
}

/// Checks the host and the namespace, and scans the mounted tree at a
/// snapshot into a new `tree.db`.
///
/// The steps follow "Admitting a mount" in the design note. Any failure
/// refuses the mount with the reason and the numbers involved.
pub(crate) async fn admit(
    client: &Client,
    config: &MountConfig,
    disk: &dyn DiskSpace,
    timer: &dyn MonotonicTimer,
    progress: &Progress,
) -> Result<Admission> {
    progress.report(MountProgress::CheckingHost);
    let host = check_host(config, disk)?;
    progress.report(MountProgress::ReadingCapabilities);
    let capabilities = client.get_capabilities().await?;
    let metadata = client.get_namespace(&config.namespace_id).await?;
    if config.access == MountAccess::Writable && metadata.naming != NamespaceNaming::CaseSensitive {
        return Err(MountError::CaseInsensitiveNaming {
            namespace_id: config.namespace_id.clone(),
        });
    }
    let inline_limit_bytes = if capabilities.supports(FEATURE_COMMIT_INLINE_CONTENT) {
        capabilities
            .limits
            .get(LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION)
            .copied()
            .unwrap_or(0)
    } else {
        0
    };
    let transports = Transports {
        direct_put: capabilities.supports(FEATURE_UPLOADS_DIRECT_PUT),
        direct_multipart: capabilities.supports(FEATURE_UPLOADS_DIRECT_MULTIPART),
        direct_get: capabilities.supports(FEATURE_DOWNLOADS_DIRECT_GET),
    };
    progress.report(MountProgress::CreatingSnapshot);
    let mut lease = SnapshotLease::create(client, &config.namespace_id, timer).await?;
    let scanned = scan(
        client,
        config,
        &mut lease,
        timer,
        inline_limit_bytes,
        transports,
        progress,
    )
    .await;
    let deleted = lease.delete(client).await;
    let database = match scanned {
        Ok(database) => database,
        Err(error) => {
            database::remove_files(&config.database_path())?;
            return Err(error);
        }
    };
    deleted?;
    database.mark_admitted()?;
    Ok(Admission {
        database,
        reflinks: host.reflinks,
    })
}

/// Reads the root and its ancestors at the snapshot, and lists the root's
/// subtree into a new `tree.db`, stopping one entry past the budget. Reports
/// the entries listed when it starts and after each page that fits the
/// budget.
async fn scan(
    client: &Client,
    config: &MountConfig,
    lease: &mut SnapshotLease,
    timer: &dyn MonotonicTimer,
    inline_limit_bytes: u64,
    transports: Transports,
    progress: &Progress,
) -> Result<Database> {
    let budget = config.max_entries;
    progress.report(MountProgress::Scanning {
        listed_entries: 0,
        max_entries: budget,
    });
    let snapshot_id = lease.snapshot_id().clone();
    let root = client
        .stat_with_options(
            &NamespacePath::new(config.namespace_id.clone(), config.path.clone()),
            &snapshot_stat(&snapshot_id),
        )
        .await?;
    if !matches!(root.kind, PathEntryKind::Directory {}) {
        return Err(MountError::NotADirectory {
            path: config.path.to_string(),
        });
    }
    let ancestors = read_ancestors(
        client,
        &config.namespace_id,
        root.parent_inode_id,
        &snapshot_stat(&snapshot_id),
    )
    .await?;
    let mut database = Database::create(
        &config.database_path(),
        &MountRow {
            config: config.clone(),
            applied_seq: lease.captured_seq(),
            root_inode_id: root.inode_id,
            root_path: root.path.clone(),
            inline_limit_bytes,
            transports,
            publication_stop: None,
        },
    )?;
    let (kind, state) = RemoteState::from_entry(&root, None)
        .expect("a directory entry has a state that the engine knows");
    {
        let transaction = database.transaction()?;
        write_item(
            &transaction,
            &ItemRecord::clean(
                ItemId::ROOT,
                Some(root.inode_id),
                kind,
                state,
                Visibility::Shown,
            ),
        )?;
        database::write_ancestors(&transaction, &ancestors)?;
        transaction.commit()?;
    }
    assert_eq!(kind, ItemKind::Directory, "the root should be a directory");
    let mut listing = SubtreeScan::new(
        client,
        &config.namespace_id,
        &snapshot_id,
        &root,
        Some(budget.saturating_add(1)),
    );
    let mut listed_entries: u64 = 0;
    while let Some(entries) = listing.next_page().await? {
        listed_entries += entries.len() as u64;
        if listed_entries > budget {
            return Err(MountError::EntryBudgetExceeded {
                path: config.path.to_string(),
                listed_entries,
                max_entries: budget,
            });
        }
        if entries.iter().any(|entry| is_reserved(entry, &root)) {
            return Err(MountError::ReservedName {
                path: config.path.to_string(),
            });
        }
        write_page(&mut database, &entries)?;
        progress.report(MountProgress::Scanning {
            listed_entries,
            max_entries: budget,
        });
        lease.keep_alive(client, timer).await?;
    }
    Ok(database)
}

/// Writes one listing page in one transaction. It's a step of its own, so
/// the scan's future keeps no transaction across an await and stays `Send`.
fn write_page(database: &mut Database, entries: &[PathEntry]) -> Result<()> {
    let transaction = database.transaction()?;
    write_listing(&transaction, entries)?;
    transaction.commit()?;
    Ok(())
}

fn is_reserved(entry: &PathEntry, root: &PathEntry) -> bool {
    entry.parent_inode_id == Some(root.inode_id)
        && entry
            .display_name
            .as_ref()
            .is_some_and(|name| name.as_str() == CONTROL_DIRECTORY_NAME)
}
