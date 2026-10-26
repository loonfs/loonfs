//! A simulated namespace for the engine: a [`SimulatedServer`] with one
//! namespace, the real client on its service, commits through the server's
//! test driver, and the engine's tree and requests in forms a test compares.

use bytes::Bytes;
use loonfs_client::{Client, ListOptions, NamespacePath, ReadAt};
use loonfs_mount::{Engine, ItemId, ItemKind, MountAccess, MountConfig, ReadHandle};
use loonfs_mount_sim::{Operation, RequestRecord, SimulatedServer, SimulatedServerConfig};
use loonfs_test_support::clock::bounded;
use loonfs_types::api::v0::{
    Commit, CommitRequest, DeleteDirectoryBehavior, FilesystemOperation, PathEntry, PathEntryKind,
};
use loonfs_types::options::Consistency;
use loonfs_types::{
    AbsolutePath, ActorId, AttributeInclusion, AttributeKey, AttributeValue, ChangeSeq, CommitId,
    ContentRef, InodeId, NamespaceId, NamespaceNaming,
};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::time::Duration;
use tempfile::TempDir;

/// How long a test waits for the engine.
pub(crate) const WAIT: Duration = Duration::from_secs(30);
/// The directory in the namespace that every test mounts.
pub(crate) const MOUNT_PATH: &str = "/work/project";
/// The poll interval of a test's mount. The ticker never fires during a
/// test, so every feed poll after the first comes from `refresh_at_least`,
/// and the trace of a test is the same on every run.
const POLL_INTERVAL_MS: u64 = 3_600_000;
/// One mebibyte.
pub(crate) const MIB: u64 = 1 << 20;

/// A simulated server with one case-sensitive namespace, `demo`, that has
/// `/work/project` (the mount's root, below `/work`) and `/elsewhere`
/// (outside the mount), and a client of the server for the test's own
/// listings and commits over HTTP.
pub(crate) struct Fixture {
    pub(crate) server: SimulatedServer,
    pub(crate) client: Client,
    pub(crate) namespace_id: NamespaceId,
    pub(crate) actor: ActorId,
    state_directory: TempDir,
}

pub(crate) fn fixture() -> Fixture {
    fixture_with(SimulatedServerConfig::default())
}

pub(crate) fn fixture_with(config: SimulatedServerConfig) -> Fixture {
    Fixture::new(config, NamespaceNaming::CaseSensitive)
}

/// Returns `/work/project/<relative>`, or the mount's root for `""`.
pub(crate) fn path(relative: &str) -> AbsolutePath {
    let path = match relative {
        "" => MOUNT_PATH.to_owned(),
        _ if relative.starts_with('/') => relative.to_owned(),
        _ => format!("{MOUNT_PATH}/{relative}"),
    };
    AbsolutePath::parse(path).expect("an absolute path")
}

pub(crate) fn mkdir(relative: &str, parents: bool) -> FilesystemOperation {
    FilesystemOperation::CreateDirectory {
        path: path(relative),
        parents,
    }
}

pub(crate) fn put(relative: &str, bytes: &[u8]) -> FilesystemOperation {
    FilesystemOperation::PutFile {
        path: path(relative),
        content_ref: None,
        inline_content: Some(bytes.to_vec()),
        behavior: Default::default(),
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

pub(crate) fn move_path(source: &str, destination: &str) -> FilesystemOperation {
    FilesystemOperation::MovePath {
        source_path: path(source),
        destination_path: path(destination),
        precondition: Default::default(),
    }
}

pub(crate) fn delete(relative: &str) -> FilesystemOperation {
    FilesystemOperation::DeletePath {
        path: path(relative),
        behavior: DeleteDirectoryBehavior::Recursive,
        expected_inode_id: None,
    }
}

/// Undeletes an item where its deletion found it.
pub(crate) fn undelete(inode_id: InodeId, deletion_seq: ChangeSeq) -> FilesystemOperation {
    FilesystemOperation::Undelete {
        inode_id,
        deletion_seq,
        destination_path: None,
        destination_parent_inode_id: None,
        destination_display_name: None,
    }
}

pub(crate) fn update_attributes(
    relative: &str,
    set: &[(&str, &str)],
    remove: &[&str],
) -> FilesystemOperation {
    FilesystemOperation::UpdateAttributes {
        path: path(relative),
        set: set
            .iter()
            .map(|(key, value)| {
                (
                    AttributeKey::parse(key).expect("an attribute key"),
                    AttributeValue::parse(value).expect("an attribute value"),
                )
            })
            .collect(),
        remove: remove
            .iter()
            .map(|key| AttributeKey::parse(key).expect("an attribute key"))
            .collect(),
        expected_inode_id: None,
        expected_attributes_revision_no: None,
    }
}

pub(crate) fn request(operations: Vec<FilesystemOperation>) -> CommitRequest {
    CommitRequest {
        commit_id: CommitId::generate(),
        message: None,
        content_tokens: Vec::new(),
        preconditions: Vec::new(),
        operations,
    }
}

/// Returns `size` bytes that change every eight bytes, so a read from the
/// wrong offset can't pass.
pub(crate) fn pattern(size: u64) -> Bytes {
    (0..size)
        .map(|offset| u8::try_from(offset / 8 % 251).expect("a byte"))
        .collect::<Vec<_>>()
        .into()
}

impl Fixture {
    /// Returns a fixture whose namespace has `naming`.
    pub(crate) fn new(config: SimulatedServerConfig, naming: NamespaceNaming) -> Self {
        let server = SimulatedServer::new(config);
        let namespace_id = NamespaceId::parse("demo").expect("a namespace ID");
        server.create_namespace(&namespace_id, naming);
        let client = Client::with_transport(server.client_config(), server.service())
            .expect("a valid client config");
        let fixture = Self {
            server,
            client,
            namespace_id,
            actor: ActorId::parse("other-host").expect("an actor"),
            state_directory: tempfile::tempdir().expect("a state directory"),
        };
        fixture.commit(vec![mkdir(MOUNT_PATH, true), mkdir("/elsewhere", false)]);
        fixture
    }

    /// Commits through the server's test driver, as another host would, and
    /// expects the commit to succeed.
    pub(crate) fn commit(&self, operations: Vec<FilesystemOperation>) -> Commit {
        self.server
            .commit(&self.namespace_id, &self.actor, &request(operations))
            .expect("the commit should succeed")
    }

    /// Writes a file through an upload, for content larger than a commit
    /// takes inline. The upload's requests go to the trace.
    pub(crate) async fn upload(&self, relative: &str, bytes: &[u8]) -> Commit {
        self.client
            .put_file(&self.namespace_path(relative), bytes, &self.actor)
            .await
            .expect("the upload should succeed")
    }

    pub(crate) fn namespace_path(&self, relative: &str) -> NamespacePath {
        NamespacePath::new(self.namespace_id.clone(), path(relative))
    }

    /// Reads an entry at the live head through the test's client.
    pub(crate) async fn stat(&self, relative: &str) -> PathEntry {
        self.client
            .stat(&self.namespace_path(relative))
            .await
            .expect("the entry")
    }

    pub(crate) async fn inode(&self, relative: &str) -> InodeId {
        self.stat(relative).await.inode_id
    }

    pub(crate) fn head_seq(&self) -> ChangeSeq {
        self.server.head_seq(&self.namespace_id)
    }

    /// Returns a read-only mount's configuration, with a small free-space
    /// floor and a poll interval that doesn't pass during a test.
    pub(crate) fn config(&self) -> MountConfig {
        let mut config = MountConfig::new(
            self.namespace_id.clone(),
            path(""),
            MountAccess::ReadOnly,
            self.state_directory.path().to_path_buf(),
        );
        config.min_free_bytes = MIB;
        config.poll_interval_ms = POLL_INTERVAL_MS;
        config
    }

    pub(crate) fn cache_file(&self, content_ref: &ContentRef) -> std::path::PathBuf {
        self.state_directory
            .path()
            .join("cache")
            .join(content_ref.content_id.as_str())
    }

    pub(crate) fn has_tree_database(&self) -> bool {
        self.state_directory.path().join("tree.db").exists()
    }

    /// Admits a mount with `config` through a new client of the server.
    pub(crate) async fn admit(&self, config: MountConfig) -> loonfs_mount::Result<Engine> {
        let client = Client::with_transport(self.server.client_config(), self.server.service())
            .expect("a valid client config");
        bounded(WAIT, Engine::mount(client, config))
            .await
            .expect("admission ends in time")
    }

    /// Admits a mount with the test configuration, and waits for the poll
    /// that the engine starts with, so the test's commits come after it.
    pub(crate) async fn mount(&self) -> Engine {
        self.mount_with(self.config()).await
    }

    /// Admits a mount with `config`, and waits for the poll that the engine
    /// starts with.
    pub(crate) async fn mount_with(&self, config: MountConfig) -> Engine {
        let engine = self.admit(config).await.expect("the mount is admitted");
        refresh(&engine, engine.status().applied_seq).await;
        engine
    }

    /// Waits until the server has received a request for `operation` since
    /// the trace was last taken.
    pub(crate) async fn wait_for_request(&self, operation: Operation) {
        eventually(|| {
            self.server
                .trace()
                .iter()
                .any(|record| record.operation == operation)
        })
        .await;
    }

    /// Lists the mount's subtree at the live head through the test's client.
    pub(crate) async fn listed_tree(&self) -> BTreeMap<String, Shown> {
        let options = ListOptions {
            include_attributes: AttributeInclusion::Include,
            recursive: true,
            at: ReadAt::Live(Some(Consistency::Strong)),
        };
        let entries = self
            .client
            .list_with_options(&self.namespace_path(""), &options)
            .collect_up_to(100_000)
            .await
            .expect("a listing");
        let prefix = format!("{MOUNT_PATH}/");
        entries
            .iter()
            .map(|entry| {
                let relative = entry
                    .path
                    .as_str()
                    .strip_prefix(&prefix)
                    .expect("a listed entry is below the mount's root");
                (relative.to_owned(), Shown::of_entry(entry))
            })
            .collect()
    }
}

/// What a tree shows at one path: the kind, and the size of a file or the
/// length of a symlink's target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Shown {
    pub(crate) kind: ItemKind,
    pub(crate) size_bytes: u64,
}

impl Shown {
    fn of_entry(entry: &PathEntry) -> Self {
        let attribute = |key: &str| {
            entry
                .attributes
                .as_ref()
                .and_then(|projection| projection.attributes.as_map().get(key))
                .map(|value| value.as_str().to_owned())
        };
        match &entry.kind {
            PathEntryKind::File { size_bytes, .. } => match attribute("fs.type").as_deref() {
                Some("symlink") => Self {
                    kind: ItemKind::Symlink,
                    size_bytes: attribute("fs.target").map_or(0, |target| target.len() as u64),
                },
                _ => Self {
                    kind: ItemKind::File,
                    size_bytes: *size_bytes,
                },
            },
            _ => Self {
                kind: ItemKind::Directory,
                size_bytes: 0,
            },
        }
    }
}

/// Walks the engine's Local tree from the root, by `readdir` and `getattr`.
pub(crate) fn engine_tree(engine: &Engine) -> BTreeMap<String, Shown> {
    let mut tree = BTreeMap::new();
    let mut queue = vec![(ItemId::ROOT, String::new())];
    while let Some((directory, prefix)) = queue.pop() {
        for entry in engine.readdir(directory, 0) {
            let name = entry.name.to_str().expect("a UTF-8 name");
            let relative = format!("{prefix}{name}");
            let attr = engine.getattr(entry.item).expect("a listed item");
            if attr.kind == ItemKind::Directory {
                queue.push((entry.item, format!("{relative}/")));
            }
            tree.insert(
                relative,
                Shown {
                    kind: attr.kind,
                    size_bytes: attr.size_bytes,
                },
            );
        }
    }
    tree
}

/// Returns the item at `relative` in the engine's Local tree.
pub(crate) fn item(engine: &Engine, relative: &str) -> Option<ItemId> {
    let mut item = ItemId::ROOT;
    for name in relative.split('/') {
        item = engine.lookup(item, OsStr::new(name))?.item;
    }
    Some(item)
}

/// Waits until the engine has applied the commit at `seq`.
pub(crate) async fn refresh(engine: &Engine, seq: ChangeSeq) {
    bounded(WAIT, engine.refresh_at_least(seq))
        .await
        .expect("the refresh ends in time")
        .expect("the refresh");
}

/// Waits until `condition` is true, for an effect of work that the engine
/// runs in the background.
pub(crate) async fn eventually(mut condition: impl FnMut() -> bool) {
    bounded(WAIT, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the condition is true in time");
}

pub(crate) async fn open(engine: &Engine, relative: &str) -> ReadHandle {
    let item = item(engine, relative).expect("the file");
    bounded(WAIT, engine.open_read(item))
        .await
        .expect("the open ends in time")
        .expect("the open")
}

pub(crate) async fn read(handle: &ReadHandle, offset: u64, length: u64) -> Bytes {
    bounded(WAIT, handle.read(offset, length))
        .await
        .expect("the read ends in time")
        .expect("the read")
}

/// Returns the operation of each request.
pub(crate) fn operations(trace: &[RequestRecord]) -> Vec<Operation> {
    trace.iter().map(|record| record.operation).collect()
}

/// Returns the content reads among the requests.
pub(crate) fn content_reads(trace: &[RequestRecord]) -> Vec<&RequestRecord> {
    trace
        .iter()
        .filter(|record| record.operation.is_content_read())
        .collect()
}

/// Returns the bytes that the content reads among the requests returned.
pub(crate) fn fetched_bytes(trace: &[RequestRecord]) -> u64 {
    trace
        .iter()
        .filter_map(|record| record.byte_range)
        .map(|range| range.length())
        .sum()
}
