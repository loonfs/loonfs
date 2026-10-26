//! A scripted namespace: the canned responses of the routes that a mount
//! calls, which a test sets up and changes as it goes.

use crate::service::{RecordedRequest, Reply, ScriptedService};
use bytes::Bytes;
use http::{Method, StatusCode};
use loonfs_mount::{Engine, MountAccess, MountConfig, Result as MountResult};
use loonfs_test_support::clock::bounded;
use loonfs_types::api::v0::{
    Commit, CreateDownloadByInodeResponse, DownloadRange, FilesystemChange, ListChangesResponse,
    ListInodeChildrenResponse, ObjectTransferAccess, PathEntry, PathEntryKind,
};
use loonfs_types::{
    AbsolutePath, ActorId, BindingVersion, ChangeSeq, ChecksumAlgorithm, CommitId, ContentId,
    ContentRef, DisplayName, ErrorCode, InodeId, NamespaceId, RevisionNo,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

/// How long a test waits for the engine.
pub(crate) const WAIT: Duration = Duration::from_secs(10);

/// The inode of the mounted directory, `/work/project`.
pub(crate) const ROOT: u64 = 10;
/// The inode of the mounted directory's parent, `/work`.
pub(crate) const WORK: u64 = 9;
/// An inode outside the mount.
pub(crate) const OUTSIDE: u64 = 2;
const NAMESPACE: &str = "demo";
const OBJECTS: &str = "http://objects.invalid";

/// What the scripted routes respond with.
pub(crate) struct Script {
    pub(crate) head_seq: u64,
    pub(crate) retention_floor_seq: u64,
    pub(crate) direct_get: bool,
    pub(crate) case_sensitive: bool,
    /// Each inode's entry, for a stat by inode or by path.
    pub(crate) entries: HashMap<u64, PathEntry>,
    /// The recursive listing of each directory, in depth-first order.
    pub(crate) listings: HashMap<u64, Vec<PathEntry>>,
    pub(crate) commits: Vec<Commit>,
    /// Each revision's bytes.
    pub(crate) contents: HashMap<(u64, u64), Bytes>,
    /// How many ranged reads return the wrong bytes before the right ones.
    pub(crate) corrupt_ranged_reads: usize,
    /// The last commit that the feed returns, so a test can keep the feed
    /// behind the head.
    pub(crate) feed_through_seq: u64,
    snapshots: u64,
}

/// A scripted namespace and the state directory of a mount of it.
pub(crate) struct ScriptedNamespace {
    pub(crate) service: ScriptedService,
    pub(crate) script: Arc<Mutex<Script>>,
    pub(crate) state_directory: TempDir,
}

pub(crate) fn namespace_id() -> NamespaceId {
    NamespaceId::parse(NAMESPACE).expect("a namespace ID")
}

fn binding_version(inode: u64) -> BindingVersion {
    BindingVersion::parse(format!("binding-{inode}")).expect("a binding version")
}

/// Returns the content reference of `bytes` under `content_id`.
pub(crate) fn content(content_id: &ContentId, bytes: &[u8]) -> ContentRef {
    ContentRef::blob_v1(
        namespace_id(),
        content_id.clone(),
        bytes,
        ChecksumAlgorithm::Crc64nvme,
    )
}

fn entry(inode: u64, parent: Option<u64>, path: &str, kind: PathEntryKind) -> PathEntry {
    let path = AbsolutePath::parse(path).expect("an absolute path");
    PathEntry {
        namespace_id: namespace_id(),
        display_name: path
            .final_component()
            .map(|component| component.to_display_name()),
        path,
        inode_id: InodeId(inode),
        created_by: ActorId::parse("tester").expect("an actor"),
        created_at_ms: 1_000,
        kind,
        head_seq: ChangeSeq(1),
        parent_inode_id: parent.map(InodeId),
        binding_version: parent.map(|_| binding_version(inode)),
        attributes: None,
    }
}

/// A directory entry.
pub(crate) fn directory(inode: u64, parent: u64, path: &str) -> PathEntry {
    entry(inode, Some(parent), path, PathEntryKind::Directory {})
}

/// A file entry at revision 1 of `content_ref`.
pub(crate) fn file(inode: u64, parent: u64, path: &str, content_ref: &ContentRef) -> PathEntry {
    entry(
        inode,
        Some(parent),
        path,
        PathEntryKind::File {
            revision_no: RevisionNo(1),
            size_bytes: content_ref.size_bytes,
            content_ref: content_ref.clone(),
            revision_committed_by: ActorId::parse("tester").expect("an actor"),
            revision_committed_at_ms: 2_000,
        },
    )
}

/// A commit at `seq` with `events`.
pub(crate) fn commit(seq: u64, events: Vec<FilesystemChange>) -> Commit {
    Commit {
        namespace_id: namespace_id(),
        commit_id: CommitId::parse(format!("commit-{seq}")).expect("a commit ID"),
        committed_seq: ChangeSeq(seq),
        committed_by: ActorId::parse("other-host").expect("an actor"),
        committed_at_ms: 3_000 + seq,
        message: None,
        events,
    }
}

/// A name for an event.
pub(crate) fn name(name: &str) -> DisplayName {
    DisplayName::parse(name).expect("a display name")
}

/// A `file_created` event.
pub(crate) fn file_created(
    inode: u64,
    parent: u64,
    file_name: &str,
    content_ref: &ContentRef,
) -> FilesystemChange {
    FilesystemChange::FileCreated {
        inode_id: InodeId(inode),
        parent_inode_id: InodeId(parent),
        display_name: name(file_name),
        binding_version: binding_version(inode),
        revision_no: RevisionNo(1),
        content_ref: content_ref.clone(),
    }
}

/// A `moved` event.
pub(crate) fn moved(inode: u64, from: u64, to: u64, new_name: &str) -> FilesystemChange {
    FilesystemChange::Moved {
        inode_id: InodeId(inode),
        source_parent_inode_id: InodeId(from),
        source_display_name: name("source"),
        destination_parent_inode_id: InodeId(to),
        destination_display_name: name(new_name),
        binding_version: BindingVersion::parse(format!("moved-{inode}-{to}"))
            .expect("a binding version"),
    }
}

/// A `deleted` event.
pub(crate) fn deleted(inode: u64, parent: u64, old_name: &str) -> FilesystemChange {
    FilesystemChange::Deleted {
        inode_id: InodeId(inode),
        deleted_binding: loonfs_types::api::v0::DirectoryBinding {
            parent_inode_id: InodeId(parent),
            name_key: loonfs_types::NameKey::parse(old_name).expect("a name key"),
            display_name: name(old_name),
        },
    }
}

impl ScriptedNamespace {
    /// Returns a namespace whose `/work/project` directory has `listing` as
    /// its subtree, at head sequence 5.
    pub(crate) fn new(listing: Vec<PathEntry>, contents: Vec<(u64, Bytes)>) -> Self {
        let mut entries = HashMap::new();
        let root = entry(1, None, "/", PathEntryKind::Directory {});
        entries.insert(1, root);
        entries.insert(WORK, directory(WORK, 1, "/work"));
        entries.insert(OUTSIDE, directory(OUTSIDE, 1, "/elsewhere"));
        entries.insert(ROOT, directory(ROOT, WORK, "/work/project"));
        for listed in &listing {
            entries.insert(listed.inode_id.0, listed.clone());
        }
        let script = Arc::new(Mutex::new(Script {
            head_seq: 5,
            retention_floor_seq: 0,
            direct_get: true,
            case_sensitive: true,
            entries,
            listings: HashMap::from([(ROOT, listing)]),
            commits: Vec::new(),
            contents: contents
                .into_iter()
                .map(|(inode, bytes)| ((inode, 1), bytes))
                .collect(),
            corrupt_ranged_reads: 0,
            feed_through_seq: u64::MAX,
            snapshots: 0,
        }));
        let service = ScriptedService::default();
        route_namespace(&service, &script);
        route_inodes(&service, &script);
        route_objects(&service, &script);
        Self {
            service,
            script,
            state_directory: tempfile::tempdir().expect("a state directory"),
        }
    }

    /// Changes the script.
    pub(crate) fn edit(&self, change: impl FnOnce(&mut Script)) {
        change(
            &mut self
                .script
                .lock()
                .expect("the script lock should not be poisoned"),
        );
    }

    /// Adds a commit and moves the head to it.
    pub(crate) fn push_commit(&self, commit: Commit) {
        self.edit(|script| {
            script.head_seq = commit.committed_seq.0;
            script.commits.push(commit);
        });
    }

    /// Returns a read-only mount's configuration, with a small free-space
    /// floor and a short poll interval.
    pub(crate) fn config(&self) -> MountConfig {
        let mut config = MountConfig::new(
            namespace_id(),
            AbsolutePath::parse("/work/project").expect("an absolute path"),
            MountAccess::ReadOnly,
            self.state_directory.path().to_path_buf(),
        );
        config.min_free_bytes = 1 << 20;
        config.poll_interval_ms = 50;
        config
    }

    /// Mounts `/work/project` with `config`.
    pub(crate) async fn mount_with(&self, config: MountConfig) -> MountResult<Engine> {
        Engine::mount(self.service.client(), config).await
    }

    /// Mounts `/work/project` with the test configuration.
    pub(crate) async fn mount(&self) -> Engine {
        self.mount_with(self.config())
            .await
            .expect("the mount is admitted")
    }

    /// Returns the requests whose path ends with `suffix`.
    pub(crate) fn requests_to(&self, suffix: &str) -> Vec<RecordedRequest> {
        self.service
            .requests()
            .into_iter()
            .filter(|request| request.path.ends_with(suffix))
            .collect()
    }
}

fn lock(script: &Arc<Mutex<Script>>) -> std::sync::MutexGuard<'_, Script> {
    script
        .lock()
        .expect("the script lock should not be poisoned")
}

fn json(value: impl serde::Serialize) -> Reply {
    Reply::Json(serde_json::to_value(value).expect("a reply should serialize"))
}

fn route_namespace(service: &ScriptedService, script: &Arc<Mutex<Script>>) {
    let state = script.clone();
    service.route(Method::GET, "/v0/capabilities", move |_| {
        let direct_get = lock(&state).direct_get;
        Reply::Json(serde_json::json!({
            "protocol_version": "v0",
            "api_groups": ["filesystem/v0"],
            "features": {
                "filesystem.commits.inline_content": true,
                "filesystem.downloads.direct_get": direct_get,
            },
            "limits": {"commit.max_inline_content_bytes_per_operation": 65536},
        }))
    });
    let state = script.clone();
    service.route(Method::GET, "/v0/namespaces/demo", move |_| {
        let script = lock(&state);
        Reply::Json(serde_json::json!({
            "namespace_id": NAMESPACE,
            "access": {"kind": "unrestricted"},
            "naming": if script.case_sensitive { "case_sensitive" } else { "case_insensitive" },
            "created_at_ms": 1_000,
            "created_by": "tester",
            "head_seq": script.head_seq,
            "retention_floor_seq": script.retention_floor_seq,
        }))
    });
    let state = script.clone();
    service.route(Method::POST, "/v0/namespaces/demo/snapshots", move |_| {
        let mut script = lock(&state);
        script.snapshots += 1;
        Reply::Json(serde_json::json!({
            "snapshot_id": format!("pin_{:020}-{:016x}", script.snapshots, script.snapshots),
            "namespace_id": NAMESPACE,
            "name": "loonfs-mount",
            "captured_seq": script.head_seq,
            "created_at_ms": 1_000,
            "expires_at_ms": 601_000,
        }))
    });
    service.route(
        Method::DELETE,
        "/v0/namespaces/demo/snapshots/*",
        |request| {
            let snapshot_id = request.path.rsplit('/').next().expect("a snapshot ID");
            Reply::Json(serde_json::json!({"namespace_id": NAMESPACE, "snapshot_id": snapshot_id}))
        },
    );
    let state = script.clone();
    service.route(
        Method::GET,
        "/v0/namespaces/demo/filesystem/entry",
        move |request| {
            let path = request.query("path").expect("a path");
            let script = lock(&state);
            match script
                .entries
                .values()
                .find(|entry| entry.path.as_str() == path)
            {
                Some(entry) => json(entry),
                None => Reply::Error(StatusCode::NOT_FOUND, ErrorCode::PathNotFound),
            }
        },
    );
    let state = script.clone();
    service.route(Method::GET, "/v0/namespaces/demo/changes", move |request| {
        let after_seq: u64 = request
            .query("after_seq")
            .and_then(|seq| seq.parse().ok())
            .expect("an `after_seq`");
        let limit: usize = request
            .query("limit")
            .and_then(|limit| limit.parse().ok())
            .expect("a `limit`");
        let script = lock(&state);
        if after_seq < script.retention_floor_seq {
            return Reply::Error(StatusCode::CONFLICT, ErrorCode::RebootstrapRequired);
        }
        let mut changes: Vec<Commit> = script
            .commits
            .iter()
            .filter(|commit| {
                commit.committed_seq.0 > after_seq
                    && commit.committed_seq.0 <= script.feed_through_seq
            })
            .cloned()
            .collect();
        let more = changes.len() > limit;
        changes.truncate(limit);
        json(ListChangesResponse {
            namespace_id: namespace_id(),
            after_seq: ChangeSeq(after_seq),
            head_seq: ChangeSeq(script.head_seq),
            next_after_seq: more
                .then(|| changes.last().map(|commit| commit.committed_seq))
                .flatten(),
            changes,
        })
    });
}

fn route_inodes(service: &ScriptedService, script: &Arc<Mutex<Script>>) {
    let state = script.clone();
    service.route(
        Method::GET,
        "/v0/namespaces/demo/inodes/*",
        move |request| {
            let parts: Vec<&str> = request.path.split('/').skip(5).collect();
            let inode: u64 = parts[0]
                .strip_prefix("ino_")
                .and_then(|inode| inode.parse().ok())
                .expect("an inode ID");
            let mut script = lock(&state);
            match parts.as_slice() {
                [_] => match script.entries.get(&inode) {
                    Some(entry) => json(entry),
                    None => Reply::Error(StatusCode::NOT_FOUND, ErrorCode::InodeNotFound),
                },
                [_, "children"] => list_children(&script, inode, request),
                [_, "revisions", revision, "content"] => {
                    let revision: u64 = revision.parse().expect("a revision number");
                    let Some(bytes) = script.contents.get(&(inode, revision)).cloned() else {
                        return Reply::Error(StatusCode::NOT_FOUND, ErrorCode::RevisionNotFound);
                    };
                    if request.header("range").is_some() && script.corrupt_ranged_reads > 0 {
                        script.corrupt_ranged_reads -= 1;
                        return Reply::Content(Bytes::from(vec![b'!'; bytes.len()]));
                    }
                    Reply::Content(bytes)
                }
                _ => Reply::Error(StatusCode::NOT_FOUND, ErrorCode::RouteNotFound),
            }
        },
    );
    let state = script.clone();
    service.route(
        Method::POST,
        "/v0/namespaces/demo/inodes/*",
        move |request| {
            let parts: Vec<&str> = request.path.split('/').skip(5).collect();
            let [inode, "revisions", revision, "downloads"] = parts.as_slice() else {
                return Reply::Error(StatusCode::NOT_FOUND, ErrorCode::RouteNotFound);
            };
            let inode: u64 = inode
                .strip_prefix("ino_")
                .and_then(|inode| inode.parse().ok())
                .expect("an inode ID");
            let revision: u64 = revision.parse().expect("a revision number");
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("a download request body");
            let start_offset = body["start_offset"].as_u64().unwrap_or(0);
            let script = lock(&state);
            let bytes = &script.contents[&(inode, revision)];
            let size = bytes.len() as u64;
            let content_id = ContentId::parse(format!("con_{:032x}", inode * 1_000 + revision))
                .expect("a content ID");
            json(CreateDownloadByInodeResponse {
                namespace_id: namespace_id(),
                inode_id: InodeId(inode),
                revision_no: RevisionNo(revision),
                content_ref: content(&content_id, bytes),
                ranges: vec![DownloadRange {
                    start_offset,
                    length: size - start_offset,
                    access: ObjectTransferAccess::PresignedUrl {
                        method: "GET".to_owned(),
                        url: format!("{OBJECTS}/{inode}/{revision}"),
                        headers: BTreeMap::from([(
                            "range".to_owned(),
                            format!("bytes={start_offset}-{}", size - 1),
                        )]),
                        expires_at_ms: u64::MAX,
                    },
                }],
            })
        },
    );
}

fn route_objects(service: &ScriptedService, script: &Arc<Mutex<Script>>) {
    let state = script.clone();
    service.route(Method::GET, "/*", move |request| {
        let parts: Vec<u64> = request
            .path
            .trim_start_matches('/')
            .split('/')
            .filter_map(|part| part.parse().ok())
            .collect();
        let [inode, revision] = parts.as_slice() else {
            return Reply::Error(StatusCode::NOT_FOUND, ErrorCode::RouteNotFound);
        };
        Reply::Content(lock(&state).contents[&(*inode, *revision)].clone())
    });
}

fn list_children(script: &Script, inode: u64, request: &RecordedRequest) -> Reply {
    let Some(listing) = script.listings.get(&inode) else {
        return Reply::Error(StatusCode::NOT_FOUND, ErrorCode::InodeNotFound);
    };
    let limit: usize = request
        .query("limit")
        .and_then(|limit| limit.parse().ok())
        .expect("a `limit`");
    let start: usize = request
        .query("cursor")
        .and_then(|cursor| cursor.parse().ok())
        .unwrap_or(0);
    let end = (start + limit).min(listing.len());
    json(ListInodeChildrenResponse {
        namespace_id: namespace_id(),
        parent_inode_id: InodeId(inode),
        head_seq: ChangeSeq(script.head_seq),
        entries: listing[start..end].to_vec(),
        next_cursor: (end < listing.len()).then(|| end.to_string()),
    })
}

/// Waits until the engine has applied the commit at `seq`.
pub(crate) async fn refresh(engine: &Engine, seq: u64) {
    bounded(WAIT, engine.refresh_at_least(ChangeSeq(seq)))
        .await
        .expect("the refresh ends in time")
        .expect("the refresh");
}

/// Waits until `condition` is true, for an effect of work that the engine runs
/// in the background.
pub(crate) async fn eventually(mut condition: impl FnMut() -> bool) {
    bounded(WAIT, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the condition is true in time");
}
