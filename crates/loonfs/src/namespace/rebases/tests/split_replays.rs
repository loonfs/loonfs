//! Rebases replayed in one replay commit and with a replay commit after each
//! of the fork's commits. A replay commit skips each conflicting part of a
//! visible inode's change on its own and applies the rest, so most
//! histories end with the same tree in main either way. A history in which
//! the fork passes through a name that only a split replay sees can end
//! differently, and its replay commits report the conflict.

use crate::{
    AccessState, ActorId, AttributeChanges, DeleteOptions, ListOptions, LoonFs, Namespace,
    PutFileOptions, ReplayForkResponse, Writable,
};
use futures::future::BoxFuture;
use futures::FutureExt;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{attribute_key, attribute_text, namespace_id, writer_id};
use loonfs_types::options::AttributeInclusion;
use loonfs_types::{
    AccessGrants, AccessRight, AccessRights, CommitSeq, DeleteDirectoryBehavior,
    DestinationBehavior, FilesystemEvent, InodeId, MergeConflictKind, PrincipalId, RevisionNo,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::TempDir;

fn actor() -> ActorId {
    loonfs_test_support::test_actor()
}

/// Each conflict of a replay commit, as its kind and its path in the fork.
type Report = Vec<(MergeConflictKind, Option<String>)>;

/// `main` with the file `/x.txt` (`base`, with the attribute `owner=base`)
/// and the directory `/d`, its fork `main-fork`, and the rebase's new fork
/// once the first replay commit ran.
struct History {
    _directory: TempDir,
    runtime: LoonFs<Writable>,
    main: Namespace<Writable>,
    fork: Namespace<Writable>,
    new_fork: Option<Namespace<Writable>>,
    reports: Vec<Report>,
    /// Whether the history replays after each of the fork's commits.
    split: bool,
}

impl History {
    async fn new(split: bool) -> Self {
        Self::seeded(split, &[]).await
    }

    /// Creates the history of [`History`], with main also having the files
    /// `files` (each with the bytes `base`) before the fork.
    async fn seeded(split: bool, files: &[&str]) -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let runtime = LoonFs::builder_with_store(store, crate::Context::Service)
            .writer_id(writer_id("rebase-writer"))
            .build()
            .await
            .expect("runtime");
        let [main_id, fork_id] = ["main", "main-fork"].map(namespace_id);
        runtime
            .create_namespace(&main_id, &actor())
            .await
            .expect("main");
        let main = runtime.open_namespace(&main_id).expect("open main");
        main.put_file("/x.txt", b"base", &actor())
            .await
            .expect("seed");
        set_attribute(&main, "/x.txt", "base").await;
        main.create_directory("/d", &actor())
            .await
            .expect("seed a directory");
        for path in files {
            main.put_file(path, b"base", &actor())
                .await
                .expect("seed a file");
        }
        runtime
            .fork_namespace(&main_id, &fork_id, &actor())
            .await
            .expect("fork");
        let fork = runtime.open_namespace(&fork_id).expect("open the fork");
        Self {
            _directory: directory,
            runtime,
            main,
            fork,
            new_fork: None,
            reports: Vec::new(),
            split,
        }
    }

    /// Creates the rebase's new fork on the first call, and replays the
    /// fork's commits so far into it.
    async fn replay(&mut self) {
        let new_fork = match self.new_fork.take() {
            Some(new_fork) => new_fork,
            None => {
                let new_fork_id = namespace_id("main-rebase");
                self.runtime
                    .fork_namespace_for_rebase(
                        self.main.id(),
                        self.fork.id(),
                        &new_fork_id,
                        &actor(),
                    )
                    .await
                    .expect("the rebase's new fork");
                self.runtime
                    .open_namespace(&new_fork_id)
                    .expect("open the new fork")
            }
        };
        let replayed = new_fork
            .replay_fork(self.fork.id(), &actor())
            .await
            .expect("replay");
        self.reports.push(summary(&replayed));
        self.new_fork = Some(new_fork);
    }

    /// Replays the fork's commits so far in a split history. Each fork
    /// commit below calls it.
    async fn split_here(&mut self) {
        if self.split {
            self.replay().await;
        }
    }

    /// Replays the fork in one replay commit, unless a split history
    /// already replayed each of its commits, merges the new fork into main,
    /// and returns main's tree and the replay commits' reports.
    async fn finish(mut self) -> (Tree, Vec<Report>) {
        if !self.split {
            self.replay().await;
        }
        let new_fork = self.new_fork.as_ref().expect("the new fork");
        self.main
            .merge_fork(new_fork.id(), &actor())
            .await
            .expect("merge the new fork");
        (tree(&self.main).await, self.reports)
    }

    async fn write(&mut self, path: &str, bytes: &[u8]) {
        write(&self.fork, path, bytes).await;
        self.split_here().await;
    }

    async fn set_attribute(&mut self, path: &str, owner: &str) {
        set_attribute(&self.fork, path, owner).await;
        self.split_here().await;
    }

    async fn set_access(&mut self, path: &str, access: AccessState) {
        set_access(&self.fork, path, access).await;
        self.split_here().await;
    }

    async fn rename(&mut self, from: &str, to: &str) {
        rename(&self.fork, from, to).await;
        self.split_here().await;
    }

    async fn create_directory(&mut self, path: &str) {
        self.fork
            .create_directory(path, &actor())
            .await
            .expect("create a directory");
        self.split_here().await;
    }

    /// Deletes `path` in the fork, and returns the inode and the deletion's
    /// sequence.
    async fn delete(&mut self, path: &str) -> (InodeId, CommitSeq) {
        let inode_id = self.fork.stat(path).await.expect("stat").inode_id;
        let deleted = self.fork.delete_path(path, &actor()).await.expect("delete");
        self.split_here().await;
        (inode_id, deleted.committed_seq)
    }

    async fn restore(&mut self, (inode_id, deletion_seq): (InodeId, CommitSeq)) {
        self.fork
            .restore_deleted(inode_id, deletion_seq, &actor())
            .await
            .expect("restore");
        self.split_here().await;
    }
}

/// Deletes the directory `path` in `namespace` and everything below it.
async fn delete_tree(namespace: &Namespace<Writable>, path: &str) {
    namespace
        .delete_path_with_options(
            path,
            &actor(),
            &DeleteOptions {
                behavior: DeleteDirectoryBehavior::Recursive,
                ..DeleteOptions::default()
            },
        )
        .await
        .expect("delete recursively");
}

/// A namespace's tree: each path's bytes, attributes, and the last access
/// row that the namespace's change feed published for the path's inode.
type Tree = BTreeMap<String, String>;

async fn tree(namespace: &Namespace<Writable>) -> Tree {
    let mut access: BTreeMap<InodeId, String> = BTreeMap::new();
    let commits = namespace
        .list_changes(CommitSeq(0))
        .collect_up_to(10_000)
        .await
        .expect("the change feed");
    for event in commits.into_iter().flat_map(|commit| commit.events) {
        if let FilesystemEvent::AccessChanged {
            inode_id,
            boundary,
            grants,
            ..
        } = event
        {
            access.insert(inode_id, format!("{boundary} {grants:?}"));
        }
    }
    let entries = namespace
        .list_with_options(
            "/",
            &ListOptions {
                include_attributes: AttributeInclusion::Include,
                recursive: true,
            },
        )
        .collect_up_to(10_000)
        .await
        .expect("list");
    let mut tree = Tree::new();
    for entry in entries {
        let path = entry.path.to_string();
        let bytes = namespace
            .read_file(&path)
            .await
            .ok()
            .map(|file| String::from_utf8_lossy(&file.bytes).into_owned());
        tree.insert(
            path,
            format!(
                "bytes={bytes:?} attributes={:?} access={}",
                entry.attributes.map(|projection| projection.attributes),
                access.get(&entry.inode_id).map_or("none", String::as_str)
            ),
        );
    }
    tree
}

fn summary(replayed: &ReplayForkResponse) -> Report {
    replayed
        .conflicts
        .iter()
        .map(|conflict| {
            (
                conflict.kind,
                conflict
                    .ours
                    .as_ref()
                    .map(|version| version.path.to_string()),
            )
        })
        .collect()
}

/// The report entry of a conflict of `kind` on the fork's `path`.
fn on(kind: MergeConflictKind, path: &str) -> (MergeConflictKind, Option<String>) {
    (kind, Some(path.to_owned()))
}

async fn write(namespace: &Namespace<Writable>, path: &str, bytes: &[u8]) {
    namespace
        .put_file_with_options(
            path,
            bytes,
            &actor(),
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::default()
            },
        )
        .await
        .expect("write");
}

/// Sets the attribute `owner` of `path` to `owner`.
async fn set_attribute(namespace: &Namespace<Writable>, path: &str, owner: &str) {
    namespace
        .update_attributes(
            path,
            &actor(),
            AttributeChanges {
                set: BTreeMap::from([(attribute_key("owner"), attribute_text(owner))]),
                ..AttributeChanges::default()
            },
        )
        .await
        .expect("set the attribute");
}

async fn set_access(namespace: &Namespace<Writable>, path: &str, access: AccessState) {
    namespace
        .update_access(path, &actor(), access)
        .await
        .expect("set access");
}

async fn rename(namespace: &Namespace<Writable>, from: &str, to: &str) {
    namespace.move_path(from, to, &actor()).await.expect("move");
}

/// Access with a grant of `rights` to the principal `team`.
fn grants(rights: &[AccessRight]) -> AccessState {
    AccessState {
        boundary: false,
        grants: AccessGrants::new(BTreeMap::from([(
            PrincipalId::parse("team").expect("principal"),
            AccessRights::from_iter(rights.iter().copied()),
        )]))
        .expect("grants"),
    }
}

/// The text that [`tree`] shows for `access`.
fn access_text(access: &AccessState) -> String {
    format!("{} {:?}", access.boundary, access.grants)
}

/// A history: main's commits and the fork's, run in one replay or split.
type Steps = fn(bool) -> BoxFuture<'static, History>;

/// Runs `steps` in one replay and split, and returns main's tree and the
/// reports of each run.
async fn both(steps: Steps) -> ((Tree, Vec<Report>), (Tree, Vec<Report>)) {
    let one = steps(false).await.finish().await;
    let split = steps(true).await.finish().await;
    (one, split)
}

/// Main writes `/x.txt`, and the fork writes it and then sets an attribute.
fn content_conflict_then_attribute(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/x.txt", b"main-1").await;
        history.write("/x.txt", b"fork-1").await;
        history.set_attribute("/x.txt", "fork-2").await;
        history
    }
    .boxed()
}

/// Main writes `/x.txt`, and the fork writes it and then renames it.
fn content_conflict_then_rename(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/x.txt", b"main-1").await;
        history.write("/x.txt", b"fork-1").await;
        history.rename("/x.txt", "/y.txt").await;
        history
    }
    .boxed()
}

/// Main takes `/taken.txt`, and the fork moves `/x.txt` there and writes it.
fn move_to_a_taken_name_then_write(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/taken.txt", b"main's").await;
        history.rename("/x.txt", "/taken.txt").await;
        history.write("/taken.txt", b"fork-2").await;
        history
    }
    .boxed()
}

/// The fork sets an attribute and sets it back.
fn attribute_set_back(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/other.txt", b"main").await;
        history.set_attribute("/x.txt", "fork-1").await;
        history.set_attribute("/x.txt", "base").await;
        history
    }
    .boxed()
}

/// The fork deletes `/x.txt` and restores it.
fn delete_then_restore(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/other.txt", b"main").await;
        let deleted = history.delete("/x.txt").await;
        history.restore(deleted).await;
        history
    }
    .boxed()
}

/// The fork deletes `/x.txt` and main merges the fork, then the fork
/// restores `/x.txt` and deletes it again.
fn restore_then_delete(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        let x = history.fork.stat("/x.txt").await.expect("stat").inode_id;
        let deleted = history
            .fork
            .delete_path("/x.txt", &actor())
            .await
            .expect("delete");
        history
            .main
            .merge_fork(history.fork.id(), &actor())
            .await
            .expect("merge the delete");
        write(&history.main, "/other.txt", b"main").await;
        history.restore((x, deleted.committed_seq)).await;
        history.delete("/x.txt").await;
        history
    }
    .boxed()
}

/// Main writes `/x.txt`, and the fork writes it and then restores the
/// revision that it started from.
fn content_conflict_then_back(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/x.txt", b"main-1").await;
        history.write("/x.txt", b"fork-1").await;
        history
            .fork
            .restore_revision("/x.txt", RevisionNo(1), &actor())
            .await
            .expect("restore the first revision");
        history.split_here().await;
        history
    }
    .boxed()
}

/// Main and the fork set an attribute of `/d`, and the fork renames it.
fn directory_rename_beside_an_attribute_conflict(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        set_attribute(&history.main, "/d", "main").await;
        history.set_attribute("/d", "fork").await;
        history.rename("/d", "/e").await;
        history
    }
    .boxed()
}

/// Main renames `/d`, and the fork renames it too and sets an attribute.
fn directory_attribute_beside_a_rename_conflict(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        rename(&history.main, "/d", "/f").await;
        history.rename("/d", "/e").await;
        history.set_attribute("/e", "fork").await;
        history
    }
    .boxed()
}

/// Main takes `/e`, and the fork renames `/d` to `/e` and writes a file in
/// it.
fn file_in_a_directory_whose_move_is_skipped(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/e", b"main's e").await;
        history.rename("/d", "/e").await;
        history.write("/e/new.txt", b"new").await;
        history
    }
    .boxed()
}

/// Main deletes `/d`, and the fork moves `/x.txt` into it and writes it.
fn move_into_a_deleted_directory_then_write(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        history
            .main
            .delete_path("/d", &actor())
            .await
            .expect("delete");
        history.rename("/x.txt", "/d/x.txt").await;
        history.write("/d/x.txt", b"fork-2").await;
        history
    }
    .boxed()
}

/// Main takes `/n`, and the fork creates the directory `/n`, moves `/x.txt`
/// into it, and writes it.
fn move_into_a_skipped_create_then_write(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/n", b"main's n").await;
        history.create_directory("/n").await;
        history.rename("/x.txt", "/n/x.txt").await;
        history.write("/n/x.txt", b"fork-2").await;
        history
    }
    .boxed()
}

/// Main writes below `/d`, and the fork renames `/d` and sets an attribute.
fn subtree_conflict_then_attribute(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/d/m.txt", b"main's m").await;
        history.rename("/d", "/e").await;
        history.set_attribute("/e", "fork").await;
        history
    }
    .boxed()
}

/// Main takes `/taken.txt`, and the fork moves `/x.txt` there and deletes
/// it.
fn move_to_a_taken_name_then_delete(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/taken.txt", b"main's").await;
        history.rename("/x.txt", "/taken.txt").await;
        history.delete("/taken.txt").await;
        history
    }
    .boxed()
}

/// Main takes `/taken.txt`, and the fork moves `/x.txt` there and then to
/// a free name.
fn move_to_a_taken_name_then_a_free_one(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/taken.txt", b"main's").await;
        history.rename("/x.txt", "/taken.txt").await;
        history.rename("/taken.txt", "/free.txt").await;
        history
    }
    .boxed()
}

/// Main takes `/e`, and the fork renames `/d` to `/e`, moves `/x.txt` into
/// it, and writes it.
fn move_into_a_directory_whose_move_is_skipped(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/e", b"main's e").await;
        history.rename("/d", "/e").await;
        history.rename("/x.txt", "/e/x.txt").await;
        history.write("/e/x.txt", b"fork-3").await;
        history
    }
    .boxed()
}

/// Main renames `/x.txt`, and the fork swaps the names of `/x.txt` and `/d`
/// through `/t`, so the half of the swap that moves `/x.txt` conflicts.
fn swap_with_a_binding_conflict(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        rename(&history.main, "/x.txt", "/c.txt").await;
        history.rename("/x.txt", "/t").await;
        history.rename("/d", "/x.txt").await;
        history.rename("/t", "/d").await;
        history
    }
    .boxed()
}

/// Main renames `/d`, and the fork renames it too and writes files in it.
fn directory_rename_conflict_with_children(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        rename(&history.main, "/d", "/f").await;
        history.rename("/d", "/e").await;
        history.write("/e/a.txt", b"a").await;
        history.write("/e/b.txt", b"b").await;
        history
    }
    .boxed()
}

/// The fork renames `/d` and writes files in it.
fn directory_rename_with_children(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/other.txt", b"main").await;
        history.rename("/d", "/e").await;
        history.write("/e/a.txt", b"a").await;
        history.write("/e/b.txt", b"b").await;
        history
    }
    .boxed()
}

/// Main writes `/x.txt`, and the fork writes it, deletes it, and creates a
/// new `/x.txt`.
fn content_conflict_then_delete_then_create(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/x.txt", b"main-1").await;
        history.write("/x.txt", b"fork-1").await;
        history.delete("/x.txt").await;
        history.write("/x.txt", b"new").await;
        history
    }
    .boxed()
}

/// Main and the fork set an attribute of `/x.txt`, and the fork writes it.
fn attribute_conflict_then_content(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        set_attribute(&history.main, "/x.txt", "main").await;
        history.set_attribute("/x.txt", "fork").await;
        history.write("/x.txt", b"fork-2").await;
        history
    }
    .boxed()
}

/// Main writes `/x.txt`, and the fork sets its access, writes it, and sets
/// its access again.
fn access_beside_a_content_conflict(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/x.txt", b"main-1").await;
        history
            .set_access("/x.txt", grants(&[AccessRight::Read]))
            .await;
        history.write("/x.txt", b"fork-1").await;
        history
            .set_access("/x.txt", grants(&[AccessRight::Read, AccessRight::Write]))
            .await;
        history
    }
    .boxed()
}

/// Main deletes `/d`, and the fork moves `/x.txt` into it, writes it, and
/// moves it to a free name.
fn move_into_a_deleted_directory_then_a_free_name(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        history
            .main
            .delete_path("/d", &actor())
            .await
            .expect("delete");
        history.rename("/x.txt", "/d/x.txt").await;
        history.write("/d/x.txt", b"fork-2").await;
        history.rename("/d/x.txt", "/y.txt").await;
        history
    }
    .boxed()
}

/// Main takes `/n`, and the fork creates the directory `/n`, moves `/x.txt`
/// into it, writes it, and moves it to a free name.
fn move_into_a_skipped_create_then_a_free_name(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/n", b"main's n").await;
        history.create_directory("/n").await;
        history.rename("/x.txt", "/n/x.txt").await;
        history.write("/n/x.txt", b"fork-2").await;
        history.rename("/n/x.txt", "/y.txt").await;
        history
    }
    .boxed()
}

/// Main writes below `/d`, and the fork renames `/d`, sets its attribute,
/// moves its child out, and moves `/x.txt` into it.
fn subtree_conflict_with_children_in_and_out(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::seeded(split, &["/d/c.txt"]).await;
        write(&history.main, "/d/m.txt", b"main's m").await;
        history.rename("/d", "/e").await;
        history.set_attribute("/e", "fork").await;
        history.rename("/e/c.txt", "/c.txt").await;
        history.rename("/x.txt", "/e/x.txt").await;
        history
    }
    .boxed()
}

/// Main deletes `/d` and `/d/y.txt`, and the fork writes `/d/y.txt`, sets
/// its attribute, and moves it out.
fn file_under_a_deleted_directory(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::seeded(split, &["/d/y.txt"]).await;
        delete_tree(&history.main, "/d").await;
        history.write("/d/y.txt", b"fork-1").await;
        history.set_attribute("/d/y.txt", "fork").await;
        history.rename("/d/y.txt", "/y.txt").await;
        history
    }
    .boxed()
}

/// Main deletes `/d` and `/d/y.txt`, and the fork moves `/d/y.txt` out and
/// then writes it.
fn file_moved_out_of_a_deleted_directory(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::seeded(split, &["/d/y.txt"]).await;
        delete_tree(&history.main, "/d").await;
        history.rename("/d/y.txt", "/y.txt").await;
        history.write("/y.txt", b"fork-1").await;
        history
    }
    .boxed()
}

/// Main deletes `/x.txt`, and the fork writes it, sets its attribute, and
/// moves it.
fn file_that_main_deleted(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        history
            .main
            .delete_path("/x.txt", &actor())
            .await
            .expect("delete");
        history.write("/x.txt", b"fork-1").await;
        history.set_attribute("/x.txt", "fork").await;
        history.rename("/x.txt", "/y.txt").await;
        history
    }
    .boxed()
}

/// Main takes `/n`, and the fork creates `/n`, writes it, and moves it to a
/// free name.
fn skipped_create_then_a_free_name(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/n", b"main's n").await;
        history.write("/n", b"fork-1").await;
        history.write("/n", b"fork-2").await;
        history.rename("/n", "/m").await;
        history
    }
    .boxed()
}

/// Main renames `/d`, and the fork renames it too, moves its child out,
/// moves `/x.txt` into it, and renames it again.
fn skipped_directory_move_with_children_in_and_out(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::seeded(split, &["/d/c.txt"]).await;
        rename(&history.main, "/d", "/f").await;
        history.rename("/d", "/e").await;
        history.rename("/e/c.txt", "/c.txt").await;
        history.rename("/x.txt", "/e/x.txt").await;
        history.rename("/e", "/g").await;
        history
    }
    .boxed()
}

/// Main writes `/x.txt`, and the fork writes it and deletes it (the delete
/// conflicts on content), then creates a directory at `/x.txt` and a file
/// in it.
fn conflicting_delete_then_a_directory(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/x.txt", b"main-1").await;
        history.write("/x.txt", b"fork-1").await;
        history.delete("/x.txt").await;
        history.create_directory("/x.txt").await;
        history.write("/x.txt/a.txt", b"a").await;
        history
    }
    .boxed()
}

/// Main takes `/taken.txt` and sets the attribute of `/x.txt`, and the
/// fork moves `/x.txt` to `/taken.txt`, sets its attribute, and writes it.
fn two_conflicts_then_write(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/taken.txt", b"main's").await;
        set_attribute(&history.main, "/x.txt", "main").await;
        history.rename("/x.txt", "/taken.txt").await;
        history.set_attribute("/taken.txt", "fork").await;
        history.write("/taken.txt", b"fork-3").await;
        history
    }
    .boxed()
}

/// The histories that end with the same tree in main in one replay and
/// split.
const SAME_END: &[(&str, Steps)] = &[
    (
        "content conflict, then attribute",
        content_conflict_then_attribute,
    ),
    (
        "content conflict, then rename",
        content_conflict_then_rename,
    ),
    (
        "move to a taken name, then write",
        move_to_a_taken_name_then_write,
    ),
    ("attribute set back", attribute_set_back),
    ("delete, then restore", delete_then_restore),
    ("restore, then delete", restore_then_delete),
    ("content conflict, then back", content_conflict_then_back),
    (
        "directory rename beside an attribute conflict",
        directory_rename_beside_an_attribute_conflict,
    ),
    (
        "directory attribute beside a rename conflict",
        directory_attribute_beside_a_rename_conflict,
    ),
    (
        "file in a directory whose move is skipped",
        file_in_a_directory_whose_move_is_skipped,
    ),
    (
        "move into a deleted directory, then write",
        move_into_a_deleted_directory_then_write,
    ),
    (
        "move into a skipped create, then write",
        move_into_a_skipped_create_then_write,
    ),
    (
        "subtree conflict, then attribute",
        subtree_conflict_then_attribute,
    ),
    (
        "move to a taken name, then delete",
        move_to_a_taken_name_then_delete,
    ),
    (
        "move to a taken name, then a free one",
        move_to_a_taken_name_then_a_free_one,
    ),
    (
        "move into a directory whose move is skipped",
        move_into_a_directory_whose_move_is_skipped,
    ),
    ("swap with a binding conflict", swap_with_a_binding_conflict),
    (
        "directory rename conflict with children",
        directory_rename_conflict_with_children,
    ),
    (
        "directory rename with children",
        directory_rename_with_children,
    ),
    (
        "content conflict, then delete, then create",
        content_conflict_then_delete_then_create,
    ),
    (
        "attribute conflict, then content",
        attribute_conflict_then_content,
    ),
    (
        "access beside a content conflict",
        access_beside_a_content_conflict,
    ),
    (
        "move into a deleted directory, then a free name",
        move_into_a_deleted_directory_then_a_free_name,
    ),
    (
        "move into a skipped create, then a free name",
        move_into_a_skipped_create_then_a_free_name,
    ),
    (
        "subtree conflict with children in and out",
        subtree_conflict_with_children_in_and_out,
    ),
    (
        "file under a deleted directory",
        file_under_a_deleted_directory,
    ),
    (
        "file moved out of a deleted directory",
        file_moved_out_of_a_deleted_directory,
    ),
    ("file that main deleted", file_that_main_deleted),
    (
        "skipped create, then a free name",
        skipped_create_then_a_free_name,
    ),
    (
        "skipped directory move with children in and out",
        skipped_directory_move_with_children_in_and_out,
    ),
    (
        "conflicting delete, then a directory",
        conflicting_delete_then_a_directory,
    ),
    ("two conflicts, then write", two_conflicts_then_write),
];

/// Checks that each history in [`SAME_END`] ends with the same tree in main
/// in one replay and split.
#[tokio::test]
async fn a_split_replay_ends_as_one_replay() {
    for (name, steps) in SAME_END {
        let ((one, one_reports), (split, split_reports)) = both(*steps).await;
        assert_eq!(
            split, one,
            "{name}: one replay reported {one_reports:?}, split {split_reports:?}"
        );
    }
}

/// Checks main's tree after a few of the histories in [`SAME_END`], so
/// that the comparison isn't of two equal wrong trees.
#[tokio::test]
async fn the_parts_beside_a_conflict_apply() {
    let ((tree, reports), _) = both(content_conflict_then_attribute).await;
    assert!(tree["/x.txt"].contains("main-1") && tree["/x.txt"].contains("fork-2"));
    assert_eq!(reports, [vec![on(MergeConflictKind::Content, "/x.txt")]]);
    let ((tree, _), _) = both(content_conflict_then_rename).await;
    assert!(tree["/y.txt"].contains("main-1") && !tree.contains_key("/x.txt"));
    let ((tree, _), _) = both(move_into_a_deleted_directory_then_write).await;
    assert!(tree["/x.txt"].contains("fork-2"));
    let ((tree, _), _) = both(subtree_conflict_then_attribute).await;
    assert!(tree["/d"].contains("fork") && tree.contains_key("/d/m.txt"));
    let ((tree, one_reports), (_, split_reports)) = both(content_conflict_then_back).await;
    assert!(tree["/x.txt"].contains("main-1"));
    assert_eq!(one_reports, [Report::new()]);
    assert_eq!(split_reports[1], [on(MergeConflictKind::Content, "/x.txt")]);
    let ((tree, _), _) = both(access_beside_a_content_conflict).await;
    let read_write = grants(&[AccessRight::Read, AccessRight::Write]);
    assert!(tree["/x.txt"].contains(&access_text(&read_write)));
}

/// Main takes `/tmp`, and the fork swaps the names of `/x.txt` and `/d`
/// through `/tmp`.
fn swap_through_a_taken_name(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/tmp", b"main's tmp").await;
        history.rename("/x.txt", "/tmp").await;
        history.rename("/d", "/x.txt").await;
        history.rename("/tmp", "/d").await;
        history
    }
    .boxed()
}

/// Checks a swap through a name that main took. One replay sees only the
/// swap, applies it, and reports nothing. A split replay sees the fork
/// pass through the taken name, reports a `name` conflict in each replay
/// commit, and moves nothing.
#[tokio::test]
async fn a_split_swap_through_a_taken_name_reports_the_name() {
    let ((one, one_reports), (split, split_reports)) = both(swap_through_a_taken_name).await;
    assert_eq!(one_reports, [Report::new()]);
    assert!(one["/d"].contains("base") && one["/x.txt"].contains("bytes=None"));
    assert_eq!(
        split_reports,
        [
            vec![on(MergeConflictKind::Name, "/tmp")],
            vec![on(MergeConflictKind::Name, "/x.txt")],
            vec![on(MergeConflictKind::Name, "/d")],
        ]
    );
    assert!(split["/x.txt"].contains("base") && split["/d"].contains("bytes=None"));
}

/// Main takes `/taken.txt`, and the fork moves `/x.txt` there, deletes it,
/// and restores it.
fn move_to_a_taken_name_then_delete_then_restore(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/taken.txt", b"main's").await;
        history.rename("/x.txt", "/taken.txt").await;
        let deleted = history.delete("/taken.txt").await;
        history.restore(deleted).await;
        history
    }
    .boxed()
}

/// Checks a move to a name that main took, then a delete and a restore.
/// One replay sees the move only, skips it for its name, and keeps
/// `/x.txt`. A split replay applies the delete and then skips the restore
/// for its name, so `/x.txt` ends deleted.
#[tokio::test]
async fn a_split_restore_to_a_taken_name_reports_the_name() {
    let ((one, one_reports), (split, split_reports)) =
        both(move_to_a_taken_name_then_delete_then_restore).await;
    assert_eq!(
        one_reports,
        [vec![on(MergeConflictKind::Name, "/taken.txt")]]
    );
    assert!(one["/x.txt"].contains("base"));
    assert_eq!(
        split_reports,
        [
            vec![on(MergeConflictKind::Name, "/taken.txt")],
            Vec::new(),
            vec![on(MergeConflictKind::Name, "/taken.txt")],
        ]
    );
    assert!(!split.contains_key("/x.txt"));
}

/// Merges the fork into main directly, expects `merge_conflict`, checks
/// that main is unchanged, and returns the conflict count and the first
/// conflict's kind and fork path.
async fn refused_merge(history: &History) -> (u64, (MergeConflictKind, Option<String>)) {
    let before = tree(&history.main).await;
    let error = history
        .main
        .merge_fork(history.fork.id(), &actor())
        .await
        .expect_err("the merge conflicts");
    let crate::Error::Core(crate::CoreError::MergeConflict {
        conflict_count,
        conflict,
    }) = error
    else {
        panic!("not a merge conflict: {error:?}");
    };
    assert_eq!(
        tree(&history.main).await,
        before,
        "the merge applies nothing"
    );
    let path = conflict
        .ours
        .as_ref()
        .map(|version| version.path.to_string());
    (conflict_count, (conflict.kind, path))
}

/// Checks that a merge whose move, content, and attributes all conflict
/// reports the move.
#[tokio::test]
async fn a_merge_reports_a_conflicting_move_first() {
    let history = History::new(false).await;
    rename(&history.main, "/x.txt", "/m.txt").await;
    write(&history.main, "/m.txt", b"main-1").await;
    set_attribute(&history.main, "/m.txt", "main").await;
    rename(&history.fork, "/x.txt", "/y.txt").await;
    write(&history.fork, "/y.txt", b"fork-1").await;
    set_attribute(&history.fork, "/y.txt", "fork").await;
    assert_eq!(
        refused_merge(&history).await,
        (1, on(MergeConflictKind::Binding, "/y.txt"))
    );
}

/// Checks that a merge whose content, attributes, and access all conflict
/// reports the content.
#[tokio::test]
async fn a_merge_reports_conflicting_content_first() {
    let history = History::new(false).await;
    write(&history.main, "/x.txt", b"main-1").await;
    set_attribute(&history.main, "/x.txt", "main").await;
    set_access(&history.main, "/x.txt", grants(&[AccessRight::Read])).await;
    write(&history.fork, "/x.txt", b"fork-1").await;
    set_attribute(&history.fork, "/x.txt", "fork").await;
    set_access(
        &history.fork,
        "/x.txt",
        grants(&[AccessRight::Read, AccessRight::Write]),
    )
    .await;
    assert_eq!(
        refused_merge(&history).await,
        (1, on(MergeConflictKind::Content, "/x.txt"))
    );
}

/// Checks that a merge whose attributes and access conflict reports the
/// attributes, and that a replay commit applies the content and reports
/// both.
#[tokio::test]
async fn a_merge_reports_conflicting_attributes_first() {
    let mut history = History::new(false).await;
    set_attribute(&history.main, "/x.txt", "main").await;
    set_access(&history.main, "/x.txt", grants(&[AccessRight::Read])).await;
    write(&history.fork, "/x.txt", b"fork-1").await;
    set_attribute(&history.fork, "/x.txt", "fork").await;
    set_access(
        &history.fork,
        "/x.txt",
        grants(&[AccessRight::Read, AccessRight::Write]),
    )
    .await;
    assert_eq!(
        refused_merge(&history).await,
        (1, on(MergeConflictKind::Attributes, "/x.txt"))
    );
    history.replay().await;
    assert_eq!(
        history.reports,
        [vec![
            on(MergeConflictKind::Attributes, "/x.txt"),
            on(MergeConflictKind::Access, "/x.txt"),
        ]]
    );
}

/// Checks that a resolver's write between replay commits is kept, and that
/// the fork's later access change applies beside it.
#[tokio::test]
async fn a_resolvers_write_between_replays_is_kept_beside_a_later_access_change() {
    let mut history = History::new(true).await;
    write(&history.main, "/x.txt", b"main-1").await;
    history.write("/x.txt", b"fork-1").await;
    let new_fork = history.new_fork.as_ref().expect("the new fork");
    write(new_fork, "/x.txt", b"resolved").await;
    let read_write = grants(&[AccessRight::Read, AccessRight::Write]);
    history.set_access("/x.txt", read_write.clone()).await;
    history.write("/x.txt", b"fork-2").await;
    let (tree, reports) = history.finish().await;
    assert!(tree["/x.txt"].contains("resolved"), "{tree:?}");
    assert!(
        tree["/x.txt"].contains(&access_text(&read_write)),
        "{tree:?}"
    );
    assert_eq!(
        reports,
        [
            vec![on(MergeConflictKind::Content, "/x.txt")],
            Vec::new(),
            vec![on(MergeConflictKind::Content, "/x.txt")],
        ]
    );
}

/// Main takes `/n`, and the fork creates `/m`, writes it, and moves it to
/// `/n`.
fn create_then_a_taken_name(split: bool) -> BoxFuture<'static, History> {
    async move {
        let mut history = History::new(split).await;
        write(&history.main, "/n", b"main's n").await;
        history.write("/m", b"fork-1").await;
        history.write("/m", b"fork-2").await;
        history.rename("/m", "/n").await;
        history
    }
    .boxed()
}

/// Checks a create whose file the fork then moves to a name that main took.
/// One replay sees only a create at the taken name and skips it. A split
/// replay creates `/m`, then reports `/n` as taken, so `/m` stays.
#[tokio::test]
async fn a_split_create_then_a_taken_name_keeps_the_create() {
    let ((one, one_reports), (split, split_reports)) = both(create_then_a_taken_name).await;
    assert_eq!(one_reports, [vec![on(MergeConflictKind::Name, "/n")]]);
    assert!(!one.contains_key("/m"));
    assert_eq!(
        split_reports,
        [
            Vec::new(),
            Vec::new(),
            vec![on(MergeConflictKind::Name, "/n")]
        ]
    );
    assert!(split["/m"].contains("fork-2"));
}
