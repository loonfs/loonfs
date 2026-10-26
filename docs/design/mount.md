# Namespace mounts

This note describes `loonfs mount`, which shows a [namespace](../specs/glossary.md#namespace), or one directory in it, as a directory on a Linux host, and `loonfs run`, which mounts one or more namespaces, runs a command, and unmounts them when the command exits. Programs on the host read and write the mounted directory with ordinary file calls, the mount publishes their changes to the namespace as commits, and other hosts that mount the same namespace see those changes. (In this note, a "host" is one Linux machine or sandbox, and a "mount" is one mounted directory together with the local state that serves it. The mount's code calls each file, directory, or symlink in the mounted tree an "item".)

A daemon on the host serves every mount through FUSE, the kernel's interface for a filesystem that a user-space process serves. For each mount, the daemon keeps a copy of the namespace's directory tree and the content it has read on local disk, so most file calls never wait for the network. It collects local changes, publishes the net result in batches, and follows the namespace's [change feed](../specs/glossary.md#change-feed) to apply other hosts' commits. The daemon is a client of the public LoonFS API like any other, and nothing in the server refers to a mount.

## The cost of sending each file call to the server

This section describes why a mount can't send each file call to the server.

A program changes files with many small calls (open, write, rename, close), and it expects each call to return at local-disk speed. A namespace changes through commits, each commit is an HTTP request that takes tens to hundreds of milliseconds, and every commit adds WAL records and metadata rows. If the mount sent a commit for each write call, a program that writes a 1 MiB file in 4 KiB calls would wait for 256 commits and leave 256 revisions in the file's history. If the mount asked the server for metadata on every lookup, a build that checks 50,000 files would send 50,000 requests.

Sharing adds a second problem. Two hosts can change the same file at the same time, and neither host's kernel can see the other host's calls. LoonFS doesn't lock files across hosts, so the mount has to find each conflict when it publishes, and resolve it without losing either version. A "conflict" is a local change to a field group (one part of an item's state, see [Local state](#local-state)) whose server state changed since the change's base, which is the published state that the local change started from.

The mount solves both problems on the host. It serves metadata calls from its local copy of the tree, serves content from a local cache, and accepts writes into local files. It publishes the net result of many calls as one commit, normally once a second. Every operation it sends includes a [precondition](../specs/glossary.md#precondition) on the state it was planned against, so a commit that conflicts with another host's change fails instead of overwriting it.

## Guarantees

This section describes what a mount promises to the programs that use it, and the words this note uses for durability.

A mount makes these promises:

- Writes go to local disk at local speed, and the mount publishes them to the namespace as atomic commits, normally within a second.
- Reads are lazy and cached. Only the first read of content that isn't cached, and opening such a file for writing, wait for the network.
- `fsync` and `loonfs sync` return once every write they cover is published.
- Other hosts see a published change within about a second, and a log that one host appends to can be followed from another host.
- When hosts write one file concurrently, the mount detects the conflict when it publishes and resolves it with the mount's conflict policy. No published version is lost, because every revision stays in the file's history until the [retention floor](../specs/glossary.md#retention-floor) passes it.
- If the host can't serve a mount, or the namespace has more entries than the budget, the mount fails before the directory is mounted, and the error says why.

This note uses two words for how durable a write is:

- "Accepted": a `write` call returned. The bytes are in the file's "working file", the local copy of a file that's being written. They survive a crash of the daemon, and they survive a crash of the machine only if the working file reached the disk, which the mount doesn't promise.
- "Published": the commit that includes the write was acknowledged. The bytes are durable in object storage.

A "barrier" is a call (`fsync` or `loonfs sync`) that returns once every write it covers, and the metadata those writes depend on (their parent directories and renames), is published. A barrier is the one durability promise that the mount makes. The publish interval (one second by default) is a target, not a deadline. Large uploads, a slow server, and outages make publication take longer, and `loonfs status` reports the age of the oldest change that isn't published.

Why doesn't a write wait for its publication? A commit takes tens to hundreds of milliseconds, and programs issue thousands of write calls a second. A program that needs a write to be durable calls `fsync`, as it would on a local disk, where `write` doesn't promise durability either.

Why does the mount resolve conflicts instead of preventing them? Preventing them would need a lock or a lease that every client of the namespace honors, which the server doesn't have, and a lock that a sandbox took before it died would block every other host until the lock expired. Preconditions make every conflict an explicit rejection at the server, and the revision history keeps every version that was published, so the mount can resolve a conflict after the fact without losing data.

## Namespace requirements

This section describes what a namespace needs before a mount can write to it.

A kernel compares file names byte for byte, but the default naming mode (`case_insensitive`) folds case and Unicode normalization (see [Names and paths](../specs/format.md#14-names-and-paths)). In such a namespace, `Makefile` and `makefile` are one name, so a program that creates both would find after publication that its second file had replaced its first. For that reason, a writable mount requires a namespace that was created with `case_sensitive` naming. The mount reads the namespace's `naming` field when it starts, and it refuses a writable mount of a `case_insensitive` namespace with that reason. A read-only mount of such a namespace is allowed, and it shows each entry under its stored display name.

The `case_sensitive` mode still compares names after NFC normalization, so a name with a precomposed accented letter (such as U+00E9) and the same name with a plain `e` followed by a combining accent are one name in the namespace and two names to the kernel. The mount refuses a create or rename locally, with `EEXIST`, when the new name has the same name key as another entry in the directory. It also refuses, with `EINVAL`, a name that LoonFS can't store (a name that isn't UTF-8, or that breaks the display-name rules, such as `a:b` or `CON`). It refuses, with `ENAMETOOLONG`, a create whose namespace path would be longer than 4,096 bytes or 128 components, because the mount's "planner", the code that turns local changes into commits, creates new items by path (see [Planning a commit](#planning-a-commit)). And it refuses, with `EFBIG`, a write that would make a file larger than the largest file that the deployment's advertised upload transports accept.

Every publication of a changed file adds a revision, so a file that changes every second gains 86,400 revisions a day, and LoonFS reclaims none of them until the retention floor passes them. A namespace that's mounted for writing should therefore have a [retention window](../specs/glossary.md#retention-window), set when the namespace is created or with a `retention_window` run (see [Retention runs](../specs/api.md#retention-runs)). This is a deployment prerequisite, not something the mount checks, because a deployment can also advance the floor in other ways.

The name `.loonfs` at the mount's root is reserved for the mount's control directory (see [The FUSE adapter](#the-fuse-adapter)). The mount refuses to start on a directory that has a child with that name, and if another client creates one later, the mount doesn't show it and reports it in status.

In an ACL namespace (see [Authorization in ACL namespaces](../specs/api.md#authorization-in-acl-namespaces)), the mount acts as the actor and subject that the CLI's profile or flags set, and the server authorizes each request. A mount of an ACL namespace requires a subject that's an administrator of the namespace, because reading the change feed with a subject and creating a [snapshot](../specs/glossary.md#pin) both require an administrator. Admission checks this when it creates its snapshot as the subject, and it refuses the mount with that reason. If the server rejects a commit with `forbidden`, the change becomes a "refused publication", which the mount doesn't publish. The mount keeps the change's bytes in its control directory and reports the refusal, as [Resolving conflicts](#resolving-conflicts) describes.

## Crates and processes

This section describes how the mount's code is divided into crates, which crates each one depends on, and how the daemon runs.

The mount is a client of the public API, so it has to work with nothing but what the API offers. If the mount's code could call into the server's crates, the mount could depend on server behavior that other clients can't reach, and the server would gain a behavior that only one client uses. So the crates are divided this way:

| Crate | Contains | Depends on |
| --- | --- | --- |
| `loonfs-mount` | The engine (the trees, the cache, [captures](#accepting-writes), the planner, publication, and feed application), the daemon, and the daemon's IPC | `loonfs-client` and `loonfs-types` |
| `loonfs-fuse` | The FUSE adapter, built only on Linux | `loonfs-mount` and `fuser` 0.18.0, pinned to that release |
| `loonfs-cli` | The `mount`, `unmount`, `sync`, `refresh`, `status`, and `run` commands, and the hidden `daemon` command | `loonfs-mount` on every platform, and `loonfs-fuse` when it's built for Linux |

The CLI builds on every platform. On a platform other than Linux, the mount commands exist and fail at once with the reason that mounts are available on Linux only, so a script gets one consistent error instead of a missing command.

`loonfs-mount` never depends on `loonfs-core` or the embedded runtime crate (`loonfs`), and nothing in those crates refers to a mount. A test in `loonfs-mount` reads the workspace's dependency graph and fails the build if the engine crate depends on either one, directly or through another crate. The simulation in [Testing](#testing) uses `loonfs-model` and `loonfs-sim` as test dependencies, and the dependency test ignores test dependencies.

The engine contains no kernel types. Its concepts are an item's identity, a revision's identity, content read by range, local changes, barriers, and a stream of changes for the kernel to invalidate. Descriptors, FUSE lookup counts, open flags, and kernel invalidation calls belong to the adapter. The engine's interface has this shape:

```rust
pub struct Engine { /* one per mount */ }

impl Engine {
    // The Local tree, read from memory.
    pub fn lookup(&self, parent: ItemId, name: &OsStr) -> Option<Entry>;
    pub fn getattr(&self, item: ItemId) -> Option<Attr>;
    pub fn readdir(&self, item: ItemId, offset: u64) -> impl Iterator<Item = DirEntry>;
    pub fn readlink(&self, item: ItemId) -> Option<OsString>;

    // Content. A ReadHandle reads a revision (or the working file),
    // and a WriteHandle also writes the working file.
    pub async fn open_read(&self, item: ItemId) -> Result<ReadHandle>;
    pub async fn open_write(&self, request: OpenWrite) -> Result<WriteHandle>;
    pub async fn barrier(&self, items: &[ItemId]) -> Result<ChangeSeq>;

    // Changes to the Local tree.
    pub async fn mkdir(&self, request: Mkdir) -> Result<Entry>;
    pub async fn symlink(&self, request: Symlink) -> Result<Entry>;
    pub async fn rename(&self, request: Rename) -> Result<()>;
    pub async fn unlink(&self, parent: ItemId, name: &OsStr) -> Result<()>;
    pub async fn rmdir(&self, parent: ItemId, name: &OsStr) -> Result<()>;
    pub async fn setattr(&self, request: SetAttr) -> Result<Attr>;
    pub fn forget(&self, item: ItemId, lookups: u64);

    // Control.
    pub async fn sync(&self) -> Result<ChangeSeq>;
    pub async fn refresh_at_least(&self, seq: ChangeSeq) -> Result<()>;
    pub fn subscribe_invalidations(&self) -> Receiver<Invalidation>;
    pub fn record_mount_method(&self, method: MountMethod);
    pub fn status(&self) -> Status;
}
```

An `ItemId` is a local number that the engine assigns to each item, and it's also the inode number that the kernel shows. The engine maps each item to a LoonFS inode ID once the item is published. The planner can change which LoonFS inode an item publishes to (see [Planning a commit](#planning-a-commit)), but it never changes an item's `ItemId`.

The receiver that `subscribe_invalidations` returns is bounded (4,096 items). If the adapter falls behind, the engine drops the invalidations that don't fit, and status reports how many it dropped. The adapter calls `record_mount_method` after it mounts, because whether the mount went through `mount(2)` or `fusermount3` is visible only inside the adapter, which calls `fuser`. `readdir` doesn't count lookups, so an adapter that serves readdirplus calls `lookup` for each entry it returns.

Each mount has one control task. Only the control task changes the trees, the dirty set, the generations, and the pending batch. Network requests, uploads, captures, and feed polls run in worker tasks that send their results to the control task as messages, and the adapter sends every change through the control task's channel. Metadata reads (`lookup`, `getattr`, `readdir`, and `readlink`) read the in-memory tree under a read lock, so they don't wait for the channel. With the workers replaced by fakes, the control task is a deterministic state machine over its messages, which is what the simulation relies on.

The daemon is one process for each host user, and it serves all of that user's mounts. The CLI talks to it over a Unix socket at `$XDG_RUNTIME_DIR/loonfs/daemon.sock` (or in the state directory when that variable isn't set), with one JSON message per line. The socket accepts the operations `mount`, `unmount`, `sync`, `refresh`, `status`, `list`, and `shutdown`. `loonfs mount` starts the daemon (as the hidden `loonfs daemon` command) when no daemon is running, and the daemon exits 60 seconds after its last mount is removed. The daemon takes a lock on its state directory, so a second daemon for the same directory refuses to start. The daemon starts and stops kernel mounts through an interface that the adapter implements, and the hidden command connects the two. The simulation implements the same interface with a fake kernel.

The daemon keeps credentials in memory. Each `loonfs mount` sends the resolved profile (the server's address, the token, and the actor and subject settings) over the socket, so a daemon that restarts resumes a mount the next time `loonfs mount` runs for it.

Why is there a daemon, and not a process per mount? One process can share the HTTP connection pool, the upload and download limits, and the disk budget across a host's mounts, and `loonfs sync` and `loonfs status` need a process that keeps running after `loonfs mount` returns.

## Commands

This section describes the commands, their options, and their exit statuses.

The commands take these arguments:

```text
loonfs mount <namespace>[/<path>] <dir> [--ro] [options]
loonfs unmount <dir> [--force] [--sync-timeout-ms <ms>]
loonfs sync <dir>
loonfs refresh <dir> [--at-least <seq>]
loonfs status [<dir>] [--json]
loonfs run --mount <namespace>[/<path>]=<dir>[:ro]... [options] -- <command> [<arg>...]
```

`loonfs mount` admits the mount (see [Admitting a mount](#admitting-a-mount)) and returns once the directory is ready. It's idempotent. If the directory already has a mount of the same namespace and path, the command returns success. If the mount's state exists from a daemon that stopped, the command resumes that state:

1. It detaches a stale kernel mount at the directory, if there is one.
2. It loads the trees.
3. It resumes the [pending batch](#local-state) and the [captures](#accepting-writes).
4. It follows the change feed from the mount's [applied sequence](#admitting-a-mount).
5. It mounts the directory again.

A mount of a different namespace or path at a directory whose state has unpublished changes is refused.

`loonfs unmount` runs a barrier over the whole mount, waiting up to the sync timeout, and then unmounts the directory and removes the mount's state, except the copies in the control directory, whose location it prints. If the barrier fails, the command reports why and leaves the mount in place. `--force` unmounts without the barrier and leaves the state on disk for a later `loonfs mount`.

`loonfs sync` runs a barrier over the whole mount and prints the sequence of the last commit it waited for. `loonfs refresh` reads the change feed now, and with `--at-least`, waits until the mount has applied that sequence, which a script uses to see a commit whose sequence another host printed. `loonfs status` reports each mount's state (see [Failure modes](#failure-modes)).

`loonfs mount` and `loonfs run` accept these options:

| Option | Default | Meaning |
| --- | --- | --- |
| `--ro` (`:ro` in a `run` spec) | Off | Mount read-only. |
| `--max-entries <n>` | 100,000 | The entry budget that admission checks. |
| `--publish-interval-ms <ms>` | 1,000 | The "publish interval", how often the planner publishes changes. |
| `--append-interval-ms <ms>` | 1,000 | How often the planner publishes files whose only change is an append. |
| `--poll-interval-ms <ms>` | 500 | How often the mount reads the change feed. |
| `--conflict <policy>` | `keep_both` | The conflict policy: `keep_both`, `overwrite`, or `refuse`. |
| `--cache-max-bytes <n>` | 8 GiB | The eviction target for cached content that nothing uses. |
| `--cache-min-free <n>` | 2 GiB | The free space that the mount keeps on the state directory's filesystem. |
| `--pinned-max-bytes <n>` | 16 GiB | The hard limit on local bytes that the mount can't evict. |
| `--state-dir <path>` | `$XDG_STATE_HOME/loonfs`, or `~/.local/state/loonfs` | Where the daemon keeps each mount's state, working files, captures, and cache. |
| `--uid <uid>`, `--gid <gid>` | The user running the command | The owner that every item shows. |
| `--sync-timeout-ms <ms>` | 60,000 | How long a barrier waits for publication before it fails. |

The mount also takes the CLI's selectors and identity flags (`--profile`, `--actor-id`, `--subject-id`, `--principal-scope`, and `--principals`). The mount records the actor and subject in its state, because a retried commit has to send the same identity headers as the first attempt (see [Publishing a batch](#publishing-a-batch)).

`loonfs run` mounts every `--mount` in parallel and waits until all of them are ready. It then starts the command with the same environment and standard streams, and forwards `SIGINT`, `SIGTERM`, and `SIGHUP` to it. When the command exits, `run` runs a barrier over every mount, unmounts them, and exits with the command's status. `run` has its own statuses for its own failures:

| Status | Meaning |
| --- | --- |
| 3 | A mount couldn't start, so the command never ran. `run` unmounts the mounts that did start. |
| 4 | The command exited, but a barrier failed. The mount's state stays on disk, so a later `loonfs mount` can publish it. |
| 5 | The daemon died while the command ran. `run` sends the command `SIGTERM`, waits 10 seconds, sends `SIGKILL`, keeps the state, and exits. |

`loonfs mount`, `unmount`, `sync`, and `refresh` follow the CLI's usual statuses, 1 for a command that ran and failed and 2 for a command line that the CLI rejected.

Why does `run` fail the command when the daemon dies, instead of starting a new daemon? A new daemon can't serve the descriptors that the command already has open, because the kernel fails every request on a FUSE mount whose daemon exited with `ENOTCONN`. The command's results can't be trusted after that, so `run` stops it, and the caller chooses whether to run it again. The state on disk keeps every accepted write.

## Admitting a mount

This section describes the checks a mount passes before the kernel mount, and how the mount reads its first copy of the tree.

A mount that would fail later, because the host can't serve it or the namespace is larger than the mount can keep, should fail before any program sees the directory. So the mount runs every check, and reads the whole tree, before it mounts the directory. To admit a mount, the daemon:

1. Checks the host. `/dev/fuse` has to open, and the daemon has to be able to mount (as root, or as a user with `fusermount3` installed). The state directory has to be on one filesystem that supports sparse files, with at least the free space that `--cache-min-free` sets. The daemon records whether that filesystem supports reflinks (`FICLONE`), and status reports it.
2. Reads the capability document and the namespace's metadata (`GET /v0/namespaces/{ns}`). A writable mount requires `case_sensitive` naming. The mount records the inline limit (`commit.max_inline_content_bytes_per_operation`, when `filesystem.commits.inline_content` is advertised) and the transports that the deployment advertises.
3. Creates a snapshot of the namespace with a `ttl_ms` of 600,000 (ten minutes). The daemon extends the snapshot by another ten minutes whenever less than half of its lifetime is left while the scan runs. The lifetime doesn't need to match the scan's length (a scan of the default budget takes seconds), because it only bounds how long the snapshot outlives a daemon that dies during the scan.
4. Reads the entry at the mount's path at the snapshot, which has to be a directory, and records its inode ID as the mount's root. It then reads each ancestor by inode at the same snapshot, from the root's `parent_inode_id` up to the namespace's root, and records their inode IDs (see [Following the change feed](#following-the-change-feed)). Because these reads and the listing all use the snapshot, a move of the root after the snapshot is in the feed after the snapshot's captured sequence, so the mount sees the move.
5. Lists the root's subtree at the same snapshot, with the recursive inode listing (see [`GET /filesystem/entries` and `GET /inodes/{inode_id}/children`](../specs/api.md#65-get-filesystementries-and-get-inodesinode_idchildren)), and writes each entry into the trees. The listing's query parameters are `recursive=true`, `snapshot_id`, `include_attributes=true`, and `limit=1000`. If the listing reaches one entry more than the entry budget, the daemon stops, deletes the snapshot, and refuses the mount with both numbers. A directory that the listing returns without its entries, because it's 128 levels below the root, is listed by inode at the same snapshot, and its entries count toward the budget.
6. Deletes the snapshot, and records the snapshot's captured sequence as the mount's "applied sequence", the sequence of the last commit whose changes are in the mount's trees.
7. Starts following the change feed after the applied sequence.
8. Mounts the directory and reports that the mount is ready.

If any step fails, the daemon reports the reason and the numbers involved (for example, the namespace's entry count and the budget, or the free space and `--cache-min-free`), and nothing is mounted. Progress through the scan is reported over the socket.

A read-only mount runs the same steps, except the naming check. It has no working files, captures, planner, or dirty set, and its Local tree is a view of its Committed tree. A host can have any number of read-only mounts.

Why does the scan use a snapshot? A listing of more than one page can span several heads, so an entry that's renamed while the listing runs can be returned twice or missed (see [`GET /filesystem/entries`](../specs/api.md#65-get-filesystementries-and-get-inodesinode_idchildren)). A listing at a snapshot is one consistent cut, and following the feed from the snapshot's captured sequence brings the tree up to date. The mount deletes the snapshot when the scan ends, because a snapshot kept for the mount's lifetime would keep every manifest and segment it pins from garbage collection for as long as the mount runs.

Why does admission list the tree instead of asking the server for a count? The scan reads every entry anyway, and a listing that stops one entry past the budget costs at most one page more than the budget allows. A count that the server keeps would be a new mechanism in the server for one client.

## Local state

This section describes the state the engine keeps for each mount: two trees, the server-observed columns, the dirty set, generations, and the pending batch.

The engine keeps state, not a list of operations to replay. Every commit it sends is derived from the difference between two trees, so 10,000 writes to one file in one second become one content operation, and a file that's created and deleted between two publications never reaches the server. The state is stored in one transactional embedded database file per mount (`tree.db`), and the engine keeps an in-memory copy of the tree for metadata reads.

The two trees are:

- The "Committed tree": for each item, the published state that the item's local changes are relative to (its "base"). The engine doesn't change an item's base while the item has unpublished changes.
- The "Local tree": the tree that the kernel shows to programs now.

Next to the Committed tree, each item has "server-observed" columns, which record the server's current state of the item as the change feed or a rebuild last recorded it. For an item without local changes, the server-observed columns equal the Committed columns. For an item with local changes, the feed writes only the server-observed columns, so they can move ahead of the base, and the difference is a conflict that the planner finds before it sends anything.

An item's state is divided into three field groups, and each group is clean or dirty on its own:

- The "binding": the parent directory, the name, and the [binding version](../specs/glossary.md#binding-version).
- The "content": the revision number, the content reference, the size, and the working file.
- The "attributes": the mode, the modification time, the symlink target, and the attribute revision.

For example, if a program renames a file on this host while another host appends to it, the binding is dirty on this host and the content is clean, so the engine applies the append to the Committed and Local columns and publishes the rename.

Each item's record has these fields:

| Fields | Meaning |
| --- | --- |
| `item_id` | The item's `ItemId`, which is also its kernel inode number. |
| `remote_inode_id` | The LoonFS inode ID that the item publishes to, or none before its first publication. |
| `kind` | File, directory, or symlink. |
| `c_parent`, `c_name`, `c_binding_version` | The Committed binding. The binding version is stored as text, because it's an opaque token. |
| `c_revision_no`, `c_content_ref`, `c_size` | The Committed content. |
| `c_revision_committed_at_ms`, `c_fs_mtime_ms`, `c_fs_mtime_revision_no` | The current revision's commit time (`revision_committed_at_ms`) and the raw values of the `fs.mtime_ms` and `fs.mtime_revision_no` attributes, which the content group records so that the engine can compute the shown modification time again when the content changes. |
| `c_mode`, `c_mtime_ms`, `c_target`, `c_attributes_revision_no` | The Committed attributes. |
| `s_*` | The server-observed copy of every Committed field. |
| `s_hidden_by` | The inode ID and deletion sequence of a remote deletion that hides the item. |
| `s_append_only` | Whether every remote revision since the base was an append that started at the previous size. |
| `l_parent`, `l_name`, `l_size`, `l_mode`, `l_mtime_ms`, `l_target` | The Local tree. An item whose `l_parent` is empty was unlinked locally, and its record stays while the kernel has descriptors or lookups for it. |
| `l_mtime_explicit` | Whether a program set the modification time explicitly. |
| `materialized` | Whether the item has a working file. |
| `generation`, `published_generation` | The item's local generation, and the generation of its base. |
| `lowest_write_offset`, `truncated_below_base` | What [Accepting writes](#accepting-writes) uses to tell whether a change is an append. |
| `fingerprint` | The fingerprint of the base's bytes, when the engine has computed it. |

The database also records the mount's settings, its applied sequence, the paths and inode IDs of the root and its ancestors, the [captures](#accepting-writes), the pending batch, and the sequences of the engine's own commits that the feed hasn't reached yet. Its `entering_directories` table lists the directories that entered the mount and stay hidden until the feed reaches their snapshot (see [Following the change feed](#following-the-change-feed)), and its `cache_files` table lists each complete cache file and the size it covers. The mount's row has an `admitted` flag, which admission sets once it has written the scan and the applied sequence, so the next start discards a mount whose admission was interrupted.

The kernel's lookup counts are kept in memory only, because they don't survive a daemon restart, and storing them would add a database write to every lookup, the kernel's most frequent request, to keep a count that the next start discards.

The "dirty set" is the set of items with at least one dirty field group. The binding is dirty when the Local binding differs from the Committed one, the content is dirty when the generation is ahead of the published generation, and the attributes are dirty when a Local attribute differs from the Committed one. A modification time that differs from the Committed one by less than the publish interval, and that no program set explicitly, doesn't make the attributes dirty. The database keeps the dirty set in a `dirty_groups` table with a partial index, and the engine keeps a copy of it in memory, so a plan costs time in proportion to the number of dirty items, and nothing when there are none.

A "generation" is a number on each item that the engine increments on every write and truncate. A plan publishes the generation that it captured, and an acknowledgment moves the item's base to that generation and no further. If a program wrote again while the commit was in flight, the item stays dirty, relative to its new base.

The "pending batch" is the one commit the mount has in flight, stored exactly as it was sent (see [Publishing a batch](#publishing-a-batch)). The database has at most one.

These invariants are true whenever the control task isn't in the middle of a message:

- An item is clean when its Committed, Local, and server-observed columns agree and its generation equals its published generation. The mount is fully published when the dirty set is empty and there's no pending batch.
- An item's base changes only through an acknowledgment, a conflict resolution, or a feed event for a clean field group.
- The feed and a rebuild write the server-observed columns of a dirty field group, and never its Committed or Local columns.

When the daemon restarts, the engine loads the database, treats every item with a working file as possibly changed (a write might have reached the working file after the last generation was recorded), resumes the pending batch if there is one, and only then plans anything new.

Why two trees and not a list of the calls that programs made? A list grows with every call, and replaying it publishes every intermediate state, including files that were created and deleted again. A difference between two trees costs time in proportion to what's dirty and publishes only the net change.

Why keep the server-observed columns separate from the base? If the feed wrote a foreign change into the base of a dirty item, the next plan would send that item's local change with a precondition on the foreign state, and the server would accept it, so the local change would overwrite the other host's change without anyone seeing a conflict.

## Content states

This section describes where a file's bytes are on the host, and how the cache keeps them.

A write open "materializes" a file, which means that it creates the file's working file. Each file is in one of four content states:

| State | Bytes are in | Leaves the state when |
| --- | --- | --- |
| Absent | The server only | A read fetches part of it (to Partial), or a write open materializes it (to Materialized). |
| Partial | A sparse cache file, with the present ranges recorded in memory | The present ranges cover the file (to Complete), a write open materializes it, or eviction removes it (to Absent). |
| Complete | A cache file that covers the whole revision | A write open materializes it, or eviction removes it (to Absent). |
| Materialized | The item's working file, plus any [captures](#accepting-writes) | The last descriptor closes and the item is clean (to Complete, or Absent if the cache evicted the file). |

When a materialized item leaves its state, the engine deletes the working file. The acknowledgment of the item's last commit already renamed its last capture into the cache, so the item is Complete, unless the cache has evicted that file since, in which case it's Absent.

The cache is in `<state dir>/mounts/<mount ID>/cache/`, and it's keyed by content ID, because an append that extends a [chain](../specs/glossary.md#chain) keeps the chain's content ID. A cache file is either `<content ID>`, complete up to a size recorded next to it, or `<content ID>.partial`. The present ranges of a partial file are kept in memory only, so a restart deletes partial files. A partial file becomes complete with a rename once its ranges cover the size and its checksum matches. An append extends a complete file in place and records the new size (see [Reading files](#reading-files)).

A working file is at `staging/<item ID>`. It's the one mutable copy of a file that's being written, and every descriptor opened while the item is materialized reads and writes it, so a program reads its own writes. Captures are at `captures/<item ID>/<generation>`.

The working files, captures, and cache are on one filesystem, so moving a capture into the cache is a rename, and copying a cache file into a working file can be a reflink.

The engine evicts cache files that no descriptor uses, complete or partial, least recently used first, to stay under `--cache-max-bytes` and above `--cache-min-free`. A partial file is evicted as a whole, because its present ranges aren't stored on disk and a restart drops it anyway. Working files, captures, the copies in the control directory, and cache files with open descriptors (including partial files) can't be evicted, and they count against `--pinned-max-bytes` (see [Resource bounds](#resource-bounds)).

Why can a partial file be evicted? A program that reads part of a file and closes it leaves a partial file behind. If partial files were pinned, each abandoned partial read of a current file would stay pinned until the daemon restarts, and enough of them would make reads fail with `ENOSPC`.

## Reading files

This section describes how the engine fetches content, and what a program reads through an open descriptor when another host changes the file.

A file that the engine read once in full shouldn't be fetched again, and a program that reads 4 KiB of a large file shouldn't cause the whole file to be fetched. So each read handle has its own read window, and the engine fetches by range:

- A file of at most 8 MiB is fetched whole on its first read, with a proxied read of the whole file, which the server verifies.
- A larger file starts with a window of 1 MiB, aligned to 1 MiB. On sequential reads, each new window is the smallest of twice the previous window, 64 MiB, and the bytes that the handle has consumed less the bytes fetched for it that the reader hasn't read yet, and the engine requests the next window when the reader has consumed half of the current one. So the bytes fetched ahead of a reader never exceed what it has consumed, and the total that a handle fetches stays under twice the bytes it consumed plus its first 1 MiB window. Windows grow by about 1.6 times each instead of doubling, so a 40 MiB sequential read makes seven ranged reads before its first grant. A read at another offset resets the window to 1 MiB.
- A fetch of at most 8 MiB is a [ranged read](../specs/glossary.md#ranged-read) through the server, by inode and revision (`GET /inodes/{inode_id}/revisions/{revision_no}/content` with a `Range` header). The server serves bytes that are still in the WAL tail from memory, so a ranged read of a small tail writes nothing to object storage.
- A larger fetch, which only a grown sequential window makes, uses a download grant from the fetch's offset (see [Download transport](../specs/api.md#610-download-transport)). The handle reads the grant's stream, through the client's download stream, which keeps four range responses in flight, and stops reading when its window is full. A read outside the streamed region drops the stream.

A deployment that doesn't advertise `filesystem.downloads.direct_get` has no grants, so the engine makes every fetch there as ranged reads of at most 8 MiB.

All fetches of one mount share a limit of 256 MiB and 32 requests in flight. A fetch for a read that's waiting comes before read-ahead. The engine doesn't fetch whole files in the background, and it doesn't prefetch.

A ranged read and a grant aren't verified by the server, so the engine verifies a cache file when its ranges first cover the revision. It computes the checksum of the whole file in the content reference's algorithm and compares it with the reference's checksum. A file that doesn't match is deleted and fetched again once, and a second mismatch fails the reads that wait for it with `EIO`.

A descriptor follows the file's working file while the file has one, and it's pinned to the revision it opened otherwise. When another host changes a file that has no working file on this host, the change event records which kind of change it is:

- An append (a `content_changed` event with `append_offset`) extends the revision. If the cache has the previous revision and its size equals the event's `append_offset`, the cache file becomes the new revision's file. If a descriptor has the file open, the engine fetches the appended range at once and appends it to the cache file. Otherwise the cache file becomes a partial file, and a read of the appended range fetches that range. Open descriptors follow the file as it grows, and the adapter sends the kernel an attribute invalidation with the new size. A read at the end of the file returns the new bytes once they're fetched, which is how `tail -f` follows a log that another host appends to. The engine verifies the extension by combining the checksum of the cached bytes with the checksum of the appended bytes, as the server does, so it doesn't read the earlier bytes again.
- Any other new revision replaces the file. The name then refers to a new item with a new kernel inode number, whether or not the file is open, and the next open reads the new revision. Open descriptors stay on the old item and the revision they opened. If an open descriptor's revision isn't completely cached, the engine requests one grant for the rest of that revision and fetches it at once, within the grant's 15-minute lifetime. The old revision's cache file is deleted after the last descriptor closes.

A deletion of an open file works like a replacement. Open descriptors keep their revision, and the engine fetches the rest of that revision at once.

A file with a working file follows a different rule, because its descriptors share the working file. If the file has no local content change, the engine adopts the new revision (see [Resolving conflicts](#resolving-conflicts)), so every descriptor reads the new bytes and later writes apply on top of them. If the file has a local content change, the new revision is a conflict.

The engine uses `append_offset`, never content ID continuity, to tell whether a change is an append, because an append that starts a new chain (for example, in a fork, or after a restore) has a new content ID (see [`GET /changes`](../specs/api.md#611-get-changes)).

Why does a replacement change the name's kernel inode number? The kernel keeps one page cache for each inode number, and descriptors on one inode share it. If a descriptor pinned to the old revision and a descriptor on the new revision shared an inode number, each could read pages that the other one had cached. With a new inode number, the old descriptors keep the old pages, and a program that follows a log by name (`tail -F`) sees the replacement as a new file, as it would see a rotated log.

Why is a new window limited by the bytes that the handle has consumed? A reader that stops after a few windows shouldn't have paid for the next doubling. With windows that only double, a handle could have fetched four times what it had consumed when a new window begins.

Why does a pinned descriptor fetch the rest of its revision at once? The server keeps a replaced revision's rows and bytes for at least an hour, because the retention floor never passes a commit that's younger than the default garbage collection grace (see [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor)), and a grant's URLs work for 15 minutes. Fetching the rest at once ends the read within both limits, so a program that keeps a file open for a day can still read every byte of the revision it opened.

## Accepting writes

This section describes how the engine accepts writes into working files, and how it takes the captures that a commit publishes.

Every read and write goes through the daemon, so every byte that a program writes passes through the engine. A write open works this way:

- A new file gets a new item and an empty working file.
- An existing file opened with `O_TRUNC` gets an empty working file, and its generation is incremented.
- Any other write open of an existing file first brings the file to Complete, which waits for the fetch (this is the one write open that waits for the network), and then reflinks or copies the cache file into a new working file.
- A file that's already materialized uses its working file.

The engine writes each write request to the working file with `pwrite` and increments the item's generation. A truncate increments the generation too.

A file's content change is an "append" when every byte written since the base is at or beyond the base's size, and no truncate since the base cut the file below that size. For example, a log that's written with `O_APPEND` is an append, and so is a file that a program extends with `pwrite` at its end. A file whose middle was overwritten isn't, even if the program then appended to it. The engine keeps the lowest offset written since the base, and whether a truncate went below the base's size, separately for the writes before and after the latest capture, so an acknowledgment can move the base to the capture and keep what the later writes did.

A "capture" is an immutable copy of a working file at one generation. A commit publishes captures, never a working file, because a program can write the working file while its upload or commit is in flight. To take a capture of a file whose change is an append, the engine copies the bytes from the base's size to the end of the working file. To take a capture of any other change, it reflinks the working file (`FICLONE`), or copies it with `copy_file_range` when the filesystem has no reflinks. The engine then calls `fsync` on the capture and records it in the database in one transaction. Nothing writes a capture again.

Before a capture is published, the engine compares its fingerprint (BLAKE3 over 4 MiB chunks) with the base's fingerprint, when the engine has computed it. If they're equal, the change has no content (for example, an editor saved the file without changing it), and the planner publishes no revision for it.

An item that's dirty in 20 plans in a row and isn't an append is published once a minute instead of once every publish interval, because each publication adds a revision to the file's history. The engine logs a warning once. Files that match `*.db`, `*-wal`, `*-shm`, or `*-journal` start at once a minute. This is a limit on how often a busy file is published, not a promise that the published revisions of several files are consistent with each other (see [Databases and generated files](#databases-and-generated-files)).

Why does a write open of an existing file fetch the whole file? The working file has to contain the whole file, so that a program can read and write it like a local file, and the capture of a change that isn't an append has to contain the whole file too. A program that appends a few bytes to a large file on another host's mount therefore costs a download of the whole file the first time it opens the file for writing.

Why does an append need every byte since the base to be at or beyond the base's size? An append operation adds bytes after the server's current revision. If a program had overwritten bytes in the middle of the file and then appended, an append of the tail alone would publish a file whose middle is still the old bytes.

## Planning a commit

This section describes how the planner turns the dirty set into a commit.

The planner runs on the control task every publish interval for every dirty item, every append interval for items whose only change is an append, immediately when a barrier waits, and once when the daemon starts. It never runs while a batch is pending. It resumes the pending batch first instead.

For each dirty field group whose server-observed columns differ from its Committed columns, the planner sends nothing and hands the item to [Resolving conflicts](#resolving-conflicts) first. Every other dirty group becomes operations:

| Local change | Operation |
| --- | --- |
| A new directory | `create_directory`, by path |
| A new file at or below the inline limit | `put_file`, by path, with `behavior: no_replace` and `inline_content` |
| A new file above the inline limit | `put_file`, by path, with `behavior: no_replace`, the capture's `content_ref`, and its content token |
| A new symlink | `put_file`, by path, with empty `inline_content` |
| An append | `append_file_by_inode` with `expected_revision_no`, one operation for each 256 KiB of the tail |
| Any other content change, at or below the inline limit | `put_file_revision_by_inode` with `expected_revision_no` and `inline_content` |
| Any other content change, above the inline limit | `put_file_revision_by_inode` with `expected_revision_no`, the capture's `content_ref`, and its content token |
| A move or rename | `move_by_inode` with `expected_binding_version` |
| An unlink of a published file | `delete_by_inode` with `expected_binding_version`, not recursive, with request-level `file_revision` and `attributes_revision` preconditions on the item's base |
| An rmdir of a published directory | `delete_by_inode` with `expected_binding_version`, not recursive |
| A rename onto a published file by an item that has a LoonFS inode | `copy_by_inode` with `behavior: replace`, `expected_destination_inode_id`, and `expected_destination_revision_no`, then `delete_by_inode` of the renamed item's inode |
| A rename onto a published file by an item that was never published | `put_file_revision_by_inode` on the replaced file's inode, with the capture inline or uploaded |
| A change of mode or symlink target, a time that a program set, or a modification time more than one publish interval old | `update_attributes_by_inode` with `expected_attributes_revision_no`, or `update_attributes` by path for a new item |

A delete by inode checks only the binding version, which a content change doesn't move, so the `file_revision` and `attributes_revision` preconditions make a file's delete fail if another host changed the file's content or attributes after the engine's last poll. These preconditions count against the limit of 1,024 preconditions per request, at which the planner also splits a plan. A directory needs neither, because a non-recursive delete of a directory that gained an entry fails with `directory_not_empty`.

The mount stores a file's metadata in attributes with the `fs.` prefix, because a LoonFS inode is a file or a directory and has no mode:

| Key | Value |
| --- | --- |
| `fs.mode` | The permission bits in octal, such as `755`. Without the key, a file has mode 644 and a directory 755. The planner writes the key when the mode differs from that default. |
| `fs.mtime_ms` | A file's modification time, in milliseconds since the Unix epoch. |
| `fs.mtime_revision_no` | The content revision that `fs.mtime_ms` describes, written together with it. For a content change that the same request publishes, it's the new revision, which is the base revision plus one. |
| `fs.type` | `symlink` for a symlink, which is a small empty file. |
| `fs.target` | A symlink's target. |

The planner writes `fs.mtime_ms` and `fs.mtime_revision_no` only when a program set the file's times explicitly (a `setattr` that changes the time), or when the file's local modification time is more than one publish interval before the time the planner runs, which happens when publication was delayed. For any other content change, the planner writes no attribute, and at the acknowledgment the Committed modification time takes the Local one, because the two differ by less than the publish interval. To show a file's modification time, the mount uses `fs.mtime_ms` only when `fs.mtime_revision_no` equals the file's current revision number. Otherwise it uses the current revision's commit time, so a file that another client wrote without the attribute shows its commit time instead of a stale one. A directory's modification time is the local time of the last change the mount saw in it, and the mount doesn't publish it. Other attribute keys are kept unchanged and aren't shown.

The planner addresses an item that already has a LoonFS inode ID by that ID. It uses the path form for an operation that has to refer to something that the server hasn't assigned an identity to yet: a new item, an item under a new directory, and an item that an earlier operation in the same request moved, whose new binding version doesn't exist yet when the request is built. The planner computes each path in the state that the earlier operations in the request leave, starting from the path of the nearest directory that existed before the request, and the request includes a `path_binding` precondition that this directory's path still resolves to its inode ID. So if a rename on another host made the path stale, the request fails, instead of creating a file in the wrong place (a `put_file` creates missing parent directories, so a stale path would otherwise succeed). The engine keeps the root's path, updates it when the feed moves the root or one of its ancestors, and derives the paths below the root from the Committed tree.

The planner orders the operations so that each one is valid in the state that the earlier operations in the request leave:

- An operation that frees a name (a move away or a delete) comes before an operation that binds that name.
- A directory comes before anything created in it or moved into it.
- A non-recursive directory delete comes after the deletes and moves of everything in it, so the planner deletes a removed tree's files before its directories.
- A cycle of moves (for example, two files that swap names) starts with a move of one item to a temporary name in its parent (`.loonfs-move-<item ID>`), and the last move of the cycle is a `move_path` from that temporary name.
- Otherwise, creations come first, then content changes, moves, deletes, and attribute updates, in item order.

A rename onto an existing file keeps the target's LoonFS history. Locally, the rename is exact. The renamed item takes the name, and the replaced item is unlinked but stays readable and writable through the descriptors that are open on it. When the planner finds a name whose file in the Committed tree is unlinked locally and is now taken by another item, it publishes the renamed item's bytes as the replaced file's next revision, in one request:

- If the renamed item has a LoonFS inode, the request is a replacing copy of that inode onto the replaced file, with the replaced file's parent inode and name as the destination and its Committed revision as the expected destination revision, followed by a `delete_by_inode` of the renamed item's own inode. A `file_revision` precondition checks that the renamed item's inode is still at its base revision, so the copy publishes the bytes this host published. A replacing copy appends a revision to the destination inode, which keeps its identity and its history (see [`POST /commits`](../specs/api.md#68-post-commits)). If the renamed item has local changes after its base revision, they publish to the replaced file's inode in the next plan.
- If the renamed item was never published (it was created and renamed within one publish interval), the request publishes its capture as the replaced file's next revision, with the replaced file's Committed revision as `expected_revision_no`, inline or through a new upload by size.

A copy over an existing file changes only its content, so a mode or symlink target that differs from the replaced file's is an attribute update in the same request. The acknowledgment maps the renamed item to the replaced file's inode and binding. The replaced item no longer has a LoonFS inode, so writes through its open descriptors aren't published. For example, an editor that writes `report.txt.tmp`, calls `fsync` on it, and renames it over `report.txt` publishes `report.txt.tmp` as a file of its own at the `fsync`. At the rename, one commit copies the revision of `report.txt.tmp` onto `report.txt` as that file's next revision and deletes `report.txt.tmp`. A rename onto an empty directory deletes the directory and then moves the item.

A capture at or below the deployment's inline limit (64 KiB by default, advertised as `commit.max_inline_content_bytes_per_operation`) is sent as inline content. A larger capture is uploaded before the planner includes it (see [Publishing a batch](#publishing-a-batch)). A deployment that doesn't advertise `filesystem.commits.inline_content` has an inline limit of zero, so every new file and replacement there is uploaded. Appends are inline on every deployment, because an append doesn't depend on that feature. Each request has an inline budget of 1 MiB, which the planner fills in the order of the dirty set, and a capture that doesn't fit the budget is uploaded instead. An append whose tail doesn't fit the budget is published by upload as a replacing revision, because a file's tail can't be split between an append and an upload.

Several appends to one file can follow each other in one request. Each one expects the revision that the previous append in the request creates (one more than its own expected revision), because the server plans a request's operations in order. A tail of 600 KiB is therefore three appends, of 256 KiB, 256 KiB, and 88 KiB.

A plan that exceeds a request's limits (4,096 operations, 1,024 preconditions, or a 2 MiB body, which fits about 1.5 MiB of raw inline bytes after base64) is split at operation boundaries that keep each group that has to be atomic in one request: a replacing copy or put and its delete, a cycle of moves, and a creation and its attributes. The planner sends the first request, and it plans again as soon as that request is acknowledged.

An inode-addressed operation can't commit twice, even after its [commit receipt](../specs/glossary.md#commit-receipt) is gone, because its own success invalidates its precondition (an expected revision, binding version, or attribute revision), revision numbers only grow, and a binding version never recurs. A creation by path can commit twice, because its name-absence check passes again once another host deletes the name. So the engine never resends a request whose outcome is unknown before it has read the change feed to the head, and an echo of the batch's commit ID in the feed is the acknowledgment.

Why isn't `fs.mtime_ms` written with every content change? A write sets the local modification time to the current time, and the commit that publishes it is made within the publish interval, so the commit time is the modification time to within a second. An attribute on every change would double the operations of every save and every append, and add an attribute revision for each one. The attribute is needed only when the time differs from the commit time by more than that, because a program set it or publication was delayed.

Why are new items created by path? A new item's inode ID doesn't exist until its commit is acknowledged, so an inode-addressed request can't create a directory and a file in it, or set a new file's mode, in one commit. Paths let one request publish a new tree with its attributes, and the `path_binding` precondition makes a stale path fail instead of writing to the wrong place.

Why does the planner delete a removed tree's files one by one, instead of deleting the top directory recursively? A recursive delete would also delete an entry that another host had just created in the tree, without a conflict. A non-recursive delete of a directory that isn't empty fails with `directory_not_empty`, so the conflict is explicit. The cost is one trash entry for each deleted item.

Why does a rename onto a file keep the target's history? Editors and many other programs save a file by writing a temporary file and renaming it over the original. If the rename replaced the target's inode, every save would start a new history, and the file's earlier revisions would be in the trash under a deleted inode instead of in the file's own history.

Why does the time attribute record its revision? An attribute map has one update time for the whole map (`attributes_updated_at_ms`), so if the mount compared that time with the revision's commit time, a later change of mode would make an old explicit time look current. The revision number ties the time to the content that it describes.

Why a copy, and not a put of the renamed item's bytes? A put that refers to uploaded content needs a content token, which a completed upload issues for a limited time, and a put of inline bytes needs the bytes on this host. A copy refers to the renamed item's revision on the server, so it needs neither.

## Publishing a batch

This section describes how the engine uploads captures, sends the pending batch, and handles the result.

A commit has to be retried with exactly the same request after a lost response or a restart, or the retry is a different commit (see [Commit responses and safe retry](../specs/api.md#52-commit-responses-and-safe-retry)). So the engine stores each request before it sends it, and sends only the stored request until the batch is resolved.

The engine uploads a capture that's above the inline limit before any plan includes it. When an item's content change needs an upload, the engine takes a capture and starts its upload through the client's upload, which uses one of the transports that the deployment advertises. Each item has at most one upload in flight, and each mount at most four. The engine records the upload session ID with the capture, and when the upload completes, it records the content reference and the content token. The planner includes an item's content change once its newest capture's upload has completed. If the program wrote again during the upload, the plan publishes the captured generation and the item stays dirty. An upload that a restart interrupted starts again in a new session, and the engine aborts the old session.

To publish a plan, the engine:

1. Generates a new random commit ID.
2. Builds the request, with each inline capture's bytes and each uploaded capture's content reference and token.
3. Stores the pending batch in one database transaction: the request body, the actor and subject headers, the commit ID, and for each item, the generation it publishes, its capture, and its operations.
4. Sends the request.

An acknowledgment comes from the commit response or from the commit's echo in the change feed (the commit with the batch's commit ID), whichever the engine reads first. For each item in the batch, the acknowledgment moves the Committed columns of the field groups the batch published to the published state, with the revision numbers, binding versions, attribute revisions, and new inode IDs from the commit's events, and sets the server-observed columns to the same values. The engine moves each capture into the cache. A tail capture is appended to the base's cache file when the cache has it, and is dropped otherwise. If the item's generation still equals the generation the batch published, the item is clean, and otherwise it stays dirty relative to its new base. The engine then deletes the pending batch and records the commit's sequence, so that the feed skips the commit when it reaches it. All of this happens in one database transaction.

The engine maps a new item to its inode ID with the commit's events, which are in request order. A creation by path creates its parents explicitly, so each creation produces one `file_created` or `directory_created` event, and the event's parent and display name identify the creation.

The engine resolves the other responses this way:

| Response | What the engine does |
| --- | --- |
| A transport failure, `commit_outcome_unknown`, `deadline_exceeded`, `server_busy`, `commit_queue_full`, `shutting_down`, `writer_session_closed`, `store_contention`, `maintenance_required`, or `stale_head` | Reads the change feed to the head, and if the feed doesn't contain the batch's commit ID, sends the stored request again, with a backoff from 250 ms to 30 seconds. The mount sends no head precondition, so the engine handles `stale_head` like a transient failure. |
| `content_not_prepared` | Reads each uploaded capture's session for a new token, stores the new tokens in the pending batch, and sends the request again. |
| `stale_revision`, `path_conflict`, `binding_version_mismatch`, `stale_attributes`, `directory_not_empty`, `would_cycle`, `inode_not_found`, or `not_deleted` | A conflict. The error's `operation_index` or `precondition_index` identifies the item. |
| `forbidden` | A refused publication of the item that the error identifies. |
| `namespace_deleted` | Stops publishing (see [Failure modes](#failure-modes)). |
| `commit_id_reuse_conflict`, `invalid_request`, or `content_too_large` | Stops publishing for the mount and reports the error, because the planner checks every limit and never changes a stored request, so only a bug in the engine can cause these errors. |
| `storage_permission_denied`, `namespace_corrupt`, or `server_error` | Sends the stored request again with the same backoff, and reports the error in status. |

Before the engine handles a conflict or a rejection, it reads the change feed up to the current head. If the feed contains the batch's commit ID, an earlier attempt committed after all, and the echo acknowledges the batch. Otherwise the rejection stands. The engine deletes the pending batch, hands the items that failed to [Resolving conflicts](#resolving-conflicts) (the feed has brought the other host's change into their server-observed columns), and plans the rest again.

A retry sends the stored body unchanged, except for content tokens. A token lasts an hour, so when a token that the engine would send is more than 50 minutes old, the engine reads the capture's upload session for a new one first. The commit's [semantic fingerprint](../specs/glossary.md#semantic-fingerprint) covers the actor and subject headers, the operations, the message, and the preconditions, but not the tokens, so a retry with a new token is the same commit.

A barrier runs the planner at once over the whole dirty set, after the pending batch is resolved, and returns when every item it covers is published at the generation it had when the barrier started. A whole-set plan includes the changes that an item depends on (its new parent directories, and the moves and deletes that free its name). An `fsync` covers one item, and `loonfs sync` and the barriers of `unmount` and `run` cover every item. A barrier fails, with `EIO` for an `fsync`, when the sync timeout passes, when it covers a refused publication that hasn't been reported, or when publication has stopped.

Why is there one pending batch, and not a pipeline of commits? With one batch in flight, every plan starts from a base that the server has acknowledged, and an acknowledgment moves each item's base to exactly the generation it published. With several batches in flight, a later batch's preconditions would depend on an earlier batch's outcome, and a failure in the middle would leave later batches with preconditions that can never pass.

Why are captures uploaded before the plan that publishes them? An upload of a large file can take minutes. If the upload were part of the pending batch, every other change on the mount, including a one-byte change to a small file, would wait for it.

Why does the engine read the feed before every resend? An earlier attempt may have committed, and once its receipt is removed, a resend of a creation by path can commit a second time, and a resend of an inode-addressed operation is rejected. The feed contains every commit after the mount's applied sequence, so the commit's echo shows whether an earlier attempt committed.

## Following the change feed

This section describes how the engine applies other hosts' commits.

The engine reads the change feed with `after_seq` set to its applied sequence and `consistency=strong`, every poll interval (500 ms by default), and again at once when a page ends before the head (the response includes `next_after_seq`). A poll can't use `eventual` consistency, because an `after_seq` above the head that the poll reads is an invalid request, and an `eventual` poll can read a head older than the mount's applied sequence (see [`GET /changes`](../specs/api.md#611-get-changes)).

The engine applies each commit in one database transaction, so a commit is applied entirely or not at all, and it sends the kernel's invalidations once per page. A commit whose commit ID is the pending batch's is the batch's echo, and it acknowledges the batch. A commit whose sequence the engine recorded at an acknowledgment is skipped. For every other commit, the engine applies each event in order. One operation of a request can produce several events (a put that creates missing parents, a replacing move that deletes the file it moves over and then moves the source, and a copy that creates a file and then sets its attributes), and each event is applied as the state it records.

For each event, the engine writes the server-observed columns of the field group that the event changes. If that group is clean, the engine also writes the Committed and Local columns, and sends the kernel the matching invalidation. If the group is dirty, the planner finds the difference before its next plan.

| Event | What the engine does |
| --- | --- |
| `directory_created`, `file_created` | Adds the item if its parent is in the mount. If a new local item has the same name, the remote item is added to the Committed tree and kept out of the Local tree until the conflict is resolved. |
| `content_changed` | Updates the content group, as [Reading files](#reading-files) describes. An event with `append_offset` equal to the previous size keeps `s_append_only` set, and any other event clears it. |
| `attributes_changed` | Updates the attributes group from the `fs.` keys. |
| `moved` | If both parents are in the mount, updates the binding. If the destination is outside the mount, removes the item and its subtree, as for a deletion. If the source is outside and the destination is inside, reads the item (see below). |
| `deleted` | Removes the item and everything below it, because a recursive delete has one event for the deleted directory only. A dirty item below it stays, with `s_hidden_by` set to the deleted root and the commit's sequence. |
| `undeleted` | Reads the item, as for a move into the mount, because the event doesn't include the item's revision or its subtree. |
| `deletion_expired`, `access_changed` | Ignored. |

When an event brings an item into the mount that the engine doesn't have (a move from outside the mount, or an undelete), the engine reads the item by inode. For a directory, it creates a snapshot with a ten-minute `ttl_ms`, lists the directory recursively at the snapshot, and keeps the listed rows out of the Local tree until the applied sequence reaches the snapshot's captured sequence. The commits in between are applied to those rows as they are to any other rows, and an event in them for another item that the engine doesn't have is read at the same snapshot. When the applied sequence reaches the captured sequence, the engine adds the directory to the Local tree and deletes the snapshot. This listing is the one subtree listing that the engine makes after admission, apart from a rebuild, and its size is bounded by the subtree that entered the mount.

The engine follows the mount's root by inode ID, so a move of the root or of a directory above it doesn't change what the mount shows. The feed records a deletion of the root or of a directory above it as one event for the deleted directory. To find that event, the engine keeps the inode IDs of the root's ancestors, and it updates them when a `moved` event moves one of them or the root. When the feed deletes one of them, the engine stops publishing (see [Failure modes](#failure-modes)).

If the feed returns `rebootstrap_required`, because the retention floor passed the mount's applied sequence while the daemon was stopped, the engine rebuilds. It lists the root's subtree at a new snapshot, as admission does, and writes the server-observed columns of every item, and the Committed and Local columns of the clean field groups. Items that the listing doesn't include are removed if they're clean. A dirty item that the listing doesn't include is recorded as deleted on the server, which the conflict policy treats as a deletion that can't be undone, so `overwrite` creates the item again. Dirty field groups keep their bases, so every remote change to a dirty item becomes a conflict at the next plan. A pending batch whose outcome is still unknown is treated as rejected, and if it did commit, its items' bases differ from the server's state, so the conflict policy handles them. Under `keep_both` and `overwrite`, a creation that was never acknowledged is created again, and under `refuse`, its bytes go to the control directory. The engine then sets its applied sequence to the snapshot's captured sequence and deletes the snapshot.

Why does the engine keep a moved-in directory hidden until the feed reaches the snapshot? The listing shows the directory as it was at the snapshot, which can be later than the commit that moved it in, and the feed between the two can change the same entries. If the engine showed the listing at once, a program could see an entry change back to an earlier state as the feed caught up.

Why poll instead of waiting on the server? A request that the server kept open until a commit arrived would occupy one of the server's request slots for as long as it waited, so a few hundred idle mounts would use the server's whole request limit. A strong poll every 500 ms costs one short request, and its head confirmation is shared with the server's other strong reads.

## Resolving conflicts

This section describes how the engine finds a conflict with another host's change, and the three conflict policies that resolve it.

The engine finds a [conflict](#the-cost-of-sending-each-file-call-to-the-server) in two places: before a plan, when a dirty group's server-observed columns differ from its Committed columns, and after a rejected request, once the feed has brought the other host's change into the server-observed columns. A field group that only one side changed isn't a conflict, so a local rename and a remote content change of the same file both take effect.

The mount's conflict policy (`--conflict`) has three values:

- `keep_both` (the default): the remote change stays, and the local version is published next to it, so both versions are visible in the tree. For a file's content, the local bytes are published as a sibling whose name adds a conflict suffix and a sequence (`report.txt.conflict-418`), and the local file adopts the remote revision.
- `overwrite`: the local change is published on top of the remote one. For a file's content, the local bytes are published as the next revision, and the remote revision stays in the file's history. No program gets an error, and status lists the conflict.
- `refuse`: the local change is a refused publication, and the remote change stays. For a file's content, the local file adopts the remote revision, and the local bytes are kept in the mount's control directory. The write already returned success, so the refusal is reported at the next `fsync` of the file, at the next barrier over the whole mount, and in status.

To "adopt" a remote revision, the engine makes it the item's base, and if the item has a working file, replaces the working file's bytes with the remote revision's bytes, so every descriptor on the item reads them. Later writes apply on top of the adopted revision and publish as usual.

An append that conflicts with remote appends is re-based instead, under `keep_both` and `overwrite`. If the local change is an append and every remote revision since the base was an append that started at the previous size (`s_append_only`), the engine fetches the remote tail into the working file ahead of its own new bytes, moves the base to the remote revision, and plans its append again against that base. Both hosts' lines end up in the file, in the order they were published. A program that writes with `O_APPEND` doesn't notice, but a program that writes at specific offsets and reads them back sees its bytes move. If any remote revision since the base wasn't an append, the conflict is an ordinary content conflict. Under `refuse`, an append conflict is refused like any other.

Other conflicts follow the same three rules. Under `overwrite`, the local change wins, with an undelete or a new creation where it's needed. Under `keep_both`, the remote structure wins, and the local bytes go to a sibling. Under `refuse`, the remote change wins, and the local bytes go to the control directory.

| Local change | Remote change | `keep_both` | `overwrite` | `refuse` |
| --- | --- | --- | --- | --- |
| Content | Content | The local bytes go to a sibling, and the file adopts the remote revision. | The local bytes become the next revision. | The local bytes go to the control directory, and the file adopts the remote revision. |
| An append | Appends | Re-based. | Re-based. | As for content. |
| Content | The item, or a directory above it, was deleted | The item leaves the tree, and its local bytes go to a sibling in the nearest directory that still exists. | The item comes back, and the local change is published. | The item leaves the tree, and its local bytes go to the control directory. |
| A move, a rename, or attributes | The item, or a directory above it, was deleted | The item leaves the tree. | As for content. | The item leaves the tree. |
| A new item at a name | Another item was created at that name, or moved to it | The local item, with everything below it, is published under the sibling name. | The local item takes the name. | The local item, with everything below it, goes to the control directory. |
| A move or rename | The item was moved or renamed | The item moves to the remote location. | The item is moved from where the remote side put it to its local location. | The item moves to the remote location. |
| A directory delete | Something in the directory was created or changed | The directory comes back with the remote entries. | The directory is deleted recursively, and the remote entries go to the trash. | As for `keep_both`. |
| An unlink | The file's content or attributes changed | The file comes back with the remote change. | The unlink is published, and the remote revision is recoverable from the trash. | As for `keep_both`. |
| Attributes | Attributes | The remote attributes stay. | The local attributes are published. | The remote attributes stay. |

Under `overwrite`, an item that a remote deletion hid comes back through an undelete of the deletion that the `deleted` event identifies, or through a new creation if the deletion can't be undone, and the local change is then published. A new local item that takes a name from a remote item replaces it in one of two ways. If both are files, the local bytes become the remote file's next revision, as a local rename over the file would. Otherwise the remote item is deleted recursively, and the local item is created in its place.

When a directory that was deleted locally comes back with remote entries, the local deletes of the entries that the remote side didn't touch are still published.

A new local directory at a name where the remote side also created a directory isn't a conflict. The local directory takes the remote directory's inode, and its entries publish into that directory, each with its own conflicts. For example, two hosts that both run `mkdir -p out/logs` end up with one `out/logs`. Under `refuse`, every row of the table is a refused publication, including the rows that keep no bytes, and it's reported as described below.

A sibling's name is the original name followed by `.conflict-` and the sequence of the remote commit that the item conflicted with. If that name is taken, the engine adds `-2`, `-3`, and so on, and if the result would be longer than 255 bytes, the engine shortens the original name at a character boundary. The bytes of a refused publication are kept at `.loonfs/conflicts/<sequence>-<name>` in the control directory, and status lists the original path, the remote commit, and the reason. A refused publication is reported once to the next `fsync` of the file, as `EIO`, and once to the next barrier over the whole mount, which fails, so `loonfs run` exits with status 4, and `loonfs unmount` leaves the mount in place until `loonfs unmount` runs again.

Why is re-basing limited to remote appends? A re-base keeps the base's bytes and adds the remote tail before the local one. If the remote side replaced the file, the base's bytes are no longer the start of the remote file, so a re-base would publish a file that neither host wrote.

Why does `keep_both` publish the sibling instead of keeping it on the host? A sandbox's disk is often discarded when its command ends. A sibling in the namespace survives the sandbox, and every host that mounts the namespace can see it.

## The FUSE adapter

This section describes how the adapter mounts the directory and serves the kernel's requests.

The adapter uses `fuser` 0.18.0 and has no FUSE code of its own. `fuser` mounts with `mount(2)` first, and uses `fusermount3` only when `mount(2)` returns `EPERM`. So a daemon that runs as root, which is the usual case in a sandbox, mounts without `fusermount3`, and a daemon that runs as another user still works where `fusermount3` is installed. Status reports whether the mount went through `mount(2)` or `fusermount3`.

The adapter mounts with `default_permissions`, as an ordinary mount option, so the kernel checks each item's mode against the uid and gid it shows. To let users other than the daemon's user use the mount, the adapter sets `fuser`'s session ACL to `SessionACL::All`. `SessionACL::All` sets `allow_other`. Without it, `fuser` also rejects callers whose uid differs from the daemon's, so a root daemon that serves other users has to set it. Through `mount(2)` as root, `allow_other` needs nothing more. Through `fusermount3` as another user, it needs `user_allow_other` in `/etc/fuse.conf`. A read-only mount is also mounted read-only. During initialization, the adapter asks for readdirplus, which `fuser` leaves off by default. Unmounting detaches the mount, and it's idempotent.

The adapter serves the kernel's requests this way:

- `lookup`, `getattr`, `readdir` (and readdirplus), and `readlink` read the Local tree. Entries and attributes of items without local changes have a cache lifetime of one second, and those of items with local changes have a lifetime of zero, so the kernel asks the daemon each time.
- `open` for reading calls `open_read`. The adapter sets the keep-cache flag on opens of clean, complete files, so repeated reads of an unchanged file come from the kernel's page cache, and leaves it off for files with local changes.
- `create`, and `open` for writing, call `open_write`, and each `write` is a `pwrite` to the working file through the engine.
- `flush`, which the kernel sends once for each `close` of each duplicated descriptor, returns without doing anything. `release`, which the kernel sends once after the last close, closes the descriptor in the engine. `fsync` calls `barrier` on the item and returns when the barrier returns.
- `forget` calls `forget` with the kernel's count.
- `mkdir`, `rename`, `unlink`, `rmdir`, `symlink`, and `setattr` change the Local tree and return. A rename is exact locally. The source item takes the destination name, and a replaced destination is unlinked but stays alive while descriptors or lookups refer to it. Keeping the target's history is the planner's job, and the adapter isn't involved.
- `statfs` returns the cache's limits, the free space, and the entry budget.

The mount doesn't support every request that a local filesystem does:

| Request | Result |
| --- | --- |
| `link` | `EPERM`, as `link(2)` returns on a filesystem without hard links. |
| `mknod` of anything but a regular file | `EPERM`, so a program that creates sockets or FIFOs needs a directory outside the mount. |
| `chown` | `EPERM`. |
| `ioctl` and `fallocate` | `ENOSYS`. |
| `copy_file_range` | `ENOSYS`, so the kernel falls back to reads and writes. |
| Extended attributes | `ENOTSUP`. |
| File locks | The kernel's local locks, which don't reach other hosts. |

Every item shows the uid and gid that `--uid` and `--gid` set. A deployment whose command runs as another user than the daemon sets them to that user, so the command can write files with mode 644.

The adapter reads `subscribe_invalidations` on a thread of its own and sends the kernel entry, inode, and delete notifications through the session's notifier. An append invalidates the item's attributes and the appended range, so the kernel's cached pages before the old end stay valid. A replacement or an adoption invalidates the entry or the inode, which drops the kernel's cached pages.

The control directory `.loonfs/` at the mount's root is a read-only virtual directory. It contains `status.json` (the same report as `loonfs status --json`), `seq` (the applied sequence), and `conflicts/` (the bytes of refused publications, stored in the mount's state directory).

Measured on a 7.2 kernel with `fuser` 0.18.0, one FUSE round trip costs about 1 to 1.5 microseconds. A buffered sequential read of a file that the daemon served from memory ran at 2.6 to 7.5 GB/s, against 11 to 22 GB/s for the same read from the kernel's in-memory filesystem. A `stat` within a one-second attribute lifetime cost about 0.2 microseconds and sent nothing to the daemon. With a lifetime of zero, it cost about 2 microseconds without `default_permissions` and 3 with it, because `default_permissions` adds a second `getattr` to each `stat`.

Why does every read and write go through the daemon? `fuser` has no splice or zero-copy path, so every byte crosses the daemon, and at 2.6 to 7.5 GB/s that path is faster than the network that fills the cache. The keep-cache flag lets the kernel serve repeated reads of unchanged files from its page cache, which removes the daemon from the most common repeated reads. Going through the daemon also means that every write passes through the engine, and the append rule needs to see every write.

Why a one-second cache lifetime for clean items, and zero for items with local changes? A clean item changes only when the feed changes it, and the engine invalidates the kernel's entry at once, so the lifetime only bounds how long a lost invalidation could last, and it saves about 3 microseconds on every `stat`. An item with local changes changes size and time with every write, and a lifetime of zero keeps `stat` exact, at a cost of about 3 microseconds per call.

## Databases and generated files

This section describes what the mount promises for a database file, and where generated files belong.

A database on the mount works within one host, because locks, write ordering, and `fsync` are local to the host. Each published revision of a database file is a rate-limited copy of that file, and the published revisions of a database's several files (for example, the main file and its write-ahead log) aren't consistent with each other. A copy that another host can open is made with the database's own backup command, writing into the mount, or by copying the database while nothing writes to it.

Caches and build output belong in a directory outside the mount, because they're rewritten often and other hosts don't need them, and each publication of them costs a revision.

## Resource bounds

This section describes the limits on what a mount can use, over its whole lifetime.

The entry budget is checked at admission. After that, local creates and commits from other hosts can grow the tree past it, and the engine never applies part of a commit to stay under the budget. When the tree has more than twice the budget's entries, status and `.loonfs/status.json` report a warning, and the mount keeps working.

The engine's memory for the tree is at most about 400 bytes per entry, so the default budget of 100,000 entries takes about 40 MB.

Local disk is divided into two kinds of bytes:

- Evictable bytes are cache files, complete or partial, that no descriptor uses. `--cache-max-bytes` is the eviction target for them, and the engine also evicts to keep `--cache-min-free` free.
- Pinned bytes are working files, captures, uploads in flight, the copies in the control directory, and cache files with open descriptors, including partial files. `--pinned-max-bytes` is a hard limit on them. When it's reached, a new materialization and a write that would grow pinned bytes fail with `ENOSPC`. Open files can keep pinned bytes above the limit, because the engine can't free them.

A daemon with several mounts uses at most the sum of their limits. Each mount has its own cache directory, so no cache file is shared between mounts.

All fetches of one mount share a limit of 256 MiB and 32 requests in flight, and each mount has at most four uploads in flight.

## Failure modes

This section describes what a mount does when something fails.

| Failure | What the mount does |
| --- | --- |
| The server is unreachable. | Metadata reads and cached content keep working. A read of content that isn't cached fails with `EIO` after 30 seconds. Writes are accepted, and the dirty set grows. |
| The feed returns `rebootstrap_required`. | The engine rebuilds (see [Following the change feed](#following-the-change-feed)). |
| A conflict. | The conflict policy resolves it (see [Resolving conflicts](#resolving-conflicts)). |
| The mount's root, a directory above it, or the namespace is deleted. | The engine stops publishing, copies every unpublished change into the control directory, and reports the deletion in status. Every barrier fails, and `loonfs run` exits with status 4. |
| The disk is full, or pinned bytes reach their limit. | Writes and new materializations fail with `ENOSPC`, and the cache stops filling. The engine logs this once. |
| The daemon crashes. | Open descriptors fail with `ENOTCONN`, and `loonfs run` fails the command with status 5. The next `loonfs mount` loads the trees, treats every item with a working file as possibly changed, resumes the pending batch, and plans again. |
| The machine crashes. | Published generations are durable. Accepted writes that weren't published can be lost, or torn in the working file, and the engine captures whatever the working file contains as a new generation after the restart. A capture is written, synced, and renamed before the database transaction that refers to it, so no record refers to a capture that isn't on disk. |
| The host can't serve a mount (no `/dev/fuse`, no way to mount, a filesystem without sparse files, or a second daemon on the state directory). | `loonfs mount` fails with the reason before it mounts anything. |

While the server is unreachable, status reports the age of the oldest unpublished change. A barrier fails once the sync timeout passes, and `loonfs run` then exits with status 4 and keeps the state.

`loonfs status` reports, for each mount: whether it's ready, whether it mounted through `mount(2)` or `fusermount3`, the applied sequence, the number of dirty items and the age of the oldest unpublished change, the pending batch and its attempts, uploads in flight, the cache's evictable and pinned bytes, the number of invalidations dropped, the entry count against the budget, whether the filesystem supports reflinks, conflicts and refused publications, and any reason that publication stopped.

## Scaling invariants

This section lists the measurable properties that the mount has to keep, which the tests in [Testing](#testing) check.

1. After a mount is ready, no engine code lists a subtree on the server, except the listing of a directory that enters the mount (which reads at most the entries that entered) and a rebuild after `rebootstrap_required`. Both are recorded in metrics.
2. In steady state, a mount sends at most two feed polls a second, one commit per publish interval and one per append interval (more only when a plan exceeds a request's limits), the uploads of captures above the inline limit, and content fetches in proportion to the bytes that programs read or are about to read (at most twice the bytes each handle consumes, plus its first window).
3. The tree takes at most 400 bytes of memory per entry.
4. A plan with D dirty items does work in proportion to D, and constant work when D is zero.
5. 10,000 writes to one file in one second publish exactly one content operation and no attribute update.
6. A namespace with more entries than the budget fails admission before the kernel mount, after listing at most the budget plus one entries.
7. When 32 child sandboxes append to logs and one parent follows them with `tail -f`, each appended line is visible to the parent within the append interval plus the poll interval plus 500 ms, at the 99th percentile.
8. A sequential read of a complete cached file through the mount reaches at least half the throughput of reading the cache file directly, and a second open of an unchanged file reads from the kernel's page cache without a read request to the daemon.
9. With 1,000 materialized files, a `kill -9` of the daemon at any point in capturing, uploading, sending, or acknowledging converges after a restart with no lost and no duplicated commits, and every item's published bytes equal the generation that was published.
10. 500 concurrent opens that each read 4 KiB of a 100 MiB file transfer less than 1 GiB in total.
11. For any pair of Committed and Local trees and any pending batch that the simulation generates, planning, applying, and following the feed reach a clean tree, and the simulated server's preconditions reject no operation.

## Costs

This section describes what the design costs, and which workloads pay for it.

- Every publication of a changed file is a revision, and the namespace keeps it until the retention floor passes it, so a mount needs a retention window.
- A write open of an existing file that isn't cached downloads the whole file, so a small append to a large file costs a full download on its first write open.
- Without reflinks, every capture of a change that isn't an append copies the whole file.
- An append tail larger than the inline budget is published as a replacing revision, so a reader on another host that has the file open stays on the revision it opened, until it opens the file again.
- A program that keeps a file open while another host replaces it costs a download of the rest of the revision it opened.
- A barrier plans the whole dirty set and waits for its commits, so an `fsync` of one small file waits for the commit of every other change that's ready, and a database that calls `fsync` on every transaction waits for a commit on every transaction.
- A removed tree is deleted one item at a time, with one trash entry for each item.
- Each mount sends two strong feed polls a second, and each strong poll makes the server confirm its head, which costs a store request that the server shares with the strong reads that arrive while it's in flight. A server with 1,000 mounts receives 2,000 polls a second.
- Admission reads every entry of the mounted tree at a snapshot, and so does a rebuild. Creating a snapshot makes the server fold the namespace's WAL tail first (see [Snapshots](../specs/api.md#snapshots)), so each admission, rebuild, and listing of a directory that enters the mount costs the server a fold.

## Testing

This section describes the simulation, the tests of specific orderings and rules, and the suites that run against a real kernel.

The simulation runs the engine's control task against a simulated server, a fake object store, a fake disk, and a fake kernel, under the virtual clock and fault injection of `loonfs-sim`. The simulated server is built on `loonfs-model`, the executable reference model of the metadata semantics, so the simulation can't drift from the real binding and visibility rules. The simulated server turns each request operation into the format's deltas and applies them with the model. The simulation adds what the model doesn't have: commit receipts and replays, preconditions and their errors, the change feed with echoes that can arrive before or after the response and pages that can be delayed, snapshots, a content store (uploads, inline content, ranged reads, and grants), retention (a floor that removes receipts and returns `rebootstrap_required`), and crash points between a write to a working file, a capture's rename, an upload's completion, a database transaction, a send, and an acknowledgment.

The simulation runs two kinds of test:

- Convergence: random Committed and Local trees with a random pending batch must reach a clean tree (invariant 11). A failing case is shrunk by removing items.
- Faults: random sequences of kernel calls, feed events, request failures, and crashes run against a model that records the Local tree that programs should see and the generations that were acknowledged. The tests check that the tree that the kernel shows is never wrong, that no acknowledged generation is ever lost, and that the mount converges once the faults stop.

One test covers the hardest ordering in the design:

1. Pause the upload of generation B of a file.
2. Write generation C, and rename the file.
3. Deliver the response to B's commit and B's echo in the feed, in both orders.

The test checks that B is published and its bytes equal its capture, that C stays dirty, that descriptors opened before the rename still read and write the right local file, and that the next plan publishes C to the right LoonFS inode.

These tests check other orderings and rules:

- A file overwritten in the middle and then appended to publishes a replacing revision, not an append.
- Two files open across a replacing rename, with reads and writes through every descriptor afterward.
- A retry whose receipt is gone fails a precondition and doesn't commit twice.
- A rebuild after `rebootstrap_required` keeps the bases of dirty items.
- A populated directory moves into the mount, and a deleted directory is undeleted into it.
- A partly cached open revision is replaced, and its reader finishes reading it.
- A `tail -f` reader on one host follows appends from another host.
- Duplicated descriptors send several `flush` requests before one `release`.
- Each conflict policy, for each row of the conflict table, including a re-based append.
- Two names that differ only in Unicode normalization.
- The engine crate's dependency test.

Against a real kernel, the mount runs a POSIX conformance suite (every test except hard links and file flags, with each exclusion listed), a randomized file-operation exerciser that checks every read against a model of the file, and the generic quick group of a filesystem regression suite where it applies.

The workloads are a version-control checkout of 50,000 files followed by a status scan, a package manager's install of a dependency tree (which is expected to trigger the warning for busy files), a program's configuration directory that it rewrites often, a fan-out of 32 child sandboxes that append to logs while one parent follows them, and a reader that opens many files and reads a little of each.

## API sections that the mount uses

This section lists the parts of the API specification that the mount uses. The mount needs no change to the format or the API, because it's a client of the public API.

- [Commit identity and preconditions](../specs/api.md#51-commit-identity-and-preconditions) and [Commit responses and safe retry](../specs/api.md#52-commit-responses-and-safe-retry), for the operations' preconditions, the request's `path_binding` preconditions, and retries.
- [`POST /commits`](../specs/api.md#68-post-commits), for inline content, appends, replacing copies, and the inode-addressed operations.
- [Upload transport](../specs/api.md#69-upload-transport) and [Content references and tokens](../specs/api.md#content-references-and-tokens), for the uploads of new captures and their tokens.
- [Ranged reads](../specs/api.md#ranged-reads) and [Download transport](../specs/api.md#610-download-transport), for fetches.
- [`GET /changes`](../specs/api.md#611-get-changes), for the feed, `append_offset`, and `rebootstrap_required`.
- [Snapshots](../specs/api.md#snapshots) and the recursive listing in [`GET /filesystem/entries` and `GET /inodes/{inode_id}/children`](../specs/api.md#65-get-filesystementries-and-get-inodesinode_idchildren), for admission and rebuilds.
- [Retention runs](../specs/api.md#retention-runs), for the hour that a replaced revision is kept, and the retention window.

## Alternatives considered

Have the server enforce a writer lease for each mounted subtree. A lease would prevent conflicts instead of resolving them, but it needs a new server resource, an epoch on every commit, and rules for other clients that write inside a leased subtree and for moves across a lease's boundary. A lease that a sandbox took before it died blocks every other host until the lease expires. The server's preconditions already make every conflict an explicit rejection.

Have the server keep a feed request open until a new commit arrives. It would show changes sooner than a 500 ms poll, but each waiting request occupies one of the server's request slots, so a few hundred idle mounts would use the server's whole request limit.

Keep a snapshot for the mount's whole lifetime, so that a reader can keep reading a replaced revision. The snapshot pins a manifest and every segment it lists, so for as long as the mount runs, garbage collection couldn't reclaim anything that the pinned manifest refers to, however much of it later commits replace. The hour that the retention floor keeps and fetching the rest of a revision at once cover open readers without a pin.

Let the kernel read and write a local backing file directly (passthrough). It would make reads of cached files as fast as local reads, but it needs one backing file per inode at a time, it hides writes from the daemon (so the append rule would need a state for writes the daemon didn't see), and it depends on a recent kernel. The daemon's read path runs at 2.6 to 7.5 GB/s from memory, and the keep-cache flag serves repeated reads from the page cache.

Add a server endpoint that counts a subtree's entries for admission. It's a new mechanism in the server for one client, and the scan that admission needs anyway reads at most one page more than the budget.

Address every operation by inode ID. A new item's inode ID doesn't exist until its commit is acknowledged, so a new tree would publish one level per commit, and a new file's mode and symlink target would publish in a commit after the file, so other hosts would briefly see a symlink as an empty file.

Publish an append tail that's larger than the inline budget as a series of append commits. Each commit with more than 1 MiB of inline bytes is a WAL object of its own and moves the fold trigger forward, so a burst of 100 MiB would be 100 commits in a row, and the fold would write all of it out of the WAL again, where one upload publishes it in one request.

Upload every file instead of sending small files inline. Each upload takes three requests (begin, content, and complete) and leaves a content object for every small file, while inline content publishes with the commit.

Fetch the whole file in the background when a program opens it. A program that reads 4 KiB of a 100 MiB file would transfer 100 MiB, so 500 such programs would transfer about 50 GiB.

Let a subject that isn't an administrator mount an ACL namespace. Reading the change feed with a subject requires an administrator, so the mount would have to read the feed as the token holder, and the feed would then include changes to entries that the subject can't read. That reveals those entries' names to the subject, and it disagrees with the admission listing, which leaves those entries out.

Retire the replaced file's local identity on a rename onto it, and move that identity to the renamed file, so that the local inode number stays with the LoonFS inode. Descriptors open on the replaced file would then read and write the renamed file's bytes, which a local filesystem never does.
