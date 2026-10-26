//! A read-only mount of a namespace: listing, reading, the control
//! directory, refused writes, an append and a replacement that another
//! client makes, and a clean unmount.

use crate::harness::{eventually, runtime, Harness};
use loonfs_client::{
    AttributeChanges, Client, CreateDirectoryOptions, NamespacePath, PutFileOptions,
};
use loonfs_types::{
    AbsolutePath, ActorId, AttributeKey, AttributeValue, ChangeSeq, DestinationBehavior,
    NamespaceId,
};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read as _;
use std::os::unix::fs::{FileExt as _, MetadataExt as _};
use std::path::Path;

const NAMESPACE: &str = "mounted";
const README: &[u8] = b"hello from a mounted namespace\n";
const DATA_BYTES: u64 = 200_000;
const REPLACED_BYTES: u64 = 3 << 20;
const EROFS: i32 = 30;

/// Returns `size` bytes that differ for each `seed` and change every eight
/// bytes, so a read of the wrong revision or offset can't pass.
fn pattern(size: u64, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|offset| u8::try_from(offset / 8 % 251).expect("a byte") ^ seed)
        .collect()
}

struct Writer {
    client: Client,
    actor: ActorId,
    namespace: NamespaceId,
}

impl Writer {
    fn path(&self, path: &str) -> NamespacePath {
        NamespacePath::new(
            self.namespace.clone(),
            AbsolutePath::parse(path).expect("an absolute path"),
        )
    }
}

/// Creates the namespace's tree through the client, and returns the
/// sequence of its last commit.
async fn populate(writer: &Writer) -> ChangeSeq {
    let Writer { client, actor, .. } = writer;
    client
        .create_namespace(&writer.namespace, actor)
        .await
        .expect("the namespace");
    let parents = CreateDirectoryOptions {
        parents: true,
        ..CreateDirectoryOptions::default()
    };
    for directory in ["/docs/nested", "/empty"] {
        client
            .create_directory_with_options(&writer.path(directory), actor, &parents)
            .await
            .expect("a directory");
    }
    let files = [
        ("/docs/readme.txt", README.to_vec()),
        ("/docs/nested/data.bin", pattern(DATA_BYTES, 1)),
        ("/docs/link", Vec::new()),
        ("/log.txt", b"line 1\n".to_vec()),
        ("/replaced.bin", pattern(REPLACED_BYTES, 2)),
    ];
    for (path, bytes) in files {
        client
            .put_file(&writer.path(path), &bytes, actor)
            .await
            .expect("a file");
    }
    let symlink = [("fs.type", "symlink"), ("fs.target", "readme.txt")]
        .into_iter()
        .map(|(key, value)| {
            (
                AttributeKey::parse(key).expect("a key"),
                AttributeValue::parse(value).expect("a value"),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let changes = AttributeChanges {
        set: symlink,
        remove: Vec::new(),
    };
    client
        .update_attributes(&writer.path("/docs/link"), actor, changes)
        .await
        .expect("the symlink's attributes")
        .committed_seq
}

/// Mounts a namespace read-only through the CLI and the daemon, checks what
/// programs see, follows an append and a replacement, and unmounts.
#[test]
#[ignore = "mounts a FUSE filesystem; run with `--ignored` after building loonfs-cli and loonfs-server"]
fn a_read_only_mount_shows_the_namespace_and_follows_other_hosts_changes() {
    let harness = Harness::start();
    let runtime = runtime();
    let writer = Writer {
        client: harness.client(),
        actor: ActorId::parse("mount-test").expect("an actor"),
        namespace: NamespaceId::parse(NAMESPACE).expect("a namespace"),
    };
    let populated_seq = runtime.block_on(populate(&writer));
    let mount = harness.mount_directory();
    let owner = std::fs::metadata(harness.directory.path()).expect("the test's directory");
    let (uid, gid) = (owner.uid().to_string(), owner.gid().to_string());
    let directory = mount.to_str().expect("a UTF-8 directory");
    let mounted = harness.loonfs_ok([
        "mount",
        NAMESPACE,
        directory,
        "--ro",
        "--uid",
        &uid,
        "--gid",
        &gid,
        "--poll-interval-ms",
        "100",
    ]);
    assert!(mounted.starts_with("mounted mounted:/ at "), "{mounted}");
    assert!(harness.is_mounted());
    let again = harness.loonfs_ok(["mount", NAMESPACE, directory, "--ro"]);
    assert!(again.contains("already has the mount"), "{again}");

    check_listing_and_reads(&mount, &owner);
    check_control_directory(&harness, &mount, populated_seq);
    check_refused_writes(&mount);
    check_append(&runtime, &writer, &mount);
    check_replacement(&runtime, &writer, &mount);

    let unmounted = harness.loonfs_ok(["unmount", directory]);
    assert!(unmounted.contains("and removed its state"), "{unmounted}");
    assert!(!harness.is_mounted());
    assert_eq!(harness.loonfs_ok(["status"]).trim(), "no mounts");
    let repeated = harness.loonfs_ok(["unmount", directory]);
    assert!(repeated.contains("has no mount"), "{repeated}");
}

/// Checks `ls`, `cat`, `readlink`, and `stat` of the mounted tree.
fn check_listing_and_reads(mount: &Path, owner: &std::fs::Metadata) {
    let names = |directory: &Path| {
        let mut names: Vec<String> = std::fs::read_dir(directory)
            .expect("a listing")
            .map(|entry| {
                entry
                    .expect("an entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        names.sort();
        names
    };
    assert_eq!(
        names(mount),
        [".loonfs", "docs", "empty", "log.txt", "replaced.bin"]
    );
    assert_eq!(names(&mount.join("docs")), ["link", "nested", "readme.txt"]);
    assert!(names(&mount.join("empty")).is_empty());

    let readme = mount.join("docs/readme.txt");
    assert_eq!(std::fs::read(&readme).expect("cat"), README);
    assert_eq!(
        std::fs::read(mount.join("docs/nested/data.bin")).expect("cat"),
        pattern(DATA_BYTES, 1)
    );
    let link = mount.join("docs/link");
    assert_eq!(
        std::fs::read_link(&link).expect("readlink"),
        Path::new("readme.txt")
    );
    assert_eq!(std::fs::read(&link).expect("cat through the link"), README);

    let stat = std::fs::metadata(&readme).expect("stat");
    assert!(stat.is_file());
    assert_eq!(stat.len(), README.len() as u64);
    assert_eq!(stat.mode() & 0o7777, 0o644);
    assert_eq!((stat.uid(), stat.gid()), (owner.uid(), owner.gid()));
    let link_stat = std::fs::symlink_metadata(&link).expect("lstat");
    assert!(link_stat.file_type().is_symlink());
    assert!(std::fs::metadata(mount.join("docs/nested"))
        .expect("stat")
        .is_dir());
}

/// Checks `.loonfs/status.json` and `.loonfs/seq`, and that `loonfs status`
/// reports the same mount.
fn check_control_directory(harness: &Harness, mount: &Path, populated_seq: ChangeSeq) {
    let status: serde_json::Value = serde_json::from_slice(
        &std::fs::read(mount.join(".loonfs/status.json")).expect("the status file"),
    )
    .expect("the status decodes");
    assert_eq!(status["namespace_id"], NAMESPACE);
    assert_eq!(status["path"], "/");
    assert_eq!(status["access"], "read_only");
    assert_eq!(status["readiness"], "ready");
    // The daemon runs as root, directly or through `sudo`.
    assert_eq!(status["mount_method"], "mount_syscall");
    let applied_seq = status["applied_seq"].as_u64().expect("a sequence");
    assert!(applied_seq >= populated_seq.0, "{status}");
    let seq: u64 = std::fs::read_to_string(mount.join(".loonfs/seq"))
        .expect("the seq file")
        .trim()
        .parse()
        .expect("a sequence");
    assert!(seq >= applied_seq);

    let reported: serde_json::Value =
        serde_json::from_str(&harness.loonfs_ok(["--json", "status"])).expect("status JSON");
    let mounts = reported["data"]["mounts"].as_array().expect("mounts");
    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0]["directory"], mount.to_str().expect("UTF-8"));
    assert_eq!(mounts[0]["status"]["namespace_id"], NAMESPACE);
}

/// Checks that a read-only mount refuses changes with `EROFS`.
fn check_refused_writes(mount: &Path) {
    let refused = [
        std::fs::write(mount.join("new.txt"), b"new"),
        std::fs::create_dir(mount.join("new")),
        std::fs::remove_file(mount.join("log.txt")),
        File::options()
            .append(true)
            .open(mount.join("log.txt"))
            .map(drop),
    ];
    for result in refused {
        let error = result.expect_err("a change to a read-only mount");
        assert_eq!(error.raw_os_error(), Some(EROFS), "{error}");
    }
}

/// Checks that an append through the client reaches a reader that has the
/// file open: a read at the old end returns the new bytes.
fn check_append(runtime: &tokio::runtime::Runtime, writer: &Writer, mount: &Path) {
    let mut log = File::open(mount.join("log.txt")).expect("open");
    let mut contents = String::new();
    log.read_to_string(&mut contents).expect("read");
    assert_eq!(contents, "line 1\n");
    runtime
        .block_on(
            writer
                .client
                .append_file(&writer.path("/log.txt"), b"line 2\n", &writer.actor),
        )
        .expect("the append");
    let appended = eventually("an open reader reads the appended line", || {
        let mut buffer = [0; 64];
        let read = log.read_at(&mut buffer, 7).expect("a read at the old end");
        (read > 0).then(|| buffer[..read].to_vec())
    });
    assert_eq!(appended, b"line 2\n");
}

/// Checks that a replacement through the client leaves an open reader on the
/// revision it opened, and that a new open reads the new revision.
fn check_replacement(runtime: &tokio::runtime::Runtime, writer: &Writer, mount: &Path) {
    let path = mount.join("replaced.bin");
    let old = pattern(REPLACED_BYTES, 2);
    let new = pattern(REPLACED_BYTES, 3);
    let reader = File::open(&path).expect("open");
    let mut head = vec![0; 4096];
    reader.read_exact_at(&mut head, 0).expect("a read");
    assert_eq!(head, old[..4096]);
    let old_inode = reader.metadata().expect("fstat").ino();

    let replace = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..PutFileOptions::default()
    };
    runtime
        .block_on(writer.client.put_file_with_options(
            &writer.path("/replaced.bin"),
            &new,
            &writer.actor,
            &replace,
        ))
        .expect("the replacement");
    eventually("a new open reads the new revision", || {
        (std::fs::read(&path).ok()? == new).then_some(())
    });
    assert_ne!(std::fs::metadata(&path).expect("stat").ino(), old_inode);

    let mut rest = vec![0; old.len() - head.len()];
    reader
        .read_exact_at(&mut rest, head.len() as u64)
        .expect("a read of the old revision");
    assert!(
        rest == old[head.len()..],
        "the open reader left its revision"
    );
    assert_eq!(reader.metadata().expect("fstat").len(), REPLACED_BYTES);
}
