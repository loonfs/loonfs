//! Human-readable text for the results of `loonfs mount`, `unmount`,
//! `status`, and `refresh`.

use loonfs_mount::daemon::protocol::{
    MountOutcome, MountStatusReport, MountedReport, StateDisposal, UnmountOutcome, UnmountedReport,
};
use loonfs_mount::{MountAccess, MountMethod, Readiness, ReflinkSupport, Status};
use loonfs_types::ChangeSeq;
use std::fmt::Write as _;
use std::path::Path;

pub(super) fn human_mounted(report: &MountedReport) -> String {
    let directory = report.mount.directory.display();
    let target = target(&report.mount.status);
    match report.outcome {
        MountOutcome::Mounted => format!(
            "mounted {target} at {directory} ({}), at seq {}",
            access(report.mount.status.access),
            report.mount.status.applied_seq.0
        ),
        MountOutcome::AlreadyMounted => format!("{directory} already has the mount of {target}"),
    }
}

pub(super) fn human_unmounted(report: &UnmountedReport) -> String {
    let directory = report.directory.display();
    let state = report.state_directory.display();
    match report.outcome {
        UnmountOutcome::Unmounted {
            state: StateDisposal::Removed,
        } => format!("unmounted {directory} and removed its state"),
        UnmountOutcome::Unmounted {
            state: StateDisposal::Kept,
        } => {
            format!("unmounted {directory}; its state stays in {state} for a later `loonfs mount`")
        }
        UnmountOutcome::Unmounted {
            state: StateDisposal::RemovedAfterRelease,
        } => format!(
            "detached {directory}; programs still have files open in it, and its state is \
             removed once they close them"
        ),
        UnmountOutcome::DetachedStale => format!(
            "detached {directory}, whose mount's daemon had stopped; its state stays in {state} \
             for a later `loonfs mount`"
        ),
        UnmountOutcome::NotMounted => format!("{directory} has no mount"),
    }
}

pub(super) fn human_refreshed(directory: &Path, applied_seq: ChangeSeq) -> String {
    format!(
        "{} has applied the namespace through seq {}",
        directory.display(),
        applied_seq.0
    )
}

pub(super) fn human_mount_statuses(mounts: &[MountStatusReport]) -> String {
    if mounts.is_empty() {
        return "no mounts".to_owned();
    }
    let mut text = String::new();
    for (index, mount) in mounts.iter().enumerate() {
        if index > 0 {
            text.push('\n');
        }
        write_mount_status(&mut text, &mount.directory, &mount.status);
    }
    text
}

fn write_mount_status(text: &mut String, directory: &Path, status: &Status) {
    let readiness = match status.readiness {
        Readiness::Ready => "ready",
        Readiness::Rebuilding => "rebuilding (listing the namespace again)",
    };
    let method = match status.mount_method {
        Some(MountMethod::MountSyscall) => "mount(2)",
        Some(MountMethod::Fusermount3) => "fusermount3",
        None => "-",
    };
    let reflinks = match status.reflinks {
        ReflinkSupport::Supported => "supported",
        ReflinkSupport::Unsupported => "unsupported",
    };
    let cache = &status.cache;
    let _ = writeln!(text, "{}", directory.display());
    let rows = [
        ("namespace", target(status)),
        ("access", access(status.access).to_owned()),
        ("state", readiness.to_owned()),
        ("mounted with", method.to_owned()),
        ("applied seq", status.applied_seq.0.to_string()),
        (
            "entries",
            format!("{} (budget {})", status.entries, status.max_entries),
        ),
        (
            "cache",
            format!(
                "{} evictable bytes (target {}), {} pinned bytes (limit {})",
                cache.evictable_bytes,
                cache.max_cache_bytes,
                cache.pinned_bytes,
                cache.max_pinned_bytes
            ),
        ),
        ("owner", format!("uid {}, gid {}", status.uid, status.gid)),
        ("state dir", status.state_directory.display().to_string()),
        (
            "free space",
            status
                .available_bytes
                .map_or_else(|| "-".to_owned(), |bytes| format!("{bytes} bytes")),
        ),
        ("reflinks", reflinks.to_owned()),
    ];
    for (label, value) in rows {
        let _ = writeln!(text, "  {label:<13}{value}");
    }
    if let Some(error) = &status.feed_error {
        let _ = writeln!(text, "  {:<13}{error}", "feed error");
    }
    if let Some(stop) = &status.publication_stop {
        let _ = writeln!(text, "  {:<13}{stop:?}", "stopped");
    }
    for warning in &status.warnings {
        let _ = writeln!(text, "  {:<13}{warning:?}", "warning");
    }
}

fn target(status: &Status) -> String {
    crate::commands::render_target(&status.namespace_id, &status.path)
}

fn access(access: MountAccess) -> &'static str {
    match access {
        MountAccess::ReadOnly => "read-only",
        MountAccess::Writable => "writable",
    }
}
