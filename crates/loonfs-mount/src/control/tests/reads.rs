//! Reads: verification, the second fetch after a mismatch, and the grant
//! streams that read handles keep.

use super::*;

/// Opens a read handle on `name` in the root.
fn open(task: &mut ControlTask, shared: &SharedState, name: &str) -> OpenedRead {
    let item = shared
        .tree
        .read()
        .expect("the tree lock")
        .lookup(ItemId::ROOT, OsStr::new(name))
        .expect("the file is shown")
        .item;
    let (reply, opened) = oneshot::channel();
    task.handle(ControlMessage::OpenRead { item, reply });
    opened.blocking_recv().expect("a reply").expect("a handle")
}

fn size_of(opened: &OpenedRead) -> u64 {
    opened
        .view
        .lock()
        .expect("the view lock")
        .revision
        .content_ref
        .size_bytes
}

/// Sends a read of every byte of the handle's revision.
fn read_all(task: &mut ControlTask, opened: &OpenedRead) -> PendingRead {
    let size = size_of(opened);
    let (reply, read) = oneshot::channel();
    task.handle(ControlMessage::Read {
        handle: opened.handle,
        demand: 0..size,
        needed: 0..size,
        reply,
    });
    read
}

fn only_fetch(work: Vec<Work>) -> FetchJob {
    let [Work::Fetch(job)] = &work[..] else {
        panic!("one fetch should start: {work:?}");
    };
    job.clone()
}

/// Sends the outcome of the one verification that `work` starts.
fn verify(task: &mut ControlTask, work: Vec<Work>, result: Verification) {
    let [Work::Verify(job)] = &work[..] else {
        panic!("one verification should start: {work:?}");
    };
    task.handle(done(WorkOutcome::Verified {
        content_id: job.content_id.clone(),
        file: job.file.clone(),
        target: job.target.clone(),
        result: Ok(result),
    }));
}

/// A read whose reply hasn't been received yet.
type PendingRead = oneshot::Receiver<crate::error::Result<Arc<CacheFile>>>;

/// Opens `a.txt` and reads all of it, which returns its bytes before they're
/// verified, and then fails their verification. Returns the handle, a read
/// sent after the mismatch, and the fetch that starts again.
fn read_and_mismatch() -> (
    tempfile::TempDir,
    ControlTask,
    OpenedRead,
    PendingRead,
    RecordedWork,
) {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, shared, workers) = task(directory.path(), &[file(11, 10, "/project/a.txt")]);
    task.start();
    workers.take();
    let opened = open(&mut task, &shared, "a.txt");
    let size = size_of(&opened);
    let read = |task: &mut ControlTask| read_all(task, &opened);

    let first = read(&mut task);
    let fetch = only_fetch(workers.take());
    assert_eq!(fetch.transport, FetchTransport::WholeFile);
    task.handle(done(WorkOutcome::Fetched {
        fetch_id: fetch.fetch_id,
        result: Ok(Fetched { written: 0..size }),
    }));
    let first = first.blocking_recv().expect("a reply");
    assert!(
        first.is_ok(),
        "a read returns bytes before they're verified"
    );
    verify(&mut task, workers.take(), Verification::Mismatch);
    let refetch = only_fetch(workers.take());
    assert_eq!(refetch.range, 0..size);

    let mut after_mismatch = read(&mut task);
    assert!(
        workers.take().is_empty(),
        "the refetch fetches every byte already"
    );
    task.handle(done(WorkOutcome::Fetched {
        fetch_id: refetch.fetch_id,
        result: Ok(Fetched { written: 0..size }),
    }));
    assert!(
        matches!(
            after_mismatch.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ),
        "a read after a mismatch waits until the second fetch matches"
    );
    let file = opened.view.lock().expect("the view lock").file.clone();
    assert_eq!(
        file.map(|file| file.present_prefix()),
        Some(0),
        "a read handle has none of the second fetch's bytes"
    );
    (directory, task, opened, after_mismatch, workers)
}

/// Checks that a read that arrives after a mismatch waits for the second
/// fetch to match, and then returns its bytes.
#[test]
fn a_read_after_a_mismatch_waits_for_the_second_fetch() {
    let (_directory, mut task, opened, read, workers) = read_and_mismatch();
    verify(&mut task, workers.take(), Verification::Matches);
    let file = read.blocking_recv().expect("a reply").expect("the bytes");
    let size = opened
        .view
        .lock()
        .expect("the view lock")
        .revision
        .content_ref
        .size_bytes;
    assert_eq!(file.present_prefix(), size);
    assert_eq!(task.mismatched_files, 1);
}

/// Checks that a read that arrives after a mismatch fails with a content
/// mismatch, `EIO`, when the second fetch doesn't match either.
#[test]
fn a_read_after_a_mismatch_fails_when_the_second_fetch_mismatches() {
    let (_directory, mut task, _opened, read, workers) = read_and_mismatch();
    verify(&mut task, workers.take(), Verification::Mismatch);
    let result = read.blocking_recv().expect("a reply");
    assert!(
        matches!(result, Err(MountError::ContentMismatch { .. })),
        "{result:?}"
    );
    assert_eq!(task.mismatched_files, 2);
    assert!(
        workers.take().is_empty(),
        "a file is fetched again only once"
    );
}

/// Checks that the grant stream of a read handle that closes while its
/// grant fetch runs is dropped: the fetch keeps the stream for the handle
/// after the drop that the close sent, so the end of the fetch drops it
/// again.
#[test]
fn a_handle_that_closes_during_its_grant_fetch_drops_the_stream() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, shared, workers) = task_with(
        directory.path(),
        &[file(11, 10, "/project/a.txt")],
        |mount| {
            mount.config.max_whole_file_bytes = 1;
            mount.config.max_ranged_read_bytes = 4;
            mount.transports.direct_get = true;
        },
    );
    task.start();
    workers.take();
    let opened = open(&mut task, &shared, "a.txt");
    let size = size_of(&opened);
    let _read = read_all(&mut task, &opened);
    let fetch = only_fetch(workers.take());
    assert_eq!(fetch.transport, FetchTransport::Grant);
    assert_eq!(fetch.stream_owner, Some(opened.handle));

    task.handle(ControlMessage::CloseRead {
        handle: opened.handle,
    });
    let dropped = |work: &[Work]| {
        work.iter()
            .filter(|work| matches!(work, Work::DropStream { handle } if *handle == opened.handle))
            .count()
    };
    assert_eq!(dropped(&workers.take()), 1, "the close drops the stream");
    task.handle(done(WorkOutcome::Fetched {
        fetch_id: fetch.fetch_id,
        result: Ok(Fetched { written: 0..size }),
    }));
    assert_eq!(
        dropped(&workers.take()),
        1,
        "the end of the fetch drops the stream that it kept"
    );
}

/// Checks that the second fetch after a mismatch reads the file's own
/// target from a revision that has all of its bytes, when two handles read
/// the same content at revisions of two sizes, and not the bytes of the
/// handle with the highest revision number.
#[test]
fn the_second_fetch_reads_the_files_target_from_a_revision_that_covers_it() {
    let directory = tempfile::tempdir().expect("a state directory");
    let content_id = ContentId::generate();
    let content = |bytes: &[u8]| {
        ContentRef::blob_v1(
            namespace_id(),
            content_id.clone(),
            bytes,
            ChecksumAlgorithm::Crc64nvme,
        )
    };
    let (mut task, shared, workers) = task(
        directory.path(),
        &[
            file_with(
                11,
                10,
                "/project/long.txt",
                RevisionNo(1),
                content(b"0123456789abcd"),
            ),
            file_with(
                12,
                10,
                "/project/short.txt",
                RevisionNo(5),
                content(b"0123456789"),
            ),
        ],
    );
    task.start();
    workers.take();
    let long = open(&mut task, &shared, "long.txt");
    let _short = open(&mut task, &shared, "short.txt");
    let _read = read_all(&mut task, &long);
    let fetch = only_fetch(workers.take());
    task.handle(done(WorkOutcome::Fetched {
        fetch_id: fetch.fetch_id,
        result: Ok(Fetched { written: 0..14 }),
    }));
    verify(&mut task, workers.take(), Verification::Mismatch);
    let refetch = only_fetch(workers.take());
    assert_eq!(refetch.inode_id, InodeId(11));
    assert_eq!(refetch.range, 0..14);
}

/// Checks that a read that waits for a cache file fails when the file is
/// removed, instead of waiting for bytes that no fetch writes.
#[test]
fn a_read_that_waits_for_a_removed_file_fails() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, shared, workers) = task(directory.path(), &[file(11, 10, "/project/a.txt")]);
    task.start();
    workers.take();
    let opened = open(&mut task, &shared, "a.txt");
    let mut read = read_all(&mut task, &opened);
    only_fetch(workers.take());
    let content_id = opened
        .view
        .lock()
        .expect("the view lock")
        .revision
        .content_ref
        .content_id
        .clone();
    task.remove_cache_file(&content_id);
    task.publish_and_deliver();
    let result = read.try_recv().expect("a reply");
    assert!(
        matches!(result, Err(MountError::CacheFileRemoved { .. })),
        "{result:?}"
    );
}

/// Checks that the waiting reads shrink when their owners go away: a read
/// whose caller stopped waiting leaves when the next read of the same
/// content arrives, and a handle's reads leave when it closes.
#[test]
fn waiting_reads_leave_with_their_callers_and_handles() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, shared, workers) = task(directory.path(), &[file(11, 10, "/project/a.txt")]);
    task.start();
    workers.take();
    let opened = open(&mut task, &shared, "a.txt");
    let abandoned = read_all(&mut task, &opened);
    drop(abandoned);
    let _waiting = read_all(&mut task, &opened);
    assert_eq!(task.read_waiters.len(), 1, "the abandoned read left");
    task.handle(ControlMessage::CloseRead {
        handle: opened.handle,
    });
    assert_eq!(task.read_waiters.len(), 0, "the closed handle's read left");
}
