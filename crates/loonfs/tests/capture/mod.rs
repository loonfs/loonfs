//! The tracing capture of the tracing test binaries. Each binary includes
//! this file with `mod capture;` and stays its own process, for the reason
//! `tracing_capture.rs` gives.

use loonfs::{LoonFs, StoreConfig, Writable};
use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::fmt::format::FmtSpan;

struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("capture lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Installs a debug-level subscriber on this thread that writes every event
/// and every span close into the returned buffer, as JSON lines when `json`
/// is set. The subscriber is thread-local, so the capture can't leak into
/// other tests, and it stays installed until the guard drops.
pub(crate) fn capture(json: bool) -> (Arc<Mutex<Vec<u8>>>, DefaultGuard) {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        // Closing spans are recorded so a span names itself in the capture
        // whether or not it emitted an event.
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(move || CaptureWriter(Arc::clone(&sink)));
    let guard = if json {
        tracing::subscriber::set_default(subscriber.json().finish())
    } else {
        tracing::subscriber::set_default(subscriber.finish())
    };
    (captured, guard)
}

/// Returns the log captured since the last call and empties the buffer.
pub(crate) fn take_log(captured: &Arc<Mutex<Vec<u8>>>) -> String {
    let bytes = std::mem::take(&mut *captured.lock().expect("capture lock"));
    String::from_utf8(bytes).expect("captured log is utf8")
}

/// Builds a writer as `writer_id` on a local store at `root`.
pub(crate) async fn writer(root: &Path, writer_id: &str) -> LoonFs<Writable> {
    LoonFs::builder(StoreConfig::LocalFs(LocalFsStoreConfig {
        root: root.to_string_lossy().into_owned(),
        key_prefix: None,
    }))
    .writer_id(loonfs_test_support::ids::writer_id(writer_id))
    .build()
    .await
    .expect("build writer")
}

/// Returns the first line of `log` that contains `needle`.
#[allow(
    dead_code,
    reason = "`operation_spans.rs` parses its JSON lines instead"
)]
pub(crate) fn find_line<'log>(log: &'log str, needle: &str) -> &'log str {
    log.lines()
        .find(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("`{needle}` missing from captured tracing:\n{log}"))
}
