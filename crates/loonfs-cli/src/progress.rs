//! Progress output for file transfers.
//!
//! Human-readable and JSON progress are written to standard error so standard
//! output remains available for results or file content. Event and field names
//! are part of the CLI contract.

use crate::args::RuntimeBehavior;
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use std::io::{IsTerminal, Write};
use std::sync::Mutex;

/// Minimum interval between progress reports.
const MIN_REPORT_INTERVAL_MS: u64 = 250;

/// How a running transfer is described, decided once per invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProgressMode {
    /// Redraw one line on standard error.
    Human,
    /// Write one JSON object per line to standard error.
    Events,
    /// Disable progress output.
    Off,
}

impl ProgressMode {
    /// Selects the progress mode from CLI flags and terminal detection.
    pub(crate) fn detect(no_progress: bool, json: bool) -> Self {
        if no_progress {
            Self::Off
        } else if json {
            Self::Events
        } else if std::io::stderr().is_terminal() {
            Self::Human
        } else {
            Self::Off
        }
    }

    /// Whether human-readable progress lines belong on standard error.
    /// Event mode writes structured JSON through [`ProgressReporter`].
    pub(crate) fn human_lines_enabled(self) -> bool {
        self == Self::Human
    }
}

/// Which transfer an event is about. The value an agent sees in `op`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProgressOp {
    Get,
    Put,
}

impl ProgressOp {
    fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
        }
    }
}

/// Current progress counters.
#[derive(Debug)]
struct ProgressState {
    bytes_done: u64,
    /// Bytes transferred during this run, excluding resumed bytes.
    bytes_moved: u64,
    bytes_total: Option<u64>,
    files_done: u64,
    files_total: Option<u64>,
    /// Most recently started file in a recursive transfer.
    current: Option<String>,
    /// Byte count included in the last report.
    reported_bytes: Option<u64>,
    reported_ms: u64,
    reported_percent: Option<u64>,
    /// Width of the current terminal line.
    drawn_width: usize,
}

impl ProgressState {
    /// Returns whether progress changed enough to report.
    fn due(&self, now_ms: u64) -> bool {
        if self.reported_bytes == Some(self.bytes_done) {
            return false;
        }
        if self.reported_bytes.is_none() {
            return true;
        }
        now_ms.saturating_sub(self.reported_ms) >= MIN_REPORT_INTERVAL_MS
            || self.percent() != self.reported_percent
    }

    fn percent(&self) -> Option<u64> {
        self.bytes_total.filter(|total| *total > 0).map(|total| {
            let done = self.bytes_done.min(total);
            done.saturating_mul(100) / total
        })
    }

    /// Returns whether this operation may cover more than one file.
    fn many_files(&self) -> bool {
        self.files_total.is_none_or(|total| total > 1)
    }
}

/// Counts one operation's bytes and says so, at a rate a human or an agent
/// can use.
///
/// Shared behind an `Arc` by recursive transfers, whose per-file jobs run
/// several at a time, so the accounting is one operation's and not one
/// file's. The lock is never held across an await.
#[derive(Debug)]
pub(crate) struct ProgressReporter {
    mode: ProgressMode,
    op: ProgressOp,
    /// What a `progress` event names: the file for a single transfer, the
    /// root for a tree.
    path: String,
    timer: StdMonotonicTimer,
    started_ms: u64,
    state: Mutex<ProgressState>,
}

impl ProgressReporter {
    /// A reporter for one operation over `path`, silent unless the
    /// invocation asked for progress.
    pub(crate) fn new(runtime: RuntimeBehavior, op: ProgressOp, path: impl Into<String>) -> Self {
        let timer = StdMonotonicTimer::default();
        let started_ms = timer.monotonic_now_ms();
        Self {
            mode: runtime.progress,
            op,
            path: path.into(),
            timer,
            started_ms,
            state: Mutex::new(ProgressState {
                bytes_done: 0,
                bytes_moved: 0,
                bytes_total: None,
                files_done: 0,
                files_total: None,
                current: None,
                reported_bytes: None,
                reported_ms: started_ms,
                reported_percent: None,
                drawn_width: 0,
            }),
        }
    }

    /// Whether anything is listening. Callers skip the accounting entirely
    /// when nothing is.
    pub(crate) fn enabled(&self) -> bool {
        self.mode != ProgressMode::Off
    }

    /// States the size of what is about to move, when it is knowable.
    pub(crate) fn expect(&self, bytes_total: Option<u64>, files_total: Option<u64>) {
        if !self.enabled() {
            return;
        }
        let mut state = self.lock();
        state.bytes_total = bytes_total;
        state.files_total = files_total;
    }

    /// What this operation has moved so far.
    pub(crate) fn bytes_done(&self) -> u64 {
        self.lock().bytes_done
    }

    /// Credits bytes a previous, interrupted run already moved.
    ///
    /// They count toward the total and the percentage, because they are
    /// part of the file that will be there at the end. They do not count
    /// toward the rate: this run did not move them, and pretending it did
    /// would make the first estimate of what is left a fantasy.
    pub(crate) fn already_done(&self, bytes: u64) {
        if !self.enabled() || bytes == 0 {
            return;
        }
        let mut state = self.lock();
        state.bytes_done = state.bytes_done.saturating_add(bytes);
    }

    /// Adds bytes a transfer just moved, reporting when a report is due.
    pub(crate) fn advance(&self, bytes: u64) {
        if !self.enabled() {
            return;
        }
        let now_ms = self.timer.monotonic_now_ms();
        let mut state = self.lock();
        state.bytes_done = state.bytes_done.saturating_add(bytes);
        state.bytes_moved = state.bytes_moved.saturating_add(bytes);
        if state.due(now_ms) {
            self.report(&mut state, now_ms);
        }
    }

    /// One file of a tree started moving.
    pub(crate) fn file_started(&self, path: &str, bytes_total: Option<u64>) {
        if !self.enabled() {
            return;
        }
        if self.mode == ProgressMode::Human {
            // Several files of a tree are in flight at once, so the line
            // names the one that started last: enough to see the transfer
            // moving through the tree without pretending it is serial.
            self.lock().current = Some(path.to_owned());
            return;
        }
        self.emit_event(&Event::File {
            kind: "file_started",
            op: self.op.as_str(),
            path,
            bytes_total,
            bytes_done: None,
            elapsed_ms: self.elapsed_ms(),
        });
    }

    /// One file of a tree finished, with what it actually moved.
    pub(crate) fn file_finished(&self, path: &str, bytes_done: u64) {
        if !self.enabled() {
            return;
        }
        {
            let mut state = self.lock();
            state.files_done = state.files_done.saturating_add(1);
        }
        if self.mode == ProgressMode::Events {
            self.emit_event(&Event::File {
                kind: "file_finished",
                op: self.op.as_str(),
                path,
                bytes_total: None,
                bytes_done: Some(bytes_done),
                elapsed_ms: self.elapsed_ms(),
            });
        }
    }

    /// A stretch where time passes and bytes do not — the commit a finished
    /// upload waits on, or the head start a resumed download begins from.
    /// Emitted at the transition, not on a timer.
    ///
    /// A transfer of several files reports none of these. Each file of a
    /// tree passes through the same stretches, several at a time, so one
    /// file entering its commit says nothing about where the operation is —
    /// and on a terminal it would leave the line flickering between the two.
    pub(crate) fn phase(&self, phase: &str) {
        if !self.enabled() {
            return;
        }
        let mut state = self.lock();
        if state.many_files() {
            return;
        }
        let now_ms = self.timer.monotonic_now_ms();
        match self.mode {
            ProgressMode::Events => self.emit_event(&Event::Phase {
                kind: "phase",
                op: self.op.as_str(),
                path: &self.path,
                phase,
                bytes_done: state.bytes_done,
                elapsed_ms: now_ms.saturating_sub(self.started_ms),
            }),
            ProgressMode::Human => {
                let line = format!("{} {} {phase}...", self.op.as_str(), self.path);
                self.draw(&mut state, &line);
            }
            ProgressMode::Off => {}
        }
    }

    /// Ends the operation: a last report of where it got to, and a terminal
    /// left as clean as it was found.
    pub(crate) fn finish(&self) {
        if !self.enabled() {
            return;
        }
        let now_ms = self.timer.monotonic_now_ms();
        let mut state = self.lock();
        if self.mode == ProgressMode::Events {
            self.report(&mut state, now_ms);
            return;
        }
        self.erase(&mut state);
    }

    fn elapsed_ms(&self) -> u64 {
        self.timer
            .monotonic_now_ms()
            .saturating_sub(self.started_ms)
    }

    /// Recovers a poisoned lock rather than failing a transfer over it: the
    /// state behind it is a byte count nobody's correctness rests on.
    fn lock(&self) -> std::sync::MutexGuard<'_, ProgressState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn report(&self, state: &mut ProgressState, now_ms: u64) {
        let elapsed_ms = now_ms.saturating_sub(self.started_ms);
        let rate_bps = rate_bps(state.bytes_moved, elapsed_ms);
        match self.mode {
            ProgressMode::Events => self.emit_event(&Event::Progress {
                kind: "progress",
                op: self.op.as_str(),
                path: &self.path,
                bytes_done: state.bytes_done,
                bytes_total: state.bytes_total,
                files_done: state.files_done,
                files_total: state.files_total,
                rate_bps,
                elapsed_ms,
            }),
            ProgressMode::Human => {
                let line = self.human_line(state, rate_bps);
                self.draw(state, &line);
            }
            ProgressMode::Off => return,
        }
        state.reported_bytes = Some(state.bytes_done);
        state.reported_ms = now_ms;
        state.reported_percent = state.percent();
    }

    fn human_line(&self, state: &ProgressState, rate_bps: u64) -> String {
        let tree = state.many_files();
        let mut line = format!("{} {}", self.op.as_str(), self.path);
        if let Some(files_total) = state.files_total.filter(|_| tree) {
            line.push_str(&format!("  {}/{files_total} files", state.files_done));
        }
        line.push_str(&format!("  {}", byte_size(state.bytes_done)));
        match (state.bytes_total, state.percent()) {
            (Some(total), Some(percent)) => {
                line.push_str(&format!("/{}  {percent}%", byte_size(total)));
            }
            _ => line.push_str("  (size unknown)"),
        }
        line.push_str(&format!("  {}/s", byte_size(rate_bps)));
        if let Some(eta) = eta_seconds(state.bytes_done, state.bytes_total, rate_bps) {
            line.push_str(&format!("  eta {}", clock(eta)));
        }
        if let Some(current) = state.current.as_deref().filter(|_| tree) {
            line.push_str(&format!("  {current}"));
        }
        line
    }

    /// Redraws the one line in place, covering whatever the last one left.
    fn draw(&self, state: &mut ProgressState, line: &str) {
        let width = line.chars().count();
        let padding = state.drawn_width.saturating_sub(width);
        let _ = write!(
            std::io::stderr().lock(),
            "\r{line}{:padding$}",
            "",
            padding = padding
        );
        let _ = std::io::stderr().lock().flush();
        state.drawn_width = width;
    }

    fn erase(&self, state: &mut ProgressState) {
        if state.drawn_width == 0 {
            return;
        }
        let width = state.drawn_width;
        let _ = write!(std::io::stderr().lock(), "\r{:width$}\r", "", width = width);
        let _ = std::io::stderr().lock().flush();
        state.drawn_width = 0;
    }

    fn emit_event(&self, event: &Event<'_>) {
        let Ok(line) = serde_json::to_string(event) else {
            return;
        };
        let _ = writeln!(std::io::stderr().lock(), "{line}");
    }
}

/// The event vocabulary, as agents parse it off standard error.
#[derive(serde::Serialize)]
#[serde(untagged)]
enum Event<'a> {
    Progress {
        kind: &'static str,
        op: &'static str,
        path: &'a str,
        bytes_done: u64,
        bytes_total: Option<u64>,
        files_done: u64,
        files_total: Option<u64>,
        rate_bps: u64,
        elapsed_ms: u64,
    },
    File {
        kind: &'static str,
        op: &'static str,
        path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        bytes_total: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        bytes_done: Option<u64>,
        elapsed_ms: u64,
    },
    Phase {
        kind: &'static str,
        op: &'static str,
        path: &'a str,
        phase: &'a str,
        bytes_done: u64,
        elapsed_ms: u64,
    },
}

/// Whole bytes a second, averaged over the transfer so far. Zero until a
/// millisecond has passed, which is the only honest answer before then.
fn rate_bps(bytes_done: u64, elapsed_ms: u64) -> u64 {
    if elapsed_ms == 0 {
        return 0;
    }
    bytes_done.saturating_mul(1_000) / elapsed_ms
}

/// Seconds left at the current average rate, when both the total and the
/// rate are known.
fn eta_seconds(bytes_done: u64, bytes_total: Option<u64>, rate_bps: u64) -> Option<u64> {
    let total = bytes_total?;
    if rate_bps == 0 {
        return None;
    }
    Some(total.saturating_sub(bytes_done) / rate_bps)
}

/// Binary units, three significant figures, as every transfer tool spells
/// them.
fn byte_size(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// `MM:SS`, or `HH:MM:SS` once there is an hour to show.
fn clock(seconds: u64) -> String {
    let (hours, minutes, seconds) = (seconds / 3_600, (seconds / 60) % 60, seconds % 60);
    if hours > 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes}:{seconds:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(bytes_done: u64, bytes_total: Option<u64>) -> ProgressState {
        ProgressState {
            bytes_done,
            bytes_moved: bytes_done,
            bytes_total,
            files_done: 0,
            files_total: None,
            current: None,
            reported_bytes: None,
            reported_ms: 0,
            reported_percent: None,
            drawn_width: 0,
        }
    }

    fn silent_reporter(op: ProgressOp, path: &str) -> ProgressReporter {
        ProgressReporter::new(
            RuntimeBehavior {
                json: false,
                no_input: false,
                interactive: false,
                progress: ProgressMode::Off,
            },
            op,
            path,
        )
    }

    #[test]
    fn a_report_waits_for_the_interval_or_a_new_percentage() {
        let mut state = ProgressState {
            bytes_done: 10,
            bytes_moved: 10,
            bytes_total: Some(1_000),
            files_done: 0,
            files_total: None,
            current: None,
            reported_bytes: None,
            reported_ms: 0,
            reported_percent: None,
            drawn_width: 0,
        };
        assert!(state.due(0), "the first report never waits");

        state.reported_bytes = Some(10);
        state.reported_percent = Some(1);
        assert!(
            !state.due(10_000),
            "a report that would repeat itself is skipped however long it has been"
        );

        state.bytes_done = 11;
        assert!(
            !state.due(0),
            "bytes inside the same percent wait for the interval"
        );
        assert!(state.due(MIN_REPORT_INTERVAL_MS));

        state.bytes_done = 20;
        assert!(state.due(0), "a new whole percent does not wait");
    }

    #[test]
    fn an_unknown_total_has_no_percentage_and_no_eta() {
        assert_eq!(state(10, None).percent(), None);
        assert_eq!(eta_seconds(10, None, 1_000), None);
    }

    #[test]
    fn a_terminal_line_says_what_a_watcher_needs() {
        let one_file = silent_reporter(ProgressOp::Get, "/docs/big.bin");
        let mut single = state(512 * 1_024, Some(1_024 * 1_024));
        single.files_total = Some(1);
        single.current = Some("/docs/big.bin".to_owned());
        assert_eq!(
            one_file.human_line(&single, 256 * 1_024),
            "get /docs/big.bin  512.0 KiB/1.0 MiB  50%  256.0 KiB/s  eta 0:02"
        );

        let tree = silent_reporter(ProgressOp::Put, "demo:/up");
        let mut many = state(300, Some(1_000));
        many.files_done = 2;
        many.files_total = Some(10);
        many.current = Some("/up/docs/a.txt".to_owned());
        assert_eq!(
            tree.human_line(&many, 100),
            "put demo:/up  2/10 files  300 B/1000 B  30%  100 B/s  eta 0:07  /up/docs/a.txt"
        );

        // A pipe has no total, so it has no percentage and nothing to
        // estimate — only what has gone by.
        let piped = silent_reporter(ProgressOp::Put, "/piped.bin");
        let mut unknown = state(2_048, None);
        unknown.files_total = Some(1);
        assert_eq!(
            piped.human_line(&unknown, 1_024),
            "put /piped.bin  2.0 KiB  (size unknown)  1.0 KiB/s"
        );
    }

    #[test]
    fn a_rate_needs_elapsed_time_and_an_eta_needs_a_rate() {
        assert_eq!(rate_bps(1_000, 0), 0);
        assert_eq!(rate_bps(1_000, 1_000), 1_000);
        assert_eq!(eta_seconds(0, Some(2_000), 0), None);
        assert_eq!(eta_seconds(500, Some(2_000), 500), Some(3));
    }

    #[test]
    fn sizes_and_clocks_read_the_way_transfer_tools_spell_them() {
        assert_eq!(byte_size(0), "0 B");
        assert_eq!(byte_size(1_023), "1023 B");
        assert_eq!(byte_size(1_024), "1.0 KiB");
        assert_eq!(byte_size(1_536), "1.5 KiB");
        assert_eq!(byte_size(5 * 1_024 * 1_024 * 1_024), "5.0 GiB");
        assert_eq!(clock(9), "0:09");
        assert_eq!(clock(75), "1:15");
        assert_eq!(clock(3_725), "1:02:05");
    }
}
