//! Monotonic timers, the wall clock, and duration-to-millisecond conversions.

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Supplies monotonic milliseconds for retry deadlines and deterministic test injection.
pub trait MonotonicTimer: std::fmt::Debug + Send + Sync {
    /// Returns milliseconds since an arbitrary per-timer origin.
    fn monotonic_now_ms(&self) -> u64;
}

/// Measures monotonic elapsed time, including host sleep and a freeze that the host can't see.
///
/// Each reading adds the boot-time clock's progress since the previous reading, which counts host
/// sleep. A reading also adds a forward step of the wall clock that the boot-time clock didn't
/// see: the wall clock's progress less the boot-time clock's, when that difference is at least
/// [`WALL_CLOCK_NOISE_MARGIN`]. So a freeze that the boot-time clock misses (a virtual machine
/// restored from a snapshot, or paused by its hypervisor) counts once the wall clock is corrected.
/// A wall clock that steps backward adds nothing and hides none of the boot-time clock's
/// progress. Every instance adds to one process-wide total, so readings from any two standard
/// timers are comparable.
#[derive(Debug, Default)]
pub struct StdMonotonicTimer(());

impl MonotonicTimer for StdMonotonicTimer {
    #[expect(
        clippy::disallowed_methods,
        reason = "the standard timer reads the system time to count a freeze the boot-time clock \
                  misses, and tests inject their own timer"
    )]
    fn monotonic_now_ms(&self) -> u64 {
        static ELAPSED: Mutex<Option<Elapsed>> = Mutex::new(None);
        let mut elapsed = ELAPSED
            .lock()
            .expect("the elapsed time lock should not be poisoned");
        let (boot, wall) = (monotonic_now(), SystemTime::now());
        let elapsed = elapsed.get_or_insert(Elapsed {
            boot,
            wall,
            total: Duration::ZERO,
        });
        saturating_duration_ms(elapsed.advance(boot, wall))
    }
}

/// The smallest amount by which the wall clock's progress between two readings can exceed the
/// boot-time clock's and count as a step of the wall clock.
///
/// A smaller difference is noise: the two clocks are read a moment apart, and that moment varies
/// from reading to reading. Adding every such difference would make the timer run fast in
/// proportion to how often it's read. The margin is far below the shortest budget that reads the
/// timer.
const WALL_CLOCK_NOISE_MARGIN: Duration = Duration::from_secs(1);

/// The standard timer's total elapsed time, and the clock readings it was last advanced to.
#[derive(Debug)]
struct Elapsed {
    boot: Duration,
    wall: SystemTime,
    total: Duration,
}

impl Elapsed {
    /// Adds the boot-time clock's progress since the previous readings, and a forward step of the
    /// wall clock that the boot-time clock didn't see, and returns the total.
    fn advance(&mut self, boot: Duration, wall: SystemTime) -> Duration {
        let boot_progress = boot.saturating_sub(self.boot);
        let wall_progress = wall.duration_since(self.wall).unwrap_or_default();
        let unseen = wall_progress.saturating_sub(boot_progress);
        self.total = self.total.saturating_add(boot_progress);
        if unseen >= WALL_CLOCK_NOISE_MARGIN {
            self.total = self.total.saturating_add(unseen);
        }
        self.boot = boot;
        self.wall = wall;
        self.total
    }
}

/// These clocks count host sleep, so publication budgets keep advancing where `Instant` would stop.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[expect(
    clippy::disallowed_methods,
    unsafe_code,
    reason = "the boot-time clock reads through libc, because Instant stops during host sleep"
)]
fn monotonic_now() -> Duration {
    #[cfg(target_os = "linux")]
    const CLOCK: libc::clockid_t = libc::CLOCK_BOOTTIME;
    #[cfg(target_vendor = "apple")]
    const CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC;

    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // The pointer refers to writable storage for one initialized timespec.
    let result = unsafe { libc::clock_gettime(CLOCK, &mut time) };
    assert_eq!(result, 0, "the monotonic clock should be available");
    Duration::new(
        u64::try_from(time.tv_sec).expect("monotonic seconds should be nonnegative"),
        u32::try_from(time.tv_nsec).expect("monotonic nanoseconds should fit u32"),
    )
}

/// `Instant` stops during host sleep. That is adequate for the client's retry timing, and the
/// runtime refuses to build on these platforms.
#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
#[expect(
    clippy::disallowed_methods,
    reason = "this platform has no boot-time clock, so Instant is the fallback"
)]
fn monotonic_now() -> Duration {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(std::time::Instant::now);
    start.elapsed()
}

/// The wall clock a writer stamps into durable state.
pub trait WallClock: std::fmt::Debug + Send + Sync {
    /// Unix milliseconds now.
    fn now_ms(&self) -> u64;
}

/// The system wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    #[expect(
        clippy::disallowed_methods,
        reason = "the runtime's wall clock reads the system time, and tests inject their own clock"
    )]
    fn now_ms(&self) -> u64 {
        unix_ms(SystemTime::now())
    }
}

/// Converts a wall-clock instant to unix milliseconds.
fn unix_ms(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH)
        .map(|elapsed| {
            u64::try_from(elapsed.as_millis()).expect("unix milliseconds should fit u64")
        })
        .expect(
            "the system clock should read after the unix epoch, which the runtime builder \
             checks with `system_clock_reads_after_epoch` before it starts",
        )
}

/// Converts a duration to whole milliseconds, saturating at `u64::MAX`.
pub fn saturating_duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Converts a duration to whole milliseconds at compile time. A value past
/// `u64::MAX` milliseconds fails the build instead of truncating.
pub const fn duration_ms(duration: Duration) -> u64 {
    let seconds_ms = duration
        .as_secs()
        .checked_mul(1_000)
        .expect("the duration should fit u64 milliseconds");
    seconds_ms + duration.subsec_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_timers_created_at_different_times_read_on_one_scale() {
        let first = StdMonotonicTimer::default();
        let started_at_ms = first.monotonic_now_ms();
        let mut earlier_ms = started_at_ms;
        while earlier_ms == started_at_ms {
            earlier_ms = first.monotonic_now_ms();
        }
        let second = StdMonotonicTimer::default();
        assert!(second.monotonic_now_ms() >= earlier_ms);
    }

    const DAY: Duration = Duration::from_secs(24 * 60 * 60);
    const HALF_HOUR: Duration = Duration::from_secs(30 * 60);

    fn elapsed() -> Elapsed {
        Elapsed {
            boot: Duration::ZERO,
            wall: UNIX_EPOCH,
            total: Duration::ZERO,
        }
    }

    #[test]
    fn a_backward_correction_after_a_forward_jump_hides_no_elapsed_time() {
        let mut elapsed = elapsed();
        let started = elapsed.advance(Duration::ZERO, UNIX_EPOCH + DAY);
        assert_eq!(started, DAY, "the forward jump counts in full");
        // Half an hour passes, and the wall clock is set back by a day.
        assert_eq!(
            elapsed.advance(HALF_HOUR, UNIX_EPOCH + HALF_HOUR) - started,
            HALF_HOUR
        );
    }

    #[test]
    fn a_freeze_after_a_forward_jump_and_a_backward_correction_counts() {
        let mut elapsed = elapsed();
        let second = Duration::from_secs(1);
        elapsed.advance(second, UNIX_EPOCH + DAY + second);
        elapsed.advance(2 * second, UNIX_EPOCH + 2 * second);
        // The wall clock moves half an hour that the boot-time clock doesn't see.
        assert_eq!(
            elapsed.advance(3 * second, UNIX_EPOCH + 3 * second + HALF_HOUR),
            DAY + 3 * second + HALF_HOUR
        );
    }

    #[test]
    fn a_wall_clock_jump_backward_adds_only_the_boot_time_clocks_progress() {
        let mut elapsed = elapsed();
        let earlier = UNIX_EPOCH - DAY;
        assert_eq!(
            elapsed.advance(Duration::from_millis(3), earlier),
            Duration::from_millis(3)
        );
        assert_eq!(
            elapsed.advance(Duration::from_millis(5), earlier + Duration::from_millis(2)),
            Duration::from_millis(5)
        );
    }

    #[test]
    fn small_differences_between_the_clocks_add_nothing() {
        let mut elapsed = elapsed();
        let step = Duration::from_micros(100);
        let mut total = Duration::ZERO;
        for reading in 1..=10_000_u32 {
            // The wall clock is read up to 99 microseconds after the boot-time clock.
            let lag = Duration::from_micros(u64::from(reading * 37 % 100));
            total = elapsed.advance(reading * step, UNIX_EPOCH + reading * step + lag);
        }
        assert_eq!(total, 10_000 * step);
    }

    #[test]
    fn the_wall_clock_counts_milliseconds_from_the_unix_epoch() {
        assert_eq!(unix_ms(UNIX_EPOCH + Duration::from_millis(1)), 1);
    }
}
