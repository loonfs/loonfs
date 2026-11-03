//! The raises of namespace hints that follow this runtime's manifest
//! publications: at most one raise of each hint per `HINT_RAISE_INTERVAL_MS`,
//! and one trailing raise after an interval that skipped raises.

use super::registry::RegistryShared;
use super::{lock_state, sleep_ms};
use loonfs_core::limits::HINT_RAISE_INTERVAL_MS;
use loonfs_types::{ManifestNo, MonotonicTimer, NamespaceId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

/// The raises of namespace hints that this runtime started, so that the
/// runtime raises each namespace's hint at most once per
/// `HINT_RAISE_INTERVAL_MS` (format spec, "The current manifest and WAL
/// tip").
#[derive(Default)]
pub(super) struct HintRaises {
    /// The last raise of each namespace's hint that started less than
    /// `HINT_RAISE_INTERVAL_MS` ago or that a trailing raise follows. Each
    /// start removes every entry that is neither, so the map lists at most
    /// the namespaces whose hints this runtime raised in the last interval or
    /// still has to raise when an interval ends.
    raises: Mutex<HashMap<NamespaceId, HintRaise>>,
}

/// The last raise of one namespace's hint that this runtime started.
struct HintRaise {
    /// When the raise started, on the monotonic timer.
    started_at_ms: u64,
    /// The highest manifest number among the raises skipped since this raise
    /// started, or `None` when the interval skipped no raise. The trailing
    /// raise raises the hint to this number once the interval ends.
    trailing: Option<ManifestNo>,
}

impl HintRaise {
    fn starting_at(now_ms: u64) -> Self {
        Self {
            started_at_ms: now_ms,
            trailing: None,
        }
    }

    fn ends_at_ms(&self) -> u64 {
        self.started_at_ms.saturating_add(HINT_RAISE_INTERVAL_MS)
    }
}

/// What one raise of a namespace's hint does next.
enum NextRaise {
    /// Raises the hint to this manifest number now.
    Now(ManifestNo),
    /// Leaves the raise to the trailing raise, which starts once the
    /// interval ends at `due_at_ms` on the monotonic timer.
    Trailing { due_at_ms: u64 },
    /// Does nothing, because another raise covers this one.
    Covered,
}

impl HintRaises {
    /// Returns what a raise of the namespace's hint to `manifest_no` does
    /// at `now_ms` on the monotonic timer.
    ///
    /// The raise starts now, unless this runtime started a raise of the hint
    /// less than `HINT_RAISE_INTERVAL_MS` ago. In that case the interval's
    /// trailing raise covers it, and the first raise that the interval skips
    /// starts the trailing raise's wait. A raise that starts after the
    /// interval ends also covers a trailing raise that hasn't started yet.
    fn start(&self, namespace_id: &NamespaceId, manifest_no: ManifestNo, now_ms: u64) -> NextRaise {
        let mut raises = lock_state(&self.raises);
        raises.retain(|_, raise| raise.trailing.is_some() || now_ms < raise.ends_at_ms());
        if let Some(raise) = raises.get_mut(namespace_id) {
            if now_ms < raise.ends_at_ms() {
                let waiting = raise.trailing.is_some();
                raise.trailing = raise.trailing.max(Some(manifest_no));
                return if waiting {
                    NextRaise::Covered
                } else {
                    NextRaise::Trailing {
                        due_at_ms: raise.ends_at_ms(),
                    }
                };
            }
        }
        let trailing = raises
            .insert(namespace_id.clone(), HintRaise::starting_at(now_ms))
            .and_then(|raise| raise.trailing);
        NextRaise::Now(trailing.map_or(manifest_no, |trailing| trailing.max(manifest_no)))
    }

    /// Returns what the trailing raise of the interval that ends at
    /// `due_at_ms` does at `now_ms`.
    ///
    /// The trailing raise starts once the interval has ended, unless a raise
    /// that started after the interval ended has covered it.
    fn start_trailing(&self, namespace_id: &NamespaceId, due_at_ms: u64, now_ms: u64) -> NextRaise {
        let mut raises = lock_state(&self.raises);
        match raises.get_mut(namespace_id) {
            Some(raise) if raise.ends_at_ms() == due_at_ms => match raise.trailing {
                Some(_) if now_ms < due_at_ms => NextRaise::Trailing { due_at_ms },
                Some(manifest_no) => {
                    *raise = HintRaise::starting_at(now_ms);
                    NextRaise::Now(manifest_no)
                }
                None => NextRaise::Covered,
            },
            _ => NextRaise::Covered,
        }
    }
}

impl RegistryShared {
    /// Raises the namespace's hint to `manifest_no`, a manifest that this
    /// runtime published, with one compare-and-swap, unless this runtime
    /// started a raise of the namespace's hint less than
    /// `HINT_RAISE_INTERVAL_MS` ago (format spec, "The current manifest and
    /// WAL tip").
    ///
    /// When the raise is skipped, this returns at once, and the interval's
    /// trailing raise raises the hint once the interval ends, to the highest
    /// manifest number among the raises that the interval skipped. A failed
    /// raise is logged and ignored, and leaves the hint behind the newest
    /// manifest until a later publication raises it. Discovery probes past a
    /// hint that lags.
    pub(super) async fn raise_hint(
        self: &Arc<Self>,
        namespace_id: &NamespaceId,
        manifest_no: ManifestNo,
    ) {
        let timer = self.context.runtime_core.timer();
        match self
            .hint_raises
            .start(namespace_id, manifest_no, timer.monotonic_now_ms())
        {
            NextRaise::Now(manifest_no) => self.raise_hint_now(namespace_id, manifest_no).await,
            NextRaise::Trailing { due_at_ms } => {
                // The registry's runtime runs the trailing raise, as it runs
                // the session tasks. `LoonFs::drain` doesn't wait for it,
                // because a drain would then wait out the interval after
                // every burst of publications, and the hint is a best-effort
                // pointer.
                self.context.runtime.spawn(run_trailing_raise(
                    Arc::downgrade(self),
                    Arc::clone(timer),
                    namespace_id.clone(),
                    due_at_ms,
                ));
            }
            NextRaise::Covered => {}
        }
    }

    async fn raise_hint_now(&self, namespace_id: &NamespaceId, manifest_no: ManifestNo) {
        if let Err(error) = self
            .context
            .runtime_core
            .namespace_store(namespace_id)
            .raise_hint(manifest_no, None)
            .await
        {
            tracing::debug!(
                namespace_id = %namespace_id,
                manifest_no = %manifest_no,
                error = %error,
                "the namespace hint raise failed, and a later publication raises the hint"
            );
        }
    }
}

/// Waits until `due_at_ms` on `timer`, and then starts the namespace's
/// trailing raise unless a later raise covered it.
///
/// The raise needs no writer session, so closing the namespace's session
/// doesn't stop it. A shutdown drops the raise if the raise hasn't started,
/// and so does dropping the registry. With the standard timer, which reads
/// the clock that Tokio sleeps on, the first wake finds the interval ended.
/// A test timer can lag the sleep, and then the task sleeps again until the
/// timer reaches `due_at_ms`.
async fn run_trailing_raise(
    shared: Weak<RegistryShared>,
    timer: Arc<dyn MonotonicTimer>,
    namespace_id: NamespaceId,
    due_at_ms: u64,
) {
    loop {
        sleep_ms(due_at_ms.saturating_sub(timer.monotonic_now_ms())).await;
        let Some(shared) = shared.upgrade() else {
            return;
        };
        if shared.lock_state().closed {
            return;
        }
        match shared
            .hint_raises
            .start_trailing(&namespace_id, due_at_ms, timer.monotonic_now_ms())
        {
            NextRaise::Now(manifest_no) => {
                shared.raise_hint_now(&namespace_id, manifest_no).await;
                return;
            }
            NextRaise::Trailing { .. } => {}
            NextRaise::Covered => return,
        }
    }
}
