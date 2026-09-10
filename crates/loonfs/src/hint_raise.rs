//! Paces attempts to raise each namespace's WAL discovery hint.
//!
//! The raise follows the durable put and is awaited before acknowledgement for simplicity.
//! It costs one compare-and-swap per `HINT_RAISE_SEGMENTS` batches, or when the manifest
//! revalidation interval elapses. Correctness requires only that it follow the put.

use crate::fs::ReadCore;
use crate::NamespaceId;
use loonfs_api::WalNo;
use loonfs_core::control::LoadedHint;
use loonfs_core::limits::HINT_RAISE_SEGMENTS;
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Default)]
pub(crate) struct DiscoveryHints {
    namespaces: Mutex<BTreeMap<NamespaceId, Raised>>,
}

struct Raised {
    known: Option<LoadedHint>,
    wal_no: WalNo,
    at_ms: u64,
}

impl DiscoveryHints {
    /// Raises the hint once the tip is `HINT_RAISE_SEGMENTS` past the last
    /// raise or the revalidation interval has elapsed, whichever comes
    /// first. Readers never depend on it: they observe commits through the
    /// next WAL number. A failed raise is retried at the next trigger and
    /// never fails a commit.
    pub(crate) async fn raise_if_due(
        &self,
        core: &ReadCore,
        namespace_id: &NamespaceId,
        wal_no: WalNo,
    ) {
        let now_ms = core.inner.timer.monotonic_now_ms();
        let interval_ms = core
            .runtime_cache_config()
            .manifest_revalidation_interval_ms;
        let known = {
            let mut namespaces = self
                .namespaces
                .lock()
                .expect("hint state lock should be healthy");
            let raised = namespaces
                .entry(namespace_id.clone())
                .or_insert_with(|| Raised {
                    known: None,
                    wal_no: WalNo(0),
                    at_ms: now_ms,
                });
            let due = wal_no.0.saturating_sub(raised.wal_no.0) >= HINT_RAISE_SEGMENTS
                || now_ms.saturating_sub(raised.at_ms) >= interval_ms;
            if !due {
                return;
            }
            raised.known.take()
        };
        match loonfs_core::control::raise_namespace_hint(core.store(), namespace_id, wal_no, known)
            .await
        {
            Ok(hint) => {
                self.namespaces
                    .lock()
                    .expect("hint state lock should be healthy")
                    .insert(
                        namespace_id.clone(),
                        Raised {
                            known: Some(hint),
                            wal_no,
                            at_ms: now_ms,
                        },
                    );
            }
            Err(error) => {
                tracing::warn!(%namespace_id, %error, "namespace hint raise failed");
            }
        }
    }
}

#[cfg(test)]
mod tests;
