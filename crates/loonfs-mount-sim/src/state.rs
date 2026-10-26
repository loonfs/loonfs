//! The server's mutable state, and the deterministic IDs it assigns.

use crate::config::SimulatedServerConfig;
use crate::content_store::ContentStore;
use crate::downloads::Grants;
use crate::error::{MountSimError, Result};
use crate::faults::Faults;
use crate::namespace::NamespaceState;
use crate::tokens::ContentTokens;
use crate::trace::Trace;
use crate::uploads::Uploads;
use loonfs_sim::{DeterministicClock, SimClock as _};
use loonfs_types::{CommitId, ContentId, ErrorCode, NamespaceId, PinId, UploadId};
use std::collections::BTreeMap;

/// Everything the simulated server keeps, behind the server's one lock.
#[derive(Debug)]
pub(crate) struct ServerState {
    pub(crate) config: SimulatedServerConfig,
    pub(crate) clock: DeterministicClock,
    pub(crate) namespaces: BTreeMap<NamespaceId, NamespaceState>,
    pub(crate) content: ContentStore,
    pub(crate) content_tokens: ContentTokens,
    pub(crate) uploads: Uploads,
    pub(crate) grants: Grants,
    pub(crate) faults: Faults,
    pub(crate) trace: Trace,
    pub(crate) ids: IdSource,
}

impl ServerState {
    pub(crate) fn new(config: SimulatedServerConfig, clock: DeterministicClock) -> Self {
        let trace = Trace::new(config.trace_capacity);
        Self {
            config,
            clock,
            namespaces: BTreeMap::new(),
            content: ContentStore::default(),
            content_tokens: ContentTokens::default(),
            uploads: Uploads::default(),
            grants: Grants::default(),
            faults: Faults::default(),
            trace,
            ids: IdSource::default(),
        }
    }

    /// Returns the virtual clock's time in Unix milliseconds.
    pub(crate) fn now_ms(&self) -> u64 {
        self.clock.now().as_nanos() / 1_000_000
    }

    pub(crate) fn namespace(&self, namespace_id: &NamespaceId) -> Result<&NamespaceState> {
        self.namespaces
            .get(namespace_id)
            .ok_or_else(|| namespace_not_found(namespace_id))
    }

    pub(crate) fn namespace_mut(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<&mut NamespaceState> {
        self.namespaces
            .get_mut(namespace_id)
            .ok_or_else(|| namespace_not_found(namespace_id))
    }
}

fn namespace_not_found(namespace_id: &NamespaceId) -> MountSimError {
    MountSimError::new(
        ErrorCode::NamespaceNotFound,
        format!("namespace `{namespace_id}` doesn't exist"),
    )
}

/// The source of every ID the server assigns. The IDs come from one counter, so a test that sends
/// the same requests gets the same IDs.
#[derive(Debug, Default)]
pub(crate) struct IdSource {
    next: u64,
}

impl IdSource {
    pub(crate) fn next_number(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    pub(crate) fn content_id(&mut self) -> ContentId {
        ContentId::parse(format!("con_{:032x}", self.next_number()))
            .expect("a counter in hex should be a valid content ID")
    }

    pub(crate) fn upload_id(&mut self) -> UploadId {
        UploadId::parse(format!("upl_{:032x}", self.next_number()))
            .expect("a counter in hex should be a valid upload ID")
    }

    pub(crate) fn commit_id(&mut self) -> CommitId {
        CommitId::parse(format!("c_{:032x}", self.next_number()))
            .expect("a counter in hex should be a valid commit ID")
    }

    /// Returns a snapshot ID. The simulation has no manifests, so the ID's manifest number is the
    /// captured sequence plus one, which keeps it positive.
    pub(crate) fn snapshot_id(&mut self, captured_seq: u64) -> PinId {
        PinId::parse(format!(
            "pin_{:020}-{:016x}",
            captured_seq + 1,
            self.next_number()
        ))
        .expect("a positive manifest number and a counter in hex should be a valid pin ID")
    }

    pub(crate) fn request_id(&mut self) -> String {
        format!("req_{:032x}", self.next_number())
    }

    pub(crate) fn content_token(&mut self) -> String {
        format!("sim-content-token-{}", self.next_number())
    }
}
