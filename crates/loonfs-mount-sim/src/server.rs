//! [`SimulatedServer`]: the test driver's handle on the simulated server.

use crate::commits::{create_commit, CommitIdentity};
use crate::config::{CapabilitySwitches, SimulatedServerConfig};
use crate::error::Result;
use crate::faults::{ContentReadFault, EchoOrder, Hold};
use crate::namespace::NamespaceState;
use crate::retention::advance_retention_floor;
use crate::service::SimulatedService;
use crate::state::ServerState;
use crate::trace::RequestRecord;
use loonfs_client::ClientConfig;
use loonfs_sim::{DeterministicClock, SimClock as _, SimDuration, SimInstant};
use loonfs_types::api::v0::{Commit, CommitRequest, Snapshot};
use loonfs_types::{ActorId, ChangeSeq, NamespaceId, NamespaceNaming};
use std::sync::{Arc, Mutex, MutexGuard};

/// The URL of the simulated server, which [`SimulatedServer::client_config`] sets as the client's
/// `server_url`.
pub const SIMULATED_SERVER_URL: &str = "http://sim.loonfs.test";

/// An in-memory LoonFS server for the mount's tests.
///
/// The server's [`service`](Self::service) serves the HTTP API, and the other methods drive the
/// server directly: they create namespaces, commit without HTTP, advance the retention floor,
/// change the advertised capabilities, arm fault hooks, and read the trace of requests. Clones
/// share one server.
#[derive(Debug, Clone)]
pub struct SimulatedServer {
    shared: Arc<Mutex<ServerState>>,
    clock: DeterministicClock,
}

impl SimulatedServer {
    /// Creates a server with no namespaces.
    pub fn new(config: SimulatedServerConfig) -> Self {
        let clock = DeterministicClock::new(SimInstant::from_nanos(
            config.start_at_ms.saturating_mul(1_000_000),
        ));
        Self {
            shared: Arc::new(Mutex::new(ServerState::new(config, clock.clone()))),
            clock,
        }
    }

    /// Returns the `tower` service that serves the HTTP API, for
    /// `loonfs_client::Client::with_transport`.
    pub fn service(&self) -> SimulatedService {
        SimulatedService::new(self.shared.clone())
    }

    /// Returns a client configuration that points at the simulated server.
    pub fn client_config(&self) -> ClientConfig {
        ClientConfig {
            server_url: SIMULATED_SERVER_URL.to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        }
    }

    /// Returns the server's virtual clock. Time moves only when a test advances it, and clones
    /// share the same time.
    pub fn clock(&self) -> &DeterministicClock {
        &self.clock
    }

    /// Moves the virtual clock forward by `delta_ms`, which expires the
    /// snapshots, grants, and tokens whose lifetime passes.
    pub fn advance_clock(&self, delta_ms: u64) {
        self.clock.advance(SimDuration::from_millis(delta_ms));
    }

    /// Returns the virtual clock's time in Unix milliseconds.
    pub fn now_ms(&self) -> u64 {
        self.state().now_ms()
    }

    /// Creates an empty namespace with the given naming mode.
    ///
    /// # Panics
    ///
    /// Panics if the namespace exists.
    pub fn create_namespace(&self, namespace_id: &NamespaceId, naming: NamespaceNaming) {
        let mut state = self.state();
        let now_ms = state.now_ms();
        let previous = state.namespaces.insert(
            namespace_id.clone(),
            NamespaceState::new(namespace_id.clone(), naming, now_ms),
        );
        assert!(
            previous.is_none(),
            "namespace `{namespace_id}` already exists"
        );
    }

    /// Changes a namespace's naming mode, which `GET /v0/namespaces/{ns}` returns.
    ///
    /// # Panics
    ///
    /// Panics if the namespace doesn't exist or has commits, because the name keys of its
    /// bindings were derived with the earlier mode.
    pub fn set_naming(&self, namespace_id: &NamespaceId, naming: NamespaceNaming) {
        let mut state = self.state();
        let namespace = state
            .namespace_mut(namespace_id)
            .expect("the test should name a namespace it created");
        assert!(
            namespace.commits.is_empty(),
            "a namespace's naming mode can change only before its first commit"
        );
        namespace.naming = naming;
    }

    /// Changes the features that the capability document advertises. A client caches the
    /// document, so a client created before the change keeps the earlier document.
    pub fn set_capabilities(&self, capabilities: CapabilitySwitches) {
        self.state().config.capabilities = capabilities;
    }

    /// Admits and publishes a commit without HTTP, as a request from `actor` with no subject
    /// would, and returns the commit. If the namespace has a receipt for the commit ID, the
    /// call replays the original commit.
    pub fn commit(
        &self,
        namespace_id: &NamespaceId,
        actor: &ActorId,
        request: &CommitRequest,
    ) -> Result<Commit> {
        let identity = CommitIdentity {
            actor: actor.clone(),
            subject: None,
        };
        create_commit(&mut self.state(), namespace_id, &identity, request)
            .map(|outcome| outcome.into_commit())
    }

    /// Returns a namespace's head sequence.
    ///
    /// # Panics
    ///
    /// Panics if the namespace doesn't exist.
    pub fn head_seq(&self, namespace_id: &NamespaceId) -> ChangeSeq {
        self.state()
            .namespace(namespace_id)
            .expect("the test should name a namespace it created")
            .head_seq()
    }

    /// Advances a namespace's retention floor to `to_seq`, or to the head if `to_seq` is above
    /// it, and returns the floor.
    ///
    /// The advance removes the commit receipts at or below the floor, so the change feed returns
    /// `rebootstrap_required` for an `after_seq` below it. When deletions at or below the floor
    /// are still recoverable, the advance expires them in one commit attributed to `loonfs`, as
    /// the namespace's writer does.
    ///
    /// # Panics
    ///
    /// Panics if the namespace doesn't exist.
    pub fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
        to_seq: ChangeSeq,
    ) -> ChangeSeq {
        advance_retention_floor(&mut self.state(), namespace_id, to_seq)
            .expect("the test should name a namespace it created")
    }

    /// Returns a namespace's snapshots, including expired ones, which nothing collects.
    ///
    /// # Panics
    ///
    /// Panics if the namespace doesn't exist.
    pub fn snapshots(&self, namespace_id: &NamespaceId) -> Vec<Snapshot> {
        self.state()
            .namespace(namespace_id)
            .expect("the test should name a namespace it created")
            .snapshots
            .values()
            .cloned()
            .collect()
    }

    /// Returns every request recorded since the server started or the trace was last taken.
    pub fn trace(&self) -> Vec<RequestRecord> {
        self.state().trace.snapshot()
    }

    /// Returns and clears the recorded requests.
    pub fn take_trace(&self) -> Vec<RequestRecord> {
        self.state().trace.take()
    }

    /// Makes the next request that commits return a transport failure to the client, while the
    /// commit stands. A request that a receipt replays, or that fails, doesn't use the fault.
    pub fn drop_next_commit_response(&self) {
        self.state().faults.drop_next_commit_response();
    }

    /// Orders the next commit's response and its echo in the change feed, and returns the hold
    /// that delays the later of the two.
    ///
    /// The order applies to the next request that makes a new commit. A request that a receipt
    /// replays, or that fails, doesn't use it. Each call queues one order. While a commit is kept
    /// out of the feed, the feed doesn't show any later commit of its namespace either, because it
    /// returns commits in order.
    pub fn order_next_commit_echo(&self, order: EchoOrder) -> Hold {
        self.state().faults.order_next_commit_echo(order)
    }

    /// Delays every change-feed response until the returned hold is released.
    ///
    /// # Panics
    ///
    /// Panics if an earlier feed hold hasn't been released.
    pub fn hold_feed_pages(&self) -> Hold {
        self.state().faults.hold_feed_pages()
    }

    /// Makes the next successful content read fail with `fault`. Each call queues one fault.
    pub fn fail_next_content_read(&self, fault: ContentReadFault) {
        self.state().faults.fail_next_content_read(fault);
    }

    /// Delivers the next `passing` successful content reads, and delays every later one until the
    /// returned hold is released. A content read that the hold delays doesn't use a queued
    /// content-read fault.
    ///
    /// # Panics
    ///
    /// Panics if an earlier content-read hold hasn't been released.
    pub fn hold_content_reads_after(&self, passing: usize) -> Hold {
        self.state().faults.hold_content_reads_after(passing)
    }

    fn state(&self) -> MutexGuard<'_, ServerState> {
        self.shared
            .lock()
            .expect("the simulated server's lock should not be poisoned")
    }
}

impl Default for SimulatedServer {
    fn default() -> Self {
        Self::new(SimulatedServerConfig::default())
    }
}
