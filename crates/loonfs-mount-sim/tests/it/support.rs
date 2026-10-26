//! Fixtures for the simulated server's tests: a server with one namespace, a real client on its
//! service, and builders for commit operations.

use bytes::Bytes;
use http::{HeaderMap, Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use loonfs_client::{Body, Client, ClientConfig};
use loonfs_mount_sim::{
    MountSimError, SimulatedServer, SimulatedServerConfig, SIMULATED_SERVER_URL,
};
use loonfs_types::api::v0::{Commit, CommitRequest, FilesystemOperation};
use loonfs_types::{
    AbsolutePath, ActorId, CommitId, DisplayName, InodeId, NamespaceId, NamespaceNaming, PathEntry,
};
use tower::ServiceExt as _;

/// A simulated server with one case-sensitive namespace, `demo`, and a client on its service.
pub(crate) struct Fixture {
    pub(crate) server: SimulatedServer,
    pub(crate) client: Client,
    pub(crate) namespace_id: NamespaceId,
    pub(crate) actor: ActorId,
}

pub(crate) fn fixture() -> Fixture {
    fixture_with(SimulatedServerConfig::default())
}

pub(crate) fn fixture_with(config: SimulatedServerConfig) -> Fixture {
    let server = SimulatedServer::new(config);
    let namespace_id = NamespaceId::parse("demo").expect("namespace ID");
    server.create_namespace(&namespace_id, NamespaceNaming::CaseSensitive);
    let client = client_for(&server, server.client_config());
    Fixture {
        server,
        client,
        namespace_id,
        actor: ActorId::parse("tester").expect("actor ID"),
    }
}

pub(crate) fn client_for(server: &SimulatedServer, config: ClientConfig) -> Client {
    Client::with_transport(config, server.service()).expect("a valid client config")
}

pub(crate) fn path(value: &str) -> AbsolutePath {
    AbsolutePath::parse(value).expect("absolute path")
}

pub(crate) fn name(value: &str) -> DisplayName {
    DisplayName::parse(value).expect("display name")
}

pub(crate) fn request(operations: Vec<FilesystemOperation>) -> CommitRequest {
    CommitRequest {
        commit_id: CommitId::generate(),
        message: None,
        content_tokens: Vec::new(),
        preconditions: Vec::new(),
        operations,
    }
}

pub(crate) fn mkdir(value: &str) -> FilesystemOperation {
    FilesystemOperation::CreateDirectory {
        path: path(value),
        parents: false,
    }
}

pub(crate) fn put(value: &str, bytes: &[u8]) -> FilesystemOperation {
    FilesystemOperation::PutFile {
        path: path(value),
        content_ref: None,
        inline_content: Some(bytes.to_vec()),
        behavior: Default::default(),
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

pub(crate) fn append(inode_id: InodeId, bytes: &[u8]) -> FilesystemOperation {
    FilesystemOperation::AppendFileByInode {
        inode_id,
        inline_content: bytes.to_vec(),
        expected_revision_no: None,
    }
}

impl Fixture {
    /// Commits through the test driver, and expects the commit to succeed.
    pub(crate) fn commit(&self, operations: Vec<FilesystemOperation>) -> Commit {
        self.try_commit(&request(operations))
            .expect("the commit should succeed")
    }

    pub(crate) fn try_commit(&self, request: &CommitRequest) -> Result<Commit, MountSimError> {
        self.server.commit(&self.namespace_id, &self.actor, request)
    }

    pub(crate) async fn stat(&self, value: &str) -> PathEntry {
        self.client
            .stat(&loonfs_client::NamespacePath::parse("demo", value).expect("namespace path"))
            .await
            .expect("stat")
    }

    /// Sends one raw request through the service and returns the status, headers, and body.
    pub(crate) async fn send(
        &self,
        method: Method,
        path_and_query: &str,
        headers: &[(&str, &str)],
        body: Bytes,
    ) -> (StatusCode, HeaderMap, Bytes) {
        let mut builder = Request::builder()
            .method(method)
            .uri(format!("{SIMULATED_SERVER_URL}{path_and_query}"));
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let response = self
            .server
            .service()
            .oneshot(builder.body(Body::from(body)).expect("a valid request"))
            .await
            .expect("the simulated service doesn't fail without a fault");
        let (parts, body) = response.into_parts();
        let bytes = body.collect().await.expect("a complete body").to_bytes();
        (parts.status, parts.headers, bytes)
    }

    pub(crate) async fn get(&self, path_and_query: &str) -> (StatusCode, HeaderMap, Bytes) {
        self.send(Method::GET, path_and_query, &[], Bytes::new())
            .await
    }
}
