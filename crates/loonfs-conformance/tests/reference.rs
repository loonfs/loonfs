//! Runs every shared SDK case through `loonfs-client`.

#![allow(clippy::panic, clippy::unwrap_used)]

use bytes::Bytes;
use loonfs_api::options::DirectMultipartUploadOptions;
use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, ContentToken,
    CreateSnapshotRequest, ExtendSnapshotRequest, FilesystemChange, ListChangesResponse,
    ListSnapshotsResponse, ReleaseSnapshotResponse, SnapshotSummary, UploadContentClaim,
    UploadMode, UploadPartChecksumClaim, UploadSessionStatus,
};
use loonfs_api::{
    ActorRef, ApiError, ChangeSeq, Checksum, CommitId, CommitRequest, ContentRef,
    DeleteDirectoryBehavior, DestinationBehavior, DisplayName, FilesystemOperation, NamespaceId,
    PathEntry,
};
use loonfs_client::{
    Client, ClientConfig, ClientError, CommitOptions, CreateDirectoryOptions, DeleteOptions,
    ListInodeChildrenOptions, ListPathEntriesOptions, MoveOptions, NamespacePath, PutFileOptions,
    StatPathOptions,
};
use loonfs_conformance::server::{start_server, ConformanceServer, AUTH_TOKEN};
use loonfs_conformance::{byte_pattern, load_cases, validate_page_walk, Case};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::HashSet;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rust_client_matches_the_reference_corpus() {
    let cases = load_cases().expect("load cases");
    let harness = Harness::start().await;

    for case in &cases {
        match case.name.as_str() {
            "children_by_inode" => run_children_by_inode(&harness, case).await,
            "inode_mutations" => run_inode_mutations(&harness, case).await,
            "error_contract" => run_error_contract(&harness, case).await,
            "commit_replay" => run_commit_replay(&harness, case).await,
            "upload_direct_put" => run_direct_put(&harness, case).await,
            "upload_multipart" => run_multipart(&harness, case).await,
            "upload_abort" => run_abort(&harness, case).await,
            "download" => run_download(&harness, case).await,
            "pagination" => run_pagination(&harness, case).await,
            "changes" => run_changes(&harness, case).await,
            "snapshots" => run_snapshots(&harness, case).await,
            "end_to_end" => run_end_to_end(&harness, case).await,
            // Each SDK harness runs this case against its own proxy.
            "proxy" => {}
            name => panic!("unknown case {name}"),
        }
    }
}

struct Harness {
    client: Client,
    unauthenticated_client: Client,
    raw_client: reqwest::Client,
    server_url: String,
    _server: ConformanceServer,
}

impl Harness {
    async fn start() -> Self {
        let server = start_server().await.expect("start conformance server");
        let server_url = server.base_url.clone();
        let client = configured_client(&server_url, Some(AUTH_TOKEN));
        let unauthenticated_client = configured_client(&server_url, None);

        Self {
            client,
            unauthenticated_client,
            raw_client: reqwest::Client::new(),
            server_url,
            _server: server,
        }
    }
}

fn configured_client(server_url: &str, auth_token: Option<&str>) -> Client {
    Client::new(ClientConfig {
        server_url: server_url.to_owned(),
        auth_token: auth_token.map(Into::into),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid conformance client")
}

fn parse_values<R, E>(case: &Case) -> (R, E)
where
    R: DeserializeOwned,
    E: DeserializeOwned,
{
    let request = serde_json::from_value(case.request.clone())
        .unwrap_or_else(|error| panic!("{} request did not parse: {error}", case.name));
    let expected = serde_json::from_value(case.expected.clone())
        .unwrap_or_else(|error| panic!("{} expected values did not parse: {error}", case.name));
    (request, expected)
}

fn namespace_id(value: &str) -> NamespaceId {
    NamespaceId::parse(value).expect("valid fixture namespace")
}

fn namespace_path(namespace_id: &str, path: &str) -> NamespacePath {
    NamespacePath::parse(namespace_id, path).expect("valid fixture namespace path")
}

fn commit_id(value: &str) -> CommitId {
    CommitId::parse(value).expect("valid fixture commit id")
}

fn display_name(value: &str) -> DisplayName {
    DisplayName::parse(value).expect("valid fixture display name")
}

fn commit_options(actor: &ActorRef, id: &str) -> CommitOptions {
    let mut options = CommitOptions::new(actor.clone());
    options.commit_id = Some(commit_id(id));
    options
}

fn put_options(actor: &ActorRef, id: &str) -> PutFileOptions {
    let mut options = PutFileOptions::new(actor.clone());
    options.commit = commit_options(actor, id);
    options
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorRequest {
    namespace_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorExpected {
    unauthenticated: ErrorStatusExpected,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorStatusExpected {
    status: u16,
    code: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorOutcome {
    status: u16,
    code: String,
    param: String,
}

async fn run_error_contract(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<ErrorRequest, ErrorExpected>(case);
    let error = harness
        .unauthenticated_client
        .get_namespace(&namespace_id(&request.namespace_id))
        .await
        .expect_err("unauthenticated request must fail");
    match error {
        ClientError::Api {
            status,
            code,
            request_id,
            ..
        } => {
            assert_eq!(status, expected.unauthenticated.status);
            assert_eq!(code, expected.unauthenticated.code);
            assert!(request_id.is_some());
        }
        other => panic!("expected API error, found {other:?}"),
    }

    let malformed = harness
        .raw_client
        .post(format!(
            "{}/v0/namespaces/{}/commits",
            harness.server_url, request.namespace_id
        ))
        .bearer_auth(AUTH_TOKEN)
        .json(&serde_json::json!({
            "commit_id": "conf-error-malformed-body",
            "actor": {
                "kind": "service",
                "id": "conformance-error",
            },
            "operations": [{
                "kind": "create_directory",
                "path": "relative",
            }],
        }))
        .send()
        .await
        .expect("send malformed body");
    assert_raw_error(
        malformed,
        &ErrorOutcome {
            status: 400,
            code: "invalid_request".to_owned(),
            param: "/operations/0/path".to_owned(),
        },
    )
    .await;

    let invalid_query = harness
        .raw_client
        .get(format!(
            "{}/v0/namespaces/{}/changes?after_seq={}",
            harness.server_url, request.namespace_id, "not-a-sequence"
        ))
        .bearer_auth(AUTH_TOKEN)
        .send()
        .await
        .expect("send invalid query");
    assert_raw_error(
        invalid_query,
        &ErrorOutcome {
            status: 400,
            code: "invalid_request".to_owned(),
            param: "after_seq".to_owned(),
        },
    )
    .await;
}

async fn assert_raw_error(response: reqwest::Response, expected: &ErrorOutcome) {
    assert_eq!(response.status().as_u16(), expected.status);
    let error: ApiError = response.json().await.expect("decode API error envelope");
    assert_eq!(error.code, expected.code);
    assert_eq!(error.param.as_deref(), Some(expected.param.as_str()));
    assert!(error.request_id.is_some());
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitReplayRequest {
    namespace_id: String,
    commit_id: String,
    actor: ActorRef,
    message: String,
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitReplayExpected {
    committed_seq: u64,
}

async fn run_commit_replay(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<CommitReplayRequest, CommitReplayExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create replay namespace");
    let commit = CommitRequest::single(
        commit_id(&request.commit_id),
        request.actor,
        Some(request.message),
        FilesystemOperation::CreateDirectory {
            path: loonfs_api::AbsolutePath::parse(&request.path).expect("fixture path"),
            parents: false,
        },
    );
    let first = harness
        .client
        .create_commit(&namespace, &commit)
        .await
        .expect("first commit");
    let replayed = harness
        .client
        .create_commit(&namespace, &commit)
        .await
        .expect("replayed commit");

    assert_eq!(first.committed_seq.0, expected.committed_seq);
    assert_eq!(first.commit_id.as_str(), request.commit_id);
    assert_eq!(replayed.committed_seq, first.committed_seq);
    assert_eq!(replayed, first);
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectPutRequest {
    namespace_id: String,
    path: String,
    commit_id: String,
    actor: ActorRef,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectPutExpected {
    mode: String,
    size_bytes: u64,
    checksum_algorithm: String,
    committed_seq: u64,
}

async fn run_direct_put(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<DirectPutRequest, DirectPutExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create direct-put namespace");
    let payload = request.content_utf8.as_bytes();
    let begin = harness
        .client
        .create_direct_put_upload(&namespace, Some(payload.len() as u64))
        .await
        .expect("begin direct PUT");
    assert_eq!(upload_mode_name(begin.mode()), expected.mode);
    let (upload_id, checksum_algorithm, access) = match begin {
        BeginUploadResponse::DirectPut {
            upload_id,
            checksum_algorithm,
            access,
            ..
        } => (upload_id, checksum_algorithm, access),
        other => panic!("expected direct_put, found {other:?}"),
    };
    assert_eq!(checksum_algorithm.as_str(), expected.checksum_algorithm);

    harness
        .client
        .upload_via_presigned_url(&access, payload)
        .await
        .expect("transfer direct PUT");
    let completed = harness
        .client
        .complete_upload(
            &namespace,
            &upload_id,
            &CompleteUploadRequest::DirectPut {
                content: UploadContentClaim {
                    size_bytes: payload.len() as u64,
                    checksum: Checksum::compute(checksum_algorithm, payload),
                },
            },
        )
        .await
        .expect("complete direct PUT");
    let content_ref = completed
        .content_ref()
        .expect("completed content ref")
        .clone();
    let content_token = completed.content_token().cloned();
    assert_eq!(content_ref.size_bytes, expected.size_bytes);
    assert_eq!(
        content_ref.checksum.algorithm.as_str(),
        expected.checksum_algorithm
    );
    assert!(content_ref.checksum.matches(payload));

    let spec = namespace_path(&request.namespace_id, &request.path);
    let committed = harness
        .client
        .commit_completed_upload(
            &spec,
            content_ref.clone(),
            content_token,
            &put_options(&request.actor, &request.commit_id),
        )
        .await
        .expect("commit direct PUT");
    assert_eq!(committed.committed_seq.0, expected.committed_seq);
    let stat = harness
        .client
        .get_path_entry(&spec, &StatPathOptions::default())
        .await
        .expect("stat direct PUT file");
    assert_eq!(stat.content_ref(), Some(&content_ref));
    let readback = harness
        .client
        .get_file_bytes(&spec, &Default::default())
        .await
        .expect("read direct PUT file");
    assert_eq!(readback, payload);
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MultipartRequest {
    namespace_id: String,
    path: String,
    commit_id: String,
    actor: ActorRef,
    part_size_bytes: u64,
    content_pattern: BytePattern,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BytePattern {
    length: usize,
    modulus: u8,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MultipartExpected {
    mode: String,
    part_count: usize,
    size_bytes: u64,
    checksum_algorithm: String,
    committed_seq: u64,
}

async fn run_multipart(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<MultipartRequest, MultipartExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create multipart namespace");
    let payload = byte_pattern(
        request.content_pattern.length,
        request.content_pattern.modulus,
    )
    .expect("valid fixture pattern");
    let begin = harness
        .client
        .create_direct_multipart_upload(
            &namespace,
            DirectMultipartUploadOptions {
                part_size_bytes: Some(request.part_size_bytes),
            },
        )
        .await
        .expect("begin multipart upload");
    assert_eq!(upload_mode_name(begin.mode()), expected.mode);
    let (upload_id, part_size_bytes, checksum_algorithm) = match begin {
        BeginUploadResponse::DirectMultipart {
            upload_id,
            part_size_bytes,
            checksum_algorithm,
            ..
        } => (upload_id, part_size_bytes, checksum_algorithm),
        other => panic!("expected direct_multipart, found {other:?}"),
    };
    assert_eq!(part_size_bytes, request.part_size_bytes);
    assert_eq!(checksum_algorithm.as_str(), expected.checksum_algorithm);

    let chunks = payload
        .chunks(usize::try_from(request.part_size_bytes).expect("part size fits usize"))
        .collect::<Vec<_>>();
    assert_eq!(chunks.len(), expected.part_count);
    let claims = chunks
        .iter()
        .enumerate()
        .map(|(index, chunk)| UploadPartChecksumClaim {
            part_number: index as u32 + 1,
            checksum: Checksum::compute(checksum_algorithm, chunk),
        })
        .collect::<Vec<_>>();
    let signed = harness
        .client
        .sign_upload_parts(&namespace, &upload_id, claims.clone())
        .await
        .expect("sign multipart parts");
    assert_eq!(signed.parts.len(), expected.part_count);
    let mut completed_parts = Vec::with_capacity(chunks.len());
    for signed_part in &signed.parts {
        let index = signed_part.part_number as usize - 1;
        completed_parts.push(
            harness
                .client
                .upload_part_via_presigned_url(
                    signed_part.part_number,
                    &signed_part.access,
                    claims[index].checksum.clone(),
                    Bytes::copy_from_slice(chunks[index]),
                )
                .await
                .expect("upload multipart part"),
        );
    }
    completed_parts.sort_by_key(|part| part.part_number);
    let whole_checksum = Checksum::compute(checksum_algorithm, &payload);
    let completion_request = CompleteUploadRequest::DirectMultipart {
        content: UploadContentClaim {
            size_bytes: payload.len() as u64,
            checksum: whole_checksum.clone(),
        },
        parts: completed_parts,
    };
    let first = harness
        .client
        .complete_upload(&namespace, &upload_id, &completion_request)
        .await
        .expect("complete multipart upload");
    let first_content_ref = first.content_ref().expect("multipart content ref").clone();
    let first_completed_at_ms = completed_at_ms(&first.status);
    let replayed = harness
        .client
        .complete_upload(&namespace, &upload_id, &completion_request)
        .await
        .expect("replay multipart completion");
    assert_eq!(replayed.namespace_id, first.namespace_id);
    assert_eq!(replayed.upload_id, first.upload_id);
    assert_eq!(replayed.mode, first.mode);
    assert_eq!(replayed.content_ref(), Some(&first_content_ref));
    assert_eq!(completed_at_ms(&replayed.status), first_completed_at_ms);
    assert_eq!(first_content_ref.size_bytes, expected.size_bytes);
    assert_eq!(first_content_ref.checksum, whole_checksum);
    assert!(first_content_ref.checksum.matches(&payload));

    let spec = namespace_path(&request.namespace_id, &request.path);
    let committed = harness
        .client
        .commit_completed_upload(
            &spec,
            first_content_ref,
            replayed.content_token().cloned(),
            &put_options(&request.actor, &request.commit_id),
        )
        .await
        .expect("commit multipart upload");
    assert_eq!(committed.committed_seq.0, expected.committed_seq);
    let readback = harness
        .client
        .get_file_bytes(&spec, &Default::default())
        .await
        .expect("read multipart file");
    assert_eq!(readback, payload);
}

fn completed_at_ms(status: &UploadSessionStatus) -> u64 {
    match status {
        UploadSessionStatus::Completed {
            completed_at_ms, ..
        } => *completed_at_ms,
        other => panic!("expected completed upload, found {other:?}"),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AbortRequest {
    namespace_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AbortExpected {
    mode: String,
    status: String,
}

async fn run_abort(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<AbortRequest, AbortExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create abort namespace");
    let begin = harness
        .client
        .create_upload(&namespace, &BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin abortable upload");
    assert_eq!(upload_mode_name(begin.mode()), expected.mode);
    let first = harness
        .client
        .abort_upload(&namespace, begin.upload_id())
        .await
        .expect("abort upload");
    let replayed = harness
        .client
        .abort_upload(&namespace, begin.upload_id())
        .await
        .expect("replay abort");
    assert_eq!(upload_status_name(&first.status), expected.status);
    assert_eq!(replayed, first);
    let first_aborted_at_ms = aborted_at_ms(&first.status);
    assert_eq!(aborted_at_ms(&replayed.status), first_aborted_at_ms);
}

fn aborted_at_ms(status: &UploadSessionStatus) -> u64 {
    match status {
        UploadSessionStatus::Aborted { aborted_at_ms } => *aborted_at_ms,
        other => panic!("expected aborted upload, found {other:?}"),
    }
}

fn upload_mode_name(mode: UploadMode) -> &'static str {
    match mode {
        UploadMode::ServiceProxied => "service_proxied",
        UploadMode::DirectPut => "direct_put",
        UploadMode::DirectMultipart => "direct_multipart",
    }
}

fn upload_status_name(status: &UploadSessionStatus) -> &'static str {
    match status {
        UploadSessionStatus::Open { .. } => "open",
        UploadSessionStatus::Completed { .. } => "completed",
        UploadSessionStatus::Aborted { .. } => "aborted",
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DownloadRequest {
    namespace_id: String,
    path: String,
    commit_id: String,
    actor: ActorRef,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DownloadExpected {
    size_bytes: u64,
    checksum_algorithm: String,
    committed_seq: u64,
}

async fn run_download(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<DownloadRequest, DownloadExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create download namespace");
    let spec = namespace_path(&request.namespace_id, &request.path);
    let committed = harness
        .client
        .put_file_bytes(
            &spec,
            request.content_utf8.as_bytes(),
            &put_options(&request.actor, &request.commit_id),
        )
        .await
        .expect("put download file");
    assert_eq!(committed.committed_seq.0, expected.committed_seq);
    let stat = harness
        .client
        .get_path_entry(&spec, &StatPathOptions::default())
        .await
        .expect("stat download file");
    let grant = harness
        .client
        .create_download(&spec, &Default::default())
        .await
        .expect("begin direct download");
    assert_eq!(stat.content_ref(), Some(&grant.content_ref));
    assert_eq!(grant.content_ref.size_bytes, expected.size_bytes);
    assert_eq!(
        grant.content_ref.checksum.algorithm.as_str(),
        expected.checksum_algorithm
    );

    let bytes = stream_grant(&harness.client, &grant).await;
    assert_eq!(bytes.len() as u64, grant.content_ref.size_bytes);
    assert!(grant.content_ref.checksum.matches(&bytes));
    assert_eq!(bytes, request.content_utf8.as_bytes());
}

async fn stream_grant(client: &Client, grant: &loonfs_api::v0::BeginDownloadResponse) -> Vec<u8> {
    let mut stream = client
        .open_direct_download(grant)
        .await
        .expect("open direct download");
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next_chunk().await.expect("read direct download") {
        bytes.extend_from_slice(&chunk);
    }
    bytes
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PaginationRequest {
    namespace_id: String,
    directory: String,
    actor: ActorRef,
    entry_names: Vec<String>,
    page_size: u32,
    resume_after_page: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PaginationExpected {
    entry_count: usize,
    minimum_page_count: usize,
    head_seq: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildrenByInodeRequest {
    namespace_id: String,
    directory: String,
    renamed_directory: String,
    rename_commit_id: String,
    actor: ActorRef,
    entry_names: Vec<String>,
    page_size: u32,
    rename_after_page: usize,
    resume_after_page: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildrenByInodeExpected {
    entry_count: usize,
    minimum_page_count: usize,
    initial_head_seq: u64,
    renamed_head_seq: u64,
}

async fn run_children_by_inode(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<ChildrenByInodeRequest, ChildrenByInodeExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create children-by-inode namespace");
    let directory = namespace_path(&request.namespace_id, &request.directory);
    harness
        .client
        .create_directory(
            &directory,
            &CreateDirectoryOptions::new(request.actor.clone()),
        )
        .await
        .expect("create children-by-inode directory");
    for name in request.entry_names.iter().rev() {
        let path = namespace_path(
            &request.namespace_id,
            &format!("{}/{}", request.directory, name),
        );
        harness
            .client
            .create_directory(&path, &CreateDirectoryOptions::new(request.actor.clone()))
            .await
            .expect("create child entry");
    }

    let parent_inode_id = harness
        .client
        .get_path_entry(&directory, &StatPathOptions::default())
        .await
        .expect("stat children-by-inode directory")
        .inode_id;
    let mut observed = Vec::new();
    let mut cursor = None;
    let mut page_count = 0usize;
    let mut saved_cursor = None;
    let mut resume_offset = None;
    loop {
        let page = harness
            .client
            .list_inode_children_page(
                &namespace,
                parent_inode_id,
                Some(request.page_size),
                cursor.as_deref(),
                &ListInodeChildrenOptions::default(),
            )
            .await
            .expect("list children-by-inode page");
        page_count += 1;
        assert_eq!(page.namespace_id, namespace);
        assert_eq!(page.parent_inode_id, parent_inode_id);
        let expected_head_seq = if page_count <= request.rename_after_page {
            expected.initial_head_seq
        } else {
            expected.renamed_head_seq
        };
        assert_eq!(page.head_seq.0, expected_head_seq);
        observed.extend(page.entries.iter().map(|entry| {
            entry
                .display_name
                .as_ref()
                .expect("listed name")
                .to_string()
        }));
        cursor = page.next_cursor;
        if page_count == request.resume_after_page {
            saved_cursor = cursor.clone();
            resume_offset = Some(observed.len());
        }
        if page_count == request.rename_after_page {
            let renamed_directory =
                namespace_path(&request.namespace_id, &request.renamed_directory);
            let mut options = MoveOptions::new(request.actor.clone());
            options.commit = commit_options(&request.actor, &request.rename_commit_id);
            let renamed = harness
                .client
                .move_path(&directory, &renamed_directory, &options)
                .await
                .expect("rename children-by-inode directory");
            assert_eq!(renamed.committed_seq.0, expected.renamed_head_seq);
            let renamed_inode_id = harness
                .client
                .get_path_entry(&renamed_directory, &StatPathOptions::default())
                .await
                .expect("stat renamed children-by-inode directory")
                .inode_id;
            assert_eq!(renamed_inode_id, parent_inode_id);
        }
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(observed.len(), expected.entry_count);
    assert!(page_count >= expected.minimum_page_count);

    let saved_cursor = saved_cursor.expect("saved mid-walk cursor");
    let resume_offset = resume_offset.expect("saved mid-walk offset");
    let mut pager = harness.client.list_inode_children_pager(
        &namespace,
        parent_inode_id,
        Some(request.page_size),
        Some(saved_cursor),
        &ListInodeChildrenOptions::default(),
    );
    let mut resumed = Vec::new();
    while let Some(page) = pager.next().await {
        let page = page.expect("resume children-by-inode page");
        assert_eq!(page.namespace_id, namespace);
        assert_eq!(page.parent_inode_id, parent_inode_id);
        assert_eq!(page.head_seq.0, expected.renamed_head_seq);
        resumed.extend(page.entries.iter().map(|entry| {
            entry
                .display_name
                .as_ref()
                .expect("listed name")
                .to_string()
        }));
    }
    validate_page_walk(&request.entry_names, &observed, resume_offset, &resumed)
        .expect("children-by-inode pagination invariants");
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InodeMutationsRequest {
    namespace_id: String,
    directory: String,
    actor: ActorRef,
    path_directory_name: String,
    path_file_name: String,
    inode_directory_name: String,
    inode_file_name: String,
    renamed_file_name: String,
    moved_file_name: String,
    content_utf8: String,
    revised_content_utf8: String,
    malformed_binding_generation: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InodeMutationsExpected {
    entry_names: Vec<String>,
    revised_revision_no: u64,
    moved_committed_seq: u64,
    deleted_committed_seq: u64,
    stale_binding_generation: ErrorStatusExpected,
    malformed_binding_generation: ErrorStatusExpected,
}

async fn run_inode_mutations(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<InodeMutationsRequest, InodeMutationsExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    let child_path = |name: &str| {
        namespace_path(
            &request.namespace_id,
            &format!("{}/{name}", request.directory),
        )
    };
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create inode-mutations namespace");
    let directory = namespace_path(&request.namespace_id, &request.directory);
    harness
        .client
        .create_directory(
            &directory,
            &CreateDirectoryOptions::new(request.actor.clone()),
        )
        .await
        .expect("create inode-mutations directory");
    harness
        .client
        .create_directory(
            &child_path(&request.path_directory_name),
            &CreateDirectoryOptions::new(request.actor.clone()),
        )
        .await
        .expect("create path-addressed directory");
    harness
        .client
        .put_file_bytes(
            &child_path(&request.path_file_name),
            request.content_utf8.as_bytes(),
            &put_options(&request.actor, "conf-inode-mutations-path-file"),
        )
        .await
        .expect("put path-addressed file");

    let parent_inode_id = harness
        .client
        .get_path_entry(&directory, &StatPathOptions::default())
        .await
        .expect("stat inode-mutations directory")
        .inode_id;
    harness
        .client
        .create_commit(
            &namespace,
            &CommitRequest::single(
                commit_id("conf-inode-mutations-inode-directory"),
                request.actor.clone(),
                None,
                FilesystemOperation::CreateDirectoryByInode {
                    parent_inode_id,
                    display_name: display_name(&request.inode_directory_name),
                },
            ),
        )
        .await
        .expect("create directory by inode");
    let (content_ref, content_tokens) =
        stage_content(harness, &namespace, request.content_utf8.as_bytes()).await;
    harness
        .client
        .create_commit(
            &namespace,
            &CommitRequest {
                commit_id: commit_id("conf-inode-mutations-inode-file"),
                actor: request.actor.clone(),
                message: None,
                content_tokens,
                operations: vec![FilesystemOperation::PutFileByInode {
                    parent_inode_id,
                    display_name: display_name(&request.inode_file_name),
                    content_ref,
                }],
            },
        )
        .await
        .expect("put file by inode");

    let listing = harness
        .client
        .list_path_entries_page(&directory, None, None, &ListPathEntriesOptions::default())
        .await
        .expect("list inode-mutations directory");
    let names: Vec<String> = listing
        .entries
        .iter()
        .map(|entry| {
            entry
                .display_name
                .as_ref()
                .expect("listed name")
                .to_string()
        })
        .collect();
    assert_eq!(names, expected.entry_names);
    let generations: HashSet<&str> = listing
        .entries
        .iter()
        .map(|entry| {
            entry
                .binding_generation
                .as_deref()
                .expect("listed binding generation")
        })
        .collect();
    assert_eq!(generations.len(), listing.entries.len());
    let entry_named = |name: &str| {
        listing
            .entries
            .iter()
            .find(|entry| {
                entry
                    .display_name
                    .as_ref()
                    .is_some_and(|display_name| display_name.as_str() == name)
            })
            .unwrap_or_else(|| panic!("listed entry `{name}` is missing"))
    };
    let inode_file = entry_named(&request.inode_file_name);
    let path_file = entry_named(&request.path_file_name);
    assert_eq!(
        entry_named(&request.inode_directory_name).inode_kind(),
        entry_named(&request.path_directory_name).inode_kind()
    );
    assert_eq!(inode_file.inode_kind(), path_file.inode_kind());
    assert_eq!(inode_file.size_bytes(), path_file.size_bytes());
    assert_eq!(inode_file.parent_inode_id, Some(parent_inode_id));
    let inode_directory_id = entry_named(&request.inode_directory_name).inode_id;
    let file_inode_id = inode_file.inode_id;
    let expected_revision_no = inode_file.revision_no().expect("listed revision");

    let (content_ref, content_tokens) =
        stage_content(harness, &namespace, request.revised_content_utf8.as_bytes()).await;
    harness
        .client
        .create_commit(
            &namespace,
            &CommitRequest {
                commit_id: commit_id("conf-inode-mutations-revision"),
                actor: request.actor.clone(),
                message: None,
                content_tokens,
                operations: vec![FilesystemOperation::PutFileRevisionByInode {
                    inode_id: file_inode_id,
                    content_ref,
                    expected_revision_no,
                }],
            },
        )
        .await
        .expect("put file revision by inode");
    let file_path = child_path(&request.inode_file_name);
    let revised = harness
        .client
        .get_path_entry(&file_path, &StatPathOptions::default())
        .await
        .expect("stat revised file");
    assert_eq!(
        revised.revision_no().expect("revised revision").0,
        expected.revised_revision_no
    );
    assert_eq!(
        harness
            .client
            .get_file_bytes(&file_path, &Default::default())
            .await
            .expect("read revised file"),
        request.revised_content_utf8.as_bytes()
    );
    let stale_generation = revised
        .binding_generation
        .expect("revised binding generation");

    let renamed_file = child_path(&request.renamed_file_name);
    let mut rename_options = MoveOptions::new(request.actor.clone());
    rename_options.commit = commit_options(&request.actor, "conf-inode-mutations-rename");
    harness
        .client
        .move_path(&file_path, &renamed_file, &rename_options)
        .await
        .expect("rename file by path");

    let move_by_inode = |id: &str, expected_binding_generation: String| {
        CommitRequest::single(
            commit_id(id),
            request.actor.clone(),
            None,
            FilesystemOperation::MoveByInode {
                inode_id: file_inode_id,
                expected_binding_generation,
                to_parent_inode_id: inode_directory_id,
                to_display_name: display_name(&request.moved_file_name),
                behavior: DestinationBehavior::NoReplace,
                expected_destination_inode_id: None,
                expected_destination_revision_no: None,
            },
        )
    };
    let stale = harness
        .client
        .create_commit(
            &namespace,
            &move_by_inode("conf-inode-mutations-stale-move", stale_generation),
        )
        .await
        .expect_err("stale binding generation must fail");
    assert_api_error(&stale, &expected.stale_binding_generation);
    let malformed = harness
        .client
        .create_commit(
            &namespace,
            &move_by_inode(
                "conf-inode-mutations-malformed-move",
                request.malformed_binding_generation.clone(),
            ),
        )
        .await
        .expect_err("malformed binding generation must fail");
    assert_api_error(&malformed, &expected.malformed_binding_generation);

    let fresh_generation = harness
        .client
        .get_path_entry(&renamed_file, &StatPathOptions::default())
        .await
        .expect("stat renamed file")
        .binding_generation
        .expect("renamed binding generation");
    let moved = harness
        .client
        .create_commit(
            &namespace,
            &move_by_inode("conf-inode-mutations-move", fresh_generation.clone()),
        )
        .await
        .expect("move by inode");
    assert_eq!(moved.committed_seq.0, expected.moved_committed_seq);
    let moved_entry = harness
        .client
        .get_path_entry(
            &namespace_path(
                &request.namespace_id,
                &format!(
                    "{}/{}/{}",
                    request.directory, request.inode_directory_name, request.moved_file_name
                ),
            ),
            &StatPathOptions::default(),
        )
        .await
        .expect("stat moved file");
    assert_eq!(moved_entry.inode_id, file_inode_id);
    let moved_generation = moved_entry
        .binding_generation
        .expect("moved binding generation");
    assert_ne!(moved_generation, fresh_generation);

    let feed = harness
        .client
        .list_changes(
            &namespace,
            ChangeSeq(expected.moved_committed_seq - 1),
            &loonfs_client::ListChangesOptions {
                limit: Some(1),
                snapshot_id: None,
            },
        )
        .await
        .expect("list inode-mutations changes");
    match feed
        .changes
        .first()
        .expect("moved change")
        .events
        .as_slice()
    {
        [FilesystemChange::Moved {
            binding_generation, ..
        }] => assert_eq!(binding_generation, &moved_generation),
        other => panic!("expected one moved event, found {other:?}"),
    }

    let deleted = harness
        .client
        .create_commit(
            &namespace,
            &CommitRequest::single(
                commit_id("conf-inode-mutations-delete"),
                request.actor.clone(),
                None,
                FilesystemOperation::DeleteByInode {
                    inode_id: file_inode_id,
                    expected_binding_generation: moved_generation,
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                },
            ),
        )
        .await
        .expect("delete by inode");
    assert_eq!(deleted.committed_seq.0, expected.deleted_committed_seq);
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotsRequest {
    namespace_id: String,
    directory: String,
    actor: ActorRef,
    snapshot_name: String,
    replaced_file_name: String,
    deleted_file_name: String,
    added_file_name: String,
    captured_content_utf8: String,
    current_content_utf8: String,
    deleted_content_utf8: String,
    added_content_utf8: String,
    create_ttl_ms: u64,
    extend_ttl_ms: u64,
    unknown_snapshot_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotsExpected {
    snapshot_head_seq: u64,
    captured_revision_no: u64,
    captured_entry_names: Vec<String>,
    current_revision_no: u64,
    current_entry_names: Vec<String>,
    snapshot_change_seqs: Vec<u64>,
    snapshot_gone: ErrorStatusExpected,
    snapshot_not_found: ErrorStatusExpected,
    revision_with_snapshot: ErrorStatusExpected,
    zero_ttl: ErrorStatusExpected,
}

async fn run_snapshots(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<SnapshotsRequest, SnapshotsExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    let child_path = |name: &str| {
        namespace_path(
            &request.namespace_id,
            &format!("{}/{name}", request.directory),
        )
    };
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create snapshots namespace");

    let directory = namespace_path(&request.namespace_id, &request.directory);
    let mut directory_options = CreateDirectoryOptions::new(request.actor.clone());
    directory_options.commit = commit_options(&request.actor, "conf-snapshots-create-directory");
    harness
        .client
        .create_directory(&directory, &directory_options)
        .await
        .expect("create snapshots directory");

    let replaced_path = child_path(&request.replaced_file_name);
    harness
        .client
        .put_file_bytes(
            &replaced_path,
            request.captured_content_utf8.as_bytes(),
            &put_options(&request.actor, "conf-snapshots-create-replaced"),
        )
        .await
        .expect("create replaced snapshot file");
    let deleted_path = child_path(&request.deleted_file_name);
    harness
        .client
        .put_file_bytes(
            &deleted_path,
            request.deleted_content_utf8.as_bytes(),
            &put_options(&request.actor, "conf-snapshots-create-deleted"),
        )
        .await
        .expect("create deleted snapshot file");

    let snapshots_url = format!(
        "{}/v0/namespaces/{}/snapshots",
        harness.server_url, request.namespace_id
    );
    let snapshot: SnapshotSummary = raw_success_json(
        harness
            .raw_client
            .post(&snapshots_url)
            .bearer_auth(AUTH_TOKEN)
            .json(&CreateSnapshotRequest {
                name: request.snapshot_name.clone(),
                ttl_ms: request.create_ttl_ms,
            }),
        "create snapshot",
    )
    .await;
    assert_eq!(snapshot.namespace_id, namespace);
    assert_eq!(snapshot.name, request.snapshot_name);
    assert_eq!(snapshot.head_seq.0, expected.snapshot_head_seq);
    assert!(snapshot.expires_at_ms > snapshot.created_at_ms);

    let mut replace_options = put_options(&request.actor, "conf-snapshots-replace-file");
    replace_options.behavior = DestinationBehavior::Replace;
    harness
        .client
        .put_file_bytes(
            &replaced_path,
            request.current_content_utf8.as_bytes(),
            &replace_options,
        )
        .await
        .expect("replace snapshot file");
    let added_path = child_path(&request.added_file_name);
    harness
        .client
        .put_file_bytes(
            &added_path,
            request.added_content_utf8.as_bytes(),
            &put_options(&request.actor, "conf-snapshots-add-file"),
        )
        .await
        .expect("add file after snapshot");
    let mut delete_options = DeleteOptions::new(request.actor.clone());
    delete_options.commit = commit_options(&request.actor, "conf-snapshots-delete-file");
    harness
        .client
        .delete_path(&deleted_path, &delete_options)
        .await
        .expect("delete file after snapshot");

    let snapshot_id = snapshot.snapshot_id.as_str();
    let entry_url = format!(
        "{}/v0/namespaces/{}/filesystem/entry",
        harness.server_url, request.namespace_id
    );
    let captured_entry: PathEntry = raw_success_json(
        harness
            .raw_client
            .get(&entry_url)
            .bearer_auth(AUTH_TOKEN)
            .query(&[
                ("path", replaced_path.absolute_path().as_str()),
                ("snapshot_id", snapshot_id),
            ]),
        "stat snapshot file",
    )
    .await;
    assert_eq!(
        captured_entry
            .revision_no()
            .expect("captured file revision")
            .0,
        expected.captured_revision_no
    );
    let current_entry: PathEntry = raw_success_json(
        harness
            .raw_client
            .get(&entry_url)
            .bearer_auth(AUTH_TOKEN)
            .query(&[("path", replaced_path.absolute_path().as_str())]),
        "stat current file",
    )
    .await;
    assert_eq!(
        current_entry
            .revision_no()
            .expect("current file revision")
            .0,
        expected.current_revision_no
    );

    let entries_url = format!(
        "{}/v0/namespaces/{}/filesystem/entries",
        harness.server_url, request.namespace_id
    );
    let captured_listing: loonfs_api::v0::ListPathEntriesResponse = raw_success_json(
        harness
            .raw_client
            .get(&entries_url)
            .bearer_auth(AUTH_TOKEN)
            .query(&[
                ("path", directory.absolute_path().as_str()),
                ("snapshot_id", snapshot_id),
            ]),
        "list snapshot directory",
    )
    .await;
    assert_eq!(captured_listing.head_seq.0, expected.snapshot_head_seq);
    assert_eq!(
        listed_entry_names(&captured_listing.entries),
        expected.captured_entry_names
    );
    let current_listing: loonfs_api::v0::ListPathEntriesResponse = raw_success_json(
        harness
            .raw_client
            .get(&entries_url)
            .bearer_auth(AUTH_TOKEN)
            .query(&[("path", directory.absolute_path().as_str())]),
        "list current directory",
    )
    .await;
    assert_eq!(
        listed_entry_names(&current_listing.entries),
        expected.current_entry_names
    );

    let content_url = format!(
        "{}/v0/namespaces/{}/filesystem/content",
        harness.server_url, request.namespace_id
    );
    let captured_content = raw_success_bytes(
        harness
            .raw_client
            .get(&content_url)
            .bearer_auth(AUTH_TOKEN)
            .query(&[
                ("path", replaced_path.absolute_path().as_str()),
                ("snapshot_id", snapshot_id),
            ]),
        "read snapshot content",
    )
    .await;
    assert_eq!(
        captured_content.as_ref(),
        request.captured_content_utf8.as_bytes()
    );
    let current_content = raw_success_bytes(
        harness
            .raw_client
            .get(&content_url)
            .bearer_auth(AUTH_TOKEN)
            .query(&[("path", replaced_path.absolute_path().as_str())]),
        "read current content",
    )
    .await;
    assert_eq!(
        current_content.as_ref(),
        request.current_content_utf8.as_bytes()
    );

    let changes_url = format!(
        "{}/v0/namespaces/{}/changes",
        harness.server_url, request.namespace_id
    );
    let feed: ListChangesResponse = raw_success_json(
        harness
            .raw_client
            .get(&changes_url)
            .bearer_auth(AUTH_TOKEN)
            .query(&[
                ("after_seq", "0"),
                ("limit", "100"),
                ("snapshot_id", snapshot_id),
            ]),
        "list snapshot changes",
    )
    .await;
    assert_eq!(feed.through_seq.0, expected.snapshot_head_seq);
    assert_eq!(feed.next_after_seq, None);
    assert_eq!(
        feed.changes
            .iter()
            .map(|change| change.committed_seq.0)
            .collect::<Vec<_>>(),
        expected.snapshot_change_seqs
    );

    let extended: SnapshotSummary = raw_success_json(
        harness
            .raw_client
            .post(format!("{snapshots_url}/{snapshot_id}/extend"))
            .bearer_auth(AUTH_TOKEN)
            .json(&ExtendSnapshotRequest {
                ttl_ms: request.extend_ttl_ms,
            }),
        "extend snapshot",
    )
    .await;
    assert_eq!(extended.snapshot_id, snapshot.snapshot_id);
    assert_eq!(extended.head_seq.0, expected.snapshot_head_seq);
    assert_eq!(extended.name, request.snapshot_name);
    assert!(extended.expires_at_ms > snapshot.expires_at_ms);

    let listed: ListSnapshotsResponse = raw_success_json(
        harness
            .raw_client
            .get(&snapshots_url)
            .bearer_auth(AUTH_TOKEN),
        "list snapshots",
    )
    .await;
    assert_eq!(listed.namespace_id, namespace);
    assert_eq!(listed.next_cursor, None);
    assert_eq!(listed.snapshots.len(), 1);
    assert_eq!(listed.snapshots[0].snapshot_id, snapshot.snapshot_id);

    let release_url = format!("{snapshots_url}/{snapshot_id}/release");
    for label in ["release snapshot", "release snapshot again"] {
        let released: ReleaseSnapshotResponse = raw_success_json(
            harness
                .raw_client
                .post(&release_url)
                .bearer_auth(AUTH_TOKEN),
            label,
        )
        .await;
        assert_eq!(released.namespace_id, namespace);
        assert_eq!(released.snapshot_id, snapshot.snapshot_id);
    }

    let released_read = harness
        .raw_client
        .get(&entry_url)
        .bearer_auth(AUTH_TOKEN)
        .query(&[
            ("path", replaced_path.absolute_path().as_str()),
            ("snapshot_id", snapshot_id),
        ])
        .send()
        .await
        .expect("send released snapshot read");
    assert_raw_status_error(released_read, &expected.snapshot_gone).await;
    let released_extend = harness
        .raw_client
        .post(format!("{snapshots_url}/{snapshot_id}/extend"))
        .bearer_auth(AUTH_TOKEN)
        .json(&ExtendSnapshotRequest {
            ttl_ms: request.extend_ttl_ms,
        })
        .send()
        .await
        .expect("send released snapshot extend");
    assert_raw_status_error(released_extend, &expected.snapshot_gone).await;

    let unknown_read = harness
        .raw_client
        .get(&entry_url)
        .bearer_auth(AUTH_TOKEN)
        .query(&[
            ("path", replaced_path.absolute_path().as_str()),
            ("snapshot_id", request.unknown_snapshot_id.as_str()),
        ])
        .send()
        .await
        .expect("send unknown snapshot read");
    assert_raw_status_error(unknown_read, &expected.snapshot_not_found).await;
    let revision_with_snapshot = harness
        .raw_client
        .get(&content_url)
        .bearer_auth(AUTH_TOKEN)
        .query(&[
            ("path", replaced_path.absolute_path().as_str()),
            ("revision_no", "1"),
            ("snapshot_id", snapshot_id),
        ])
        .send()
        .await
        .expect("send revision with snapshot read");
    assert_raw_status_error(revision_with_snapshot, &expected.revision_with_snapshot).await;
    let zero_ttl = harness
        .raw_client
        .post(&snapshots_url)
        .bearer_auth(AUTH_TOKEN)
        .json(&CreateSnapshotRequest {
            name: request.snapshot_name,
            ttl_ms: 0,
        })
        .send()
        .await
        .expect("send zero-ttl snapshot create");
    assert_raw_status_error(zero_ttl, &expected.zero_ttl).await;
}

fn listed_entry_names(entries: &[PathEntry]) -> Vec<String> {
    entries
        .iter()
        .map(|entry| {
            entry
                .display_name
                .as_ref()
                .expect("listed name")
                .to_string()
        })
        .collect()
}

async fn raw_success_json<T>(request: reqwest::RequestBuilder, label: &str) -> T
where
    T: DeserializeOwned,
{
    let response = request
        .send()
        .await
        .unwrap_or_else(|error| panic!("{label} request failed: {error}"));
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        panic!("{label} returned {status}: {body}");
    }
    response
        .json()
        .await
        .unwrap_or_else(|error| panic!("decode {label} response: {error}"))
}

async fn raw_success_bytes(request: reqwest::RequestBuilder, label: &str) -> Bytes {
    let response = request
        .send()
        .await
        .unwrap_or_else(|error| panic!("{label} request failed: {error}"));
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        panic!("{label} returned {status}: {body}");
    }
    response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("read {label} response: {error}"))
}

async fn assert_raw_status_error(response: reqwest::Response, expected: &ErrorStatusExpected) {
    assert_eq!(response.status().as_u16(), expected.status);
    let error: ApiError = response.json().await.expect("decode API error envelope");
    assert_eq!(error.code, expected.code);
}

fn assert_api_error(error: &ClientError, expected: &ErrorStatusExpected) {
    match error {
        ClientError::Api { status, code, .. } => {
            assert_eq!(*status, expected.status);
            assert_eq!(code, &expected.code);
        }
        other => panic!("expected API error, found {other:?}"),
    }
}

async fn stage_content(
    harness: &Harness,
    namespace_id: &NamespaceId,
    bytes: &[u8],
) -> (ContentRef, Vec<ContentToken>) {
    let begin = harness
        .client
        .create_upload(namespace_id, &BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin service-proxied upload");
    let BeginUploadResponse::ServiceProxied { upload_id, .. } = begin else {
        panic!("expected service_proxied, found {begin:?}");
    };
    harness
        .client
        .put_upload_content(namespace_id, &upload_id, bytes)
        .await
        .expect("stage upload content");
    let completed = harness
        .client
        .complete_upload(
            namespace_id,
            &upload_id,
            &CompleteUploadRequest::ServiceProxied {},
        )
        .await
        .expect("complete service-proxied upload");
    (
        completed
            .content_ref()
            .expect("completed content ref")
            .clone(),
        completed.content_token().cloned().into_iter().collect(),
    )
}

async fn run_pagination(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<PaginationRequest, PaginationExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create pagination namespace");
    let directory = namespace_path(&request.namespace_id, &request.directory);
    harness
        .client
        .create_directory(
            &directory,
            &CreateDirectoryOptions::new(request.actor.clone()),
        )
        .await
        .expect("create pagination directory");
    for name in &request.entry_names {
        let path = namespace_path(
            &request.namespace_id,
            &format!("{}/{}", request.directory, name),
        );
        harness
            .client
            .create_directory(&path, &CreateDirectoryOptions::new(request.actor.clone()))
            .await
            .expect("create pagination entry");
    }

    let mut observed = Vec::new();
    let mut cursor = None;
    let mut page_count = 0usize;
    let mut saved_cursor = None;
    let mut resume_offset = None;
    loop {
        let page = harness
            .client
            .list_path_entries_page(
                &directory,
                Some(request.page_size),
                cursor.as_deref(),
                &ListPathEntriesOptions::default(),
            )
            .await
            .expect("list pagination page");
        page_count += 1;
        assert_eq!(page.head_seq.0, expected.head_seq);
        observed.extend(page.entries.iter().map(|entry| {
            entry
                .display_name
                .as_ref()
                .expect("listed name")
                .to_string()
        }));
        cursor = page.next_cursor;
        if page_count == request.resume_after_page {
            saved_cursor = cursor.clone();
            resume_offset = Some(observed.len());
        }
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(observed.len(), expected.entry_count);
    assert!(page_count >= expected.minimum_page_count);
    assert!(cursor.is_none());

    let saved_cursor = saved_cursor.expect("saved mid-walk cursor");
    let resume_offset = resume_offset.expect("saved mid-walk offset");
    let mut resumed = Vec::new();
    let mut cursor = Some(saved_cursor);
    loop {
        let page = harness
            .client
            .list_path_entries_page(
                &directory,
                Some(request.page_size),
                cursor.as_deref(),
                &ListPathEntriesOptions::default(),
            )
            .await
            .expect("resume pagination page");
        resumed.extend(page.entries.iter().map(|entry| {
            entry
                .display_name
                .as_ref()
                .expect("listed name")
                .to_string()
        }));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    validate_page_walk(&request.entry_names, &observed, resume_offset, &resumed)
        .expect("pagination invariants");
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangesRequest {
    namespace_id: String,
    path: String,
    commit_id: String,
    actor: ActorRef,
    after_seq: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangesExpected {
    committed_seq: u64,
    change_count: usize,
}

async fn run_changes(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<ChangesRequest, ChangesExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create changes namespace");
    let commit = CommitRequest::single(
        commit_id(&request.commit_id),
        request.actor.clone(),
        None,
        FilesystemOperation::CreateDirectory {
            path: loonfs_api::AbsolutePath::parse(&request.path).expect("fixture path"),
            parents: false,
        },
    );
    let committed = harness
        .client
        .create_commit(&namespace, &commit)
        .await
        .expect("commit change");
    assert_eq!(committed.committed_seq.0, expected.committed_seq);
    let feed = harness
        .client
        .list_changes(
            &namespace,
            ChangeSeq(request.after_seq),
            &Default::default(),
        )
        .await
        .expect("list changes");
    assert_eq!(feed.changes.len(), expected.change_count);
    let change = feed.changes.first().expect("one change");
    assert_eq!(change.commit_id.as_str(), request.commit_id);
    assert_eq!(change.committed_by, request.actor);
    assert!(matches!(
        change.events.as_slice(),
        [FilesystemChange::DirectoryCreated { .. }]
    ));
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EndToEndRequest {
    namespace_id: String,
    directory: String,
    upload_path: String,
    moved_path: String,
    actor: ActorRef,
    content_utf8: String,
    commit_ids: EndToEndCommitIds,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EndToEndCommitIds {
    mkdir: String,
    upload: String,
    r#move: String,
    remove: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EndToEndExpected {
    mkdir_committed_seq: u64,
    upload_committed_seq: u64,
    move_committed_seq: u64,
    remove_committed_seq: u64,
    size_bytes: u64,
    revision_count: usize,
    change_count: usize,
}

async fn run_end_to_end(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<EndToEndRequest, EndToEndExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create end-to-end namespace");
    let directory = namespace_path(&request.namespace_id, &request.directory);
    let mut mkdir_options = CreateDirectoryOptions::new(request.actor.clone());
    mkdir_options.commit = commit_options(&request.actor, &request.commit_ids.mkdir);
    let mkdir = harness
        .client
        .create_directory(&directory, &mkdir_options)
        .await
        .expect("create end-to-end directory");
    assert_eq!(mkdir.committed_seq.0, expected.mkdir_committed_seq);

    let upload_path = namespace_path(&request.namespace_id, &request.upload_path);
    let upload = harness
        .client
        .put_file_bytes(
            &upload_path,
            request.content_utf8.as_bytes(),
            &put_options(&request.actor, &request.commit_ids.upload),
        )
        .await
        .expect("upload end-to-end file");
    assert_eq!(upload.committed_seq.0, expected.upload_committed_seq);
    let stat = harness
        .client
        .get_path_entry(&upload_path, &StatPathOptions::default())
        .await
        .expect("stat end-to-end file");
    assert_eq!(stat.size_bytes(), Some(expected.size_bytes));
    let uploaded_inode = stat.inode_id;

    let initial_listing = harness
        .client
        .list_path_entries_page(&directory, None, None, &ListPathEntriesOptions::default())
        .await
        .expect("list uploaded file");
    assert!(initial_listing
        .entries
        .iter()
        .any(|entry| entry.path.as_ref() == request.upload_path));

    let grant = harness
        .client
        .create_download(&upload_path, &Default::default())
        .await
        .expect("begin end-to-end download");
    let streamed = stream_grant(&harness.client, &grant).await;
    assert_eq!(streamed, request.content_utf8.as_bytes());

    let moved_path = namespace_path(&request.namespace_id, &request.moved_path);
    let mut move_options = MoveOptions::new(request.actor.clone());
    move_options.commit = commit_options(&request.actor, &request.commit_ids.r#move);
    let moved = harness
        .client
        .move_path(&upload_path, &moved_path, &move_options)
        .await
        .expect("move end-to-end file");
    assert_eq!(moved.committed_seq.0, expected.move_committed_seq);
    let moved_listing = harness
        .client
        .list_path_entries_page(&directory, None, None, &ListPathEntriesOptions::default())
        .await
        .expect("list moved file");
    assert!(moved_listing
        .entries
        .iter()
        .any(|entry| entry.path.as_ref() == request.moved_path));

    let revisions = harness
        .client
        .list_file_revisions_page(&moved_path, None, None)
        .await
        .expect("list end-to-end revisions");
    assert_eq!(revisions.revisions.len(), expected.revision_count);
    assert_eq!(
        revisions.revisions[0].commit_id.as_str(),
        request.commit_ids.upload
    );

    let changes = harness
        .client
        .list_changes(&namespace, ChangeSeq(0), &Default::default())
        .await
        .expect("list end-to-end changes before remove");
    assert_eq!(changes.changes.len(), expected.change_count - 1);
    let mut delete_options = DeleteOptions::new(request.actor.clone());
    delete_options.commit = commit_options(&request.actor, &request.commit_ids.remove);
    let removed = harness
        .client
        .delete_path(&moved_path, &delete_options)
        .await
        .expect("remove end-to-end file");
    assert_eq!(removed.committed_seq.0, expected.remove_committed_seq);

    let changes = harness
        .client
        .list_changes(&namespace, ChangeSeq(0), &Default::default())
        .await
        .expect("list complete end-to-end changes");
    assert_eq!(changes.changes.len(), expected.change_count);
    let expected_ids = [
        request.commit_ids.mkdir.as_str(),
        request.commit_ids.upload.as_str(),
        request.commit_ids.r#move.as_str(),
        request.commit_ids.remove.as_str(),
    ];
    assert_eq!(
        changes
            .changes
            .iter()
            .map(|change| change.commit_id.as_str())
            .collect::<Vec<_>>(),
        expected_ids
    );
    assert!(changes
        .changes
        .iter()
        .all(|change| change.committed_by == request.actor));

    let trash = harness
        .client
        .list_trash_page(&namespace, None, None)
        .await
        .expect("list end-to-end trash");
    let removed_entry = trash
        .entries
        .iter()
        .find(|entry| entry.inode_id == uploaded_inode)
        .expect("removed inode in trash");
    assert_eq!(removed_entry.deletion_seq, removed.committed_seq);
}
