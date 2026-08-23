//! Runs every shared SDK case through `loonfs-client`.

#![allow(clippy::panic, clippy::unwrap_used)]

use bytes::Bytes;
use loonfs_api::options::DirectMultipartUploadOptions;
use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, CompleteMultipartUploadRequest, CompleteUploadRequest,
    FilesystemChange, UploadContentClaim, UploadMode, UploadPartChecksumClaim, UploadSessionStatus,
};
use loonfs_api::{
    ActorRef, ApiError, ChangeSeq, Checksum, CommitId, CommitRequest, FilesystemOperation,
    NamespaceId,
};
use loonfs_client::{
    Client, ClientConfig, ClientError, CommitOptions, CreateDirectoryOptions, DeleteOptions,
    ListPathEntriesOptions, MoveOptions, NamespacePath, PutFileOptions, StatPathOptions,
};
use loonfs_conformance::server::{start_server, ConformanceServer, AUTH_TOKEN};
use loonfs_conformance::{byte_pattern, load_cases, validate_page_walk, Case};
use serde::de::DeserializeOwned;
use serde::Deserialize;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rust_client_matches_the_reference_corpus() {
    let cases = load_cases().expect("load cases");
    let harness = Harness::start().await;

    for case in &cases {
        match case.name.as_str() {
            "error_contract" => run_error_contract(&harness, case).await,
            "commit_replay" => run_commit_replay(&harness, case).await,
            "upload_direct_put" => run_direct_put(&harness, case).await,
            "upload_multipart" => run_multipart(&harness, case).await,
            "upload_abort" => run_abort(&harness, case).await,
            "download" => run_download(&harness, case).await,
            "pagination" => run_pagination(&harness, case).await,
            "changes" => run_changes(&harness, case).await,
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
    unauthenticated: UnauthorizedExpected,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnauthorizedExpected {
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
        .commit(&namespace, &commit)
        .await
        .expect("first commit");
    let replayed = harness
        .client
        .commit(&namespace, &commit)
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
        .begin_direct_put(&namespace, Some(payload.len() as u64))
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
        .stat_path(&spec, &StatPathOptions::default())
        .await
        .expect("stat direct PUT file");
    assert_eq!(stat.content_ref(), Some(&content_ref));
    let readback = harness
        .client
        .get_file_bytes(&spec)
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
        .begin_direct_multipart(
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
    let completion_request = CompleteMultipartUploadRequest {
        content: UploadContentClaim {
            size_bytes: payload.len() as u64,
            checksum: whole_checksum.clone(),
        },
        parts: completed_parts,
    };
    let first = harness
        .client
        .complete_multipart_upload(&namespace, &upload_id, &completion_request)
        .await
        .expect("complete multipart upload");
    let first_content_ref = first.content_ref().expect("multipart content ref").clone();
    let first_completed_at_ms = completed_at_ms(&first.status);
    let replayed = harness
        .client
        .complete_multipart_upload(&namespace, &upload_id, &completion_request)
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
        .get_file_bytes(&spec)
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
        .begin_upload(&namespace, &BeginUploadRequest::ServiceProxied {})
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
        .stat_path(&spec, &StatPathOptions::default())
        .await
        .expect("stat download file");
    let grant = harness
        .client
        .begin_download(&spec, None)
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
        .commit(&namespace, &commit)
        .await
        .expect("commit change");
    assert_eq!(committed.committed_seq.0, expected.committed_seq);
    let feed = harness
        .client
        .list_changes(&namespace, ChangeSeq(request.after_seq), None)
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
        .stat_path(&upload_path, &StatPathOptions::default())
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
        .begin_download(&upload_path, None)
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
        .list_changes(&namespace, ChangeSeq(0), None)
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
        .list_changes(&namespace, ChangeSeq(0), None)
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
