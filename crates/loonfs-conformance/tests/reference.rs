//! Runs every shared SDK case through `loonfs-client`.

#![allow(clippy::panic, clippy::unwrap_used)]

use bytes::Bytes;
use loonfs_client::{
    AccessState, AppendFileOptions, AttributeChanges, Client, ClientConfig, ClientError,
    CommitOptions, CreateDirectoryOptions, DeleteOptions, DownloadOptions, MoveOptions,
    NamespacePath, PutFileOptions, UndeleteDestination, UndeleteOptions,
    UpdateAttributesByInodeOptions,
};
use loonfs_conformance::server::{start_server, ConformanceServer, StoreShape, AUTH_TOKEN};
use loonfs_conformance::{byte_pattern, load_cases, validate_page_walk, Case};
use loonfs_test_support::ids::{first_page, page_limit};
use loonfs_types::api::v0::{
    CompleteUploadBody, ContentToken, CreateSnapshotRequest, CreateUploadBody,
    DeleteSnapshotResponse, ExtendSnapshotRequest, FilesystemChange, ListChangesResponse,
    ListSnapshotsResponse, ObjectTransferAccess, SnapshotSummary, UploadContentClaim, UploadMode,
    UploadPartChecksumClaim, UploadSessionStatus,
};
use loonfs_types::options::DirectMultipartUploadOptions;
use loonfs_types::PageRequest;
use loonfs_types::{
    AccessGrants, ActorId, ApiError, AttributeKey, AttributeValue, AttributesRevisionNo,
    BindingVersion, ChangeSeq, Checksum, ChecksumAlgorithm, CommitId, CommitPrecondition,
    CommitRequest, ContentRef, DeleteDirectoryBehavior, DestinationBehavior,
    DestinationPrecondition, DisplayName, FilesystemOperation, NamespaceId, PathEntry, RevisionNo,
    FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT, ROOT_INODE_ID,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::HashSet;
use std::io::Write;

#[tokio::test]
async fn rust_client_matches_the_reference_corpus_s3() {
    run_reference_corpus(StoreShape::S3, ChecksumAlgorithm::Crc64nvme).await;
}

#[tokio::test]
async fn rust_client_matches_the_reference_corpus_gcs() {
    run_reference_corpus(StoreShape::Gcs, ChecksumAlgorithm::Crc32c).await;
}

async fn run_reference_corpus(shape: StoreShape, checksum_algorithm: ChecksumAlgorithm) {
    assert_eq!(shape.checksum_algorithm(), checksum_algorithm);
    let cases = load_cases().expect("load cases");
    let harness = Harness::start(shape).await;

    for case in &cases {
        if case.name == "upload_multipart" && shape == StoreShape::Gcs {
            writeln!(
                std::io::stderr(),
                "skip gcs/{}: requires direct multipart; the GCS shape has no multipart issuer",
                case.name
            )
            .expect("write skip reason");
            continue;
        }
        match case.name.as_str() {
            "append" => run_append(&harness, case).await,
            "children_by_inode" => run_children_by_inode(&harness, case).await,
            "inode_addressing" => run_inode_addressing(&harness, case).await,
            "inode_mutations" => run_inode_mutations(&harness, case).await,
            "error_contract" => run_error_contract(&harness, case).await,
            "commit_replay" => run_commit_replay(&harness, case).await,
            "upload_direct_put" => run_direct_put(&harness, case).await,
            "upload_modes" => run_upload_modes(&harness, case).await,
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
    shape: StoreShape,
    client: Client,
    unauthenticated_client: Client,
    raw_client: reqwest::Client,
    server_url: String,
    _server: ConformanceServer,
}

impl Harness {
    async fn start(shape: StoreShape) -> Self {
        let server = start_server(shape).await.expect("start conformance server");
        let server_url = server.base_url.clone();
        let client = configured_client(&server_url, Some(AUTH_TOKEN));
        let unauthenticated_client = configured_client(&server_url, None);

        Self {
            shape,
            client,
            unauthenticated_client,
            raw_client: reqwest::Client::new(),
            server_url,
            _server: server,
        }
    }

    fn assert_checksum_algorithm(&self, actual: ChecksumAlgorithm, fixture: ChecksumAlgorithm) {
        assert_eq!(fixture, StoreShape::S3.checksum_algorithm());
        assert_eq!(actual, self.shape.checksum_algorithm());
    }
}

fn listed_name(entry: &PathEntry) -> String {
    entry
        .display_name
        .as_ref()
        .expect("listed name")
        .to_string()
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

fn commit_options(id: &str) -> CommitOptions {
    CommitOptions {
        commit_id: Some(commit_id(id)),
        ..Default::default()
    }
}

fn put_options(id: &str) -> PutFileOptions {
    PutFileOptions {
        commit: commit_options(id),
        ..Default::default()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadModesRequest {
    namespace_id: NamespaceId,
    actor_id: ActorId,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadModesExpected {
    checksum_algorithm: ChecksumAlgorithm,
    s3: Vec<UploadMode>,
    gcs: Vec<UploadMode>,
    unsupported: ErrorStatusExpected,
}

async fn run_upload_modes(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<UploadModesRequest, UploadModesExpected>(case);
    let expected_modes = match harness.shape {
        StoreShape::S3 => &expected.s3,
        StoreShape::Gcs => &expected.gcs,
    };
    let capabilities = harness
        .client
        .get_capabilities()
        .await
        .expect("capabilities");
    let mut advertised_modes = vec![UploadMode::ServiceProxied];
    for (feature, mode) in [
        (FEATURE_UPLOADS_DIRECT_PUT, UploadMode::DirectPut),
        (
            FEATURE_UPLOADS_DIRECT_MULTIPART,
            UploadMode::DirectMultipart,
        ),
    ] {
        if capabilities.supports(feature) {
            advertised_modes.push(mode);
        }
    }
    assert_eq!(&advertised_modes, expected_modes);
    harness
        .client
        .create_namespace(&request.namespace_id, &request.actor_id)
        .await
        .expect("create upload modes namespace");
    for body in [
        CreateUploadBody::ServiceProxied {},
        CreateUploadBody::DirectPut { size_bytes: None },
        CreateUploadBody::DirectMultipart {
            part_size_bytes: None,
        },
    ] {
        let begin = harness
            .client
            .create_upload(&request.namespace_id, &body)
            .await;
        if !expected_modes.contains(&body.mode()) {
            assert_api_error(
                &begin.expect_err("unsupported upload mode"),
                &expected.unsupported,
            );
            continue;
        }
        let begin = begin.expect("begin supported upload mode");
        assert_eq!(begin.mode, body.mode());
        match begin.status {
            UploadSessionStatus::Open {
                checksum_algorithm,
                access,
                part_size_bytes,
                ..
            } => {
                harness.assert_checksum_algorithm(checksum_algorithm, expected.checksum_algorithm);
                assert_eq!(access.is_some(), begin.mode == UploadMode::DirectPut);
                assert_eq!(
                    part_size_bytes.is_some(),
                    begin.mode == UploadMode::DirectMultipart
                );
            }
            other => panic!("expected open upload, found {other:?}"),
        }
        harness
            .client
            .abort_upload(&request.namespace_id, &begin.upload_id)
            .await
            .expect("abort mode check upload");
    }
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
        .header("Loonfs-Actor", "conformance-error")
        .json(&serde_json::json!({
            "commit_id": "conf-error-malformed-body",
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
    preconditions: Vec<loonfs_types::CommitPrecondition>,
    namespace_id: String,
    commit_id: String,
    actor_id: ActorId,
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
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create replay namespace");
    let commit = CommitRequest::single(
        commit_id(&request.commit_id),
        Some(request.message),
        FilesystemOperation::CreateDirectory {
            path: loonfs_types::AbsolutePath::parse(&request.path).expect("fixture path"),
            parents: false,
        },
    )
    .preconditions(request.preconditions);
    let first = harness
        .client
        .commit(&namespace, &request.actor_id, &commit)
        .await
        .expect("first commit");
    let replayed = harness
        .client
        .commit(&namespace, &request.actor_id, &commit)
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
    actor_id: ActorId,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectPutExpected {
    begin_status: String,
    mode: String,
    size_bytes: u64,
    checksum_algorithm: ChecksumAlgorithm,
    committed_seq: u64,
}

async fn run_direct_put(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<DirectPutRequest, DirectPutExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create direct-put namespace");
    let payload = request.content_utf8.as_bytes();
    let begin = harness
        .client
        .create_direct_put_upload(&namespace, Some(payload.len() as u64))
        .await
        .expect("begin direct PUT");
    assert_eq!(upload_mode_name(begin.mode), expected.mode);
    assert_eq!(
        serde_json::to_value(&begin).expect("session JSON")["status"],
        expected.begin_status
    );
    let upload_id = begin.upload_id;
    let (checksum_algorithm, access) = match begin.status {
        UploadSessionStatus::Open {
            checksum_algorithm,
            access: Some(access),
            ..
        } => (checksum_algorithm, access),
        other => panic!("expected direct_put, found {other:?}"),
    };
    harness.assert_checksum_algorithm(checksum_algorithm, expected.checksum_algorithm);

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
            &CompleteUploadBody::DirectPut {
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
    harness.assert_checksum_algorithm(content_ref.checksum.algorithm, expected.checksum_algorithm);
    assert!(content_ref.checksum.matches(payload));

    let spec = namespace_path(&request.namespace_id, &request.path);
    let committed = harness
        .client
        .commit_completed_upload(
            &spec,
            content_ref.clone(),
            content_token,
            &request.actor_id,
            &put_options(&request.commit_id),
            None,
        )
        .await
        .expect("commit direct PUT");
    assert_eq!(committed.committed_seq.0, expected.committed_seq);
    let stat = harness
        .client
        .stat(&spec)
        .await
        .expect("stat direct PUT file");
    assert_eq!(stat.content_ref(), Some(&content_ref));
    let readback = harness
        .client
        .read_file(&spec)
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
    actor_id: ActorId,
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
    begin_status: String,
    mode: String,
    part_count: usize,
    size_bytes: u64,
    checksum_algorithm: ChecksumAlgorithm,
    committed_seq: u64,
}

async fn run_multipart(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<MultipartRequest, MultipartExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create multipart namespace");
    let payload = byte_pattern(
        request.content_pattern.length,
        request.content_pattern.modulus,
    )
    .expect("valid fixture pattern");
    let begin = harness
        .client
        .create_direct_multipart_upload_with_options(
            &namespace,
            &DirectMultipartUploadOptions {
                part_size_bytes: Some(request.part_size_bytes),
            },
        )
        .await
        .expect("begin multipart upload");
    assert_eq!(upload_mode_name(begin.mode), expected.mode);
    assert_eq!(
        serde_json::to_value(&begin).expect("session JSON")["status"],
        expected.begin_status
    );
    let upload_id = begin.upload_id;
    let (part_size_bytes, checksum_algorithm) = match begin.status {
        UploadSessionStatus::Open {
            part_size_bytes: Some(part_size_bytes),
            checksum_algorithm,
            ..
        } => (part_size_bytes, checksum_algorithm),
        other => panic!("expected direct_multipart, found {other:?}"),
    };
    assert_eq!(part_size_bytes, request.part_size_bytes);
    harness.assert_checksum_algorithm(checksum_algorithm, expected.checksum_algorithm);

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
    let completion_request = CompleteUploadBody::DirectMultipart {
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
            &request.actor_id,
            &put_options(&request.commit_id),
            None,
        )
        .await
        .expect("commit multipart upload");
    assert_eq!(committed.committed_seq.0, expected.committed_seq);
    let readback = harness
        .client
        .read_file(&spec)
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
    actor_id: ActorId,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AbortExpected {
    begin_status: String,
    mode: String,
    status: String,
}

async fn run_abort(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<AbortRequest, AbortExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create abort namespace");
    let begin = harness
        .client
        .create_upload(&namespace, &CreateUploadBody::ServiceProxied {})
        .await
        .expect("begin abortable upload");
    assert_eq!(upload_mode_name(begin.mode), expected.mode);
    assert_eq!(
        serde_json::to_value(&begin).expect("session JSON")["status"],
        expected.begin_status
    );
    let first = harness
        .client
        .abort_upload(&namespace, &begin.upload_id)
        .await
        .expect("abort upload");
    let replayed = harness
        .client
        .abort_upload(&namespace, &begin.upload_id)
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
    actor_id: ActorId,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DownloadExpected {
    size_bytes: u64,
    checksum_algorithm: ChecksumAlgorithm,
    committed_seq: u64,
}

async fn run_download(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<DownloadRequest, DownloadExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    harness
        .client
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create download namespace");
    let spec = namespace_path(&request.namespace_id, &request.path);
    let committed = harness
        .client
        .put_file_with_options(
            &spec,
            request.content_utf8.as_bytes(),
            &request.actor_id,
            &put_options(&request.commit_id),
        )
        .await
        .expect("put download file");
    assert_eq!(committed.committed_seq.0, expected.committed_seq);
    let stat = harness
        .client
        .stat(&spec)
        .await
        .expect("stat download file");
    let grant = harness
        .client
        .create_download(&spec)
        .await
        .expect("begin direct download");
    assert_eq!(stat.content_ref(), Some(&grant.content_ref));
    assert_eq!(grant.content_ref.size_bytes, expected.size_bytes);
    harness.assert_checksum_algorithm(
        grant.content_ref.checksum.algorithm,
        expected.checksum_algorithm,
    );

    let bytes = stream_grant(&harness.client, &grant).await;
    assert_eq!(bytes.len() as u64, grant.content_ref.size_bytes);
    assert!(grant.content_ref.checksum.matches(&bytes));
    assert_eq!(bytes, request.content_utf8.as_bytes());
}

async fn stream_grant(
    client: &Client,
    grant: &loonfs_types::api::v0::CreateDownloadResponse,
) -> Vec<u8> {
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
struct AppendRequest {
    namespace_id: String,
    path: String,
    empty_path: String,
    actor_id: ActorId,
    content_utf8: String,
    content_repetitions: usize,
    appended_utf8: String,
    commit_ids: AppendCommitIds,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendCommitIds {
    put: String,
    append: String,
    empty_append: String,
    empty_put: String,
    append_to_empty: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendExpected {
    put_committed_seq: u64,
    append_committed_seq: u64,
    previous_revision_no: u64,
    previous_size_bytes: u64,
    previous_range: String,
    appended_revision_no: u64,
    appended_size_bytes: u64,
    resume_offset: u64,
    resumed_range: String,
    empty_append: ErrorStatusExpected,
    empty_put_committed_seq: u64,
    append_to_empty_committed_seq: u64,
}

fn append_options(id: &str) -> AppendFileOptions {
    AppendFileOptions {
        commit: commit_options(id),
        ..Default::default()
    }
}

fn signed_range(access: &ObjectTransferAccess) -> Option<&str> {
    let ObjectTransferAccess::PresignedUrl { headers, .. } = access;
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("range"))
        .map(|(_, value)| value.as_str())
}

async fn run_append(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<AppendRequest, AppendExpected>(case);
    let client = &harness.client;
    let actor = &request.actor_id;
    client
        .create_namespace(&namespace_id(&request.namespace_id), actor)
        .await
        .expect("create append namespace");
    let spec = namespace_path(&request.namespace_id, &request.path);
    let content = request.content_utf8.repeat(request.content_repetitions);
    let content = content.as_bytes();
    let appended = request.appended_utf8.as_bytes();

    let put = client
        .put_file_with_options(&spec, content, actor, &put_options(&request.commit_ids.put))
        .await
        .expect("put the file to append to");
    assert_eq!(put.committed_seq.0, expected.put_committed_seq);
    let append = client
        .append_file_with_options(
            &spec,
            appended,
            actor,
            &append_options(&request.commit_ids.append),
        )
        .await
        .expect("append to the file");
    assert_eq!(append.committed_seq.0, expected.append_committed_seq);

    let stat = client.stat(&spec).await.expect("stat the appended file");
    assert_eq!(
        stat.revision_no().expect("appended revision").0,
        expected.appended_revision_no
    );
    let current = client
        .create_download(&spec)
        .await
        .expect("grant the appended revision");
    assert_eq!(stat.content_ref(), Some(&current.content_ref));
    assert_eq!(current.content_ref.size_bytes, expected.appended_size_bytes);
    assert_eq!(
        stream_grant(client, &current).await,
        [content, appended].concat()
    );

    let previous_revision = RevisionNo::from(expected.previous_revision_no);
    let proxied = client
        .read_file_revision(&spec, previous_revision)
        .await
        .expect("read the previous revision through the server");
    assert_eq!(proxied, content);
    let previous = client
        .create_revision_download(&spec, previous_revision)
        .await
        .expect("grant the previous revision");
    assert_eq!(current.ranges.len(), 2);
    assert_eq!(
        previous.content_ref.size_bytes,
        expected.previous_size_bytes
    );
    assert_eq!(
        signed_range(&previous.ranges[0].access),
        Some(expected.previous_range.as_str())
    );
    assert_eq!(stream_grant(client, &previous).await, content);

    let resumed = client
        .create_download_with_options(
            &spec,
            &DownloadOptions {
                start_offset: expected.resume_offset,
                ..Default::default()
            },
        )
        .await
        .expect("grant the appended bytes");
    assert_eq!(
        signed_range(&resumed.ranges[0].access),
        Some(expected.resumed_range.as_str())
    );
    assert_eq!(resumed.ranges.len(), 1);
    assert_eq!(resumed.ranges[0].start_offset, expected.resume_offset);
    let mut stream = client
        .open_direct_download(&resumed)
        .await
        .expect("open the resumed download");
    stream.fold_resumed_prefix(&[content, appended].concat()[..expected.resume_offset as usize]);
    let mut rest = Vec::new();
    while let Some(chunk) = stream
        .next_chunk()
        .await
        .expect("read the resumed download")
    {
        rest.extend_from_slice(&chunk);
    }
    assert_eq!(
        rest,
        appended[(expected.resume_offset - expected.previous_size_bytes) as usize..]
    );

    let refused = client
        .append_file_with_options(
            &spec,
            &[],
            actor,
            &append_options(&request.commit_ids.empty_append),
        )
        .await
        .expect_err("an empty append is refused");
    assert_api_error(&refused, &expected.empty_append);

    let empty_spec = namespace_path(&request.namespace_id, &request.empty_path);
    let empty_put = client
        .put_file_with_options(
            &empty_spec,
            &[],
            actor,
            &put_options(&request.commit_ids.empty_put),
        )
        .await
        .expect("put an empty file");
    assert_eq!(empty_put.committed_seq.0, expected.empty_put_committed_seq);
    let empty = client
        .create_download(&empty_spec)
        .await
        .expect("grant the empty file");
    assert_eq!(empty.content_ref.size_bytes, 0);
    assert_eq!(signed_range(&empty.ranges[0].access), None);
    assert!(stream_grant(client, &empty).await.is_empty());

    let append_to_empty = client
        .append_file_with_options(
            &empty_spec,
            appended,
            actor,
            &append_options(&request.commit_ids.append_to_empty),
        )
        .await
        .expect("append to the empty file");
    assert_eq!(
        append_to_empty.committed_seq.0,
        expected.append_to_empty_committed_seq
    );
    let filled = client
        .create_download(&empty_spec)
        .await
        .expect("grant the appended empty file");
    assert_ne!(filled.content_ref.content_id, empty.content_ref.content_id);
    assert_eq!(stream_grant(client, &filled).await, appended);
    let still_empty = client
        .create_revision_download(&empty_spec, previous_revision)
        .await
        .expect("grant the empty revision");
    assert_eq!(still_empty.content_ref, empty.content_ref);
    assert_eq!(signed_range(&still_empty.ranges[0].access), None);
    assert!(stream_grant(client, &still_empty).await.is_empty());
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PaginationRequest {
    namespace_id: String,
    directory: String,
    actor_id: ActorId,
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
    actor_id: ActorId,
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
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create children-by-inode namespace");
    let directory = namespace_path(&request.namespace_id, &request.directory);
    harness
        .client
        .create_directory(&directory, &request.actor_id)
        .await
        .expect("create children-by-inode directory");
    for name in request.entry_names.iter().rev() {
        let path = namespace_path(
            &request.namespace_id,
            &format!("{}/{}", request.directory, name),
        );
        harness
            .client
            .create_directory(&path, &request.actor_id)
            .await
            .expect("create child entry");
    }

    let parent_inode_id = harness
        .client
        .stat(&directory)
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
            .list_by_inode(&namespace, parent_inode_id)
            .page(PageRequest {
                limit: page_limit(request.page_size),
                cursor: cursor.clone(),
            })
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
        observed.extend(page.entries.iter().map(listed_name));
        cursor = page.next_cursor;
        if page_count == request.resume_after_page {
            saved_cursor = cursor.clone();
            resume_offset = Some(observed.len());
        }
        if page_count == request.rename_after_page {
            let renamed_directory =
                namespace_path(&request.namespace_id, &request.renamed_directory);
            let options = MoveOptions {
                commit: commit_options(&request.rename_commit_id),
                ..Default::default()
            };
            let renamed = harness
                .client
                .move_path_with_options(&directory, &renamed_directory, &request.actor_id, &options)
                .await
                .expect("rename children-by-inode directory");
            assert_eq!(renamed.committed_seq.0, expected.renamed_head_seq);
            let renamed_inode_id = harness
                .client
                .stat(&renamed_directory)
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
    let mut pager = harness.client.list_by_inode(&namespace, parent_inode_id);
    let mut cursor = Some(saved_cursor);
    let mut resumed = Vec::new();
    while cursor.is_some() {
        let page = pager
            .page(PageRequest {
                limit: page_limit(request.page_size),
                cursor,
            })
            .await
            .expect("resume children-by-inode page");
        assert_eq!(page.namespace_id, namespace);
        assert_eq!(page.parent_inode_id, parent_inode_id);
        assert_eq!(page.head_seq.0, expected.renamed_head_seq);
        resumed.extend(page.entries.iter().map(listed_name));
        cursor = page.next_cursor;
    }
    validate_page_walk(&request.entry_names, &observed, resume_offset, &resumed)
        .expect("children-by-inode pagination invariants");
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InodeMutationsRequest {
    namespace_id: String,
    directory: String,
    actor_id: ActorId,
    path_directory_name: String,
    path_file_name: String,
    inode_directory_name: String,
    inode_file_name: String,
    renamed_file_name: String,
    moved_file_name: String,
    content_utf8: String,
    revised_content_utf8: String,
    malformed_binding_version: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InodeMutationsExpected {
    entry_names: Vec<String>,
    revised_revision_no: u64,
    moved_committed_seq: u64,
    deleted_committed_seq: u64,
    stale_binding_version: ErrorStatusExpected,
    malformed_binding_version: ErrorStatusExpected,
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
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create inode-mutations namespace");
    let directory = namespace_path(&request.namespace_id, &request.directory);
    harness
        .client
        .create_directory(&directory, &request.actor_id)
        .await
        .expect("create inode-mutations directory");
    harness
        .client
        .create_directory(&child_path(&request.path_directory_name), &request.actor_id)
        .await
        .expect("create path-addressed directory");
    harness
        .client
        .put_file_with_options(
            &child_path(&request.path_file_name),
            request.content_utf8.as_bytes(),
            &request.actor_id,
            &put_options("conf-inode-mutations-path-file"),
        )
        .await
        .expect("put path-addressed file");

    let parent_inode_id = harness
        .client
        .stat(&directory)
        .await
        .expect("stat inode-mutations directory")
        .inode_id;
    harness
        .client
        .commit(
            &namespace,
            &request.actor_id,
            &CommitRequest::single(
                commit_id("conf-inode-mutations-inode-directory"),
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
        .commit(
            &namespace,
            &request.actor_id,
            &CommitRequest {
                preconditions: Vec::new(),
                commit_id: commit_id("conf-inode-mutations-inode-file"),
                message: None,
                content_tokens,
                operations: vec![FilesystemOperation::CreateFileByInode {
                    parent_inode_id,
                    display_name: display_name(&request.inode_file_name),
                    content_ref: Some(content_ref),
                    inline_content: None,
                }],
            },
        )
        .await
        .expect("put file by inode");

    let listing = harness
        .client
        .list(&directory)
        .page(first_page())
        .await
        .expect("list inode-mutations directory");
    let names: Vec<String> = listing.entries.iter().map(listed_name).collect();
    assert_eq!(names, expected.entry_names);
    let versions: HashSet<&str> = listing
        .entries
        .iter()
        .map(|entry| {
            entry
                .binding_version
                .as_ref()
                .map(BindingVersion::as_str)
                .expect("listed binding version")
        })
        .collect();
    assert_eq!(versions.len(), listing.entries.len());
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
        .commit(
            &namespace,
            &request.actor_id,
            &CommitRequest {
                preconditions: Vec::new(),
                commit_id: commit_id("conf-inode-mutations-revision"),
                message: None,
                content_tokens,
                operations: vec![FilesystemOperation::PutFileRevisionByInode {
                    inode_id: file_inode_id,
                    content_ref: Some(content_ref),
                    inline_content: None,
                    expected_revision_no,
                }],
            },
        )
        .await
        .expect("put file revision by inode");
    let file_path = child_path(&request.inode_file_name);
    let revised = harness
        .client
        .stat(&file_path)
        .await
        .expect("stat revised file");
    assert_eq!(
        revised.revision_no().expect("revised revision").0,
        expected.revised_revision_no
    );
    assert_eq!(
        harness
            .client
            .read_file(&file_path)
            .await
            .expect("read revised file"),
        request.revised_content_utf8.as_bytes()
    );
    let stale_version = revised.binding_version.expect("revised binding version");

    let renamed_file = child_path(&request.renamed_file_name);
    let rename_options = MoveOptions {
        commit: commit_options("conf-inode-mutations-rename"),
        ..Default::default()
    };
    harness
        .client
        .move_path_with_options(
            &file_path,
            &renamed_file,
            &request.actor_id,
            &rename_options,
        )
        .await
        .expect("rename file by path");

    let move_by_inode = |id: &str, expected_binding_version: BindingVersion| {
        CommitRequest::single(
            commit_id(id),
            None,
            FilesystemOperation::MoveByInode {
                inode_id: file_inode_id,
                expected_binding_version,
                destination_parent_inode_id: inode_directory_id,
                destination_display_name: display_name(&request.moved_file_name),
                precondition: loonfs_types::DestinationPrecondition {
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            },
        )
    };
    let stale = harness
        .client
        .commit(
            &namespace,
            &request.actor_id,
            &move_by_inode("conf-inode-mutations-stale-move", stale_version),
        )
        .await
        .expect_err("stale binding version must fail");
    assert_api_error(&stale, &expected.stale_binding_version);
    let malformed = harness
        .raw_client
        .post(format!(
            "{}/v0/namespaces/{}/commits",
            harness.server_url, request.namespace_id
        ))
        .bearer_auth(AUTH_TOKEN)
        .header("Loonfs-Actor", request.actor_id.as_str())
        .json(&serde_json::json!({
            "commit_id": "conf-inode-mutations-malformed-move",
            "operations": [{
                "kind": "move_by_inode",
                "inode_id": file_inode_id,
                "expected_binding_version": request.malformed_binding_version,
                "destination_parent_inode_id": inode_directory_id,
                "destination_display_name": request.moved_file_name,
                "behavior": "no_replace",
            }],
        }))
        .send()
        .await
        .expect("send malformed binding version");
    assert_raw_status_error(malformed, &expected.malformed_binding_version).await;

    let fresh_version = harness
        .client
        .stat(&renamed_file)
        .await
        .expect("stat renamed file")
        .binding_version
        .expect("renamed binding version");
    let moved = harness
        .client
        .commit(
            &namespace,
            &request.actor_id,
            &move_by_inode("conf-inode-mutations-move", fresh_version.clone()),
        )
        .await
        .expect("move by inode");
    assert_eq!(moved.committed_seq.0, expected.moved_committed_seq);
    let moved_entry = harness
        .client
        .stat(&namespace_path(
            &request.namespace_id,
            &format!(
                "{}/{}/{}",
                request.directory, request.inode_directory_name, request.moved_file_name
            ),
        ))
        .await
        .expect("stat moved file");
    assert_eq!(moved_entry.inode_id, file_inode_id);
    let moved_version = moved_entry.binding_version.expect("moved binding version");
    assert_ne!(moved_version, fresh_version);

    let feed = harness
        .client
        .list_changes(&namespace, ChangeSeq(expected.moved_committed_seq - 1))
        .page(PageRequest {
            limit: page_limit(1),
            cursor: None,
        })
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
            binding_version, ..
        }] => assert_eq!(binding_version, &moved_version),
        other => panic!("expected one moved event, found {other:?}"),
    }

    let deleted = harness
        .client
        .commit(
            &namespace,
            &request.actor_id,
            &CommitRequest::single(
                commit_id("conf-inode-mutations-delete"),
                None,
                FilesystemOperation::DeleteByInode {
                    inode_id: file_inode_id,
                    expected_binding_version: moved_version,
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
struct InodeAddressingRequest {
    namespace_id: String,
    directory: String,
    actor_id: ActorId,
    source_file_name: String,
    renamed_file_name: String,
    copy_file_name: String,
    restored_file_name: String,
    first_content_utf8: String,
    second_content_utf8: String,
    attribute_key: String,
    attribute_value: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InodeAddressingExpected {
    current_revision_no: u64,
    copied_committed_seq: u64,
    restored_revision_no: u64,
    attributes_revision_no: u64,
    entry_names: Vec<String>,
    stale_binding_version: ErrorStatusExpected,
    occupied_name: ErrorStatusExpected,
    unrestricted_access: ErrorStatusExpected,
    deleted_content: ErrorStatusExpected,
}

async fn run_inode_addressing(harness: &Harness, case: &Case) {
    let (request, expected) = parse_values::<InodeAddressingRequest, InodeAddressingExpected>(case);
    let namespace = namespace_id(&request.namespace_id);
    let actor = &request.actor_id;
    let child_path = |name: &str| {
        namespace_path(
            &request.namespace_id,
            &format!("{}/{name}", request.directory),
        )
    };
    harness
        .client
        .create_namespace(&namespace, actor)
        .await
        .expect("create inode-addressing namespace");
    let directory = namespace_path(&request.namespace_id, &request.directory);
    harness
        .client
        .create_directory(&directory, actor)
        .await
        .expect("create inode-addressing directory");
    let source_path = child_path(&request.source_file_name);
    harness
        .client
        .put_file_with_options(
            &source_path,
            request.first_content_utf8.as_bytes(),
            actor,
            &put_options("conf-inode-addressing-first"),
        )
        .await
        .expect("put first revision");
    harness
        .client
        .put_file_with_options(
            &source_path,
            request.second_content_utf8.as_bytes(),
            actor,
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                commit: commit_options("conf-inode-addressing-second"),
                ..Default::default()
            },
        )
        .await
        .expect("put second revision");
    let parent_inode_id = harness
        .client
        .stat(&directory)
        .await
        .expect("stat inode-addressing directory")
        .inode_id;
    let source = harness
        .client
        .stat(&source_path)
        .await
        .expect("stat source");
    let source_inode_id = source.inode_id;
    let stale_version = source.binding_version.expect("source binding version");
    let renamed_path = child_path(&request.renamed_file_name);
    harness
        .client
        .move_path_with_options(
            &source_path,
            &renamed_path,
            actor,
            &MoveOptions {
                commit: commit_options("conf-inode-addressing-rename"),
                ..Default::default()
            },
        )
        .await
        .expect("rename source by path");
    let renamed = harness
        .client
        .stat(&renamed_path)
        .await
        .expect("stat renamed source");
    let fresh_version = renamed
        .binding_version
        .clone()
        .expect("renamed binding version");

    assert_eq!(
        harness
            .client
            .read_file_by_inode(&namespace, source_inode_id)
            .await
            .expect("read current content by inode"),
        request.second_content_utf8.as_bytes()
    );
    let grant = harness
        .client
        .create_download_by_inode(&namespace, source_inode_id)
        .await
        .expect("grant current content by inode");
    assert_eq!(grant.inode_id, source_inode_id);
    assert_eq!(grant.revision_no.0, expected.current_revision_no);
    assert_eq!(Some(&grant.content_ref), renamed.content_ref());

    let copy = |id: &str, preconditions: Vec<CommitPrecondition>| {
        CommitRequest::single(
            commit_id(id),
            None,
            FilesystemOperation::CopyByInode {
                inode_id: source_inode_id,
                destination_parent_inode_id: parent_inode_id,
                destination_display_name: display_name(&request.copy_file_name),
                precondition: DestinationPrecondition::default(),
            },
        )
        .preconditions(preconditions)
    };
    let binding = |expected_binding_version: BindingVersion| CommitPrecondition::InodeBinding {
        inode_id: source_inode_id,
        expected_binding_version,
    };
    let absence = |name: &str| CommitPrecondition::NameAbsence {
        parent_inode_id,
        display_name: display_name(name),
    };
    let stale = harness
        .client
        .commit(
            &namespace,
            actor,
            &copy(
                "conf-inode-addressing-stale-copy",
                vec![binding(stale_version)],
            ),
        )
        .await
        .expect_err("a stale binding precondition must fail");
    assert_api_error(&stale, &expected.stale_binding_version);
    let occupied = harness
        .client
        .commit(
            &namespace,
            actor,
            &copy(
                "conf-inode-addressing-occupied-copy",
                vec![
                    binding(fresh_version.clone()),
                    absence(&request.renamed_file_name),
                ],
            ),
        )
        .await
        .expect_err("a bound name must fail its absence precondition");
    assert_api_error(&occupied, &expected.occupied_name);
    let copied = harness
        .client
        .commit(
            &namespace,
            actor,
            &copy(
                "conf-inode-addressing-copy",
                vec![binding(fresh_version), absence(&request.copy_file_name)],
            ),
        )
        .await
        .expect("copy by inode");
    assert_eq!(copied.committed_seq.0, expected.copied_committed_seq);
    let copy_path = child_path(&request.copy_file_name);
    let copy_entry = harness.client.stat(&copy_path).await.expect("stat copy");
    assert_ne!(copy_entry.inode_id, source_inode_id);
    assert_eq!(
        harness
            .client
            .read_file(&copy_path)
            .await
            .expect("read copy"),
        request.second_content_utf8.as_bytes()
    );

    harness
        .client
        .restore_revision_by_inode_with_options(
            &namespace,
            source_inode_id,
            RevisionNo(1),
            actor,
            &commit_options("conf-inode-addressing-restore"),
        )
        .await
        .expect("restore revision by inode");
    let restored = harness
        .client
        .stat(&renamed_path)
        .await
        .expect("stat restored source");
    assert_eq!(
        restored.revision_no().expect("restored revision").0,
        expected.restored_revision_no
    );
    assert_eq!(
        harness
            .client
            .read_file_by_inode(&namespace, source_inode_id)
            .await
            .expect("read restored content by inode"),
        request.first_content_utf8.as_bytes()
    );

    let attribute_key = AttributeKey::parse(&request.attribute_key).expect("attribute key");
    let attribute_value = AttributeValue::parse(&request.attribute_value).expect("attribute value");
    harness
        .client
        .update_attributes_by_inode_with_options(
            &namespace,
            copy_entry.inode_id,
            actor,
            AttributeChanges {
                set: std::collections::BTreeMap::from([(
                    attribute_key.clone(),
                    attribute_value.clone(),
                )]),
                remove: Vec::new(),
            },
            &UpdateAttributesByInodeOptions {
                commit: commit_options("conf-inode-addressing-attributes"),
                expected_attributes_revision_no: Some(AttributesRevisionNo(0)),
            },
        )
        .await
        .expect("update attributes by inode");
    let labeled = harness
        .client
        .stat(&copy_path)
        .await
        .expect("stat labeled copy")
        .attributes
        .expect("projected attributes");
    assert_eq!(
        labeled.attributes_revision_no.0,
        expected.attributes_revision_no
    );
    assert_eq!(
        labeled.attributes.as_map().get(&attribute_key),
        Some(&attribute_value)
    );

    let unrestricted = harness
        .client
        .update_access_by_inode(
            &namespace,
            ROOT_INODE_ID,
            actor,
            AccessState {
                boundary: false,
                grants: AccessGrants::default(),
            },
        )
        .await
        .expect_err("an unrestricted namespace holds no access rows");
    assert_api_error(&unrestricted, &expected.unrestricted_access);

    let deletion = harness
        .client
        .delete_path_with_options(
            &copy_path,
            actor,
            &DeleteOptions {
                commit: commit_options("conf-inode-addressing-delete-copy"),
                ..Default::default()
            },
        )
        .await
        .expect("delete copy");
    harness
        .client
        .undelete_with_options(
            &namespace,
            copy_entry.inode_id,
            deletion.committed_seq,
            actor,
            &UndeleteOptions {
                commit: commit_options("conf-inode-addressing-undelete"),
                destination: UndeleteDestination::Name {
                    parent_inode_id,
                    display_name: display_name(&request.restored_file_name),
                },
            },
        )
        .await
        .expect("undelete under a parent inode");
    assert_eq!(
        harness
            .client
            .stat(&child_path(&request.restored_file_name))
            .await
            .expect("stat undeleted copy")
            .inode_id,
        copy_entry.inode_id
    );

    harness
        .client
        .delete_path_with_options(
            &renamed_path,
            actor,
            &DeleteOptions {
                commit: commit_options("conf-inode-addressing-delete-source"),
                ..Default::default()
            },
        )
        .await
        .expect("delete source");
    let deleted = harness
        .client
        .read_file_by_inode(&namespace, source_inode_id)
        .await
        .expect_err("a deleted inode has no current content");
    assert_api_error(&deleted, &expected.deleted_content);

    let listing = harness
        .client
        .list(&directory)
        .page(first_page())
        .await
        .expect("list inode-addressing directory");
    assert_eq!(
        listing.entries.iter().map(listed_name).collect::<Vec<_>>(),
        expected.entry_names
    );
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotsRequest {
    namespace_id: String,
    directory: String,
    actor_id: ActorId,
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
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create snapshots namespace");

    let directory = namespace_path(&request.namespace_id, &request.directory);
    let directory_options = CreateDirectoryOptions {
        commit: commit_options("conf-snapshots-create-directory"),
        ..Default::default()
    };
    harness
        .client
        .create_directory_with_options(&directory, &request.actor_id, &directory_options)
        .await
        .expect("create snapshots directory");

    let replaced_path = child_path(&request.replaced_file_name);
    harness
        .client
        .put_file_with_options(
            &replaced_path,
            request.captured_content_utf8.as_bytes(),
            &request.actor_id,
            &put_options("conf-snapshots-create-replaced"),
        )
        .await
        .expect("create replaced snapshot file");
    let deleted_path = child_path(&request.deleted_file_name);
    harness
        .client
        .put_file_with_options(
            &deleted_path,
            request.deleted_content_utf8.as_bytes(),
            &request.actor_id,
            &put_options("conf-snapshots-create-deleted"),
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
    assert_eq!(snapshot.captured_seq.0, expected.snapshot_head_seq);
    assert!(snapshot.expires_at_ms > snapshot.created_at_ms);

    let mut replace_options = put_options("conf-snapshots-replace-file");
    replace_options.behavior = DestinationBehavior::Replace;
    harness
        .client
        .put_file_with_options(
            &replaced_path,
            request.current_content_utf8.as_bytes(),
            &request.actor_id,
            &replace_options,
        )
        .await
        .expect("replace snapshot file");
    let added_path = child_path(&request.added_file_name);
    harness
        .client
        .put_file_with_options(
            &added_path,
            request.added_content_utf8.as_bytes(),
            &request.actor_id,
            &put_options("conf-snapshots-add-file"),
        )
        .await
        .expect("add file after snapshot");
    let delete_options = DeleteOptions {
        commit: commit_options("conf-snapshots-delete-file"),
        ..Default::default()
    };
    harness
        .client
        .delete_path_with_options(&deleted_path, &request.actor_id, &delete_options)
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
    let captured_listing: loonfs_types::api::v0::ListPathEntriesResponse = raw_success_json(
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
    let current_listing: loonfs_types::api::v0::ListPathEntriesResponse = raw_success_json(
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
    assert_eq!(extended.captured_seq.0, expected.snapshot_head_seq);
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

    let delete_url = format!("{snapshots_url}/{snapshot_id}");
    let deleted: DeleteSnapshotResponse = raw_success_json(
        harness
            .raw_client
            .delete(&delete_url)
            .bearer_auth(AUTH_TOKEN),
        "delete snapshot",
    )
    .await;
    assert_eq!(deleted.namespace_id, namespace);
    assert_eq!(deleted.snapshot_id, snapshot.snapshot_id);
    let deleted_again = harness
        .raw_client
        .delete(&delete_url)
        .bearer_auth(AUTH_TOKEN)
        .send()
        .await
        .expect("send second snapshot delete");
    assert_raw_status_error(deleted_again, &expected.snapshot_not_found).await;

    let deleted_read = harness
        .raw_client
        .get(&entry_url)
        .bearer_auth(AUTH_TOKEN)
        .query(&[
            ("path", replaced_path.absolute_path().as_str()),
            ("snapshot_id", snapshot_id),
        ])
        .send()
        .await
        .expect("send deleted snapshot read");
    assert_raw_status_error(deleted_read, &expected.snapshot_not_found).await;
    let deleted_extend = harness
        .raw_client
        .post(format!("{snapshots_url}/{snapshot_id}/extend"))
        .bearer_auth(AUTH_TOKEN)
        .json(&ExtendSnapshotRequest {
            ttl_ms: request.extend_ttl_ms,
        })
        .send()
        .await
        .expect("send deleted snapshot extend");
    assert_raw_status_error(deleted_extend, &expected.snapshot_not_found).await;

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
    entries.iter().map(listed_name).collect()
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
        .create_upload(namespace_id, &CreateUploadBody::ServiceProxied {})
        .await
        .expect("begin service-proxied upload");
    assert_eq!(begin.mode, UploadMode::ServiceProxied);
    assert!(matches!(begin.status, UploadSessionStatus::Open { .. }));
    let upload_id = begin.upload_id;
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
            &CompleteUploadBody::ServiceProxied {},
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
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create pagination namespace");
    let directory = namespace_path(&request.namespace_id, &request.directory);
    harness
        .client
        .create_directory(&directory, &request.actor_id)
        .await
        .expect("create pagination directory");
    for name in &request.entry_names {
        let path = namespace_path(
            &request.namespace_id,
            &format!("{}/{}", request.directory, name),
        );
        harness
            .client
            .create_directory(&path, &request.actor_id)
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
            .list(&directory)
            .page(PageRequest {
                limit: page_limit(request.page_size),
                cursor: cursor.clone(),
            })
            .await
            .expect("list pagination page");
        page_count += 1;
        assert_eq!(page.head_seq.0, expected.head_seq);
        observed.extend(page.entries.iter().map(listed_name));
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
            .list(&directory)
            .page(PageRequest {
                limit: page_limit(request.page_size),
                cursor: cursor.clone(),
            })
            .await
            .expect("resume pagination page");
        resumed.extend(page.entries.iter().map(listed_name));
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
    actor_id: ActorId,
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
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create changes namespace");
    let commit = CommitRequest::single(
        commit_id(&request.commit_id),
        None,
        FilesystemOperation::CreateDirectory {
            path: loonfs_types::AbsolutePath::parse(&request.path).expect("fixture path"),
            parents: false,
        },
    );
    let committed = harness
        .client
        .commit(&namespace, &request.actor_id, &commit)
        .await
        .expect("commit change");
    assert_eq!(committed.committed_seq.0, expected.committed_seq);
    let feed = harness
        .client
        .list_changes(&namespace, ChangeSeq(request.after_seq))
        .page(first_page())
        .await
        .expect("list changes");
    assert_eq!(feed.changes.len(), expected.change_count);
    let change = feed.changes.first().expect("one change");
    assert_eq!(change.commit_id.as_str(), request.commit_id);
    assert_eq!(change.committed_by, request.actor_id);
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
    actor_id: ActorId,
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
        .create_namespace(&namespace, &request.actor_id)
        .await
        .expect("create end-to-end namespace");
    let directory = namespace_path(&request.namespace_id, &request.directory);
    let mkdir_options = CreateDirectoryOptions {
        commit: commit_options(&request.commit_ids.mkdir),
        ..Default::default()
    };
    let mkdir = harness
        .client
        .create_directory_with_options(&directory, &request.actor_id, &mkdir_options)
        .await
        .expect("create end-to-end directory");
    assert_eq!(mkdir.committed_seq.0, expected.mkdir_committed_seq);

    let upload_path = namespace_path(&request.namespace_id, &request.upload_path);
    let upload = harness
        .client
        .put_file_with_options(
            &upload_path,
            request.content_utf8.as_bytes(),
            &request.actor_id,
            &put_options(&request.commit_ids.upload),
        )
        .await
        .expect("upload end-to-end file");
    assert_eq!(upload.committed_seq.0, expected.upload_committed_seq);
    let stat = harness
        .client
        .stat(&upload_path)
        .await
        .expect("stat end-to-end file");
    assert_eq!(stat.size_bytes(), Some(expected.size_bytes));
    let uploaded_inode = stat.inode_id;

    let initial_listing = harness
        .client
        .list(&directory)
        .page(first_page())
        .await
        .expect("list uploaded file");
    assert!(initial_listing
        .entries
        .iter()
        .any(|entry| entry.path.as_ref() == request.upload_path));

    let grant = harness
        .client
        .create_download(&upload_path)
        .await
        .expect("begin end-to-end download");
    let streamed = stream_grant(&harness.client, &grant).await;
    assert_eq!(streamed, request.content_utf8.as_bytes());

    let moved_path = namespace_path(&request.namespace_id, &request.moved_path);
    let move_options = MoveOptions {
        commit: commit_options(&request.commit_ids.r#move),
        ..Default::default()
    };
    let moved = harness
        .client
        .move_path_with_options(&upload_path, &moved_path, &request.actor_id, &move_options)
        .await
        .expect("move end-to-end file");
    assert_eq!(moved.committed_seq.0, expected.move_committed_seq);
    let moved_listing = harness
        .client
        .list(&directory)
        .page(first_page())
        .await
        .expect("list moved file");
    assert!(moved_listing
        .entries
        .iter()
        .any(|entry| entry.path.as_ref() == request.moved_path));

    let revisions = harness
        .client
        .list_file_revisions(&moved_path)
        .page(first_page())
        .await
        .expect("list end-to-end revisions");
    assert_eq!(revisions.revisions.len(), expected.revision_count);
    assert_eq!(
        revisions.revisions[0].commit_id.as_str(),
        request.commit_ids.upload
    );

    let changes = harness
        .client
        .list_changes(&namespace, ChangeSeq(0))
        .page(first_page())
        .await
        .expect("list end-to-end changes before remove");
    assert_eq!(changes.changes.len(), expected.change_count - 1);
    let delete_options = DeleteOptions {
        commit: commit_options(&request.commit_ids.remove),
        ..Default::default()
    };
    let removed = harness
        .client
        .delete_path_with_options(&moved_path, &request.actor_id, &delete_options)
        .await
        .expect("remove end-to-end file");
    assert_eq!(removed.committed_seq.0, expected.remove_committed_seq);

    let changes = harness
        .client
        .list_changes(&namespace, ChangeSeq(0))
        .page(first_page())
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
        .all(|change| change.committed_by == request.actor_id));

    let trash = harness
        .client
        .list_trash(&namespace)
        .page(first_page())
        .await
        .expect("list end-to-end trash");
    let removed_entry = trash
        .entries
        .iter()
        .find(|entry| entry.inode_id == uploaded_inode)
        .expect("removed inode in trash");
    assert_eq!(removed_entry.deletion_seq, removed.committed_seq);
}
