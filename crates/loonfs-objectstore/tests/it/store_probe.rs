use async_trait::async_trait;
use bytes::Bytes;
use loonfs_objectstore::keys::probe_run_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::probe::{run_store_contract_probe, StoreProbeOutcome, StoreProbeReport};
use loonfs_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreExtension, PutMode, Result};
use loonfs_test_support::stores::{
    FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationSelector,
};
use loonfs_types::{ProbeId, StoreProbeResponse};
use std::assert_matches;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

const POISON_PROVIDER_DETAIL: &str = "<Error>AccessDenied</Error> \
    arn:aws:iam::123456789012:role/private-role private-bucket \
    namespaces/customer-a/hint.json x-amz-request-id=provider-request \
    x-amz-id-2=provider-host-id AKIAEXAMPLE";

fn outcome<'a>(report: &'a StoreProbeReport, name: &str) -> &'a StoreProbeOutcome {
    &report
        .checks
        .iter()
        .find(|check| check.name == name)
        .expect("report should carry the named check")
        .outcome
}

fn local_store(temp_dir: &TempDir) -> LocalFsStore {
    LocalFsStore::new(temp_dir.path()).expect("create local object store")
}

#[tokio::test]
async fn provider_failures_make_every_probe_message_safe_to_paste() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = FailStore::new(
        local_store(&temp_dir),
        KeyPredicate::any(),
        OperationSelector::Any,
        InjectedError::PermissionDenied(POISON_PROVIDER_DETAIL.to_owned()),
    );
    store.fail_all();
    let probe_id = ProbeId::generate();
    let run_prefix = probe_run_prefix(&probe_id);
    let report = run_store_contract_probe(&store, &probe_id).await;
    let messages: Vec<_> = report
        .checks
        .iter()
        .filter_map(|check| match &check.outcome {
            StoreProbeOutcome::Failed { message } => Some(message.as_str()),
            StoreProbeOutcome::Passed | StoreProbeOutcome::Unsupported => None,
        })
        .collect();

    assert!(!messages.is_empty());
    for message in messages {
        assert!(
            message.contains("object-store permission denied"),
            "{message}"
        );
        for marker in [
            "<Error>AccessDenied</Error>",
            "arn:aws:iam::123456789012:role/private-role",
            "private-bucket",
            "namespaces/customer-a/hint.json",
            "x-amz-request-id=provider-request",
            "x-amz-id-2=provider-host-id",
            "AKIAEXAMPLE",
            run_prefix.as_str(),
        ] {
            assert!(
                !message.contains(marker),
                "probe message leaked {marker}: {message}"
            );
        }
    }
}

#[tokio::test]
async fn the_report_contains_the_complete_probe_set_once() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = local_store(&temp_dir);

    let report = run_store_contract_probe(&store, &ProbeId::generate()).await;

    let expected = std::collections::BTreeSet::from([
        "create_if_absent_enforced",
        "compare_and_swap_rejects_stale",
        "compare_and_swap_missing_object_rejected",
        "get_with_metadata_round_trip",
        "head_reports_last_modified",
        "visibility_after_write",
        "visibility_after_delete",
        "delete_missing_idempotent",
        "sorted_listing",
        "range_reads",
        "multipart_round_trip",
        "stored_checksum_readback",
        "cleanup_leaves_prefix_empty",
    ]);
    let names: std::collections::BTreeSet<_> =
        report.checks.iter().map(|check| check.name).collect();
    assert_eq!(
        report.checks.len(),
        names.len(),
        "probe names must be unique"
    );
    assert_eq!(
        names, expected,
        "the object-store layer owns the complete probe inventory"
    );
}

/// A store whose local filesystem honours everything except
/// compare-and-swap: the one shape a preconditions-ignoring S3 gateway
/// presents, and the one that silently corrupts fenced writes.
#[derive(Debug)]
struct StaleCompareAndSwapAcceptingStore {
    inner: LocalFsStore,
    accepted_a_stale_swap: Arc<AtomicBool>,
}

#[async_trait]
impl ObjectStore for StaleCompareAndSwapAcceptingStore {
    loonfs_test_support::delegate_object_store!(self => self.inner; except put);

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        if let PutMode::CompareAndSwap { .. } = mode {
            self.accepted_a_stale_swap.store(true, Ordering::SeqCst);
            self.inner.delete(key).await?;
            return self.inner.put_if_absent(key, bytes).await;
        }
        self.inner.put(key, bytes, mode).await
    }
}

#[tokio::test]
async fn a_store_that_ignores_compare_and_swap_fails_only_the_checks_about_it() {
    let temp_dir = TempDir::new().expect("tempdir");
    let accepted_a_stale_swap = Arc::new(AtomicBool::new(false));
    let store = StaleCompareAndSwapAcceptingStore {
        inner: local_store(&temp_dir),
        accepted_a_stale_swap: Arc::clone(&accepted_a_stale_swap),
    };

    let report = run_store_contract_probe(&store, &ProbeId::generate()).await;

    assert!(accepted_a_stale_swap.load(Ordering::SeqCst));
    assert!(!StoreProbeResponse::from(report.clone()).all_passed());
    assert_matches!(
        outcome(&report, "compare_and_swap_rejects_stale"),
        StoreProbeOutcome::Failed { message } if message.contains("does not enforce")
    );
    assert_matches!(
        outcome(&report, "compare_and_swap_missing_object_rejected"),
        StoreProbeOutcome::Failed { .. }
    );
    // A failed check ends that check, not the run: everything after it
    // still reports, and the run still cleans up after itself.
    assert_eq!(
        outcome(&report, "create_if_absent_enforced"),
        &StoreProbeOutcome::Passed
    );
    assert_eq!(outcome(&report, "range_reads"), &StoreProbeOutcome::Passed);
    assert_eq!(
        outcome(&report, "cleanup_leaves_prefix_empty"),
        &StoreProbeOutcome::Passed
    );
    assert!(store
        .list_prefix("probe-runs/")
        .await
        .expect("list the probe prefix")
        .is_empty());
}

/// A store that stamps every object at the unix epoch (what an
/// S3-compatible endpoint that omits `Last-Modified` becomes once the AWS
/// client path synthesizes a time for it).
#[tokio::test]
async fn a_store_that_stamps_every_object_at_the_epoch_fails_loudly() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = MetadataMapStore::aged(local_store(&temp_dir), KeyPredicate::any());

    let report = run_store_contract_probe(&store, &ProbeId::generate()).await;

    assert!(!StoreProbeResponse::from(report.clone()).all_passed());
    assert_matches!(
        outcome(&report, "head_reports_last_modified"),
        StoreProbeOutcome::Failed { message } if message.contains("unix epoch")
    );
}

/// A checksum-capable provider may encounter an object written without a
/// stored checksum. That per-object answer is distinct from the store
/// lacking the readback capability altogether.
#[tokio::test]
async fn a_present_object_without_a_stored_checksum_fails_the_probe() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = MetadataMapStore::new(
        local_store(&temp_dir),
        KeyPredicate::any(),
        |mut metadata| {
            metadata.checksum = None;
            metadata
        },
    );

    let report = run_store_contract_probe(&store, &ProbeId::generate()).await;

    assert_matches!(
        outcome(&report, "stored_checksum_readback"),
        StoreProbeOutcome::Failed { .. }
    );
    assert!(!StoreProbeResponse::from(report).all_passed());
}
