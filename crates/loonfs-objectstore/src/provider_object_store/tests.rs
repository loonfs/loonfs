//! Unit tests for the upstream object-store adapter.

use super::*;
use crate::metrics::{InstrumentedObjectStore, VecObjectStoreMetricsRecorder};
use crate::test_support::{auth_rejection, transport_glitch};
use crate::ObjectStoreExtension as _;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use provider_store::list::PaginatedListResult;
use std::time::Duration;

fn memory_store() -> ProviderObjectStore {
    retrying_store(Arc::new(FlakyStore::default()))
}

/// Builds a store whose provider clients are scripted doubles, and whose
/// signer is a [`TestSigner`] over `provider`.
fn provider_store(
    provider: Arc<FlakyStore>,
    one_attempt: Arc<FlakyStore>,
    kind: ConfiguredObjectStoreKind,
    key_prefix: Option<&str>,
) -> ProviderObjectStore {
    let key_scope = KeyScope::new(key_prefix).expect("key prefix");
    ProviderObjectStore::new(ProviderParts {
        provider: Arc::clone(&provider),
        one_attempt,
        signer: Arc::new(TestSigner {
            provider,
            key_scope: key_scope.clone(),
        }),
        client_multipart: None,
        kind,
        key_scope,
        location: "bucket".to_owned(),
        io_runtime: StoreIoRuntime::new().expect("store io runtime"),
        retry: Arc::new(TransportRetry::new(Arc::new(
            loonfs_types::StdMonotonicTimer::default(),
        ))),
    })
}

/// Reads stored checksums from the provider double and creates a
/// multipart payload with one conditional put on the provider double,
/// standing in for a provider's signed requests.
struct TestSigner {
    provider: Arc<FlakyStore>,
    key_scope: KeyScope,
}

impl TestSigner {
    async fn create(&self, key: &str, bytes: Vec<u8>) -> Result<ObjectMetadata> {
        let (size_bytes, checksum) = (bytes.len() as u64, Checksum::crc64nvme(&bytes));
        let options = PutOptions {
            mode: provider_store::PutMode::Create,
            ..Default::default()
        };
        let path = Path::from(self.key_scope.object_key(key)?);
        let result = provider_store::ObjectStore::put_opts(
            self.provider.as_ref(),
            &path,
            PutPayload::from(bytes),
            options,
        )
        .await
        .map_err(|error| map_provider_error(key, error, ProviderClient::OneAttempt))?;
        Ok(ObjectMetadata {
            etag: result.e_tag,
            version: result.version,
            size_bytes,
            last_modified_at_ms: None,
            checksum: Some(checksum),
        })
    }
}

#[async_trait]
impl ProviderSigner for TestSigner {
    async fn head_metadata(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        let provider = &self.provider;
        provider.heads.fetch_add(1, Ordering::SeqCst);
        let path = Path::from(self.key_scope.object_key(key)?);
        let mut metadata = match provider.inner.get(&path).await {
            Ok(result) => {
                let meta = result.meta.clone();
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| map_provider_error(key, error, ProviderClient::Retrying))?;
                Some(ObjectMetadata {
                    etag: meta.e_tag,
                    version: meta.version,
                    size_bytes: meta.size,
                    last_modified_at_ms: last_modified_at_ms(meta.last_modified.into()),
                    checksum: Some(Checksum::crc64nvme(&bytes)),
                })
            }
            Err(error) if provider_not_found(&error) => None,
            Err(error) => return Err(map_provider_error(key, error, ProviderClient::Retrying)),
        };
        if provider
            .missing_checksum
            .lock()
            .expect("checksum fault")
            .as_deref()
            == Some(key)
        {
            if let Some(metadata) = &mut metadata {
                metadata.checksum = None;
            }
        }
        Ok(metadata)
    }

    async fn create_checked(
        &self,
        key: &str,
        bytes: Bytes,
        _checksum: &Checksum,
    ) -> Result<ObjectMetadata> {
        self.create(key, bytes.to_vec()).await
    }

    async fn put_if_absent(
        &self,
        key: &str,
        head: Bytes,
        mut rest: PartReader<'_>,
        _part_window: usize,
    ) -> Result<ObjectMetadata> {
        let mut bytes = head.to_vec();
        while let Some(part) = rest.next_part().await? {
            bytes.extend_from_slice(&part);
        }
        self.create(key, bytes).await
    }

    async fn assemble(
        &self,
        _key: &str,
        _sources: &[AssemblySource],
        _tail: Vec<Bytes>,
        _expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        Err(ObjectStoreError::Unsupported("test assembly"))
    }
}

async fn memory_page(
    store: &InMemory,
    prefix: Option<&str>,
    options: PaginatedListOptions,
    inclusive_offset: bool,
) -> provider_store::Result<PaginatedListResult> {
    let prefix = prefix.unwrap_or_default();
    let max_keys = options.max_keys.unwrap_or(1_000);
    let mut listed: Vec<ObjectMeta> = provider_store::ObjectStore::list(store, None)
        .try_collect()
        .await?;
    listed.sort_by(|left, right| left.location.as_ref().cmp(right.location.as_ref()));
    let mut objects = Vec::new();
    let mut common_prefixes: Vec<String> = Vec::new();
    let mut truncated = false;
    let mut last_key = None;
    for meta in listed {
        let key = meta.location.as_ref().to_owned();
        let after_offset = options
            .offset
            .as_deref()
            .is_none_or(|offset| key.as_str() > offset || (inclusive_offset && key == offset));
        let after_page = options
            .page_token
            .as_deref()
            .is_none_or(|token| key.as_str() > token);
        if !key.starts_with(prefix) || !after_offset || !after_page {
            continue;
        }
        let common = options.delimiter.as_deref().and_then(|delimiter| {
            key[prefix.len()..]
                .find(delimiter)
                .map(|end| key[..prefix.len() + end + delimiter.len()].to_owned())
        });
        if common.is_some() && common.as_ref() == common_prefixes.last() {
            continue;
        }
        if objects.len() + common_prefixes.len() == max_keys {
            truncated = true;
            break;
        }
        last_key = Some(key);
        match common {
            Some(common) => common_prefixes.push(common),
            None => objects.push(meta),
        }
    }
    if inclusive_offset {
        if let Some(offset) = options.offset.as_deref() {
            if objects
                .first()
                .is_some_and(|first| first.location.as_ref() == offset)
            {
                objects.remove(0);
            }
        }
    }
    Ok(PaginatedListResult {
        result: ListResult {
            common_prefixes: common_prefixes
                .iter()
                .map(Path::parse)
                .collect::<std::result::Result<_, _>>()?,
            objects,
            extensions: Default::default(),
        },
        page_token: if truncated { last_key } else { None },
    })
}

#[test]
fn a_synthesized_epoch_stamp_reads_as_no_timestamp_at_all() {
    let after_epoch = |ms| UNIX_EPOCH + Duration::from_millis(ms);
    assert_eq!(last_modified_at_ms(UNIX_EPOCH), None);
    assert_eq!(
        last_modified_at_ms(UNIX_EPOCH - Duration::from_millis(1)),
        None
    );
    assert_eq!(last_modified_at_ms(after_epoch(1)), Some(1));
    assert_eq!(
        last_modified_at_ms(after_epoch(1_754_000_000_000)),
        Some(1_754_000_000_000)
    );
}

#[test]
fn compare_tokens_populate_only_the_matching_provider_field() {
    for (kind, expected_etag, expected_version) in [
        (ConfiguredObjectStoreKind::AwsS3, Some("token"), None),
        (ConfiguredObjectStoreKind::GcpGcs, None, Some("token")),
    ] {
        let flaky = Arc::new(FlakyStore::default());
        let store = provider_store(Arc::clone(&flaky), flaky, kind, None);
        let provider_store::PutMode::Update(version) =
            store.map_put_mode(PutMode::CompareAndSwap {
                expected_etag: "token".to_owned(),
            })
        else {
            panic!("compare-and-swap maps to an update")
        };

        assert_eq!(version.e_tag.as_deref(), expected_etag);
        assert_eq!(version.version.as_deref(), expected_version);
    }
}

#[tokio::test]
async fn an_etag_store_sends_the_etag_as_if_none_match() {
    let flaky = Arc::new(FlakyStore::default());
    let store = retrying_store(Arc::clone(&flaky));
    let key = "namespaces/demo/boundary.json";
    let last_header = || flaky.if_none_match.lock().expect("read options").pop();
    assert_eq!(
        store.get_if_changed(key, "\"0\"").await.expect("read"),
        None
    );
    assert_eq!(last_header(), Some(Some("\"0\"".to_owned())));

    let first = store
        .put_if_absent(key, Bytes::from_static(b"one"))
        .await
        .expect("create");
    let first_etag = first.etag.expect("etag");
    assert_eq!(
        store.get_if_changed(key, &first_etag).await.expect("read"),
        Some(GetIfChangedOutcome::Unchanged)
    );
    assert_eq!(last_header(), Some(Some(first_etag.clone())));

    let second = store
        .compare_and_swap(key, &first_etag, Bytes::from_static(b"two"))
        .await
        .expect("replace");
    let Some(GetIfChangedOutcome::Changed(body)) =
        store.get_if_changed(key, &first_etag).await.expect("read")
    else {
        panic!("a replaced object returns its body");
    };
    assert_eq!(last_header(), Some(Some(first_etag)));
    assert_eq!(body.bytes, Bytes::from_static(b"two"));
    assert_eq!(body.metadata.etag, second.etag);
}

#[tokio::test]
async fn a_generation_store_reads_in_full_and_compares_the_generation() {
    let flaky = Arc::new(FlakyStore {
        versions_from_etags: true,
        ..Default::default()
    });
    let store = provider_store(
        Arc::clone(&flaky),
        Arc::clone(&flaky),
        ConfiguredObjectStoreKind::GcpGcs,
        None,
    );
    let key = "namespaces/demo/boundary.json";
    store
        .put_if_absent(key, Bytes::from_static(b"one"))
        .await
        .expect("create");
    let first = store.get_with_metadata(key).await.expect("read");
    let first_generation = first.expect("object").metadata.etag.expect("generation");
    assert_eq!(
        store
            .get_if_changed(key, &first_generation)
            .await
            .expect("read"),
        Some(GetIfChangedOutcome::Unchanged)
    );

    store.delete(key).await.expect("delete");
    store
        .put_if_absent(key, Bytes::from_static(b"two"))
        .await
        .expect("create again");
    let Some(GetIfChangedOutcome::Changed(body)) = store
        .get_if_changed(key, &first_generation)
        .await
        .expect("read")
    else {
        panic!("a replaced object returns its body");
    };
    assert_eq!(body.bytes, Bytes::from_static(b"two"));
    assert_ne!(body.metadata.etag, Some(first_generation));
    let headers = flaky.if_none_match.lock().expect("read options").clone();
    assert!(!headers.is_empty());
    assert!(headers.iter().all(Option::is_none), "{headers:?}");
}

#[tokio::test]
async fn partial_key_prefixes_keep_times_and_resume_across_provider_pages() {
    let store = memory_store();
    let prefix = "objects/con_a";
    let keys: Vec<_> = (0..1002)
        .map(|index| format!("{prefix}{index:04x}/full"))
        .collect();
    for key in keys
        .iter()
        .map(String::as_str)
        .chain(["objects/con_b/full"])
    {
        store
            .put_if_absent(key, Bytes::from_static(b"listed"))
            .await
            .expect("put");
    }
    let entries: Vec<_> = store
        .list_entries_from_stream(prefix, None)
        .try_collect()
        .await
        .expect("list pages");
    assert_eq!(
        entries.iter().map(|entry| &entry.key).collect::<Vec<_>>(),
        keys.iter().collect::<Vec<_>>()
    );
    for entry in entries {
        let head = store.head(&entry.key).await.expect("head").expect("exists");
        assert_eq!(entry.last_modified_at_ms, head.last_modified_at_ms);
        assert_eq!(entry.size_bytes, head.size_bytes);
    }
    let remaining: Vec<_> = store
        .list_prefix_from_stream(prefix, Some(&keys[999]))
        .try_collect()
        .await
        .expect("resume");
    assert_eq!(remaining, keys[1000..]);
}

#[tokio::test]
async fn child_prefix_pages_resume_after_whole_children_inside_the_key_prefix() {
    let flaky = Arc::new(FlakyStore::default());
    let store = retrying_store(Arc::clone(&flaky));
    for key in [
        "namespaces/a/boundary.json",
        "namespaces/a1",
        "namespaces/b/boundary.json",
        "namespaces/b/wal/00000000000000000001.wal.zst",
        "namespaces/b0/boundary.json",
        "namespaces/c/boundary.json",
    ] {
        store
            .put_if_absent(key, Bytes::from_static(b"listed"))
            .await
            .expect("put");
    }
    provider_store::ObjectStoreExt::put(
        &flaky.inner,
        &Path::from("tenant-b/namespaces/z/boundary.json"),
        PutPayload::from_static(b"outside"),
    )
    .await
    .expect("put outside the key prefix");

    let first = child_page(&store, "namespaces/", None, 2).await;
    assert_eq!(first.items, ["namespaces/a/"]);
    assert_eq!(first.next_cursor.as_deref(), Some("namespaces/a1"));
    let second = child_page(&store, "namespaces/", Some("namespaces/a1"), 2).await;
    assert_eq!(second.items, ["namespaces/b/", "namespaces/b0/"]);
    assert_eq!(second.next_cursor.as_deref(), Some("namespaces/b0/"));
    let third = child_page(&store, "namespaces/", Some("namespaces/b0/"), 2).await;
    assert_eq!(third.items, ["namespaces/c/"]);
    assert_eq!(third.next_cursor, None);

    let after_child = child_page(&store, "namespaces/", Some("namespaces/b/"), 2).await;
    assert_eq!(after_child.items, ["namespaces/b0/", "namespaces/c/"]);
    let objects_only = child_page(&store, "namespaces/", Some("namespaces/a/"), 1).await;
    assert!(objects_only.items.is_empty());
    assert_eq!(objects_only.next_cursor.as_deref(), Some("namespaces/a1"));
    let root = child_page(&store, "", None, 10).await;
    assert_eq!(root.items, ["namespaces/"]);
}

/// Azure starts a listing at the cursor itself and object_store removes
/// that entry, so a page of one is empty when the cursor is an object.
#[tokio::test]
async fn child_prefix_pages_follow_the_token_past_an_empty_page() {
    let flaky = Arc::new(FlakyStore {
        inclusive_list_offset: true,
        ..FlakyStore::default()
    });
    let store = retrying_store(Arc::clone(&flaky));
    for key in ["namespaces/a", "namespaces/b/boundary.json"] {
        store
            .put_if_absent(key, Bytes::from_static(b"listed"))
            .await
            .expect("put");
    }
    let first = child_page(&store, "namespaces/", None, 1).await;
    assert!(first.items.is_empty(), "{first:?}");
    assert_eq!(first.next_cursor.as_deref(), Some("namespaces/a"));
    let second = child_page(&store, "namespaces/", Some("namespaces/a"), 1).await;
    assert_eq!(second.items, ["namespaces/b/"]);
    assert_eq!(second.next_cursor, None);
}

async fn child_page(
    store: &ProviderObjectStore,
    prefix: &str,
    start_after: Option<&str>,
    limit: u32,
) -> Page<String, String> {
    let limit = EffectiveLimit::new(std::num::NonZeroU32::new(limit).expect("nonzero limit"));
    store
        .list_child_prefixes(prefix, start_after, limit)
        .await
        .expect("list child prefixes")
}

#[tokio::test]
async fn ranged_reads_match_the_reference_contract_in_one_round_trip() {
    let store = memory_store();
    let key = "namespaces/demo/segments/seg_abc.sst";
    store
        .put_if_absent(key, Bytes::from_static(b"0123456789"))
        .await
        .expect("put");

    let range = |start, end| {
        Some(ByteRange {
            start_inclusive: start,
            end_exclusive: end,
        })
    };

    // In-bounds slice.
    assert_eq!(
        store.get(key, range(2, 6)).await.expect("bounded"),
        Some(Bytes::from_static(b"2345"))
    );
    // An end past the object clamps.
    assert_eq!(
        store.get(key, range(6, 99)).await.expect("clamped"),
        Some(Bytes::from_static(b"6789"))
    );
    // Reading at the exact end is empty, not an error.
    assert_eq!(
        store.get(key, range(10, 12)).await.expect("at end"),
        Some(Bytes::new())
    );
    // A start past the end is an invalid range.
    assert_matches!(
        store.get(key, range(11, 12)).await,
        Err(ObjectStoreError::InvalidRange { .. })
    );
    // An inverted range is rejected without any store call.
    assert_matches!(
        store.get(key, range(6, 2)).await,
        Err(ObjectStoreError::InvalidRange { .. })
    );
    // Zero-length reads answer from existence and size alone.
    assert_eq!(
        store.get(key, range(4, 4)).await.expect("zero length"),
        Some(Bytes::new())
    );

    // A missing object is `Ok(None)` however the request is shaped,
    // matching what the unranged read and the local provider return.
    let missing = "namespaces/demo/segments/seg_missing.sst";
    assert_eq!(
        store.get(missing, range(0, 4)).await.expect("missing"),
        None
    );
    assert_eq!(
        store.get(missing, range(3, 3)).await.expect("missing zero"),
        None
    );
}

#[tokio::test]
async fn provider_store_enforces_create_and_cas_preconditions() {
    let store = memory_store();
    let key = "namespaces/demo/boundary.json";
    let first = store
        .put_if_absent(key, Bytes::from_static(b"one"))
        .await
        .expect("first put");

    assert_matches!(
        store.put_if_absent(key, Bytes::from_static(b"two")).await,
        Err(ObjectStoreError::PreconditionFailed { .. })
    );
    assert_matches!(
        store
            .compare_and_swap(key, "stale", Bytes::from_static(b"two"))
            .await,
        Err(ObjectStoreError::PreconditionFailed { .. })
    );
    assert_matches!(
        store
            .compare_and_swap(
                "namespaces/missing/boundary.json",
                "missing",
                Bytes::from_static(b"two")
            )
            .await,
        Err(ObjectStoreError::PreconditionFailed { .. })
    );
    let etag = first.etag.expect("etag");
    store
        .compare_and_swap(key, &etag, Bytes::from_static(b"two"))
        .await
        .expect("cas");
    assert_eq!(
        store.get(key, None).await.expect("get"),
        Some(Bytes::from_static(b"two"))
    );
}

#[tokio::test]
async fn provider_stream_reports_invalid_prefix() {
    let store = memory_store();
    let mut stream = store.list_prefix_stream("../");
    assert_matches!(
        stream.next().await,
        Some(Err(ObjectStoreError::InvalidKey { .. }))
    );
}

use provider_store::{CopyOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions};
use std::assert_matches;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Debug)]
enum WriteScript {
    FailWithoutWriting,
    WriteThenFail,
    FailAuth,
}

/// Provider double that fails scripted attempts before delegating to an
/// in-memory store, so retry behavior is observable per attempt.
#[derive(Default)]
struct FlakyStore {
    inner: InMemory,
    put_script: Mutex<VecDeque<WriteScript>>,
    delete_script: Arc<Mutex<VecDeque<WriteScript>>>,
    puts: AtomicUsize,
    gets: AtomicUsize,
    heads: AtomicUsize,
    deletes: Arc<AtomicUsize>,
    missing_checksum: Mutex<Option<String>>,
    /// The `If-None-Match` value of every read, in order.
    if_none_match: Mutex<Vec<Option<String>>>,
    /// Reports each read's ETag as its version too, as GCS reports a
    /// generation, because the in-memory store reports no version.
    versions_from_etags: bool,
    /// Starts a listing at the offset itself and then removes that entry,
    /// as object_store does for Azure, whose `startFrom` is inclusive.
    inclusive_list_offset: bool,
}

impl fmt::Debug for FlakyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlakyStore").finish_non_exhaustive()
    }
}

impl fmt::Display for FlakyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlakyStore")
    }
}

#[async_trait]
impl provider_store::ObjectStore for FlakyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> provider_store::Result<PutResult> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let script = self.put_script.lock().expect("put script").pop_front();
        match script {
            Some(WriteScript::FailWithoutWriting) => Err(transport_glitch()),
            Some(WriteScript::WriteThenFail) => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(transport_glitch())
            }
            Some(WriteScript::FailAuth) => Err(auth_rejection(location)),
            None => self.inner.put_opts(location, payload, opts).await,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> provider_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> provider_store::Result<GetResult> {
        let reads = if options.head {
            &self.heads
        } else {
            &self.gets
        };
        reads.fetch_add(1, Ordering::SeqCst);
        self.if_none_match
            .lock()
            .expect("read options")
            .push(options.if_none_match.clone());
        let mut result = self.inner.get_opts(location, options).await?;
        if self.versions_from_etags {
            result.meta.version = result.meta.e_tag.clone();
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, provider_store::Result<Path>>,
    ) -> BoxStream<'static, provider_store::Result<Path>> {
        let inner = self.inner.clone();
        let deletes = Arc::clone(&self.deletes);
        let delete_script = Arc::clone(&self.delete_script);
        locations
            .then(move |location| {
                let inner = inner.clone();
                let deletes = Arc::clone(&deletes);
                let delete_script = Arc::clone(&delete_script);
                async move {
                    let location = location?;
                    deletes.fetch_add(1, Ordering::SeqCst);
                    let script = delete_script.lock().expect("delete script").pop_front();
                    match script {
                        Some(WriteScript::FailWithoutWriting) => Err(transport_glitch()),
                        Some(WriteScript::WriteThenFail) => {
                            inner.delete(&location).await?;
                            Err(transport_glitch())
                        }
                        Some(WriteScript::FailAuth) => Err(auth_rejection(&location)),
                        None => inner.delete(&location).await.map(|()| location),
                    }
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, provider_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> provider_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> provider_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl PaginatedListStore for FlakyStore {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> provider_store::Result<PaginatedListResult> {
        memory_page(&self.inner, prefix, options, self.inclusive_list_offset).await
    }
}

fn retrying_store(flaky: Arc<FlakyStore>) -> ProviderObjectStore {
    provider_store(
        Arc::clone(&flaky),
        flaky,
        ConfiguredObjectStoreKind::LocalFs,
        Some("tenant-a"),
    )
}

fn script_puts(flaky: &FlakyStore, script: impl IntoIterator<Item = WriteScript>) {
    flaky.put_script.lock().expect("put script").extend(script);
}

#[tokio::test]
async fn an_immutable_write_creates_at_every_size_and_stores_its_checksum() {
    let flaky = Arc::new(FlakyStore::default());
    let store = multipart_test_store(Arc::clone(&flaky));
    for (key, payload, multipart) in [
        (
            "namespaces/demo/uploads/upl_2.json",
            multipart_payload(15),
            false,
        ),
        (
            MULTIPART_KEY,
            multipart_payload(MULTIPART_TEST_THRESHOLD + 1),
            true,
        ),
    ] {
        let written = store
            .put_immutable_verified(key, Bytes::from(payload.clone()))
            .await
            .expect("immutable write");
        let head = store.head(key).await.expect("head").expect("written");
        // A multipart write's checksum comes from the test signer, so
        // only the single put's checksum is compared with the payload.
        if !multipart {
            assert_eq!(written.checksum, Some(Checksum::crc64nvme(&payload)));
        }
        assert_eq!(head.checksum, written.checksum);
    }

    assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn an_occupied_key_is_checked_by_one_head_of_its_stored_checksum() {
    let flaky = Arc::new(FlakyStore::default());
    let store = retrying_store(Arc::clone(&flaky));
    let ours = Bytes::from_static(b"ours");
    let identical = "namespaces/demo/segments/seg_1.sst";
    let different = "namespaces/demo/segments/seg_2.sst";
    let without_checksum = "namespaces/demo/segments/seg_3.sst";
    store
        .put_if_absent(identical, ours.clone())
        .await
        .expect("seed identical");
    store
        .put_if_absent(different, Bytes::from_static(b"theirs"))
        .await
        .expect("seed different");
    seed_scoped_object(&flaky, without_checksum, ours.clone()).await;
    *flaky.missing_checksum.lock().expect("checksum fault") = Some(without_checksum.to_owned());
    flaky.puts.store(0, Ordering::SeqCst);

    let accepted = store
        .put_immutable_verified(identical, ours.clone())
        .await
        .expect("an equal stored checksum identifies this object");
    assert_eq!(accepted.checksum, Some(Checksum::crc64nvme(&ours)));
    assert_matches!(
        store.put_immutable_verified(different, ours.clone()).await,
        Err(ObjectStoreError::DifferentObject { object_key })
            if object_key == different
    );
    assert_matches!(
        store.put_immutable_verified(without_checksum, ours).await,
        Err(ObjectStoreError::StoredChecksumMissing { object_key })
            if object_key == without_checksum
    );
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 3);
    assert_eq!(flaky.heads.load(Ordering::SeqCst), 3);
    assert_eq!(
        flaky.gets.load(Ordering::SeqCst),
        0,
        "no bytes are read back"
    );
}

#[tokio::test(start_paused = true)]
async fn a_successful_write_whose_answer_was_lost_resolves_through_its_stored_checksum() {
    let flaky = Arc::new(FlakyStore::default());
    let store = retrying_store(Arc::clone(&flaky));
    script_puts(&flaky, [WriteScript::WriteThenFail]);
    let key = "namespaces/demo/uploads/upl_3.json";

    store
        .put_immutable_verified(key, Bytes::from_static(b"payload"))
        .await
        .expect("the retry finds its own successful write");

    assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
    assert_eq!(flaky.heads.load(Ordering::SeqCst), 1);
    assert_eq!(flaky.gets.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn immutable_transport_failures_retry_inside_the_operation() {
    let flaky = Arc::new(FlakyStore::default());
    let store = retrying_store(Arc::clone(&flaky));
    script_puts(
        &flaky,
        [
            WriteScript::FailWithoutWriting,
            WriteScript::FailWithoutWriting,
        ],
    );
    let key = "namespaces/demo/uploads/upl_4.json";

    store
        .put_immutable_verified(key, Bytes::from_static(b"payload"))
        .await
        .expect("transient failures are retried");

    assert_eq!(flaky.puts.load(Ordering::SeqCst), 3);
    assert_eq!(flaky.gets.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn immutable_transport_failure_surfaces_after_the_retry_budget() {
    let flaky = Arc::new(FlakyStore::default());
    let store = retrying_store(Arc::clone(&flaky));
    script_puts(&flaky, (0..11).map(|_| WriteScript::FailWithoutWriting));
    let key = "namespaces/demo/uploads/upl_9.json";

    let error = store
        .put_immutable_verified(key, Bytes::from_static(b"payload"))
        .await
        .expect_err("persistent failure exhausts the immutable retry budget");

    assert_matches!(error, ObjectStoreError::Transport { .. });
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 11);
    assert_eq!(flaky.heads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn compare_and_swap_never_retries_transport_failures() {
    let flaky = Arc::new(FlakyStore::default());
    let store = retrying_store(Arc::clone(&flaky));
    let key = "namespaces/demo/boundary.json";
    let seeded = store
        .put_if_absent(key, Bytes::from_static(b"one"))
        .await
        .expect("seed head");
    let etag = seeded.etag.expect("etag");
    script_puts(&flaky, [WriteScript::FailWithoutWriting]);

    let error = store
        .compare_and_swap(key, &etag, Bytes::from_static(b"two"))
        .await
        .expect_err("compare-and-swap surfaces the transport failure");

    assert_matches!(error, ObjectStoreError::Transport { .. });
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
    assert_eq!(
        store.get(key, None).await.expect("get"),
        Some(Bytes::from_static(b"one"))
    );
}

#[tokio::test]
async fn a_conditional_put_goes_once_to_the_client_that_never_resends() {
    let resending = Arc::new(FlakyStore::default());
    let one_attempt = Arc::new(FlakyStore {
        inner: resending.inner.clone(),
        ..FlakyStore::default()
    });
    let store = provider_store(
        Arc::clone(&resending),
        Arc::clone(&one_attempt),
        ConfiguredObjectStoreKind::LocalFs,
        None,
    );
    script_puts(&one_attempt, [WriteScript::WriteThenFail]);
    let key = "namespaces/demo/uploads/upl_11.json";

    let error = store
        .put_if_absent(key, Bytes::from_static(b"claim"))
        .await
        .expect_err("the successful write's lost answer reaches the caller");
    assert_matches!(error, ObjectStoreError::Transport { .. });
    assert_eq!(one_attempt.puts.load(Ordering::SeqCst), 1);

    assert_eq!(
        store.get(key, None).await.expect("get"),
        Some(Bytes::from_static(b"claim"))
    );
    assert_eq!(resending.puts.load(Ordering::SeqCst), 0);
    assert_eq!(resending.gets.load(Ordering::SeqCst), 1);
    assert_eq!(one_attempt.puts.load(Ordering::SeqCst), 1);
    assert_eq!(one_attempt.gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn non_transport_provider_errors_are_not_retried() {
    let flaky = Arc::new(FlakyStore::default());
    let store = retrying_store(Arc::clone(&flaky));
    script_puts(&flaky, [WriteScript::FailAuth]);
    let key = "namespaces/demo/uploads/upl_5.json";

    let error = store
        .put_immutable_verified(key, Bytes::from_static(b"payload"))
        .await
        .expect_err("auth rejection surfaces immediately");

    assert_matches!(error, ObjectStoreError::PermissionDenied { .. });
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
}

/// Checks that a delete is one call to the client that retries its own
/// requests, so a failure from that client other than a timeout returns
/// at once and isn't classified as retryable.
#[tokio::test]
async fn a_delete_is_one_call_to_the_client_that_retries_it() {
    let flaky = Arc::new(FlakyStore::default());
    let store = retrying_store(Arc::clone(&flaky));
    let key = "namespaces/demo/uploads/upl_6.json";
    store
        .put_if_absent(key, Bytes::from_static(b"payload"))
        .await
        .expect("seed object");

    for script in [WriteScript::FailWithoutWriting, WriteScript::WriteThenFail] {
        flaky.deletes.store(0, Ordering::SeqCst);
        flaky
            .delete_script
            .lock()
            .expect("delete script")
            .push_back(script);
        let error = store
            .delete(key)
            .await
            .expect_err("the retrying client's failure is the delete's");
        assert_eq!(error.kind(), crate::ObjectStoreErrorKind::Other);
        assert_eq!(flaky.deletes.load(Ordering::SeqCst), 1);
    }
    assert!(store.head(key).await.expect("head").is_none());
    store
        .delete(key)
        .await
        .expect("a delete of a missing object succeeds");
}

#[tokio::test(start_paused = true)]
async fn a_retried_call_reports_its_attempts_to_the_metrics_wrapper() {
    let flaky = Arc::new(FlakyStore::default());
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let store = InstrumentedObjectStore::new(retrying_store(Arc::clone(&flaky)), recorder.clone());

    store
        .put_if_absent(
            "namespaces/demo/uploads/upl_8.json",
            Bytes::from_static(b"payload"),
        )
        .await
        .expect("one conditional put");
    script_puts(&flaky, [WriteScript::FailWithoutWriting]);
    store
        .put_immutable_verified(
            "namespaces/demo/segments/seg_8.sst",
            Bytes::from_static(b"payload"),
        )
        .await
        .expect("the immutable write converges");

    let attempts: Vec<_> = recorder
        .samples()
        .iter()
        .map(|sample| sample.attempts)
        .collect();
    assert_eq!(
        attempts,
        [1, 2],
        "a call that never retried made one attempt, and the retried write two"
    );
}

const MULTIPART_TEST_THRESHOLD: usize = 1024;
const MULTIPART_TEST_PART: usize = 512;
const MULTIPART_KEY: &str = "namespaces/demo/content/con_5bsqqg28t5cy4tqkff04hmasw9";

/// Retrying store with a test-sized multipart geometry: payloads of
/// 1024+ bytes go multipart in 512-byte parts.
fn multipart_test_store(flaky: Arc<FlakyStore>) -> ProviderObjectStore {
    retrying_store(flaky)
        .multipart_geometry(MULTIPART_TEST_THRESHOLD as u64, MULTIPART_TEST_PART as u64)
}

fn multipart_payload(len: usize) -> Vec<u8> {
    (0..251_u8).cycle().take(len).collect()
}

/// Places an object at `key` directly on the inner store, bypassing the
/// scripted transport and its counters.
async fn seed_scoped_object(flaky: &FlakyStore, key: &str, bytes: Bytes) {
    provider_store::ObjectStore::put_opts(
        &flaky.inner,
        &Path::from(format!("tenant-a/{key}")),
        bytes.into(),
        PutOptions::default(),
    )
    .await
    .expect("seed object");
}

#[derive(Default)]
struct CountingSigner {
    part_windows: Mutex<Vec<usize>>,
}

impl CountingSigner {
    fn part_windows(&self) -> Vec<usize> {
        self.part_windows.lock().expect("part windows").clone()
    }
}

#[async_trait]
impl ProviderSigner for CountingSigner {
    async fn head_metadata(&self, _key: &str) -> Result<Option<ObjectMetadata>> {
        Ok(None)
    }

    async fn create_checked(
        &self,
        _key: &str,
        _bytes: Bytes,
        _checksum: &Checksum,
    ) -> Result<ObjectMetadata> {
        Err(ObjectStoreError::Unsupported("test create"))
    }

    async fn put_if_absent(
        &self,
        _key: &str,
        head: Bytes,
        mut rest: PartReader<'_>,
        part_window: usize,
    ) -> Result<ObjectMetadata> {
        self.part_windows
            .lock()
            .expect("part windows")
            .push(part_window);
        let mut checksum = loonfs_types::ChecksumDigest::new(ChecksumAlgorithm::Crc64nvme);
        checksum.update(&head);
        let mut size_bytes = head.len() as u64;
        while let Some(part) = rest.next_part().await? {
            checksum.update(&part);
            size_bytes += part.len() as u64;
        }
        Ok(ObjectMetadata {
            etag: Some("\"controller\"".to_owned()),
            version: None,
            size_bytes,
            last_modified_at_ms: None,
            checksum: Some(checksum.finish()),
        })
    }

    async fn assemble(
        &self,
        _key: &str,
        _sources: &[AssemblySource],
        _tail: Vec<Bytes>,
        _expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        Err(ObjectStoreError::Unsupported("test assembly"))
    }
}

#[tokio::test]
async fn a_publication_is_one_put_and_only_verified_creates_get_the_part_window() {
    let flaky = Arc::new(FlakyStore::default());
    let controller = Arc::new(CountingSigner::default());
    let store = multipart_test_store(Arc::clone(&flaky))
        .signer(Arc::clone(&controller) as Arc<dyn ProviderSigner>);
    let payload = multipart_payload(1300);

    store
        .put_if_absent(
            "namespaces/demo/wal/000000000000000001.wal",
            Bytes::from(payload.clone()),
        )
        .await
        .expect("a publication is one conditional request at every size");
    assert!(controller.part_windows().is_empty());
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

    let written = store
        .put_immutable_verified(MULTIPART_KEY, Bytes::from(payload.clone()))
        .await
        .expect("a large verified write creates through the controller");
    assert_eq!(
        controller.part_windows(),
        [PROVIDER_MULTIPART_PARTS_IN_FLIGHT]
    );
    assert_eq!(written.size_bytes, payload.len() as u64);
    assert_eq!(written.checksum, Some(Checksum::crc64nvme(&payload)));
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

    let uploaded = store
        .put_streamed(
            MULTIPART_KEY,
            streamed(&payload, 100),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("a large streamed upload creates through the controller");
    assert_eq!(uploaded, payload.len() as u64);
    assert_eq!(
        controller.part_windows(),
        [PROVIDER_MULTIPART_PARTS_IN_FLIGHT, 1],
        "a client's upload keeps one part in flight"
    );
}

#[tokio::test]
async fn large_create_if_absent_stays_single_request() {
    let flaky = Arc::new(FlakyStore::default());
    let store = multipart_test_store(Arc::clone(&flaky));
    let payload = multipart_payload(1300);

    store
        .put_if_absent(MULTIPART_KEY, Bytes::from(payload.clone()))
        .await
        .expect("create absent large object");
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

    let error = store
        .put_if_absent(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
        .await
        .expect_err("existing object fails the create precondition");
    assert_matches!(error, ObjectStoreError::PreconditionFailed { .. });
    assert_eq!(
        flaky.heads.load(Ordering::SeqCst),
        0,
        "the conflict is decided by the provider precondition, not a pre-check"
    );
}

/// Cuts a payload into stream chunks that deliberately do not line up
/// with the part size, since a caller's chunk boundaries never do.
fn streamed(payload: &[u8], chunk_bytes: usize) -> ByteStream {
    let chunks: Vec<Bytes> = payload
        .chunks(chunk_bytes)
        .map(Bytes::copy_from_slice)
        .collect();
    stream::iter(chunks.into_iter().map(Ok)).boxed()
}

#[tokio::test]
async fn a_streamed_put_cuts_the_stream_into_parts_and_preserves_bytes() {
    let flaky = Arc::new(FlakyStore::default());
    let store = multipart_test_store(Arc::clone(&flaky));
    // Parts of 512, 512, and 276 bytes, delivered in 100-byte chunks so
    // every part boundary falls inside a chunk.
    let payload = multipart_payload(1300);

    let size_bytes = store
        .put_streamed(
            MULTIPART_KEY,
            streamed(&payload, 100),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("streamed multipart put");

    assert_eq!(size_bytes, 1300);
    assert_eq!(
        store.get(MULTIPART_KEY, None).await.expect("get"),
        Some(Bytes::from(payload))
    );
}

#[tokio::test]
async fn a_short_streamed_put_is_one_request_that_keeps_its_precondition() {
    let flaky = Arc::new(FlakyStore::default());
    let store = multipart_test_store(Arc::clone(&flaky));
    let payload = multipart_payload(MULTIPART_TEST_PART - 1);

    store
        .put_streamed(
            MULTIPART_KEY,
            streamed(&payload, 64),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("short streamed put");
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

    let error = store
        .put_streamed(
            MULTIPART_KEY,
            streamed(&payload, 64),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect_err("the key is taken and create-only means it");
    assert_matches!(error, ObjectStoreError::PreconditionFailed { .. });
    assert_eq!(
        store.get(MULTIPART_KEY, None).await.expect("get"),
        Some(Bytes::from(payload))
    );
}

#[tokio::test]
async fn a_streamed_compare_and_swap_longer_than_one_part_is_unsupported() {
    let flaky = Arc::new(FlakyStore::default());
    let store = multipart_test_store(Arc::clone(&flaky));
    let payload = multipart_payload(1300);

    let error = store
        .put_streamed(
            MULTIPART_KEY,
            streamed(&payload, 100),
            PutMode::CompareAndSwap {
                expected_etag: "\"etag\"".to_owned(),
            },
        )
        .await
        .expect_err("no store emulates a multipart compare-and-swap");

    let ObjectStoreError::Unsupported(capability) = error else {
        panic!("expected Unsupported, got {error:?}");
    };
    assert_eq!(capability, "streamed compare-and-swap longer than one part");
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 0);
    assert_eq!(flaky.heads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_empty_streamed_put_writes_an_empty_object() {
    let flaky = Arc::new(FlakyStore::default());
    let store = multipart_test_store(Arc::clone(&flaky));

    let size_bytes = store
        .put_streamed(
            MULTIPART_KEY,
            stream::empty().boxed(),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("empty streamed put");

    assert_eq!(size_bytes, 0);
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.get(MULTIPART_KEY, None).await.expect("get"),
        Some(Bytes::new())
    );
}

#[tokio::test]
async fn a_streamed_put_that_fails_mid_stream_writes_nothing() {
    let flaky = Arc::new(FlakyStore::default());
    let store = multipart_test_store(Arc::clone(&flaky));
    let head = multipart_payload(MULTIPART_TEST_PART);
    let body = stream::iter([
        Ok(Bytes::from(head)),
        Ok(Bytes::from(multipart_payload(64))),
        Err(ObjectStoreError::transport(
            MULTIPART_KEY,
            "the client stopped sending",
        )),
    ])
    .boxed();

    let error = store
        .put_streamed(MULTIPART_KEY, body, PutMode::CreateIfAbsent)
        .await
        .expect_err("a payload that stops is not a write");

    assert_matches!(error, ObjectStoreError::Transport { .. });
    assert_eq!(flaky.puts.load(Ordering::SeqCst), 0);
    assert_eq!(store.get(MULTIPART_KEY, None).await.expect("get"), None);
}
