use loon_client::upload::{upload_small_file_from_path, UploadedBlockObject};
use loon_model::build_uploaded_content;
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::ObjectStore;
use loon_testkit::scenario::Scenario;
use loon_types::{decode_content_manifest_json, ContentManifestPayload, NamespaceId};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[test]
fn small_file_upload_fixture_writes_block_and_manifest() {
    let scenario = load_fixture("client/small_file_upload_writes_block_and_manifest.yaml");
    let initial: UploadInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<UploadActionEnvelope> = scenario.decode_actions().expect("decode actions");
    let expect: UploadExpect = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("small-file-upload");
    let source_path = temp_dir.path().join(&initial.local_file.relative_path);
    let store_root = temp_dir.path().join("objectstore");
    let source_bytes = initial.local_file.content_utf8.as_bytes();

    assert_eq!(actions.len(), 1, "fixture should contain one upload action");
    assert!(actions[0].upload_small_file);

    fs::create_dir_all(source_path.parent().expect("source file parent"))
        .expect("create source file parent");
    fs::write(&source_path, source_bytes).expect("write source file");
    fs::create_dir_all(&store_root).expect("create local object store root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let uploaded = upload_small_file_from_path(&store, &initial.namespace_id, &source_path)
        .expect("upload small file");
    let model = build_uploaded_content(initial.namespace_id.clone(), source_bytes)
        .expect("build model upload result");

    assert_eq!(uploaded.file_size_bytes, expect.file_size_bytes);
    assert_eq!(uploaded.file_digest_sha256, expect.file_digest_sha256);
    assert_eq!(
        uploaded.content_manifest_digest,
        expect.content_manifest_digest
    );
    assert_eq!(uploaded.manifest_object_key, expect.manifest_object_key);
    assert_eq!(uploaded.block_objects, expect.block_objects);
    assert_eq!(uploaded.file_digest_sha256, model.file_digest_sha256);
    assert_eq!(
        uploaded.content_manifest_digest,
        model.content_manifest_digest
    );
    assert_eq!(uploaded.manifest_envelope, model.manifest_envelope);

    for expected_block in &expect.block_objects {
        let stored = store
            .get(&expected_block.object_key, None)
            .expect("read stored block")
            .expect("stored block should exist");
        assert_eq!(stored, source_bytes);
    }

    let manifest_bytes = store
        .get(&expect.manifest_object_key, None)
        .expect("read stored manifest")
        .expect("stored manifest should exist");
    let manifest = decode_content_manifest_json(&manifest_bytes).expect("decode stored manifest");
    assert_eq!(manifest, uploaded.manifest_envelope);
    assert_eq!(manifest.payload, expect.manifest_payload);

    let uploaded_again = upload_small_file_from_path(&store, &initial.namespace_id, &source_path)
        .expect("re-upload small file");
    assert_eq!(uploaded_again, uploaded);
}

#[derive(Debug, Deserialize)]
struct UploadInitial {
    namespace_id: NamespaceId,
    local_file: FixtureLocalFile,
}

#[derive(Debug, Deserialize)]
struct FixtureLocalFile {
    relative_path: PathBuf,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
struct UploadActionEnvelope {
    upload_small_file: bool,
}

#[derive(Debug, Deserialize)]
struct UploadExpect {
    file_size_bytes: u64,
    file_digest_sha256: String,
    content_manifest_digest: String,
    manifest_object_key: String,
    manifest_payload: ContentManifestPayload,
    block_objects: Vec<ExpectedBlockObject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct ExpectedBlockObject {
    object_key: String,
    content_digest_sha256: String,
    plaintext_size_bytes: u64,
}

impl PartialEq<ExpectedBlockObject> for UploadedBlockObject {
    fn eq(&self, other: &ExpectedBlockObject) -> bool {
        self.object_key == other.object_key
            && self.content_digest_sha256 == other.content_digest_sha256
            && self.plaintext_size_bytes == other.plaintext_size_bytes
    }
}

impl PartialEq<UploadedBlockObject> for ExpectedBlockObject {
    fn eq(&self, other: &UploadedBlockObject) -> bool {
        other == self
    }
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

type TestDir = loon_testkit::tempdir::TestDir;
