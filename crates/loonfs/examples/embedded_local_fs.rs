use loon_objectstore::fs::LocalFsStore;
use loonfs::{
    CreateNamespaceOptions, Fs, NamespaceId, PutFileBehavior, PutFileOptions, SharedObjectStore,
};
use std::sync::Arc;

#[allow(clippy::print_stdout)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This example intentionally prints the file content it just read.
    let root = std::env::temp_dir().join("loonfs-embedded-local-fs-example");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(root)?);
    let fs = Fs::builder(store).writer_id("embedded-example").build()?;

    let namespace_id = NamespaceId::parse("demo")?;
    fs.create_namespace(
        &namespace_id,
        CreateNamespaceOptions {
            allow_existing: true,
        },
    )
    .await?;
    fs.put_file_bytes(
        &namespace_id,
        "/hello.txt",
        b"hello from embedded LoonFS\n",
        PutFileOptions {
            behavior: PutFileBehavior::ReplaceExisting,
            commit_id: None,
        },
    )
    .await?;

    let file = fs.read_file_bytes(&namespace_id, "/hello.txt").await?;
    println!("{}", String::from_utf8_lossy(&file.bytes));
    Ok(())
}
