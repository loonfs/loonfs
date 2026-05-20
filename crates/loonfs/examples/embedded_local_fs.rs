use loon_objectstore::fs::LocalFsStore;
use loonfs::{
    CreateNamespaceOptions, Fs, NamespaceId, PutFileBehavior, PutFileOptions, SharedObjectStore,
};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join("loonfs-embedded-local-fs-example");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(root)?);
    let fs = Fs::builder(store).writer_id("embedded-example").build()?;

    let namespace_id = NamespaceId::parse("demo")?;
    fs.create_namespace(
        &namespace_id,
        CreateNamespaceOptions {
            allow_existing: true,
        },
    )?;
    fs.put_file_bytes(
        &namespace_id,
        "/hello.txt",
        b"hello from embedded LoonFS\n",
        PutFileOptions {
            behavior: PutFileBehavior::ReplaceExisting,
            commit_id: None,
        },
    )?;

    let file = fs.read_file_bytes(&namespace_id, "/hello.txt")?;
    println!("{}", String::from_utf8_lossy(&file.bytes));
    Ok(())
}
