use loonfs::{
    CreateNamespaceOptions, DestinationBehavior, FsWriter, NamespaceId, PutFileOptions, StoreConfig,
};

#[allow(clippy::print_stdout)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This example intentionally prints the file content it just read.
    let root = std::env::temp_dir().join("loonfs-embedded-local-fs-example");

    // Short-lived embedders keep the default ManualOnly policy: writes never
    // schedule background maintenance behind the caller's back. A long-lived
    // server would opt into FsBackgroundWork::Enabled instead.
    let writer = FsWriter::builder(StoreConfig::LocalFs {
        root: root.to_string_lossy().into_owned(),
        key_prefix: None,
    })
    .writer_id("embedded-example")
    .build()
    .await?;

    let namespace_id = NamespaceId::parse("demo")?;
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions {
                allow_existing: true,
            },
        )
        .await?;
    writer
        .put_file_bytes(
            &namespace_id,
            "/hello.txt",
            b"hello from embedded LoonFS\n",
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                commit_id: None,
                message: None,
                expected_revision_no: None,
            },
        )
        .await?;

    let reader = writer.reader();
    let file = reader.get_file_bytes(&namespace_id, "/hello.txt").await?;
    println!("{}", String::from_utf8_lossy(&file.bytes));

    writer.shutdown().await?;
    Ok(())
}
