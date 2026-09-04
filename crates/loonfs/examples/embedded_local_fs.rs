use loonfs::{
    CreateNamespaceOptions, DestinationBehavior, FsWriter, NamespaceId, PutFileOptions, StoreConfig,
};

#[allow(clippy::print_stdout)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This example intentionally prints the file content it just read.
    let root = std::env::temp_dir().join("loonfs-embedded-local-fs-example");

    // This short-lived example does not need a maintenance runner. A
    // long-running server would compose one beside the writer.
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
                commit: loonfs::CommitOptions::new(loonfs::ActorRef::service(
                    loonfs::ActorId::parse("embedded-example").expect("valid actor id"),
                )),
                expected_inode_id: None,
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
