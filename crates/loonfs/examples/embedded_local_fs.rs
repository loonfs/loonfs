use loonfs::{
    ActorId, CreateNamespaceOptions, DestinationBehavior, LoonFs, NamespaceId, PutFileOptions,
    StoreConfig,
};

#[allow(clippy::print_stdout)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This example intentionally prints the file content it just read.
    let root = std::env::temp_dir().join("loonfs-embedded-local-fs-example");

    // This short-lived example does not need a maintenance runner. A
    // long-running server would compose one beside the runtime.
    let runtime = LoonFs::builder(StoreConfig::LocalFs {
        root: root.to_string_lossy().into_owned(),
        key_prefix: None,
    })
    .writer_id("embedded-example")
    .build()
    .await?;

    let namespace_id = NamespaceId::parse("demo")?;
    let actor = ActorId::parse("embedded-example")?;
    runtime
        .create_namespace_with_options(
            &namespace_id,
            &actor,
            &CreateNamespaceOptions {
                allow_existing: true,
                ..Default::default()
            },
        )
        .await?;
    let namespace = runtime.open_namespace(&namespace_id)?;
    namespace
        .put_file_with_options(
            "/hello.txt",
            b"hello from embedded LoonFS\n",
            &actor,
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..Default::default()
            },
        )
        .await?;

    let file = namespace.read_file("/hello.txt").await?;
    println!("{}", String::from_utf8_lossy(&file.bytes));

    runtime.shutdown().await?;
    Ok(())
}
