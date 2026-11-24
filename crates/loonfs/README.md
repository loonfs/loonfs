# loonfs

`loonfs` is the embedded LoonFS runtime. You'd use it to read and write LoonFS namespaces from your own Rust process, which reads and writes the object store directly. If you'd rather run LoonFS as a separate process that many clients share, use [`loonfs-server`](../loonfs-server), the standalone server over this crate, and connect to it with [`loonfs-client`](../loonfs-client), the async HTTP client for that server.

The crate has two main types. A `LoonFs` is the runtime. It has the object-store client and the read budgets, and it reads through a `MetadataCache`. A `Namespace` handle acts on one namespace, so its methods don't take a namespace ID. Both types come in one of two modes, `ReadOnly` or `Writable`, and the write methods exist only on the writable types, so calling one on a read-only handle doesn't compile.

`LoonFs::namespace` returns a `Namespace<ReadOnly>`, which has no writer session, so creating one sends no request. On a writable runtime, `LoonFs::open_namespace` returns a `Namespace<Writable>`, which is that namespace's writer session. The session stays open while your code keeps a clone of the handle, and `Namespace::close` ends it. A writable runtime also creates and forks namespaces, and `LoonFs::shutdown` stops new writes and waits for the work the runtime accepted.

```rust
use loonfs::{ActorId, Context, LoonFs, NamespaceId, WriterId};

let actor_id = ActorId::parse("usr_8f3c").expect("valid actor id");
let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

let runtime = LoonFs::builder(store_config, Context::Service)
    .writer_id(WriterId::parse("server-a").expect("valid writer id"))
    .build()
    .await?;
runtime.create_namespace(&namespace_id, &actor_id).await?;

let namespace = runtime.open_namespace(&namespace_id)?;
namespace.put_file("/hello.txt", b"hello", &actor_id).await?;
let file = namespace.read_file("/hello.txt").await?;
assert_eq!(file.bytes, b"hello");

runtime.shutdown().await?;
```

`store_config` is a `StoreConfig` for one object store (S3, Cloudflare R2, Google Cloud Storage, Azure Blob Storage, or a local directory). The `Context` is the authority that the runtime's reads and commits act in: `Context::Service`, which checks no rights, or a subject's principals, whose rights LoonFS checks. A host that serves many users builds the runtime in the service context and clones it with `LoonFs::with_context` for each request.

Several runtimes in one process can share one `MetadataCache` and one `ExecutionBudget`. Pass them to `LoonFsBuilder::metadata_cache` and `LoonFsBuilder::execution_budget`. The cache stores decoded segment blocks and manifests, which runtimes over clones of one store share, and each runtime's namespace heads and WAL tails. The budget limits the reads, publications, folds, and compactions in flight across every runtime that uses it. A runtime that's given neither builds a private cache and budget with the default limits.

## Maintenance

A writable session keeps its own namespace's metadata compact. When its WAL tail reaches the fold threshold, the session folds the tail into metadata segments, and after each fold that publishes a manifest, it runs compaction units over the namespace while one is due. The runtime starts no other maintenance by itself.

A host that needs more maintenance, such as folding a tail that stopped growing, finishing compaction that a session left due after its last fold, or collecting garbage, calls the methods of the `Maintenance` value that `LoonFs::maintenance` returns, on a schedule of its own. (The maintenance scheduler in `loonfs-http` does this, and `loonfs-server` runs it.) Each namespace has one fold path and one compaction path in a runtime, which the session and every maintenance call share, so a call waits for a fold or a compaction unit in progress instead of repeating it.

The `engine` module re-exports the parts that an extension uses to keep its own index over a namespace, such as the segment block cache and the types of an extension's manifest entry. [`loonfs-grep`](../loonfs-grep) is the example, and a host doesn't need the module.

[Maintenance hosting](../../docs/design/maintenance-hosting.md) describes the fold and compaction paths and how a host schedules maintenance, and the [API spec](../../docs/specs/api.md#4-sdk-shape) describes the runtime's handles.
