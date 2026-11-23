# loonfs-client

`loonfs-client` is the async Rust HTTP client for a LoonFS server, such as [`loonfs-server`](../loonfs-server). You'd use it when your application reaches LoonFS over the network instead of opening the object store itself. If you'd rather run LoonFS inside your own process, use the embedded runtime, [`loonfs`](../loonfs), instead. The client doesn't do any namespace work. It sends HTTP API requests and decodes the responses, and the server does the work.

You build a `Client` with `Client::new`, which takes a `ClientConfig` and a `Context`. The context is the authority that every request acts in: `Context::Service`, sent as `Loonfs-Context: service`, or the principals of the user that the requests act for, sent as `Loonfs-Principals`. The context has no default, so a client that acts for a user can't send requests in the service context by mistake. `Client::with_context` clones a client with another context.

```rust
use loonfs_client::{Client, ClientConfig, NamespacePath};
use loonfs_types::{ActorId, Context};

let client = Client::new(
    ClientConfig {
        server_url: "http://127.0.0.1:9400".to_owned(),
        auth_token: Some("dev-token".into()),
        actor: Some(ActorId::parse("usr_8f3c").expect("valid actor id")),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    },
    Context::Service,
)?;
let target = NamespacePath::parse("demo", "/hello.txt")?;
let bytes = client.read_file(&target).await?;
```

`ClientConfig.actor` is the actor that the client sends as `Loonfs-Actor` on commits, uploads, namespace creation, and forks, and LoonFS records that actor on what those operations create. A client without an actor can read, but an operation that records an actor fails before anything is sent. `Client::with_actor` clones a client with another actor. `ClientConfig::load` reads the same config from a TOML file, and both paths run the same validation (for example, a bearer token needs an `https` URL unless the server is on a loopback address).

A `NamespacePath` is a namespace ID and an absolute path, both validated when the value is built. The per-operation options types, such as `PutFileOptions` and `ReadFileOptions`, are defined in [`loonfs-types`](../loonfs-types) and shared with the embedded runtime, so code that sets options works the same way with either crate.

By default, the client retries a read, a commit, or another operation that's safe to repeat after a network error, a `server_busy`, `commit_queue_full`, or `shutting_down` response, or a response from a proxy or a storage provider without the API's error body whose status is 408, 429, 500, 502, 503, or 504 (such as a load balancer's 502), up to three times. `ClientError::retryable_without_operator_action` returns whether the client retries an error. Apart from content transfers, the retries of one operation also stop after 90 seconds. Set `disable_transient_retry` to send each request once. `request_timeout_ms` limits each HTTP attempt. Without it, a content transfer keeps going while the connection makes progress, up to a 60-second inactivity timeout.

The [API spec](../../docs/specs/api.md) defines the requests the client sends, and its [Identity headers](../../docs/specs/api.md#identity-headers) section lists which operations need an actor or a context.
