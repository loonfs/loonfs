# loonfs-server

`loonfs-server` is the reference LoonFS server. It's a standalone binary that runs an embedded LoonFS runtime behind the v0 HTTP API, so many remote clients can share one writer. LoonFS coordinates writers through object-storage writes, so a change of writer takes about 200 ms. Because of that, most applications are better served by one long-lived writer in a server that many clients share than by clients that each compete to be the writer.

If you'd rather run LoonFS inside your own Rust process, use the [`loonfs`](../loonfs) library instead. If you want to serve the HTTP API from your own server, use [`loonfs-http`](../loonfs-http), which implements the API as a library.

All durable data is stored in object storage. The server keeps only caches and in-progress work locally, and it rebuilds both when it restarts.

## Run it locally

```bash
cargo build --release -p loonfs-server
./target/release/loonfs-server --config crates/loonfs-server/config/local-fs.example.toml
```

The example config listens on `127.0.0.1:9400`, uses `dev-token` as its auth token, and writes its objects under `./.loonfs-store`. To connect the CLI to it, create a remote profile:

```bash
LOONFS_AUTH_TOKEN=dev-token loonfs --no-input profile create remote default \
  --server-url http://127.0.0.1:9400
```

## Run it in a container

Every release publishes the server image as `ghcr.io/loonfs/loonfs-server:vX.Y.Z`. One image manifest covers both `linux/amd64` and `linux/arm64`.

```bash
docker run --rm -p 9400:9400 \
  -v /etc/loonfs/server.toml:/etc/loonfs/config.toml:ro \
  ghcr.io/loonfs/loonfs-server:vX.Y.Z
```

The image reads its config from `/etc/loonfs/config.toml` and runs as uid 10001. [Running it in a container](docs/self-hosting.md#running-it-in-a-container) covers the secrets, the mounts, the shutdown timeout, and how to build the same image from this crate's `Dockerfile`.

## Configuration

The `config/` directory contains one example config for each object store: `local_fs`, `aws_s3`, `gcp_gcs`, and `cloudflare_r2`. Each one documents the provider credentials and the optional server settings. Copy the one for your store and edit it.

To size the server's memory from one number, set the top-level `memory_limit_bytes`. If you leave it out on Linux, the server reads the limit from cgroup v2, and then from cgroup v1. If it doesn't find a finite limit, it keeps the existing defaults and logs that it's unsized. [Set one number](docs/self-hosting.md#set-one-number) shows the formula.

When a limit is available, each memory setting is derived from it by default. A setting you set explicitly overrides its share, but the final values still have to fit within the limit.

| Setting | Default with a limit | Default without a limit |
| --- | --- | --- |
| `metadata_cache.max_segment_bytes` | Derived: 30% of managed memory, or 40% when grep isn't served | 256 MiB |
| `metadata_cache.max_head_bytes` | 16 MiB (not derived) | 16 MiB |
| `metadata_cache.max_wal_tail_bytes` | Derived: 10% | 64 MiB |
| `metadata_cache.max_wal_object_bytes` | Derived: 3% | 32 MiB |
| `metadata_cache.max_content_object_bytes` | Derived: 7% | 96 MiB |
| `grep.max_cache_bytes` | Derived: 10% when grep is served | 256 MiB when grep is served |
| `max_read_working_bytes` | Derived: 20% | 256 MiB |
| `publication.max_estimated_bytes` | Derived: 10% | 64 MiB |
| `max_merge_input_bytes` | Derived: 10% divided by `max_concurrent_compactions` | 64 MiB per compaction |

You can set a cache size or `max_read_working_bytes` to zero to disable retention. The publication byte limit and the merge input limit must be positive. `grep.max_cache_bytes` applies only when the configured grep mode serves queries.

The host admission settings are top-level positive integers:

| Setting | Default | At the limit |
| --- | ---: | --- |
| `max_in_flight_requests` | 256 | Returns `503 server_busy` before reading a request body. Health and readiness probes are exempt. |
| `max_connections` | 1,024 | Waits before TCP accept until a connection closes. TLS handshakes and idle keep-alive connections count toward the limit. |

A request keeps its slot until its response body yields its last frame, fails, or is dropped. Each response data frame is at most 64 KiB. Uploads and downloads also take a slot from their existing transfer limits. The sizing formula reserves 472 KiB for each configured connection, and changing the connection, transfer, fold, or compaction counts changes the memory left for the derived pools.

After a request releases its permit, the HTTP transport can still keep response data in memory, up to its write threshold plus one frame for each response in flight. With the library defaults, that's 472 KiB per HTTP/1 connection (408 KiB + 64 KiB), or 464 KiB per HTTP/2 stream (400 KiB + 64 KiB), which are both about 480 KiB. HTTP/2 allows 200 streams per connection by default, so its total is 90.625 MiB per connection. At `max_connections = 1024`, the totals are 472 MiB for HTTP/1 or 90.625 GiB for HTTP/2. This retained response data is the one memory cost that the request cap doesn't cover. (These figures count response data, not total connection memory.)

To validate a config without starting the server, run:

```bash
loonfs-server --config /etc/loonfs/server.toml --check-config
```

The command prints one line and exits. It checks the config fields, the TLS certificate and key, and write access to the local cache directory. To check write access, it creates the cache directory if it's missing, and then creates and removes a temporary file in it. It doesn't open the cache device, allocate cache capacity, or touch existing cache files (the server locks and recovers the cache when it starts). The check doesn't bind the configured address or perform any object-store operation, but for a local filesystem store, it does create the store's root directory.

If your container host can't mount a config file, you can pass the same TOML in `LOONFS_SERVER_CONFIG_TOML` and leave out `--config`. Keep credentials and the two server secrets in their own environment variables instead of putting them in the inline TOML.

## Deploying it

Read [Self-hosting LoonFS](docs/self-hosting.md) for the topology, a minimal config, the probes, logging, the local cache, upgrades, and what a one-writer deployment doesn't do.

If your application submits filesystem changes on behalf of its users, read [Actor attribution](docs/actor-attribution.md).

Every release publishes the Helm chart as `oci://ghcr.io/loonfs/charts/loonfs-server`, at the same version as the server it runs. The chart's source is in [`deploy/helm/loonfs-server`](deploy/helm/loonfs-server). It deploys one pod and one Service, and nothing else.

To check an installation from the outside, run [`scripts/smoke-test.sh`](scripts/smoke-test.sh). To check the image, run [`scripts/test-image.sh`](scripts/test-image.sh).
