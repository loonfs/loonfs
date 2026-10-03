# loonfs-server

The reference LoonFS server. It hosts an embedded LoonFS runtime behind the
v0 HTTP API, so remote clients share one writer instead of competing for the
single-writer role against object storage.

Object storage holds every durable byte. This process holds caches and
in-flight work, and both are rebuilt on the next start.

## Run it locally

```bash
cargo build --release -p loonfs-server
./target/release/loonfs-server --config crates/loonfs-server/config/local-fs.example.toml
```

The example listens on `127.0.0.1:9400`, uses `dev-token`, and writes its
objects under `./.loonfs-store`. Point a remote CLI profile at it:

```bash
LOONFS_AUTH_TOKEN=dev-token loonfs --no-input profile create remote default \
  --server-url http://127.0.0.1:9400
```

## Run it in a container

Every release publishes the image as `ghcr.io/loonfs/loonfs-server:vX.Y.Z`,
one manifest covering `linux/amd64` and `linux/arm64`.

```bash
docker run --rm -p 9400:9400 \
  -v /etc/loonfs/server.toml:/etc/loonfs/config.toml:ro \
  ghcr.io/loonfs/loonfs-server:vX.Y.Z
```

The image reads `/etc/loonfs/config.toml` and runs as uid 10001.
[docs/self-hosting.md](docs/self-hosting.md#running-it-in-a-container)
covers the secrets, the mounts, the shutdown timeout, and building the same
image from this crate's `Dockerfile`.

## Configuration

`config/` holds one example per object store: `local-fs`, `aws-s3`,
`gcp-gcs`, `cloudflare-r2`, and `azure-abs`. Each one documents the provider
credentials and the optional server settings. Copy the one you need and edit
it.

The host admission settings are top-level positive integers:

| Setting | Default | At the limit |
| --- | ---: | --- |
| `max_in_flight_requests` | 256 | Returns `503 server_busy` before reading a request body; health and readiness probes are exempt. |
| `max_connections` | 1,024 | Waits before TCP accept until a connection closes; TLS handshakes and idle keep-alive connections count. |

A request holds its slot until its response body yields its last frame,
fails, or is dropped. Response data frames are at most 64 KiB. Uploads
and downloads also take their existing transfer slots. Size both settings
from pod memory left after runtime budgets, using measured peak memory per
request and per connection, with room for allocator overhead.

After the request permit is released, the HTTP transport can still retain
response data up to its write threshold plus one frame per response in
flight. With the library defaults, that is 472 KiB per HTTP/1 connection
(408 KiB + 64 KiB), or 464 KiB per HTTP/2 stream (400 KiB + 64 KiB), both
about 480 KiB. HTTP/2 allows 200 streams per connection by default, so its
sum is 90.625 MiB per connection. At `max_connections = 1024`, these sums
are 472 MiB for HTTP/1 or 90.625 GiB for HTTP/2. Retained transport response
bytes are the one memory term the request cap does not cover. These figures
count response data, not total connection memory.

Validate a config without starting the server:

```bash
loonfs-server --config /etc/loonfs/server.toml --check-config
```

Container hosts without configuration-file mounts may supply the same TOML
through `LOONFS_SERVER_CONFIG_TOML` and omit `--config`. Keep credentials and
the two server secrets in their dedicated environment variables rather than
putting them in the inline TOML.

The command prints one line and exits. It checks the config fields, the TLS
certificate and key, and write access to the local cache directory. It
creates the cache directory if it is missing, then creates and removes a
temporary file in it. It does not open the cache device, allocate cache
capacity, or touch existing cache files. The server locks and recovers the
cache when it starts. The check does not bind the configured address, and it
performs no object-store operation. For a local filesystem store, it does
create the store's root directory.

## Deploying it

Read [docs/self-hosting.md](docs/self-hosting.md) for the topology, the
minimal config, the probes, logging, the local cache, upgrades, and what a
one-writer deployment does not do.

Read [docs/actor-attribution.md](docs/actor-attribution.md) when an application
submits filesystem changes on behalf of its users.

Every release publishes the Helm chart as
`oci://ghcr.io/loonfs/charts/loonfs-server`, at the same version as the
server it runs. [`deploy/helm/loonfs-server`](deploy/helm/loonfs-server) is
that chart's source: one pod, one Service, nothing else.

[`scripts/smoke-test.sh`](scripts/smoke-test.sh) checks an install from the
outside, and [`scripts/test-image.sh`](scripts/test-image.sh) checks the
image.
