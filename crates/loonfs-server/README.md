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

Validate a config without starting the server:

```bash
loonfs-server --config /etc/loonfs/server.toml --check-config
```

Container hosts without configuration-file mounts may supply the same TOML
through `LOONFS_SERVER_CONFIG_TOML` and omit `--config`. Keep credentials and
the two server secrets in their dedicated environment variables rather than
putting them in the inline TOML.

The command prints one line and exits. It runs the checks a start runs
before it serves: the config fields, the TLS certificate and key, and the
local block cache directory. It does not bind the configured address and it
performs no object-store operation, so it belongs in a deployment pipeline
ahead of the rollout. Opening the cache takes the directory lock a start
takes, so run the check where the server is not already running.

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
