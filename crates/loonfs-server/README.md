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
LOONFS_AUTH_TOKEN=dev-token loonfs init default --no-input \
  --mode remote \
  --server-url http://127.0.0.1:9400
```

## Run it in a container

```bash
docker build -f crates/loonfs-server/Dockerfile -t loonfs-server:dev .
docker run --rm -p 9400:9400 \
  -v /etc/loonfs/server.toml:/etc/loonfs/config.toml:ro loonfs-server:dev
```

The image reads `/etc/loonfs/config.toml` and runs as uid 10001.
[docs/self-hosting.md](docs/self-hosting.md#running-it-in-a-container)
covers the secrets, the mounts, and the shutdown timeout.

## Configuration

`config/` holds one example per object store: `local-fs`, `aws-s3`,
`gcp-gcs`, `cloudflare-r2`, and `azure-abs`. Each one documents the provider
credentials and the optional server settings. Copy the one you need and edit
it.

Validate a config without starting the server:

```bash
loonfs-server --config /etc/loonfs/server.toml --check-config
```

The command prints one line and exits. It reports the same errors a start
would report, so it belongs in a deployment pipeline ahead of the rollout.

## Deploying it

Read [docs/self-hosting.md](docs/self-hosting.md) for the topology, the
minimal config, the probes, logging, and the local cache.

[`deploy/helm/loonfs-server`](deploy/helm/loonfs-server) holds a Helm chart
that runs the image on Kubernetes as one pod.
