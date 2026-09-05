# Self-hosting LoonFS

LoonFS runs as one server connected to an object store. All durable data stays
in the object store. The server keeps only temporary state in memory and its
optional local cache.

Run one active server per deployment. Do not run two active servers against
the same LoonFS data. LoonFS does not support automatic failover or horizontal
scaling. A restart or upgrade makes the API unavailable until the server
starts again.

## Deployment checklist

1. Choose an object store and create a server config.
2. Create an API token and a content-token secret.
3. Configure TLS in LoonFS or in a proxy in front of it.
4. Validate the config.
5. Deploy the container or Helm chart.
6. Check the API, object store, and a file round trip.

## 1. Create a config

Start with the example for your object store:

- [local filesystem](../config/local-fs.example.toml)
- [Amazon S3](../config/aws-s3.example.toml)
- [Google Cloud Storage](../config/gcp-gcs.example.toml)
- [Azure Blob Storage](../config/azure-abs.example.toml)
- [Cloudflare R2](../config/cloudflare-r2.example.toml)

The server rejects unknown fields and invalid values at startup.

This is a minimal config for a local filesystem store. It assumes that a
proxy or load balancer provides TLS:

```toml
bind = "0.0.0.0:9400"
writer_id = "loonfs-server-1"
allow_remote_without_tls = true

[store]
kind = "local-fs"
root = "/var/lib/loonfs/store"
```

Only set `allow_remote_without_tls = true` when a trusted proxy provides TLS.
If LoonFS provides TLS directly, remove that setting and add:

```toml
[tls]
cert_path = "/etc/loonfs/tls/server.crt"
key_path = "/etc/loonfs/tls/server.key"
```

Generate the two required secrets and store them in your secret manager:

```bash
export LOONFS_AUTH_TOKEN="$(openssl rand -hex 32)"
export LOONFS_CONTENT_TOKEN_SECRET="$(openssl rand -hex 32)"
```

`LOONFS_AUTH_TOKEN` protects the HTTP API.
`LOONFS_CONTENT_TOKEN_SECRET` signs content transfer tokens. The config can
contain these values, but environment variables make it easier to keep them
out of the config file.

Do not expose a server without authentication. LoonFS rejects an unauthenticated
non-loopback config unless `allow_unauthenticated_remote = true` is set. That
setting is intended only for controlled testing.

For S3 or Google Cloud Storage, configure the bucket to remove incomplete
multipart uploads. Both providers call this action
`AbortIncompleteMultipartUpload`.

## 2. Validate the config

If the server binary is installed locally, run:

```bash
loonfs-server --config /etc/loonfs/server.toml --check-config
```

To validate with the published container image, run:

```bash
docker run --rm \
  --env LOONFS_AUTH_TOKEN \
  --env LOONFS_CONTENT_TOKEN_SECRET \
  --volume /etc/loonfs/server.toml:/etc/loonfs/config.toml:ro \
  --volume /var/lib/loonfs/store:/var/lib/loonfs/store \
  ghcr.io/loonfs/loonfs-server:vX.Y.Z \
  --config /etc/loonfs/config.toml --check-config
```

Replace `X.Y.Z` with the release version you are deploying. Remove the local
store volume when using cloud storage, and pass any provider credentials that
your config requires.

This command validates the config, TLS files, and local cache. It does not
contact the object store. Check object-store access after the server starts.

## 3. Deploy

Choose Docker or Kubernetes.

## Running it in a container

Every release publishes one image for `linux/amd64` and `linux/arm64`:

```text
ghcr.io/loonfs/loonfs-server:vX.Y.Z
```

There is no floating `latest` tag. Always choose a version.

The image runs as uid and gid 10001. For a local filesystem store, create a
writable data directory:

```bash
sudo install -d -o 10001 -g 10001 /var/lib/loonfs/store
```

Start the server:

```bash
docker run --detach \
  --name loonfs-server \
  --restart unless-stopped \
  --stop-timeout 660 \
  --env LOONFS_AUTH_TOKEN \
  --env LOONFS_CONTENT_TOKEN_SECRET \
  --volume /etc/loonfs/server.toml:/etc/loonfs/config.toml:ro \
  --volume /var/lib/loonfs/store:/var/lib/loonfs/store \
  --publish 9400:9400 \
  ghcr.io/loonfs/loonfs-server:vX.Y.Z
```

For cloud storage, remove the local store volume and pass the provider
credentials required by the selected config. Mount TLS files if the server
terminates TLS itself.

Check startup and health:

```bash
docker logs loonfs-server
curl http://127.0.0.1:9400/health
```

Use `https://` when LoonFS or the route in front of it provides TLS.

To build the image yourself, run this command from the repository root:

```bash
docker build -f crates/loonfs-server/Dockerfile -t loonfs-server:dev .
```

## Running it on Kubernetes

Install `kubectl` and Helm before starting. Use a cloud object store with the
published chart. The chart does not create persistent storage for a
`local-fs` store.

Create a namespace:

```bash
kubectl create namespace loonfs
```

Create a Secret containing the server config:

```bash
kubectl --namespace loonfs create secret generic loonfs-server-config \
  --from-file=config.toml=/etc/loonfs/server.toml
```

If the config refers to a TLS certificate, key, or provider credential file,
add each file to this Secret. The files are mounted under `/etc/loonfs`.

Create a second Secret for environment variables:

```bash
kubectl --namespace loonfs create secret generic loonfs-server-secrets \
  --from-literal=LOONFS_AUTH_TOKEN="$LOONFS_AUTH_TOKEN" \
  --from-literal=LOONFS_CONTENT_TOKEN_SECRET="$LOONFS_CONTENT_TOKEN_SECRET"
```

Provider credentials supplied through environment variables can go in the
same Secret.

Install the chart:

```bash
helm install loonfs-server oci://ghcr.io/loonfs/charts/loonfs-server \
  --version X.Y.Z \
  --namespace loonfs \
  --set config.existingSecret=loonfs-server-config \
  --set 'extraEnvFrom[0].secretRef.name=loonfs-server-secrets'
```

Wait for the pod:

```bash
kubectl --namespace loonfs rollout status deployment/loonfs-server
```

The chart creates one Deployment and one ClusterIP Service. It does not
create an ingress or load balancer. Add your own route and terminate TLS there,
or configure TLS in the server.

See the chart [README](../deploy/helm/loonfs-server/README.md) for all values,
including resources, scheduling, private registries, and the optional cache.

## 4. Verify the deployment

For Kubernetes, forward the Service to your workstation:

```bash
kubectl --namespace loonfs port-forward service/loonfs-server 9400:9400
```

Check both probes:

```bash
curl http://127.0.0.1:9400/health
curl http://127.0.0.1:9400/readiness
```

Create a CLI profile for the server:

```bash
LOONFS_AUTH_TOKEN="$LOONFS_AUTH_TOKEN" loonfs --no-input profile create remote self-hosted \
  --server-url http://127.0.0.1:9400
```

Use the public `https://` URL instead when checking the complete network and
TLS path.

Check the object store:

```bash
loonfs maintenance store probe
```

`store probe` creates and removes temporary objects. It catches invalid
credentials, the wrong bucket or region, and stores that do not provide the
operations LoonFS requires.

For Kubernetes, the smoke test also checks the rollout, probes, object store,
and a file upload and download:

Install `kubectl`, `curl`, and the `loonfs` CLI before running it.

```bash
export LOONFS_AUTH_TOKEN
crates/loonfs-server/scripts/smoke-test.sh --namespace loonfs
```

The script creates a temporary namespace and deletes it before exiting.

## Production checklist

- Run exactly one active LoonFS server per deployment.
- Require an auth token.
- Use TLS in LoonFS or in a trusted proxy.
- Keep secrets in a secret manager or Kubernetes Secret.
- Keep a `local-fs` store on durable storage that uid 10001 can write.
- Configure removal of incomplete multipart uploads for cloud buckets.
- Allow at least 660 seconds for graceful shutdown.
- Set memory and open-file limits before enabling the local cache.
- Monitor health, readiness, metrics, and repeated error logs.
- Run the smoke test after installation and every upgrade.

## Probes and metrics

| Route | Authentication | Meaning |
| --- | --- | --- |
| `GET /health` | None | The process is running. Use this for liveness. |
| `GET /readiness` | None | The server is accepting work. It returns 503 during shutdown. |
| `GET /metrics` | Bearer token | Prometheus metrics for the server. |

Health and readiness do not contact the object store. Use
`loonfs maintenance store probe` when you need to check storage access.

Prometheus must send the API token as
`Authorization: Bearer <LOONFS_AUTH_TOKEN>` when scraping `/metrics`.

## Logs

The server writes JSON logs to standard output.

- Leave `LOONFS_TRACE` unset, or set it to `json`, to enable logging.
- Set `LOONFS_TRACE=off` to disable logging.
- Use `RUST_LOG` to change the filter, for example
  `RUST_LOG=loonfs_core=debug`.

The server rejects unsupported `LOONFS_TRACE` values instead of guessing.

## Resource sizing

Start with enough memory for:

```text
max_concurrent_uploads × 8 MiB
+ max_concurrent_downloads × max_download_bytes
+ local_cache.memory_bytes
+ memory for metadata maintenance and the server
```

The defaults allow 8 concurrent uploads, 16 concurrent downloads, and
256 MiB per download. Set a memory limit comfortably above the calculated
minimum.

`max_writer_sessions` defaults to 10,000. A request that needs another writer
session answers `writer_capacity_exceeded`; raise the limit if the deployment
must keep more namespaces writable at once.

`max_concurrent_folds` defaults to 2. A sustained
`loonfs.publisher.wal_folds_waiting` gauge means WAL folds are waiting at the
cap; raise it only after accounting for the additional object-store and CPU
work.

Publication admission counts queued and active callers, including duplicate
commits, conflicts, and namespace deletes. A caller that disconnects stays
charged until its admitted work settles. Requests past a count or estimated
byte limit receive `commit_queue_full`; admitted work waits for a shared
publication slot. Each namespace has its own allowance so one busy tenant
cannot consume the default host budget.

```toml
[publication]
max_requests = 8192
max_requests_per_namespace = 1024
max_estimated_bytes = 67108864
max_estimated_bytes_per_namespace = 8388608
max_concurrent_publications = 8
```

These are the defaults; every value must be positive. The byte estimate counts
request data, prepared proofs, and queue bookkeeping. It excludes allocator
slack, HTTP request buffers, and the metadata/working copies a publication
loads. Size process memory for those costs and the separate fold/cache limits
too. Embedded hosts set the same limits with `FsWriterBuilder::publication_limits`.

## Optional local cache

The local cache stores replaceable copies of metadata blocks. It is not
durable and can be deleted while the server is stopped.

```toml
[local_cache]
path = "/var/lib/loonfs/cache"
memory_bytes = 67108864
disk_bytes = 107374182400
```

`disk_bytes` must be at least 96 MiB. The cache allocates 16 MiB files up to
that limit and keeps them open. Set the process open-file limit higher than
`disk_bytes / 16 MiB`.

On Kubernetes, set `localCache.enabled=true`, set `localCache.sizeLimit`
higher than `disk_bytes`, and use `/var/cache/loonfs` as the config path. The
chart uses an `emptyDir`, so a replacement pod starts with an empty cache.

## Shutdown and upgrades

The server handles `SIGTERM` by stopping new requests, waiting for active
requests, and finishing shutdown work. The default shutdown deadline is
600 seconds. Docker and the Helm chart should allow 660 seconds before
sending `SIGKILL`.

Before an upgrade, flush each namespace with the current version:

```bash
loonfs maintenance flush --namespace <namespace>
```

For Docker, pull the new version and replace the container with the same
config, secrets, and object store.

For Kubernetes, run:

```bash
helm upgrade loonfs-server oci://ghcr.io/loonfs/charts/loonfs-server \
  --version X.Y.Z \
  --namespace loonfs \
  --reuse-values
```

The chart stops the old pod before starting the new one. The API is
unavailable during this period. Run the smoke test after the rollout.

To roll back the Helm release:

```bash
helm rollback loonfs-server --namespace loonfs
```

## Background maintenance

`maintenance` defaults to `serve_and_maintain`, which serves the maintenance
API group and schedules metadata maintenance and garbage collection. Use
`serve_only` to serve explicit requests without scheduling work,
`maintain_only` to schedule work without serving the group, or `disabled` to
do neither. A mode that does not serve the group answers every route under
`/v0/maintenance/` with `route_not_found`; a mode that does not maintain leaves
scheduled work to another process. Long metadata compactions can log progress
for an extended period. No action is required unless failures repeat.

`compaction_required` asks the registered `metadata_compaction` job to run.
The self-hosted server schedules that follow-up automatically.

## Current limitations

- One process or pod serves the API.
- Restarts and upgrades cause a short outage.
- A second replica does not share traffic with the first.
- There is no leader election or automatic failover.
- The Helm chart creates only a Deployment and ClusterIP Service.
