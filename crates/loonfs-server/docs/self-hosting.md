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
- Check the release notes for format compatibility before an upgrade; a namespace prefix copy or a fork is not a complete backup.

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

These metrics show whether the limits in [Resource sizing](#resource-sizing)
fit the namespaces a server serves.

| Metric | Type | What moves it |
| --- | --- | --- |
| `loonfs.namespace_head_cache.gets` | Counter, `result` label | A read looks up its namespace head: `hit` or `miss`. |
| `loonfs.head_state_cache.evictions` | Counter | A head anchor or WAL-tail projection is evicted at the `metadata_cache.max_head_state_bytes` limit. The tails writers publish from are evicted the same way. |
| `loonfs.head_state_cache.retained_decoded_bytes` | Gauge | Decoded bytes of head anchors and WAL-tail projections held now, writers' tails included. |
| `loonfs.metadata_segment_cache.retained_decoded_bytes` | Gauge | Decoded bytes the metadata segment cache holds, up to `metadata_cache.max_segment_bytes`. |
| `loonfs.publisher.tail_replays` | Counter | A publish rereads the WAL tail from the store instead of finding it in the head-state cache. This happens on a session's first publish, after the cache evicted the tail, after a failed publish, when the namespace's last write was more than a minute ago, and when a fold the publisher did not run has published a new manifest. |
| `loonfs.publisher.sessions_open` | Gauge | Writer sessions the server holds: one for each namespace it has written since it started, plus any whose admitted work is still finishing. |
| `loonfs.maintenance.sweep_passes` | Counter, `result` label | A maintenance sweep pass ends: `ok` when it listed every namespace, `error` when the listing failed. |
| `loonfs.maintenance.sweep_pass_seconds` | Histogram | How long one sweep pass took. A pass that takes longer than `maintenance_interval_ms` is followed at once by the next. |
| `loonfs.maintenance.sweep_visit_failures` | Counter, `call` label | One call of a sweep visit failed on one namespace: `metadata`, `grep_index`, `gc`, or `grep_gc`. The next pass tries it again. |
| `loonfs.maintenance.sweep_namespaces` | Gauge | Namespaces the last finished sweep pass listed, deleted ones included. |
| `loonfs.object_store.operations` | Counter, `operation`, `result`, and `key_class` labels | A store call finishes. `key_class` is `content`, `wal_object`, `namespace_manifest` (manifests and the hint), `metadata_segment`, `gc_control` (pins), `metadata` (upload sessions), or `unknown`. |

## Logs

The server writes JSON logs to standard output.

- Leave `LOONFS_TRACE` unset, or set it to `json`, to enable logging.
- Set `LOONFS_TRACE=off` to disable logging.
- Use `RUST_LOG` to change the filter, for example
  `RUST_LOG=loonfs_core=debug`.

The server rejects unsupported `LOONFS_TRACE` values instead of guessing.

## Grep indexing

Set `[grep].mode` to choose whether this server serves searches, maintains the
index, or does both. Omit the table to disable grep. The optional
`max_files_per_step` and `max_content_bytes_per_step` bound input work per
indexing step; their defaults are 256 files and 64 MiB, and both must be positive.
The maintenance sweep builds the index, so indexing runs only on a server
whose `maintenance` mode maintains. See
[Background maintenance](#background-maintenance).

Segment sizes, merge thresholds, and reorganization step sizes use engine
defaults, like metadata compaction. The accepted input limits are
`max_files_per_step` and `max_content_bytes_per_step`.

## Resource sizing

`request_deadline_ms` defaults to 60000 ms for metadata and query requests; streamed content and long-running operator work are exempt.

Each cache, queue, and unit of work has its own memory budget. The values
below are ceilings, not allocations. Memory grows toward a ceiling only as a
cache fills or as work runs. Allocator overhead and HTTP buffers are not
counted in any budget and sit on top.

| Budget | Setting | Default | What it bounds | Kind |
| --- | --- | --- | --- | --- |
| Metadata segment cache | `metadata_cache.max_segment_bytes` | 256 MiB | Decoded metadata blocks and manifests | Steady |
| Head state | `metadata_cache.max_head_state_bytes` | 64 MiB | Cached namespace heads and WAL tails, for reads and for writer sessions, for any number of namespaces | Steady |
| Publication queue | `publication.max_estimated_bytes` | 64 MiB | Estimated bytes of admitted commit requests | Steady |
| Proxied uploads | `max_concurrent_uploads` | 8 uploads | At most one 8 MiB transfer part per upload body | Per request |
| Proxied downloads | `max_concurrent_downloads` | 16 streams | One 8 MiB read chunk per content stream | Per request |
| Block memo | `max_block_memo_bytes` | 64 MiB | Metadata blocks one read, publication, or fold keeps | Per operation |
| Merge input | `max_merge_input_bytes` | 64 MiB | Decoded blocks one compaction or maintenance step merges | Per operation |
| Segment output | None | 32 MiB | Encoded segments one fold, compaction, or maintenance step holds while it writes them | Per operation |
| WAL folds | `max_concurrent_folds` | 2 | Folds running at once, including folds that maintenance requests start | Concurrency |
| Compactions | `max_concurrent_compactions` | 2 | Metadata merges running at once, bounded steps and streaming compactions alike, whether writer sessions, the maintenance sweep, or maintenance requests start them | Concurrency |
| Sweep visits | `max_concurrent_maintenance` | 8 | Namespaces the maintenance sweep visits at once, including their grep indexing | Concurrency |
| Publications | `publication.max_concurrent_publications` | 8 | Publications running at once | Concurrency |

A fold holds a block memo and its segment output, so it can use up to 96 MiB.
A merge holds its merge input and its segment output, so it can also use up
to 96 MiB, and `max_concurrent_compactions` merges can use that much each. A
sweep visit that folds the WAL tail and then merges takes a fold permit for
the fold and then a compaction permit for the merge, so the fold and
compaction limits, not `max_concurrent_maintenance`, bound that memory.

A maintenance step merges only the runs that fit in `max_merge_input_bytes`.
A larger window runs as a streaming compaction, which holds at most that much
decoded input at once. A lower value therefore moves work from maintenance
steps to compactions.

The segment output budget has no setting. A segment larger than the budget
is written alone. A checkpoint, snapshot, or fork that has to fold the WAL
tail first runs that fold with the default 64 MiB block memo.

Maintenance requests sent to the API run outside
`max_concurrent_maintenance`. Their folds and merges take the same fold and
compaction permits as every other fold and merge.

`max_upload_bytes` and `max_download_bytes` limit the size of one proxied
transfer. Both default to 256 MiB. They do not reserve memory.

Without grep and without the local cache, the budgets that have a
process-wide limit add up to 960 MiB by default:

| Budget | Ceiling |
| --- | --- |
| Metadata segment cache | 256 MiB |
| Head state | 64 MiB |
| Publication queue | 64 MiB |
| 8 uploads at 8 MiB | 64 MiB |
| 16 downloads at 8 MiB | 128 MiB |
| 2 folds at 96 MiB | 192 MiB |
| 2 compactions at 96 MiB | 192 MiB |
| Total | 960 MiB |

The total leaves out writer sessions, the block memos of reads
and publications, the WAL tails that running reads, publications, and folds
hold outside the head-state cache, and the work of maintenance requests sent
to the API other than their folds and merges. Eight running
publications can hold up to 512 MiB of block memos at the default budget.
Reads have no concurrency limit, so their block memos have no total.

Grep adds a 256 MiB block cache on a server that answers queries. The cache
has no setting. A query reads up to 32 candidate files of at most 8 MiB each
at once, so one query can hold up to 256 MiB. Queries have no concurrency
limit. Index building runs in sweep visits and in the index pass over held
sessions, and each step reads at most `max_content_bytes_per_step` of
content. Up to `max_concurrent_maintenance` sweep visits and one index pass
build at once, so indexing can read nine steps of content at once by
default, up to 576 MiB. Lower `max_concurrent_maintenance` or
`[grep].max_content_bytes_per_step` to lower that ceiling.

The local cache adds `memory_bytes`, 64 MiB of write buffers for its disk
tier, and up to 256 MiB of inserts waiting for the disk tier. The last two
have no setting.

This config for a 256 MiB container uses a 64 MiB segment cache, a 16 MiB
head-state budget, 8 MiB block memos, an 8 MiB merge input, two running
publications, one fold, one compaction, and one sweep visit at a time:

```toml
max_concurrent_folds = 1
max_concurrent_compactions = 1
max_concurrent_maintenance = 1
max_concurrent_uploads = 2
max_concurrent_downloads = 2
max_merge_input_bytes = 8388608
max_block_memo_bytes = 8388608

[publication]
max_estimated_bytes = 8388608
max_concurrent_publications = 2

[metadata_cache]
max_segment_bytes = 67108864
max_head_state_bytes = 16777216
```

| Budget | Ceiling |
| --- | --- |
| Metadata segment cache | 64 MiB |
| Head state | 16 MiB |
| Publication queue | 8 MiB |
| 2 uploads at 8 MiB | 16 MiB |
| 2 downloads at 8 MiB | 16 MiB |
| 2 publications at an 8 MiB block memo | 16 MiB |
| 1 fold: 8 MiB block memo and 32 MiB segment output | 40 MiB |
| 1 compaction: 8 MiB merge input and 32 MiB segment output | 40 MiB |
| Total | 216 MiB |

64 + 16 + 8 + 16 + 16 + 16 + 40 + 40 = 216 MiB, which leaves 40 MiB of the
256 MiB for allocator overhead, HTTP buffers, and the block memos of reads.
Each read keeps at most 8 MiB. Three cases can still pass the limit:

- Many large reads at once, because reads have no concurrency limit.
- A namespace delete that folds the WAL tail. It runs as a publication and
  adds a 32 MiB segment output.
- A checkpoint, snapshot, or fork that folds the WAL tail. That fold keeps
  the default 64 MiB block memo.

The server keeps one writer session for each namespace it has written since
it started. There is no cap and no eviction. One idle session holds about
3 KiB of heap, so 10,000 written namespaces hold about 30 MiB. A session's
WAL tail lives in the head-state cache between publishes, so it is counted
there, not here. The server stops holding a session when its
namespace is deleted, or when a request finds that the namespace does not
exist. After a restart, the first write to each namespace acquires a new
writer epoch.

`max_concurrent_folds` defaults to 2. A sustained
`loonfs.publisher.wal_folds_waiting` gauge means WAL folds are waiting at the
cap; raise it only after accounting for the additional object-store and CPU
work.

`min_publish_interval_ms` defaults to 1000 ms between publication starts per namespace; cold namespaces publish immediately.

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
too. Embedded hosts set the same limits with `LoonFsBuilder::publication_limits`.

Hosted servers use the `[inline_content]` table with the settings below. Inline
writes are enabled by default at a 64 KiB threshold. Set
`inline_content_threshold_bytes = false` to disable them. Capability discovery
advertises `filesystem.commits.inline_content` and `commit.max_inline_content_bytes_per_operation`
by default and omits both when disabled.

Embedded hosts configure inline writes with `LoonFsBuilder::inline_content`
and `InlineContentPolicy`. These settings do not change reader format limits.

| Setting | Default | Meaning |
| --- | --- | --- |
| `inline_content_threshold_bytes` | 64 KiB | Prepares content at or under this size inline; `None` in the embedded policy or `false` in server TOML disables inline writes. |
| `inline_content_wal_object_budget_bytes` | 1 MiB | Limits inline bytes in one WAL object and stages overflow in operation order. |
| `inline_content_fold_at_bytes` | 2 MiB | Makes an automatic fold due when unfolded inline bytes reach this size. |
| `inline_content_tail_limit_bytes` | 32 MiB | Stages new content when unfolded and admitted inline bytes would exceed this size. |

The tail limit uses the tail size the session recorded at its last publish.
Evicting the tail from the head-state cache does not drop that size. After the
session's tail position is invalidated, the limit uses the last tail size this
session observed. That size counts a WAL put whose outcome is unknown as landed. A session that has not observed the tail admits
at most the WAL object budget, and its first publish observes the tail. The tail
can exceed the limit by at most the WAL object budget: for a new session's first
inline commit, and after a put whose outcome is unknown. Another writer can make
the remembered size stale until this session's next publish. The
`MAX_UNFOLDED_WAL_OBJECTS` write stop refuses new commits regardless of the
inline tail limit.

If a commit already succeeded, retrying the same request returns the original
result without uploading the file again, as long as the commit receipt is still
available. This also works after a restart or on another server. Changed bytes
or a different subject return `commit_id_reuse_conflict`.

The threshold cannot exceed 256 KiB and the WAL object budget cannot exceed 4 MiB.
The WAL object budget, fold trigger, and tail limit must be positive, and the fold
trigger cannot exceed the tail limit. Inline payloads count toward the existing
publication byte limits. Explicit metadata maintenance uses
`MetadataMaintenanceOptions::inline_content_fold_at_bytes`, also 2 MiB by default,
when a writer-derived handle has an observed tail count. Maintenance probes use
the WAL object threshold, the time of the tail's newest commit, and manifest
descriptors; they do not replay the tail.

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
requests and running maintenance sweep visits, and finishing shutdown work.
The default shutdown deadline is 600 seconds, and it bounds both waits. Docker and the Helm chart should allow 660 seconds before
sending `SIGKILL`.

Before an upgrade, fold each namespace with the current version:

```bash
loonfs maintenance fold --namespace <namespace>
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

Roll back only to a release whose notes say it reads this release's durable
format. A fold is not a format downgrade. To roll back the Helm release:

```bash
helm rollback loonfs-server --namespace loonfs
```

## Background maintenance

`maintenance` defaults to `serve_and_maintain`, which serves the maintenance
API group and runs the maintenance sweep. Use `serve_only` to serve explicit
requests without the sweep, `maintain_only` to run the sweep without serving
the group, or `disabled` to do neither. A mode that does not serve the group
answers every route under `/v0/maintenance/` with `route_not_found`; a mode
that does not maintain leaves background work to another process. Long
metadata compactions can log progress for an extended period. No action is
required unless failures repeat.

Background work has two parts. A writer session folds its own WAL tail at
the fold thresholds, and it compacts its namespace's metadata after each fold
it publishes. The sweep does everything else on a cadence. Nothing is
scheduled by hints, and the sweep keeps no state about a namespace between
passes, so a restart loses no work: the first pass after a start visits
every namespace.

A sweep pass lists every namespace in the store, deleted ones included. It
reads one page of up to 1,000 namespace ids per list request and visits up
to `max_concurrent_maintenance` namespaces at once, 8 by default. A visit
does this, in order:

1. Folds a WAL tail whose newest commit is `idle_fold_after_ms` old, then
   compacts the namespace's metadata while compaction is due. A large
   compaction runs as a streaming compaction.
2. When `[grep].mode` maintains the index, runs grep build steps while each
   one publishes, at most 16, then one reorganize step once the index is up
   to date.
3. On a collection pass, collects garbage, then grep garbage.

A pass starts every `maintenance_interval_ms`, 300000 ms (5 minutes) by
default. When a pass takes longer, the next one starts as soon as it ends.
A pass collects garbage when `gc_interval_ms`, 3600000 ms (1 hour) by
default, has passed since the start of the last collection pass that listed
every namespace. The first pass after a start collects.

When `[grep].mode` maintains the index, a second, shorter pass runs every 5
seconds over the writer sessions this server holds, one at a time. It runs
build steps for a session only when the session has committed since that
pass last indexed it, so a commit through this server is indexed within
seconds. A held
session that has not committed costs no store request. The two passes never
build one namespace's index at the same time. Commits made through another
server or writer are indexed by the next sweep pass. A grep query stays
correct while the index is behind: it scans the files committed after the
index, and fails with `index_lagging` past its scan budget unless
`allow_stale` is set.

A failed call on one namespace is logged with the namespace id and the call
name, counted in `loonfs.maintenance.sweep_visit_failures`, and tried again
on the next pass. It does not stop the visits to other namespaces. A failed
namespace listing ends the pass with a warning, and the next pass lists
again. On shutdown, the server stops the sweep at the same moment it stops
admitting requests. No new visit starts, and a running streaming compaction
stops at its next block. After the request drain, the server waits for the
visits that are still running, until the same `shutdown_deadline_ms` that
bounds the drain. At that deadline it drops the visits still running, logs
a warning, and shuts the runtime down. A dropped visit leaves what a crash
leaves, and a later pass does the work again. Like an abandoned request, it
does not make the shutdown fail.

One pass costs one list request per page of namespaces, plus a fixed number
of requests for each namespace. An idle namespace, one with nothing to fold,
compact, or collect, costs:

| Pass | Grep not maintained | Grep maintained, index not enabled | Grep index enabled |
| --- | --- | --- | --- |
| Does not collect | 6 GET or HEAD | 7 GET or HEAD | 26 GET or HEAD |
| Collects | 12 GET or HEAD and 7 LIST | 20 GET or HEAD and 9 LIST | 38 GET or HEAD and 9 LIST |

An object still inside its collection grace adds a request on a collection
pass, and a namespace with work due adds the requests of that work. Deleted
namespaces stay listed, so each one keeps this cost on every pass. A server
with 1,000 idle namespaces and no grep sends about 6,000 requests every
5 minutes and about 19,000 on each hourly collection pass. Raise
`maintenance_interval_ms` and `gc_interval_ms` when that is too many.

The sweep collects deleted namespaces too. A collection pass reclaims a
deleted namespace's content once its retirement grace passes, about 63
minutes after the delete, and later passes keep collecting it. Late writes
through already-issued upload capabilities and dependent forks can extend
reclamation; the later passes find them.
`deleted.retired_content_objects` in a GC report counts listed content
objects that the pass deleted; a pass with nothing left under the content
prefix reports zero. See the API spec's
[namespace deletion section](../../../docs/specs/api.md#63-delete-v0namespacesns)
for the blockers.

A `metadata_compaction_required` compaction outcome in a `metadata`
maintenance response means a streaming compaction is due. The writer
session or the next sweep visit runs it.

A namespace that stops writing below the fold thresholds is folded by the
first sweep pass after its newest commit is `idle_fold_after_ms` old. The
period defaults to 900000 ms, which is 15 minutes, so at the default
interval an idle tail is folded within about 20 minutes, after a restart
too. A namespace written more often than once per period is never folded
this way; its writer folds it at the thresholds. Set
`idle_fold_after_ms = 0` to turn the rule off. An explicit `metadata`
maintenance request uses the same period as the sweep.
`loonfs maintenance loop` does not read the server config and always uses
15 minutes. Embedded hosts pass
`MetadataMaintenanceOptions::idle_fold_after_ms` to
`Maintenance::maintain_metadata_while_due_with_options`, where zero also
turns the rule off.

## Current limitations

- One process or pod serves the API.
- Restarts and upgrades cause a short outage.
- A second replica does not share traffic with the first.
- There is no leader election or automatic failover.
- The Helm chart creates only a Deployment and ClusterIP Service.
