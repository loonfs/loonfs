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

## Set one number

Set top-level `memory_limit_bytes` to the pod's memory limit:

```toml
memory_limit_bytes = 4294967296
```

When absent on Linux, the server reads `/sys/fs/cgroup/memory.max` (v2),
then `/sys/fs/cgroup/memory/memory.limit_in_bytes` (v1). Missing or unreadable
files, `max`, invalid numbers, and values at least 1 EiB are ignored. Without
a finite limit, all existing defaults remain and startup logs that the server
is unsized.

```text
fixed = 128 MiB
      + max_connections x 472 KiB
      + (max_concurrent_uploads + max_concurrent_downloads) x 8 MiB
headroom = 10% of memory_limit_bytes
managed = memory_limit_bytes - fixed - headroom
```

The fixed terms cover process cost, transport retention, and transfer chunks.
Headroom covers allocator retention. Startup fails when managed memory is
below 256 MiB or the final pools plus fixed costs and headroom exceed the
limit. The error names each term. Fractions round down to whole bytes.

| Pool | Share of managed memory | Setting |
| --- | ---: | --- |
| Metadata segment cache | 40% | `metadata_cache.max_segment_bytes` |
| Head state cache | 10% | `metadata_cache.max_head_state_bytes` |
| Grep cache | 10% | `grep.max_cache_bytes`; added to the segment share when grep is not served |
| Read working memory | 20% | `max_read_working_bytes` |
| Admitted publication bytes | 10% | `publication.max_estimated_bytes` |
| Merge input | 10% | `max_merge_input_bytes`, divided by `max_concurrent_compactions` |

Individual settings remain as overrides. The sum check uses the final values,
including one merge input allowance per concurrent compaction. Startup logs
one line per term with its final value and whether it was derived or set.
Without sizing, omitted pools are logged as defaults.

Counts keep their defaults: 8 publications, 2 folds, 2 compactions, 64 reads,
256 requests, 1,024 connections, 8 uploads, and 16 downloads. Explicit count
settings remain in effect. The formula uses the final connection and transfer
counts and divides merge input by the final compaction count.

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
| `loonfs_server_in_flight_requests` | Gauge | Requests holding a slot, through response completion. |
| `loonfs_server_connection_accept_waiting` | Gauge | 1 while TCP accept waits for a free connection slot, otherwise 0. This is not an OS backlog count; that count is not available to the server. |
| `loonfs_server_busy_rejections_total` | Counter, `kind` label | A concurrency refusal: `request`, `upload`, or `download`. Connections wait and do not increment this counter. |
| `loonfs.namespace_head_cache.gets` | Counter, `result` label | A read looks up its namespace head: `hit` or `miss`. |
| `loonfs.head_state_cache.evictions` | Counter | A head anchor or WAL-tail projection is evicted at the `metadata_cache.max_head_state_bytes` limit. The tails writers publish from are evicted the same way. |
| `loonfs.head_state_cache.retained_decoded_bytes` | Gauge | Decoded bytes of head anchors and WAL-tail projections held now, writers' tails included. |
| `loonfs.metadata_segment_cache.retained_decoded_bytes` | Gauge | Decoded bytes the metadata segment cache holds, up to `metadata_cache.max_segment_bytes`. |
| `loonfs.publisher.tail_replays` | Counter | A publish rereads the WAL tail from the store instead of finding it in the head-state cache. This happens on a session's first publish, after the cache evicted the tail, after a failed publish, when the namespace's last write was more than a minute ago, and when a fold the publisher did not run has published a new manifest. |
| `loonfs.publisher.sessions_open` | Gauge | Open writer sessions and sessions whose admitted work is still finishing. A fenced session stops counting once its work ends. |
| `loonfs.maintenance.sweep_passes` | Counter, `result` label | A full sweep pass ends: `ok` when it listed every namespace, `error` when the listing failed. |
| `loonfs.maintenance.sweep_pass_seconds` | Histogram | How long one full sweep pass took. Full passes run at start and every `full_sweep_interval_ms`. |
| `loonfs.maintenance.sweep_visit_failures` | Counter, `call` label | One call of a sweep visit failed on one namespace: `metadata`, `grep_index`, `gc`, or `grep_gc`. The next pass tries it again. |
| `loonfs.maintenance.sweep_namespaces` | Gauge | Namespaces the last finished full sweep pass listed, deleted ones included. |
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
The optional `max_concurrent_steps` bounds how many indexing steps hold file
content or index segments at once; it defaults to 2 and must be positive. See
[Resource sizing](#resource-sizing).
The maintenance sweep builds the index, so indexing runs only on a server
whose `maintenance` mode maintains. See
[Background maintenance](#background-maintenance).

Segment sizes, merge thresholds, and reorganization step sizes use engine
defaults, like metadata compaction. The accepted input limits are
`max_files_per_step` and `max_content_bytes_per_step`.

## Resource sizing

`request_deadline_ms` defaults to 60000 ms for metadata and query requests; streamed content and long-running operator work are exempt.

`max_in_flight_requests` defaults to 256 and `max_connections` to 1,024.
Both must be positive. The sizing formula uses the configured connection
count. Keep the pod's open-file limit above that count.

The request cap runs before authentication and body extraction. A request
past it receives `503 server_busy` with `Retry-After: 1`, the usual error
envelope, and a request ID. No body is read and no store call is made.
Each admitted request holds its slot until its response body yields its
last frame, fails, or is dropped. Downloads, JSON pages, and error envelopes
all use data frames of at most 64 KiB. Uploads and downloads also count
against their existing transfer caps. `GET` and `HEAD` on `/health` and
`/readiness` are exempt, including probe query parameters. `/metrics` is
subject to the request cap.

After a request releases its permit, the HTTP transport can still retain
response data up to its write threshold plus one frame per response in
flight. At the library defaults, HTTP/1 retains at most 408 KiB + 64 KiB =
472 KiB per connection. HTTP/2 retains at most 400 KiB + 64 KiB = 464 KiB
per stream. Both are about 480 KiB. The default 200 HTTP/2 streams sum to
90.625 MiB per connection. At `max_connections = 1024`, the sums are
472 MiB for HTTP/1 or 90.625 GiB for HTTP/2. Retained transport response
bytes are the one memory term the request cap does not cover. These figures
count response data, not total connection memory.

At the connection cap, the server waits before TCP accept. Clients may
complete a TCP connection into the OS backlog but receive no HTTP response
or TLS handshake until a slot is free. If the backlog fills, connecting
clients can time out or fail according to the OS. Idle keep-alive connections
and incomplete TLS handshakes hold slots until they close. Probes still need
a connection slot, so size the connection cap to leave room for them.

Each cache, queue, and unit of work has its own memory budget. The values
below are ceilings, not allocations. Memory grows toward a ceiling only as a
cache fills or as work runs. The sizing formula reserves fixed costs and
headroom separately from the pools. It does not measure total process memory.

| Budget | Setting | Default | What it bounds | Kind |
| --- | --- | --- | --- | --- |
| Metadata segment cache | `metadata_cache.max_segment_bytes` | Derived: 40%, or 50% without grep; otherwise 256 MiB | Decoded metadata blocks and manifests | Steady |
| Grep cache | `grep.max_cache_bytes` | Derived: 10%; otherwise 256 MiB | Decoded grep blocks and manifests when serving queries | Steady |
| Head state | `metadata_cache.max_head_state_bytes` | Derived: 10%; otherwise 64 MiB | Cached namespace heads and WAL tails, for reads and for writer sessions, for any number of namespaces | Steady |
| Publication queue | `publication.max_estimated_bytes` | Derived: 10%; otherwise 64 MiB | Estimated bytes of admitted commit requests | Steady |
| HTTP requests | `max_in_flight_requests` | 256 | Requests through response completion, excluding health and readiness probes | Concurrency |
| Connections | `max_connections` | 1,024 | Accepted TCP connections, including TLS handshakes and idle keep-alive connections | Concurrency |
| Proxied uploads | `max_concurrent_uploads` | 8 uploads | At most one 8 MiB transfer part per upload body | Per request |
| Proxied downloads | `max_concurrent_downloads` | 16 streams | One 8 MiB read chunk per content stream | Per request |
| Read working memory | `max_read_working_bytes` | Derived: 20%; otherwise 256 MiB | Blocks retained across read, publication, fold, and bounded compaction memos | Shared |
| Merge input | `max_merge_input_bytes` | Derived: 10% / compaction count; otherwise 64 MiB | Decoded blocks one compaction or maintenance step merges | Per operation |
| Segment output | None | 32 MiB | Encoded segments one fold, compaction, or maintenance step holds while it writes them | Per operation |
| WAL folds | `max_concurrent_folds` | 2 | Folds running at once, including the folds that maintenance requests, namespace deletes, and checkpoint, snapshot, and fork creation start | Concurrency |
| Compactions | `max_concurrent_compactions` | 2 | Metadata merges running at once, bounded steps and streaming compactions alike, whether writer sessions, the maintenance sweep, or maintenance requests start them | Concurrency |
| Sweep visits | `max_concurrent_maintenance` | 8 | Namespaces the maintenance sweep visits at once, including their grep indexing | Concurrency |
| Grep steps | `[grep].max_concurrent_steps` | 2 | Grep build and reorganize steps that hold file content or index segments at once, across daily visits and session ticks | Concurrency |
| Reads | `max_concurrent_reads` | 64 | Public reads running at once; each page takes one slot | Concurrency |
| Publications | `publication.max_concurrent_publications` | 8 | Publications running at once | Concurrency |

Memos share one read working memory pool. A full pool makes a memo evict
its own entries or leave new blocks unretained. Reads never wait or fail for
memory. The pool counts decoded data, index and filter sections, stored
read-ahead bytes, keys, reference counts, and memo containers. Zero disables
retention. Blocks a caller already holds remain valid after eviction.

A fold holds its segment output and draws its memo from the shared pool.
A merge holds its merge input and its segment output. Without sizing or
overrides, that is up to 96 MiB per concurrent compaction. A
sweep visit that folds the WAL tail and then merges takes a fold permit for
the fold and then a compaction permit for the merge, so the fold and
compaction limits, not `max_concurrent_maintenance`, bound that memory.

A maintenance step merges only the runs that fit in `max_merge_input_bytes`.
A larger window runs as a streaming compaction, which holds at most that much
decoded input at once. A lower value therefore moves work from maintenance
steps to compactions.

The segment output budget has no setting. A segment larger than the budget
is written alone. A checkpoint, snapshot, or fork that has to fold the WAL
tail first runs that fold with the shared read working memory pool. Creating one
waits for a fold permit, even when the tail is already folded. A fork of a
snapshot never folds and takes no fold permit.

Maintenance requests sent to the API run outside
`max_concurrent_maintenance`. Their folds and merges take the same fold and
compaction permits as every other fold and merge.

`max_upload_bytes` and `max_download_bytes` limit the size of one proxied
transfer. Both default to 256 MiB. They do not reserve memory.

Without a memory limit, grep, local cache, or overrides, the budgets that
have a process-wide limit add up to 1,088 MiB:

| Budget | Ceiling |
| --- | --- |
| Metadata segment cache | 256 MiB |
| Head state | 64 MiB |
| Publication queue | 64 MiB |
| 8 uploads at 8 MiB | 64 MiB |
| 16 downloads at 8 MiB | 128 MiB |
| Read working memory | 256 MiB |
| 2 fold outputs at 32 MiB | 64 MiB |
| 2 compactions at 96 MiB | 192 MiB |
| Total | 1,088 MiB |

The total leaves out writer sessions, page results, decoded blocks borrowed
by callers after memo eviction, the WAL tails that running operations hold
outside the head-state cache, and maintenance work other than folds and
merges. Shared cache entries and memo entries are charged independently,
even when they refer to the same allocation. Public reads wait at
`max_concurrent_reads`; HTTP reads also share the request cap. The pool bounds
memo retention, not all read memory. Embedded reads share the read limit but
have no HTTP request cap.

Grep uses `grep.max_cache_bytes` for its block cache when serving queries.
It is derived by default with a memory limit, or 256 MiB without one. A query
reads up to 32 candidate files of at most 8 MiB each at once, so one query can hold up to 256 MiB. Queries have no concurrency
limit of their own; HTTP queries share the request cap. Index building runs
in daily sweep visits and session ticks. A build step reads at most
`max_content_bytes_per_step` of file content, 64 MiB by default, and a
reorganize step merges at most 64 MiB of
decoded index blocks. A step that finds work waits for one of
`[grep].max_concurrent_steps` permits before it reads either, and holds it
until it publishes. Indexing therefore holds at most two steps of content at
once by default, up to 128 MiB, whatever `max_concurrent_maintenance` is. A
step that finds the index up to date takes no permit, so idle visits still
run in parallel. Lower `[grep].max_concurrent_steps` to lower that ceiling.
The `loonfs_grep_steps_running` and `loonfs_grep_steps_waiting` gauges
report the steps that hold a permit and the steps that wait for one. A
sustained `loonfs_grep_steps_waiting` means indexing steps wait at the cap.

The server builds one `GrepStepBudget` from `[grep].max_concurrent_steps`
for its grep worker. A host that embeds grep and runs more than one worker,
for example one for each runtime, passes a clone of one budget to every
worker. The limit is then one ceiling for the process. A worker given its
own budget adds its own steps on top.

The local cache adds `memory_bytes`, 64 MiB of write buffers for its disk
tier, and up to 256 MiB of inserts waiting for the disk tier. The last two
have no setting.

A 256 MiB container cannot pass the sizing check because managed memory alone
must have at least 256 MiB after fixed costs and headroom. With the default
counts, 2 GiB leaves about 1,051 MiB of managed memory. Use the formula above
when changing counts or overriding individual pools.

The server keeps one writer session for each namespace it has written since
it started. There is no cap and no eviction. One idle session holds about
3 KiB of heap, so 10,000 written namespaces hold about 30 MiB. A session's
WAL tail lives in the head-state cache between publishes, so it is counted
there, not here. The server stops holding a session when its
namespace is deleted, or when a request finds that the namespace does not
exist. A request that finds a fenced session fails with `writer_fenced`,
and the server drops the handle. That session is dead. The server does not
resubmit the request. A later request starts a new session, whose first
publish takes the namespace back. While the old session's work ends, an open
can return `writer_session_closed` (503); a later request can open once it
ends. Other namespaces keep their sessions.

Each dropped fenced session produces a warning with `namespace_id` and
`active_writer_id` when the error carries the winning writer. The host table
exposes the cumulative count as `Namespaces::fenced_sessions_dropped`.
Sustained fencing of one namespace means two writers are being sent its
traffic. Route that namespace to one writer. After a restart, the first
write to each namespace acquires a new writer epoch.

`max_concurrent_folds` defaults to 2. A sustained
`loonfs.execution_budget.folds_waiting` gauge means WAL folds are waiting at
the cap; raise it only after accounting for the additional object-store and
CPU work. `loonfs.execution_budget.compactions_waiting` says the same about
`max_concurrent_compactions`.

`min_publish_interval_ms` defaults to 1000 ms between publication starts per namespace; cold namespaces publish immediately.

`max_concurrent_reads` defaults to 64. Public runtime reads wait for a slot
before their first store request. Each pager page takes one slot and releases
it before returning. A streamed or ranged read releases its slot after
metadata and content lookup, before returning the body. The existing transfer
limits bound the body. Buffered reads keep the slot until the call returns.
The `loonfs.execution_budget.reads_running` and
`loonfs.execution_budget.reads_waiting` gauges report active and queued reads.

Embedded hosts set `ExecutionBudgetBuilder::max_concurrent_reads`. Read-only
and writable runtimes can share the budget through
`LoonFsBuilder::execution_budget`. Reads inside commits, folds, compactions,
pin creation, and collection call core directly and take no read slot.
Grep queries and index steps take a slot for each public read they call.
A read slot holder never waits for a grep step permit.

Publication admission counts queued and active callers, including duplicate
commits, conflicts, and namespace deletes. A caller that disconnects stays
charged until its admitted work settles. Requests past a count or estimated
byte limit receive `commit_queue_full`; admitted work waits for a shared
publication slot. Each namespace has its own allowance so one busy tenant
cannot consume the default host budget. The
`loonfs.execution_budget.admission_rejections` counter counts the requests
refused at the totals, and a sustained
`loonfs.execution_budget.publications_waiting` gauge means namespaces are
waiting for a publication slot.

```toml
[publication]
max_requests = 8192
max_requests_per_namespace = 1024
max_estimated_bytes = 67108864
max_estimated_bytes_per_namespace = 8388608
max_concurrent_publications = 8
```

These are the defaults without a memory limit; `max_estimated_bytes` is
derived by default when sized. Every value must be positive. The byte estimate
counts request data, prepared proofs, and queue bookkeeping. It excludes allocator
slack, HTTP request buffers, and the metadata/working copies a publication
loads. Size process memory for those costs and the separate fold/cache limits
too. Embedded hosts set the per-namespace limits with
`LoonFsBuilder::publication_limits`. They set `max_requests`,
`max_estimated_bytes`, and `max_concurrent_publications` on the execution
budget with `ExecutionBudgetBuilder::max_admitted_requests`,
`max_admitted_bytes`, and `max_concurrent_publications`. Runtimes that share
one budget share these three limits; each runtime keeps its own per-namespace
limits.

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

A writer session folds its WAL tail at the fold thresholds and compacts its
metadata after each fold it publishes, at most 16 compaction units each time.
It records its last published seq, monotonic publication time, and whether
metadata maintenance found nothing left due. A publish clears that last fact.
The host keeps the remaining scheduling facts beside the held handle.

Every `tick_interval_ms`, 5000 ms (5 seconds) by default, the sweep walks held
entries without cloning their handles. A session is Active until one tick has
passed without a commit. It then becomes Settling while work remains due, or
Quiet when metadata is caught up, its maintained grep index is current, and
collection is not due. A Quiet tick costs zero store requests.

A tick runs the first due step for each session, in this order:

1. Metadata, including a tail waiting for its idle fold age: one maintenance
   call, at most 16 compaction units. If work remains or the call fails,
   `maintenance_interval_ms`, 300000 ms (5 minutes) by default, is the retry
   delay. Reaching the idle fold age permits one earlier attempt. A session
   fenced by another compactor costs 6 requests per retry
   interval, rather than every tick.
2. Grep index, when maintained and dirty or behind the published seq: up to
   16 build steps and then a reorganization step. Unfinished work stays dirty
   for another tick. HTTP enable and disable hold a session and mark its entry
   dirty, so a build continues even without a local commit.
3. Collection, when the published seq moved since its last successful
   collection and `gc_interval_ms`, 3600000 ms (1 hour) by default, has elapsed.
   The first collection waits that interval from opening the held entry.
4. Close, when Quiet, idle past `idle_session_close_after_ms`, 1800000 ms
   (30 minutes) by default, and exclusively held. Last opened time, published
   seq, and handle ownership are checked under the table lock. An open of the
   closing namespace waits for the drain and gets a fresh session. Opens of
   other namespaces do not wait for this close. The fresh session's first publish
   acquires a new writer epoch. Close costs zero store requests.

Each held session retains its publisher, engine state, head position, and
allocated queue capacities. Closing releases these after admitted work ends.
Shared caches and execution limits keep their own lifetimes. A server whose
maintenance mode does not maintain has no sweep to close idle sessions.

The daily pass runs at start and every `full_sweep_interval_ms`, 86400000 ms
(24 hours) by default. It lists every namespace, runs metadata and index
maintenance, and collects garbage. It covers namespaces this host does not
write to, forks, deleted namespaces, work left by a crashed process, and
garbage still inside a grace window. Losing the host's in-memory facts loses
no durable progress.

The daily pass and ticks run alongside each other. Both share
`max_concurrent_maintenance` visit slots, 8 by default, and a per-namespace
visiting set prevents overlap. A streaming compaction holds one slot while
other free slots continue work. The daily pass lists up to 1000 namespace ids
per page and requests another page when its visit slots allow it. Hosts can
call `Sweep::run_pass` and `Sweep::tick` directly.

All interval and idle-close settings must be positive. The grep query path
remains correct while its index is behind: it scans later commits and fails
with `index_lagging` past its scan budget unless `allow_stale` is set.

A failed call on one namespace is logged with the namespace id and the call
name, counted in `loonfs.maintenance.sweep_visit_failures`, and tried again
on the next eligible pass. It does not stop the visits to other namespaces. A failed
namespace listing starts no new visit. The visits already running finish,
the pass ends with a warning, and the next full pass lists again. On shutdown,
the server stops the sweep at the same moment it stops admitting requests.
No new visit starts, and a running streaming compaction stops at its next
block. After the request drain, the server waits for the visits that are
still running, until the same `shutdown_deadline_ms` that bounds the drain.
At that deadline it drops the visits still running, logs a warning, and
shuts the runtime down. A dropped visit leaves what a crash leaves, and a
later pass does the work again. Like an abandoned request, it does not make
the shutdown fail.

An idle namespace costs no request between full passes. A full pass costs
one list request per page of namespaces, plus a fixed number of requests for
each namespace. An idle namespace, one with nothing to fold,
compact, or collect, costs:

| Pass | Grep not maintained | Grep maintained, index not enabled | Grep index enabled |
| --- | --- | --- | --- |
| Does not collect | 6 GET or HEAD | 7 GET or HEAD | 26 GET or HEAD |
| Collects | 12 GET or HEAD and 7 LIST | 20 GET or HEAD and 9 LIST | 38 GET or HEAD and 9 LIST |

An object still inside its collection grace adds a request on a collection
pass, and a namespace with work due adds the requests of that work. Deleted
namespaces stay listed, so each one keeps this cost on every full pass. Core
metadata still costs 6 requests per visit and GC adds 13, for 19 total. A
server with 1,000 caught-up idle namespaces and no grep sends no requests for
them between full passes, and about 19,000 on each daily full pass, plus the
namespace listing.

The full pass collects deleted namespaces too. Their content becomes eligible
once its retirement grace passes, about 63 minutes after the delete. The next
full pass collects eligible content, and later full passes keep collecting it.
Late writes through already-issued upload capabilities and dependent forks can extend
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
first eligible metadata tick after its newest commit is `idle_fold_after_ms` old. The
period defaults to 900000 ms, which is 15 minutes. With a free visit slot,
a held tail gets its first idle-fold attempt within one tick after that age,
even when the metadata retry delay is longer. After a restart or a write
through another process, it waits for
a full pass after the age threshold. A namespace written more often than once
per period is never folded this way; its writer folds it at the thresholds. Set
`idle_fold_after_ms = 0` to turn the rule off. An explicit `metadata`
maintenance request uses the same period as the sweep.
An embedded CLI profile does not read the server config and always uses
15 minutes. Embedded hosts pass
`MetadataMaintenanceOptions::idle_fold_after_ms` to
`Maintenance::maintain_metadata_while_due_with_options`, where zero also
turns the rule off. Both `maintain_metadata_while_due` forms return
`Result<bool>`: `true` means no metadata work remains or can become due without
another commit under the options used. The answer uses reads already made by
the call.

## Current limitations

- One process or pod serves the API.
- Restarts and upgrades cause a short outage.
- A second replica does not share traffic with the first.
- There is no leader election or automatic failover.
- The Helm chart creates only a Deployment and ClusterIP Service.
