# Self-hosting LoonFS

This guide describes how to run `loonfs-server` on your own infrastructure, with Docker or Kubernetes, and how to size, monitor, upgrade, and maintain it.

A LoonFS deployment is one server connected to an object store. All durable data is stored in the object store, and the server keeps only temporary state, in memory and in its optional local cache.

Run one active server per deployment, and don't run two active servers against the same LoonFS data. LoonFS doesn't support automatic failover or horizontal scaling. A restart or an upgrade makes the API unavailable until the server starts again.

## Deployment checklist

To deploy a server, follow these steps (the numbered sections below describe them):

1. Choose an object store and create a server config.
2. Create an API token and a content-token secret.
3. Configure TLS in LoonFS or in a proxy in front of it.
4. Validate the config.
5. Deploy the container or the Helm chart.
6. Check the API, the object store, and a file round trip.

## 1. Create a config

Start with the example for your object store:

- [local filesystem](../config/local-fs.example.toml)
- [Amazon S3](../config/aws-s3.example.toml)
- [Google Cloud Storage](../config/gcp-gcs.example.toml)
- [Cloudflare R2](../config/cloudflare-r2.example.toml)

The server rejects unknown fields and invalid values at startup.

Here's a minimal config for a local filesystem store. It assumes that a proxy or load balancer provides TLS:

```toml
bind = "0.0.0.0:9400"
writer_id = "loonfs-server-1"
allow_remote_without_tls = true

[store]
kind = "local_fs"
root = "/var/lib/loonfs/store"
```

Set `allow_remote_without_tls = true` only when a trusted proxy provides TLS. If LoonFS provides TLS directly, remove that setting and add:

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

`LOONFS_AUTH_TOKEN` protects the HTTP API, and `LOONFS_CONTENT_TOKEN_SECRET` signs content transfer tokens. The config can contain these values, but environment variables make it easier to keep them out of the config file.

Don't expose a server without authentication. LoonFS rejects a config without authentication on a non-loopback address, unless `allow_unauthenticated_remote = true` is set. That setting is intended only for controlled testing.

For S3 or Google Cloud Storage, configure the bucket to remove incomplete multipart uploads. Both providers call this action `AbortIncompleteMultipartUpload`.

## Set one number

This section describes how the server sizes its memory pools from one memory limit.

Set the top-level `memory_limit_bytes` to the pod's memory limit:

```toml
memory_limit_bytes = 4294967296
```

If you leave it out on Linux, the server reads `/sys/fs/cgroup/memory.max` (cgroup v2), and then `/sys/fs/cgroup/memory/memory.limit_in_bytes` (cgroup v1). It ignores a missing or unreadable file, the value `max`, an invalid number, and any value of 1 EiB or more. If the server doesn't find a finite limit, it keeps all the existing defaults and logs at startup that it's unsized.

From the limit, the server calculates the managed memory that it divides among its pools:

```text
fixed = 128 MiB
      + max_connections x 472 KiB
      + (max_concurrent_uploads + max_concurrent_downloads) x 8 MiB
headroom = 10% of memory_limit_bytes
managed = memory_limit_bytes - fixed - headroom
```

The fixed terms cover the process's own cost, response data that the HTTP transport retains, and transfer chunks. The headroom covers memory that the allocator retains. Startup fails if managed memory is below 256 MiB, or if the final pools plus the fixed costs and headroom exceed the limit. The error message lists each term. Fractions are rounded down to whole bytes.

| Pool | Share of managed memory | Setting |
| --- | ---: | --- |
| Metadata segment cache | 30% | `metadata_cache.max_segment_bytes` |
| Head state cache | 10% | `metadata_cache.max_head_state_bytes` |
| Object cache | 10% | `metadata_cache.max_object_bytes` |
| Grep cache | 10% | `grep.max_cache_bytes` (added to the segment cache's share when grep isn't served) |
| Read working memory | 20% | `max_read_working_bytes` |
| Admitted publication bytes | 10% | `publication.max_estimated_bytes` |
| Merge input | 10% | `max_merge_input_bytes`, divided by `max_concurrent_compactions` |

You can still set each pool's setting to override its share. The sum check uses the final values, including one merge input allowance for each concurrent compaction. At startup, the server logs one line for each term, with its final value and whether it was derived or set. Without sizing, the pools you didn't set are logged as defaults.

Sizing doesn't change the counts, which keep their defaults: 8 publications, 2 folds, 2 compactions, 64 reads, 256 requests, 1,024 connections, 8 uploads, and 16 downloads. Count settings you set explicitly still apply. The formula uses the final connection and transfer counts, and it divides merge input by the final compaction count.

## 2. Validate the config

Validate the config before you deploy it. If the server binary is installed locally, run:

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

Replace `X.Y.Z` with the release version you're deploying. If you use cloud storage, remove the local store volume, and pass any provider credentials that your config requires.

This command validates the config, the TLS files, and the local cache, but it doesn't contact the object store. Check object-store access after the server starts (see [4. Verify the deployment](#4-verify-the-deployment)).

## 3. Deploy

Deploy the server with Docker, as described in [Running it in a container](#running-it-in-a-container), or with Kubernetes, as described in [Running it on Kubernetes](#running-it-on-kubernetes).

## Running it in a container

Every release publishes one image for `linux/amd64` and `linux/arm64`:

```text
ghcr.io/loonfs/loonfs-server:vX.Y.Z
```

There's no floating `latest` tag, so always choose a version.

The image runs as uid and gid 10001. For a local filesystem store, create a data directory that this user can write to:

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

For cloud storage, remove the local store volume, and pass the provider credentials that your config requires. If the server terminates TLS itself, mount the TLS files too.

Check the startup logs and the health probe:

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

Before you start, install `kubectl` and Helm. Use a cloud object store with the published chart, because the chart doesn't create persistent storage for a `local_fs` store.

Create a namespace:

```bash
kubectl create namespace loonfs
```

Create a Secret that contains the server config:

```bash
kubectl --namespace loonfs create secret generic loonfs-server-config \
  --from-file=config.toml=/etc/loonfs/server.toml
```

If the config refers to a TLS certificate, a key, or a provider credential file, add each file to this Secret. The chart mounts the files under `/etc/loonfs`.

Create a second Secret for the environment variables:

```bash
kubectl --namespace loonfs create secret generic loonfs-server-secrets \
  --from-literal=LOONFS_AUTH_TOKEN="$LOONFS_AUTH_TOKEN" \
  --from-literal=LOONFS_CONTENT_TOKEN_SECRET="$LOONFS_CONTENT_TOKEN_SECRET"
```

If you pass provider credentials through environment variables, you can put them in the same Secret.

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

The chart creates one Deployment and one ClusterIP Service, but no ingress or load balancer. Add your own route and terminate TLS there, or configure TLS in the server.

The chart's [README](../deploy/helm/loonfs-server/README.md) lists all the values, including resources, scheduling, private registries, and the optional cache.

## 4. Verify the deployment

Check that the server, the object store, and file transfers work. On Kubernetes, first forward the Service to your workstation:

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

To check the complete network and TLS path, use the public `https://` URL instead.

On Kubernetes, the smoke test checks the rollout, the health and readiness probes, and a file upload and download. Install `kubectl`, `curl`, and the `loonfs` CLI, and then run:

```bash
export LOONFS_AUTH_TOKEN
crates/loonfs-server/scripts/smoke-test.sh --namespace loonfs
```

The script creates a temporary LoonFS namespace and deletes it before it exits.

## Production checklist

Before you run a server in production, check this list:

- Run exactly one active LoonFS server per deployment.
- Require an auth token.
- Use TLS in LoonFS or in a trusted proxy.
- Keep secrets in a secret manager or a Kubernetes Secret.
- Keep a `local_fs` store on durable storage that uid 10001 can write to.
- Configure cloud buckets to remove incomplete multipart uploads.
- Allow at least 660 seconds for graceful shutdown.
- Set memory and open-file limits before you enable the local cache.
- Monitor health, readiness, metrics, and repeated error logs.
- Run the smoke test after installation and after every upgrade.
- Check the release notes for format compatibility before an upgrade. A copy of a namespace prefix, or a fork, isn't a complete backup.

## Probes and metrics

The server has two unauthenticated probes and a metrics route:

| Route | Authentication | Meaning |
| --- | --- | --- |
| `GET /health` | None | The process is running. Use this for liveness. |
| `GET /readiness` | None | The server is accepting work. It returns 503 during shutdown. |
| `GET /metrics` | Bearer token | Prometheus metrics for the server. |

Health and readiness don't contact the object store.

When Prometheus scrapes `/metrics`, it must send the API token as `Authorization: Bearer <LOONFS_AUTH_TOKEN>`.

These metrics show whether the limits in [Resource sizing](#resource-sizing) fit the namespaces a server serves:

| Metric | Type | What moves it |
| --- | --- | --- |
| `loonfs_server_in_flight_requests` | Gauge | Requests that have a slot, until their responses complete. |
| `loonfs_server_connection_accept_waiting` | Gauge | 1 while TCP accept waits for a free connection slot, and 0 otherwise. This isn't a count of the OS backlog, because that count isn't available to the server. |
| `loonfs_server_busy_rejections_total` | Counter, `kind` label | A concurrency refusal: `request`, `upload`, or `download`. Connections wait instead, so they don't increment this counter. |
| `loonfs_namespace_head_cache_gets_total` | Counter, `result` label | A read that starts from the read cache looks up its namespace head: `hit` or `miss`. |
| `loonfs.read_head.source` | Counter, `source` label | A read takes the namespace head it starts from: `position` when the server has the namespace's writer session, so the read sends no request to confirm the head except the session's check for a newer manifest, `anchor` when the head comes from the read cache and the read confirms it, or `eventual` when an `eventual` read takes the read cache's head within its maximum staleness, without a request. |
| `loonfs_head_state_cache_evictions_total` | Counter | A namespace head or WAL-tail projection is evicted at the `metadata_cache.max_head_state_bytes` limit. A namespace head larger than the limit isn't cached, and each read of it loads it on its own. |
| `loonfs_head_state_cache_retained_decoded_bytes` | Gauge | Bytes charged to the namespace heads and WAL-tail projections in the cache now. Each entry's charge includes its key and the cache's own allocation for the entry. |
| `loonfs_metadata_segment_cache_retained_decoded_bytes` | Gauge | Bytes charged to the entries in the metadata segment cache, up to `metadata_cache.max_segment_bytes`. Each entry's charge includes its key and the cache's own allocation for the entry. |
| `loonfs.publisher.tail_replays` | Counter | A publish reads the WAL tail from the store, because the session has no position to start from. This happens on a session's first publish, after a failed publish, when a successor manifest or WAL object that another process wrote is found, after a fork of the namespace's head, and after about 19 minutes without confirming the basis. After a shorter quiet period, the session confirms the tip with two HEAD requests, and it doesn't read the tail again. A manifest that this runtime publishes itself doesn't cause a replay, because after a fold, a compaction, a checkpoint, a snapshot, or a retention advance in the same runtime, the session moves to the new manifest and keeps the tail above it. Garbage collection publishes no manifest, so it doesn't cause a replay either. |
| `loonfs.publisher.sessions_open` | Gauge | Open writer sessions, and sessions whose admitted work is still finishing. A fenced session stops being counted once its work ends. |
| `loonfs.maintenance.sweep_passes` | Counter, `result` label | A full sweep pass ends: `ok` if it listed every namespace, or `error` if the listing failed. |
| `loonfs.maintenance.sweep_pass_seconds` | Histogram | How long one full sweep pass took. Full passes run when the server starts and every `full_sweep_interval_ms`. |
| `loonfs.maintenance.sweep_visit_failures` | Counter, `call` label | One call of a sweep visit failed on one namespace: `metadata`, `grep_index`, `gc`, or `grep_gc`. The sweep tries the call again after a backoff. |
| `loonfs.maintenance.sweep_panics` | Counter | A sweep call or an idle-session close panicked. The sweep contains the panic, counts a panicked call as a `server_error` failure, and goes on. A nonzero count is a bug to report. |
| `loonfs.maintenance.sweep_backoff_entries` | Gauge | The calls that failed on a namespace and are waiting for their next try. Each pair of a namespace and a call counts once, up to 10,000. |
| `loonfs.maintenance.sweep_namespaces` | Gauge | The number of namespaces that the last finished full sweep pass listed, including deleted namespaces. |
| `loonfs.object_store.operations` | Counter, `operation`, `result`, and `key_class` labels | A store call finishes. `key_class` is `content`, `wal_object`, `namespace_manifest` (manifests and the hint), `metadata_segment`, `gc_control` (pins), `metadata` (upload sessions), or `unknown`. |

## Logs

The server writes JSON logs to standard output.

- Leave `LOONFS_TRACE` unset, or set it to `json`, to enable logging.
- Set `LOONFS_TRACE=off` to disable logging.
- Use `RUST_LOG` to change the filter, for example `RUST_LOG=loonfs_core=debug`.

The server rejects any other `LOONFS_TRACE` value, instead of guessing what it means.

## Grep indexing

Set `[grep].mode` to choose whether this server serves searches, maintains the search index, or does both. Leave out the `[grep]` table to disable grep.

The maintenance sweep builds the index, so indexing runs only on a server whose `maintenance` mode maintains (see [Background maintenance](#background-maintenance)).

These optional settings limit indexing work, and each one must be positive:

- `max_files_per_step` (256 files by default) and `max_content_bytes_per_step` (64 MiB by default) limit the input work of each indexing step.
- `max_concurrent_steps` (2 by default) limits how many indexing steps have file content or index segments in memory at once.

[Resource sizing](#resource-sizing) describes how these limits affect memory.

Segment sizes, merge thresholds, and reorganization step sizes use the engine's defaults, as they do for metadata compaction. The only input limits the config accepts are `max_files_per_step` and `max_content_bytes_per_step`.

## Resource sizing

This section describes the limits on the server's requests, connections, memory, and concurrent work.

`request_deadline_ms` sets the deadline for metadata and query requests, and it defaults to 60000 ms. Streamed content and long-running operator work are exempt.

`max_in_flight_requests` defaults to 256, and `max_connections` defaults to 1,024. Both must be positive. The sizing formula uses the configured connection count. Keep the pod's open-file limit above that count.

The server applies the request cap before authentication and before it reads the request body. A request over the cap receives `503 server_busy` with `Retry-After: 1`, the usual error envelope, and a request ID, and the server doesn't read its body or make any store call. Each admitted request keeps its slot until its response body yields its last frame, fails, or is dropped. Downloads, JSON pages, and error envelopes all use data frames of at most 64 KiB. Uploads and downloads also count against their existing transfer caps.

`GET` and `HEAD` requests to `/health` and `/readiness` are exempt from the request cap, including requests with probe query parameters. `/metrics` is subject to the request cap.

After a request releases its permit, the HTTP transport can still keep response data in memory, up to its write threshold plus one frame for each response in flight. At the library defaults, HTTP/1 keeps at most 408 KiB + 64 KiB = 472 KiB per connection, and HTTP/2 keeps at most 400 KiB + 64 KiB = 464 KiB per stream. Both are about 480 KiB. The default 200 HTTP/2 streams add up to 90.625 MiB per connection. At `max_connections = 1024`, the totals are 472 MiB for HTTP/1 or 90.625 GiB for HTTP/2. This retained response data is the one memory cost that the request cap doesn't cover. (These figures count response data, not total connection memory.)

At the connection cap, the server waits before it accepts another TCP connection. A client can still complete a TCP connection into the OS backlog, but it receives no HTTP response or TLS handshake until a slot is free. If the backlog fills, connecting clients can time out or fail, depending on the OS. Idle keep-alive connections and incomplete TLS handshakes keep their slots until they close. Probes still need a connection slot, so size the connection cap to leave room for them.

Each cache, queue, and unit of work has its own memory budget. The values in the table below are ceilings, not allocations, so memory grows toward a ceiling only as a cache fills or as work runs. The sizing formula reserves the fixed costs and headroom separately from the pools, and it doesn't measure total process memory.

| Budget | Setting | Default | What it bounds | Kind |
| --- | --- | --- | --- | --- |
| Metadata segment cache | `metadata_cache.max_segment_bytes` | Derived: 30%, or 40% without grep. 256 MiB without a memory limit | Decoded metadata blocks and manifests | Steady |
| Grep cache | `grep.max_cache_bytes` | Derived: 10%. 256 MiB without a memory limit | Decoded grep blocks and manifests when serving queries | Steady |
| Head state | `metadata_cache.max_head_state_bytes` | Derived: 10%. 64 MiB without a memory limit | Cached namespace heads and WAL tails for reads, for any number of namespaces | Steady |
| Completed upload sessions | None | 16 MiB | The content ID and completion time of each completed upload session, which garbage collection keeps between passes | Steady |
| Object cache | `metadata_cache.max_object_bytes` | Derived: 10%. 128 MiB without a memory limit | Decoded WAL objects, and verified content objects of up to 1 MiB, that reads keep for later reads | Steady |
| Publication queue | `publication.max_estimated_bytes` | Derived: 10%. 64 MiB without a memory limit | Estimated bytes of admitted commit requests | Steady |
| HTTP requests | `max_in_flight_requests` | 256 | Requests until their responses complete, excluding health and readiness probes | Concurrency |
| Connections | `max_connections` | 1,024 | Accepted TCP connections, including TLS handshakes and idle keep-alive connections | Concurrency |
| Proxied uploads | `max_concurrent_uploads` | 8 uploads | At most one 8 MiB transfer part per upload body | Per request |
| Proxied downloads | `max_concurrent_downloads` | 16 streams | One 8 MiB read chunk per content stream | Per request |
| Read working memory | `max_read_working_bytes` | Derived: 20%. 256 MiB without a memory limit | Blocks retained across read, publication, fold, and bounded compaction memos | Shared |
| Merge input | `max_merge_input_bytes` | Derived: 10% divided by the compaction count. 64 MiB without a memory limit | Decoded blocks that one compaction or maintenance step merges | Per operation |
| Segment output | None | 32 MiB | Encoded segments that one fold, compaction, or maintenance step keeps in memory while it writes them | Per operation |
| WAL folds | `max_concurrent_folds` | 2 | Folds running at once, including the folds that maintenance requests, namespace deletes, and checkpoint, snapshot, and fork creation start | Concurrency |
| Compactions | `max_concurrent_compactions` | 2 | Metadata merges running at once, bounded steps and streaming compactions alike, whether writer sessions, the maintenance sweep, or maintenance requests start them | Concurrency |
| Sweep visits | `max_concurrent_maintenance` | 8 | Namespaces the maintenance sweep visits at once, including their grep indexing | Concurrency |
| Grep steps | `[grep].max_concurrent_steps` | 2 | Grep build and reorganize steps that have file content or index segments in memory at once, across daily visits and session ticks | Concurrency |
| Reads | `max_concurrent_reads` | 64 | Public reads running at once. Each page takes one slot | Concurrency |
| Publications | `publication.max_concurrent_publications` | 8 | Publications running at once | Concurrency |

All memos share one read working memory pool. When the pool is full, a memo evicts its own entries or doesn't retain new blocks, and reads don't wait or fail because of memory. The pool counts decoded data, index and filter sections, stored read-ahead bytes, keys, reference counts, and memo containers. Setting it to zero disables retention. A block that a caller already has stays valid after it's evicted.

A fold keeps its segment output in memory and takes its memo from the shared pool. A merge keeps its merge input and its segment output in memory, which comes to up to 96 MiB per concurrent compaction without sizing or overrides. A sweep visit that folds the WAL tail and then merges takes a fold permit for the fold, and then a compaction permit for the merge. So the fold and compaction limits bound that memory, not `max_concurrent_maintenance`.

A maintenance step merges only the runs that fit in `max_merge_input_bytes`. A larger window runs as a streaming compaction, which keeps at most that much decoded input in memory at once. So lowering `max_merge_input_bytes` moves work from maintenance steps to compactions.

The segment output budget has no setting, and a segment larger than the budget is written on its own.

When a checkpoint, snapshot, or fork has to fold the WAL tail first, it runs that fold with the shared read working memory pool. Creating a checkpoint, snapshot, or fork waits for a fold permit, even when the tail is already folded. A fork of a snapshot doesn't fold, and it doesn't take a fold permit.

Maintenance requests sent to the API don't count against `max_concurrent_maintenance`, but their folds and merges take the same fold and compaction permits as every other fold and merge.

`max_upload_bytes` and `max_download_bytes` limit the size of one proxied transfer, and both default to 256 MiB. They don't reserve any memory.

Without a memory limit, grep, a local cache, or overrides, the budgets that have a process-wide limit add up to 1,088 MiB:

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

The total leaves out:

- writer sessions,
- each open writer session's WAL tail, which the write stop (128 unfolded WAL objects) and the inline tail limit bound,
- page results,
- decoded blocks that callers still use after a memo evicts them,
- the WAL tails that running operations keep outside the head-state cache, and
- maintenance work other than folds and merges.

Shared cache entries and memo entries are charged separately, even when they refer to the same allocation. HTTP requests and runtime reads have their own concurrency limits, but page results and other temporary allocations are still outside the budgets in this total.

When the server serves grep queries, grep uses `grep.max_cache_bytes` for its block cache. With a memory limit, this setting is derived by default, and without one, it's 256 MiB. A query reads up to 32 candidate files of at most 8 MiB each at once, so one query can use up to 256 MiB. Queries have no concurrency limit of their own, but HTTP queries share the request cap.

Index building runs in daily sweep visits and in session ticks. A build step reads at most `max_content_bytes_per_step` of file content (64 MiB by default), and a reorganize step merges at most 64 MiB of decoded index blocks. A step that finds work waits for one of the `[grep].max_concurrent_steps` permits before it reads either kind of data, and it keeps the permit until it publishes. So by default, indexing has at most two steps of content in memory at once (up to 128 MiB), whatever `max_concurrent_maintenance` is. A step that finds the index up to date doesn't take a permit, so idle visits still run in parallel. To lower that ceiling, lower `[grep].max_concurrent_steps`.

The `loonfs_grep_steps_running` gauge reports the steps that have a permit, and the `loonfs_grep_steps_waiting` gauge reports the steps that are waiting for one. If `loonfs_grep_steps_waiting` stays above zero, indexing steps are waiting at the cap.

The server builds one `GrepStepBudget` from `[grep].max_concurrent_steps` for its grep worker. If you embed grep in a host that runs more than one worker (for example, one for each runtime), pass a clone of one budget to every worker, so the limit is one ceiling for the whole process. A worker that has its own budget adds its own steps on top of that ceiling.

The local cache adds `memory_bytes`, 64 MiB of write buffers for its disk tier, and up to 256 MiB of inserts waiting for the disk tier. There's no setting for the last two.

A 256 MiB container can't pass the sizing check, because managed memory alone must be at least 256 MiB after the fixed costs and headroom. With the default counts, a 2 GiB limit leaves about 1,051 MiB of managed memory. Use the formula in [Set one number](#set-one-number) when you change counts or override individual pools.

The server keeps a writer session open for each namespace it has written to, until background maintenance closes the session (see [Background maintenance](#background-maintenance)). One idle session uses about 3 KiB of heap, so 10,000 open sessions use about 30 MiB. A session also keeps its WAL tail in memory until a fold moves the tail into segments. The write stop (128 unfolded WAL objects) and the inline tail limit bound that tail. The server stops keeping a session when its namespace is deleted, or when a request finds that the namespace doesn't exist.

If a request finds that its namespace's session has been fenced, the request fails with `writer_fenced`, and the server drops the session's handle. The fenced session can't be used again, and the server doesn't resubmit the request. A later request starts a new session, and that session's first publish takes the namespace back. While the old session's work ends, opening the namespace can return `writer_session_closed` (503), and a later request can open it once that work ends. Other namespaces keep their sessions.

When the server drops a fenced session, it logs a warning with `namespace_id`, and with `active_writer_id` when the error includes the winning writer. The host table reports the number of fenced sessions dropped so far as `Namespaces::fenced_sessions_dropped`. If one namespace keeps getting fenced, two writers are receiving its traffic, so route that namespace to one writer. After a restart, the first write to each namespace acquires a new writer epoch.

`max_concurrent_folds` defaults to 2. If the `loonfs.execution_budget.folds_waiting` gauge stays above zero, WAL folds are waiting at the cap. Raise the cap only after you account for the extra object-store and CPU work. The `loonfs.execution_budget.compactions_waiting` gauge reports the same thing for `max_concurrent_compactions`.

`min_publish_interval_ms` sets the minimum time between publication starts in one namespace, and it defaults to 1000 ms. A cold namespace publishes immediately.

`consistency` sets the runtime's default consistency, which every read that doesn't set its own takes, and it defaults to `eventual`. `max_staleness_ms` sets the default maximum staleness of an eventual read, and it defaults to 1000 ms. While the server's last confirmation of a namespace head is younger than that, an eventual read reads at that head without a store request. A deployment whose readers follow writes made through other servers sets `consistency = "strong"`, so every read without its own setting confirms the head first. `manifest_revalidation_interval_ms` sets how often a read of a namespace that this server doesn't write checks for a manifest that another process published, and it defaults to 1000 ms. The server rejects either millisecond setting above 1155000 ms (19 minutes and 15 seconds), the revalidation bound.

`max_concurrent_reads` defaults to 64. A public runtime read waits for a slot before its first store request. Each pager page takes one slot and releases it before it returns. A streamed or ranged read releases its slot after the metadata and content lookup, before it returns the body, and the existing transfer limits bound the body. A buffered read keeps its slot until the call returns. The `loonfs.execution_budget.reads_running` and `loonfs.execution_budget.reads_waiting` gauges report active and queued reads.

An embedded host sets this limit with `ExecutionBudgetBuilder::max_concurrent_reads`, and read-only and writable runtimes can share the budget through `LoonFsBuilder::execution_budget`. Reads inside commits, folds, compactions, pin creation, and garbage collection call core directly, and they don't take a read slot. Grep queries and index steps take a slot for each public read they call. A caller that has a read slot doesn't wait for a grep step permit.

Publication admission counts queued and active callers, including duplicate commits, conflicts, and namespace deletes. A caller that disconnects is still counted until its admitted work settles. A request over a count limit or an estimated byte limit receives `commit_queue_full`, and admitted work waits for a shared publication slot. Each namespace has its own allowance, so one busy tenant can't use up the default host budget. The `loonfs.execution_budget.admission_rejections` counter counts the requests refused because the `max_requests` or `max_estimated_bytes` total was full. If the `loonfs.execution_budget.publications_waiting` gauge stays above zero, namespaces are waiting for a publication slot.

```toml
[publication]
max_requests = 8192
max_requests_per_namespace = 1024
max_estimated_bytes = 67108864
max_estimated_bytes_per_namespace = 8388608
max_concurrent_publications = 8
```

These are the defaults without a memory limit. With sizing, `max_estimated_bytes` is derived by default. Every value must be positive. The byte estimate counts request data, prepared proofs, and queue bookkeeping, but not allocator slack, HTTP request buffers, or the metadata and working copies that a publication loads. Size process memory for those costs, and for the separate fold and cache limits too.

An embedded host sets the per-namespace limits with `LoonFsBuilder::publication_policy`. It sets `max_requests`, `max_estimated_bytes`, and `max_concurrent_publications` on the execution budget, with `ExecutionBudgetBuilder::max_admitted_requests`, `max_admitted_bytes`, and `max_concurrent_publications`. Runtimes that share one budget share these three limits, but each runtime keeps its own per-namespace limits.

A hosted server configures [inline content](../../../docs/specs/glossary.md#inline-content) (a small file's bytes, included in the commit itself) in the `[inline_content]` table, with the settings below. Inline writes are enabled by default, with a 64 KiB threshold. To disable them, set `inline_content_threshold_bytes = false`. By default, capability discovery advertises `filesystem.commits.inline_content` and `commit.max_inline_content_bytes_per_operation`, and it leaves out both when inline writes are disabled.

An embedded host configures inline writes with `LoonFsBuilder::inline_content` and `InlineContentPolicy`. These settings don't change the format limits that readers apply.

| Setting | Default | Meaning |
| --- | --- | --- |
| `inline_content_threshold_bytes` | 64 KiB | Content at or under this size is prepared inline. `None` in the embedded policy, or `false` in the server TOML, disables inline writes. |
| `inline_content_wal_object_budget_bytes` | 1 MiB | Limits the inline bytes in one WAL object. The overflow is staged, in operation order. |
| `inline_content_fold_at_bytes` | 2 MiB | Makes an automatic fold due when unfolded inline bytes reach this size. |
| `inline_content_tail_limit_bytes` | 32 MiB | New content is staged when unfolded and admitted inline bytes would exceed this size. |

The threshold can't exceed 256 KiB, and the WAL object budget can't exceed 4 MiB. The WAL object budget, the fold trigger, and the tail limit must be positive, and the fold trigger can't exceed the tail limit. Inline payloads count toward the existing publication byte limits.

The tail limit uses the tail size that the session recorded at its last publish. After the session's tail position is invalidated, the limit uses the last tail size that this session observed, and that size counts a WAL put whose outcome is unknown as if it succeeded. A session that hasn't observed the tail admits at most the WAL object budget, and its first publish observes the tail.

The tail can exceed the limit by at most the WAL object budget, in two cases: a new session's first inline commit, and after a put whose outcome is unknown. Another writer can make the remembered size stale until this session's next publish. The `MAX_UNFOLDED_WAL_OBJECTS` write stop refuses new commits regardless of the inline tail limit.

Explicit metadata maintenance uses `MaintainMetadataOptions::inline_content_fold_at_bytes` (also 2 MiB by default) when a writer-derived handle has an observed tail count. Maintenance probes use the WAL object threshold, the time of the tail's newest commit, and manifest descriptors, and they don't replay the tail.

If a commit already succeeded, retrying the same request returns the original result without uploading the file again, as long as the commit receipt is still available. This also works after a restart, or on another server. A retry with changed bytes or a different subject returns `commit_id_reuse_conflict`.

## Optional local cache

The local cache stores replaceable copies of metadata blocks. It isn't durable, and you can delete it while the server is stopped.

```toml
[local_cache]
path = "/var/lib/loonfs/cache"
memory_bytes = 67108864
disk_bytes = 107374182400
```

`disk_bytes` must be at least 96 MiB. The cache allocates 16 MiB files up to that limit and keeps them open, so set the process's open-file limit higher than `disk_bytes / 16 MiB`.

On Kubernetes, set `localCache.enabled=true`, set `localCache.sizeLimit` higher than `disk_bytes`, and use `/var/cache/loonfs` as the cache `path` in the config. The chart uses an `emptyDir`, so a replacement pod starts with an empty cache.

## Shutdown and upgrades

This section describes how the server shuts down, and how to upgrade it or roll it back.

When the server receives `SIGTERM`, it stops accepting new requests, waits for active requests and running maintenance sweep visits, and finishes its shutdown work. The shutdown deadline (600 seconds by default) limits both waits. Docker and the Helm chart should allow 660 seconds before they send `SIGKILL`.

Before an upgrade, fold each namespace with the current version:

```bash
loonfs maintenance fold --namespace <namespace>
```

For Docker, pull the new version and replace the container with one that uses the same config, secrets, and object store.

For Kubernetes, run:

```bash
helm upgrade loonfs-server oci://ghcr.io/loonfs/charts/loonfs-server \
  --version X.Y.Z \
  --namespace loonfs \
  --reuse-values
```

The chart stops the old pod before it starts the new one, so the API is unavailable in between. Run the smoke test after the rollout.

Roll back only to a release whose release notes say that it reads this release's durable format. A fold doesn't downgrade the format. To roll back the Helm release, run:

```bash
helm rollback loonfs-server --namespace loonfs
```

## Background maintenance

This section describes the background maintenance that folds, compacts, indexes, and collects garbage, and how many store requests it costs.

The `maintenance` setting has four modes:

- `serve_and_maintain` (default): serves the maintenance API group and runs the maintenance sweep.
- `serve_only`: serves explicit maintenance requests, without the sweep.
- `maintain_only`: runs the sweep, without serving the maintenance API group.
- `disabled`: does neither.

A mode that doesn't serve the group responds to every route under `/v0/maintenance/` with `route_not_found`. A mode that doesn't maintain leaves background work to another process. Long metadata compactions can log progress for an extended period, and you don't need to do anything unless failures repeat.

A writer session folds its WAL tail at the fold thresholds. After each fold it publishes, it compacts its metadata, at most 16 compaction units each time. The session records its last published sequence number, the monotonic time of that publication, and whether metadata maintenance found no work left due. A publish clears that last fact. The host keeps the other scheduling facts next to the session's handle.

Every `tick_interval_ms` (5000 ms, or 5 seconds, by default), the sweep walks the host's session entries without cloning their handles. A session is Active until one tick has passed without a commit. After that, it's Settling while work is still due, or Quiet when its metadata is caught up, its maintained grep index is current, and collection isn't due. A Quiet tick costs zero store requests.

```text
+--------+  a tick without a commit, work due   +----------+
| Active | -----------------------------------> | Settling |
+--------+                                      +----------+
    |                                                 |
    | a tick without a commit, nothing due            | nothing left due
    v                                                 |
+-------+                                             |
| Quiet | <-------------------------------------------+
+-------+
```

A tick runs the first due step for each session, in this order:

1. **Metadata.** This step covers metadata work, including a tail that's waiting for its idle fold age. It makes one maintenance call, of at most 16 compaction units. If work remains or the call fails, the retry delay is `maintenance_interval_ms` (300000 ms, or 5 minutes, by default), but reaching the idle fold age allows one earlier attempt. A session fenced by another compactor costs 6 requests once per retry interval, instead of on every tick.
2. **Grep index.** When the index is maintained, and it's dirty or behind the published sequence number, this step runs up to 16 build steps and then a reorganization step. Unfinished work stays dirty for another tick. Enabling or disabling the index over HTTP makes the host keep a session for the namespace and marks the session's entry dirty, so a build continues even without a local commit.
3. **Collection.** This step collects garbage when the published sequence number has moved since the last successful collection and `gc_interval_ms` (3600000 ms, or 1 hour, by default) has passed. The first collection waits that interval after the host opens the session's entry.
4. **Close.** This step closes the session when it's Quiet, it has been idle longer than `idle_session_close_after_ms` (1800000 ms, or 30 minutes, by default), and the host's handle is the only reference to the session. The host checks the last opened time, the published sequence number, and handle ownership under the table lock. Opening the closing namespace waits for the drain and returns a fresh session, but opens of other namespaces don't wait for this close. The fresh session's first publish acquires a new writer epoch. Closing costs zero store requests.

Each session that the host keeps has its own publisher, engine state, head position, and allocated queue capacities. Closing the session releases them after its admitted work ends. Shared caches and execution limits have their own lifetimes. A server whose maintenance mode doesn't maintain has no sweep, so nothing closes its idle sessions.

The daily pass (the full sweep pass) runs when the server starts and then every `full_sweep_interval_ms` (86400000 ms, or 24 hours, by default). It lists every namespace, runs metadata and index maintenance, and collects garbage. It covers namespaces that this host doesn't write to, forks, deleted namespaces, work left by a crashed process, and garbage that was still inside a grace window. If the host loses its in-memory scheduling facts, no durable progress is lost.

The daily pass and the ticks run at the same time. They share the `max_concurrent_maintenance` visit slots (8 by default), and a per-namespace set of current visits keeps two visits to one namespace from overlapping. A streaming compaction uses one slot while the other free slots continue working. The daily pass lists up to 1000 namespace IDs per page, and it requests another page when its visit slots allow it. Hosts can call `Sweep::run_pass` and `Sweep::tick` directly.

All the interval and idle-close settings must be positive.

Grep queries stay correct while the index is behind. A query scans the commits after the index, and if it goes past its scan budget, it fails with `index_lagging` unless `allow_stale` is set.

When a call fails on one namespace, the server counts the failure in `loonfs.maintenance.sweep_visit_failures` and waits before it tries that call on that namespace again. The other calls on the namespace and the visits to other namespaces continue. The first wait is one tick interval, and each failure in a row doubles it, up to one hour. For example, with the default 5-second tick interval, a call that keeps failing is tried again after 5 seconds, 10 seconds, 20 seconds, and so on, and then once an hour. A success ends the wait. A call that panics counts as a failure with the code `server_error`, so it waits and is tried again the same way, and the visits to other namespaces continue. `loonfs.maintenance.sweep_backoff_entries` counts the calls that are waiting. The server tracks at most 10,000 of them, and at that limit a new failure replaces the entry whose next try is soonest.

The server logs the first failure at error level, each later failure at warning level, and the first success after a failure at info level. Each of these events has `namespace_id`, `call`, `code`, and `attempts` fields. Every panic is logged at error level with `namespace_id`, `call`, and a `panic` field that holds the panic message, and a panicked call also has `code` and `attempts`. While a call's wait is below one hour, an idle held session stays open, so the ticks keep trying the call. Once the wait reaches one hour, the session closes when it's idle, and the next full pass tries the call again.

Why doesn't the server stop maintaining a corrupt or forbidden namespace? Some failures that look permanent clear without a restart. Expired credentials and clock skew both return a permission error, a rolling deploy fences the old writer for a while, and an operator can repair a namespace in place. The doubling wait keeps a failing namespace from filling the logs or costing many store requests, and the namespace is maintained again as soon as the cause is fixed.

If listing the namespaces fails, the pass doesn't start any new visits. The visits already running finish, the pass ends with a warning, and the next full pass lists the namespaces again.

On shutdown, the server stops the sweep in these steps:

1. It stops the sweep at the same moment it stops admitting requests. No new visit starts, and a running streaming compaction stops at its next block.
2. After the request drain, it waits for the visits that are still running, up to the same `shutdown_deadline_ms` that limits the drain.
3. At that deadline, it drops the visits that are still running, logs a warning, and shuts the runtime down.

A dropped visit leaves the same state that a crash would, and a later pass does the work again. Like an abandoned request, a dropped visit doesn't make the shutdown fail.

An idle namespace costs no requests between full passes. A full pass costs one list request per page of namespaces, plus a fixed number of requests for each namespace. For an idle namespace (one with nothing to fold, compact, or collect), a full pass costs:

| Pass | Grep not maintained | Grep maintained, index not enabled | Grep index enabled |
| --- | --- | --- | --- |
| Does not collect | 6 GET or HEAD | 7 GET or HEAD | 21 GET or HEAD |
| Collects | 12 GET or HEAD and 10 LIST | 20 GET or HEAD and 12 LIST | 38 GET or HEAD and 12 LIST |

On a collection pass, each object that's still inside its collection grace adds a request, and a namespace with work due adds the requests for that work. Deleted namespaces stay listed, so each one keeps this cost on every full pass. Core metadata still costs 6 requests per visit, and garbage collection adds 16, for a total of 22. For example, a server with 1,000 caught-up idle namespaces and no grep sends no requests for them between full passes, and about 22,000 on each daily full pass, plus the namespace listing.

The full pass also collects deleted namespaces. A deleted namespace's content becomes eligible once its retirement grace passes, about 61 minutes after the delete. The next full pass collects eligible content, and later full passes keep collecting it. Late writes through upload capabilities that were already issued, and dependent forks, can extend reclamation, and the later passes find them. In a garbage collection report, `deleted.retired_content_objects` counts the listed content objects that the pass deleted, and a pass with nothing left under the content prefix reports zero. For what can block reclamation, see the API specification's [namespace deletion section](../../../docs/specs/api.md#63-delete-v0namespacesns).

If a `metadata` maintenance response includes the compaction outcome `metadata_compaction_required`, a streaming compaction is due, and the writer session or the next sweep visit runs it.

A namespace whose writes stop before its tail reaches the fold thresholds is folded by the first eligible metadata tick after its newest commit is `idle_fold_after_ms` old. This period defaults to 900000 ms, which is 15 minutes. If a visit slot is free, a tail that the host keeps gets its first idle-fold attempt within one tick after that age, even when the metadata retry delay is longer. After a restart, or after a write through another process, the fold waits for a full pass after the age threshold. A namespace that's written more often than once per period isn't folded this way. Instead, its writer folds it at the thresholds.

Set `idle_fold_after_ms = 0` to turn the rule off. An explicit `metadata` maintenance request uses the same period as the sweep. An embedded CLI profile doesn't read the server config, and it always uses 15 minutes.

Embedded hosts pass `MaintainMetadataOptions::idle_fold_after_ms` to `Maintenance::maintain_metadata_while_due_with_options`, where zero also turns the rule off. Both `maintain_metadata_while_due` forms return `Result<bool>`. `true` means that, under the options used, no metadata work remains or can become due without another commit. The call works out the result from the reads it has already made.

## Current limitations

A self-hosted deployment currently has these limitations:

- One process or pod serves the API.
- Restarts and upgrades cause a short outage.
- A second replica doesn't share traffic with the first.
- There's no leader election or automatic failover.
- The Helm chart creates only a Deployment and a ClusterIP Service.
