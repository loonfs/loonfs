# Self-hosting LoonFS

This guide describes how to run `loonfs-server` on your own infrastructure, with Docker or Kubernetes, and how to size, monitor, upgrade, and maintain it.

A LoonFS deployment is one server connected to an object store. All durable data is stored in the object store, and the server keeps only temporary state, in memory and in an optional cache on local disk.

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

[transport]
kind = "plaintext_behind_proxy"

[store]
kind = "local_fs"
root = "/var/lib/loonfs/store"
```

Use `kind = "plaintext_behind_proxy"` only when a trusted proxy provides TLS. If LoonFS provides TLS directly, use this `[transport]` table instead:

```toml
[transport]
kind = "tls"
cert_path = "/etc/loonfs/tls/server.crt"
key_path = "/etc/loonfs/tls/server.key"
```

Generate the two required secrets and store them in your secret manager:

```bash
export LOONFS_AUTH_TOKEN="$(openssl rand -hex 32)"
export LOONFS_CONTENT_TOKEN_SECRET="$(openssl rand -hex 32)"
```

`LOONFS_AUTH_TOKEN` protects the HTTP API, and `LOONFS_CONTENT_TOKEN_SECRET` signs content transfer tokens. The config can contain these values, as the `token` of an `[auth]` table with `kind = "bearer_token"` and as `content_token_secret`, but environment variables make it easier to keep them out of the config file.

Don't expose a server without authentication. LoonFS rejects a config without authentication on a non-loopback address, unless the config has an `[auth]` table with `kind = "open"`. That setting is intended only for controlled testing.

For S3, configure the bucket to remove incomplete multipart uploads. S3 calls this action `AbortIncompleteMultipartUpload`. (LoonFS doesn't use multipart uploads on Google Cloud Storage, so a GCS bucket needs no such rule.)

## Set one number

This section describes how the server sizes its memory budgets from one memory limit.

Set the top-level `memory_limit_bytes` to the pod's memory limit:

```toml
memory_limit_bytes = 4294967296
```

If you leave it out on Linux, the server reads `/sys/fs/cgroup/memory.max` (cgroup v2), and then `/sys/fs/cgroup/memory/memory.limit_in_bytes` (cgroup v1). It ignores a missing or unreadable file, the value `max`, an invalid number, and any value of 1 EiB or more. If the server doesn't find a finite limit, it keeps all the existing defaults and logs at startup that it's unsized.

From the limit, the server calculates the managed memory that it divides among its budgets:

```text
fixed = 128 MiB
      + max_connections x 472 KiB
      + (max_concurrent_uploads + max_concurrent_downloads) x 8 MiB
      + (max_concurrent_folds + max_concurrent_compactions) x 32 MiB
      + max_concurrent_content_writes x 16 MiB (72 MiB on R2)
      + 32 MiB
      + 64 MiB + metadata_cache.ssd.max_segment_bytes / 4 KiB x 168 B, with a [metadata_cache.ssd] table
headroom = 200 MiB + 20% of (memory_limit_bytes - fixed - 200 MiB)
managed = memory_limit_bytes - fixed - headroom - metadata_cache.max_manifest_bytes
        - metadata_cache.max_head_bytes - max_merge_bytes
```

`fixed` covers the process's own cost, response data that the HTTP transport keeps, transfer chunks, the encoded segments that each fold and compaction writes, the bytes that each content write keeps in memory, the completed upload sessions that garbage collection keeps between collections, and the SSD tier of the metadata segment cache. A content write keeps at most 16 MiB in memory, which is the part that an assembly builds and the source read that it copies into that part, at 8 MiB each. On R2, the part can be 64 MiB, because an R2 assembly sends a part through the server whenever the part doesn't lie within one source object, so a content write keeps up to 72 MiB. The SSD tier's term is its 64 MiB write queue plus its index of the blocks on disk, at 168 bytes for each block. The tier charges each block at least 4 KiB against its budget, so it keeps at most one block for each 4 KiB of `max_segment_bytes`, and the term is a bound: about 484 MiB for a 10 GiB tier, and about 4.2 GiB for a 100 GiB tier. (In a compacted namespace of 20,000 files, the average stored block was 4.1 KiB.) (See [Keeping metadata blocks on local SSD](#keeping-metadata-blocks-on-local-ssd).)

The headroom covers memory that the allocator keeps after the server frees it. The budgets share the other 80% of what's left after `fixed` and the 200 MiB, so the headroom is 200 MiB plus a quarter of the budgets.

The budgets of the manifest cache (`metadata_cache.max_manifest_bytes`) and the head cache (`metadata_cache.max_head_bytes`) aren't shares of managed memory, because the memory that manifests and namespace heads need depends on the namespaces that the server reads, not on the host. They're 32 MiB and 16 MiB unless you set them, with or without a limit. The merge allowance (`max_merge_bytes`) isn't a share either, because the memory that a merge needs depends on the merges that callers make, not on the host. It's 512 MiB unless you set it, with or without a limit. Setting it to zero turns merges off, and the formula then reserves nothing for them (see [Resource sizing](#resource-sizing)). Startup fails if managed memory is below 256 MiB, or if the final budgets plus `fixed` and the headroom exceed the limit. The error message lists each term. Fractions are rounded down to whole bytes.

| Budget | Share of managed memory | Setting |
| --- | ---: | --- |
| Metadata segment cache | 30% | `metadata_cache.max_segment_bytes` |
| WAL tail cache | 10% | `metadata_cache.max_wal_tail_bytes` |
| Content object cache | 10% | `metadata_cache.max_content_object_bytes` |
| Grep cache | 10% | `grep.max_cache_bytes` (added to the segment cache's share when grep isn't served) |
| Read working memory | 20% | `max_read_working_bytes` |
| Admitted publication bytes | 10% | `max_admitted_bytes` |
| Compaction input | 10% | `max_compaction_input_bytes`, divided by `max_concurrent_compactions` |

You can still set each budget's setting to override its share. The sum check uses the final values, including one compaction input allowance for each concurrent compaction. At startup, the server logs one line for each term, with its final value and whether it was derived or set. Without sizing, the budgets you didn't set are logged as defaults.

Sizing doesn't change the counts, which keep their defaults: 8 publications, 2 folds, 2 compactions, 32 content writes, 64 reads, 256 requests, 1,024 connections, 8 uploads, and 16 downloads. Count settings you set explicitly still apply. The formula uses the final connection, transfer, fold, compaction, and content write counts, and it divides compaction input by the final compaction count.

Why is the headroom 200 MiB plus a quarter of the budgets? In the lab, the server's peak memory on Linux with the three glibc settings below was 10% to 32% above the same workload's memory on macOS, and a quiet phase stayed about 200 MiB above it. With glibc's defaults, the peaks were up to about three times the macOS figures (769 MiB against 256 MiB for one workload), which is more than the headroom covers. So for a Linux deployment, set these environment variables (the published image already sets them):

- `MALLOC_ARENA_MAX=2` limits glibc to two arenas (separate heaps), instead of up to eight for each CPU core.
- `MALLOC_TRIM_THRESHOLD_=131072` makes glibc return free memory at the top of a heap to the kernel once it's more than 128 KiB.
- `MALLOC_MMAP_THRESHOLD_=131072` makes glibc allocate each block of 128 KiB or more with `mmap`, so freeing the block returns its memory to the kernel.

Why doesn't the formula include grep queries or the WAL tails of open writer sessions? Neither has a process-wide limit that the formula could add. One query reads up to 256 MiB at once, and only the request cap limits how many queries run. The write stop and the inline tail limit bound each session's tail, but nothing limits the number of open sessions.

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

This command validates the config and the TLS files, but it doesn't contact the object store. With an SSD tier, it also creates the tier's directory if it's missing, and checks that the server can write a file in it. Check object-store access after the server starts (see [4. Verify the deployment](#4-verify-the-deployment)).

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
- Configure S3 buckets to remove incomplete multipart uploads.
- Allow at least 660 seconds for graceful shutdown.
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
| `loonfs_manifest_cache_gets_total` | Counter, `result` label | A read looks up its namespace's decoded manifest: `hit` or `miss`. After a miss, the read loads the manifest from the store. |
| `loonfs_manifest_cache_evictions_total` | Counter | A manifest is evicted at the `metadata_cache.max_manifest_bytes` budget. Metadata blocks don't evict manifests, because manifests have a budget of their own. |
| `loonfs_manifest_cache_retained_decoded_bytes` | Gauge | Bytes charged to the manifests in the manifest cache now. Each entry's charge includes its key and the cache's own allocation for the entry. |
| `loonfs_head_cache_gets_total` | Counter, `result` label | The read cache looks up a namespace head: `hit` or `miss`. A read that confirms or loads its head can look the head up more than once, so `loonfs.read_head.source` is the count of reads. |
| `loonfs.read_head.source` | Counter, `source` label | A read takes the namespace head it starts from: `position` when the server has the namespace's writer session, so the read sends no request to confirm the head except the session's check for a newer manifest, `refreshed` when the head comes from the read cache and the read confirms it, `loaded` when the read cache has no head for the namespace and the read loads it from the store, or `eventual` when an `eventual` read takes the read cache's head within its maximum staleness, without a request. |
| `loonfs_head_cache_evictions_total` | Counter | A namespace head is evicted at the `metadata_cache.max_head_bytes` limit. A namespace head larger than the limit isn't cached, and each read of it loads it on its own. |
| `loonfs_head_cache_retained_decoded_bytes` | Gauge | Bytes charged to the namespace heads in the cache now. Each entry's charge includes its key and the cache's own allocation for the entry. |
| `loonfs_wal_tail_cache_evictions_total` | Counter | A WAL-tail projection is evicted at the `metadata_cache.max_wal_tail_bytes` limit. A WAL-tail projection never evicts a namespace head, because heads have a limit of their own. |
| `loonfs_wal_tail_cache_retained_decoded_bytes` | Gauge | Bytes charged to the WAL-tail projections in the cache now. Each entry's charge includes its key and the cache's own allocation for the entry. |
| `loonfs_segment_cache_gets_total` | Counter, `tier` and `result` labels | A read looks up a metadata block in a tier of the segment cache. `tier` is `memory`, or `ssd` for the [SSD tier](#keeping-metadata-blocks-on-local-ssd), and `result` is `hit` or `miss`. A read that misses memory reads the block from disk, and a read that misses the SSD tier goes to the store, so `{tier="ssd",result="hit"}` records the GETs that the SSD tier saved. A damaged file that the server deletes is recorded as a miss. A server without the SSD tier reports no `ssd` series. |
| `loonfs_segment_cache_inserts_total` | Counter, `tier` label | A tier keeps a new block. The SSD tier also records each block that it finds in its directory when the server starts. |
| `loonfs_segment_cache_evictions_total` | Counter, `tier` label | A tier deletes its least recently used block at its budget, `metadata_cache.max_segment_bytes` in memory or `metadata_cache.ssd.max_segment_bytes` on disk. |
| `loonfs_segment_cache_rejections_total` | Counter, `tier` label | A tier doesn't keep a block. In memory, the block is larger than the whole budget. On disk, the block is larger than the whole budget, the write queue is full, or the write failed, and the block stays in memory. |
| `loonfs_segment_cache_retained_decoded_bytes` | Gauge | Bytes charged to the blocks in the metadata segment cache in memory, up to `metadata_cache.max_segment_bytes`. Each entry's charge includes its key and the cache's own allocation for the entry. |
| `loonfs_segment_cache_retained_stored_bytes` | Gauge | Bytes charged to the blocks on the SSD tier, up to `metadata_cache.ssd.max_segment_bytes`. Each block's charge is its stored length (4 KiB for a smaller block) and its index entry's allocation in memory, so the gauge is larger than the bytes on disk when blocks are small. |
| `loonfs_content_object_cache_evictions_total` | Counter | A content object is evicted at the `metadata_cache.max_content_object_bytes` limit. |
| `loonfs_grep_cache_gets_total` | Counter, `result` label | A grep query looks up a decoded block of the grep index in the grep cache: `hit` or `miss`. A server that doesn't serve grep has no grep cache, so it reports no `loonfs_grep_cache_*` series. |
| `loonfs_grep_cache_evictions_total` | Counter | A decoded grep block is evicted at the `grep.max_cache_bytes` limit. |
| `loonfs_grep_cache_retained_decoded_bytes` | Gauge | Bytes charged to the decoded grep blocks in the cache now, up to `grep.max_cache_bytes`. Each entry's charge includes its key and the cache's own allocation for the entry. |
| `loonfs_grep_worker_cache_gets_total` | Counter, `result` label | A grep build or reorganize step looks up a decoded block of the grep index in the worker's cache: `hit` or `miss`. A server that doesn't maintain the grep index has no worker cache, so it reports no `loonfs_grep_worker_cache_*` series. |
| `loonfs_grep_worker_cache_evictions_total` | Counter | A decoded grep block is evicted at the worker cache's 16 MiB limit, which no setting changes. |
| `loonfs_grep_worker_cache_retained_decoded_bytes` | Gauge | Bytes charged to the decoded grep blocks in the worker's cache now, up to 16 MiB. Each entry's charge includes its key and the cache's own allocation for the entry. |
| `loonfs_grep_publications_total` | Counter, `outcome` label | A publication of the server's grep steps ends: `published` when the change is in the manifest after the one that it was built on, `rebuilt` when its put lost to a manifest with the same grep entry (such as a fold's) and the change was published over that manifest, or `superseded` when another process changed grep's entry first, so the change wasn't published, and the next step starts again from the current manifest. A server that doesn't maintain the grep index reports no series. |
| `loonfs.publisher.tail_replays` | Counter | A publish reads the WAL tail from the store, because the session has no position to start from. This happens on a session's first publish, after a failed publish, when a successor manifest or WAL object that another process wrote is found, after a fork of the namespace's head, and when a check finds that the session's manifest is at or below the namespace's manifest boundary. After a quiet period of any length, the session confirms the tip with a HEAD request for the next WAL number, a HEAD request for the next manifest number, and a conditional read of the manifest boundary, and it doesn't read the tail again. A session that commits at least every 5 minutes sends those requests at most once every 5 minutes. A manifest that this runtime publishes itself doesn't cause a replay, because after a fold, a compaction, a snapshot, or a grep publication in the same runtime, the session moves to the new manifest and keeps the tail above it. Garbage collection publishes no manifest, so it doesn't cause a replay either. |
| `loonfs.publisher.sessions_open` | Gauge | Open writer sessions, and sessions whose admitted work is still finishing. A fenced session stops being counted once its work ends. |
| `loonfs.publisher.write_stop_waits` | Counter | A commit waits in its namespace's queue at the write stop for the session's fold. Each commit increments it once, however many folds it waits for. |
| `loonfs.publisher.write_stop_wait_seconds` | Histogram | How long a namespace's queue waited at the write stop, until a fold published, the fold task ended, or the write-stop wait passed. |
| `loonfs.maintenance.sweeps` | Counter, `result` label | A sweep ends: `ok` if it listed every namespace, or `error` if the listing failed. |
| `loonfs.maintenance.sweep_seconds` | Histogram | How long one sweep took. Sweeps run when the server starts and every `sweep_interval_ms`. |
| `loonfs.maintenance.sweep_failures` | Counter, `call` label | One maintenance call failed on one namespace or one pool: `metadata`, `grep_index`, `gc`, `retention` (the sweep's retention advance), or `pool_gc` (the sweep's collection of a pool). The `gc` call also deletes the grep index's unreferenced segments. The scheduler tries the call again after a backoff, or on the next sweep for `retention` and `pool_gc`. |
| `loonfs.maintenance.sweep_panics` | Counter | A maintenance call or an idle-session close panicked. The scheduler contains the panic, counts a panicked call as a `server_error` failure, and goes on. A nonzero count is a bug to report. |
| `loonfs.maintenance.sweep_namespaces` | Gauge | The number of namespaces that the last finished sweep listed, including deleted and retired namespaces. |
| `loonfs.maintenance.compaction_skipped` | Counter, `reason` label | A metadata compaction unit published its output with the rows that an expired deletion hides kept instead of removed: `expired_row_removal_walk_limit` when the walk that finds the inodes to keep reached its limit of 4,096 open directories. The server logs the skip at error level with `namespace_id` and `job_id` fields when the walk stops, whether or not the unit goes on to publish. If the count rises with every compaction of a namespace, the namespace's metadata keeps those rows. The `unknown` series stays at 0. |
| `loonfs.gc.skipped` | Counter, `reason` label | A pool's collection deleted no content objects, because it couldn't read the roots of every member of the pool: `content_deletion_pool_budget` when the time to read them ran out, or `content_deletion_unreadable_member` when one member's roots couldn't be read. The server logs each skip at error level with a `pool_id` field. If the count rises on every sweep, the pool's unreferenced content objects stay in the store. The `unknown` series stays at 0. |
| `loonfs.object_store.operations` | Counter, `operation`, `result`, and `key_class` labels | A store call finishes. `key_class` is `content`, `wal_object`, `namespace_manifest` (manifests and the manifest boundary), `metadata_segment`, `gc_control` (snapshots), `metadata` (upload sessions), or `unknown`. |

## Logs

The server writes JSON logs to standard output.

- Leave `LOONFS_TRACE` unset, or set it to `json`, to enable logging.
- Set `LOONFS_TRACE=off` to disable logging.
- Use `RUST_LOG` to change the filter, for example `RUST_LOG=loonfs_core=debug`.

The server rejects any other `LOONFS_TRACE` value, instead of guessing what it means.

## Grep indexing

Set `[grep].mode` to choose whether this server serves searches, maintains the search index, or does both. Leave out the `[grep]` table to disable grep.

The maintenance scheduler builds the index, so indexing runs only on a server whose `maintenance` mode maintains (see [Background maintenance](#background-maintenance)).

These optional settings limit indexing work, and each one must be positive:

- `max_files_per_step` (256 files by default, and at most 1,000) and `max_content_bytes_per_step` (64 MiB by default) limit the input work of each indexing step.
- `max_concurrent_steps` (2 by default) limits how many indexing steps have file content or index segments in memory at once.

[Resource sizing](#resource-sizing) describes how these limits affect memory.

Segment sizes, reorganization thresholds, and reorganization step sizes use the engine's defaults, as they do for metadata compaction. The only input limits the config accepts are `max_files_per_step` and `max_content_bytes_per_step`.

## Resource sizing

This section describes the limits on the server's requests, connections, memory, and concurrent work.

`request_deadline_ms` sets the deadline for metadata and query requests, and it defaults to 60000 ms. Streamed content and long-running operator work are exempt. The server sets the write-stop wait to half of this deadline. A commit that reaches the write stop (64 MiB of unfolded WAL bytes or 1,024 unfolded WAL objects) waits up to that long for the session's fold. Because the wait is half the deadline, such a commit receives `maintenance_required`, which means it wasn't committed, instead of `deadline_exceeded`, which leaves its outcome unknown.

`max_in_flight_requests` defaults to 256, and `max_connections` defaults to 1,024. Both must be positive. The sizing formula uses the configured connection count. Keep the pod's open-file limit above that count.

The server applies the request cap before authentication and before it reads the request body. A request over the cap receives `503 server_busy` with `Retry-After: 1`, the usual error envelope, and a request ID, and the server doesn't read its body or make any store call. Each admitted request keeps its slot until its response body yields its last frame, fails, or is dropped. Downloads, JSON pages, and error envelopes all use data frames of at most 64 KiB. Uploads and downloads also count against their existing transfer caps.

`GET` and `HEAD` requests to `/health` and `/readiness` are exempt from the request cap, including requests with probe query parameters. `/metrics` is subject to the request cap.

After a request releases its permit, the HTTP transport can still keep response data in memory, up to its write threshold plus one frame for each response in flight. At the library defaults, HTTP/1 keeps at most 408 KiB + 64 KiB = 472 KiB per connection, and HTTP/2 keeps at most 400 KiB + 64 KiB = 464 KiB per stream. Both are about 480 KiB. The default 200 HTTP/2 streams add up to 90.625 MiB per connection. At `max_connections = 1024`, the totals are 472 MiB for HTTP/1 or 90.625 GiB for HTTP/2. This retained response data is the one memory cost that the request cap doesn't cover. (These figures count response data, not total connection memory.)

At the connection cap, the server waits before it accepts another TCP connection. A client can still complete a TCP connection into the OS backlog, but it receives no HTTP response or TLS handshake until a slot is free. If the backlog fills, connecting clients can time out or fail, depending on the OS. Idle keep-alive connections and incomplete TLS handshakes keep their slots until they close. Probes still need a connection slot, so size the connection cap to leave room for them.

Each cache, queue, and unit of work has its own memory budget. The values in the table below are ceilings, not allocations, so memory grows toward a ceiling only as a cache fills or as work runs. The sizing formula reserves the fixed costs and headroom separately from the budgets, and it doesn't measure total process memory.

| Budget | Setting | Default | What it bounds | Kind |
| --- | --- | --- | --- | --- |
| Metadata segment cache | `metadata_cache.max_segment_bytes` | Derived: 30%, or 40% without grep. 256 MiB without a memory limit | Decoded metadata blocks | Steady |
| Manifest cache | `metadata_cache.max_manifest_bytes` | 32 MiB, with or without a memory limit | Decoded manifests for reads, about 600 namespaces of 10,000 files at 56 KB each, or about half as many when each has a grep index | Steady |
| SSD tier of the segment cache | `metadata_cache.ssd.max_segment_bytes` | None, because the tier is off without `[metadata_cache.ssd]` | The tier's 64 MiB write queue, and an index of 168 bytes for each block on disk | Steady |
| Grep cache | `grep.max_cache_bytes` | Derived: 10%. 256 MiB without a memory limit | Decoded grep blocks when serving queries | Steady |
| Head cache | `metadata_cache.max_head_bytes` | 16 MiB, with or without a memory limit | Cached namespace heads for reads, about 18,000 namespaces at 0.9 KB each | Steady |
| WAL tail cache | `metadata_cache.max_wal_tail_bytes` | Derived: 10%. 64 MiB without a memory limit | Cached WAL tails for reads, for any number of namespaces | Steady |
| Completed upload sessions | None | 32 MiB | The content ID and completion time of each completed upload session, which garbage collection keeps between collections | Steady |
| Content object cache | `metadata_cache.max_content_object_bytes` | Derived: 10%. 96 MiB without a memory limit | Verified content objects of up to 1 MiB that reads keep for later reads | Steady |
| Publication queue | `max_admitted_bytes` | Derived: 10%. 64 MiB without a memory limit | Estimated bytes of admitted commit requests, other than a merge | Steady |
| HTTP requests | `max_in_flight_requests` | 256 | Requests until their responses complete, excluding health and readiness probes | Concurrency |
| Connections | `max_connections` | 1,024 | Accepted TCP connections, including TLS handshakes and idle keep-alive connections | Concurrency |
| Proxied uploads | `max_concurrent_uploads` | 8 uploads | At most one 8 MiB transfer part per upload body | Per request |
| Proxied downloads | `max_concurrent_downloads` | 16 streams | One 8 MiB read chunk per content stream | Per request |
| Read working memory | `max_read_working_bytes` | Derived: 20%. 256 MiB without a memory limit | Blocks retained across read, publication, fold, and snapshot-fold memos | Shared |
| Compaction input | `max_compaction_input_bytes` | Derived: 10% divided by the compaction count. 64 MiB without a memory limit | Decoded blocks that one compaction keeps in memory while it runs | Per operation |
| Merge allowance | `max_merge_bytes` | 512 MiB, with or without a memory limit. Zero turns merges off | Estimated peak memory of the merges that the server plans and publishes at the same time, three times each merge's planned WAL bytes or the memory that planning the merge uses, whichever is larger | Shared |
| Segment output | None | 32 MiB | Encoded segments that one fold, compaction, or maintenance step keeps in memory while it writes them | Per operation |
| Content writes | `max_concurrent_content_writes` | 32 writes | Content object writes running at once, for the content that folds, validation, and direct downloads write and for staged uploads. Each keeps at most 16 MiB in memory, or 72 MiB on R2 | Per operation |
| WAL folds | `max_concurrent_folds` | 2 | Folds running at once, including the folds that maintenance requests, namespace deletes, and snapshot and fork creation start | Concurrency |
| Compactions | `max_concurrent_compactions` | 2 | Metadata compactions running at once, whether writer sessions, the maintenance scheduler, or maintenance requests start them | Concurrency |
| Maintenance slots | `max_concurrent_maintenance` | 8 | Namespaces whose maintenance steps run at once, including their grep indexing | Concurrency |
| Grep steps | `[grep].max_concurrent_steps` | 2 | Grep build and reorganize steps that have file content or index segments in memory at once, across sweeps and session steps | Concurrency |
| Reads | `max_concurrent_reads` | 64 | Public reads running at once. Each page takes one slot | Concurrency |
| Publications | `max_concurrent_publications` | 8 | Publications running at once | Concurrency |

All memos share one read working memory budget. When the read working memory is full, a memo evicts its own entries or doesn't retain new blocks, and reads don't wait or fail because of memory. The budget counts decoded data, index and filter sections, keys, reference counts, and memo containers. Setting it to zero disables retention. A block that a caller already has stays valid after it's evicted.

A fold keeps its segment output in memory and takes its memo from the shared read working memory. A compaction keeps its compaction input and its segment output in memory, which comes to up to 96 MiB per concurrent compaction without sizing or overrides. A metadata step that folds the WAL tail and then compacts takes a fold permit for the fold, and then a compaction permit for the compaction. So the fold and compaction limits bound that memory, not `max_concurrent_maintenance`.

A compaction keeps at most `max_compaction_input_bytes` of decoded input in memory at once, whatever the size of its window, so lowering the setting makes compactions read in smaller waves.

The segment output budget has no setting, and a segment larger than the budget is written on its own.

When a snapshot or a fork has to fold the WAL tail first, it runs that fold with the shared read working memory. Creating a snapshot or a fork waits for a fold permit, even when the tail is already folded. A fork of a snapshot doesn't fold, and it doesn't take a fold permit.

Maintenance requests sent to the API don't count against `max_concurrent_maintenance`, but their folds and compactions take the same fold and compaction permits as every other fold and compaction, and a maintenance request's fold or compaction unit also waits for a fold or a unit of the same namespace that's already running.

`max_upload_bytes` and `max_download_bytes` limit the size of one proxied transfer, and both default to 256 MiB. They don't reserve any memory.

Without a memory limit, grep, an SSD tier, or overrides, the budgets in the table above that have a byte ceiling add up to 2,288 MiB:

| Budget | Ceiling |
| --- | --- |
| Metadata segment cache | 256 MiB |
| Manifest cache | 32 MiB |
| Head cache | 16 MiB |
| WAL tail cache | 64 MiB |
| Content object cache | 96 MiB |
| Completed upload sessions | 32 MiB |
| Publication queue | 64 MiB |
| Merge allowance | 512 MiB |
| 8 uploads at 8 MiB | 64 MiB |
| 16 downloads at 8 MiB | 128 MiB |
| 32 content writes at 16 MiB | 512 MiB |
| Read working memory | 256 MiB |
| 2 fold outputs at 32 MiB | 64 MiB |
| 2 compactions at 96 MiB | 192 MiB |
| Total | 2,288 MiB |

The total leaves out:

- writer sessions,
- each open writer session's WAL tail, which the write stop (64 MiB of unfolded WAL bytes or 1,024 unfolded WAL objects) and the inline tail limit bound,
- page results,
- decoded blocks that callers still use after a memo evicts them,
- the WAL tails that running operations keep outside the WAL tail cache, and
- maintenance work other than folds and compactions.

Shared cache entries and memo entries are charged separately, even when they refer to the same allocation. HTTP requests and runtime reads have their own concurrency limits, but page results and other temporary allocations are still outside the budgets in this total.

When the server serves grep queries, grep uses `grep.max_cache_bytes` for its block cache. With a memory limit, this setting is derived by default, and without one, it's 256 MiB. A query reads up to 32 candidate files of at most 8 MiB each at once, so one query can use up to 256 MiB. Queries have no concurrency limit of their own, but HTTP queries share the request cap.

Index building runs in the index steps of sweeps and of open sessions. A build step reads at most `max_content_bytes_per_step` of file content (64 MiB by default), and a reorganization step keeps at most 64 MiB of decoded index blocks in memory. A step that finds work waits for one of the `[grep].max_concurrent_steps` permits before it reads either kind of data, and it keeps the permit until it publishes. So by default, indexing has at most two steps of content in memory at once (up to 128 MiB), whatever `max_concurrent_maintenance` is. A step that finds the index up to date doesn't take a permit, so the index steps of idle namespaces still run in parallel. To lower that ceiling, lower `[grep].max_concurrent_steps`.

The `loonfs_grep_steps_running` gauge reports the steps that have a permit, and the `loonfs_grep_steps_waiting` gauge reports the steps that are waiting for one. If `loonfs_grep_steps_waiting` stays above zero, indexing steps are waiting at the cap.

The server builds one `GrepStepBudget` from `[grep].max_concurrent_steps` for its grep worker. If you embed grep in a host that runs more than one worker (for example, one for each runtime), pass a clone of one budget to every worker, so the limit is one ceiling for the whole process. A worker that has its own budget adds its own steps on top of that ceiling.

A 256 MiB container can't pass the sizing check, because managed memory alone must be at least 256 MiB after `fixed` and the headroom. With the default counts, a 2.5 GiB limit leaves about 157 MiB of managed memory, which is too little, and the smallest limit that passes is 2,684 MiB. On R2, the default 32 content writes take 2,304 MiB, so the smallest limit that passes is 4,476 MiB unless you lower `max_concurrent_content_writes` or `max_merge_bytes`. A deployment that never merges sets `max_merge_bytes = 0`, and then the smallest limit that passes is 2,044 MiB, or 3,836 MiB on R2. Use the formula in [Set one number](#set-one-number) when you change counts or override individual budgets.

The server keeps a writer session open for each namespace it has written to, until background maintenance closes the session (see [Background maintenance](#background-maintenance)). One idle session uses about 3 KiB of heap, so 10,000 open sessions use about 30 MiB. A session also keeps its WAL tail in memory until a fold moves the tail into segments. The write stop (64 MiB of unfolded WAL bytes or 1,024 unfolded WAL objects) and the inline tail limit bound that tail. The server stops keeping a session when its namespace is deleted, or when the session or the maintenance scheduler finds that the namespace was deleted or doesn't exist.

If a request finds that its namespace's session has been fenced, the request fails with `writer_fenced`, and the next request for the namespace drops the session's handle. The fenced session can't be used again, and the server doesn't resubmit the request. A later request starts a new session, and that session's first publish takes the namespace back. While the old session's work ends, opening the namespace can return `writer_session_closed` (503), and a later request can open it once that work ends. Other namespaces keep their sessions.

When the server drops a fenced session, it logs a warning with `namespace_id`. If one namespace keeps getting fenced, two writers are receiving its traffic, so route that namespace to one writer. After a restart, the first write to each namespace acquires a new writer epoch.

`max_concurrent_folds` defaults to 2. If the `loonfs.execution_budget.folds_waiting` gauge stays above zero, WAL folds are waiting at the cap. Raise the cap only after you account for the extra object-store and CPU work. The `loonfs.execution_budget.compactions_waiting` gauge reports the same thing for `max_concurrent_compactions`.

`max_concurrent_content_writes` defaults to 32. A fold writes up to 32 content objects at once, and each write takes a content write permit, as does each content object that validation or a direct download writes and each staged upload. So by default, one fold can use every permit. If the `loonfs.execution_budget.content_writes_waiting` gauge stays above zero, content writes are waiting at the cap, and the `loonfs.execution_budget.content_writes_running` gauge reports the writes that have a permit. Each permit adds 16 MiB to `fixed` (72 MiB on R2), so raising the cap leaves less managed memory. An embedded host sets this limit with `ExecutionBudgetBuilder::max_concurrent_content_writes`.

`min_publish_interval_ms` sets the minimum time between publication starts in one namespace, and it defaults to 200 ms. A request to an idle namespace publishes immediately. A request that arrives less than the interval after the last publication started waits until the interval has passed, and the next WAL object includes the commits queued in that time. So a longer interval writes fewer, larger WAL objects and keeps the namespace's [fold](../../../docs/specs/glossary.md#fold) rate low, and a shorter interval lowers commit latency and raises throughput. Measured on S3 with 256 clients that each wait for their commits, one namespace published 244 commits per second with a median commit latency of 1,049 ms at an interval of 1,000 ms. At 100 ms, it published 1,330 commits per second with a median latency of 183 ms, and at 15 ms (the default for an embedded runtime), 1,930 per second with a median latency of 127 ms. The default of 200 ms writes at most five WAL objects a second per namespace and avoids most of the latency and throughput cost of a 1,000 ms interval. (The interval bounds how many WAL objects the namespace writes each second, and so how many folds it needs, and each fold costs segment and manifest puts and reads.)

`consistency` sets the runtime's default consistency, which every read that doesn't set its own takes, and it defaults to `eventual`. `max_staleness_ms` sets the default maximum staleness of an eventual read, and it defaults to 1000 ms. While the server's last confirmation of a namespace head is younger than that, an eventual read reads at that head without a store request. A deployment whose readers follow writes made through other servers sets `consistency = "strong"`, so every read without its own setting confirms the head first. `manifest_revalidation_interval_ms` sets how often a read of a namespace that this server doesn't write checks for a manifest that another process published, and it defaults to 1000 ms. The server rejects either millisecond setting above 1200000 ms (20 minutes), the revalidation bound.

`max_concurrent_reads` defaults to 64. A public runtime read waits for a slot before its first store request. Each pager page takes one slot and releases it before it returns. A streamed or ranged read releases its slot after the metadata and content lookup, before it returns the body, and the existing transfer limits bound the body. A buffered read keeps its slot until the call returns. The `loonfs.execution_budget.reads_running` and `loonfs.execution_budget.reads_waiting` gauges report active and queued reads.

An embedded host sets this limit with `ExecutionBudgetBuilder::max_concurrent_reads`, and read-only and writable runtimes can share the budget through `LoonFsBuilder::execution_budget`. Reads inside commits, folds, compactions, snapshot creation, and garbage collection call core directly, and they don't take a read slot. Grep queries and index steps take a slot for each public read they call. A caller that has a read slot doesn't wait for a grep step permit.

Publication admission counts queued and active callers, including duplicate commits, conflicts, and namespace deletes. A caller that disconnects is still counted until its admitted work settles. A request over a count limit or an estimated byte limit receives `commit_queue_full`, and admitted work waits for a shared publication slot. Each namespace has its own allowance, so one busy tenant can't use up the default host budget. The `loonfs.execution_budget.admission_rejections` counter counts the requests refused because the `max_admitted_requests` or `max_admitted_bytes` total was full. If the `loonfs.execution_budget.publications_waiting` gauge stays above zero, namespaces are waiting for a publication slot.

A [merge](../../../docs/specs/glossary.md#merge) counts as one request toward the request limits, but it isn't charged to the byte limits, which are sized for request bodies and would cut a large merge into pieces. Instead, merges share a merge allowance of their own (`max_merge_bytes`) that the sizing formula reserves. Before a merge plans, it reserves 8 MiB of the allowance (or the whole allowance, if the allowance is smaller than 8 MiB), so the allowance covers the merge's planning as well as its publication. While the merge plans, its reservation grows to the merge's estimated memory, and the merge keeps the whole reservation until its commit settles. The estimate is three times the merge's planned WAL bytes, or the memory that planning the merge uses (the fork's touched inodes, their paths, and one page of the fork's commits), whichever is larger. For example, with the default allowance of 512 MiB, at most 64 merges plan at the same time, and an allowance below 8 MiB admits one merge at a time.

If other merges, into any namespace, have reserved so much of the allowance that a merge can't make its first reservation or can't grow that reservation, the merge receives `server_busy` immediately. A retry is admitted once the allowance has room for the merge's reservation. A merge whose estimated memory exceeds the whole allowance receives `merge_too_large`, and the caller merges the fork's commits in several smaller ranges. The `loonfs.execution_budget.admitted_merges` gauge reports the number of admitted merges, the `loonfs.execution_budget.admitted_merge_bytes` gauge reports the bytes that those merges reserve, and `admission_rejections` doesn't count a merge that's refused because the allowance has no room for the merge. The server doesn't fold the namespace after a merge commits. The merge's WAL object counts toward the namespace's fold trigger like any other WAL object, so the namespace's writer session folds a large merge in its own folds.

The allowance covers the memory of every admitted merge, from the merge's plan through its publication. The peak comes during publication, which builds the commit's deltas and encodes its WAL object while the merge's operations are still in memory. (In the measurements below, planning a merge of 100,000 file creates peaked at about 563 MB, and its publication at 855 MB.) So the server estimates the peak as three times the merge's planned WAL bytes, the bound that also decides whether the merge fits one WAL object. The planned bytes allow for every name at its longest. In measurements of merges of file creates, the peak was 1.8 times the planned bytes with 13-byte names (855 MB for 100,000 creates) and 3.1 times with 255-byte names (1,665 MB for about 110,000 creates), so a merge of names near the 255-byte limit can exceed its estimate by a few percent. A file create plans about 4,850 bytes, so the default allowance of 512 MiB admits a merge of about 36,000 file creates. One WAL object fits about 110,000 creates, and an allowance of 1.5 GiB (three times the WAL object's 512 MiB) admits every merge that fits one WAL object. To raise the allowance, set `max_merge_bytes`, and size the pod for it, because the sizing formula reserves the whole allowance. To turn merges off, set `max_merge_bytes = 0`. The sizing formula then reserves nothing for merges, capability discovery leaves out `filesystem.merges`, and the merge route returns `not_supported`.

A commit at the write stop waits in its namespace's queue for the session's fold, for at most the write-stop wait, and it keeps its request slot while it waits. LoonFS releases the namespace's publication slot during the wait, so other namespaces keep publishing. One namespace's waiting commits can take every `max_in_flight_requests` slot, so other requests receive `server_busy` until the session's fold finishes, unless you set `max_requests_per_namespace` below `max_in_flight_requests`. The `loonfs.publisher.write_stop_waits` counter records the commits that waited, and `loonfs.publisher.write_stop_refusals` records the commits that waited for the whole write-stop wait.

```toml
max_admitted_requests = 8192
max_admitted_bytes = 67108864
max_merge_bytes = 536870912
max_concurrent_publications = 8

[publication]
max_requests_per_namespace = 1024
max_estimated_bytes_per_namespace = 8388608
```

These are the defaults without a memory limit. With sizing, `max_admitted_bytes` is derived by default, and `max_merge_bytes` keeps its default. Every value must be positive, except `max_merge_bytes`, where zero turns merges off. The byte estimate counts request data, prepared proofs, and queue bookkeeping, but not allocator slack, HTTP request buffers, or the metadata and working copies that a publication loads. Size process memory for those costs, and for the separate fold and cache limits too.

The `[publication]` table is the runtime's `PublicationPolicy`, and an embedded host sets the per-namespace limits and the write-stop wait in `RuntimeConfig::publication`. It sets `max_admitted_requests`, `max_admitted_bytes`, `max_merge_bytes`, and `max_concurrent_publications` on the execution budget, with the `ExecutionBudgetBuilder` methods of the same names. Runtimes that share one budget share these four limits, so their merges share one merge allowance, but each runtime keeps its own per-namespace limits.

A hosted server configures [inline content](../../../docs/specs/glossary.md#inline-content) (a small file's bytes, included in the commit itself) in the `[inline_content]` table, with the settings below. Inline writes are enabled by default, with a 64 KiB threshold. To disable them, set `inline_content_threshold_bytes = false`. By default, capability discovery advertises `filesystem.commits.inline_content` and `commit.max_inline_content_bytes_per_operation`, and it leaves out both when inline writes are disabled.

The `[inline_content]` table is the runtime's `InlineContentPolicy`, which an embedded host sets in `RuntimeConfig::inline_content`. These settings don't change the format limits that readers apply.

| Setting | Default | Meaning |
| --- | --- | --- |
| `inline_content_threshold_bytes` | 64 KiB | Content at or under this size is prepared inline. `None` in the embedded policy, or `false` in the server TOML, disables inline writes. |
| `inline_content_wal_object_budget_bytes` | 1 MiB | Limits the inline bytes in one WAL object. The overflow is staged, in operation order. |
| `inline_content_tail_limit_bytes` | 32 MiB | New content is staged when unfolded and admitted inline bytes would exceed this size. |

The threshold can't exceed 256 KiB, and the WAL object budget can't exceed 4 MiB. The WAL object budget and the tail limit must be positive. Inline payloads count toward the existing publication byte limits.

The tail limit uses the tail size from the session's position. A session has no position before its first publish, and after its position is dropped (for example, after a put whose outcome is unknown). Without a position, the session admits at most the WAL object budget of inline content until its next publish reads the tail from the store, so the tail can exceed the limit by the WAL object budget each time the position is dropped before a fold. At the write stop (64 MiB of unfolded WAL bytes or 1,024 unfolded WAL objects), new commits wait for a fold regardless of the inline tail limit.

Inline pieces are WAL bytes, so the fold trigger of 8 MiB of unfolded WAL bytes or 256 unfolded WAL objects covers them, and it isn't a setting. Explicit metadata maintenance and the maintenance scheduler fold at the same trigger as the writer session. Without the server's session for the namespace, they count the WAL bytes of the tail that they load. Maintenance probes use the WAL object threshold, the time of the tail's newest commit, and manifest descriptors, and they don't replay the tail.

If a commit already succeeded, retrying the same request returns the original result without uploading the file again, as long as the commit receipt is still available. This also works after a restart, or on another server. A retry with changed bytes or a different subject returns `commit_id_reuse_conflict`.

## Keeping metadata blocks on local SSD

This section describes the SSD tier of the metadata segment cache, which keeps metadata blocks on the server's local disk as well as in memory.

The server keeps the metadata blocks that reads load in memory, up to `metadata_cache.max_segment_bytes`. A block that isn't in memory costs a GET, and memory is empty after a restart. With the SSD tier, the server also writes every block that the cache inserts to a directory on local disk, and a read that doesn't find a block in memory reads the block from disk instead of the store. The server reads the directory when it starts, so the blocks on disk stay readable after a restart.

To turn the tier on, add the `[metadata_cache.ssd]` table with both of its settings:

```toml
[metadata_cache.ssd]
directory = "/var/lib/loonfs/ssd"
max_segment_bytes = 10737418240
```

- `directory` is the directory on the server's local SSD that the tier writes its blocks to. The server creates it if it's missing, and the server doesn't start if it can't create or read the directory.
- `max_segment_bytes` is the tier's budget for blocks on disk, for every namespace that the server serves. The tier charges each block its stored length, or 4 KiB for a smaller block, and when it's over the budget, it deletes its least recently used block. So a tier of blocks smaller than 4 KiB uses less disk than its budget, and the memory of its index stays within the term in `fixed`. The value must be greater than zero.

The tier keeps each block in a file of its own, with the block's bytes exactly as the segment stores them. A read checks a block from disk against the length and checksum that the segment records, as it checks a block from the store. If a block fails the check, or its file can't be read, the server deletes the file and reads the block from the store again.

The directory contains nothing that the server depends on, so you can empty it at any time, even while the server runs, and the reads that follow go to the store. Give each server a directory of its own. Each server keeps its own index of the directory, so two servers on one directory would delete blocks that the other server still lists, and the directory could grow past either budget.

The tier writes blocks to disk from a queue of up to 64 MiB, so a read doesn't wait for a disk write. If the queue is full when the cache inserts a block, the server doesn't write that block to disk, and the block stays in memory. The tier's queue and its index of the blocks on disk are a term in `fixed`, as [Set one number](#set-one-number) describes.

## Shutdown and upgrades

This section describes how the server shuts down, and how to upgrade it or roll it back.

When the server receives `SIGTERM`, it stops accepting new requests, waits for active requests and running maintenance steps, and finishes its shutdown work. The shutdown deadline (600 seconds by default) limits both waits. Docker and the Helm chart should allow 660 seconds before they send `SIGKILL`.

For Docker, pull the new version and replace the container with one that uses the same config, secrets, and object store.

For Kubernetes, run:

```bash
helm upgrade loonfs-server oci://ghcr.io/loonfs/charts/loonfs-server \
  --version X.Y.Z \
  --namespace loonfs \
  --reuse-values
```

The chart stops the old pod before it starts the new one, so the API is unavailable in between. Run the smoke test after the rollout.

Roll back only to a release whose release notes say that it reads this release's durable format. To roll back the Helm release, run:

```bash
helm rollback loonfs-server --namespace loonfs
```

## Background maintenance

This section describes the background maintenance that folds, compacts, indexes, advances retention floors, and collects garbage, and how many store requests it costs.

The `maintenance` setting has four modes:

- `serve_and_maintain` (default): serves the maintenance API group and runs the maintenance scheduler.
- `serve_only`: serves explicit maintenance requests, without the scheduler.
- `maintain_only`: runs the scheduler, without serving the maintenance API group.
- `disabled`: does neither.

A mode that doesn't serve the group responds to every route under `/v0/maintenance/` with `route_not_found`. A mode that doesn't maintain leaves background work to another process. Long metadata compactions can log progress for an extended period, and you don't need to do anything unless failures repeat.

A writer session folds its WAL tail at the fold thresholds. After each fold it publishes, it compacts its metadata, at most 16 compaction units each time. The server runs at most one fold and one compaction unit of a namespace at a time, whichever part of the server starts them (the session, the scheduler, a maintenance request, a merge, a snapshot, a fork, or a delete). A fold or a unit that starts while another one of the same namespace is running waits for the running one to finish instead of doing the same work again. Commits don't wait for compaction. The session records its last published sequence number and the monotonic time of that publication, and the host keeps the other scheduling facts next to the session's handle.

The scheduler does maintenance in "steps". A step is one bounded unit of maintenance work on one namespace: the metadata step, the collection step (one garbage collection), the grep index step, or the close of an idle session. Every `maintenance_interval_ms` (5000 ms, or 5 seconds, by default), the scheduler runs the due steps of each session that the host has open, including a session that's still committing. For each session, it runs the metadata step, and then the first of the other three steps that's due:

1. **Metadata.** This step folds the WAL tail if a fold is due, and then runs compaction units while a unit is due, at most 16. A fold is due once the tail reaches the fold thresholds (256 WAL objects, or 8 MiB of WAL objects in the tail), or once the tail's newest commit is `idle_fold_after_ms` old (15 minutes by default, described below). The step runs for a session that has a writer position. A session's "writer position" is its in-memory record of the namespace's manifest and the unfolded tail above it. A session has a position from its first publish until a fork of the namespace's head, another writer's takeover, a change that the session can't follow, or a check that finds the position's manifest at or below the namespace's manifest boundary drops the position. The position's age doesn't drop it. The step checks the position to see whether work is due, so a step that finds nothing due costs zero store requests, and the scheduler runs the step again at the next interval. If the step leaves work unfinished, it runs again `metadata_retry_interval_ms` later (300000 ms, or 5 minutes, by default), but if the tail reaches the idle fold age before then, the next run of the step happens without waiting. If the step fails, the retry delay is the later of that interval and the failure backoff described below, and reaching the idle fold age doesn't shorten it. A session fenced by another compactor costs 7 requests once per retry interval, instead of at every maintenance interval. So an idle session with a position gets the step at every retry time at no store cost until work is due, and the step folds the idle tail from the position at the idle fold age. The scheduler skips the metadata step of a session without a position (one that hasn't published since it opened, or whose position was dropped), and the sweep maintains that namespace's metadata, except that when such a session's tail reaches the idle fold age, the scheduler runs the step once and folds the tail.
2. **Collection.** This step collects the namespace's garbage when the published sequence number has moved since the last successful collection and `gc_interval_ms` (3600000 ms, or 1 hour, by default) has passed. The first collection waits that interval after the host opens the session's entry. Collection comes before the grep index step, so a session with a commit at every maintenance interval still collects once per `gc_interval_ms`. The step collects the namespace and nothing else. The content of the namespace's pool is collected by sweeps (described below).
3. **Grep index.** When the index is maintained, and it's dirty or behind the published sequence number, this step runs up to 16 grep steps. Each one is a reorganization step when a level of the index is full or a reorganization is in progress, and a build step otherwise. Unfinished work stays dirty for the next interval. Enabling or disabling the index over HTTP makes the host keep a session for the namespace and marks the session's entry dirty, so a build continues even without a local commit.
4. **Close.** This step closes the session when it has been idle longer than `idle_session_close_after_ms` (1800000 ms, or 30 minutes, by default), the host's handle is the only reference to the session, and the session's metadata needs no more steps: the metadata step in the same run found it caught up, or the session has no position. A session that can't write anymore (its caller closed it, another writer fenced it, or its namespace was deleted) closes as soon as the host's handle is the only reference to it, without waiting for the idle age. The host checks the last opened time, the published sequence number, and handle ownership under the table lock. Opening the closing namespace waits for the drain and returns a fresh session, but opens of other namespaces don't wait for this close. The fresh session's first publish acquires a new writer epoch. Closing costs zero store requests.

Each session that the host keeps has its own publisher, engine state, head position, and allocated queue capacities. Closing the session releases them after its admitted work ends. Shared caches and execution limits have their own lifetimes. A server whose maintenance mode doesn't maintain has no scheduler, so nothing closes its idle sessions.

A "sweep" is the scheduler listing every namespace in the store and running each listed namespace's due steps. A sweep runs when the server starts and then every `sweep_interval_ms` (86400000 ms, or 24 hours, by default). It runs metadata and index maintenance, advances retention floors under each namespace's retention window (as described below), and collects garbage. Then it collects every pool in the store (described below). The sweep skips the metadata step for a namespace whose session has a writer position, because the session steps maintain that namespace's metadata. It covers namespaces that this host doesn't write to, forks, deleted and retired namespaces, work left by a crashed process, and garbage that was still inside a grace window. If the host loses its in-memory scheduling facts, no durable progress is lost.

Sweeps and session steps run at the same time. They share the `max_concurrent_maintenance` slots (8 by default), and the scheduler keeps a claim on each namespace whose steps are running, so the steps of one namespace never overlap. A long compaction uses one slot while the other free slots continue working. A sweep lists up to 1000 namespace IDs per page, and it requests another page when its slots allow it. Hosts can call `Scheduler::run_sweep` and `Scheduler::run_due_steps` directly.

All the interval and idle-close settings must be positive.

A namespace's [retention window](../../../docs/specs/glossary.md#retention-window) is how long, in milliseconds, the server keeps the namespace's history before it advances the namespace's [retention floor](../../../docs/specs/glossary.md#retention-floor) past that history. The namespace's manifest stores the window. The window is set when the namespace is created, with `retention_window_ms` in the create request or `loonfs namespace create --retention-window-ms`, and a `retention_window` maintenance job or `loonfs maintenance retention window` changes or clears the window later. The server's top-level `retention_window_ms` setting is a default window. When a create request doesn't include a window, the server records the setting as the new namespace's window. (A fork copies its source's window and doesn't use the setting.) When it's set, the setting must be at least the [grace window](../../../docs/specs/glossary.md#grace-window) of 86400000 ms (24 hours), and a server whose config sets a shorter window fails at startup. Changing the setting doesn't change the window of a namespace that already exists. For example, if you change the setting from 30 days to 7 days, a namespace created without a window before the change keeps its 30-day window. A namespace without a window (the default) keeps its whole history until an operator advances the floor with a `retention` maintenance job or `loonfs maintenance retention advance`.

```toml
# Keep 30 days of history in each namespace created without a window.
retention_window_ms = 2592000000
```

Each sweep advances the floor of every namespace that isn't deleted to the last commit at or before the current time minus the namespace's window. The sweep doesn't advance the floor of a namespace without a window. The sweep uses the same advance as a `retention` run, so the cutoff rule in [Advancing the retention floor](../../../docs/specs/format.md#101-advancing-the-retention-floor) applies. The session steps don't advance the floor on their own schedule, and a server whose `maintenance` mode doesn't run the scheduler doesn't advance any floor. If a namespace's session steps are running when the sweep reaches the namespace, the sweep doesn't wait for them, and the session steps make the sweep's advance after they finish. A failed advance counts as a `retention` failure in `loonfs.maintenance.sweep_failures`, and the next sweep tries it again. The session steps don't retry a failed advance, so the server keeps no retry state for it, even for a session that the host keeps. The server logs the failure at error level on each sweep that finds it, like a failure on a namespace without a kept session, and the failure doesn't keep an idle session open.

An advance records the new floor in a "floor object", a WAL object with no commits, at the next WAL number (see the [glossary](../../../docs/specs/glossary.md#floor-object), and the [retention window design note](../../../docs/design/retention-window.md#who-advances-the-floor) for why). When this server has a writer session for the namespace, the session creates the floor object as one item of its queue, after the commits that it admitted before the advance. When this server has no session for the namespace, the advance creates the floor object itself and opens no session. The advance then folds the namespace when no manifest records the new floor, so the fold's manifest records the floor. The advance claims no writer epoch, so a writer session for the namespace in another process isn't fenced. That session's next commit collides with the floor object, and the session plans the commit again against the new floor, so the session's callers see no error. A store can have more than one maintaining server. If two servers advance one namespace's floor, each server creates a floor object, and the higher floor applies. A namespace whose floor wouldn't move gets no floor object.

For example, with a 30-day window and the default sweep interval, a deletion stays in the trash for at least 30 days. The first sweep after the deletion is 30 days old moves the floor past the deletion, so the deletion expires at that advance, between 30 and 31 days after the commit that deleted it. The next base compaction and garbage collection then reclaim the deleted bytes.

The cutoff is compared with commit timestamps, which come from writers' clocks. The advance stops at the first commit whose timestamp is later than the cutoff, so one commit with a late timestamp keeps the floor below that commit until the window passes the timestamp.

Why don't the session steps advance the floor at every maintenance interval? An advance checks that the manifest's segments exist, creates a floor object, and folds the namespace. A window measured in days doesn't need a finer schedule than the sweep, and the sweep publishes nothing for a namespace whose floor has already reached its cutoff.

Why does the sweep leave its advance to the session steps that are running on the namespace? Those steps might be running a long compaction, so the sweep can't wait for them. If the sweep skipped the advance, the window would be a day longer for the namespaces that are busiest.

Grep queries stay correct while the index is behind. A query scans the commits after the index, and if it goes past its scan budget, it fails with `index_lagging` unless `allow_stale` is set.

When a call fails on one namespace, the server counts the failure in `loonfs.maintenance.sweep_failures`. The other calls on the namespace and the steps of other namespaces continue. A failed collection of a pool counts under the call name `pool_gc`, the server logs it at error level with a `pool_id` field, and the next sweep tries it again. A pool whose record is gone when its collection runs isn't a failure, because a create call can delete its pool record after the sweep lists the pool. For a session that the host keeps, the session's entry records when each call is tried next, and the scheduler waits until then. The failure backoff is one maintenance interval after the first failure, and each failure in a row doubles it, up to one hour. A failed index or collection call waits for its backoff. A failed metadata call still has its work left, so it waits for the later of its backoff and `metadata_retry_interval_ms`. A success ends the backoff. A call that panics counts as a failure with the code `server_error`, so it waits and is tried again the same way.

For example, with the default 5-second maintenance interval and 5-minute metadata retry interval, a collection that keeps failing is tried again after 5 seconds, 10 seconds, 20 seconds, and so on, and then once an hour. A metadata step that keeps failing is tried again every 5 minutes (its backoff stays shorter than 5 minutes for the first six failures). Once a failing metadata step's backoff reaches one hour, the session closes when it's idle, and the next sweep tries the step again.

A sweep runs every call on every namespace that it lists, without waiting for a session's next try. A namespace that the host doesn't keep a session for has no entry, so the server records nothing about its failures, and the next sweep is its retry.

The server logs the first failure in a row on a kept session at error level and each later one at warning level, and it logs the first success after a failure at info level. A failure on a namespace without a kept session is logged at error level on each sweep that finds it, which is once a day by default. Each failure event has `namespace_id`, `call`, and `code` fields, and on a kept session it also has `attempts` (the failures in a row) and `retry_after_ms` (the wait before the next try). The success event has `namespace_id`, `call`, and `attempts`. Every panic is logged at error level with the failure fields and a `panic` field that holds the panic message. While a collection or index call's backoff is below one hour, an idle held session stays open, so the scheduler keeps trying the call. Once the backoff reaches one hour, the session closes when it's idle, and the next sweep tries the call again. A failing metadata step follows the same rule, so once its backoff reaches one hour, the session closes when it's idle, and the next sweep tries the step again.

Why doesn't the server stop maintaining a corrupt or forbidden namespace? Some failures that look permanent clear without a restart. Expired credentials and clock skew both return a permission error, a rolling deploy fences the old writer for a while, and an operator can repair a namespace in place. The doubling wait keeps a failing namespace from filling the logs or costing many store requests, and the namespace is maintained again as soon as the cause is fixed.

If listing the namespaces fails, the sweep doesn't start the steps of any more namespaces. The namespaces whose steps are already running finish, the sweep ends with a warning, and the next sweep lists the namespaces again.

On shutdown, the server stops the scheduler in these steps:

1. It stops the scheduler at the same moment it stops admitting requests. No new step starts, and a running compaction stops at its next block.
2. After the request drain, it waits for the steps that are still running, up to the same `shutdown_deadline_ms` that limits the drain.
3. At that deadline, it drops the steps that are still running, logs a warning, and shuts the runtime down.

A dropped step leaves the same state that a crash would, and a later sweep does the work again. Like an abandoned request, a dropped step doesn't make the shutdown fail.

An idle namespace costs no requests between sweeps. A sweep costs one list request per page of namespaces, one list request per page of pools, and the same number of requests for each namespace. For an idle namespace (one with nothing to fold, compact, or collect, and with a pool of its own), a sweep costs these requests, including the collection of the namespace's pool:

| Grep not maintained | Grep maintained, index not enabled | Grep index enabled |
| --- | --- | --- |
| 25 GET or HEAD and 13 LIST | 28 GET or HEAD and 14 LIST | 29 GET or HEAD and 14 LIST |

In a sweep, each object that's still inside its collection grace adds a request, and a namespace with work due adds the requests for that work. A deleted namespace stays listed, so it keeps this cost on every sweep until it's retired. A retired namespace stays listed too, because its manifest boundary object stays in the store, and each retired namespace costs every sweep 9 requests when the namespace is its pool's first member: one read of the boundary for each of the metadata call, the retention advance, the namespace's collection, and the pool's collection, and the five listings of the namespace's collection. A retired fork costs 8, because its pool's collection doesn't read it, and the grep index call adds one boundary read when the server maintains grep. That cost grows with the number of retired namespaces in the store. Core metadata still costs 7 requests per namespace, the retention advance adds 7, and the collections of the namespace and its pool add 24, for a total of 38. A pool with forks adds the reads of each fork's roots to its collection. For example, a server with 1,000 caught-up idle namespaces, each in its own pool, and no grep sends no requests for them between sweeps, and about 38,000 on each daily sweep, plus the namespace and pool listings.

A sweep makes the retention advance for every namespace, whether or not `retention_window_ms` is set, because the advance reads the namespace's window from the current manifest. For an idle namespace whose floor has already reached its cutoff, or that has no window, the advance costs 7 requests, and it writes nothing. The advance plans from the namespace's view, so it finds the current manifest once (a read of the manifest boundary, a listing of the manifests above it, the newest manifest, and a conditional read of the boundary), and then checks the WAL tail after that manifest and the manifest itself (a GET of the next WAL number, a HEAD request for the next manifest number, and a read of the boundary). For a namespace with a window and commits above its floor, the advance also reads those commits in order, up to the first one that's newer than the cutoff. When the floor moves, the advance also sends one HEAD request for each segment that the manifest lists, puts the floor object at the next WAL number, and folds the namespace, which puts the fold's manifest (and its segments, when the tail has commits).

Content objects, upload records, and temporary objects are stored under the prefix of a [pool](../../../docs/specs/glossary.md#pool), the set of namespaces (the pool's "members") that share content. A namespace created without a source starts a pool, and a fork joins its source's pool. After a sweep has run the steps of every listed namespace, it lists the pools in the store and runs one collection of each pool, with the same `max_concurrent_maintenance` slots. A pool's collection deletes the content objects that no member of the pool refers to, once they're older than the grace window. A session's collection step doesn't collect pools, so a pool's content is collected once per sweep (once a day by default).

A sweep also collects deleted namespaces. A deleted namespace is retired by the first collection of the namespace after the grace window passes, 24 hours after the delete, if no snapshot is on the namespace. Retirement deletes a fork's fork snapshot on its source and raises the namespace's manifest boundary to retired. Later collections of the namespace delete its manifests, WAL objects, segments, and snapshot records once they're more than 24 hours old, and keep the boundary object, which records that the namespace ID was used. Retirement deletes no content, and a later collection of the pool deletes the content that the namespace wrote once no member refers to that content. The pool's first member (the namespace that was created with the pool) is retired after every fork of it has been retired. After that, the pool has no members, and each sweep's collection of the pool deletes the pool's remaining content under the same age rule. The content of a completed upload session is deleted by a later collection than the one that deletes the session's record. Late writes through presigned upload URLs that were already issued, and dependent forks, can delay retirement, and the later sweeps find them. In a pool's garbage collection report, `deleted.content_objects` counts the content objects that the collection deleted. For what can block retirement, see the API specification's [namespace deletion section](../../../docs/specs/api.md#63-delete-v0namespacesns). A pool's garbage collection result and a `metadata` maintenance result include `skipped`, which lists the work that the job skipped at a limit (see [Jobs and outcomes](../../../docs/specs/api.md#jobs-and-outcomes)).

An explicit `metadata` maintenance job compacts until nothing is due, so it can include a compaction of a large window, which can take minutes. Maintenance jobs are exempt from `request_deadline_ms`. If the response's `outcome` is `due`, the writer session, the next metadata step, or a later job continues the work.

A namespace whose writes stop before its tail reaches the fold thresholds is folded by the first eligible metadata step after its newest commit is `idle_fold_after_ms` old. This period defaults to 900000 ms, which is 15 minutes. If a slot is free, a tail that the host keeps gets its first idle-fold attempt within one maintenance interval after that age, even when the metadata retry interval is longer. If the server restarted, or if another process wrote the tail, the tail isn't folded until the first sweep after the tail reaches that age. A namespace that's written more often than once per period isn't folded this way. Instead, its writer folds it at the thresholds.

Set `idle_fold_after_ms = 0` to turn the rule off. An explicit `metadata` maintenance request uses the same period as the scheduler. An embedded CLI profile doesn't read the server config, and it always uses 15 minutes.

An embedded host sets the period in `RuntimeConfig::idle_fold_after_ms`, where `None` turns the rule off, and every maintenance call on that runtime uses it. Both `maintain_metadata_while_due` forms return a `MetadataMaintenanceOutcome`. `CaughtUp` means that no metadata work remains or can become due without another commit. The call works out the result from the reads it has already made.

## Current limitations

A self-hosted deployment currently has these limitations:

- One process or pod serves the API.
- Restarts and upgrades cause a short outage.
- A second replica doesn't share traffic with the first.
- There's no leader election or automatic failover.
- The Helm chart creates only a Deployment and a ClusterIP Service.
