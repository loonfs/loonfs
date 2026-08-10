# Self-hosting LoonFS

This guide covers running the LoonFS server yourself: what the process is,
how to configure it, and what it tells you while it runs.

## What the server is

The LoonFS server hosts an embedded LoonFS runtime behind the v0 HTTP API.
Remote clients talk to it instead of talking to object storage, so they share
one writer rather than competing for the single-writer role.

Object storage is the durable state. It holds file content, metadata history,
manifests, checkpoints, and the small set of mutable control objects. The
server process holds none of that durably. Its memory and its optional local
cache are copies of bytes object storage already has, so you can stop the
process, delete its disk, and start it again on another machine. It rebuilds
what it needs from the object store.

## Topology

Run one active server process.

A second process pointed at the same object store does not corrupt anything.
Writer-epoch fencing settles the conflict. The newer writer takes the epoch,
and the superseded writer's writes are then rejected. That contains the
mistake; it does not fail over. The server has no leader election, no replica
coordination, and no high-availability story. Expect a restart to take the
API offline until the new process is up.

## Configuration

The server reads one TOML file, named by `--config`. The decode is strict: an
unknown key fails the load rather than being ignored. Every field is
validated before the process binds a port.

Check a config without serving:

```bash
loonfs-server --config /etc/loonfs/server.toml --check-config
```

It prints one line naming the bind address and the store kind, then exits.

The check runs the startup work a config file cannot describe by itself. It
decodes and validates the file, it loads the TLS certificate and key, and it
opens the local block cache. It does not bind the configured address, and it
performs no object-store operation, so it says nothing about whether the
store is reachable. `loonfs admin store-probe` answers that.

Two of those steps touch the filesystem, exactly as a start does. A
`local-fs` store creates its root directory. A configured `[local_cache]`
creates its directory, takes the directory lock, allocates the disk tier,
and then releases the lock again. Run the check where the server it is
checking is not already running: the lock belongs to one process, so a check
against a running server's cache directory fails, and so would a second
start.

### The minimal config

`bind` has no default, so every config sets it. Port 9400 is a convention,
not a requirement.

A loopback bind needs `bind`, `writer_id`, `content_token_secret`, and a
`[store]` table. `content_token_secret` must be non-empty on every bind,
including loopback.

A non-loopback bind serves the network, and `0.0.0.0` and `[::]` are
non-loopback because they bind every interface. Such a bind additionally
requires:

- `auth_token`, in the file or in `LOONFS_AUTH_TOKEN`. Setting
  `allow_unauthenticated_remote = true` instead serves every endpoint to the
  network without authentication.
- either a `[tls]` table with `cert_path` and `key_path`, or
  `allow_remote_without_tls = true` when a load balancer, ingress controller,
  or sidecar terminates TLS in front of the process.

Both requirements exist because the wire carries the bearer token and the
presigned object-store URLs the upload routes hand back.

Here is a complete config for a local-fs store on `0.0.0.0:9400`, behind a
proxy that terminates TLS:

```toml
bind = "0.0.0.0:9400"
writer_id = "loonfs-server-1"
allow_remote_without_tls = true

[store]
kind = "local-fs"
root = "/var/lib/loonfs/store"
```

To terminate TLS in the server itself, drop `allow_remote_without_tls` and
name the certificate and the key:

```toml
bind = "0.0.0.0:9400"
writer_id = "loonfs-server-1"

[tls]
cert_path = "/etc/loonfs/tls/server.crt"
key_path = "/etc/loonfs/tls/server.key"

[store]
kind = "local-fs"
root = "/var/lib/loonfs/store"
```

Remote clients then use an `https://` server URL and the same auth token. If
a private CA issued the certificate, a client needs that CA's bundle as well.
The CLI takes it as `--ca-cert-path`.

Supply the two secrets through the environment:

```bash
export LOONFS_AUTH_TOKEN={auth_token}
export LOONFS_CONTENT_TOKEN_SECRET={content_token_secret}
```

A non-blank value in the file always wins over the environment variable. The
environment fills a field only when the file leaves it unset.

The crate's [`config/`](../config) directory holds one worked example per
object store, each documenting its provider credentials and the optional
settings this guide leaves out.

## Running it in a container

Every release publishes the server image:

```text
ghcr.io/loonfs/loonfs-server:vX.Y.Z
```

A release publishes one tag, and that tag names one manifest covering
`linux/amd64` and `linux/arm64`. There is no floating tag, so every
deployment names a version.

The image holds the server binary and the public root certificates. It runs
as uid 10001, and it reads `/etc/loonfs/config.toml` unless you pass another
`--config`. Mount your config at that path:

```bash
docker run --rm \
  --env LOONFS_AUTH_TOKEN={auth_token} \
  --env LOONFS_CONTENT_TOKEN_SECRET={content_token_secret} \
  --volume /etc/loonfs/server.toml:/etc/loonfs/config.toml:ro \
  --volume /var/lib/loonfs/store:/var/lib/loonfs/store \
  --publish 9400:9400 \
  ghcr.io/loonfs/loonfs-server:vX.Y.Z
```

The second mount is the `local-fs` store from the config above, and it has to
be writable by uid 10001, because that is the user the process runs as. A
cloud store needs no mount at all: the config names the bucket and the
credentials arrive through the environment.

The image declares port 9400, which is the port the examples use. The
config's `bind` decides the port the process actually listens on, so a config
that binds another port needs another `--publish`.

The first log line reports that the config loaded and that the process holds
the port:

```json
{"timestamp":"2026-08-07T01:32:23.285509Z","level":"INFO","fields":{"message":"loonfs-server is listening","bind":"0.0.0.0:9400","store":"local-fs"},"target":"loonfs_server::http::serve"}
```

Arguments after the image name replace the default command, so the same image
validates a config without serving:

```bash
docker run --rm \
  --volume /etc/loonfs/server.toml:/etc/loonfs/config.toml:ro \
  ghcr.io/loonfs/loonfs-server:vX.Y.Z --config /etc/loonfs/config.toml --check-config
```

The server is PID 1 and shuts down on SIGTERM, which is what `docker stop`
and a Kubernetes pod deletion send. It stops accepting, drains the requests
in flight, settles its background work, and exits zero. Allow enough time for
that: pass `--timeout 120` to `docker stop`. A process killed before it
settles may lose work it had already accepted. The chart below gives the pod
600 seconds for the same reason, and its README says where that number comes
from.

[`scripts/test-image.sh`](../scripts/test-image.sh) builds the image and runs
this whole path against it, down to the restart. CI runs the same script on
every change, and the release runs it against the image it has just pushed,
so the published image is the tested one.

### Building the image yourself

The crate carries the [`Dockerfile`](../Dockerfile) that the release builds
from. Build it with the repository root as the context, because the build
needs the whole workspace:

```bash
docker build -f crates/loonfs-server/Dockerfile -t loonfs-server:dev .
```

Everything above then works the same against `loonfs-server:dev`.

## Running it on Kubernetes

Every release publishes a Helm chart beside the image:

```text
oci://ghcr.io/loonfs/charts/loonfs-server
```

The chart renders a Deployment and a ClusterIP Service, and it renders
nothing else.

One pod serves the API. The chart fixes the replica count at 1 and sets the
update strategy to `Recreate`, because LoonFS has one writer. An upgrade
stops the old pod before it starts the new one, so the API is offline for
that gap. This is not a high-availability deployment, and a second replica
would not make it one.

The chart version and the server version are the same number, and the chart's
default image is `ghcr.io/loonfs/loonfs-server` at that version. Installing
chart 0.2.0 therefore runs server 0.2.0 without naming an image at all.

Create the namespace:

```bash
kubectl create namespace loonfs
```

Create the Secret holding the config. The chart does not create this Secret:

```bash
kubectl --namespace loonfs create secret generic loonfs-server-config \
  --from-file=config.toml=/etc/loonfs/server.toml
```

The whole Secret mounts read-only at `/etc/loonfs`, so every entry in it
lands at `/etc/loonfs/<entry>`. A service-account JSON, or a TLS certificate
and key, go into that same Secret, and the config names them at those paths.

Keep the two secret values in a Secret of their own, so the config file
carries no credentials:

```bash
kubectl --namespace loonfs create secret generic loonfs-server-secrets \
  --from-literal=LOONFS_AUTH_TOKEN={auth_token} \
  --from-literal=LOONFS_CONTENT_TOKEN_SECRET={content_token_secret}
```

Install the chart, naming the version you want:

```bash
helm install loonfs-server oci://ghcr.io/loonfs/charts/loonfs-server \
  --version X.Y.Z \
  --namespace loonfs \
  --set config.existingSecret=loonfs-server-config \
  --set 'extraEnvFrom[0].secretRef.name=loonfs-server-secrets'
```

Watch the pod come up:

```bash
kubectl --namespace loonfs rollout status deployment/loonfs-server
```

Reach the API from your workstation and check the probe:

```bash
kubectl --namespace loonfs port-forward service/loonfs-server 9400:9400 &
curl http://127.0.0.1:9400/health
```

`service.port` is the port the Service publishes and the port the probes
call, and the config's `bind` decides the port the process listens on. Both
have to name the same number.

The chart's [README](../deploy/helm/loonfs-server/README.md) documents every
value, the local cache's sizing rules, and the shutdown grace period.

### Installing the chart from a checkout

The chart lives at
[`deploy/helm/loonfs-server`](../deploy/helm/loonfs-server), and that path is
a chart reference like any other:

```bash
helm install loonfs-server crates/loonfs-server/deploy/helm/loonfs-server \
  --namespace loonfs \
  --set config.existingSecret=loonfs-server-config \
  --set image.repository=your-registry.example.com/loonfs-server \
  --set image.tag=dev \
  --set 'extraEnvFrom[0].secretRef.name=loonfs-server-secrets'
```

A chart out of a checkout still defaults to the published image, so name the
image you built yourself. The cluster needs that image in a registry it
reaches, or loaded onto its nodes.

## Verification

[`scripts/smoke-test.sh`](../scripts/smoke-test.sh) checks an install from
the outside. It waits for the rollout, calls both probes, probes the object
store, and puts a file into a namespace of its own and reads the same bytes
back:

```bash
export LOONFS_AUTH_TOKEN={auth_token}
crates/loonfs-server/scripts/smoke-test.sh --namespace loonfs
```

It needs `kubectl`, `curl`, and `loonfs`, and it says which one is missing.
`--release` names a Helm release other than `loonfs-server`, and
`--server-url` skips the port-forward when you already have a route to the
API.

The run touches nothing you own: the CLI profile it builds lives in a
temporary directory it removes, so the config file you use yourself is never
read or written, and the namespace it created is deleted before it exits.
Run it whenever you want, including after an upgrade.

## Probes and metrics

Three routes report on the process. Each one answers a narrow question, and
this section says exactly which.

`GET /health` answers `ok` when the process is up. It is unauthenticated and
it checks nothing else. Use it as the liveness probe.

`GET /readiness` answers `ready` while the process is up and still admitting
work. Once shutdown begins and admission closes, it answers 503
`shutting_down`, which is the signal a load balancer needs to drain the
instance. It is unauthenticated. It never touches the object store, so a
wrong bucket name or a dead credential still reports ready. Readiness is not
a store-reachability check.

`GET /metrics` answers the Prometheus text exposition format. It requires the
bearer token, because a deployment's traffic shape is not public. When no
`auth_token` is configured it answers anyone who asks, and on a non-loopback
bind that is itself a sign the deployment is misconfigured.

A scrape therefore carries the token, and a scrape configured without one
collects nothing but 401s. Give Prometheus the token in a file and name that
file in the job:

```yaml
scrape_configs:
  - job_name: loonfs-server
    authorization:
      credentials_file: /etc/prometheus/loonfs-token
    static_configs:
      - targets: ["loonfs-server.loonfs.svc:9400"]
```

`bearer_token_file` is the older spelling of the same field. A collector that
models neither sends the header itself, as
`Authorization: Bearer {auth_token}`.

To prove the object store is reachable, run the probe from a CLI profile
pointed at the same store:

```bash
loonfs admin store-probe
```

That command performs real store operations and reports what came back check
by check. It is what catches a wrong bucket, a wrong region, or an expired
credential.

## Logging

The server logs by default: JSON lines on stdout, one object per event.

`LOONFS_TRACE` selects the mode. Leave it unset for the default, set it to
`json` to say the same thing explicitly, or set it to `off` for no output at
all. Any other value fails the process rather than being guessed at.

`RUST_LOG` sets the filter when logging is on. It takes the standard
`tracing` filter syntax, so `RUST_LOG=loonfs_core=debug` raises one target
and leaves the rest alone. `LOONFS_TRACE=off` silences the output whatever
`RUST_LOG` says.

### Metadata rebuilds that run in the background

Background maintenance rewrites a namespace's metadata into fewer files as it
goes. It does that one group of metadata at a time, and one maintenance step
reads a whole group. A group that has grown past what one step may read is
handled differently, and it logs a few lines worth knowing.

The first is a warning saying that the oldest metadata run in a family group
no longer fits one reorganization step. It names the group, the run, how many
rows and bytes that run holds, and the two per-step budgets it did not fit. It
means the namespace has grown, not that anything is wrong.

The warning may repeat once or twice before the rebuild starts. A group in
this state still has newer metadata files above the frozen ones, and folding
those together is cheaper than rebuilding the whole group, so maintenance does
that first. After a couple of rounds it stops waiting and starts the rebuild,
whether or not more files have arrived meanwhile.

What follows is `a family group outgrew one reorganization step; a streaming
metadata compaction is rebuilding it`. That is a background job the server
starts and does not wait for. It reads every file the group holds,
writes the rebuilt group as it goes, and publishes the result in one step at
the end. A large group can keep it busy for a long time, and it logs
`streaming metadata compaction progress` as it works so you can see it moving.
`streaming metadata compaction rebuilt a family group` is the line that says
it landed.

Nothing needs doing about any of this. While the job runs, ordinary
maintenance carries on: the WAL still flushes, the other groups still fold,
and reads answer exactly the same thing from the first line to the last. One
job runs at a time per namespace, so a second group in the same state waits
its turn and says so. The server also runs at most two of these jobs at once
across every namespace it serves, so a third namespace that needs one waits
for a permit. That wait is not a stall: the group is claimed from the moment
it is planned, so nothing re-plans it and nothing rewrites it underneath, and
the job starts as soon as a permit frees.

A maintenance step reports which of those four states its group is in, so a
step run by hand answers the question too:

| `reorganize.kind` | What it means | What to do |
| --- | --- | --- |
| `compaction_started` | This step started the rebuild. | Nothing. Watch for the progress lines. |
| `compaction_at_capacity` | The rebuild is waiting for one of the server's two compaction permits. | Nothing. It starts when a running job finishes. |
| `compaction_running` | A rebuild is already running for this namespace. | Nothing. This group's turn comes when that one lands. |
| `compaction_required` | The group needs a rebuild and the handle that ran the step has no background work behind it. | Run one explicitly. Nothing else will. |

The last row is the only one that asks anything of you, and this server does
not report it: its admin handle shares the writer's background work, so a step
you run by hand starts the job itself even with `maintenance = "manual"`.
It is for embedded deployments whose admin handle has no writer behind it.
Those call `FsAdmin::compact_metadata`, which runs one rebuild in the caller's
own task and returns when it lands; cancelling it is dropping the future. That
call does not wait for the amortization above: a caller who asks for the
rebuild gets it on that call, because under steady writes there is always
another pair of newer files to fold instead and waiting for them to run out
would postpone the rebuild forever. A step run by hand on such a handle says
`compaction_required` for the same reason — it reports the rebuild promptly
rather than folding newer files that never reach it.

Two metrics say what the limit is doing:
`loonfs.maintenance.compactions_running` and
`loonfs.maintenance.compactions_queued`. A queue that never empties is a
process serving more namespaces than two concurrent rebuilds can keep up
with.

A restart during a rebuild loses that rebuild's work, and this is expected.
Nothing durable records the rebuild's progress: the published metadata never
moved, the files the job had written are referenced by nothing, and the next
maintenance step starts the job again from what the namespace holds by then.
The same is true of a graceful shutdown, which cancels a running job rather
than waiting for it.

Each job writes its files under
`namespaces/{namespace}/metadata/compactions/{job}/tables/` and keeps a small
`lease.json` beside them that it rewrites every few minutes while it runs.
That lease is how a collection pass tells a job that is still writing from one
that died: a job holding its lease keeps its files however old they are, and a
job whose lease has gone stale leaves them to be reclaimed within the hour. The
lease says only who owns the files and when the job last touched them; it
records nothing about the rebuild's progress, so it cannot make a restart
resume anything.

Both sides write that lease with a compare-and-swap, so only one of them can
own the files. A collection pass that finds a stale lease claims it, and from
that moment the job is fenced: its next lease write fails, it logs `streaming
metadata compaction fenced` and publishes nothing, and the pass reclaims what
it wrote. That is what stops a job that was suspended for half an hour from
waking up and publishing files a pass has already deleted. It costs that
rebuild and nothing else; the next step starts it again.

A job that publishes leaves its lease behind rather than deleting it, and the
lease then expires on its own within the hour. That is deliberate. A
collection pass that started before the publication is working from an older
picture in which those files are referenced by nothing, and the lease is what
tells it otherwise; a later pass reads the published result, sees the files
are in use, and removes only the leftover lease.

Two lines mean the job did not land and will be run again: `streaming metadata
compaction abandoned` (something else rewrote the group underneath it) and
`streaming metadata compaction superseded` (writes kept landing during its
final publication). Both are safe and both are self-correcting. `streaming
metadata compaction failed` carries the error and is worth reading; the next
step still tries again, about a minute later.

This build does not expose the per-step budgets in the config, and it does not
expose the two-job limit either, so the lever for everything a step does —
flushing, folding, collecting — remains how often maintenance runs, not how
much each run may do. The rebuild above is the one piece of upkeep that lever
does not pace, because it is not paced at all.

## The optional local cache

The server can keep encoded metadata blocks on this machine's disk, in front
of object storage. Omit the `[local_cache]` table and there is no such cache:
every block the in-memory cache misses is read from the object store.

The table takes three fields:

```toml
[local_cache]
path = "/var/lib/loonfs/cache"
memory_bytes = 67108864
disk_bytes = 107374182400
```

`disk_bytes` has a floor of 100663296 bytes, which is 96 MiB. A value below
it fails the process at startup rather than starting a cache that holds
nothing.

The disk tier claims the whole `disk_bytes` up front, as 16 MiB files under
`path`, and it holds every one of them open. Size the process's open-file
limit above `disk_bytes` divided by 16 MiB. The 100 GiB example above asks
for 6400 files.

The directory is created on start and held under an exclusive lock, so one
server owns one directory. A second server pointed at the same path fails to
start rather than interleaving writes.

Changing `disk_bytes` across a restart is safe either way. Growing it keeps
what the directory already holds. Shrinking it starts the directory empty
rather than leaving unclaimed files behind.

Nothing in the directory is durable. Deleting it while the server is stopped
is always safe, and the next start rebuilds it.

## Upgrades

Flush any long WAL tail before you switch builds.

This build reads how long a namespace's WAL tail is from the segment pointers
its head carries. Heads written by earlier builds carried only the newest 32
of them. A namespace holding more unflushed segments than that answers the
namespace status read with a head-coverage error, by design, until the tail
is folded. Run `loonfs admin flush` against such a namespace with your
current build before you upgrade, or recreate the namespace.

On Kubernetes, name the version you are moving to:

```bash
helm upgrade loonfs-server oci://ghcr.io/loonfs/charts/loonfs-server \
  --version X.Y.Z \
  --namespace loonfs \
  --reuse-values
```

The chart's update strategy is `Recreate`, so the old pod stops before the
new one starts and the API is offline for that gap. It lasts as long as a
process start, and clients see refused connections rather than slow requests.
Nothing durable rides on the gap: the object store holds the state, and the
new pod rebuilds what it needs.

Go back the same way:

```bash
helm rollback loonfs-server --namespace loonfs
```

What is running now:

```bash
helm list --namespace loonfs
kubectl --namespace loonfs get deployment/loonfs-server \
  --output jsonpath='{.spec.template.spec.containers[0].image}'
```

`helm list` reports the chart version and the app version of every release in
the namespace. The jsonpath reports the image the Deployment asks for, which
is a digest when `image.digest` is set and a tag otherwise. To read the digest
the node actually pulled, whatever the Deployment asked for, ask the pod:

```bash
kubectl --namespace loonfs get pod \
  --selector app.kubernetes.io/name=loonfs-server \
  --output jsonpath='{.items[0].status.containerStatuses[0].imageID}'
```

Check the upgrade the way you checked the install, with
[`scripts/smoke-test.sh`](../scripts/smoke-test.sh).

## Security checklist

- The image runs as uid 10001, and the chart asks the kubelet to refuse a
  root user, drop every capability, and forbid privilege escalation.
- A non-loopback bind carries an auth token. `allow_unauthenticated_remote`
  serves every endpoint to the network, and no deployment wants that.
- TLS terminates in the server or in a proxy you declare with
  `allow_remote_without_tls`. The wire carries the bearer token and the
  presigned object-store URLs.
- The config lives in a Secret, and the two secret values reach the process
  from a Secret through the environment rather than sitting in the config
  file.
- The pod mounts no service-account token. The server never calls the
  Kubernetes API.
- The local cache is disposable. It holds copies of bytes the object store
  already has, so deleting it costs nothing but the next few reads.

## Limitations

- One pod serves the API. The chart fixes the replica count at 1.
- An upgrade takes the API offline briefly, because the old pod stops before
  the new one starts.
- There is no horizontal scaling. A second replica would take the writer
  epoch from the first, not share the load with it.
- There is no automatic failover. The Deployment restarts a pod that dies,
  and the API is down until the new pod is up.
- Multi-writer and high availability are not what this chart deploys. Nothing
  here elects a leader or coordinates replicas.
