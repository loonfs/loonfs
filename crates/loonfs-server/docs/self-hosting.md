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
A `local-fs` store creates its root directory during the check; the cloud
providers touch no storage.

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

### Diagnosing a read that answers not-found for a path that should exist

Set `LOONFS_TRACE=json` and `RUST_LOG=loonfs_server=info,loonfs_core=debug`,
then repeat the read.

Find the read's span. It is the `loonfs.phase` span whose `phase` is
`walk_path`, and it names the anchor the read answered from. `head_seq` is
the namespace head the read was pinned to. `manifest_id` and
`manifest_head_seq` name the manifest its tables came from; a `manifest_id`
of 0 means the namespace has published no manifest yet, so every row is
still in the WAL. Compare `head_seq` against the sequence the write
returned. A lower `head_seq` means the read was anchored behind the write,
and nothing is missing.

Then read the events inside that span. `visibility walk found no child`
names the lookup that came back empty in its `leg` field.
`binding_unbound` and `binding_superseded` mean the name was deleted or
moved, which is an ordinary answer. `forward_binding`, `reverse_index`,
`parent_inode`, and `child_inode` mean the durable families disagree about
one name, which is a storage problem. `entry visible without content_ref`
means the path resolved to a visible file whose revision named no content.
That event carries the anchor and the inode id, and it is the only thing
that tells the case apart from a path that never existed.

The write path emits `visibility walk found no child` as well, under a
`prepare_commit` or a `build_commit_plan` span, because a commit checks
that a name is free before it binds one. Read only the events under
`walk_path`.

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
