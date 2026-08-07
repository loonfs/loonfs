# loonfs-server chart

This chart runs the LoonFS server on Kubernetes. It renders a Deployment and
a ClusterIP Service, and it renders nothing else.

The chart is not published to a registry yet. Install it from a checkout of
this repository:

```bash
helm install loonfs-server crates/loonfs-server/deploy/helm/loonfs-server \
  --namespace loonfs \
  --set config.existingSecret=loonfs-server-config
```

[`docs/self-hosting.md`](../../../docs/self-hosting.md) covers the server
itself: the config fields, the probes, the logging, and the upgrade rule.
This file covers the chart.

## Topology

One pod serves the API.

LoonFS has one writer. A second replica is not high availability: the newer
process takes the writer epoch, and the superseded process's writes are
rejected from then on. The replica count is written into the template rather
than exposed as a value, because raising it does not make the deployment more
available.

The update strategy is `Recreate` for the same reason. The old pod stops
before the new one starts, and the API is offline for that gap.

## The config Secret

The chart does not create a Secret and never sees your config. Create one
first, then name it in `config.existingSecret`:

```bash
kubectl --namespace loonfs create secret generic loonfs-server-config \
  --from-file=config.toml=/path/to/your/config.toml
```

The whole Secret mounts read-only at `/etc/loonfs`, so every entry in it
lands at `/etc/loonfs/<entry>`. Files the config points at ride along in the
same Secret:

```bash
kubectl --namespace loonfs create secret generic loonfs-server-config \
  --from-file=config.toml=/path/to/your/config.toml \
  --from-file=gcs-service-account.json=/path/to/sa.json \
  --from-file=tls.crt=/path/to/tls.crt \
  --from-file=tls.key=/path/to/tls.key
```

That config names them at `/etc/loonfs/gcs-service-account.json`,
`/etc/loonfs/tls.crt`, and `/etc/loonfs/tls.key`.

`config.key` names the entry holding the TOML, and the container reads
`/etc/loonfs/<key>`. The default is `config.toml`.

Changing the Secret does not restart the pod. Roll it yourself:

```bash
kubectl --namespace loonfs rollout restart deployment/loonfs-server
```

## The two secret values

`auth_token` and `content_token_secret` can sit in the config file, and they
can also arrive through the environment as `LOONFS_AUTH_TOKEN` and
`LOONFS_CONTENT_TOKEN_SECRET`. A non-blank value in the file wins.

The chart models no providers and no credentials. It passes `extraEnv` and
`extraEnvFrom` through as written, which is how those values reach the
process from a Secret you already keep:

```yaml
extraEnvFrom:
  - secretRef:
      name: loonfs-server-secrets
```

The same two lists carry provider credentials, `RUST_LOG`, and anything else
the process reads from the environment.

## The local cache

`localCache.enabled` mounts an `emptyDir` at `/var/cache/loonfs`. The chart
writes nothing into your config, so the config's `[local_cache]` table has to
name that same directory:

```toml
[local_cache]
path = "/var/cache/loonfs"
memory_bytes = 67108864
disk_bytes = 107374182400
```

Three numbers have to line up, and the server enforces the first of them.

`disk_bytes` has a floor of 100663296 bytes, which is 96 MiB. A smaller value
fails the process at startup rather than starting a cache that holds nothing.

The disk tier claims the whole of `disk_bytes` up front, as 16 MiB files, and
it holds every one of them open. `localCache.sizeLimit` has to be at least
`disk_bytes` for that reason. The kubelet evicts a pod whose `emptyDir` grows
past its limit, and a cache sized above the limit reaches it as it fills. The
100 GiB config above needs `sizeLimit: 100Gi` or more.

The container's open-file limit has to be above `disk_bytes` divided by
16 MiB, because every claimed file stays open. The 100 GiB config above asks
for 6400 files.

Changing `disk_bytes` across a restart is safe either way. Growing it keeps
what the directory already holds. Shrinking it starts the directory empty.
Nothing in the cache is durable, so deleting it is always safe, and the pod
deletes it every time it stops.

## Values

| Value | Default | What it does |
| --- | --- | --- |
| `image.repository` | `ghcr.io/loonfs/loonfs-server` | The repository to pull the server image from. |
| `image.tag` | `""` | The tag to run. Empty means the chart's `appVersion`. |
| `image.digest` | `""` | A digest, written as `sha256:...`. Setting it ignores the tag. |
| `image.pullPolicy` | `IfNotPresent` | The container's `imagePullPolicy`. |
| `imagePullSecrets` | `[]` | Pull secrets for a private registry. Each entry is `{name: ...}`. |
| `config.existingSecret` | `""` | The name of an existing Secret holding the config. Required. |
| `config.key` | `config.toml` | The entry in that Secret holding the TOML config. |
| `service.port` | `9400` | The port the Service publishes and the probes call. |
| `service.annotations` | `{}` | Annotations for the Service. |
| `terminationGracePeriodSeconds` | `600` | How long the kubelet waits after SIGTERM before SIGKILL. |
| `localCache.enabled` | `false` | Mount an `emptyDir` at `/var/cache/loonfs`. |
| `localCache.sizeLimit` | `""` | That `emptyDir`'s `sizeLimit`, such as `100Gi`. Empty means no limit. |
| `extraEnv` | `[]` | Environment variables for the container, as Kubernetes `EnvVar` entries. |
| `extraEnvFrom` | `[]` | Environment sources for the container, as Kubernetes `EnvFromSource` entries. |
| `podAnnotations` | `{}` | Annotations for the pod. |
| `podLabels` | `{}` | Labels for the pod, beside the ones the chart sets. |
| `nodeSelector` | `{}` | Passed through to the pod. |
| `tolerations` | `[]` | Passed through to the pod. |
| `affinity` | `{}` | Passed through to the pod. |
| `resources` | `{}` | Requests and limits for the container. |

`service.port` is the port the Service publishes, the port the container
declares, and the port the probes call. The config's `bind` decides the port
the process actually listens on, so both have to name the same number.

There is no `replicaCount`, and the chart offers no ingress, no autoscaler,
and no pod disruption budget. One writer needs none of them.

## The strict schema

`values.schema.json` sets `additionalProperties: false` at every level the
chart owns, so a misspelled value name fails `helm lint` and `helm install`:

```
Error: values don't meet the specifications of the schema(s) in the following chart(s):
loonfs-server:
- (root): Additional property replicaCount is not allowed
```

A value that is quietly ignored is worse than one that is rejected, and this
is the rule the server already applies to its TOML config. The schema stops
at the chart's own values: it checks that `config.existingSecret` is set and
that `service.port` is a port, and it does not model the TOML. Check the
config itself with the server's own flag:

```bash
loonfs-server --config /etc/loonfs/config.toml --check-config
```

## Shutdown

`terminationGracePeriodSeconds` defaults to 600.

The server's shutdown stops accepting, drains the requests in flight, and
settles its background work. It holds no timeout of its own, and one degraded
object-store operation is bounded at roughly six minutes. 600 seconds clears
that case.

The kubelet sends SIGKILL when the grace period runs out. Writer-epoch
fencing contains what that leaves behind, so a killed process corrupts
nothing. It does discard a clean settle, and work the server had already
accepted can be lost with it. Lower this value only if you would rather have
the faster rollout.
