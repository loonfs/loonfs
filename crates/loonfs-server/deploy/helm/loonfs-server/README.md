# loonfs-server chart

This chart runs the LoonFS server on Kubernetes. It renders a Deployment and a ClusterIP Service, and nothing else. To run the server with Docker instead, see [Running it in a container](../../../docs/self-hosting.md#running-it-in-a-container).

Every release publishes the chart, at the same version as the server it runs:

```bash
helm install loonfs-server oci://ghcr.io/loonfs/charts/loonfs-server \
  --version X.Y.Z \
  --namespace loonfs \
  --set config.existingSecret=loonfs-server-config
```

The chart's path in this repository also works as a chart reference. Use it to install a change that no release has published yet:

```bash
helm install loonfs-server crates/loonfs-server/deploy/helm/loonfs-server \
  --namespace loonfs \
  --set config.existingSecret=loonfs-server-config
```

[Self-hosting LoonFS](../../../docs/self-hosting.md) covers the server itself: the config fields, the probes, logging, and the upgrade rule. [Actor attribution](../../../docs/actor-attribution.md) covers the trusted-backend contract for applications that submit changes on behalf of their users. This README covers the chart.

## Topology

The chart runs one pod, and that pod serves the API.

The replica count is set in the template instead of being exposed as a value, and the update strategy is `Recreate`. The old pod stops before the new one starts, so the API is offline between the two.

Why only one replica? LoonFS has one writer, so a second replica isn't high availability. The newer process takes over as the writer with a higher [writer epoch](../../../../../docs/specs/glossary.md#writer-epoch), and LoonFS rejects the older process's writes from then on. Raising the replica count doesn't make the deployment more available, which is why the count isn't a value, and why the update strategy is `Recreate`.

## The config Secret

The chart doesn't create a Secret, and it never sees your config. Create a Secret with your config first, and then set `config.existingSecret` to its name:

```bash
kubectl --namespace loonfs create secret generic loonfs-server-config \
  --from-file=config.toml=/path/to/your/config.toml
```

The chart mounts the whole Secret read-only at `/etc/loonfs`, so each entry in it appears as the file `/etc/loonfs/<entry>`. Files that the config refers to go in the same Secret:

```bash
kubectl --namespace loonfs create secret generic loonfs-server-config \
  --from-file=config.toml=/path/to/your/config.toml \
  --from-file=gcs-service-account.json=/path/to/sa.json \
  --from-file=tls.crt=/path/to/tls.crt \
  --from-file=tls.key=/path/to/tls.key
```

The config then refers to them as `/etc/loonfs/gcs-service-account.json`, `/etc/loonfs/tls.crt`, and `/etc/loonfs/tls.key`.

`config.key` is the Secret entry that contains the TOML config, and the container reads its config from `/etc/loonfs/<key>`. The default is `config.toml`.

Changing the Secret doesn't restart the pod, so restart it yourself:

```bash
kubectl --namespace loonfs rollout restart deployment/loonfs-server
```

## The two secret values

You can set the `[auth]` table's `token` and `content_token_secret` in the config file, or pass them through the environment as `LOONFS_AUTH_TOKEN` and `LOONFS_CONTENT_TOKEN_SECRET`. If the file has a non-blank value, the server uses it instead of the environment variable.

The chart has no values for providers or credentials. It passes `extraEnv` and `extraEnvFrom` through unchanged, so you can use them to pass those values to the process from a Secret you already have:

```yaml
extraEnvFrom:
  - secretRef:
      name: loonfs-server-secrets
```

Use the same two lists for provider credentials, `RUST_LOG`, and any other environment variable the process reads.

## Values

| Value | Default | What it does |
| --- | --- | --- |
| `image.repository` | `ghcr.io/loonfs/loonfs-server` | The repository to pull the server image from. |
| `image.tag` | `""` | The tag to run. If it's empty, the chart uses `v` followed by the chart's `appVersion`, which is the tag a release publishes. |
| `image.digest` | `""` | A digest, written as `sha256:...`. If it's set, the chart ignores the tag. |
| `image.pullPolicy` | `IfNotPresent` | The container's `imagePullPolicy`. |
| `imagePullSecrets` | `[]` | Pull secrets for a private registry. Each entry is `{name: ...}`. |
| `config.existingSecret` | `""` | The name of an existing Secret that contains the config. Required. |
| `config.key` | `config.toml` | The entry in that Secret that contains the TOML config. |
| `service.port` | `9400` | The port the Service publishes and the probes call. |
| `service.annotations` | `{}` | Annotations for the Service. |
| `terminationGracePeriodSeconds` | `660` | How long the kubelet waits after SIGTERM before it sends SIGKILL. |
| `extraEnv` | `[]` | Environment variables for the container, as Kubernetes `EnvVar` entries. |
| `extraEnvFrom` | `[]` | Environment sources for the container, as Kubernetes `EnvFromSource` entries. |
| `podAnnotations` | `{}` | Annotations for the pod. |
| `podLabels` | `{}` | Labels for the pod, in addition to the ones the chart sets. |
| `nodeSelector` | `{}` | Passed through to the pod. |
| `tolerations` | `[]` | Passed through to the pod. |
| `affinity` | `{}` | Passed through to the pod. |
| `resources` | `{}` | Requests and limits for the container. |

`service.port` is the port that the Service publishes, that the container declares, and that the probes call. The config's `bind` setting determines the port that the process actually listens on, so the two have to use the same port number.

There's no `replicaCount` value, and the chart doesn't include an ingress, an autoscaler, or a pod disruption budget, because a deployment with one writer doesn't need any of them.

## The strict schema

The chart's `values.schema.json` sets `additionalProperties: false` at every level the chart owns, so a misspelled value name makes `helm lint` and `helm install` fail:

```
Error: values don't meet the specifications of the schema(s) in the following chart(s):
loonfs-server:
- (root): Additional property replicaCount is not allowed
```

The schema covers only the chart's own values. It checks that `config.existingSecret` is set and that `service.port` is a port, and it doesn't describe the TOML config. To check the config itself, use the server's own flag:

```bash
loonfs-server --config /etc/loonfs/config.toml --check-config
```

Validate chart changes with `helm template` or a real install, because `helm lint` doesn't fail on template-level errors.

Why reject unknown values? A value that's quietly ignored is worse than one that's rejected, and the server applies the same rule to its TOML config.

## Shutdown

This section describes how the server shuts down, and why `terminationGracePeriodSeconds` defaults to 660.

When the server shuts down, it stops accepting new requests, drains the requests in flight, and settles its background work. It abandons requests that are still running after `shutdown_deadline_ms` (600 seconds by default), and it drops maintenance sweep visits that are still running at the same deadline. The extra minute in the grace period lets the server finish settling its writers before the kubelet sends SIGKILL.

The kubelet sends SIGKILL when the grace period runs out. A killed process doesn't corrupt anything, because writer-epoch fencing limits the effect of whatever it leaves behind. But the process doesn't get to settle cleanly, and work that the server had already accepted can be lost. Lower this value only if you'd rather have a faster rollout.
