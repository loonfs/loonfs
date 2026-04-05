# Two-Host R2 Lease-Handoff Demo

This runbook proves the current CLI shape against two hosts, each running its own `loond` and
`loon`, with both servers pointed at the same Cloudflare R2 bucket and the same `key_prefix`.

It validates the current path-oriented client profile only. It does not exercise the lower-level
staged upload / explicit commit / ordered change-feed surface directly, even though that `/v0`
surface now exists underneath `loond`.

Run it after:

1. direct Cloudflare R2 object-store conformance
2. single-host managed smoke through `xtask`

## Prerequisites

- one Cloudflare R2 bucket
- two reachable hosts with `loon` and `loond` installed on `PATH`
- one shared `key_prefix`
- one shared bearer token
- enough time to let one lease window expire between the first blocked write and the retry

## Configs

Both hosts must share these values in their `loond` configs:

- `bucket`
- `account_id`
- `endpoint_url`
- `access_key_id`
- `secret_access_key`
- `key_prefix`
- `auth_token`

Each host must have its own values for:

- `bind`
- `writer_id`
- local CLI profile name

Create local configs like this:

1. On host A, copy
   `~/.config/loonfs/loond/examples/loond.cloudflare-r2.example.toml`
   to `~/.config/loonfs/loond/host-a.toml`, fill in the shared R2 values, and set a unique
   `bind` and `writer_id`.
2. On host B, copy the same example to `~/.config/loonfs/loond/host-b.toml` and use the same
   shared R2 values but a different `bind` and `writer_id`.
3. On host A, create the CLI-managed profile state by running:

```bash
loon profile add local host-a \
  --server-config ~/.config/loonfs/loond/host-a.toml
```

4. On host B, do the same with a distinct profile name and host B’s server config:

```bash
loon profile add local host-b \
  --server-config ~/.config/loonfs/loond/host-b.toml
```

`loon` will create `~/.config/loonfs/loon/config.toml` on first profile write.

## Start Both Servers

On host A:

```bash
loon --profile host-a local up
```

On host B:

```bash
loon --profile host-b local up
```

Wait for both `local up` commands to succeed, then confirm with `local status`.

## Host A: Create Namespace And Write

Create the namespace and upload a file through host A’s local server:

```bash
loon --profile host-a namespace create demo

loon --profile host-a filesystem put demo ./README.md /docs/README.md
```

## Host B: Prove Initial Lease Conflict

Immediately try to write through host B’s local server:

```bash
loon --json --profile host-b \
  filesystem mv demo /docs/README.md /docs/README-host-b.md
```

The command should fail with a JSON API error whose `code` is `lease_conflict`.

## Host B: Retry After Lease Expiry

Wait for the configured lease window to elapse, then retry the move through host B’s local server:

```bash
loon --json --profile host-b \
  filesystem mv demo /docs/README.md /docs/README-host-b.md
```

The retry should succeed.

## Host A: Read Host B’s Change

Use host A’s CLI against host A’s local server to verify the change that host B published:

```bash
loon --json --profile host-a filesystem ls demo /docs

loon --json --profile host-a filesystem stat demo /docs/README-host-b.md

loon --json --profile host-a \
  filesystem get demo /docs/README-host-b.md ./tmp-readme.md
```

Verify that `./tmp-readme.md` matches the expected bytes.

## Host A: Remove The File

Delete the file through host A’s local server:

```bash
loon --json --profile host-a filesystem rm demo /docs/README-host-b.md
```

## Host B: Verify Deletion

Confirm from host B that the file is gone:

```bash
loon --json --profile host-b filesystem ls demo /docs

loon --json --profile host-b filesystem stat demo /docs/README-host-b.md
```

The final `filesystem stat` should fail with `path_not_found`.

## Shutdown

On both hosts:

```bash
loon --profile host-a local down
loon --profile host-b local down
```
