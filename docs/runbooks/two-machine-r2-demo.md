# Two-Host R2 Lease-Handoff Demo

This runbook proves the current CLI-first product shape against two hosts, each running its own
`loond` process and `loon` CLI, with both servers pointed at the same Cloudflare R2 bucket and
the same `key_prefix`.

This runbook is part of the current acceptance gate. Run it after the direct R2 conformance check
and the managed single-host R2 smoke flow.

## Prerequisites

- one Cloudflare R2 bucket
- two reachable hosts with the repository checked out and Rust installed
- one shared `key_prefix`
- one shared bearer token
- enough time to let one lease window expire between the first blocked write and the retry

## Configs

Both hosts must share these values:

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
- client `server_url`

Create ignored local config files like this:

1. On host A, copy
   [`configs/loond.cloudflare-r2.example.toml`](/Users/conormccarter/Code/loondb/configs/loond.cloudflare-r2.example.toml)
   to `configs/loond.cloudflare-r2.host-a.local.toml` and fill in the shared R2 values. Set a
   unique `bind` and `writer_id`, for example `127.0.0.1:9400` and `loond-host-a`.
2. On host B, copy the same example to `configs/loond.cloudflare-r2.host-b.local.toml` and use
   the same shared R2 values but a different `bind` and `writer_id`, for example
   `127.0.0.1:9400` and `loond-host-b`.
3. On host A, copy
   [`configs/loon-client.r2.example.toml`](/Users/conormccarter/Code/loondb/configs/loon-client.r2.example.toml)
   to `configs/loon-client.r2.host-a.local.toml`, point `server_url` at host A’s `loond`, and
   set the shared `auth_token`.
4. On host B, create `configs/loon-client.r2.host-b.local.toml` the same way, but point
   `server_url` at host B’s `loond`.

Checked-in `*.example.toml` files stay sanitized. The `*.local.toml` files above are ignored by
Git.

## Start Both Servers

On host A:

```bash
cargo run -p loon-server --bin loond -- --config ./configs/loond.cloudflare-r2.host-a.local.toml
```

On host B:

```bash
cargo run -p loon-server --bin loond -- --config ./configs/loond.cloudflare-r2.host-b.local.toml
```

Wait for both servers to answer `GET /healthz`.

## Host A: Create Namespace And Write

Create the namespace and upload a file through host A’s server:

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.r2.host-a.local.toml \
  namespace create demo

cargo run -p loon-cli -- --config ./configs/loon-client.r2.host-a.local.toml \
  file put ./README.md demo:/docs/README.md
```

## Host B: Prove Initial Lease Conflict

Immediately try to write through host B’s server:

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.r2.host-b.local.toml \
  file mv demo:/docs/README.md demo:/docs/README-host-b.md --json
```

The command should fail with a JSON API error whose `code` is `lease_conflict`.

## Host B: Retry After Lease Expiry

Wait for the configured lease window to elapse, then retry the move through host B’s server:

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.r2.host-b.local.toml \
  file mv demo:/docs/README.md demo:/docs/README-host-b.md --json
```

The retry should succeed.

## Host A: Read Host B’s Change

Use host A’s CLI against host A’s server to verify the change that host B published:

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.r2.host-a.local.toml \
  file ls demo:/docs --json

cargo run -p loon-cli -- --config ./configs/loon-client.r2.host-a.local.toml \
  file stat demo:/docs/README-host-b.md --json

cargo run -p loon-cli -- --config ./configs/loon-client.r2.host-a.local.toml \
  file get demo:/docs/README-host-b.md ./tmp-readme.md --json
```

Verify that `./tmp-readme.md` matches the expected bytes.

## Host A: Remove The File

Delete the file through host A’s server:

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.r2.host-a.local.toml \
  file rm demo:/docs/README-host-b.md --json
```

## Host B: Verify Deletion

Confirm from host B that the file is gone:

```bash
cargo run -p loon-cli -- --config ./configs/loon-client.r2.host-b.local.toml \
  file ls demo:/docs --json

cargo run -p loon-cli -- --config ./configs/loon-client.r2.host-b.local.toml \
  file stat demo:/docs/README-host-b.md --json
```

The final `file stat` should fail with `path_not_found`.
