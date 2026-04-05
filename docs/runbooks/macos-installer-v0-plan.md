# macOS arm64 Installer v0 Plan

This document captures the current install and packaging plan for LoonFS so it can be revisited
later without treating it as an accepted spec.

## Summary

- target macOS arm64 only for the first installer release
- use the install command:

```bash
curl https://install.loonfs.com | sh
```

- install both `loon` and `loond` into `~/.loonfs/bin`
- use `~/.config/loonfs/` as the config root, split by binary name:
  - `~/.config/loonfs/loon/` for `loon` CLI-managed state
  - `~/.config/loonfs/loond/` for user-authored `loond` configs
  - `~/.config/loonfs/loond/examples/` for sanitized `loond` config templates
- do not bootstrap a working `loond` config or an active CLI profile during install

## Install Experience

The intended user-facing install path is:

```bash
curl https://install.loonfs.com | sh
```

The installer should:

- verify `Darwin` + `arm64`
- download `loon-darwin-arm64.tar.gz` and `SHA256SUMS` from GitHub Releases
- verify the checksum before extracting
- install `loon` and `loond` to `~/.loonfs/bin` by default
- create:
  - `~/.config/loonfs/`
  - `~/.config/loonfs/loon/`
  - `~/.config/loonfs/loond/`
  - `~/.config/loonfs/loond/examples/`
- install sanitized example `loond` configs into `~/.config/loonfs/loond/examples/`
- print a PATH hint if `~/.loonfs/bin` is not already on `PATH`

The installer should not:

- use `sudo`
- edit shell startup files
- start `loond`
- create a working `loond` config
- create a `loon` profile automatically

## First-Run Instructions

For local mode, the installer should print explicit next steps like:

```bash
export PATH="$HOME/.loonfs/bin:$PATH"

cp ~/.config/loonfs/loond/examples/loond.cloudflare-r2.example.toml \
   ~/.config/loonfs/loond/home.toml

$EDITOR ~/.config/loonfs/loond/home.toml

loon profile add local home \
  --server-config ~/.config/loonfs/loond/home.toml

loon --profile home local up
```

For remote mode, the installer should print:

```bash
loon profile add remote prod \
  --server-url https://your-loond.example.com \
  --auth-token YOUR_TOKEN
```

## Config Ownership

- `loon` owns CLI-managed state under `~/.config/loonfs/loon/`
- `loon` should default to `~/.config/loonfs/loon/config.toml`
- `loon` runtime state should live under `~/.config/loonfs/loon/runtime/`
- `loond` configs remain explicitly user-authored under `~/.config/loonfs/loond/`
- `loond` examples live under `~/.config/loonfs/loond/examples/`

Local mode is a valid product path, but it still requires an explicit `loond` config because that
config owns real operator choices such as bind address, auth token, writer identity, lease
duration, bucket/prefix, and object-store credentials.

## Branding

Use **LoonFS** for product and distribution surfaces:

- install domain: `install.loonfs.com`
- config roots: `~/.config/loonfs/...`
- binary install dir: `~/.loonfs/bin`
- release asset names
- install and release docs

Keep the executable names:

- `loon`
- `loond`

Do not rename Rust crates in this slice.

## Release Artifacts

The first release pipeline should build and publish:

- `loon-darwin-arm64.tar.gz`
- `SHA256SUMS`

The tarball should contain:

- `loon`
- `loond`
- `loond.local-fs.example.toml`
- `loond.aws-s3.example.toml`
- `loond.cloudflare-r2.example.toml`

GitHub Releases is the artifact backend in v0. `install.loonfs.com` only needs to host the shell
installer.

## Installer Script

Add a checked-in shell installer at `scripts/install.sh` intended to be served by
`install.loonfs.com`.

Supported flags for v0:

- `--version <tag>` to install a specific release
- `--bin-dir <path>` to override `~/.loonfs/bin`
- `--help`

The installer should be idempotent for binaries and example templates:

- reinstall replaces binaries
- reinstall preserves user-authored files under `~/.config/loonfs/loond/`
- reinstall does not create `~/.config/loonfs/loon/config.toml`
- reinstall may refresh the files under `~/.config/loonfs/loond/examples/`

## Documentation Changes

When this work is implemented, update:

- `README.md` to make installed binaries the default path
- `docs/runbooks/cli-v0.md` to use installed binaries first
- `docs/runbooks/two-machine-r2-demo.md` to assume installed `loon` and `loond`

`cargo run` should remain documented only as a repository-development workflow.

## Test Plan

Release pipeline checks:

- build `loon` and `loond` for macOS arm64
- verify tarball contents and executable bits
- generate and verify `SHA256SUMS`

Installer checks:

- run the installer against locally staged artifacts in CI
- assert binaries land in `~/.loonfs/bin` or the override dir
- assert examples land in `~/.config/loonfs/loond/examples/`
- assert no working `loond` config is created automatically
- assert no `~/.config/loonfs/loon/config.toml` is created automatically
- assert `loon --version` and `loond --version` succeed

Acceptance checks:

- fresh machine install using `curl https://install.loonfs.com | sh`
- add `~/.loonfs/bin` to `PATH`
- copy one example from `~/.config/loonfs/loond/examples/`
- edit it into `~/.config/loonfs/loond/<name>.toml`
- run `loon profile add local ...`
- run `loon local up`
- confirm the two-host runbook still works with installed binaries

## Explicit Defaults

- target: macOS arm64 only
- install command: `curl https://install.loonfs.com | sh`
- default bin dir: `~/.loonfs/bin`
- config root: `~/.config/loonfs`
- `loon` state root: `~/.config/loonfs/loon/`
- `loond` config root: `~/.config/loonfs/loond/`
- `loond` examples root: `~/.config/loonfs/loond/examples/`
- no config bootstrap during install
- no profile bootstrap during install
