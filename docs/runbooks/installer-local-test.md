# Local Installer Test

This runbook exercises the macOS arm64 installer locally before publishing any release artifacts.

It validates three layers:

1. shell syntax
2. installer smoke against locally staged release artifacts
3. installed-binary local-mode smoke

## Prerequisites

- macOS arm64
- `python3`
- `curl`
- standard Unix tools used by the scripts
- repository checked out locally

## 1. Shell Syntax

Run:

```bash
sh -n scripts/install.sh
sh -n scripts/package-release.sh
```

Expected result:

- no output
- exit code `0`

## 2. Installer Smoke Against Staged Artifacts

Run:

```bash
set -euo pipefail

ARTIFACT_DIR=$(mktemp -d)
HOME_DIR=$(mktemp -d)
PORT=18765

chmod +x scripts/package-release.sh scripts/install.sh
./scripts/package-release.sh "$ARTIFACT_DIR"

python3 -m http.server "$PORT" --directory "$ARTIFACT_DIR" >"$ARTIFACT_DIR/http.log" 2>&1 &
SERVER_PID=$!
trap 'kill "$SERVER_PID" >/dev/null 2>&1 || true; rm -rf "$ARTIFACT_DIR" "$HOME_DIR"' EXIT

for _ in $(seq 1 50); do
  if curl -s "http://127.0.0.1:$PORT/SHA256SUMS" >/dev/null; then
    break
  fi
  sleep 0.1
done

curl -s "http://127.0.0.1:$PORT/SHA256SUMS" >/dev/null

HOME="$HOME_DIR" \
LOONFS_RELEASE_BASE_URL="http://127.0.0.1:$PORT" \
sh ./scripts/install.sh

test -x "$HOME_DIR/.loonfs/bin/loon"
test -x "$HOME_DIR/.loonfs/bin/loond"
test -f "$HOME_DIR/.config/loonfs/loond/examples/loond.local-fs.example.toml"
test -f "$HOME_DIR/.config/loonfs/loond/examples/loond.aws-s3.example.toml"
test -f "$HOME_DIR/.config/loonfs/loond/examples/loond.cloudflare-r2.example.toml"
test ! -e "$HOME_DIR/.config/loonfs/loon/config.toml"
test ! -e "$HOME_DIR/.config/loonfs/loond/home.toml"

"$HOME_DIR/.loonfs/bin/loon" --version
"$HOME_DIR/.loonfs/bin/loond" --version
```

Expected result:

- installer completes successfully
- `loon` and `loond` are installed under `"$HOME_DIR/.loonfs/bin/"`
- example `loond` configs are installed under `"$HOME_DIR/.config/loonfs/loond/examples/"`
- no active `loon` profile config is created
- no working `loond` config is created
- both binaries print a version

## 3. Installed-Binary Local-Mode Smoke

Use the same `HOME_DIR` from step 2.

Add the installed binaries to `PATH`:

```bash
export PATH="$HOME_DIR/.loonfs/bin:$PATH"
```

Create a working local-fs `loond` config from the installed example:

```bash
cp "$HOME_DIR/.config/loonfs/loond/examples/loond.local-fs.example.toml" \
   "$HOME_DIR/.config/loonfs/loond/local.toml"
```

Edit `"$HOME_DIR/.config/loonfs/loond/local.toml"` and set `root` to an absolute path, for example:

```toml
root = "/tmp/loonfs-store"
```

Then run:

```bash
loon profile add local local \
  --server-config "$HOME_DIR/.config/loonfs/loond/local.toml"

loon --profile local local up
loon namespace create demo

printf 'hello installer\n' > /tmp/hello-installer.txt

loon filesystem put demo /tmp/hello-installer.txt /docs/hello.txt
loon filesystem ls demo /docs
loon filesystem stat demo /docs/hello.txt
loon filesystem get demo /docs/hello.txt /tmp/hello-downloaded.txt
cat /tmp/hello-downloaded.txt

loon --profile local local down
```

Expected result:

- `local up` succeeds
- `namespace create` succeeds
- `put`, `ls`, `stat`, and `get` succeed
- `/tmp/hello-downloaded.txt` contains `hello installer`
- `local down` succeeds

## Optional Reinstall Check

To verify idempotence and custom bin-dir support:

```bash
HOME="$HOME_DIR" \
LOONFS_RELEASE_BASE_URL="http://127.0.0.1:$PORT" \
sh ./scripts/install.sh

HOME="$HOME_DIR" \
LOONFS_RELEASE_BASE_URL="http://127.0.0.1:$PORT" \
sh ./scripts/install.sh --bin-dir "$HOME_DIR/custom-bin"

test -x "$HOME_DIR/custom-bin/loon"
test -x "$HOME_DIR/custom-bin/loond"
```

Expected result:

- reinstall succeeds
- custom bin dir contains both binaries
- existing config directories are preserved
