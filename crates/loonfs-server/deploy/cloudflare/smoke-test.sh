#!/usr/bin/env bash
set -euo pipefail

: "${LOONFS_SERVER_URL:?set LOONFS_SERVER_URL to the deployed Worker URL}"
: "${LOONFS_AUTH_TOKEN:?set LOONFS_AUTH_TOKEN to the deployment token}"
command -v curl >/dev/null
command -v loonfs >/dev/null

server_url="${LOONFS_SERVER_URL%/}"
work_dir="$(mktemp -d)"
namespace="smoke-$$-$RANDOM"
namespace_created=false

cleanup() {
  if [[ "$namespace_created" == true ]]; then
    loonfs namespace delete "$namespace" --yes >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

attempt=0
until curl --fail --silent --output /dev/null "$server_url/health"; do
  attempt=$((attempt + 1))
  if ((attempt == 180)); then
    echo "server did not become healthy" >&2
    exit 1
  fi
  sleep 1
done

curl --fail --silent --output /dev/null "$server_url/readiness"
export LOONFS_CONFIG="$work_dir/loonfs-config.toml"
loonfs --no-input profile create remote smoke --server-url "$server_url" >/dev/null
loonfs maintenance store probe >/dev/null

loonfs namespace create "$namespace" >/dev/null
namespace_created=true
printf 'Cloudflare smoke test\n' >"$work_dir/input"
loonfs put "$work_dir/input" /smoke.txt --namespace "$namespace" >/dev/null
loonfs cat /smoke.txt --namespace "$namespace" >"$work_dir/output"
cmp --silent "$work_dir/input" "$work_dir/output"
loonfs namespace delete "$namespace" --yes >/dev/null
namespace_created=false

echo "Cloudflare smoke test passed"
