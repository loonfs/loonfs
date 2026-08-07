#!/usr/bin/env bash
# Proves the server image works: it starts under its own config, answers the
# probes, runs as a normal user, enforces the bearer token, writes durable
# state, shuts down cleanly on SIGTERM, and finds that state again on the
# next start.
#
#   crates/loonfs-server/scripts/test-image.sh
#
# The script builds the image itself. Set LOONFS_IMAGE to test one that is
# already built, which is what CI does with the image its cached build
# produced.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
IMAGE="${LOONFS_IMAGE:-loonfs-server:test-$$}"
BUILT_IMAGE=""
FIRST_CONTAINER="loonfs-server-test-$$-1"
SECOND_CONTAINER="loonfs-server-test-$$-2"
WORK_DIR=""
NAMESPACE="container-check"
# The startup line the server logs once it holds the port. Step 5 asserts
# the container's first log line is this one, which proves both that logging
# is on by default and that the line exists.
STARTUP_MESSAGE="loonfs-server is listening"

# The server wrote the store as the image's own user, so under a runtime
# that maps container ids onto host ids the files there belong to that user
# and this script cannot remove them. A root container removes those, and
# then the directory itself, which this script does own, goes.
remove_work_dir() {
  [[ -n "$WORK_DIR" ]] || return 0
  if ! rm -rf "$WORK_DIR" 2>/dev/null; then
    docker run --rm --user 0:0 --volume "$WORK_DIR:/work" \
      --entrypoint /bin/rm "$IMAGE" -rf /work/store /work/config.toml \
      >/dev/null 2>&1 || true
    rm -rf "$WORK_DIR" 2>/dev/null || true
  fi
}

cleanup() {
  local status=$?
  docker rm --force "$FIRST_CONTAINER" "$SECOND_CONTAINER" >/dev/null 2>&1 || true
  # Before the image goes, because removing the store runs a container from it.
  remove_work_dir
  if [[ -n "$BUILT_IMAGE" ]]; then
    docker image rm --force "$BUILT_IMAGE" >/dev/null 2>&1 || true
  fi
  return "$status"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

pass() {
  echo "ok: $*"
}

# A hex string from the kernel's random source, for the two secrets. They
# live for one run and are never written anywhere but this run's config.
random_hex() {
  od -An -tx1 -N16 /dev/urandom | tr -d ' \n'
}

status_code() {
  curl --silent --output /dev/null --write-out '%{http_code}' "$@" || true
}

# Publishes the container port on a loopback port the kernel picks, then
# reports the address curl should use. Two runs on one machine never collide.
#
# `docker port` may report one mapping per address family, and the first
# line is taken. Trimming it here rather than piping through `head` keeps
# `pipefail` from seeing the broken pipe `head` leaves behind.
container_base_url() {
  local container="$1" mapping
  mapping="$(docker port "$container" 9400/tcp)"
  mapping="${mapping%%$'\n'*}"
  [[ -n "$mapping" ]] || fail "container $container published no port"
  echo "http://127.0.0.1:${mapping##*:}"
}

# The container's first log line, trimmed off the whole log for the same
# reason.
first_log_line() {
  local logs
  logs="$(docker logs "$1" 2>&1)"
  echo "${logs%%$'\n'*}"
}

# Waits for the process to answer its liveness probe. A container that has
# already exited is never going to answer, so that is reported at once
# instead of after the whole budget.
wait_for_health() {
  local container="$1" base="$2" attempt
  for attempt in $(seq 1 60); do
    if [[ "$(status_code "$base/health")" == "200" ]]; then
      return 0
    fi
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || echo false)" != "true" ]]; then
      docker logs "$container" >&2 || true
      fail "container $container exited before it answered /health"
    fi
    sleep 1
  done
  docker logs "$container" >&2 || true
  fail "container $container did not answer /health within 60 seconds"
}

command -v docker >/dev/null 2>&1 || fail "docker is not on PATH"

# 1. Build the image, unless the caller supplied one.
if [[ -z "${LOONFS_IMAGE:-}" ]]; then
  echo "building $IMAGE from $REPO_ROOT"
  docker build --file "$REPO_ROOT/crates/loonfs-server/Dockerfile" --tag "$IMAGE" "$REPO_ROOT"
  BUILT_IMAGE="$IMAGE"
fi
pass "image $IMAGE is available"

# 2. The config and the store directory this run serves.
WORK_DIR="$(mktemp -d)"
mkdir -p "$WORK_DIR/store"
# The image runs as a normal user whose id the host does not share, so the
# mounted store has to be writable by whoever that turns out to be.
chmod 0777 "$WORK_DIR" "$WORK_DIR/store"
AUTH_TOKEN="$(random_hex)"
CONTENT_TOKEN_SECRET="$(random_hex)"
cat >"$WORK_DIR/config.toml" <<'CONFIG'
bind = "0.0.0.0:9400"
writer_id = "loonfs-server-container-test"
# The bind serves every interface, so the config has to say that something
# else terminates TLS. Here that something else is the test itself.
allow_remote_without_tls = true

[store]
kind = "local-fs"
root = "/var/lib/loonfs/store"
CONFIG
chmod 0644 "$WORK_DIR/config.toml"
pass "wrote a config and a store directory under $WORK_DIR"

# 3. Run the first container. The two secrets arrive through the
#    environment, the way a deployment supplies them.
docker run --detach --name "$FIRST_CONTAINER" \
  --env "LOONFS_AUTH_TOKEN=$AUTH_TOKEN" \
  --env "LOONFS_CONTENT_TOKEN_SECRET=$CONTENT_TOKEN_SECRET" \
  --volume "$WORK_DIR/config.toml:/etc/loonfs/config.toml:ro" \
  --volume "$WORK_DIR/store:/var/lib/loonfs/store" \
  --publish "127.0.0.1::9400" \
  "$IMAGE" >/dev/null
BASE_URL="$(container_base_url "$FIRST_CONTAINER")"
pass "started $FIRST_CONTAINER on $BASE_URL"

# 4. The probes.
wait_for_health "$FIRST_CONTAINER" "$BASE_URL"
pass "GET /health answered 200"
READINESS="$(status_code "$BASE_URL/readiness")"
[[ "$READINESS" == "200" ]] || fail "GET /readiness answered $READINESS, expected 200"
pass "GET /readiness answered 200"

# 5. The first log line. Logging is on with no environment variable set, and
#    the line names the bind address and the store kind.
FIRST_LOG_LINE="$(first_log_line "$FIRST_CONTAINER")"
[[ "$FIRST_LOG_LINE" == *"$STARTUP_MESSAGE"* ]] \
  || fail "the first log line does not carry the startup message: $FIRST_LOG_LINE"
[[ "$FIRST_LOG_LINE" == *"0.0.0.0:9400"* ]] \
  || fail "the startup line does not name the bind address: $FIRST_LOG_LINE"
[[ "$FIRST_LOG_LINE" == *"local-fs"* ]] \
  || fail "the startup line does not name the store kind: $FIRST_LOG_LINE"
pass "the first log line is the startup line: $FIRST_LOG_LINE"

# 6. The process is not root.
CONTAINER_UID="$(docker exec "$FIRST_CONTAINER" id -u)"
[[ "$CONTAINER_UID" != "0" ]] || fail "the container runs as root"
pass "the container runs as uid $CONTAINER_UID"

# 7. The bearer token guards the authenticated routes.
METRICS_AUTHORIZED="$(status_code --header "Authorization: Bearer $AUTH_TOKEN" "$BASE_URL/metrics")"
[[ "$METRICS_AUTHORIZED" == "200" ]] \
  || fail "GET /metrics with the token answered $METRICS_AUTHORIZED, expected 200"
METRICS_ANONYMOUS="$(status_code "$BASE_URL/metrics")"
[[ "$METRICS_ANONYMOUS" == "401" ]] \
  || fail "GET /metrics without the token answered $METRICS_ANONYMOUS, expected 401"
pass "GET /metrics answered 200 with the token and 401 without it"

# 8. Durable state: create a namespace and read its status back.
CREATE_STATUS="$(status_code --request POST \
  --header "Authorization: Bearer $AUTH_TOKEN" \
  --header 'Content-Type: application/json' \
  --data "{\"namespace_id\":\"$NAMESPACE\"}" \
  "$BASE_URL/v0/namespaces")"
[[ "$CREATE_STATUS" == "200" ]] \
  || fail "POST /v0/namespaces answered $CREATE_STATUS, expected 200"
STATUS_CODE="$(status_code --header "Authorization: Bearer $AUTH_TOKEN" \
  "$BASE_URL/v0/namespaces/$NAMESPACE")"
[[ "$STATUS_CODE" == "200" ]] \
  || fail "GET /v0/namespaces/$NAMESPACE answered $STATUS_CODE, expected 200"
pass "created namespace $NAMESPACE and read its status back"

# 9. SIGTERM shuts the process down cleanly. The server settles its
#    background work before it returns, and a settle that failed exits
#    non-zero, so the exit code is the assertion.
docker stop --timeout 120 "$FIRST_CONTAINER" >/dev/null
EXIT_CODE="$(docker inspect --format '{{.State.ExitCode}}' "$FIRST_CONTAINER")"
[[ "$EXIT_CODE" == "0" ]] || fail "the container exited $EXIT_CODE after SIGTERM, expected 0"
pass "the container exited 0 after SIGTERM"

# 10. A new process on the same store finds the namespace.
docker run --detach --name "$SECOND_CONTAINER" \
  --env "LOONFS_AUTH_TOKEN=$AUTH_TOKEN" \
  --env "LOONFS_CONTENT_TOKEN_SECRET=$CONTENT_TOKEN_SECRET" \
  --volume "$WORK_DIR/config.toml:/etc/loonfs/config.toml:ro" \
  --volume "$WORK_DIR/store:/var/lib/loonfs/store" \
  --publish "127.0.0.1::9400" \
  "$IMAGE" >/dev/null
SECOND_BASE_URL="$(container_base_url "$SECOND_CONTAINER")"
wait_for_health "$SECOND_CONTAINER" "$SECOND_BASE_URL"
RESTART_STATUS="$(status_code --header "Authorization: Bearer $AUTH_TOKEN" \
  "$SECOND_BASE_URL/v0/namespaces/$NAMESPACE")"
[[ "$RESTART_STATUS" == "200" ]] \
  || fail "after the restart GET /v0/namespaces/$NAMESPACE answered $RESTART_STATUS, expected 200"
pass "namespace $NAMESPACE survived the restart"
docker stop --timeout 120 "$SECOND_CONTAINER" >/dev/null
SECOND_EXIT_CODE="$(docker inspect --format '{{.State.ExitCode}}' "$SECOND_CONTAINER")"
[[ "$SECOND_EXIT_CODE" == "0" ]] \
  || fail "the second container exited $SECOND_EXIT_CODE after SIGTERM, expected 0"
pass "the second container exited 0 after SIGTERM"

# 11. The trap removes the containers, the temp directory, and any image
#     this run built.
echo "PASS: the image serves, authenticates, persists, and shuts down cleanly"
