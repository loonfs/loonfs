#!/usr/bin/env bash
# Checks a LoonFS server install from the outside: the deployment rolls out,
# the probes answer, the object store honours the contract LoonFS depends on,
# and a namespace takes a file and gives the same bytes back.
#
#   export LOONFS_AUTH_TOKEN={auth_token}
#   crates/loonfs-server/scripts/smoke-test.sh --namespace loonfs
#
# The run touches nothing you own. It builds its own CLI profile in a
# temporary directory, creates a namespace named after 8 random hex digits,
# and deletes both before it exits. Run it as often as you like.
set -euo pipefail

K8S_NAMESPACE="loonfs"
RELEASE="loonfs-server"
SERVER_URL=""

# How long the deployment has to report a complete rollout.
ROLLOUT_TIMEOUT="120s"
# How long the API has to answer through a port-forward that was just
# started, in one-second attempts.
PROBE_ATTEMPTS=30

BASE_URL=""
WORK_DIR=""
PORT_FORWARD_PID=""
NAMESPACE=""
NAMESPACE_LIVES="no"
CHECKS=()

usage() {
  cat <<'USAGE'
Usage: smoke-test.sh [--namespace <k8s-namespace>] [--release <helm-release>]
                     [--server-url <url>]

  --namespace <name>    The Kubernetes namespace holding the release.
                        Default: loonfs.
  --release <name>      The Helm release name. Default: loonfs-server.
  --server-url <url>    Reach the API at this URL instead of port-forwarding
                        to the Service. Use it when you already have a route.

LOONFS_AUTH_TOKEN has to be exported. The throwaway CLI profile this script
builds authenticates with it, and the script never reads or writes the config
file you use yourself.
USAGE
}

# Every check lands here as it is decided, so the summary at the end reports
# the run whether it passed or stopped part way.
pass() {
  CHECKS+=("PASS  $1")
  echo "ok: $1"
}

fail() {
  CHECKS+=("FAIL  $1")
  echo "FAIL: $1" >&2
  exit 1
}

summary() {
  echo
  echo "Summary"
  if [[ "${#CHECKS[@]}" -gt 0 ]]; then
    local check
    for check in "${CHECKS[@]}"; do
      echo "  $check"
    done
  fi
  case " ${CHECKS[*]-} " in
    *"FAIL  "*) echo "FAIL: the deployment did not pass every check" ;;
    *) echo "PASS: the deployment serves, probes, and round-trips a file" ;;
  esac
}

cleanup() {
  local status=$?
  # The namespace goes first, because deleting it talks to the API, and on a
  # port-forwarded run that path is the process killed below.
  if [[ "$NAMESPACE_LIVES" == "yes" ]]; then
    loonfs namespace delete "$NAMESPACE" --yes >/dev/null 2>&1 || true
  fi
  if [[ -n "$PORT_FORWARD_PID" ]]; then
    kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
  fi
  if [[ -n "$WORK_DIR" ]]; then
    rm -rf "$WORK_DIR"
  fi
  summary
  return "$status"
}

status_code() {
  curl --silent --output /dev/null --write-out '%{http_code}' "$@" || true
}

# A hex string from the kernel's random source. It names this run's namespace
# and fills this run's payload, so two runs on one deployment never collide.
random_hex() {
  od -An -tx1 -N"$1" /dev/urandom | tr -d ' \n'
}

# The name the chart gives the Deployment and the Service. A release already
# named after the chart is used as it stands, which is the rule
# deploy/helm/loonfs-server/templates/_helpers.tpl applies.
chart_fullname() {
  case "$RELEASE" in
    *loonfs-server*) echo "$RELEASE" ;;
    *) echo "$RELEASE-loonfs-server" ;;
  esac
}

# What the pod has to say about a rollout that did not complete. This is the
# first thing to read when the install is wrong: an image that will not pull,
# a Secret that is not there, a config the server rejects.
report_pod_events() {
  local pod
  pod="$(kubectl --namespace "$K8S_NAMESPACE" get pods \
    --selector "app.kubernetes.io/name=loonfs-server,app.kubernetes.io/instance=$RELEASE" \
    --output 'jsonpath={.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$pod" ]]; then
    echo "no pod matches release $RELEASE in namespace $K8S_NAMESPACE" >&2
    return 0
  fi
  echo "pod $pod:" >&2
  kubectl --namespace "$K8S_NAMESPACE" get "pod/$pod" >&2 || true
  echo "recent events for $pod:" >&2
  kubectl --namespace "$K8S_NAMESPACE" get events \
    --field-selector "involvedObject.name=$pod" \
    --sort-by=.lastTimestamp >&2 || true
}


while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)
      [[ $# -ge 2 ]] || fail "--namespace needs a value"
      K8S_NAMESPACE="$2"
      shift 2
      ;;
    --release)
      [[ $# -ge 2 ]] || fail "--release needs a value"
      RELEASE="$2"
      shift 2
      ;;
    --server-url)
      [[ $# -ge 2 ]] || fail "--server-url needs a value"
      SERVER_URL="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      fail "unknown argument: $1"
      ;;
  esac
done

trap cleanup EXIT

# 1. What this script needs before it starts.
for tool in kubectl curl loonfs; do
  command -v "$tool" >/dev/null 2>&1 \
    || fail "$tool is not on PATH, and this script needs it"
done
[[ -n "${LOONFS_AUTH_TOKEN:-}" ]] \
  || fail "export LOONFS_AUTH_TOKEN with the deployment's auth token first"
pass "kubectl, curl, and loonfs are on PATH and LOONFS_AUTH_TOKEN is set"

FULLNAME="$(chart_fullname)"
WORK_DIR="$(mktemp -d)"

# 2. The deployment rolls out.
if ! kubectl --namespace "$K8S_NAMESPACE" rollout status \
  "deployment/$FULLNAME" --timeout="$ROLLOUT_TIMEOUT"; then
  report_pod_events
  fail "deployment/$FULLNAME did not roll out within $ROLLOUT_TIMEOUT"
fi
pass "deployment/$FULLNAME rolled out"

# 3. A route to the API. The operator's own route is used as given; without
#    one this forwards the Service to a loopback port the kernel picks, so
#    two runs on one workstation never collide.
if [[ -n "$SERVER_URL" ]]; then
  BASE_URL="${SERVER_URL%/}"
  pass "reaching the API at $BASE_URL"
else
  SERVICE_PORT="$(kubectl --namespace "$K8S_NAMESPACE" get "service/$FULLNAME" \
    --output 'jsonpath={.spec.ports[0].port}')"
  [[ -n "$SERVICE_PORT" ]] || fail "service/$FULLNAME publishes no port"
  kubectl --namespace "$K8S_NAMESPACE" port-forward \
    "service/$FULLNAME" ":$SERVICE_PORT" >"$WORK_DIR/port-forward.log" 2>&1 &
  PORT_FORWARD_PID=$!
  for _ in $(seq 1 "$PROBE_ATTEMPTS"); do
    forwarding="$(grep -m1 '^Forwarding from 127\.0\.0\.1:' \
      "$WORK_DIR/port-forward.log" || true)"
    if [[ -n "$forwarding" ]]; then
      local_port="${forwarding##*:}"
      BASE_URL="http://127.0.0.1:${local_port%% *}"
      break
    fi
    if ! kill -0 "$PORT_FORWARD_PID" 2>/dev/null; then
      cat "$WORK_DIR/port-forward.log" >&2
      fail "the port-forward to service/$FULLNAME exited"
    fi
    sleep 1
  done
  [[ -n "$BASE_URL" ]] || fail "the port-forward to service/$FULLNAME never opened"
  pass "port-forwarded service/$FULLNAME to $BASE_URL"
fi

# 4. The probes. /health says the process is up, and /readiness says it is
#    still admitting work.
HEALTH=""
for _ in $(seq 1 "$PROBE_ATTEMPTS"); do
  HEALTH="$(status_code "$BASE_URL/health")"
  [[ "$HEALTH" == "200" ]] && break
  sleep 1
done
[[ "$HEALTH" == "200" ]] || fail "GET /health answered $HEALTH, expected 200"
pass "GET /health answered 200"

READINESS="$(status_code "$BASE_URL/readiness")"
[[ "$READINESS" == "200" ]] || fail "GET /readiness answered $READINESS, expected 200"
pass "GET /readiness answered 200"

# 5. A CLI profile for this run alone. LOONFS_CONFIG names a file under the
#    temporary directory, so the config file the operator uses is never read
#    and never written. The token reaches the profile through the
#    environment, and this run's copy of it goes with the directory.
export LOONFS_CONFIG="$WORK_DIR/loonfs-config.toml"
loonfs --no-input profile create remote smoke --server-url "$BASE_URL" >/dev/null \
  || fail "loonfs profile create remote could not build a profile for $BASE_URL"
pass "built a throwaway CLI profile for $BASE_URL"

# 6. The object store. Readiness never touches it, so this is the check that
#    catches a wrong bucket, a wrong region, or an expired credential.
loonfs admin store probe \
  || fail "loonfs admin store probe found an object-store problem"
pass "loonfs admin store probe passed every check"

# 7. A file through a namespace of this run's own, and the same bytes back.
NAMESPACE="smoke-$(random_hex 4)"
loonfs namespace create "$NAMESPACE" >/dev/null \
  || fail "could not create namespace $NAMESPACE"
NAMESPACE_LIVES="yes"
pass "created namespace $NAMESPACE"

printf 'loonfs smoke test %s\n' "$(random_hex 16)" >"$WORK_DIR/payload"
loonfs put "$WORK_DIR/payload" /smoke.txt --namespace "$NAMESPACE" >/dev/null \
  || fail "could not put a file into namespace $NAMESPACE"
loonfs cat /smoke.txt --namespace "$NAMESPACE" >"$WORK_DIR/roundtrip" \
  || fail "could not read /smoke.txt back from namespace $NAMESPACE"
cmp --silent "$WORK_DIR/payload" "$WORK_DIR/roundtrip" \
  || fail "/smoke.txt came back with different bytes than it went in with"
pass "put /smoke.txt and read the same bytes back"

loonfs namespace delete "$NAMESPACE" --yes >/dev/null \
  || fail "could not delete namespace $NAMESPACE"
NAMESPACE_LIVES="no"
pass "deleted namespace $NAMESPACE"

# 8. The trap prints the summary, kills the port-forward, and removes the
#    temporary directory.
