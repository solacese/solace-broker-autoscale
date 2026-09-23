#!/usr/bin/env bash
# Own exactly four disposable loopback brokers for bounded automatic scale-out qualification.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
python_bin="${AUTOSCALE_PYTHON:-$root/.venv/bin/python}"
run_dir="${AUTOSCALE_SCALEOUT_RUN_DIR:-$root/state/claude-directed-qualification/automatic-scaleout-$(date +%Y%m%d-%H%M%S)}"
image="${SOLACE_IMAGE:-solace/solace-pubsub-standard:latest}"
name_prefix="codex-automatic-scaleout"
created=()

cleanup() {
  if (( ${#created[@]} )); then
    for container_id in "${created[@]}"; do
      docker rm -f "$container_id" >/dev/null 2>&1 || true
    done
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for suffix in a b c d; do
  name="$name_prefix-$suffix"
  if docker container inspect "$name" >/dev/null 2>&1; then
    echo "Qualification container $name already exists; refusing to adopt it." >&2
    exit 1
  fi
done

if docker ps --format '{{.Names}}' | grep -Eq '^codex-native-qual-'; then
  echo "Native qualification brokers are active; wait for their cleanup before scale-out." >&2
  exit 1
fi
if [[ -e "$root/state/claude-directed-qualification/performance-live-running" ]] || \
   docker ps --format '{{.Names}}' | grep -Eq '(^|-)performance($|-)'; then
  echo "A performance qualification is active; wait before scale-out qualification." >&2
  exit 1
fi

mkdir -p "$run_dir"
for index in 0 1 2 3; do
  suffix="$(printf '%b' "\\$(printf '%03o' "$((97 + index))")")"
  name="$name_prefix-$suffix"
  semp="$((28400 + index))"
  smf="$((28500 + index))"
  container_id="$(docker create --name "$name" --shm-size=1g \
    --ulimit core=-1 --ulimit nofile=2448:1048576 \
    -p "127.0.0.1:$semp:8080" -p "127.0.0.1:$smf:55555" \
    -e username_admin_globalaccesslevel=admin \
    -e username_admin_password=admin \
    "$image")"
  created+=("$container_id")
  docker start "$container_id" >/dev/null
done

"$python_bin" "$root/scripts/wait-solace-queues.py" 28400 28401 28402 28403
for semp in 28400 28401 28402 28403; do
  base="http://127.0.0.1:$semp/SEMP/v2/config/msgVpns/default"
  curl -fsS -u admin:admin -X PATCH "$base/clientUsernames/default" \
    -H 'Content-Type: application/json' -d '{"enabled":true,"password":"default"}' >/dev/null
  curl -fsS -u admin:admin -X PATCH "$base" \
    -H 'Content-Type: application/json' \
    -d '{"authenticationBasicEnabled":true,"authenticationBasicType":"internal","maxMsgSpoolUsage":1500}' >/dev/null
done

ports='[{"semp":28400,"smf":28500},{"semp":28401,"smf":28501},{"semp":28402,"smf":28502},{"semp":28403,"smf":28503}]'
(
  cd "$root/scaling-controller"
  SOLACE_TOPOLOGY_PORTS="$ports" \
  AUTOSCALE_SCALEOUT_REPORT="$run_dir/report.json" \
  "$python_bin" -m pytest tests/test_integration_scaleout.py -m integration -q
)

docker image inspect "$image" --format '{{index .RepoDigests 0}}' > "$run_dir/broker-image.txt"
printf 'Raw report: %s\n' "$run_dir/report.json"
