#!/usr/bin/env bash
# Own only these two disposable containers. Never attach to an existing fleet.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
python_bin="${AUTOSCALE_PYTHON:-$root/.venv/bin/python}"
run_dir="$root/state/manager-demo-$(date +%Y%m%d-%H%M%S)"
for name in autoscale-manager-a autoscale-manager-b; do
  if docker container inspect "$name" >/dev/null 2>&1; then
    echo "Demo container $name already exists; stop that demo before starting another." >&2
    exit 1
  fi
done
mkdir -p "$root/state"
(cd "$root/shim" && go build -o "$root/state/payments-demo" ./cmd/payments-demo)
created=()
cleanup() {
  if (( ${#created[@]} )); then
    for container_id in "${created[@]}"; do docker rm -f "$container_id" >/dev/null; done
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for side in a b; do
  if [[ "$side" == a ]]; then semp=18081; smf=15556; amqp=15672; else semp=18082; smf=15557; amqp=15673; fi
  name="autoscale-manager-$side"
  container_id=$(docker create --name "$name" --shm-size=1g --ulimit core=-1 --ulimit nofile=2448:1048576 \
    -p "127.0.0.1:$semp:8080" -p "127.0.0.1:$smf:55555" -p "127.0.0.1:$amqp:5672" \
    -e username_admin_globalaccesslevel=admin -e username_admin_password=admin \
    solace/solace-pubsub-standard:latest)
  created+=("$container_id")
  docker start "$container_id" >/dev/null
done
PYTHONPATH="$root/scaling-controller${PYTHONPATH:+:$PYTHONPATH}" "$python_bin" "$root/examples/manager-demo/run.py" --binary "$root/state/payments-demo" --output "$run_dir"
echo "Open $run_dir/index.html for the verified, self-contained presentation."
