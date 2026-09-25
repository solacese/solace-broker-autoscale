#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
config=${1:-"$root/config.example.yaml"}
inventory=${2:-"$root/inventory.example.yaml"}
cli="$root/.venv/bin/solace-autoscale"
api_pid=''
controller_pid=''

if [ ! -x "$cli" ]; then
  printf 'Missing %s; run %s/install.sh first.\n' "$cli" "$root" >&2
  exit 1
fi
if [ ! -f "$config" ] || [ ! -f "$inventory" ]; then
  printf 'Configuration and inventory files must exist.\n' >&2
  exit 1
fi

cleanup() {
  trap - EXIT INT TERM HUP
  [ -z "$controller_pid" ] || kill "$controller_pid" 2>/dev/null || true
  [ -z "$api_pid" ] || kill "$api_pid" 2>/dev/null || true
  [ -z "$controller_pid" ] || wait "$controller_pid" 2>/dev/null || true
  [ -z "$api_pid" ] || wait "$api_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM HUP

cd "$root"
"$cli" serve --config "$config" --host 127.0.0.1 --port 8099 &
api_pid=$!
for _ in 1 2 3 4 5; do
  kill -0 "$api_pid" 2>/dev/null || break
  if "$root/.venv/bin/python" -c 'import sys, urllib.request; urllib.request.urlopen(sys.argv[1], timeout=1)' \
      'http://127.0.0.1:8099/healthz' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! kill -0 "$api_pid" 2>/dev/null || ! "$root/.venv/bin/python" -c \
    'import sys, urllib.request; urllib.request.urlopen(sys.argv[1], timeout=1)' \
    'http://127.0.0.1:8099/healthz' >/dev/null 2>&1; then
  printf 'Assignment API failed to start.\n' >&2
  exit 1
fi
"$cli" run --config "$config" --inventory "$inventory" &
controller_pid=$!
while kill -0 "$api_pid" 2>/dev/null && kill -0 "$controller_pid" 2>/dev/null; do
  sleep 1
done
printf 'Controller or assignment API exited; stopping both.\n' >&2
exit 1
