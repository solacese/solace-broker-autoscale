#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
python3 -m venv "$root/.venv"
"$root/.venv/bin/python" -m pip install --upgrade pip
"$root/.venv/bin/python" -m pip install --force-reinstall "$root/scaling-controller[compile,service,smf]"
if command -v go >/dev/null 2>&1; then
  (cd "$root/shim" && go mod download)
fi
printf 'Installed. Try: %s/.venv/bin/python %s/examples/customer_library.py\n' "$root" "$root"
