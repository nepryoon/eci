#!/usr/bin/env bash
# Automated Go-server/Python-client SecurityContext + trace interop (SPEC-068).
set -euo pipefail

tmp_dir="$(mktemp -d)"
server_pid=""
cleanup() {
  if [ -n "${server_pid}" ]; then
    kill "${server_pid}" >/dev/null 2>&1 || true
    wait "${server_pid}" >/dev/null 2>&1 || true
  fi
  rm -rf -- "${tmp_dir}"
}
trap cleanup EXIT

(cd tests/interop/spec-012-secctx-py-go && go build -o "${tmp_dir}/interop-server" ./server)
"${tmp_dir}/interop-server" >"${tmp_dir}/server.log" 2>&1 &
server_pid="$!"

python3 - <<'PY'
import socket
import time

deadline = time.monotonic() + 15
while time.monotonic() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", 50052), timeout=0.5):
            break
    except OSError:
        time.sleep(0.1)
else:
    raise SystemExit("interop Go server did not become ready")
PY

libs/py/.venv/bin/python -m eci_core.examples.interop_client >"${tmp_dir}/client.log"
rg -q "risposta ricevuta: node_id='interop-node-1'" "${tmp_dir}/client.log"
rg -q "SecurityContext ricevuto:" "${tmp_dir}/server.log"
rg -q "trace context ricevuto:" "${tmp_dir}/server.log"
echo "interop Go/Python: PASS"
