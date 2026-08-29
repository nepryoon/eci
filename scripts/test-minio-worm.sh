#!/usr/bin/env bash
# T6.5: prova reale S3 Object Lock COMPLIANCE su MinIO pinned.
set -euo pipefail

container_name="eci-minio-worm-${RANDOM}-$$"
minio_image="minio/minio:RELEASE.2025-04-22T22-12-26Z"

cleanup() {
  docker rm -f "${container_name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --rm \
  --name "${container_name}" \
  -e MINIO_ROOT_USER=eci \
  -e MINIO_ROOT_PASSWORD=eci-dev-only-minio \
  -p 127.0.0.1::9000 \
  "${minio_image}" server /data >/dev/null

host_port="$(docker inspect -f '{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' "${container_name}")"
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${host_port}/minio/health/live" >/dev/null; then
    break
  fi
  sleep 1
done
curl -fsS "http://127.0.0.1:${host_port}/minio/health/live" >/dev/null

if [ ! -x services/verification/.venv/bin/python ]; then
  python3 -m venv services/verification/.venv
fi
services/verification/.venv/bin/python -m pip install -q -e libs/py -e 'services/verification[test]'

ECI_RUN_MINIO_INTEGRATION=1 \
ECI_MINIO_ENDPOINT="127.0.0.1:${host_port}" \
services/verification/.venv/bin/python -m pytest \
  services/verification/verification/test_audit_integration.py -q

