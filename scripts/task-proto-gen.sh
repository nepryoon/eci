#!/usr/bin/env bash
# task proto:gen — SPEC-002 §2. Genera gli stub Go (buf generate) e Python
# (grpc_tools.protoc) da contracts/proto/eci/retrieval/v1/retrieval.proto.
#
# I tool richiesti (buf, protoc-gen-go, protoc-gen-go-grpc, grpc_tools nel
# venv di tooling .venv-tools/) vanno installati esplicitamente PRIMA di
# eseguire questo task — se mancano lo script fallisce con l'istruzione di
# installazione, non con un errore criptico di buf (SPEC-002 §4).
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_VENV="${REPO_ROOT}/.venv-tools"

if ! command -v buf >/dev/null 2>&1; then
  echo "ERRORE: 'buf' non trovato nel PATH." >&2
  echo "Installa con: go install github.com/bufbuild/buf/cmd/buf@latest" >&2
  echo "Poi assicurati che \$(go env GOPATH)/bin sia nel PATH." >&2
  exit 1
fi

if ! command -v protoc-gen-go >/dev/null 2>&1; then
  echo "ERRORE: 'protoc-gen-go' non trovato nel PATH." >&2
  echo "Installa con: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" >&2
  echo "Poi assicurati che \$(go env GOPATH)/bin sia nel PATH." >&2
  exit 1
fi

if ! command -v protoc-gen-go-grpc >/dev/null 2>&1; then
  echo "ERRORE: 'protoc-gen-go-grpc' non trovato nel PATH." >&2
  echo "Installa con: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest" >&2
  echo "Poi assicurati che \$(go env GOPATH)/bin sia nel PATH." >&2
  exit 1
fi

if [ ! -x "${TOOLS_VENV}/bin/python" ] || ! "${TOOLS_VENV}/bin/python" -c "import grpc_tools.protoc" >/dev/null 2>&1; then
  echo "ERRORE: 'grpc_tools' non installato nel venv di tooling ${TOOLS_VENV}." >&2
  echo "Crea/popola con:" >&2
  echo "  python3 -m venv ${TOOLS_VENV} && ${TOOLS_VENV}/bin/pip install grpcio-tools" >&2
  echo "(fallback se 'python3 -m venv' non è disponibile: python3 -m virtualenv ${TOOLS_VENV})" >&2
  exit 1
fi

echo "buf $(buf --version)"
echo "protoc-gen-go: $(protoc-gen-go --version 2>&1)"
echo "protoc-gen-go-grpc: $(protoc-gen-go-grpc --version 2>&1)"
echo "grpc_tools.protoc: $("${TOOLS_VENV}/bin/python" -m grpc_tools.protoc --version 2>&1)"

echo "== proto:gen (Go, via buf generate) =="
(cd "${REPO_ROOT}/contracts" && buf generate) || exit 1

echo "== proto:gen (Python, via grpc_tools.protoc) =="
mkdir -p "${REPO_ROOT}/libs/py/eci_core/retrieval/v1"
touch \
  "${REPO_ROOT}/libs/py/eci_core/__init__.py" \
  "${REPO_ROOT}/libs/py/eci_core/retrieval/__init__.py" \
  "${REPO_ROOT}/libs/py/eci_core/retrieval/v1/__init__.py"

"${TOOLS_VENV}/bin/python" -m grpc_tools.protoc \
  -I "${REPO_ROOT}/contracts/proto/eci" \
  --python_out="${REPO_ROOT}/libs/py/eci_core" \
  --grpc_python_out="${REPO_ROOT}/libs/py/eci_core" \
  "${REPO_ROOT}/contracts/proto/eci/retrieval/v1/retrieval.proto" || exit 1

# grpc_tools genera un import assoluto basato sul path del proto
# ("from retrieval.v1 import retrieval_pb2"), non sul package Python di
# destinazione (eci_core.retrieval.v1): unico punto che referenzia l'altro
# modulo generato, va riscritto in post-processing.
GRPC_STUB="${REPO_ROOT}/libs/py/eci_core/retrieval/v1/retrieval_pb2_grpc.py"
if [ -f "${GRPC_STUB}" ]; then
  sed -i 's/^from retrieval\.v1 import retrieval_pb2/from eci_core.retrieval.v1 import retrieval_pb2/' "${GRPC_STUB}"
fi

echo "proto:gen completato."
