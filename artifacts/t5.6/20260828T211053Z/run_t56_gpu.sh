#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ECI T5.6 — real vLLM golden evaluation on one GPU.
# Recommended target: Runpod Community Cloud, 1x NVIDIA L40 48 GB.
# Public inputs only: no GitHub or Hugging Face token is required.

SCRIPT_VERSION="1.1.0"
MODEL_ID="${MODEL_ID:-Qwen/Qwen3-Coder-30B-A3B-Instruct-FP8}"
MODEL_REVISION="${MODEL_REVISION:-}"
SERVED_MODEL_NAME="${SERVED_MODEL_NAME:-eci-qwen3-coder-30b-a3b-fp8}"
VLLM_VERSION="${VLLM_VERSION:-0.28.0}"
ECI_REPOSITORY="${ECI_REPOSITORY:-https://github.com/nepryoon/eci.git}"
ECI_COMMIT="${ECI_COMMIT:-32cd8e17643358a7a4307c92dfb3a025d59045f4}"
WORKSPACE="${WORKSPACE:-/workspace}"
MAX_MODEL_LEN="${MAX_MODEL_LEN:-4096}"
MAX_NUM_SEQS="${MAX_NUM_SEQS:-1}"
GPU_MEMORY_UTILIZATION="${GPU_MEMORY_UTILIZATION:-0.90}"
VLLM_PORT="${VLLM_PORT:-8000}"
VLLM_STARTUP_TIMEOUT_SECONDS="${VLLM_STARTUP_TIMEOUT_SECONDS:-1800}"
EVAL_WALL_TIMEOUT_SECONDS="${EVAL_WALL_TIMEOUT_SECONDS:-900}"
MIN_WORKSPACE_FREE_GIB="${MIN_WORKSPACE_FREE_GIB:-50}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
STARTED_AT_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
START_EPOCH="$(date +%s)"

ECI_DIR="$WORKSPACE/eci"
VLLM_VENV="$WORKSPACE/venvs/vllm"
ECI_VENV="$ECI_DIR/services/orchestrator/.venv"
HF_HOME="${HF_HOME:-$WORKSPACE/.cache/huggingface}"
UV_CACHE_DIR="${UV_CACHE_DIR:-$WORKSPACE/.cache/uv}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$WORKSPACE/t5.6-evidence}"
EVIDENCE_DIR="$EVIDENCE_ROOT/$RUN_ID"
ARCHIVE="$WORKSPACE/eci-t5.6-$RUN_ID.tar.gz"
ARCHIVE_SHA256="$ARCHIVE.sha256"

export SCRIPT_VERSION MODEL_ID MODEL_REVISION SERVED_MODEL_NAME VLLM_VERSION
export ECI_COMMIT WORKSPACE MAX_MODEL_LEN MAX_NUM_SEQS GPU_MEMORY_UTILIZATION
export VLLM_PORT RUN_ID STARTED_AT_UTC HF_HOME UV_CACHE_DIR
export HF_XET_HIGH_PERFORMANCE="${HF_XET_HIGH_PERFORMANCE:-1}"
export HF_HUB_DISABLE_TELEMETRY="${HF_HUB_DISABLE_TELEMETRY:-1}"
export DO_NOT_TRACK="${DO_NOT_TRACK:-1}"

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

fail() {
  log "ERROR: $*"
  exit 1
}

install_missing_base_tools() {
  local missing=()
  local cmd
  for cmd in curl git jq sha256sum tar find xargs timeout; do
    command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
  done
  if ((${#missing[@]} == 0)); then
    return
  fi
  command -v apt-get >/dev/null 2>&1 \
    || fail "Missing tools: ${missing[*]} and apt-get is unavailable"
  log "Installing missing base tools: ${missing[*]}"
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    curl git jq coreutils tar ca-certificates findutils
}

install_uv() {
  if command -v uv >/dev/null 2>&1; then
    return
  fi
  log "Installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
  command -v uv >/dev/null 2>&1 \
    || fail "uv installation did not expose the executable on PATH"
}

VLLM_PID=""
stop_vllm() {
  if [[ -n "$VLLM_PID" ]] && kill -0 "$VLLM_PID" >/dev/null 2>&1; then
    log "Stopping vLLM process $VLLM_PID"
    kill "$VLLM_PID" >/dev/null 2>&1 || true
    wait "$VLLM_PID" >/dev/null 2>&1 || true
  fi
}
trap stop_vllm EXIT INT TERM

install_missing_base_tools
install_uv
mkdir -p "$WORKSPACE/venvs" "$HF_HOME" "$UV_CACHE_DIR" "$EVIDENCE_DIR"

printf '%s\n' "$RUN_ID" > "$EVIDENCE_DIR/run-id.txt"
printf '%s\n' "$STARTED_AT_UTC" > "$EVIDENCE_DIR/started-at-utc.txt"
printf '%s\n' "$SCRIPT_VERSION" > "$EVIDENCE_DIR/script-version.txt"
cp -- "$0" "$EVIDENCE_DIR/run_t56_gpu.sh"
uv --version > "$EVIDENCE_DIR/uv-version.txt" 2>&1

AVAILABLE_BYTES="$(df -PB1 "$WORKSPACE" | awk 'NR==2 {print $4}')"
MIN_FREE_BYTES="$((MIN_WORKSPACE_FREE_GIB * 1024 * 1024 * 1024))"
if (( AVAILABLE_BYTES < MIN_FREE_BYTES )); then
  fail "At least ${MIN_WORKSPACE_FREE_GIB} GiB must be free under $WORKSPACE"
fi
df -h "$WORKSPACE" > "$EVIDENCE_DIR/workspace-disk.txt"

log "Checking GPU"
command -v nvidia-smi >/dev/null 2>&1 \
  || fail "nvidia-smi is unavailable; use a CUDA-enabled Runpod template"
nvidia-smi >/dev/null || fail "nvidia-smi cannot access a GPU"
GPU_NAME="$(nvidia-smi --query-gpu=name --format=csv,noheader | head -n1 | xargs)"
GPU_MEMORY_MIB="$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits | head -n1 | xargs)"
if (( GPU_MEMORY_MIB < 45000 )); then
  fail "GPU '$GPU_NAME' exposes only ${GPU_MEMORY_MIB} MiB; T5.6 needs a 48 GB-class GPU"
fi
case "$GPU_NAME" in
  *L40*|*6000\ Ada*) ;;
  *H100*|*H200*) log "WARNING: '$GPU_NAME' is valid but economically oversized for this run" ;;
  *A100*) log "WARNING: '$GPU_NAME' is Ampere; prefer L40/RTX 6000 Ada for native FP8 execution" ;;
  *) log "WARNING: GPU is '$GPU_NAME'; retain compute-capability evidence for review" ;;
esac

nvidia-smi > "$EVIDENCE_DIR/nvidia-smi-before.txt"
nvidia-smi --query-gpu=name,uuid,driver_version,memory.total \
  --format=csv,noheader > "$EVIDENCE_DIR/gpu.csv"
printf '%s\n' "$GPU_NAME" > "$EVIDENCE_DIR/gpu-name.txt"
printf '%s\n' "$GPU_MEMORY_MIB" > "$EVIDENCE_DIR/gpu-memory-mib.txt"
uname -a > "$EVIDENCE_DIR/uname.txt"
[[ -f /etc/os-release ]] && cp /etc/os-release "$EVIDENCE_DIR/os-release.txt"
command -v lscpu >/dev/null 2>&1 && lscpu > "$EVIDENCE_DIR/lscpu.txt"
command -v free >/dev/null 2>&1 && free -h > "$EVIDENCE_DIR/memory-before.txt"

log "Preparing ECI repository"
if [[ -d "$ECI_DIR/.git" ]]; then
  [[ -z "$(git -C "$ECI_DIR" status --porcelain)" ]] \
    || fail "Existing ECI worktree is not clean: $ECI_DIR"
  ORIGIN_URL="$(git -C "$ECI_DIR" remote get-url origin)"
  [[ "$ORIGIN_URL" == "$ECI_REPOSITORY" || "$ORIGIN_URL" == "${ECI_REPOSITORY%.git}" ]] \
    || fail "Existing $ECI_DIR has unexpected origin: $ORIGIN_URL"
else
  rm -rf "$ECI_DIR"
  git clone "$ECI_REPOSITORY" "$ECI_DIR"
fi
git -C "$ECI_DIR" fetch --quiet --prune origin
git -C "$ECI_DIR" checkout --detach "$ECI_COMMIT"
[[ -z "$(git -C "$ECI_DIR" status --porcelain)" ]] \
  || fail "ECI worktree is not clean after checkout"
ACTUAL_ECI_COMMIT="$(git -C "$ECI_DIR" rev-parse HEAD)"
[[ "$ACTUAL_ECI_COMMIT" == "$ECI_COMMIT" ]] \
  || fail "Expected ECI $ECI_COMMIT, got $ACTUAL_ECI_COMMIT"
export ACTUAL_ECI_COMMIT GPU_NAME

printf '%s\n' "$ACTUAL_ECI_COMMIT" > "$EVIDENCE_DIR/eci-commit.txt"
printf '%s\n' "$ECI_REPOSITORY" > "$EVIDENCE_DIR/eci-repository.txt"
git -C "$ECI_DIR" status --short --branch > "$EVIDENCE_DIR/eci-git-status.txt"
git -C "$ECI_DIR" show -s --format=fuller HEAD > "$EVIDENCE_DIR/eci-commit-metadata.txt"
cp "$ECI_DIR/tests/golden/queries_v0.json" "$EVIDENCE_DIR/queries_v0.json"
sha256sum "$ECI_DIR/tests/golden/queries_v0.json" \
  > "$EVIDENCE_DIR/golden-dataset.sha256"
sha256sum "$ECI_DIR/services/orchestrator/orchestrator/golden_eval.py" \
  > "$EVIDENCE_DIR/golden-harness.sha256"

log "Creating isolated Python 3.12 environments"
uv python install 3.12 >/dev/null
if [[ ! -x "$VLLM_VENV/bin/python" ]]; then
  uv venv --python 3.12 --seed "$VLLM_VENV"
fi
log "Checking existing vLLM CUDA environment"
if "$VLLM_VENV/bin/python" - <<'PY_VLLM_CHECK' >/dev/null 2>&1
import torch
import vllm
assert vllm.__version__.split("+")[0] == "0.28.0"
assert torch.cuda.is_available()
assert torch.version.cuda is not None
assert torch.version.cuda.startswith("12.9")
PY_VLLM_CHECK
then
  {
    echo "Reusing validated vLLM environment"
    "$VLLM_VENV/bin/python" -c 'import vllm, torch; print("vllm=" + vllm.__version__); print("torch=" + torch.__version__); print("cuda=" + str(torch.version.cuda))'
  } | tee "$EVIDENCE_DIR/vllm-install.log"
else
  "$VLLM_VENV/bin/python" -m pip install \
    "https://github.com/vllm-project/vllm/releases/download/v${VLLM_VERSION}/vllm-${VLLM_VERSION}%2Bcu129-cp38-abi3-manylinux_2_28_x86_64.whl" \
    --extra-index-url https://download.pytorch.org/whl/cu129 \
    2>&1 | tee "$EVIDENCE_DIR/vllm-install.log"
fi

if [[ ! -x "$ECI_VENV/bin/python" ]]; then
  uv venv --python 3.12 --seed "$ECI_VENV"
fi
log "Installing the ECI orchestrator and its test dependencies"
(
  cd "$ECI_DIR/services/orchestrator"
  uv pip install --python "$ECI_VENV/bin/python" \
    -e ../../libs/py -e ".[test]"
) 2>&1 | tee "$EVIDENCE_DIR/eci-install.log"

"$VLLM_VENV/bin/python" -V > "$EVIDENCE_DIR/vllm-python-version.txt" 2>&1
"$VLLM_VENV/bin/python" - <<'PY_VERSION' > "$EVIDENCE_DIR/vllm-version.txt"
import vllm
print(vllm.__version__)
PY_VERSION
"$VLLM_VENV/bin/python" -m pip freeze > "$EVIDENCE_DIR/vllm-freeze.txt"
"$ECI_VENV/bin/python" -V > "$EVIDENCE_DIR/eci-python-version.txt" 2>&1
"$ECI_VENV/bin/python" -m pip freeze > "$EVIDENCE_DIR/eci-freeze.txt"

log "Running the container-free golden-harness tests before model download"
(
  cd "$ECI_DIR/services/orchestrator"
  "$ECI_VENV/bin/python" -m pytest orchestrator/test_golden_eval.py -q
) 2>&1 | tee "$EVIDENCE_DIR/harness-test.log"

if [[ -z "$MODEL_REVISION" ]]; then
  log "Resolving and pinning the current Hugging Face model revision"
  MODEL_REVISION="$("$VLLM_VENV/bin/python" - <<'PY_REVISION'
import os
from huggingface_hub import HfApi

info = HfApi().model_info(os.environ["MODEL_ID"], revision="main")
if not info.sha:
    raise SystemExit("Hugging Face did not return a model revision")
print(info.sha)
PY_REVISION
)"
else
  log "Using caller-supplied Hugging Face model revision $MODEL_REVISION"
fi
export MODEL_REVISION
printf '%s\n' "$MODEL_ID" > "$EVIDENCE_DIR/model-id.txt"
printf '%s\n' "$MODEL_REVISION" > "$EVIDENCE_DIR/model-revision.txt"
if [[ -n "${HF_TOKEN:-}" ]]; then
  printf 'true\n' > "$EVIDENCE_DIR/hf-token-present.txt"
else
  printf 'false\n' > "$EVIDENCE_DIR/hf-token-present.txt"
fi

"$VLLM_VENV/bin/python" - <<'PY_MODEL' > "$EVIDENCE_DIR/model-metadata.json"
import json
import os
from huggingface_hub import HfApi

info = HfApi().model_info(
    os.environ["MODEL_ID"], revision=os.environ["MODEL_REVISION"]
)
sizes = [getattr(item, "size", None) for item in (info.siblings or [])]
payload = {
    "id": info.id,
    "sha": info.sha,
    "author": info.author,
    "library_name": info.library_name,
    "pipeline_tag": info.pipeline_tag,
    "private": info.private,
    "gated": info.gated,
    "last_modified": info.last_modified.isoformat() if info.last_modified else None,
    "declared_file_bytes": sum(size for size in sizes if isinstance(size, int)),
}
print(json.dumps(payload, sort_keys=True, indent=2))
PY_MODEL

"$VLLM_VENV/bin/python" - <<'PY_GPU' > "$EVIDENCE_DIR/torch-gpu.json"
import json
import torch

payload = {
    "torch_version": torch.__version__,
    "torch_cuda_version": torch.version.cuda,
    "cuda_available": torch.cuda.is_available(),
}
if torch.cuda.is_available():
    props = torch.cuda.get_device_properties(0)
    payload.update({
        "device_name": torch.cuda.get_device_name(0),
        "compute_capability": list(torch.cuda.get_device_capability(0)),
        "device_count": torch.cuda.device_count(),
        "total_memory_bytes": props.total_memory,
    })
print(json.dumps(payload, sort_keys=True, indent=2))
if not payload["cuda_available"]:
    raise SystemExit("PyTorch cannot access CUDA")
PY_GPU

cat > "$EVIDENCE_DIR/vllm-command.txt" <<EOF_COMMAND
$VLLM_VENV/bin/vllm serve $MODEL_ID \\
  --revision $MODEL_REVISION \\
  --served-model-name $SERVED_MODEL_NAME \\
  --host 127.0.0.1 \\
  --port $VLLM_PORT \\
  --dtype auto \\
  --tensor-parallel-size 1 \\
  --max-model-len $MAX_MODEL_LEN \\
  --max-num-seqs $MAX_NUM_SEQS \\
  --gpu-memory-utilization $GPU_MEMORY_UTILIZATION \\
  --generation-config vllm \\
  --enforce-eager \\
  --no-enable-log-requests
EOF_COMMAND

command -v ninja >/dev/null 2>&1 || {
  log "Installing ninja required by FlashInfer JIT"
  apt-get update -qq
  apt-get install -y ninja-build
}

log "Starting vLLM; model files are cached under $HF_HOME"
nohup "$VLLM_VENV/bin/vllm" serve "$MODEL_ID" \
  --revision "$MODEL_REVISION" \
  --served-model-name "$SERVED_MODEL_NAME" \
  --host 127.0.0.1 \
  --port "$VLLM_PORT" \
  --dtype auto \
  --tensor-parallel-size 1 \
  --max-model-len "$MAX_MODEL_LEN" \
  --max-num-seqs "$MAX_NUM_SEQS" \
  --gpu-memory-utilization "$GPU_MEMORY_UTILIZATION" \
  --generation-config vllm \
  --enforce-eager \
  --no-enable-log-requests \
  > "$EVIDENCE_DIR/vllm.log" 2>&1 &
VLLM_PID=$!
printf '%s\n' "$VLLM_PID" > "$EVIDENCE_DIR/vllm.pid"

log "Polling the local vLLM health endpoint"
READY=0
POLL_COUNT="$((VLLM_STARTUP_TIMEOUT_SECONDS / 5))"
(( POLL_COUNT > 0 )) || POLL_COUNT=1
for _ in $(seq 1 "$POLL_COUNT"); do
  if curl -fsS "http://127.0.0.1:$VLLM_PORT/health" >/dev/null 2>&1; then
    READY=1
    break
  fi
  if ! kill -0 "$VLLM_PID" >/dev/null 2>&1; then
    tail -n 200 "$EVIDENCE_DIR/vllm.log" >&2 || true
    fail "vLLM terminated before becoming healthy"
  fi
  sleep 5
done
if [[ "$READY" != "1" ]]; then
  tail -n 200 "$EVIDENCE_DIR/vllm.log" >&2 || true
  fail "vLLM did not become ready within ${VLLM_STARTUP_TIMEOUT_SECONDS}s"
fi
printf '%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  > "$EVIDENCE_DIR/vllm-ready-at-utc.txt"

curl -fsS "http://127.0.0.1:$VLLM_PORT/v1/models" \
  | jq . > "$EVIDENCE_DIR/vllm-models.json"
jq -e --arg name "$SERVED_MODEL_NAME" '.data | any(.id == $name)' \
  "$EVIDENCE_DIR/vllm-models.json" >/dev/null \
  || fail "The served model name is missing from /v1/models"

log "Issuing one warm-up request before the timed golden run"
jq -n --arg model "$SERVED_MODEL_NAME" '{
  model: $model,
  temperature: 0,
  max_tokens: 64,
  messages: [{role: "user", content: "Return only this JSON object: {\"ok\":true}"}]
}' > "$EVIDENCE_DIR/smoke-request.json"
curl -fsS "http://127.0.0.1:$VLLM_PORT/v1/chat/completions" \
  -H 'Content-Type: application/json' \
  --data-binary "@$EVIDENCE_DIR/smoke-request.json" \
  | jq . > "$EVIDENCE_DIR/smoke-response.json"
jq -e '.choices[0].message.content | type == "string" and length > 0' \
  "$EVIDENCE_DIR/smoke-response.json" >/dev/null \
  || fail "Warm-up response is malformed"

nvidia-smi > "$EVIDENCE_DIR/nvidia-smi-serving.txt"
nvidia-smi --query-gpu=name,memory.used,memory.total,utilization.gpu,temperature.gpu,power.draw \
  --format=csv,noheader > "$EVIDENCE_DIR/gpu-serving.csv"

log "Running the real-model ECI golden evaluation"
set +e
(
  cd "$ECI_DIR"
  timeout --signal=TERM "$EVAL_WALL_TIMEOUT_SECONDS" \
    "$ECI_VENV/bin/eci" eval-golden \
      --dataset tests/golden/queries_v0.json \
      --base-url "http://127.0.0.1:$VLLM_PORT" \
      --model "$SERVED_MODEL_NAME" \
      --output "$EVIDENCE_DIR/results.jsonl" \
      --require-real
) 2>&1 | tee "$EVIDENCE_DIR/eval-stdout.log"
EVAL_RC=${PIPESTATUS[0]}
set -e
printf '%s\n' "$EVAL_RC" > "$EVIDENCE_DIR/eval-exit-code.txt"

nvidia-smi > "$EVIDENCE_DIR/nvidia-smi-after-eval.txt" || true
stop_vllm
VLLM_PID=""
trap - EXIT INT TERM

SUMMARY_FILE="$EVIDENCE_DIR/results.jsonl.summary.json"
EXECUTION_GATE="FAIL"
if [[ "$EVAL_RC" == "0" && -s "$SUMMARY_FILE" ]]; then
  if jq -e '
      .is_real == true and
      .query_count == 10 and
      .success_count == 10 and
      .error_count == 0
    ' "$SUMMARY_FILE" >/dev/null; then
    EXECUTION_GATE="PASS"
  fi
fi
export EXECUTION_GATE

PASS_RATE="$(jq -r '.pass_rate // "unavailable"' "$SUMMARY_FILE" 2>/dev/null || printf unavailable)"
FACT_RECALL="$(jq -r '.fact_recall // "unavailable"' "$SUMMARY_FILE" 2>/dev/null || printf unavailable)"
COMPLETED_AT_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
END_EPOCH="$(date +%s)"
RUN_DURATION_SECONDS="$((END_EPOCH - START_EPOCH))"
export PASS_RATE FACT_RECALL COMPLETED_AT_UTC RUN_DURATION_SECONDS

"$VLLM_VENV/bin/python" - <<'PY_MANIFEST' > "$EVIDENCE_DIR/manifest.json"
import json
import os

string_keys = [
    "SCRIPT_VERSION", "RUN_ID", "STARTED_AT_UTC", "COMPLETED_AT_UTC",
    "MODEL_ID", "MODEL_REVISION", "SERVED_MODEL_NAME", "VLLM_VERSION",
    "ACTUAL_ECI_COMMIT", "GPU_NAME", "EXECUTION_GATE",
]
payload = {key.lower(): os.environ.get(key) for key in string_keys}
payload.update({
    "max_model_len": int(os.environ["MAX_MODEL_LEN"]),
    "max_num_seqs": int(os.environ["MAX_NUM_SEQS"]),
    "gpu_memory_utilization": float(os.environ["GPU_MEMORY_UTILIZATION"]),
    "run_duration_seconds": int(os.environ["RUN_DURATION_SECONDS"]),
    "pass_rate": None if os.environ["PASS_RATE"] == "unavailable" else float(os.environ["PASS_RATE"]),
    "fact_recall": None if os.environ["FACT_RECALL"] == "unavailable" else float(os.environ["FACT_RECALL"]),
    "runpod_pod_id": os.environ.get("RUNPOD_POD_ID"),
})
print(json.dumps(payload, sort_keys=True, indent=2))
PY_MANIFEST

cat > "$EVIDENCE_DIR/README.md" <<EOF_REPORT
# ECI T5.6 real-vLLM evaluation evidence

- Run ID: \`$RUN_ID\`
- Execution gate: **$EXECUTION_GATE**
- Started: \`$STARTED_AT_UTC\`
- Completed: \`$COMPLETED_AT_UTC\`
- Duration: \`$RUN_DURATION_SECONDS seconds\`
- ECI commit: \`$ACTUAL_ECI_COMMIT\`
- Model: \`$MODEL_ID\`
- Model revision: \`$MODEL_REVISION\`
- Served model name: \`$SERVED_MODEL_NAME\`
- vLLM: \`$VLLM_VERSION\`
- GPU: \`$GPU_NAME\`
- Max model length: \`$MAX_MODEL_LEN\`
- Golden query count: \`10\`
- Pass rate: \`$PASS_RATE\`
- Fact recall: \`$FACT_RECALL\`

The execution gate passes only when the real-model run completes all ten queries
without transport, HTTP, schema, or parsing errors. Pass rate and fact recall are
reported as measured quality outcomes; they are not silently converted into an
acceptance threshold absent from the current T5.6 task definition.
EOF_REPORT

(
  cd "$EVIDENCE_DIR"
  find . -type f ! -name SHA256SUMS -print0 \
    | sort -z \
    | xargs -0 sha256sum > SHA256SUMS
)

tar -C "$EVIDENCE_ROOT" -czf "$ARCHIVE" "$RUN_ID"
(
  cd "$WORKSPACE"
  sha256sum "$(basename "$ARCHIVE")" > "$(basename "$ARCHIVE_SHA256")"
)
printf '%s\n' "$ARCHIVE" > "$WORKSPACE/LAST_T56_ARCHIVE"

log "Evidence directory: $EVIDENCE_DIR"
log "Evidence archive:   $ARCHIVE"
log "Archive checksum:   $ARCHIVE_SHA256"
log "Execution gate:     $EXECUTION_GATE"
log "Pass rate:          $PASS_RATE"
log "Fact recall:        $FACT_RECALL"
log "Transfer command:   runpodctl send \"$ARCHIVE\""

if [[ "$EXECUTION_GATE" != "PASS" ]]; then
  exit 3
fi
