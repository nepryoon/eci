"""SPEC-018 §7 — fixture di integrazione: Neo4j reale (testcontainers,
stesso pattern SPEC-016), retrieval-engine reale (sottoprocesso Go,
`go build` + binario lanciato puntato al Neo4j di test) e vllm-fake
reale (sottoprocesso `uvicorn` dal suo stesso venv, bootstrap automatico
se assente) — non mock di nessuno dei due, come richiesto esplicitamente
da SPEC-018 §7."""

import json
import os
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import grpc
import httpx
import pytest
from eci_core.grpc_client import SecurityContextInterceptor
from eci_core.retrieval.v1 import retrieval_pb2, retrieval_pb2_grpc
from testcontainers.community.neo4j import Neo4jContainer

REPO_ROOT = Path(__file__).resolve().parents[3]
RETRIEVAL_ENGINE_DIR = REPO_ROOT / "services" / "retrieval-engine"
VLLM_FAKE_DIR = REPO_ROOT / "fakes" / "vllm-fake"

NEO4J_TEST_PASSWORD = "eci-test-password-1234"

# Stesso grafo di test di SPEC-016 (services/retrieval-engine/internal/server/
# server_integration_test.go): sink-graph (SPEC-015) produrrebbe esattamente
# questa forma per order_service.go — id fissi, non generati, per asserzioni
# dirette nei test.
FILE_ID = "orch-test-file-order-service"
CLASS_ID = "orch-test-class-order-service"
PROCESS_ID = "orch-test-method-process"
VALIDATE_ID = "orch-test-method-validate"
COMPUTE_ID = "orch-test-func-compute-total"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _split_cypher_statements(src: str) -> list[str]:
    kept = [line for line in src.splitlines() if not line.strip().startswith("//")]
    joined = "\n".join(kept)
    return [s.strip() for s in joined.split(";") if s.strip()]


def _seed_graph(session) -> None:
    # Process -> Validate è l'UNICA CALLS: Validate non ha archi CALLS
    # uscenti — asimmetria deliberata, usata dai test per verificare che
    # find_callers (REVERSE) trovi Process partendo da Validate, e che
    # la direzione conti davvero (non un risultato che tornerebbe
    # identico con FORWARD).
    session.run(
        """
        CREATE (f:CodeNode:File {id: $file_id, domain: 'code', name: 'order_service.go', ast_hash: 'hash-file', repo: 'local', path: 'order_service.go'})
        CREATE (c:CodeNode:Class {id: $class_id, domain: 'code', name: 'OrderService', ast_hash: 'hash-class', repo: 'local', path: 'order_service.go'})
        CREATE (p:CodeNode:Method {id: $process_id, domain: 'code', name: 'Process', ast_hash: 'hash-process', repo: 'local', path: 'order_service.go', symbol_id: $process_id})
        CREATE (v:CodeNode:Method {id: $validate_id, domain: 'code', name: 'Validate', ast_hash: 'hash-validate', repo: 'local', path: 'order_service.go', symbol_id: $validate_id})
        CREATE (ct:CodeNode:Function {id: $compute_id, domain: 'code', name: 'computeTotal', ast_hash: 'hash-compute', repo: 'local', path: 'util.go'})
        CREATE (f)-[:CONTAINS {weight: 1}]->(c)
        CREATE (f)-[:CONTAINS {weight: 1}]->(p)
        CREATE (f)-[:CONTAINS {weight: 1}]->(v)
        CREATE (p)-[:CALLS {weight: 1}]->(v)
        """,
        file_id=FILE_ID,
        class_id=CLASS_ID,
        process_id=PROCESS_ID,
        validate_id=VALIDATE_ID,
        compute_id=COMPUTE_ID,
    )


@pytest.fixture(scope="session")
def neo4j_bolt_url():
    container = Neo4jContainer(image="neo4j:5-community", username="neo4j", password=NEO4J_TEST_PASSWORD)
    container.start()
    try:
        driver = container.get_driver()
        schema_src = (REPO_ROOT / "contracts" / "cypher" / "schema.cypher").read_text()
        with driver.session() as session:
            for stmt in _split_cypher_statements(schema_src):
                session.run(stmt)
            _seed_graph(session)
        driver.close()
        yield container.get_connection_url()
    finally:
        container.stop()


def _build_retrieval_engine() -> str:
    out_path = os.path.join(tempfile.mkdtemp(prefix="retrieval-engine-"), "retrieval-engine")
    subprocess.run(["go", "build", "-o", out_path, "."], cwd=RETRIEVAL_ENGINE_DIR, check=True)
    return out_path


class _OPAStubHandler(BaseHTTPRequestHandler):
    """Deterministic transport stub for existing orchestrator integration tests.

    SPEC-056 policy correctness is exercised separately against real pinned OPA;
    this stub only keeps the multi-container orchestration fixture focused on its
    original retrieval/LLM behavior.
    """

    def do_GET(self):
        self.send_response(200 if self.path == "/health" else 404)
        self.end_headers()

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            value = json.loads(self.rfile.read(length))
            item = value["input"]
            subject = item["subject"]
            allow = bool(
                subject.get("tenant_id")
                and subject.get("user_id")
                and (
                    item.get("action") == "/eci.retrieval.v1.RetrievalEngine/Health"
                    or (subject.get("allowed_repos") and subject.get("acl_groups"))
                )
            )
            body = json.dumps(
                {"result": {"allow": allow, "reason": "allow" if allow else "policy_denied"}}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            self.send_response(400)
            self.end_headers()

    def log_message(self, _format, *_args):
        return


def _start_opa_stub():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _OPAStubHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}"


@pytest.fixture(scope="session")
def opa_url():
    server, thread, url = _start_opa_stub()
    try:
        yield url
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def _wait_for_grpc_health(
    addr: str, timeout: float, security_context: retrieval_pb2.SecurityContext
) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        channel = grpc.intercept_channel(
            grpc.insecure_channel(addr), SecurityContextInterceptor(security_context)
        )
        try:
            stub = retrieval_pb2_grpc.RetrievalEngineStub(channel)
            stub.Health(retrieval_pb2.HealthCheckRequest(), timeout=2)
            return
        except grpc.RpcError as e:
            last_err = e
            time.sleep(0.5)
        finally:
            channel.close()
    raise RuntimeError(f"retrieval-engine non pronto su {addr} entro {timeout}s: {last_err}")


@pytest.fixture(scope="session")
def retrieval_engine_addr(neo4j_bolt_url, opa_url):
    binary = _build_retrieval_engine()
    addr = f"127.0.0.1:{_free_port()}"
    env = {
        **os.environ,
        "NEO4J_URI": neo4j_bolt_url,
        "NEO4J_USER": "neo4j",
        "NEO4J_PASSWORD": NEO4J_TEST_PASSWORD,
        "RETRIEVAL_ENGINE_ADDR": addr,
        "OPA_URL": opa_url,
        "OPA_ALLOW_INSECURE_HTTP": "true",
    }
    # The production stdout OTel exporter emits one multi-line span per authz
    # decision. A PIPE that nobody drains eventually fills and blocks the gRPC
    # server; discard test-process telemetry instead of creating that deadlock.
    proc = subprocess.Popen([binary], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, text=True)
    try:
        _wait_for_grpc_health(
            addr,
            timeout=30,
            security_context=retrieval_pb2.SecurityContext(
                tenant_id="eci-test-tenant",
                user_id="eci-test-user",
                allowed_repos=["local"],
                acl_groups=["developers"],
            ),
        )
        yield addr
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _create_venv(venv_dir: Path) -> None:
    """Stesso fallback di scripts/task-build.sh:ensure_venv() — replicato
    qui, non reinventato: `python3 -m venv` (stdlib, sempre disponibile)
    tentato per primo, `python3 -m virtualenv` SOLO se quello fallisce.
    Causa reale osservata (non ipotetica): nell'immagine Docker
    python:3.11 usata in CI, `python -m virtualenv` diretto falliva con
    `ModuleNotFoundError` (virtualenv non installato nel Python di
    sistema), mentre `python -m venv` — modulo stdlib — funziona sempre,
    a meno che manchi `ensurepip` (lo stesso caso limite già gestito dal
    fallback bash, non specifico di questo file).

    Deliberatamente `"python3"` (risolto da PATH), NON `sys.executable`:
    questa funzione gira DENTRO pytest, già in esecuzione nel venv di
    orchestrator — `sys.executable` punterebbe al python DI QUEL venv,
    che non eredita gli user site-packages del sistema (isolamento
    standard di un venv). Verificato empiricamente che questo importa
    davvero: su questo host `python3 -m venv` fallisce comunque
    (ensurepip mancante, indipendentemente da quale interprete lo invoca)
    ma il fallback `python3 -m virtualenv` funziona SOLO se `python3` è
    il Python di sistema (dove `virtualenv` è installato in
    `~/.local/lib/.../site-packages`) — dallo stesso identico comando
    lanciato tramite l'interprete di un venv nidificato, `virtualenv` non
    è importabile e il fallback fallirebbe silenziosamente. `python3` via
    PATH è esattamente cosa fa `scripts/task-build.sh:ensure_venv()`
    quando bash la invoca — stessa risoluzione, non solo la stessa
    sequenza di comandi."""
    result = subprocess.run(["python3", "-m", "venv", str(venv_dir)], capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return

    result_virtualenv = subprocess.run(
        ["python3", "-m", "virtualenv", "-q", str(venv_dir)], capture_output=True, text=True, check=False
    )
    if result_virtualenv.returncode == 0:
        return

    raise RuntimeError(
        f"impossibile creare il venv in {venv_dir}: "
        f"'python3 -m venv' -> {result.stdout.strip()!r} {result.stderr.strip()!r}; "
        f"'python3 -m virtualenv' -> {result_virtualenv.stdout.strip()!r} {result_virtualenv.stderr.strip()!r}"
    )


def _ensure_vllm_fake_venv() -> Path:
    """Bootstrap del venv di fakes/vllm-fake se assente — stessi comandi
    del suo README (SPEC-017), eseguiti qui invece che a mano: questo
    file di test dipende da un vllm-fake realmente in esecuzione."""
    uvicorn_bin = VLLM_FAKE_DIR / ".venv" / "bin" / "uvicorn"
    if uvicorn_bin.exists():
        return uvicorn_bin

    venv_dir = VLLM_FAKE_DIR / ".venv"
    _create_venv(venv_dir)
    subprocess.run(
        [
            str(venv_dir / "bin" / "pip"),
            "install",
            "-q",
            "-e",
            str(REPO_ROOT / "libs" / "py"),
            "-e",
            str(VLLM_FAKE_DIR),
        ],
        check=True,
    )
    return uvicorn_bin


def _wait_for_http(base_url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            resp = httpx.post(
                base_url + "/v1/chat/completions",
                json={"model": "vllm-fake", "messages": [{"role": "user", "content": "readiness probe"}]},
                timeout=2,
            )
            if resp.status_code == 200:
                return
        except httpx.HTTPError as e:
            last_err = e
        time.sleep(0.5)
    raise RuntimeError(f"vllm-fake non pronto su {base_url} entro {timeout}s: {last_err}")


@pytest.fixture(scope="session")
def vllm_fake_url():
    uvicorn_bin = _ensure_vllm_fake_venv()
    port = _free_port()
    proc = subprocess.Popen(
        [str(uvicorn_bin), "vllm_fake.main:app", "--port", str(port)],
        cwd=VLLM_FAKE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_http(url, timeout=30)
        yield url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
