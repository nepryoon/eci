"""SPEC-019 §2 — fixture `session`-scoped che costruisce la pipeline
REALE per intero: Postgres+outbox, Debezium/Kafka Connect, Kafka,
Neo4j, `ingestion` (Rust, un'invocazione per ciascuno dei 4 file del
fixture SPEC-009), `sink-graph`/`retrieval-engine` (Go, sottoprocessi
reali) e `vllm-fake` (riuso diretto del bootstrap già costruito in
SPEC-018 `orchestrator.conftest._ensure_vllm_fake_venv`). Nessun grafo
di test costruito a mano (a differenza di
`services/orchestrator/orchestrator/conftest.py::_seed_graph`) — questa
SPEC esiste apposta per dimostrare la pipeline viva, non un sostituto.

Un'unica sessione: tutte le fixture sono `scope="session"`, costruite
UNA sola volta e riusate da tutti i test in `test_golden.py` (SPEC-019
§2 "Setup, una sola volta per l'intera sessione di test")."""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import harness
import pytest
from eci_core.retrieval.v1 import retrieval_pb2
from orchestrator.conftest import (
    _build_retrieval_engine,
    _ensure_vllm_fake_venv,
    _free_port,
    _start_opa_stub,
    _wait_for_grpc_health,
    _wait_for_http,
)
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.neo4j import Neo4jContainer
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy

REPO_ROOT = Path(__file__).resolve().parents[2]
INGESTION_DIR = REPO_ROOT / "services" / "ingestion"
SINK_GRAPH_DIR = REPO_ROOT / "services" / "sink-graph"
RETRIEVAL_ENGINE_DIR = REPO_ROOT / "services" / "retrieval-engine"
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "sample-repo"
MIGRATIONS_DIR = REPO_ROOT / "contracts" / "sql" / "migrations"
SCHEMA_CYPHER = REPO_ROOT / "contracts" / "cypher" / "schema.cypher"
CONNECTOR_TEMPLATE = REPO_ROOT / "deploy" / "compose" / "debezium-outbox-connector.json"

# Stessi user/password/dbname già scritti (non sostituiti) nel template
# del connector — SPEC-008 §2 sostituisce SOLO database.hostname/port.
POSTGRES_USER = "eci"
POSTGRES_PASSWORD = "eci-dev-only"
POSTGRES_DB = "eci"
POSTGRES_ALIAS = "e2e-postgres"
KAFKA_ALIAS = "e2e-kafka"
CONNECTOR_NAME = "eci-outbox-connector"
NEO4J_PASSWORD = "eci-e2e-test-password-1234"

# Ordine di ingestione indifferente (SPEC-019 §2) — quello del fixture
# SPEC-009, non alterato.
FIXTURE_FILES = ["main.go", "order_service.go", "notifier.go", "util.go"]

CDC_WAIT_TIMEOUT_SECONDS = 60.0

# Numero di figli CONTAINS attesi per ciascun File del fixture SPEC-009
# (struttura nota del fixture, non parte del contratto golden dataset).
FILE_CHILD_COUNTS = {"main.go": 1, "order_service.go": 3, "notifier.go": 3, "util.go": 1}

# Entità cercate dalle 3 query `callers` del golden dataset (g01/g02/g03)
# — usate per attendere che l'indice full-text le abbia indicizzate
# prima di fidarsi di un risultato vuoto (SPEC-019 §10, deviazione).
FULLTEXT_ENTITIES = ["Validate", "Process", "computeTotal"]


@pytest.fixture(scope="session")
def docker_network():
    network = Network()
    network.create()
    try:
        yield network
    finally:
        network.remove()


@pytest.fixture(scope="session")
def postgres_container(docker_network):
    container = (
        PostgresContainer(
            image="postgres:17",
            username=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB,
            driver=None,
        )
        # wal_level=logical: stessa configurazione di SPEC-007/SPEC-008,
        # senza Debezium non può fare logical decoding.
        .with_command(
            ["postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=4", "-c", "max_replication_slots=4"]
        )
        .with_network(docker_network)
        .with_network_aliases(POSTGRES_ALIAS)
    )
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def postgres_dsn(postgres_container) -> str:
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    return f"postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host}:{port}/{POSTGRES_DB}?sslmode=disable"


@pytest.fixture(scope="session")
def migrated_postgres(postgres_dsn) -> str:
    if not shutil.which("migrate"):
        raise RuntimeError(
            "binario 'migrate' non trovato sul PATH — richiesto per applicare "
            "contracts/sql/migrations (stesso requisito già di "
            "tests/integration/outbox_cdc, SPEC-008)"
        )
    subprocess.run(
        ["migrate", "-source", f"file://{MIGRATIONS_DIR}", "-database", postgres_dsn, "up"],
        check=True,
    )
    return postgres_dsn


@pytest.fixture(scope="session")
def neo4j_container():
    container = Neo4jContainer(image="neo4j:5-community", username="neo4j", password=NEO4J_PASSWORD)
    container.start()
    try:
        driver = container.get_driver()
        with driver.session() as session:
            for stmt in harness.split_cypher_statements(SCHEMA_CYPHER.read_text()):
                session.run(stmt)
        driver.close()
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def neo4j_bolt_url(neo4j_container) -> str:
    return neo4j_container.get_connection_url()


@pytest.fixture(scope="session")
def kafka_container(docker_network):
    container = (
        KafkaContainer(image="confluentinc/cp-kafka:7.6.0")
        .with_network(docker_network)
        .with_network_aliases(KAFKA_ALIAS)
    )
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap_host(kafka_container) -> str:
    """Bootstrap server raggiungibile dall'HOST (listener PLAINTEXT) —
    a differenza del container Kafka usato in
    tests/integration/outbox_cdc (SPEC-008), che non espone nessuna
    porta host: qui `sink-graph` gira come sottoprocesso reale
    sull'host e deve potersi connettere come client Kafka vero, non
    solo via `Exec` dentro il container."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def kafka_connect_container(docker_network, kafka_container):
    container = (
        DockerContainer("quay.io/debezium/connect:latest")
        .with_env("BOOTSTRAP_SERVERS", f"{KAFKA_ALIAS}:9092")
        .with_env("GROUP_ID", "eci-e2e-connect")
        .with_env("CONFIG_STORAGE_TOPIC", "eci_e2e_connect_configs")
        .with_env("OFFSET_STORAGE_TOPIC", "eci_e2e_connect_offsets")
        .with_env("STATUS_STORAGE_TOPIC", "eci_e2e_connect_status")
        .with_exposed_ports(8083)
        .with_network(docker_network)
        .waiting_for(
            HttpWaitStrategy(8083, "/connectors").for_status_code(200).with_startup_timeout(90).with_poll_interval(2)
        )
    )
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def kafka_connect_url(kafka_connect_container) -> str:
    host = kafka_connect_container.get_container_host_ip()
    port = kafka_connect_container.get_exposed_port(8083)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def outbox_connector(kafka_connect_url, migrated_postgres) -> None:
    """Registra il connector DOPO che la migrazione ha creato la tabella
    outbox (SPEC-008 §2/§4: altrimenti Debezium logga "no changes will
    be captured" e il test fallirebbe per sequencing). Template reale
    (`deploy/compose/debezium-outbox-connector.json`), non duplicato a
    mano — solo host/porta Postgres patchati con l'alias/porta interna
    di QUESTO stack di test."""
    connector_json = harness.build_connector_config(
        CONNECTOR_TEMPLATE, CONNECTOR_NAME, hostname=POSTGRES_ALIAS, port="5432"
    )
    harness.register_connector(kafka_connect_url, connector_json, timeout=60)
    harness.wait_connector_running(kafka_connect_url, CONNECTOR_NAME, timeout=60)


def _run_ingestion(filename: str, postgres_dsn_: str) -> str:
    env = {**os.environ, "POSTGRES_DSN": postgres_dsn_}
    proc = subprocess.run(
        ["cargo", "run", "--quiet", "--manifest-path", str(INGESTION_DIR / "Cargo.toml"), "--", filename],
        cwd=FIXTURE_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"ingestion di {filename!r} fallita (exit={proc.returncode}):\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc.stdout


@pytest.fixture(scope="session")
def ingested_fixture(migrated_postgres) -> dict[str, str]:
    """Ingerisce i 4 file del fixture SPEC-009 attraverso il binario
    REALE `ingestion` (T1.1/T1.2), uno alla volta, invocato con cwd
    dentro tests/fixtures/sample-repo e il solo nome file (bare) come
    argomento — così `CodeNode.name`/`file_path` del File risultante è
    esattamente "order_service.go" ecc. (stesso identificatore usato
    dal golden dataset e ricalcolabile deterministicamente da
    `harness.file_node_id`, senza dover passare per una ricerca
    full-text sul nome file, che con un punto interno è un caso non
    banale per Lucene). Ritorna l'output testuale per il report §7."""
    outputs = {}
    for filename in FIXTURE_FILES:
        outputs[filename] = _run_ingestion(filename, migrated_postgres)
    return outputs


def _build_go_binary(service_dir: Path, name: str) -> str:
    out_path = os.path.join(tempfile.mkdtemp(prefix=f"{name}-"), name)
    subprocess.run(["go", "build", "-o", out_path, "."], cwd=service_dir, check=True)
    return out_path


@pytest.fixture(scope="session")
def sink_graph_process(neo4j_bolt_url, kafka_bootstrap_host, migrated_postgres, outbox_connector, ingested_fixture):
    """`sink-graph` (SPEC-015) come sottoprocesso Go reale, consumer
    Kafka -> MERGE Neo4j. Nessun health-check dedicato (non espone
    HTTP/gRPC, è un consumer puro): la prova di funzionamento è la
    stessa attesa di propagazione CDC (`harness.wait_for_node`) usata
    per il resto della pipeline, non un check separato qui."""
    binary = _build_go_binary(SINK_GRAPH_DIR, "sink-graph")
    env = {
        **os.environ,
        "NEO4J_URI": neo4j_bolt_url,
        "NEO4J_USER": "neo4j",
        "NEO4J_PASSWORD": NEO4J_PASSWORD,
        "POSTGRES_DSN": migrated_postgres,
        "KAFKA_BROKERS": kafka_bootstrap_host,
    }
    proc = subprocess.Popen([binary], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        yield proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture(scope="session")
def opa_url():
    server, thread, url = _start_opa_stub()
    try:
        yield url
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.fixture(scope="session")
def retrieval_engine_addr(neo4j_bolt_url, opa_url):
    binary = _build_retrieval_engine()
    addr = f"127.0.0.1:{_free_port()}"
    env = {
        **os.environ,
        "NEO4J_URI": neo4j_bolt_url,
        "NEO4J_USER": "neo4j",
        "NEO4J_PASSWORD": NEO4J_PASSWORD,
        "RETRIEVAL_ENGINE_ADDR": addr,
        "OPA_URL": opa_url,
        "OPA_ALLOW_INSECURE_HTTP": "true",
    }
    proc = subprocess.Popen([binary], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        security_context = retrieval_pb2.SecurityContext(
            tenant_id="eci-e2e-tenant",
            user_id="eci-e2e-user",
            allowed_repos=["sample-repo"],
            acl_groups=["developers"],
        )
        _wait_for_grpc_health(addr, timeout=30, security_context=security_context)
        yield addr
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture(scope="session")
def vllm_fake_url():
    """Riuso diretto del bootstrap SPEC-018
    (`orchestrator.conftest._ensure_vllm_fake_venv`/`_wait_for_http`),
    non reinventato qui (SPEC-019 §2)."""
    uvicorn_bin = _ensure_vllm_fake_venv()
    port = _free_port()
    vllm_fake_dir = REPO_ROOT / "fakes" / "vllm-fake"
    proc = subprocess.Popen(
        [str(uvicorn_bin), "vllm_fake.main:app", "--port", str(port)],
        cwd=vllm_fake_dir,
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


@pytest.fixture(scope="session")
def stack(retrieval_engine_addr, sink_graph_process, vllm_fake_url):
    """Stack completo pronto: attende la propagazione CDC PRIMA di
    restituire il controllo ai test (SPEC-019 §2 "polling... fino a
    comparsa in Neo4j, con timeout esplicito — mai un semplice sleep
    fisso"). Fallimento esplicito su quale nodo non è comparso, non un
    timeout generico (SPEC-019 §4 edge case 1).

    Deviazione dichiarata da SPEC-019 §2/§10: il testo della SPEC
    limita l'attesa ai soli 4 id di File noti. Verificato empiricamente
    (run ripetuti a stack fresco) che questo non basta a rendere la
    suite deterministica — vedi le docstring di
    `harness.wait_for_populated_contains`/`harness.wait_for_fulltext_match`
    per le due race condition reali scoperte (nodo CONTAINS "nudo"
    creato da una relazione arrivata prima del nodo; indice full-text
    eventually-consistent). Qui si attende, in aggiunta ai 4 File, che
    ciascun File abbia il numero atteso di figli CONTAINS con nome
    popolato e che le 3 entità cercate dalle query `callers` siano
    trovabili via full-text — resta un polling con timeout esplicito su
    un insieme più ampio di nodi noti, non uno sleep fisso."""
    security_context = retrieval_pb2.SecurityContext(
        tenant_id="eci-e2e-tenant",
        user_id="eci-e2e-user",
        allowed_repos=["sample-repo"],
        acl_groups=["developers"],
    )
    for filename in FIXTURE_FILES:
        file_id = harness.file_node_id(filename)
        harness.wait_for_node(retrieval_engine_addr, security_context, file_id, timeout=CDC_WAIT_TIMEOUT_SECONDS)
    for filename, expected_count in FILE_CHILD_COUNTS.items():
        file_id = harness.file_node_id(filename)
        harness.wait_for_populated_contains(
            retrieval_engine_addr, security_context, file_id, expected_count, timeout=CDC_WAIT_TIMEOUT_SECONDS
        )
    for entity_name in FULLTEXT_ENTITIES:
        harness.wait_for_fulltext_match(
            retrieval_engine_addr,
            security_context,
            entity_name,
            entity_name,
            timeout=CDC_WAIT_TIMEOUT_SECONDS,
        )
    return {
        "retrieval_addr": retrieval_engine_addr,
        "vllm_url": vllm_fake_url,
        "security_context": security_context,
    }
