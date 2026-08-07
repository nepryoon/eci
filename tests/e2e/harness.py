"""SPEC-019 — funzioni pure riusate sia da `conftest.py` (setup dello
stack) sia da `test_golden.py` (asserzioni e verifica esplicita dei due
edge case di SPEC-019 §4). Nessuna fixture pytest qui: solo logica
testabile/richiamabile isolatamente, così l'edge case "timeout CDC" e
l'edge case "categoria non riconosciuta" possono richiamare la STESSA
funzione usata dal setup/dalla verifica reale, non una copia
semplificata.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import grpc
import httpx
from eci_core.retrieval.v1 import retrieval_pb2, retrieval_pb2_grpc
from orchestrator import retrieval_client as rc

_RPC_TIMEOUT_SECONDS = 10.0

# ============================================================
# Id dei nodi: stesso identico calcolo di services/ingestion/src/lib.rs
# (`make_id`/`sha256_hex`) — sha256_hex(f"{file_path}:{qualified_name}").
# Deterministico e noto in anticipo: permette a questo harness di
# individuare File/Class/Method senza dipendere dalla tokenizzazione
# Lucene del full-text index (un nome file con un punto, es.
# "order_service.go", non è un caso semplice per un match full-text
# esatto — la ricerca via HybridSearch è riusata SOLO per la categoria
# `callers`, dove è già verificata funzionante end-to-end da SPEC-018).
# ============================================================


def node_id(file_path: str, qualified_name: str) -> str:
    return hashlib.sha256(f"{file_path}:{qualified_name}".encode()).hexdigest()


def file_node_id(file_path: str) -> str:
    return node_id(file_path, file_path)


# ============================================================
# Schema Neo4j: stesso split "riga per riga, ignora i commenti //" già
# usato in services/orchestrator/orchestrator/conftest.py
# (`_split_cypher_statements`) — duplicato qui (non importato, è privato
# e specifico di quel conftest) perché è tre righe di logica, non un
# pattern da centralizzare cross-repo per questa SPEC.
# ============================================================


def split_cypher_statements(src: str) -> list[str]:
    kept = [line for line in src.splitlines() if not line.strip().startswith("//")]
    joined = "\n".join(kept)
    return [s.strip() for s in joined.split(";") if s.strip()]


# ============================================================
# Client gRPC: riuso diretto di `orchestrator.retrieval_client._build_channel`
# (SPEC-019 §2 — "riuso del client gRPC già costruito in
# orchestrator.retrieval_client"), non un canale reinventato qui.
# ============================================================


def get_node(addr: str, security_context: retrieval_pb2.SecurityContext, node_id_: str):
    """Ritorna il RetrievedNode. Solleva grpc.RpcError (NOT_FOUND) se
    assente — propagato al chiamante, non intercettato qui (chi chiama
    decide se è un'attesa CDC in corso o un errore reale)."""
    channel = rc._build_channel(security_context, addr)
    try:
        stub = retrieval_pb2_grpc.RetrievalEngineStub(channel)
        response = stub.GetNode(
            retrieval_pb2.GetNodeRequest(security_context=security_context, node_id=node_id_),
            timeout=_RPC_TIMEOUT_SECONDS,
        )
        return response.node
    finally:
        channel.close()


def expand_contains(addr: str, security_context: retrieval_pb2.SecurityContext, node_id_: str) -> list:
    """ExpandNeighbors(FORWARD, CONTAINS) — usato per le categorie
    `contains`/`methods` (SPEC-019 §2), MAI passando per `eci ask`
    (l'orchestrator non ha un'euristica per "cosa contiene X")."""
    channel = rc._build_channel(security_context, addr)
    try:
        stub = retrieval_pb2_grpc.RetrievalEngineStub(channel)
        response = stub.ExpandNeighbors(
            retrieval_pb2.ExpandNeighborsRequest(
                security_context=security_context,
                node_id=node_id_,
                edge_types=[retrieval_pb2.EDGE_TYPE_CONTAINS],
                direction=retrieval_pb2.TRAVERSAL_DIRECTION_FORWARD,
            ),
            timeout=_RPC_TIMEOUT_SECONDS,
        )
        return list(response.neighbors)
    finally:
        channel.close()


def wait_for_node(
    addr: str,
    security_context: retrieval_pb2.SecurityContext,
    node_id_: str,
    timeout: float,
    poll_interval: float = 1.0,
) -> None:
    """Polling reale di GetNode fino a comparsa del nodo (propagazione
    CDC: Postgres outbox -> Debezium -> Kafka -> sink-graph -> Neo4j) —
    MAI un semplice sleep fisso (SPEC-019 §2). Fallimento esplicito con
    messaggio chiaro su QUALE nodo non è comparso (SPEC-019 §4 edge
    case 1), non un timeout generico."""
    deadline = time.monotonic() + timeout
    last_err: grpc.RpcError | None = None
    while True:
        try:
            get_node(addr, security_context, node_id_)
            return
        except grpc.RpcError as e:
            last_err = e
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"nodo {node_id_!r} non comparso in Neo4j entro {timeout}s "
                f"(attesa propagazione CDC: Postgres outbox -> Debezium -> Kafka -> "
                f"sink-graph -> Neo4j) — ultimo errore gRPC osservato: {last_err}"
            )
        time.sleep(poll_interval)


def wait_for_populated_contains(
    addr: str,
    security_context: retrieval_pb2.SecurityContext,
    file_id: str,
    expected_count: int,
    timeout: float,
    poll_interval: float = 1.0,
) -> None:
    """Deviazione dichiarata rispetto a SPEC-019 §2 (SPEC-019 §10):
    attendere solo la comparsa dei 4 nodi File non basta. `sink-graph`
    fa `MERGE (from) MERGE (to)` per una CodeRelation SENZA impostare
    proprietà (services/sink-graph/internal/consumer/cypher.go) — dato
    che CodeNode e CodeRelation viaggiano su topic Kafka distinti senza
    garanzia di ordine tra topic, un messaggio CodeRelation può essere
    processato PRIMA del CodeNode del suo stesso endpoint, creando un
    nodo "nudo" (solo `id`, `name` vuoto) fino a quando il CodeNode
    corrispondente non arriva. Verificato empiricamente: un secondo run
    a stack fresco ha prodotto esattamente questo sintomo (un vicino
    CONTAINS con `name == ""`). Qui si attende sia il conteggio atteso
    di figli sia che ciascuno abbia un `name` non vuoto."""
    deadline = time.monotonic() + timeout
    last_state: object = "nessun tentativo effettuato"
    while True:
        try:
            neighbors = expand_contains(addr, security_context, file_id)
            names = [n.name for n in neighbors]
            last_state = names
            if len(neighbors) == expected_count and all(names):
                return
        except grpc.RpcError as e:
            last_state = str(e)
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"CONTAINS di {file_id!r} non ha raggiunto {expected_count} figli con nome "
                f"popolato entro {timeout}s (relazione CDC processata prima del nodo, o nodo "
                f"non ancora propagato) — ultimo stato osservato: {last_state!r}"
            )
        time.sleep(poll_interval)


def wait_for_fulltext_match(
    addr: str,
    security_context: retrieval_pb2.SecurityContext,
    query_text: str,
    expected_name: str,
    timeout: float,
    poll_interval: float = 1.0,
) -> None:
    """Deviazione dichiarata rispetto a SPEC-019 §2 (SPEC-019 §10): un
    nodo esistente (MATCH diretto, immediatamente consistente) non
    implica che l'indice full-text `code_fulltext` lo abbia già
    indicizzato — gli indici full-text di Neo4j 5 sono "eventually
    consistent" per design, popolati in modo asincrono rispetto al
    commit della transazione che scrive il nodo. Verificato
    empiricamente: un secondo run a stack fresco ha prodotto Fonti
    vuote per query che al run precedente trovavano correttamente
    l'entità cercata. Qui si attende che una ricerca full-text reale
    trovi l'entità PRIMA di fidarsi di un risultato vuoto altrove
    (SPEC-019 §2 categoria `callers`)."""
    from orchestrator.retrieval_client import hybrid_search

    deadline = time.monotonic() + timeout
    last_state: object = "nessun tentativo effettuato"
    while True:
        try:
            nodes = hybrid_search(addr, query_text, security_context)
            names = {n.name for n in nodes}
            last_state = names
            if expected_name in names:
                return
        except Exception as e:  # noqa: BLE001 — qualunque fallimento gRPC è "non ancora pronto"
            last_state = str(e)
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"ricerca full-text {query_text!r} non ha ancora trovato {expected_name!r} "
                f"entro {timeout}s (indice code_fulltext eventually-consistent) — "
                f"ultimo stato osservato: {last_state!r}"
            )
        time.sleep(poll_interval)


# ============================================================
# Categorie di expected_facts (SPEC-019 §2/§4 edge case 2): un'entry con
# una chiave fuori da questo vocabolario deve fallire esplicitamente, non
# essere saltata in silenzio.
# ============================================================

KNOWN_CATEGORIES = ("callers", "contains", "node_type", "methods", "implementations")


def classify_category(expected_facts: dict) -> str:
    keys = set(expected_facts.keys())
    unknown = keys - set(KNOWN_CATEGORIES)
    if unknown or len(keys) != 1:
        raise ValueError(
            f"categoria/e di expected_facts non riconosciuta/e: {sorted(unknown) if unknown else sorted(keys)} "
            f"(chiavi presenti: {sorted(keys)}, attese esattamente una tra {KNOWN_CATEGORIES}) — "
            f"SPEC-019 §4: un'entry non gestita è un errore da correggere, non uno skip silenzioso."
        )
    return next(iter(keys))


# ============================================================
# Registrazione connector Debezium: stesso schema a due fasi (POST
# /connectors -> poll /status fino a RUNNING) già verificato in
# tests/integration/outbox_cdc/outbox_cdc_test.go (SPEC-008), qui in
# Python.
# ============================================================


def build_connector_config(template_path: Path, connector_name: str, hostname: str, port: str) -> bytes:
    doc = json.loads(template_path.read_text())
    if doc["name"] != connector_name:
        raise ValueError(f"template connector: name = {doc['name']!r}, want {connector_name!r}")
    doc["config"]["database.hostname"] = hostname
    doc["config"]["database.port"] = port
    return json.dumps(doc).encode()


def register_connector(connect_url: str, connector_json: bytes, timeout: float, poll_interval: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    last_info = "nessun tentativo effettuato"
    while True:
        try:
            resp = httpx.post(
                f"{connect_url}/connectors",
                content=connector_json,
                headers={"Content-Type": "application/json"},
                timeout=5,
            )
            if resp.status_code in (201, 409):
                return
            last_info = f"http_code={resp.status_code} body={resp.text}"
        except httpx.HTTPError as e:
            last_info = str(e)
        if time.monotonic() >= deadline:
            raise RuntimeError(f"registrazione connector fallita dopo {timeout}s: {last_info}")
        time.sleep(poll_interval)


def wait_connector_running(
    connect_url: str, connector_name: str, timeout: float, poll_interval: float = 2.0
) -> dict:
    deadline = time.monotonic() + timeout
    last_status: dict | str = "nessuno stato osservato"
    while True:
        try:
            resp = httpx.get(f"{connect_url}/connectors/{connector_name}/status", timeout=5)
            if resp.status_code == 200:
                status = resp.json()
                last_status = status
                running_tasks = sum(1 for t in status.get("tasks", []) if t.get("state") == "RUNNING")
                if status.get("connector", {}).get("state") == "RUNNING" and running_tasks >= 1:
                    return status
        except httpx.HTTPError as e:
            last_status = str(e)
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"connector {connector_name!r} non arrivato a RUNNING con almeno un task RUNNING "
                f"entro {timeout}s: ultimo stato osservato {last_status}"
            )
        time.sleep(poll_interval)
