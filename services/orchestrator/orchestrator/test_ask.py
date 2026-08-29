"""SPEC-018 §3/§4/§7 — test di integrazione: retrieval-engine (Go, contro
Neo4j via testcontainers) e vllm-fake (Python) realmente avviati per il
test (fixture in conftest.py), nessun mock di nessuno dei due. Gli
scenari 1-3/5 chiamano `run_ask` direttamente (funzione Python, SPEC-018
§7); la sezione finale verifica `eci ask` come comando installato
realmente (sottoprocesso), non solo `run_ask`."""

import subprocess
import sys
from pathlib import Path

import grpc
import pytest
from eci_core.grpc_client import SecurityContextInterceptor
from eci_core.retrieval.v1 import retrieval_pb2, retrieval_pb2_grpc

from orchestrator.ask import run_ask
from orchestrator.conftest import COMPUTE_ID, PROCESS_ID, VALIDATE_ID
from orchestrator.errors import LLMUnavailableError, RetrievalUnavailableError
from orchestrator.llm_client import chat_completion
from orchestrator.prompt import SYSTEM_MESSAGE, build_messages
from orchestrator.retrieval_client import find_callers, hybrid_search
from orchestrator.who_calls import is_who_calls_query

UNREACHABLE_GRPC_ADDR = "127.0.0.1:1"
UNREACHABLE_HTTP_URL = "http://127.0.0.1:1"


def authenticated_context():
    return retrieval_pb2.SecurityContext(
        tenant_id="eci-test-tenant",
        user_id="eci-test-user",
        allowed_repos=["local"],
        acl_groups=["developers"],
    )


# ============================================================
# SPEC-018 §3 scenario 1 — query reale, risposta + Fonti corrette.
# ============================================================


def test_scenario1_who_calls_validate_finds_process(retrieval_engine_addr, vllm_fake_url):
    result = run_ask("Chi chiama Validate?", retrieval_engine_addr, vllm_fake_url, authenticated_context())

    assert result.answer is not None
    assert "[vllm-fake risposta deterministica]" in result.answer

    node_ids = {n.node_id for n in result.nodes}
    assert PROCESS_ID in node_ids, f"Process (id={PROCESS_ID}) atteso tra le Fonti, trovato: {node_ids}"


# ============================================================
# SPEC-018 §3 scenario 2 — determinismo end-to-end, la verifica più
# importante (SPEC-018 §7): due invocazioni COMPLETE, confronto diretto.
# ============================================================


def test_scenario2_end_to_end_determinism(retrieval_engine_addr, vllm_fake_url):
    result1 = run_ask("Chi chiama Validate?", retrieval_engine_addr, vllm_fake_url, authenticated_context())
    result2 = run_ask("Chi chiama Validate?", retrieval_engine_addr, vllm_fake_url, authenticated_context())

    assert result1 == result2, f"invocazioni non identiche:\n{result1}\n---\n{result2}"
    assert result1.answer == result2.answer
    assert [n.node_id for n in result1.nodes] == [n.node_id for n in result2.nodes]


# ============================================================
# SPEC-018 §3 scenario 3 — nessun risultato: nessuna chiamata a
# vllm-fake, nessuna sezione Fonti vuota senza spiegazione.
# ============================================================


def test_scenario3_no_results_no_llm_call(retrieval_engine_addr, vllm_fake_url):
    # Nessun nodo del grafo di test contiene questo termine nel nome —
    # zero match full-text per costruzione (verificato empiricamente
    # prima di scrivere questo test, non assunto).
    result = run_ask(
        "zzzznonexistentqueryterm12345", retrieval_engine_addr, vllm_fake_url, authenticated_context()
    )

    assert result.nodes == []
    assert result.answer is None  # run_ask ritorna PRIMA di chiamare vllm-fake, per costruzione


# ============================================================
# SPEC-018 §3 scenario 4 — retrieval-engine irraggiungibile.
# ============================================================


def test_scenario4_retrieval_engine_unreachable(vllm_fake_url):
    with pytest.raises(RetrievalUnavailableError) as excinfo:
        run_ask("query qualunque", UNREACHABLE_GRPC_ADDR, vllm_fake_url, authenticated_context())

    assert UNREACHABLE_GRPC_ADDR in str(excinfo.value)


# ============================================================
# SPEC-018 §3 scenario 5 — vllm-fake irraggiungibile, retrieval-engine
# raggiungibile CON risultati: le Fonti devono essere già disponibili
# nell'eccezione.
# ============================================================


def test_scenario5_llm_unreachable_sources_still_available(retrieval_engine_addr):
    with pytest.raises(LLMUnavailableError) as excinfo:
        run_ask("Chi chiama Validate?", retrieval_engine_addr, UNREACHABLE_HTTP_URL, authenticated_context())

    err = excinfo.value
    assert UNREACHABLE_HTTP_URL in str(err)
    assert err.sources, "le Fonti devono essere popolate: retrieval-engine ha già risposto con successo"
    source_ids = {n.node_id for n in err.sources}
    assert PROCESS_ID in source_ids


# ============================================================
# Verifica dedicata del meccanismo dell'euristica "chi chiama"
# (SPEC-018 §2), non solo che Process compaia nel risultato finale: la
# REVERSE su CALLS da Validate trova Process; una direzione sbagliata
# (FORWARD, "chi Validate chiama") non troverebbe nulla, dato che
# Validate non ha archi CALLS uscenti nel grafo di test — l'asimmetria è
# deliberata (vedi conftest.py).
# ============================================================


def test_find_callers_uses_reverse_direction_on_calls(retrieval_engine_addr):
    security_context = authenticated_context()
    callers = find_callers(retrieval_engine_addr, security_context, VALIDATE_ID)
    caller_ids = {n.node_id for n in callers}
    assert caller_ids == {PROCESS_ID}, f"find_callers(Validate) = {caller_ids}, want {{Process}}"


def test_forward_expand_from_validate_finds_no_callers(retrieval_engine_addr):
    """Sanity check dell'asimmetria del grafo di test: se questo test
    fallisse (Process trovato anche in FORWARD), il test sopra non
    proverebbe che find_callers usa REVERSE — proverebbe solo che TROVA
    Process, per qualunque motivo. Verificato usando ExpandNeighbors
    direttamente (non find_callers, che è cablato su REVERSE per
    definizione)."""
    security_context = authenticated_context()
    channel = grpc.intercept_channel(
        grpc.insecure_channel(retrieval_engine_addr), SecurityContextInterceptor(security_context)
    )
    try:
        stub = retrieval_pb2_grpc.RetrievalEngineStub(channel)
        response = stub.ExpandNeighbors(
            retrieval_pb2.ExpandNeighborsRequest(
                security_context=security_context,
                node_id=VALIDATE_ID,
                edge_types=[retrieval_pb2.EDGE_TYPE_CALLS],
                direction=retrieval_pb2.TRAVERSAL_DIRECTION_FORWARD,
            )
        )
    finally:
        channel.close()
    assert list(response.neighbors) == []


def test_hybrid_search_alone_does_not_find_process_for_who_calls_query(retrieval_engine_addr):
    """Prova diretta del motivo per cui l'euristica esiste (SPEC-018 §2):
    la sola HybridSearch (full-text sul nome) sulla query letterale non
    trova Process — verificato qui contro retrieval-engine reale, non
    solo dedotto dal comportamento Lucene osservato manualmente."""
    security_context = authenticated_context()
    nodes = hybrid_search(retrieval_engine_addr, "Chi chiama Validate?", security_context)
    node_ids = {n.node_id for n in nodes}
    assert PROCESS_ID not in node_ids


# ============================================================
# Unit test della sola euristica di rilevamento frase (nessuna rete).
# ============================================================


@pytest.mark.parametrize(
    "query_text",
    ["Chi chiama Validate?", "chi chiama Process", "Who calls computeTotal?", "chiamanti di Validate"],
)
def test_is_who_calls_query_true(query_text):
    assert is_who_calls_query(query_text) is True


@pytest.mark.parametrize("query_text", ["Validate", "cosa fa Process?", "computeTotal"])
def test_is_who_calls_query_false(query_text):
    assert is_who_calls_query(query_text) is False


# ============================================================
# Verifiche di supporto: costruzione del prompt (SPEC-018 §2) e client
# HTTP verso vllm-fake, senza passare per run_ask.
# ============================================================


def test_prompt_uses_only_last_user_message_shape(retrieval_engine_addr, vllm_fake_url):
    security_context = authenticated_context()
    nodes = hybrid_search(retrieval_engine_addr, "computeTotal", security_context)
    assert nodes, "precondizione: computeTotal deve essere trovato per nome"

    messages = build_messages("Cosa fa computeTotal?", nodes)
    assert messages[0] == {"role": "system", "content": SYSTEM_MESSAGE}
    assert messages[1]["role"] == "user"
    assert "Cosa fa computeTotal?" in messages[1]["content"]
    assert "computeTotal" in messages[1]["content"]

    answer = chat_completion(vllm_fake_url, messages)
    # vllm-fake echeggia SOLO l'ultimo messaggio user (SPEC-017 §3
    # scenario 4): il contenuto del prompt (compreso il nome del nodo)
    # deve comparire letteralmente nella risposta.
    assert "computeTotal" in answer


def test_compute_total_node_exists_in_seed_graph(retrieval_engine_addr):
    # Verifica minima che il grafo di test contenga davvero il nodo
    # referenziato altrove (test_prompt_uses_only_last_user_message_shape) —
    # se questo fallisse, quel test fallirebbe per un motivo di setup,
    # non di logica.
    security_context = authenticated_context()
    nodes = hybrid_search(retrieval_engine_addr, "computeTotal", security_context)
    assert any(n.node_id == COMPUTE_ID for n in nodes)


# ============================================================
# `eci ask` come comando REALMENTE installato (SPEC-018 §9), non solo
# `run_ask` richiamata come funzione Python.
# ============================================================


def test_eci_command_is_really_installed():
    # sys.executable è .venv/bin/python: il binario `eci` di
    # [project.scripts] vive nella STESSA directory bin/ del venv, a
    # prescindere che quel venv sia "attivato" (PATH) o meno — non ci si
    # affida al PATH del processo che esegue pytest.
    eci_bin = Path(sys.executable).parent / "eci"
    assert eci_bin.is_file(), f"{eci_bin} non trovato: 'eci' non risulta installato come comando reale"

    result = subprocess.run([str(eci_bin), "ask"], capture_output=True, text=True, check=False)
    assert result.returncode == 2
    assert "query" in result.stderr.lower() or "uso" in result.stderr.lower()


def test_eci_command_empty_query_via_subprocess():
    eci_bin = Path(sys.executable).parent / "eci"
    result = subprocess.run([str(eci_bin), "ask", ""], capture_output=True, text=True, check=False)
    assert result.returncode == 2
    assert "vuota" in result.stderr.lower()


def test_eci_command_fails_closed_without_gateway_authentication():
    """The historical CLI dev identity was removed by T6.2.

    T6.6 will provide the authenticated browser/API entrypoint; until then a
    direct process cannot invent a tenant or repository scope.
    """
    eci_bin = Path(sys.executable).parent / "eci"
    result = subprocess.run(
        [str(eci_bin), "ask", "Chi chiama Validate?"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert "authenticated securitycontext required" in result.stderr.lower()
    assert result.stdout == ""
