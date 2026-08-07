"""SPEC-019 §3/§4/§7/§9 — verifica delle 10 query di
`tests/golden/queries_v0.json` contro la pipeline reale (fixture
session-scoped in `conftest.py`). 9 verificate con asserzione esatta
sull'insieme di nomi atteso (g01-g04, g06-g10); g05 esplicitamente
`pytest.skip`-pata con motivazione (nessuna omissione silenziosa). I
due edge case di §4 sono verificati con un caso concreto (non solo
descritti): un timeout CDC reale su un nodo che non comparirà mai, e
una categoria di `expected_facts` sintetica non riconosciuta."""

import json
import re
from pathlib import Path

import pytest
from orchestrator.ask import run_ask

import harness

REPO_ROOT = Path(__file__).resolve().parents[2]
GOLDEN_PATH = REPO_ROOT / "tests" / "golden" / "queries_v0.json"

# File del fixture SPEC-009 in cui vive ciascuna entry verificata via
# CONTAINS/node_type — non parte del contratto golden dataset stesso
# (che non nomina il file esplicitamente, solo nello scope_note in
# prosa), è wiring di questo test contro la struttura nota del fixture.
FILE_FOR_ENTRY = {
    "g04": "order_service.go",
    "g06": "order_service.go",
    "g07": "notifier.go",
    "g08": "util.go",
    "g09": "main.go",
}
G10_FILE = "order_service.go"
G10_QUALIFIED_NAME = "OrderService"

VERIFIABLE_IDS = ["g01", "g02", "g03", "g04", "g06", "g07", "g08", "g09", "g10"]


def _load_golden() -> list[dict]:
    return json.loads(GOLDEN_PATH.read_text())


@pytest.fixture(scope="session")
def golden_dataset():
    return _load_golden()


def _entry(golden_dataset, entry_id: str) -> dict:
    return next(e for e in golden_dataset if e["id"] == entry_id)


def _simple_name(qualified: str) -> str:
    return qualified.rsplit(".", 1)[-1]


def _expected_caller_names(query_text: str, callers: list[str]) -> set[str]:
    """SPEC-019 §2: {entità cercata} ∪ {nome del chiamante per ciascuna
    stringa "X <- Y" in callers, prendendo solo la parte dopo l'ultimo
    punto}. L'entità cercata è estratta dalla query stessa (necessario
    per g02/g03, dove `callers` è vuota e non offre nessuna X da cui
    derivarla)."""
    match = re.search(r"chiama\s+(.+)", query_text, re.IGNORECASE)
    assert match, f"query {query_text!r} non nel formato 'chi chiama <entità>?' atteso da questo helper"
    expected = {match.group(1).rstrip("?!.,;:")}
    for pair in callers:
        _, sep, caller_side = pair.partition(" <- ")
        assert sep, f"formato inatteso in callers: {pair!r} (atteso 'X <- Y')"
        expected.add(_simple_name(caller_side))
    return expected


def _verify_callers(entry: dict, stack: dict) -> None:
    result = run_ask(entry["query"], stack["retrieval_addr"], stack["vllm_url"])
    got = {n.name for n in result.nodes}
    expected = _expected_caller_names(entry["query"], entry["expected_facts"]["callers"])
    assert got == expected, f"{entry['id']}: nomi in Fonti = {got}, atteso {expected}"


def _verify_contains(entry: dict, stack: dict, filename: str) -> None:
    file_id = harness.file_node_id(filename)
    neighbors = harness.expand_contains(stack["retrieval_addr"], stack["security_context"], file_id)
    got = {n.name for n in neighbors}
    # SPEC-019 §2: confronto sempre per nome semplice — il golden dataset
    # elenca i Method con nome qualificato ("OrderService.Process"), ma
    # RetrievedNode.name espone solo il nome semplice ("Process").
    expected = {_simple_name(n) for n in entry["expected_facts"]["contains"]}
    assert got == expected, f"{entry['id']}: CONTAINS di {filename} = {got}, atteso {expected}"


def _verify_methods(entry: dict, stack: dict, filename: str) -> None:
    file_id = harness.file_node_id(filename)
    neighbors = harness.expand_contains(stack["retrieval_addr"], stack["security_context"], file_id)
    got = {n.name for n in neighbors if n.node_type == "Method"}
    expected = {_simple_name(n) for n in entry["expected_facts"]["methods"]}
    assert got == expected, f"{entry['id']}: metodi (CONTAINS filtrato node_type=Method) di {filename} = {got}, atteso {expected}"


def _verify_node_type(entry: dict, stack: dict) -> None:
    target_id = harness.node_id(G10_FILE, G10_QUALIFIED_NAME)
    node = harness.get_node(stack["retrieval_addr"], stack["security_context"], target_id)
    expected = entry["expected_facts"]["node_type"]
    assert node.node_type == expected, f"{entry['id']}: node_type di {G10_QUALIFIED_NAME} = {node.node_type!r}, atteso {expected!r}"


@pytest.mark.parametrize("entry_id", VERIFIABLE_IDS)
def test_golden_entry(entry_id, stack, golden_dataset):
    """Scenari 1-6 di SPEC-019 §3: le 9 query verificabili, ciascuna con
    un'asserzione esatta sull'insieme di nomi atteso — dispatch per
    categoria via `harness.classify_category` (SPEC-019 §2)."""
    entry = _entry(golden_dataset, entry_id)
    category = harness.classify_category(entry["expected_facts"])

    if category == "callers":
        _verify_callers(entry, stack)
    elif category == "contains":
        _verify_contains(entry, stack, FILE_FOR_ENTRY[entry_id])
    elif category == "methods":
        _verify_methods(entry, stack, FILE_FOR_ENTRY[entry_id])
    elif category == "node_type":
        _verify_node_type(entry, stack)
    else:
        pytest.fail(
            f"{entry_id}: categoria {category!r} non gestita da questo dispatch "
            f"(le uniche categorie verificate qui sono callers/contains/methods/node_type; "
            f"'implementations' è g05, gestita separatamente da "
            f"test_g05_implementations_explicitly_not_verified)"
        )


def test_g05_implementations_explicitly_not_verified(golden_dataset):
    """Scenario 7 di SPEC-019 §3: g05 segnalata esplicitamente come non
    verificata, con motivazione — `pytest.skip(reason=...)`, non
    un'assenza silenziosa dal report finale (SPEC-019 §5/§9)."""
    entry = _entry(golden_dataset, "g05")
    assert harness.classify_category(entry["expected_facts"]) == "implementations"
    pytest.skip(
        reason=(
            "g05 (implementations) non verificato in questa SPEC (SPEC-019 §2/§5): "
            "nessun arco IMPLEMENTS esiste nella pipeline reale — T1.1 (services/ingestion) "
            "non implementa il rilevamento della soddisfazione implicita di interfaccia di Go "
            "(richiede analisi semantica dei tipi, non parsing sintattico). Limite noto e "
            "dichiarato, da riaprire quando la risoluzione di tipo arriverà (probabilmente Fase 2)."
        )
    )


def test_golden_dataset_verification_coverage(golden_dataset):
    """Scenario 8 di SPEC-019 §3: il conteggio 9/10 esplicito nel
    report, non "10/10" ottenuto facendo passare g05 con un'asserzione
    vuota."""
    assert len(golden_dataset) == 10
    verified_ids = [e["id"] for e in golden_dataset if harness.classify_category(e["expected_facts"]) != "implementations"]
    not_verified_ids = [e["id"] for e in golden_dataset if harness.classify_category(e["expected_facts"]) == "implementations"]
    assert sorted(verified_ids) == sorted(VERIFIABLE_IDS)
    assert not_verified_ids == ["g05"]
    print(
        f"\nSPEC-019 copertura golden dataset: {len(verified_ids)}/10 query verificate con "
        f"asserzione reale ({sorted(verified_ids)}), 1/10 esplicitamente non verificata (g05, "
        f"vedi test_g05_implementations_explicitly_not_verified)."
    )


# ============================================================
# SPEC-019 §4 — edge case, verificati con un caso concreto (non solo
# descritti).
# ============================================================


def test_edge_case_cdc_timeout_explicit_failure(stack):
    """Riga 1 della tabella §4: un nodo che non comparirà mai in Neo4j
    (id sintetico, mai prodotto da nessuna ingestion) deve produrre un
    fallimento esplicito con messaggio chiaro su QUALE nodo non è
    comparso — non un timeout generico né un proseguimento silenzioso
    su stato parziale. Timeout breve (3s) deliberato: qui si verifica
    il comportamento di fallimento stesso, non la pazienza dell'attesa
    reale usata dal setup (60s, vedi conftest.py CDC_WAIT_TIMEOUT_SECONDS)."""
    bogus_node_id = "e2e-edge-case-node-that-will-never-exist-in-neo4j"
    with pytest.raises(TimeoutError) as exc_info:
        harness.wait_for_node(
            stack["retrieval_addr"], stack["security_context"], bogus_node_id, timeout=3.0, poll_interval=0.5
        )
    message = str(exc_info.value)
    assert bogus_node_id in message, f"messaggio di errore non nomina il nodo mancante: {message!r}"
    assert "CDC" in message, f"messaggio di errore non spiega che si tratta di un'attesa CDC: {message!r}"


def test_edge_case_unrecognized_expected_facts_category_hard_fails():
    """Riga 2 della tabella §4: un'entry con una categoria di
    expected_facts non tra le cinque note deve far fallire esplicitamente
    la classificazione (usata da `test_golden_entry` per il dispatch),
    non produrre uno skip silenzioso — un caso non gestito è un errore
    da correggere, diverso da g05 (gestito e dichiarato esplicitamente)."""
    synthetic_bad_entry = {"unknown_category_never_declared_in_spec_009": ["qualunque valore"]}
    with pytest.raises(ValueError, match="non riconosciut"):
        harness.classify_category(synthetic_bad_entry)
