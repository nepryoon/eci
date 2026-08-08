"""SPEC-023 §3/§4/§7 — test HTTP diretti (nessun testcontainers: il
servizio stesso è l'oggetto sotto test, nessuna dipendenza esterna), stesso
pattern di fakes/vllm-fake/vllm_fake/test_main.py (SPEC-017).
"""

import hashlib

from fastapi.testclient import TestClient

from embedder_fake.main import EMBEDDING_DIM, app

client = TestClient(app)


def embed(inputs) -> list[list[float]]:
    resp = client.post("/embed", json={"inputs": inputs})
    resp.raise_for_status()
    return resp.json()


# SPEC-023 §3 scenario 1.
def test_scenario1_same_text_twice_is_byte_identical():
    resp1 = embed("Chi chiama Validate?")
    resp2 = embed("Chi chiama Validate?")
    assert resp1 == resp2


# SPEC-023 §3 scenario 2.
def test_scenario2_different_texts_produce_different_vectors():
    resp1 = embed("prima funzione")
    resp2 = embed("seconda funzione")
    assert resp1 != resp2


# SPEC-023 §3 scenario 3.
def test_scenario3_vector_length_is_exactly_1536():
    resp = embed("una funzione qualunque")
    assert len(resp) == 1
    assert len(resp[0]) == 1536 == EMBEDDING_DIM


def test_multiple_inputs_same_order_same_count():
    resp = embed(["alpha", "beta", "gamma"])
    assert len(resp) == 3
    for vec in resp:
        assert len(vec) == EMBEDDING_DIM
    # Testi diversi -> vettori diversi anche all'interno della stessa
    # chiamata; stesso testo isolato -> stesso vettore della chiamata
    # batch (determinismo per-testo, non per-posizione).
    assert resp[0] != resp[1] != resp[2]
    assert embed("alpha") == [resp[0]]


def test_values_are_normalized_in_minus_one_one_range():
    resp = embed("controllo range")
    vec = resp[0]
    assert all(-1.0 <= v <= 1.0 for v in vec)
    # Non tutti zero/costanti: verifica minima che la normalizzazione
    # produca varietà reale, non un valore degenerato ripetuto.
    assert len(set(vec)) > 1


def test_vector_matches_manual_sha256_extension_reference():
    """Verifica diretta della formula di §2 (non solo che *qualche*
    vettore deterministico venga fuori): ricalcola la stessa estensione
    SHA-256 qui, indipendentemente dall'implementazione, e confronta."""
    text = "riferimento manuale"
    needed = EMBEDDING_DIM * 4
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    material = digest
    while len(material) < needed:
        digest = hashlib.sha256(digest).digest()
        material += digest
    material = material[:needed]
    expected = [
        (int.from_bytes(material[i * 4 : (i + 1) * 4], "big") / 0xFFFFFFFF) * 2 - 1
        for i in range(EMBEDDING_DIM)
    ]

    resp = embed(text)
    assert resp[0] == expected


# ============================================================
# SPEC-023 §4 — edge case, verificati esplicitamente.
# ============================================================


def test_edge_case_empty_text_is_not_special_cased():
    resp = embed("")
    assert len(resp[0]) == EMBEDDING_DIM
    # SHA-256("") è un valore ben definito e stabile: stesso risultato a
    # ogni chiamata, come qualunque altro input.
    assert embed("") == resp


def test_edge_case_inputs_field_absent_returns_400():
    resp = client.post("/embed", json={})
    assert resp.status_code == 400


def test_edge_case_inputs_wrong_type_returns_400():
    resp = client.post("/embed", json={"inputs": 42})
    assert resp.status_code == 400


def test_edge_case_invalid_json_body_returns_400():
    resp = client.post(
        "/embed",
        content=b"{questo non e' json valido",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400
