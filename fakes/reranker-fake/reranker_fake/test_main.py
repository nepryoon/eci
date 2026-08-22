"""SPEC-044 §3/§4/§7 — test HTTP diretti (nessun testcontainers: il
servizio stesso è l'oggetto sotto test, nessuna dipendenza esterna), stesso
pattern di fakes/embedder-fake/embedder_fake/test_main.py (SPEC-023).
"""

import hashlib

from fastapi.testclient import TestClient

from reranker_fake.main import app

client = TestClient(app)


def rerank(query, texts) -> list[dict]:
    resp = client.post("/rerank", json={"query": query, "texts": texts})
    resp.raise_for_status()
    return resp.json()


def test_same_query_and_text_twice_is_identical():
    resp1 = rerank("Chi chiama Validate?", ["testo A"])
    resp2 = rerank("Chi chiama Validate?", ["testo A"])
    assert resp1 == resp2


def test_different_texts_produce_different_scores():
    resp = rerank("query", ["testo alpha", "testo beta"])
    scores = {r["index"]: r["score"] for r in resp}
    assert scores[0] != scores[1]


def test_different_queries_produce_different_scores_for_same_text():
    resp1 = rerank("query uno", ["stesso testo"])
    resp2 = rerank("query due", ["stesso testo"])
    assert resp1[0]["score"] != resp2[0]["score"]


def test_response_has_one_entry_per_text_with_valid_index():
    texts = ["a", "b", "c"]
    resp = rerank("query", texts)
    assert len(resp) == len(texts)
    assert {r["index"] for r in resp} == {0, 1, 2}


def test_scores_are_normalized_in_zero_one_range():
    resp = rerank("query", ["testo1", "testo2", "testo3"])
    assert all(0.0 <= r["score"] <= 1.0 for r in resp)


def test_response_sorted_by_score_descending():
    resp = rerank("query", ["a", "b", "c", "d", "e"])
    scores = [r["score"] for r in resp]
    assert scores == sorted(scores, reverse=True)


def test_score_matches_manual_sha256_reference():
    """Verifica diretta della formula di §2 (non solo che *qualche*
    punteggio deterministico venga fuori): ricalcola qui lo stesso
    SHA-256(query + '\\x00' + text), indipendentemente
    dall'implementazione, e confronta."""
    query, text = "query di riferimento", "testo di riferimento"
    digest = hashlib.sha256((query + "\x00" + text).encode("utf-8")).digest()
    expected = int.from_bytes(digest[:4], "big") / 0xFFFFFFFF

    resp = rerank(query, [text])
    assert resp[0]["score"] == expected


# ============================================================
# SPEC-044 §4 — edge case, verificati esplicitamente.
# ============================================================


def test_edge_case_empty_texts_list_returns_empty_result():
    resp = rerank("query", [])
    assert resp == []


def test_edge_case_empty_text_is_not_special_cased():
    resp = rerank("query", [""])
    assert len(resp) == 1
    assert resp == rerank("query", [""])


def test_edge_case_query_field_absent_returns_400():
    resp = client.post("/rerank", json={"texts": ["a"]})
    assert resp.status_code == 400


def test_edge_case_texts_field_absent_returns_400():
    resp = client.post("/rerank", json={"query": "q"})
    assert resp.status_code == 400


def test_edge_case_texts_wrong_type_returns_400():
    resp = client.post("/rerank", json={"query": "q", "texts": "not a list"})
    assert resp.status_code == 400


def test_edge_case_invalid_json_body_returns_400():
    resp = client.post(
        "/rerank",
        content=b"{questo non e' json valido",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400
