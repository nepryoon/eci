"""SPEC-017 §3/§4/§7 — test HTTP diretti (nessun testcontainers: il
servizio stesso è l'oggetto sotto test, nessuna dipendenza esterna).
"""

import hashlib

from fastapi.testclient import TestClient

from vllm_fake.main import app

client = TestClient(app)

MARKER = "[vllm-fake risposta deterministica]"


def chat(messages: list[dict], **extra) -> dict:
    body = {"model": "vllm-fake", "messages": messages, **extra}
    resp = client.post("/v1/chat/completions", json=body)
    resp.raise_for_status()
    return resp.json()


# SPEC-017 §3 scenario 1.
def test_scenario1_literal_echo_with_marker():
    resp = chat([{"role": "user", "content": "Chi chiama Validate?"}])
    content = resp["choices"][0]["message"]["content"]
    assert content == f"{MARKER} Chi chiama Validate?"


# SPEC-017 §3 scenario 2.
def test_scenario2_same_request_twice_is_byte_identical():
    messages = [{"role": "user", "content": "stesso prompt"}]
    resp1 = chat(messages)
    resp2 = chat(messages)
    assert resp1 == resp2
    assert resp1["id"] == resp2["id"]


# SPEC-017 §3 scenario 3.
def test_scenario3_different_content_different_id_and_content():
    resp1 = chat([{"role": "user", "content": "prima domanda"}])
    resp2 = chat([{"role": "user", "content": "seconda domanda"}])
    assert resp1["id"] != resp2["id"]
    assert resp1["choices"][0]["message"]["content"] != resp2["choices"][0]["message"]["content"]
    assert "prima domanda" in resp1["choices"][0]["message"]["content"]
    assert "seconda domanda" in resp2["choices"][0]["message"]["content"]


# SPEC-017 §3 scenario 4.
def test_scenario4_echoes_only_last_user_message():
    resp = chat(
        [
            {"role": "system", "content": "sei un assistente"},
            {"role": "user", "content": "primo turno utente"},
            {"role": "assistant", "content": "risposta precedente"},
            {"role": "user", "content": "ultimo turno utente"},
        ]
    )
    content = resp["choices"][0]["message"]["content"]
    assert content == f"{MARKER} ultimo turno utente"
    assert "primo turno utente" not in content
    assert "risposta precedente" not in content


# SPEC-017 §3 scenario 5.
def test_scenario5_unknown_extra_fields_ignored():
    resp = chat(
        [{"role": "user", "content": "campo extra"}],
        temperature=0.7,
        top_p=0.9,
        stream=False,
        some_totally_unknown_field={"nested": True},
    )
    content = resp["choices"][0]["message"]["content"]
    assert content == f"{MARKER} campo extra"


# Forma della risposta (SPEC-017 §2), verificata una volta esplicitamente
# oltre ai controlli di contenuto sparsi negli scenari sopra.
def test_response_shape_matches_spec():
    resp = chat([{"role": "user", "content": "forma della risposta"}])
    assert resp["object"] == "chat.completion"
    assert resp["model"] == "vllm-fake"
    assert resp["id"].startswith("fake-")
    assert len(resp["id"]) == len("fake-") + 16
    choice = resp["choices"][0]
    assert choice["index"] == 0
    assert choice["message"]["role"] == "assistant"
    assert choice["finish_reason"] == "stop"
    usage = resp["usage"]
    assert usage["total_tokens"] == usage["prompt_tokens"] + usage["completion_tokens"]
    assert usage["prompt_tokens"] > 0
    assert usage["completion_tokens"] > 0


def test_id_is_sha256_of_concatenated_message_contents():
    messages = [
        {"role": "system", "content": "sistema"},
        {"role": "user", "content": "utente"},
    ]
    resp = chat(messages)
    want_hash = hashlib.sha256(b"sistemautente").hexdigest()[:16]
    assert resp["id"] == f"fake-{want_hash}"


# ============================================================
# SPEC-017 §4 — edge case, verificati esplicitamente (non solo gli
# scenari numerati di §3).
# ============================================================


def test_edge_case_no_user_message_returns_400():
    resp = client.post(
        "/v1/chat/completions",
        json={"model": "vllm-fake", "messages": [{"role": "system", "content": "solo sistema"}]},
    )
    assert resp.status_code == 400


def test_edge_case_messages_absent_returns_400():
    resp = client.post("/v1/chat/completions", json={"model": "vllm-fake"})
    assert resp.status_code == 400


def test_edge_case_messages_empty_returns_400():
    resp = client.post("/v1/chat/completions", json={"model": "vllm-fake", "messages": []})
    assert resp.status_code == 400


def test_edge_case_invalid_json_body_returns_400():
    resp = client.post(
        "/v1/chat/completions",
        content=b"{questo non e' json valido",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400
