"""Client HTTP verso vllm-fake (SPEC-018 §2): POST /v1/chat/completions,
stesso shape OpenAI-compatible di SPEC-017."""

import httpx

_HTTP_TIMEOUT_SECONDS = 10.0


def chat_completion(base_url: str, messages: list[dict]) -> str:
    """Ritorna `choices[0].message.content`. Solleva `httpx.HTTPError` su
    qualunque fallimento di connessione/risposta — il chiamante
    (orchestrator.ask) lo traduce in LLMUnavailableError, arricchito con
    le fonti già recuperate (SPEC-018 §3 scenario 5): questo modulo non
    conosce le fonti, non è sua responsabilità costruire quell'errore."""
    endpoint = base_url.rstrip("/") + "/v1/chat/completions"
    response = httpx.post(
        endpoint,
        json={"model": "vllm-fake", "messages": messages},
        timeout=_HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]
