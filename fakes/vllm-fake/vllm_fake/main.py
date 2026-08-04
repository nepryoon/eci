"""vllm-fake (SPEC-017, T0.9 scope rivisto): servizio HTTP minimale,
compatibile con l'API OpenAI Chat Completions, con risposte
DETERMINISTICHE — stesso prompt produce sempre la stessa risposta, byte
per byte. Necessario per T1.5 (orchestrator v0, chiama un LLM per la
risposta finale) e T1.6 (test E2E, deve asserire risultati esatti senza
il non-determinismo di un LLM reale).

Meccanismo deliberatamente semplice (§2): la risposta è l'eco letterale
dell'ultimo messaggio ``user``, prefissata da un marcatore — non un
tentativo di sintesi. Se il prompt di un chiamante incorpora nomi/id nel
messaggio utente, quegli stessi nomi/id compaiono letteralmente nella
risposta, verificabile con un controllo di sottostringa da chi chiama.
"""

import hashlib
import json

from eci_core.config import env_or_default
from eci_core.observability import init_tracing
from fastapi import FastAPI, HTTPException, Request

RESPONSE_MARKER = "[vllm-fake risposta deterministica]"

app = FastAPI()

# Bootstrap OTel di base per coerenza con gli altri servizi Python (SPEC-017
# §8) — non un requisito critico qui (fake di sviluppo), quindi nessuna
# gestione esplicita dello shutdown del provider: il processo è di norma
# terminato con un semplice kill/Ctrl-C, non un ciclo di vita gestito.
init_tracing(env_or_default("VLLM_FAKE_SERVICE_NAME", "vllm-fake"))


def _word_count(text: str) -> int:
    """Conteggio parole per split su spazi (§2: "non vera tokenizzazione
    — approssimazione dichiarata")."""
    return len(text.split())


@app.post("/v1/chat/completions")
async def chat_completions(request: Request) -> dict:
    # Parsing manuale del body (non un parametro Pydantic tipizzato):
    # FastAPI risponderebbe 422 sui fallimenti di validazione automatica,
    # ma SPEC-017 §4 richiede esplicitamente 400 sui tre casi elencati —
    # controllo diretto, non affidato al comportamento di default del
    # framework.
    raw_body = await request.body()
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="corpo della richiesta non e' JSON valido")

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="corpo della richiesta deve essere un oggetto JSON")

    messages = payload.get("messages")
    if not messages or not isinstance(messages, list):
        raise HTTPException(status_code=400, detail="campo 'messages' assente o vuoto")

    user_messages = [m for m in messages if isinstance(m, dict) and m.get("role") == "user"]
    if not user_messages:
        raise HTTPException(status_code=400, detail="nessun messaggio con role='user' nella richiesta")

    contents = [str(m.get("content", "")) for m in messages if isinstance(m, dict)]
    last_user_content = str(user_messages[-1].get("content", ""))

    # id: sha256 della concatenazione di TUTTI i messages[].content (§2) —
    # stesso input -> stesso id, input diverso -> id diverso (scenari 2/3).
    concatenated = "".join(contents)
    response_id = "fake-" + hashlib.sha256(concatenated.encode()).hexdigest()[:16]

    response_content = f"{RESPONSE_MARKER} {last_user_content}"

    prompt_tokens = sum(_word_count(c) for c in contents)
    completion_tokens = _word_count(response_content)

    return {
        "id": response_id,
        "object": "chat.completion",
        "model": "vllm-fake",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": response_content},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }
