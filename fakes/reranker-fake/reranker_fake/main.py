"""reranker-fake (SPEC-044, T4.4): servizio HTTP minimale che replica
l'API **nativa** di TEI per un cross-encoder di reranking
(`bge-reranker-v2-m3`) — non un giudizio di rilevanza reale, un punteggio
**deterministico** riproducibile per i test (stesso principio di
`embedder-fake`, SPEC-023 §2).

``POST /rerank {"query": "...", "texts": ["...", ...]}``
``-> [{"index": int, "score": float}, ...]`` ordinato per score
decrescente (stesso comportamento del vero endpoint TEI /rerank).

Determinismo (§2): ``SHA-256(query + "\\x00" + text)``, i primi 4 byte
big-endian normalizzati in ``[0, 1]`` (`score / 0xFFFFFFFF`) — stessa
tecnica di `embedder-fake` (§2), qui senza l'estensione per-dimensione
perché serve un solo float, non un vettore a 1536 componenti. Stessa
query+testo -> stesso punteggio, sempre; coppie diverse -> punteggi
diversi (proprietà standard di SHA-256, non lo scopo di questo fake
dimostrarla formalmente).
"""

import hashlib
import json

from eci_core.config import env_or_default
from eci_core.observability import init_tracing
from fastapi import FastAPI, HTTPException, Request

app = FastAPI()

# Bootstrap OTel di base per coerenza con gli altri servizi Python (stesso
# principio di embedder-fake/vllm-fake) — nessuna gestione esplicita dello
# shutdown, il processo è normalmente terminato con kill/Ctrl-C.
init_tracing(env_or_default("RERANKER_FAKE_SERVICE_NAME", "reranker-fake"))

_MAX_UINT32 = 0xFFFFFFFF


def _deterministic_score(query: str, text: str) -> float:
    digest = hashlib.sha256((query + "\x00" + text).encode("utf-8")).digest()
    as_uint32 = int.from_bytes(digest[:4], "big")
    return as_uint32 / _MAX_UINT32


@app.post("/rerank")
async def rerank(request: Request) -> list[dict]:
    # Parsing manuale del body (non un parametro Pydantic tipizzato): come
    # in embedder-fake (SPEC-023 §4) e vllm-fake (SPEC-017 §4), un
    # controllo diretto sui casi di errore invece del 422 automatico di
    # FastAPI — 400 per coerenza con gli altri fake.
    raw_body = await request.body()
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="corpo della richiesta non e' JSON valido")

    if not isinstance(payload, dict) or "query" not in payload or "texts" not in payload:
        raise HTTPException(status_code=400, detail="campi 'query'/'texts' assenti")

    query = payload["query"]
    texts = payload["texts"]
    if not isinstance(query, str):
        raise HTTPException(status_code=400, detail="'query' deve essere una stringa")
    if not isinstance(texts, list) or not all(isinstance(t, str) for t in texts):
        raise HTTPException(status_code=400, detail="'texts' deve essere una lista di stringhe")

    results = [{"index": i, "score": _deterministic_score(query, t)} for i, t in enumerate(texts)]
    results.sort(key=lambda r: r["score"], reverse=True)
    return results
