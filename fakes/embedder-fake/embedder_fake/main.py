"""embedder-fake (SPEC-023, T2.4): servizio HTTP minimale che replica
l'API **nativa** di TEI (Text Embeddings Inference) — non quella
OpenAI-compatible opzionale — per testare un client di embedding senza
dipendere da una GPU.

``POST /embed {"inputs": "testo" | ["testo1", "testo2", ...]}``
``-> [[float, ...], ...]`` — un vettore di 1536 elementi per ogni input,
nello stesso ordine (§2).
``GET /health -> {"status": "ok"}`` replica il probe nativo TEI senza
eseguire inferenza.

Determinismo (§2): ``SHA-256(testo)`` esteso via ri-hashing concatenato
(``h0 = SHA256(testo)``, ``h1 = SHA256(h0)``, ``h2 = SHA256(h1)``, ...)
finché non si hanno almeno ``1536 * 4`` byte; ogni gruppo di 4 byte
big-endian diventa un intero unsigned a 32 bit, normalizzato in
``[-1, 1]``. Stesso testo -> stesso vettore, sempre; testi diversi ->
vettori diversi (proprietà standard di SHA-256, non lo scopo di questo
fake dimostrarla formalmente). 1536 dimensioni fisse — nessun supporto
Matryoshka/troncamento (§2, non lo scopo del fake).
"""

import hashlib
import json

from eci_core.config import env_or_default
from eci_core.observability import init_tracing
from fastapi import FastAPI, HTTPException, Request

EMBEDDING_DIM = 1536
_BYTES_PER_FLOAT = 4
_MAX_UINT32 = 0xFFFFFFFF

app = FastAPI()

# Bootstrap OTel di base per coerenza con gli altri servizi Python (stesso
# principio di fakes/vllm-fake, SPEC-017 §8) — nessuna gestione esplicita
# dello shutdown, il processo è normalmente terminato con kill/Ctrl-C.
init_tracing(env_or_default("EMBEDDER_FAKE_SERVICE_NAME", "embedder-fake"))


@app.get("/health")
async def health() -> dict[str, str]:
    """Replicate TEI's native, non-inference health endpoint."""
    return {"status": "ok"}


def _deterministic_vector(text: str) -> list[float]:
    needed_bytes = EMBEDDING_DIM * _BYTES_PER_FLOAT
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    material = digest
    while len(material) < needed_bytes:
        digest = hashlib.sha256(digest).digest()
        material += digest
    material = material[:needed_bytes]

    vector = []
    for i in range(EMBEDDING_DIM):
        chunk = material[i * _BYTES_PER_FLOAT : (i + 1) * _BYTES_PER_FLOAT]
        as_uint32 = int.from_bytes(chunk, "big")
        normalized = (as_uint32 / _MAX_UINT32) * 2 - 1
        vector.append(normalized)
    return vector


@app.post("/embed")
async def embed(request: Request) -> list[list[float]]:
    # Parsing manuale del body (non un parametro Pydantic tipizzato): come
    # in fakes/vllm-fake (SPEC-017 §4), un controllo diretto sui tre casi
    # di errore invece del 422 automatico di FastAPI (SPEC-023 §4 chiede
    # un errore esplicito, non specifica il codice — 400 per coerenza con
    # vllm-fake).
    raw_body = await request.body()
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="corpo della richiesta non e' JSON valido")

    if not isinstance(payload, dict) or "inputs" not in payload:
        raise HTTPException(status_code=400, detail="campo 'inputs' assente")

    inputs = payload["inputs"]
    if isinstance(inputs, str):
        texts = [inputs]
    elif isinstance(inputs, list) and all(isinstance(t, str) for t in inputs):
        texts = inputs
    else:
        raise HTTPException(
            status_code=400, detail="'inputs' deve essere una stringa o una lista di stringhe"
        )

    return [_deterministic_vector(t) for t in texts]
