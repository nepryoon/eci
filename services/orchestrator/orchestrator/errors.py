"""Errori espliciti di `eci ask` (SPEC-018 §3 scenari 4/5): mai una
traceback Python grezza per un servizio a valle irraggiungibile."""


class RetrievalUnavailableError(Exception):
    """retrieval-engine irraggiungibile (SPEC-018 §3 scenario 4) — il
    messaggio include host/porta, non un errore gRPC grezzo."""

    def __init__(self, addr: str, cause: Exception) -> None:
        super().__init__(f"impossibile raggiungere retrieval-engine ({addr}): {cause}")
        self.addr = addr
        self.cause = cause


class LLMUnavailableError(Exception):
    """vllm-fake irraggiungibile (SPEC-018 §3 scenario 5). `sources`
    porta i RetrievedNode già ottenuti da retrieval-engine PRIMA del
    fallimento — la chiamata a retrieval-engine è già completata con
    successo a questo punto, quindi il chiamante può comunque costruire
    e mostrare la sezione "Fonti" prima di segnalare l'errore."""

    def __init__(self, url: str, cause: Exception, sources: list) -> None:
        super().__init__(f"impossibile raggiungere vllm-fake ({url}): {cause}")
        self.url = url
        self.cause = cause
        self.sources = sources
