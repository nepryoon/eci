"""Orchestrazione di `eci ask` (SPEC-018): query -> HybridSearch ->
prompt -> vllm-fake -> risposta con provenance. `run_ask` è la funzione
Python richiamata sia dalla CLI (`orchestrator.cli`) sia direttamente
dai test di integrazione (SPEC-018 §7: "orchestrator chiamato
direttamente, non necessariamente il processo CLI")."""

from dataclasses import dataclass

import httpx

from orchestrator.errors import LLMUnavailableError
from orchestrator.llm_client import chat_completion
from orchestrator.prompt import build_messages
from orchestrator.retrieval_client import (
    build_security_context,
    find_callers,
    hybrid_search,
)
from orchestrator.who_calls import is_who_calls_query


@dataclass
class AskResult:
    query_text: str
    nodes: list  # RetrievedNode (protobuf) — mai None, eventualmente vuota
    answer: str | None  # None SOLO se nodes è vuota (nessuna chiamata a vllm-fake)


def run_ask(query_text: str, retrieval_addr: str, vllm_url: str, tracer_provider=None) -> AskResult:
    """Solleva RetrievalUnavailableError/LLMUnavailableError sui
    fallimenti di connessione (SPEC-018 §3 scenari 4/5) — non le
    intercetta qui, il chiamante (CLI o test) decide come presentarle.
    Nessuna eccezione per "nessun risultato" (SPEC-018 §3 scenario 3):
    è un esito legittimo, non un errore — segnalato da `nodes == []` e
    `answer is None` in un AskResult normale."""
    tracer = tracer_provider.get_tracer(__name__) if tracer_provider is not None else None
    span_cm = tracer.start_as_current_span("orchestrator.ask") if tracer is not None else _noop_cm()

    with span_cm:
        security_context = build_security_context()
        nodes = hybrid_search(retrieval_addr, query_text, security_context, tracer_provider=tracer_provider)

        if is_who_calls_query(query_text):
            # SPEC-018 §2: HybridSearch da sola non può rispondere a una
            # domanda sui chiamanti (full-text solo sul nome) — per
            # ciascun nodo trovato, aggiungi anche chi lo chiama
            # (ExpandNeighbors REVERSE su CALLS), deduplicato per
            # node_id, nell'insieme usato sia per il prompt sia per
            # "Fonti".
            nodes = _merge_with_callers(retrieval_addr, security_context, nodes, tracer_provider)

        if not nodes:
            return AskResult(query_text=query_text, nodes=[], answer=None)

        messages = build_messages(query_text, nodes)
        try:
            answer = chat_completion(vllm_url, messages)
        except httpx.HTTPError as e:
            # Le fonti sono già disponibili: la chiamata a retrieval-engine
            # è già riuscita a questo punto (SPEC-018 §3 scenario 5).
            raise LLMUnavailableError(vllm_url, e, sources=nodes) from e

        return AskResult(query_text=query_text, nodes=nodes, answer=answer)


def _merge_with_callers(retrieval_addr, security_context, nodes, tracer_provider) -> list:
    merged = list(nodes)
    seen_ids = {n.node_id for n in nodes}
    for n in nodes:
        for caller in find_callers(retrieval_addr, security_context, n.node_id, tracer_provider=tracer_provider):
            if caller.node_id not in seen_ids:
                seen_ids.add(caller.node_id)
                merged.append(caller)
    return merged


class _noop_cm:
    """Context manager inerte, usato quando run_ask è chiamata senza un
    TracerProvider (es. nei test che non ne costruiscono uno) — lo span
    OTel è un arricchimento di osservabilità (SPEC-018 §8), non un
    requisito per il funzionamento di run_ask."""

    def __enter__(self):
        return None

    def __exit__(self, *exc_info):
        return False
