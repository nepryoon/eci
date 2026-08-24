"""Orchestrazione di `eci ask` (SPEC-018): query -> HybridSearch ->
prompt -> vllm-fake -> risposta con provenance. `run_ask` è la funzione
Python richiamata sia dalla CLI (`orchestrator.cli`) sia direttamente
dai test di integrazione (SPEC-018 §7: "orchestrator chiamato
direttamente, non necessariamente il processo CLI")."""

from dataclasses import dataclass

import httpx

from orchestrator.errors import LLMUnavailableError
from orchestrator.graph import run_agent
from orchestrator.llm_client import chat_completion
from orchestrator.prompt import build_messages
from orchestrator.retrieval_client import build_security_context
from orchestrator.tools import Deps, RetrievalToolRuntime


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
        runtime = RetrievalToolRuntime(Deps(retrieval_addr, security_context))
        try:
            state = run_agent(
                query_text,
                runtime,
                reasoner=lambda messages: chat_completion(vllm_url, messages),
            )
        except httpx.HTTPError as e:
            raise LLMUnavailableError(vllm_url, e, sources=list(runtime.raw_nodes.values())) from e
        nodes = [
            runtime.raw_nodes[candidate.node_id]
            for candidate in state["candidates"]
            if candidate.node_id in runtime.raw_nodes
        ]

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


class _noop_cm:
    """Context manager inerte, usato quando run_ask è chiamata senza un
    TracerProvider (es. nei test che non ne costruiscono uno) — lo span
    OTel è un arricchimento di osservabilità (SPEC-018 §8), non un
    requisito per il funzionamento di run_ask."""

    def __enter__(self):
        return None

    def __exit__(self, *exc_info):
        return False
