"""Deterministic bounded intent classifier (SPEC-048)."""

from typing import Literal

from eci_core.retrieval.v1 import retrieval_pb2
from opentelemetry import trace
from pydantic import BaseModel

STRUCTURAL_SIGNALS = (
    "chi chiama", "who calls", "chiamanti", "impatto", "impact", "dipendenze",
    "dependencies", "implementa", "implements", "estende", "extends",
)
CONCEPTUAL_SIGNALS = (
    "come funziona", "how does", "cosa fa", "what does", "spiega", "explain",
    "comprendi", "understand", "descrivi", "describe",
)


class IntentDecision(BaseModel, frozen=True):
    intent: Literal["structural", "conceptual", "hybrid"]
    graph_weight: float
    vector_weight: float
    evidence: tuple[str, ...]


def classify_intent(query: str) -> IntentDecision:
    if not query.strip():
        raise ValueError("query vuota")
    lowered = query.casefold()
    structural = tuple(signal for signal in STRUCTURAL_SIGNALS if signal in lowered)
    conceptual = tuple(signal for signal in CONCEPTUAL_SIGNALS if signal in lowered)
    if structural and not conceptual:
        values = ("structural", 0.75, 0.25, structural)
    elif conceptual and not structural:
        values = ("conceptual", 0.25, 0.75, conceptual)
    else:
        values = ("hybrid", 0.5, 0.5, (*structural, *conceptual))
    decision = IntentDecision(
        intent=values[0], graph_weight=values[1], vector_weight=values[2], evidence=values[3]
    )
    span = trace.get_tracer(__name__).start_span("orchestrator.intent.classify")
    span.set_attribute("intent", decision.intent)
    span.set_attribute("graph_weight", decision.graph_weight)
    span.set_attribute("vector_weight", decision.vector_weight)
    span.end()
    return decision


def to_proto_intent(decision: IntentDecision) -> int:
    return {
        "structural": retrieval_pb2.QUERY_INTENT_STRUCTURAL,
        "conceptual": retrieval_pb2.QUERY_INTENT_CONCEPTUAL,
        "hybrid": retrieval_pb2.QUERY_INTENT_HYBRID,
    }[decision.intent]
