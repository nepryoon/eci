import pytest
from eci_core.retrieval.v1 import retrieval_pb2

from orchestrator.intent import classify_intent, to_proto_intent
from orchestrator.retrieval_client import build_hybrid_search_request


@pytest.mark.parametrize("query", ["Chi chiama Validate?", "impatto su checkout", "dipendenze di X"])
def test_structural(query):
    decision = classify_intent(query)
    assert decision.intent == "structural"
    assert (decision.graph_weight, decision.vector_weight) == (0.75, 0.25)
    assert to_proto_intent(decision) == retrieval_pb2.QUERY_INTENT_STRUCTURAL


@pytest.mark.parametrize("query", ["Come funziona checkout?", "cosa fa Validate", "explain parser"])
def test_conceptual(query):
    decision = classify_intent(query)
    assert decision.intent == "conceptual"
    assert (decision.graph_weight, decision.vector_weight) == (0.25, 0.75)


def test_mixed_is_hybrid():
    assert classify_intent("spiega l'impatto di Validate").intent == "hybrid"


def test_unknown_is_hybrid_and_deterministic():
    first = classify_intent("Validate")
    assert first == classify_intent("Validate")
    assert first.intent == "hybrid"
    assert first.graph_weight + first.vector_weight == 1.0


def test_empty_rejected():
    with pytest.raises(ValueError, match="query vuota"):
        classify_intent("  ")


def test_decision_is_propagated_to_hybrid_request():
    context = retrieval_pb2.SecurityContext(tenant_id="authenticated")
    decision = classify_intent("chi chiama Validate?")
    request = build_hybrid_search_request("chi chiama Validate?", context, to_proto_intent(decision))
    assert request.intent == retrieval_pb2.QUERY_INTENT_STRUCTURAL
    assert request.security_context == context
