"""SPEC-002 §3 scenario 3, §7 — round-trip di marshalling protobuf sui
messaggi chiave generati da retrieval.proto."""

from eci_core.retrieval.v1 import retrieval_pb2


def test_security_context_round_trip():
    want = retrieval_pb2.SecurityContext(tenant_id="t1")
    data = want.SerializeToString()
    got = retrieval_pb2.SecurityContext()
    got.ParseFromString(data)
    assert got.tenant_id == "t1"


def test_retrieved_node_round_trip():
    want = retrieval_pb2.RetrievedNode(
        node_id="n1",
        domain=retrieval_pb2.DOMAIN_CODE,
        name="MyClass",
        scores=retrieval_pb2.NodeScores(rrf_score=0.5),
    )
    data = want.SerializeToString()
    got = retrieval_pb2.RetrievedNode()
    got.ParseFromString(data)
    assert got.node_id == "n1"
    assert got.scores.rrf_score == 0.5


def test_hybrid_search_request_round_trip():
    want = retrieval_pb2.HybridSearchRequest(
        security_context=retrieval_pb2.SecurityContext(tenant_id="t1"),
        query_text="chi chiama X?",
        top_k=25,
    )
    data = want.SerializeToString()
    got = retrieval_pb2.HybridSearchRequest()
    got.ParseFromString(data)
    assert got.security_context.tenant_id == "t1"
    assert got.top_k == 25
