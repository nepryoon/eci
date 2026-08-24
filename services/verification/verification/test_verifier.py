from copy import deepcopy

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import ValidationError

from verification.verifier import (
    CandidateAnswer,
    CitationClaim,
    RelationClaim,
    SnippetClaim,
    SymbolClaim,
    SymbolEvidence,
    TreeSitterSyntaxVerifier,
    VerificationBackendError,
    VerificationConfig,
    Verifier,
)


class Store:
    def __init__(self):
        self.symbols = {
            "A": SymbolEvidence(node_id="A", repo="r", path="a.py", start_line=1, end_line=4, commit_sha="new"),
            "B": SymbolEvidence(node_id="B", repo="r", path="b.py", start_line=2, end_line=3, commit_sha="new"),
        }
        self.relations = {("A", "B", "CALLS")}
        self.depths = []

    def symbol(self, node_id):
        return self.symbols.get(node_id)

    def relation_exists(self, source_id, target_id, edge_type, max_depth):
        self.depths.append(max_depth)
        return (source_id, target_id, edge_type) in self.relations


def candidate(**overrides):
    values = {
        "answer": "A calls B.",
        "symbol_claims": [SymbolClaim(node_id="A"), SymbolClaim(node_id="B")],
        "relation_claims": [
            RelationClaim(
                source_id="A", target_id="B", edge_type="CALLS", max_depth=1
            )
        ],
        "citations": [
            CitationClaim(
                node_id="A",
                repo="r",
                path="a.py",
                start_line=1,
                end_line=4,
                commit_sha="new",
            )
        ],
        "snippets": [
            SnippetClaim(language="python", source="def ok():\n    return 1\n")
        ],
    }
    values.update(overrides)
    return CandidateAnswer(**values)


def verifier(store=None, **config):
    return Verifier(store or Store(), TreeSitterSyntaxVerifier(), VerificationConfig(**config))


def test_approved_claims_are_deterministic_and_input_is_immutable():
    item = candidate()
    original = deepcopy(item)
    first = verifier().verify(item)
    second = verifier().verify(item)
    assert first == second
    assert item == original
    assert first.outcome == "approved"
    assert first.issues == ()


def test_symbol_hallucination_requests_regeneration_with_feedback():
    result = verifier().verify(candidate(symbol_claims=[SymbolClaim(node_id="missing")]))
    assert result.outcome == "regenerated"
    assert result.issues[0].code == "symbol-hallucination"
    assert result.feedback == ("symbol 'missing' does not exist",)


def test_relation_check_is_bounded_and_missing_relation_regenerates():
    store = Store()
    result = verifier(store, max_relation_depth=2).verify(candidate(relation_claims=[RelationClaim(source_id="B", target_id="A", edge_type="CALLS", max_depth=99)]))
    assert store.depths == [2]
    assert result.outcome == "regenerated"
    assert result.issues[0].code == "relation-nonexistent"


def test_stale_citation_is_corrected_from_current_provenance():
    stale = CitationClaim(node_id="A", repo="old", path="old.py", start_line=9, end_line=10, commit_sha="old")
    result = verifier().verify(candidate(citations=[stale]))
    assert result.outcome == "corrected"
    assert result.issues[0].code == "stale-citation"
    assert result.corrected_citations[0].path == "a.py"
    assert result.corrected_citations[0].commit_sha == "new"


def test_real_tree_sitter_reparse_rejects_invalid_snippet_and_unknown_language():
    syntax = TreeSitterSyntaxVerifier()
    assert syntax.is_valid("python", "def ok():\n    return 1\n") is True
    assert syntax.is_valid("python", "def broken(:\n") is False
    result = verifier().verify(candidate(snippets=[SnippetClaim(language="not-a-language", source="x")]))
    assert result.issues[0].detail == "unsupported-language"


def test_all_stages_report_issues_in_stable_order():
    item = candidate(
        symbol_claims=[SymbolClaim(node_id="missing")],
        relation_claims=[RelationClaim(source_id="B", target_id="A", edge_type="CALLS")],
        citations=[CitationClaim(node_id="A", repo="r", path="old.py", start_line=1, end_line=4, commit_sha="old")],
        snippets=[SnippetClaim(language="python", source="def broken(:\n")],
    )
    result = verifier().verify(item)
    assert [issue.code for issue in result.issues] == ["symbol-hallucination", "relation-nonexistent", "stale-citation", "syntax-invalid"]


def test_regeneration_budget_degrades_after_last_attempt():
    item = candidate(symbol_claims=[SymbolClaim(node_id="missing")])
    gate = verifier(max_regenerations=2)
    assert gate.verify(item, attempt=1).outcome == "regenerated"
    assert gate.verify(item, attempt=2).outcome == "degraded"


def test_backend_failure_is_closed():
    class Broken(Store):
        def symbol(self, node_id):
            raise RuntimeError("database unavailable")

    with pytest.raises(VerificationBackendError, match="symbol check failed"):
        verifier(Broken()).verify(candidate())


def test_models_and_config_reject_invalid_values():
    with pytest.raises(ValidationError):
        SymbolClaim(node_id="")
    with pytest.raises(ValidationError):
        CitationClaim(node_id="A", repo="r", path="p", start_line=4, end_line=2, commit_sha="c")
    with pytest.raises(ValueError):
        VerificationConfig(max_regenerations=1)


def test_span_has_outcome_and_issue_events():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    gate = Verifier(Store(), TreeSitterSyntaxVerifier(), tracer=provider.get_tracer("test"))
    gate.verify(candidate(symbol_claims=[SymbolClaim(node_id="missing")]))
    span = exporter.get_finished_spans()[0]
    assert span.name == "verification.verify"
    assert span.attributes["verification.outcome"] == "regenerated"
    assert span.attributes["verification.issue_count"] == 1
    assert [event.name for event in span.events] == ["verification.issue"]
