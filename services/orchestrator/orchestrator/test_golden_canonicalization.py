import hashlib
import json
from pathlib import Path

import pytest

from orchestrator.golden_canonicalization import (
    CanonicalizationError,
    DeterministicCanonicalizer,
    FactKind,
    InMemorySymbolResolver,
    ResolutionScope,
    ResolutionStatus,
    StructuredFact,
    compare_facts,
    load_sample_repo_symbols,
    structured_facts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
BASELINE = REPO_ROOT / "artifacts" / "t5.6" / "20260828T211053Z" / "results.jsonl"
QUERIES = REPO_ROOT / "tests" / "golden" / "queries_v0.json"
TAXONOMY = REPO_ROOT / "tests" / "golden" / "t5_6_failure_taxonomy_v1.json"
SCOPE = ResolutionScope(repository="tests/fixtures/sample-repo")


def fact(kind: str, value: str) -> StructuredFact:
    return StructuredFact(kind=FactKind(kind), value=value)


def test_resolver_exact_unique_ambiguous_and_not_found_are_explicit():
    resolver = InMemorySymbolResolver(
        ["OrderService.Process", "Other.Process", "OrderService.Validate", "main"]
    )

    assert resolver.resolve("main", scope=SCOPE).status is ResolutionStatus.exact
    unique = resolver.resolve("Validate", scope=SCOPE)
    assert unique.status is ResolutionStatus.unique
    assert unique.canonical_identifier == "OrderService.Validate"
    ambiguous = resolver.resolve("Process", scope=SCOPE)
    assert ambiguous.status is ResolutionStatus.ambiguous
    assert ambiguous.candidates == ("OrderService.Process", "Other.Process")
    assert resolver.resolve("Missing", scope=SCOPE).status is ResolutionStatus.not_found


def test_canonicalizer_distinguishes_exact_unqualified_verbose_and_semantic_error():
    resolver = InMemorySymbolResolver(
        ["OrderService", "OrderService.Process", "OrderService.Validate", "main"]
    )
    canonicalizer = DeterministicCanonicalizer(resolver)
    actual = (
        fact("contains", "OrderService.Process"),
        fact("contains", "Validate"),
        fact("contains", "main istanzia OrderService"),
        fact("callers", "tests/fixtures/sample-repo/order_service.go"),
    )

    results = canonicalizer.canonicalize(actual, scope=SCOPE)

    assert [result.resolution.status for result in results] == [
        ResolutionStatus.exact,
        ResolutionStatus.unique,
        ResolutionStatus.exact,
        ResolutionStatus.not_found,
    ]
    assert [
        result.primary_issue.code if result.primary_issue else None
        for result in results
    ] == [
        None,
        "unqualified_symbol",
        "verbose_fact",
        "semantic_error",
    ]
    assert results[1].canonical_fact == fact("contains", "OrderService.Validate")
    assert results[2].canonical_fact == fact("contains", "main")
    assert results[3].canonical_fact is None


def test_ambiguous_short_symbol_is_never_guessed():
    canonicalizer = DeterministicCanonicalizer(
        InMemorySymbolResolver(["OrderService.Process", "Other.Process"])
    )
    result = canonicalizer.canonicalize((fact("methods", "Process"),), scope=SCOPE)[0]

    assert result.resolution.status is ResolutionStatus.ambiguous
    assert result.canonical_fact is None
    assert result.primary_issue.code == "ambiguous_canonicalization"


def test_verbose_primary_precedes_ambiguous_secondary_condition():
    canonicalizer = DeterministicCanonicalizer(
        InMemorySymbolResolver(["OrderService.Process", "Other.Process"])
    )
    result = canonicalizer.canonicalize(
        (fact("methods", "Process is a method"),), scope=SCOPE
    )[0]

    assert result.primary_issue.code == "verbose_fact"
    assert result.resolution.status is ResolutionStatus.ambiguous
    assert result.canonical_fact is None


def test_exact_and_semantic_recall_are_separate_and_issue_order_is_stable():
    resolver = InMemorySymbolResolver(
        ["OrderService.Process", "OrderService.Validate", "Extra"]
    )
    canonical = DeterministicCanonicalizer(resolver).canonicalize(
        (
            fact("methods", "Process"),
            fact("methods", "OrderService.Validate"),
            fact("methods", "Extra"),
        ),
        scope=SCOPE,
    )
    result = compare_facts(
        canonical,
        (
            fact("methods", "OrderService.Process"),
            fact("methods", "OrderService.Validate"),
            fact("methods", "Missing"),
        ),
    )

    assert result.exact_canonical_fact_recall == pytest.approx(1 / 3)
    assert result.semantic_entity_recall == pytest.approx(2 / 3)
    assert [issue.code for issue in result.issues] == [
        "unqualified_symbol",
        "unexpected_fact",
        "missing_fact",
    ]
    assert result.unexpected_facts == ("methods=Extra",)
    assert result.missing_facts == ("methods=Missing",)


def test_canonicalization_cannot_observe_expected_facts():
    class SpyResolver(InMemorySymbolResolver):
        def __init__(self):
            super().__init__(["OrderService.Process"])
            self.calls = []

        def resolve(self, identifier, *, scope):
            self.calls.append((identifier, scope))
            return super().resolve(identifier, scope=scope)

    resolver = SpyResolver()
    canonicalizer = DeterministicCanonicalizer(resolver)
    actual = (fact("methods", "Process"),)

    first = canonicalizer.canonicalize(actual, scope=SCOPE)
    first_calls = tuple(resolver.calls)
    resolver.calls.clear()
    second = canonicalizer.canonicalize(actual, scope=SCOPE)

    assert first == second
    assert tuple(resolver.calls) == first_calls
    assert all(
        "expected" not in parameter
        for parameter in canonicalizer.canonicalize.__annotations__
    )


def test_symbol_backend_failure_is_typed_and_fail_closed():
    class FailingResolver:
        def resolve(self, identifier, *, scope):
            raise ConnectionError(
                f"symbol backend unavailable for {identifier} in {scope.repository}"
            )

    canonicalizer = DeterministicCanonicalizer(FailingResolver())

    with pytest.raises(CanonicalizationError, match="symbol resolver failed"):
        canonicalizer.canonicalize((fact("contains", "main"),), scope=SCOPE)


def test_real_t56_failures_have_required_deterministic_taxonomy():
    expected_taxonomy = json.loads(TAXONOMY.read_text(encoding="utf-8"))
    records = {
        record["query_id"]: record
        for record in (
            json.loads(line)
            for line in BASELINE.read_text(encoding="utf-8").splitlines()
        )
    }
    canonicalizer = DeterministicCanonicalizer(load_sample_repo_symbols(REPO_ROOT))

    for query_id, expected_codes in expected_taxonomy.items():
        reply = json.loads(records[query_id]["answer"])
        actual = tuple(
            fact(kind, value)
            for kind, values in reply["facts"].items()
            for value in (values if isinstance(values, list) else [values])
        )
        canonicalized = canonicalizer.canonicalize(actual, scope=SCOPE)
        actual_codes = [
            item.primary_issue.code
            for item in canonicalized
            if item.primary_issue is not None
        ]
        assert actual_codes == expected_codes, query_id


def test_real_t56_replay_metrics_are_new_and_do_not_rewrite_baseline():
    canonicalizer = DeterministicCanonicalizer(load_sample_repo_symbols(REPO_ROOT))
    exact = semantic = expected_count = resolved = actual_count = 0
    issue_counts: dict[str, int] = {}

    for record in (
        json.loads(line) for line in BASELINE.read_text(encoding="utf-8").splitlines()
    ):
        reply = json.loads(record["answer"])
        actual = structured_facts(reply["facts"])
        expected = tuple(
            fact(*flattened.split("=", 1)) for flattened in record["expected_facts"]
        )
        first = compare_facts(canonicalizer.canonicalize(actual, scope=SCOPE), expected)
        second = compare_facts(
            canonicalizer.canonicalize(actual, scope=SCOPE), expected
        )
        assert first == second
        exact += first.exact_matched_count
        semantic += first.semantic_matched_count
        expected_count += first.expected_count
        resolved += first.resolved_actual_count
        actual_count += first.actual_count
        for issue in first.issues:
            issue_counts[issue.code.value] = issue_counts.get(issue.code.value, 0) + 1

    historical_summary = json.loads(
        BASELINE.with_suffix(".jsonl.summary.json").read_text(encoding="utf-8")
    )
    assert historical_summary["pass_rate"] == 0.5
    assert historical_summary["fact_recall"] == 0.4
    assert exact / expected_count == pytest.approx(0.4)
    assert semantic / expected_count == pytest.approx(14 / 15)
    assert resolved / actual_count == pytest.approx(14 / 15)
    assert issue_counts == {
        "semantic_error": 1,
        "unqualified_symbol": 4,
        "verbose_fact": 4,
        "missing_fact": 1,
    }


def test_historical_baseline_and_queries_are_byte_identical():
    assert hashlib.sha256(BASELINE.read_bytes()).hexdigest() == (
        "a7c9b8b1a9ad04aa2e3b1a9076d718e94896b3c7a229d35200f7e3735aba08a5"
    )
    assert hashlib.sha256(QUERIES.read_bytes()).hexdigest() == (
        "67b6bca856e7bfce733be2cab38cb10e210ce831e41339393decb3f793c0f06b"
    )
