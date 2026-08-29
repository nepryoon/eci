"""Provider-neutral golden evaluation harness for OpenAI-compatible models."""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
import time
from functools import lru_cache
from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from .golden_canonicalization import (
    CanonicalizedFact,
    DeterministicCanonicalizer,
    EvaluationIssue,
    FactKind,
    IssueCode,
    ResolutionScope,
    StructuredReply,
    SymbolResolver,
    compare_facts,
    load_sample_repo_symbols,
    structured_facts,
)

PROMPT_CONTRACT_VERSION = "golden-structured-facts-v2"
PROMPT_CONTRACT = """Return only one valid JSON object with exactly these fields:
{"facts":[{"kind":"callers|methods|implementations|contains|node_type","value":"canonical value"}],"citations":["repository/path"]}
Facts and citations are separate. Every fact value must contain only canonical identifiers from the repository.
Valid examples: OrderService.Process, EmailNotifier.Notify, main, OrderService.Validate <- OrderService.Process, Class, __EMPTY__.
Invalid examples: Process, Notify is a method, main calls Process, or a citation path in a fact value.
Use __EMPTY__ as the value of one fact with the relevant fixed kind when the answer set is empty.
Do not add markdown, prose, explanations, or fields."""
LOGIC_FINGERPRINT = hashlib.sha256(
    f"{PROMPT_CONTRACT_VERSION}\n{PROMPT_CONTRACT}".encode()
).hexdigest()


class GoldenEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(min_length=1)
    query: str = Field(min_length=1)
    expected_facts: dict[FactKind, list[str] | str]
    scope_note: str = Field(min_length=1)


class EvalRecord(BaseModel):
    query_id: str
    model: str
    is_real: bool
    answer: str = ""
    expected_facts: tuple[str, ...]
    matched_facts: tuple[str, ...] = ()
    unexpected_facts: tuple[str, ...] = ()
    citations: tuple[str, ...] = ()
    context_files: tuple[str, ...] = ()
    latency_ms: float
    prompt_tokens: int = 0
    completion_tokens: int = 0
    error: str | None = None
    passed: bool = False
    canonicalized_facts: tuple[CanonicalizedFact, ...] = ()
    evaluation_issues: tuple[EvaluationIssue, ...] = ()
    semantic_matched_facts: tuple[str, ...] = ()
    semantic_unexpected_facts: tuple[str, ...] = ()
    missing_facts: tuple[str, ...] = ()
    exact_canonical_fact_recall: float = 0.0
    semantic_entity_recall: float | None = None
    canonicalization_coverage: float = 0.0
    prompt_contract_version: str = PROMPT_CONTRACT_VERSION
    logic_fingerprint: str = LOGIC_FINGERPRINT


def _is_fake(model: str) -> bool:
    return "fake" in model.casefold()


def _strip_code_fences(answer: str) -> str:
    stripped = answer.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


@lru_cache(maxsize=1)
def _sample_repo_context() -> tuple[str, tuple[str, ...]]:
    repo_root = Path(__file__).resolve().parents[3]
    fixture_dir = repo_root / "tests" / "fixtures" / "sample-repo"
    source_files = sorted(path for path in fixture_dir.glob("*.go") if path.is_file())
    if not source_files:
        raise FileNotFoundError(f"fixture repo mancante o vuoto: {fixture_dir}")
    context_files = tuple(
        path.relative_to(repo_root).as_posix() for path in source_files
    )
    rendered = []
    for path, rel in zip(source_files, context_files, strict=True):
        rendered.append(f"File: {rel}\n```go\n{path.read_text(encoding='utf-8')}\n```")
    return "\n\n".join(rendered), context_files


def _build_messages(
    entry: GoldenEntry, repository_context: str
) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "Use only the supplied repository context. "
                f"Prompt contract {PROMPT_CONTRACT_VERSION}. {PROMPT_CONTRACT}"
            ),
        },
        {
            "role": "user",
            "content": (
                f"Question: {entry.query}\nRepository context:\n{repository_context}"
            ),
        },
    ]


def run_golden_eval(
    dataset: Path,
    base_url: str,
    model: str,
    output: Path,
    *,
    force: bool = False,
    require_real: bool = False,
    timeout: float = 30.0,
    symbols: SymbolResolver | None = None,
) -> dict[str, float | int | bool | str | None | dict[str, int] | list[str]]:
    entries = TypeAdapter(list[GoldenEntry]).validate_json(dataset.read_bytes())
    if len({entry.id for entry in entries}) != len(entries):
        raise ValueError("duplicate golden id")
    entries_with_expected = tuple(
        (entry, structured_facts(entry.expected_facts)) for entry in entries
    )
    repository_context, context_files = _sample_repo_context()
    repo_root = Path(__file__).resolve().parents[3]
    resolver = symbols or load_sample_repo_symbols(repo_root)
    canonicalizer = DeterministicCanonicalizer(resolver)
    resolution_scope = ResolutionScope(repository="tests/fixtures/sample-repo")
    is_real = not _is_fake(model)
    if require_real and not is_real:
        raise ValueError("--require-real rejects fake models")
    if output.exists() and not force:
        raise FileExistsError(output)

    endpoint = base_url.rstrip("/") + "/v1/chat/completions"
    records: list[EvalRecord] = []
    with httpx.Client(timeout=timeout) as client:
        for entry, expected_structured in entries_with_expected:
            facts = tuple(fact.flattened() for fact in expected_structured)
            started = time.perf_counter()
            try:
                response = client.post(
                    endpoint,
                    json={
                        "model": model,
                        "temperature": 0,
                        "response_format": {"type": "json_object"},
                        "messages": _build_messages(entry, repository_context),
                    },
                )
                response.raise_for_status()
                payload = response.json()
                answer = payload["choices"][0]["message"]["content"]
                usage = payload.get("usage", {})
                reply = StructuredReply.model_validate_json(_strip_code_fences(answer))
                actual_structured = reply.facts
                actual_facts = tuple(fact.flattened() for fact in actual_structured)
                canonicalized = canonicalizer.canonicalize(
                    actual_structured, scope=resolution_scope
                )
                comparison = compare_facts(canonicalized, expected_structured)
                matched = tuple(fact for fact in facts if fact in actual_facts)
                unexpected = tuple(fact for fact in actual_facts if fact not in facts)
                record = EvalRecord(
                    query_id=entry.id,
                    model=model,
                    is_real=is_real,
                    answer=answer,
                    expected_facts=facts,
                    matched_facts=matched,
                    unexpected_facts=unexpected,
                    citations=reply.citations,
                    context_files=context_files,
                    latency_ms=(time.perf_counter() - started) * 1000,
                    prompt_tokens=usage.get("prompt_tokens", 0),
                    completion_tokens=usage.get("completion_tokens", 0),
                    passed=len(matched) == len(facts) and not unexpected,
                    canonicalized_facts=canonicalized,
                    evaluation_issues=comparison.issues,
                    semantic_matched_facts=comparison.semantic_matched_facts,
                    semantic_unexpected_facts=comparison.unexpected_facts,
                    missing_facts=comparison.missing_facts,
                    exact_canonical_fact_recall=comparison.exact_canonical_fact_recall,
                    semantic_entity_recall=comparison.semantic_entity_recall,
                    canonicalization_coverage=comparison.canonicalization_coverage,
                )
            except (
                httpx.HTTPError,
                ValidationError,
                ValueError,
                KeyError,
                TypeError,
            ) as exc:
                record = EvalRecord(
                    query_id=entry.id,
                    model=model,
                    is_real=is_real,
                    expected_facts=facts,
                    context_files=context_files,
                    latency_ms=(time.perf_counter() - started) * 1000,
                    error=type(exc).__name__,
                )
            records.append(record)

    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        temporary.write_text(
            "".join(record.model_dump_json() + "\n" for record in records),
            encoding="utf-8",
        )
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)

    expected = sum(len(record.expected_facts) for record in records)
    matched = sum(len(record.matched_facts) for record in records)
    latencies = sorted(record.latency_ms for record in records)
    successful = sum(record.error is None for record in records)
    passed = sum(record.passed for record in records)
    exact_canonical_matched = sum(len(record.matched_facts) for record in records)
    semantic_matched = sum(
        len(record.semantic_matched_facts) for record in records if record.error is None
    )
    actual_count = sum(
        len(record.canonicalized_facts) for record in records if record.error is None
    )
    resolved_actual = sum(
        sum(item.canonical_fact is not None for item in record.canonicalized_facts)
        for record in records
        if record.error is None
    )
    taxonomy_counts = {code.value: 0 for code in IssueCode}
    for record in records:
        for issue in record.evaluation_issues:
            taxonomy_counts[issue.code.value] += 1
    summary: dict[str, float | int | bool | str | None | dict[str, int] | list[str]] = {
        "is_real": is_real,
        "query_count": len(records),
        "success_count": successful,
        "error_count": len(records) - successful,
        "pass_rate": passed / len(records) if records else 0.0,
        "fact_recall": matched / expected if expected else 1.0,
        "exact_canonical_fact_recall": exact_canonical_matched / expected
        if expected
        else 1.0,
        "semantic_entity_recall": semantic_matched / expected if expected else 1.0,
        "semantic_metric_limitations": [],
        "canonicalization_coverage": resolved_actual / actual_count
        if actual_count
        else 1.0,
        "failure_taxonomy_counts": taxonomy_counts,
        "prompt_contract_version": PROMPT_CONTRACT_VERSION,
        "logic_fingerprint": LOGIC_FINGERPRINT,
        "latency_p50_ms": statistics.median(latencies) if latencies else 0.0,
        "latency_p95_ms": latencies[math.ceil(len(latencies) * 0.95) - 1]
        if latencies
        else 0.0,
    }
    output.with_suffix(output.suffix + ".summary.json").write_text(
        json.dumps(summary, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return summary
