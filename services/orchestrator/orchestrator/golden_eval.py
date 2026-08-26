"""Provider-neutral golden evaluation harness for OpenAI-compatible models."""

from __future__ import annotations

import json
import math
import os
import statistics
import time
from functools import lru_cache
from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError


class GoldenEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(min_length=1)
    query: str = Field(min_length=1)
    expected_facts: dict[str, list[str] | str]
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


class ModelReply(BaseModel):
    model_config = ConfigDict(extra="forbid")
    facts: dict[str, list[str] | str]
    citations: tuple[str, ...] = ()


def _facts(facts: dict[str, list[str] | str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for category, values in facts.items():
        if isinstance(values, str):
            normalized.append(f"{category}={values}")
            continue
        if not values:
            normalized.append(f"{category}=__EMPTY__")
            continue
        normalized.extend(f"{category}={value}" for value in values)
    return tuple(normalized)


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
    context_files = tuple(path.relative_to(repo_root).as_posix() for path in source_files)
    rendered = []
    for path, rel in zip(source_files, context_files, strict=True):
        rendered.append(f"File: {rel}\n```go\n{path.read_text(encoding='utf-8')}\n```")
    return "\n\n".join(rendered), context_files


def _build_messages(entry: GoldenEntry, repository_context: str) -> list[dict[str, str]]:
    fact_shape = {
        key: ([] if isinstance(value, list) else "")
        for key, value in entry.expected_facts.items()
    }
    response_shape = {"facts": fact_shape, "citations": ["tests/fixtures/sample-repo/<file>.go"]}
    return [
        {
            "role": "system",
            "content": (
                "Rispondi usando solo il contesto del repository fornito. "
                "Restituisci solo JSON valido, senza markdown o testo extra, "
                f"con questa forma esatta: {json.dumps(response_shape, ensure_ascii=False)}. "
                "Per una categoria lista senza risultati usa []."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Question: {entry.query}\n"
                f"Scope note: {entry.scope_note}\n"
                "Repository context:\n"
                f"{repository_context}"
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
) -> dict[str, float | int | bool]:
    entries = TypeAdapter(list[GoldenEntry]).validate_json(dataset.read_bytes())
    if len({entry.id for entry in entries}) != len(entries):
        raise ValueError("duplicate golden id")
    repository_context, context_files = _sample_repo_context()
    is_real = not _is_fake(model)
    if require_real and not is_real:
        raise ValueError("--require-real rejects fake models")
    if output.exists() and not force:
        raise FileExistsError(output)

    endpoint = base_url.rstrip("/") + "/v1/chat/completions"
    records: list[EvalRecord] = []
    with httpx.Client(timeout=timeout) as client:
        for entry in entries:
            facts = _facts(entry.expected_facts)
            started = time.perf_counter()
            try:
                response = client.post(
                    endpoint,
                    json={
                        "model": model,
                        "temperature": 0,
                        "messages": _build_messages(entry, repository_context),
                    },
                )
                response.raise_for_status()
                payload = response.json()
                answer = payload["choices"][0]["message"]["content"]
                usage = payload.get("usage", {})
                reply = ModelReply.model_validate_json(_strip_code_fences(answer))
                actual_facts = _facts(reply.facts)
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
                )
            except (httpx.HTTPError, ValidationError, ValueError, KeyError, TypeError) as exc:
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
    summary: dict[str, float | int | bool] = {
        "is_real": is_real,
        "query_count": len(records),
        "success_count": successful,
        "error_count": len(records) - successful,
        "pass_rate": passed / len(records) if records else 0.0,
        "fact_recall": matched / expected if expected else 1.0,
        "latency_p50_ms": statistics.median(latencies) if latencies else 0.0,
        "latency_p95_ms": latencies[math.ceil(len(latencies) * 0.95) - 1] if latencies else 0.0,
    }
    output.with_suffix(output.suffix + ".summary.json").write_text(
        json.dumps(summary, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return summary
