"""Provider-neutral golden evaluation harness for OpenAI-compatible models."""

from __future__ import annotations

import json
import math
import os
import statistics
import time
from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


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
    latency_ms: float
    prompt_tokens: int = 0
    completion_tokens: int = 0
    error: str | None = None


def _facts(entry: GoldenEntry) -> tuple[str, ...]:
    return tuple(
        value
        for values in entry.expected_facts.values()
        for value in ([values] if isinstance(values, str) else values)
    )


def _is_fake(model: str) -> bool:
    return "fake" in model.casefold()


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
    is_real = not _is_fake(model)
    if require_real and not is_real:
        raise ValueError("--require-real rejects fake models")
    if output.exists() and not force:
        raise FileExistsError(output)

    endpoint = base_url.rstrip("/") + "/v1/chat/completions"
    records: list[EvalRecord] = []
    with httpx.Client(timeout=timeout) as client:
        for entry in entries:
            facts = _facts(entry)
            started = time.perf_counter()
            try:
                response = client.post(
                    endpoint,
                    json={
                        "model": model,
                        "temperature": 0,
                        "messages": [{"role": "user", "content": entry.query}],
                    },
                )
                response.raise_for_status()
                payload = response.json()
                answer = payload["choices"][0]["message"]["content"]
                usage = payload.get("usage", {})
                matched = tuple(fact for fact in facts if fact.casefold() in answer.casefold())
                record = EvalRecord(
                    query_id=entry.id,
                    model=model,
                    is_real=is_real,
                    answer=answer,
                    expected_facts=facts,
                    matched_facts=matched,
                    latency_ms=(time.perf_counter() - started) * 1000,
                    prompt_tokens=usage.get("prompt_tokens", 0),
                    completion_tokens=usage.get("completion_tokens", 0),
                )
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                record = EvalRecord(
                    query_id=entry.id,
                    model=model,
                    is_real=is_real,
                    expected_facts=facts,
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
    summary: dict[str, float | int | bool] = {
        "is_real": is_real,
        "query_count": len(records),
        "success_count": successful,
        "error_count": len(records) - successful,
        "pass_rate": successful / len(records) if records else 0.0,
        "fact_recall": matched / expected if expected else 1.0,
        "latency_p50_ms": statistics.median(latencies) if latencies else 0.0,
        "latency_p95_ms": latencies[math.ceil(len(latencies) * 0.95) - 1] if latencies else 0.0,
    }
    output.with_suffix(output.suffix + ".summary.json").write_text(
        json.dumps(summary, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return summary
