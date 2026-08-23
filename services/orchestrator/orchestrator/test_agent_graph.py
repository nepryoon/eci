import asyncio
from types import SimpleNamespace

import pytest

from orchestrator.errors import SummarizationNotYetAvailable, SourceTextNotYetAvailable
from orchestrator.graph import (
    AgentConfig,
    check_stop,
    classify_pattern,
    initial_state,
    make_plan,
    merge_candidates,
    run_agent,
)
from orchestrator.tools import Deps, read_source, summarize_subgraph


def node(node_id: str, impact: float = 0.0):
    return SimpleNamespace(node_id=node_id, name=node_id, scores=SimpleNamespace(impact_score=impact))


def test_classify_and_plan_before_tools():
    structural = classify_pattern(initial_state("Chi chiama Validate?"))
    assert structural["pattern"] == "plan_and_solve"
    assert make_plan(structural)["plan"]
    assert classify_pattern(initial_state("capisci come funziona X"))["pattern"] == "react"


def test_stop_budgets_and_stabilization():
    state = initial_state("q")
    state["step_count"] = 2
    assert check_stop(state, AgentConfig(max_steps=2))["stop_reason"] == "step_budget_exhausted"
    state = initial_state("q")
    state["token_count"] = 10
    assert check_stop(state, AgentConfig(max_tokens=10))["stop_reason"] == "token_budget_exhausted"
    state["token_count"] = 0
    state["iteration_new_high_impact"] = 0
    state["step_count"] = 1
    assert check_stop(state, AgentConfig())["stop_reason"] == "blast_radius_stabilized"


def test_visited_nodes_are_deduplicated():
    state = initial_state("q")
    state["visited"].add("seen")
    merged = merge_candidates(state, [node("seen", 1.0), node("new", 1.0)], threshold=0.5)
    assert [n.node_id for n in merged["candidates"]] == ["new"]
    assert merged["visited"] == {"seen", "new"}


def test_unavailable_tools_are_typed():
    ctx = SimpleNamespace(deps=Deps("addr", SimpleNamespace()))
    with pytest.raises(SummarizationNotYetAvailable):
        asyncio.run(summarize_subgraph(ctx, ["n1"]))
    with pytest.raises(SourceTextNotYetAvailable):
        asyncio.run(read_source(ctx, "n1"))


def test_prompt_never_contains_visited_ids(monkeypatch):
    captured = []

    async def fake_llm(_url, messages):
        captured.extend(m["content"] for m in messages)
        return "continue"

    async def fake_search(_ctx, query, filters, entry_node_id=""):
        return [node("secret-visited-id", 1.0)]

    monkeypatch.setattr("orchestrator.graph.reason", fake_llm)
    monkeypatch.setattr("orchestrator.graph.semantic_search", fake_search)
    result = asyncio.run(run_agent("capisci X", Deps("addr", SimpleNamespace()), "fake", AgentConfig(max_steps=2)))
    assert result["visited"] == {"secret-visited-id"}
    assert "secret-visited-id" not in "\n".join(captured)
