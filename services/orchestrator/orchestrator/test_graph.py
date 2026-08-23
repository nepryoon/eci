import asyncio

import pytest
from pydantic_ai import Tool

from orchestrator.graph import (
    PATTERN_GRAPH,
    check_stop,
    count_tokens,
    initial_state,
    reasoning_messages,
)
from orchestrator.tools import (
    AGENT_TOOLS,
    SummarizationNotYetAvailable,
    summarize_subgraph,
)


def test_classifies_and_plans_before_tools():
    state = PATTERN_GRAPH.invoke(initial_state("Chi chiama Validate?"))
    assert state["pattern"] == "plan_and_solve"
    assert state["plan"][0] == "find_target"
    assert state["step_count"] == 0


def test_exploratory_query_has_no_plan():
    state = PATTERN_GRAPH.invoke(initial_state("capisci come funziona checkout"))
    assert state["pattern"] == "react"
    assert state["plan"] is None


def test_real_pydantic_tools_registered():
    assert len(AGENT_TOOLS) == 7
    assert all(isinstance(tool, Tool) for tool in AGENT_TOOLS)


def test_budgets_and_stabilization_are_distinct():
    state = initial_state("x")
    state.update(step_count=15, token_count=0, new_above_threshold=2)
    assert check_stop(state, 15, 1000) == "step_budget_exhausted"
    state.update(step_count=1, token_count=1000)
    assert check_stop(state, 15, 1000) == "token_budget_exhausted"
    state.update(token_count=1, new_above_threshold=0)
    assert check_stop(state, 15, 1000) == "blast_radius_stabilized"


def test_token_measurement_and_visited_not_in_prompt():
    visited = "private-node-id-123"
    messages = reasoning_messages("explain checkout", "semantic_search")
    assert visited not in str(messages)
    assert count_tokens(messages) > 0


def test_empty_query_fails_before_graph():
    with pytest.raises(ValueError, match="query vuota"):
        initial_state("  ")


def test_summarization_is_typed_failure():
    with pytest.raises(SummarizationNotYetAvailable):
        asyncio.run(summarize_subgraph(None, ["a"]))
