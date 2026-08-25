import asyncio

import httpx
import pytest
from eci_core.retrieval.v1 import retrieval_pb2
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic_ai import Tool

from orchestrator.ask import run_ask
from orchestrator.errors import LLMUnavailableError
from orchestrator.graph import (
    AgentConfig,
    build_agent_graph,
    check_stop,
    count_tokens,
    initial_state,
    reasoning_messages,
    run_agent,
)
from orchestrator.tools import (
    AGENT_TOOLS,
    NodeResult,
    SummarizationNotYetAvailable,
    _resolve_dependency_edges,
    summarize_subgraph,
)


class RecordingRuntime:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def execute(self, action, query, node_id):
        self.calls.append((action, node_id))
        return self.responses.pop(0) if self.responses else []


def node(node_id, score=1.0):
    return NodeResult(node_id=node_id, impact_score=score)


def test_plan_is_created_and_consumed_before_expansion():
    runtime = RecordingRuntime([[node("target")], [node("dependency")], [node("caller")]])
    state = run_agent("impatto di Validate", runtime, AgentConfig(max_steps=5))
    assert runtime.calls == [
        ("semantic_search", None),
        ("expand_dependencies", "target"),
        ("get_callers", "target"),
    ]
    assert state["stop_reason"] == "plan_completed"


def test_empty_dependency_stage_preserves_seed_for_callers():
    runtime = RecordingRuntime([[node("validate")], [], [node("process")]])
    state = run_agent("chi chiama Validate", runtime, AgentConfig(max_steps=5))
    assert runtime.calls[-1] == ("get_callers", "validate")
    assert {item.node_id for item in state["candidates"]} == {"validate", "process"}


def test_plan_processes_every_seed_before_advancing_stage():
    runtime = RecordingRuntime(
        [
            [node("seed-a"), node("seed-b")],
            [node("dep-a")],
            [node("dep-b")],
            [node("caller-a")],
            [node("caller-b")],
        ]
    )
    state = run_agent("impatto di Validate", runtime, AgentConfig(max_steps=8))
    assert runtime.calls == [
        ("semantic_search", None),
        ("expand_dependencies", "seed-a"),
        ("expand_dependencies", "seed-b"),
        ("get_callers", "seed-a"),
        ("get_callers", "seed-b"),
    ]
    assert state["stop_reason"] == "plan_completed"


def test_planned_reasoning_runs_once_after_every_seed_finishes():
    runtime = RecordingRuntime(
        [
            [node("seed-a"), node("seed-b")],
            [],
            [],
            [node("caller-a")],
            [node("caller-b")],
        ]
    )
    payloads = []
    run_agent(
        "impatto di Validate",
        runtime,
        AgentConfig(max_steps=8),
        lambda messages: payloads.append(messages),
    )
    assert len(payloads) == 1
    assert runtime.calls[-2:] == [
        ("get_callers", "seed-a"),
        ("get_callers", "seed-b"),
    ]


def test_plan_caps_seed_budget_but_still_reaches_callers():
    runtime = RecordingRuntime([[node(f"seed-{idx}") for idx in range(15)]] + [[] for _ in range(14)])
    state = run_agent("impatto di Validate", runtime)
    caller_calls = [call for call in runtime.calls if call[0] == "get_callers"]
    assert len(caller_calls) == 7
    assert caller_calls[0] == ("get_callers", "seed-0")
    assert caller_calls[-1] == ("get_callers", "seed-6")
    assert state["seed_ids"] == [f"seed-{idx}" for idx in range(7)]
    assert state["stop_reason"] == "plan_completed"


def test_invalid_dependency_edges_fail_closed():
    with pytest.raises(ValueError, match="unsupported dependency edge types"):
        _resolve_dependency_edges(["MISSPELLED"])
def test_react_selects_search_then_callees():
    runtime = RecordingRuntime([[node("target")], []])
    run_agent("capisci checkout", runtime, AgentConfig(max_steps=4))
    assert runtime.calls == [("semantic_search", None), ("get_callees", "target")]


def test_dedup_prevents_tool_call_for_visited_node_and_backtracks():
    runtime = RecordingRuntime([[node("a"), node("b")], [node("a")], []])
    state = initial_state("capisci x")
    state.update(pattern="react", frontier=["b", "a"], visited={"a"}, step_count=1)
    graph = build_agent_graph(runtime, AgentConfig(max_steps=2))
    result = graph.invoke(state)
    assert runtime.calls == [("get_callees", "b")]
    assert result["stop_reason"] == "step_budget_exhausted"


def test_empty_result_backtracks_to_alternative_frontier():
    runtime = RecordingRuntime([[node("a"), node("b")], [], [node("c")]])
    state = run_agent("capisci x", runtime, AgentConfig(max_steps=3))
    assert runtime.calls == [
        ("semantic_search", None),
        ("get_callees", "b"),
        ("get_callees", "a"),
    ]
    assert {item.node_id for item in state["candidates"]} == {"a", "b", "c"}


def test_real_pydantic_tools_registered():
    assert len(AGENT_TOOLS) == 7
    assert all(isinstance(tool, Tool) for tool in AGENT_TOOLS)


def test_budgets_and_stabilization_are_distinct():
    state = initial_state("x")
    state.update(step_count=15, token_count=0, new_above_threshold=2)
    assert check_stop(state, 15, 1000) == "step_budget_exhausted"
    state.update(step_count=1, token_count=1000)
    assert check_stop(state, 15, 1000) == "token_budget_exhausted"
    state.update(step_count=2, token_count=1, new_above_threshold=0, frontier=[])
    assert check_stop(state, 15, 1000) == "blast_radius_stabilized"


def test_duplicate_is_not_new_blast_radius_signal():
    runtime = RecordingRuntime([[node("a", 5)], [node("a", 5)]])
    state = run_agent("capisci x", runtime, AgentConfig(max_steps=4, impact_threshold=1))
    assert state["new_above_threshold"] == 0
    assert state["stop_reason"] == "blast_radius_stabilized"


def test_token_measurement_and_visited_not_in_prompt():
    messages = reasoning_messages("explain checkout", "semantic_search")
    assert "private-node-id-123" not in str(messages)
    assert count_tokens(messages) > 0


def test_reasoner_receives_real_payload_without_visited_ids():
    payloads = []
    runtime = RecordingRuntime([[node("private-node-id-123")], [node("next-node")]])
    run_agent("explain checkout", runtime, AgentConfig(max_steps=2), lambda messages: payloads.append(messages))
    assert len(payloads) == 2
    assert "private-node-id-123" not in str(payloads[1])


def test_reasoner_failure_stops_before_tool_call():
    runtime = RecordingRuntime([[node("must-not-run")]])

    def unavailable(_messages):
        raise ConnectionError("vllm-fake unreachable")

    with pytest.raises(ConnectionError, match="unreachable"):
        run_agent("explain checkout", runtime, reasoner=unavailable)
    # Seed retrieval succeeds first so SPEC-018 can still report sources; no
    # subsequent traversal tool is called after reasoning fails.
    assert runtime.calls == [("semantic_search", None)]


def test_default_step_budget_does_not_hit_langgraph_recursion_limit():
    runtime = RecordingRuntime([[node("seed")]] + [[node(f"n{i}")] for i in range(20)])
    state = run_agent("capisci x", runtime)
    assert state["stop_reason"] == "step_budget_exhausted"
    assert state["step_count"] == 15


def test_run_ask_routes_through_graph(monkeypatch):
    validate = retrieval_pb2.RetrievedNode(node_id="validate", name="Validate")
    process = retrieval_pb2.RetrievedNode(node_id="process", name="Process")

    class Runtime:
        instance = None

        def __init__(self, _deps):
            Runtime.instance = self
            self.calls = []
            self.raw_nodes = {}

        def execute(self, action, _query, node_id):
            self.calls.append((action, node_id))
            values = {
                "semantic_search": [validate],
                "expand_dependencies": [],
                "get_callers": [process],
            }[action]
            self.raw_nodes.update((value.node_id, value) for value in values)
            return [node(value.node_id) for value in values]

    monkeypatch.setattr("orchestrator.ask.RetrievalToolRuntime", Runtime)
    monkeypatch.setattr("orchestrator.ask.chat_completion", lambda _url, messages: str(messages))
    result = run_ask("chi chiama Validate", "unused", "http://fake")
    assert Runtime.instance.calls[-1] == ("get_callers", "validate")
    assert [value.node_id for value in result.nodes] == ["validate", "process"]


def test_structured_llm_failure_keeps_callers_in_sources(monkeypatch):
    validate = retrieval_pb2.RetrievedNode(node_id="validate", name="Validate")
    process = retrieval_pb2.RetrievedNode(node_id="process", name="Process")

    class Runtime:
        def __init__(self, _deps):
            self.raw_nodes = {}

        def execute(self, action, _query, _node_id):
            values = {
                "semantic_search": [validate],
                "expand_dependencies": [],
                "get_callers": [process],
            }[action]
            self.raw_nodes.update((value.node_id, value) for value in values)
            return [node(value.node_id) for value in values]

    monkeypatch.setattr("orchestrator.ask.RetrievalToolRuntime", Runtime)

    def unavailable(_url, _messages):
        raise httpx.ConnectError("unreachable")

    monkeypatch.setattr("orchestrator.ask.chat_completion", unavailable)
    with pytest.raises(LLMUnavailableError) as excinfo:
        run_ask("chi chiama Validate", "unused", "http://fake")
    assert {value.node_id for value in excinfo.value.sources} == {"validate", "process"}


def test_each_langgraph_node_emits_span_and_stop_event(monkeypatch):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr("orchestrator.graph.trace.get_tracer", provider.get_tracer)
    run_agent("capisci x", RecordingRuntime([[node("a")], []]), AgentConfig(max_steps=2))
    spans = exporter.get_finished_spans()
    names = {span.name for span in spans}
    assert "orchestrator.graph.classify_pattern" in names
    assert "orchestrator.graph.react_step" in names
    stop_spans = [span for span in spans if span.name == "orchestrator.graph.check_stop"]
    assert any(event.name == "orchestrator.stop" for span in stop_spans for event in span.events)


def test_empty_query_fails_before_graph():
    runtime = RecordingRuntime([])
    with pytest.raises(ValueError, match="query vuota"):
        run_agent("  ", runtime)
    assert runtime.calls == []


def test_summarization_is_typed_failure():
    with pytest.raises(SummarizationNotYetAvailable):
        asyncio.run(summarize_subgraph(None, ["a"]))
