"""Bounded LangGraph orchestration for SPEC-047."""

from typing import Literal, TypedDict

from langgraph.graph import END, START, StateGraph
from opentelemetry import trace

from orchestrator.tools import NodeResult


class AgentState(TypedDict, total=False):
    query: str
    pattern: Literal["react", "plan_and_solve"]
    plan: list[str] | None
    visited: set[str]
    frontier: list[str]
    candidates: list[NodeResult]
    step_count: int
    token_count: int
    stop_reason: str | None
    new_above_threshold: int


PLAN = ["find_target", "expand_implementations", "expand_callers", "prioritize", "verify"]


def classify_pattern(state: AgentState) -> dict:
    with trace.get_tracer(__name__).start_as_current_span("orchestrator.graph.classify_pattern"):
        query = state["query"].lower()
        structured = any(word in query for word in ("chi chiama", "who calls", "impatto", "dipendenze"))
        return {"pattern": "plan_and_solve" if structured else "react"}


def make_plan(state: AgentState) -> dict:
    with trace.get_tracer(__name__).start_as_current_span("orchestrator.graph.make_plan"):
        return {"plan": list(PLAN) if state["pattern"] == "plan_and_solve" else None}


def _route_plan(state: AgentState) -> str:
    return "make_plan" if state["pattern"] == "plan_and_solve" else END


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("classify_pattern", classify_pattern)
    graph.add_node("make_plan", make_plan)
    graph.add_edge(START, "classify_pattern")
    graph.add_conditional_edges("classify_pattern", _route_plan)
    graph.add_edge("make_plan", END)
    return graph.compile()


PATTERN_GRAPH = build_graph()


def initial_state(query: str) -> AgentState:
    if not query.strip():
        raise ValueError("query vuota")
    return {
        "query": query,
        "plan": None,
        "visited": set(),
        "frontier": [],
        "candidates": [],
        "step_count": 0,
        "token_count": 0,
        "stop_reason": None,
        "new_above_threshold": 0,
    }


def check_stop(state: AgentState, max_steps: int, max_tokens: int) -> str | None:
    if state["step_count"] >= max_steps:
        return "step_budget_exhausted"
    if state["token_count"] >= max_tokens:
        return "token_budget_exhausted"
    if state["step_count"] and state["new_above_threshold"] == 0:
        return "blast_radius_stabilized"
    return None


def count_tokens(messages: list[dict]) -> int:
    """Deterministic UTF-8 byte/4 estimate used until T5.3 exposes usage.

    This is deliberately named and documented as an estimate, rather than a
    word count presented as model tokens.  It needs no runtime network fetch.
    """
    byte_count = sum(len(str(message.get("content", "")).encode()) for message in messages)
    return (byte_count + 3) // 4


def reasoning_messages(query: str, action: str) -> list[dict]:
    """Visited IDs deliberately never enter the model input."""
    return [{"role": "user", "content": f"Domanda: {query}\nAzione corrente: {action}"}]


def record_stop(state: AgentState, reason: str) -> None:
    state["stop_reason"] = reason
    span = trace.get_current_span()
    span.add_event("orchestrator.stop", {"reason": reason})
