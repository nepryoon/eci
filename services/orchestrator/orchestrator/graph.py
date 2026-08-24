"""Executable bounded LangGraph agent for SPEC-047."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph
from opentelemetry import trace

from orchestrator.intent import classify_intent
from orchestrator.tools import NodeResult


class AgentState(TypedDict, total=False):
    query: str
    pattern: Literal["react", "plan_and_solve"]
    plan: list[str] | None
    plan_index: int
    visited: set[str]
    frontier: list[str]
    seed_ids: list[str]
    tool_history: set[str]
    candidates: list[NodeResult]
    step_count: int
    token_count: int
    stop_reason: str | None
    new_above_threshold: int
    last_action: str


class ToolRuntime(Protocol):
    """Boundary used by graph nodes; production calls the typed tool layer."""

    def execute(self, action: str, query: str, node_id: str | None) -> list[NodeResult]: ...


PLAN = ["semantic_search", "expand_dependencies", "get_callers"]


@dataclass(frozen=True)
class AgentConfig:
    max_steps: int = 15
    max_tokens: int = 4096
    impact_threshold: float = 0.0

    def __post_init__(self) -> None:
        if self.max_steps < 1 or self.max_tokens < 1:
            raise ValueError("i budget devono essere positivi")


DEFAULT_AGENT_CONFIG = AgentConfig()


def classify_pattern(state: AgentState) -> dict:
    with _span("classify_pattern"):
        decision = classify_intent(state["query"])
        return {"pattern": "plan_and_solve" if decision.intent == "structural" else "react"}


def make_plan(state: AgentState) -> dict:
    with _span("make_plan"):
        return {"plan": list(PLAN), "plan_index": 0}


def initial_state(query: str) -> AgentState:
    if not query.strip():
        raise ValueError("query vuota")
    return {
        "query": query,
        "plan": None,
        "plan_index": 0,
        "visited": set(),
        "frontier": [],
        "seed_ids": [],
        "tool_history": set(),
        "candidates": [],
        "step_count": 0,
        "token_count": 0,
        "stop_reason": None,
        "new_above_threshold": 0,
        "last_action": "",
    }


def check_stop(state: AgentState, max_steps: int, max_tokens: int) -> str | None:
    if state["step_count"] >= max_steps:
        return "step_budget_exhausted"
    if state["token_count"] >= max_tokens:
        return "token_budget_exhausted"
    plan = state.get("plan")
    if plan and state["plan_index"] >= len(plan):
        return "plan_completed"
    # Stabilization is meaningful only after an expansion, not after seed search.
    if state["step_count"] > 1 and state["new_above_threshold"] == 0 and not state["frontier"]:
        return "blast_radius_stabilized"
    return None


def count_tokens(messages: list[dict]) -> int:
    """Declared deterministic estimate until T5.3 provides model usage."""
    byte_count = sum(len(str(message.get("content", "")).encode()) for message in messages)
    return (byte_count + 3) // 4


def reasoning_messages(query: str, action: str) -> list[dict]:
    """The visited set is intentionally not accepted by this API."""
    return [{"role": "user", "content": f"Domanda: {query}\nAzione corrente: {action}"}]


def _next_action(state: AgentState) -> str:
    plan = state.get("plan")
    if plan:
        return plan[min(state["plan_index"], len(plan) - 1)]
    if state["step_count"] == 0:
        return "semantic_search"
    return "get_callees"


def _unvisited_frontier(state: AgentState) -> tuple[str | None, list[str]]:
    frontier = list(state["frontier"])
    while frontier:
        candidate = frontier.pop()
        if candidate not in state["visited"]:
            return candidate, frontier
    return None, frontier


def _planned_target(state: AgentState, action: str) -> str | None:
    """Planned stages operate on the original seed, not a shared DFS pop."""
    history = state["tool_history"]
    return next(
        (node_id for node_id in state["seed_ids"] if f"{action}:{node_id}" not in history),
        None,
    )


def build_agent_graph(
    runtime: ToolRuntime,
    config: AgentConfig = DEFAULT_AGENT_CONFIG,
    reasoner: Callable[[list[dict]], str] | None = None,
):
    """Compile a complete classify→plan/react→tool→stop bounded loop."""

    def react_step(state: AgentState) -> dict:
        with _span("react_step"):
            action = _next_action(state)
            messages = reasoning_messages(state["query"], action)
            target = None
            remaining = list(state["frontier"])
            if action != "semantic_search":
                if state.get("plan"):
                    target = _planned_target(state, action)
                else:
                    target, remaining = _unvisited_frontier(state)
                if target is None:
                    # A dead end backtracks through the saved frontier. If no
                    # alternative exists the stop node observes stabilization.
                    return {
                        "frontier": remaining,
                        "step_count": state["step_count"] + 1,
                        "new_above_threshold": 0,
                        "last_action": action,
                        "plan_index": state["plan_index"] + 1,
                    }

            # Deduplication happens before the downstream call.
            results = runtime.execute(action, state["query"], target)
            plan = state.get("plan")
            should_reason = not plan or action == plan[-1]
            if reasoner is not None and results and should_reason:
                # No-result behavior remains compatible with SPEC-018: do not
                # call the LLM with an empty context. Planned retrieval is
                # completed before reasoning so an unavailable LLM can still
                # report every source discovered by the deterministic plan.
                reasoner(messages)
            visited = set(state["visited"])
            history = set(state["tool_history"])
            if target is not None:
                visited.add(target)
                history.add(f"{action}:{target}")
            existing = {node.node_id for node in state["candidates"]}
            novel = [node for node in results if node.node_id not in existing and node.node_id not in visited]
            candidates = [*state["candidates"], *novel]
            remaining.extend(node.node_id for node in novel if node.node_id not in visited)
            above = sum(node.impact_score >= config.impact_threshold for node in novel)
            return {
                "visited": visited,
                "tool_history": history,
                "frontier": remaining,
                "seed_ids": (
                    [node.node_id for node in novel]
                    if action == "semantic_search"
                    else state["seed_ids"]
                ),
                "candidates": candidates,
                "step_count": state["step_count"] + 1,
                "token_count": state["token_count"] + count_tokens(messages),
                "new_above_threshold": above,
                "last_action": action,
                "plan_index": state["plan_index"] + 1,
            }

    def stop_node(state: AgentState) -> dict:
        with _span("check_stop"):
            reason = check_stop(state, config.max_steps, config.max_tokens)
            if reason:
                trace.get_current_span().add_event("orchestrator.stop", {"reason": reason})
            return {"stop_reason": reason}

    def after_classify(state: AgentState) -> str:
        return "make_plan" if state["pattern"] == "plan_and_solve" else "react_step"

    def after_stop(state: AgentState) -> str:
        return END if state.get("stop_reason") else "react_step"

    graph = StateGraph(AgentState)
    graph.add_node("classify_pattern", classify_pattern)
    graph.add_node("make_plan", make_plan)
    graph.add_node("react_step", react_step)
    graph.add_node("check_stop", stop_node)
    graph.add_edge(START, "classify_pattern")
    graph.add_conditional_edges("classify_pattern", after_classify)
    graph.add_edge("make_plan", "react_step")
    graph.add_edge("react_step", "check_stop")
    graph.add_conditional_edges("check_stop", after_stop)
    return graph.compile()


def run_agent(
    query: str,
    runtime: ToolRuntime,
    config: AgentConfig = DEFAULT_AGENT_CONFIG,
    reasoner: Callable[[list[dict]], str] | None = None,
) -> AgentState:
    # Two graph nodes execute per agent step, plus classify/plan overhead.
    recursion_limit = config.max_steps * 2 + 10
    return build_agent_graph(runtime, config, reasoner).invoke(
        initial_state(query), {"recursion_limit": recursion_limit}
    )


def _span(name: str):
    return trace.get_tracer(__name__).start_as_current_span(f"orchestrator.graph.{name}")
