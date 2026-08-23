"""LangGraph bounded dell'orchestrator T5.1."""

from dataclasses import dataclass
from contextlib import nullcontext
from typing import Literal, TypedDict

import httpx
from langgraph.graph import END, START, StateGraph

from orchestrator.errors import LLMUnavailableError
from orchestrator.tools import Deps, NodeResult, semantic_search


class AgentState(TypedDict):
    query: str
    pattern: Literal["react", "plan_and_solve"]
    plan: list[str] | None
    visited: set[str]
    candidates: list[NodeResult]
    step_count: int
    token_count: int
    stop_reason: str | None
    iteration_new_high_impact: int
    llm_url: str
    deps: Deps


@dataclass(frozen=True)
class AgentConfig:
    max_steps: int = 20
    max_tokens: int = 8_000
    impact_threshold: float = 0.0

    def __post_init__(self):
        if not 1 <= self.max_steps <= 30:
            raise ValueError("max_steps deve essere compreso tra 1 e 30")
        if self.max_tokens < 1:
            raise ValueError("max_tokens deve essere positivo")


def initial_state(query: str, deps: Deps | None = None, llm_url: str = "") -> AgentState:
    if not query.strip():
        raise ValueError("query vuota")
    return AgentState(
        query=query,
        pattern="react",
        plan=None,
        visited=set(),
        candidates=[],
        step_count=0,
        token_count=0,
        stop_reason=None,
        iteration_new_high_impact=1,
        llm_url=llm_url,
        deps=deps,  # type: ignore[typeddict-item]
    )


def classify_pattern(state: AgentState) -> dict:
    structural = ("chi chiama", "impatto", "dipendenze", "who calls")
    return {"pattern": "plan_and_solve" if any(x in state["query"].lower() for x in structural) else "react"}


def make_plan(state: AgentState) -> dict:
    return {
        "plan": ["trova il simbolo target", "espandi implementazioni", "trova chiamanti", "prioritizza", "verifica"]
    }


def merge_candidates(state: AgentState, found: list[NodeResult], threshold: float) -> dict:
    fresh = [node for node in found if node.node_id not in state["visited"]]
    visited = set(state["visited"])
    visited.update(node.node_id for node in fresh)
    high = sum(1 for node in fresh if getattr(getattr(node, "scores", None), "impact_score", 0.0) > threshold)
    return {
        "visited": visited,
        "candidates": [*state["candidates"], *fresh],
        "iteration_new_high_impact": high,
    }


async def reason(base_url: str, messages: list[dict]) -> str:
    endpoint = base_url.rstrip("/") + "/v1/chat/completions"
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(endpoint, json={"model": "vllm-fake", "messages": messages})
        response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]


async def react_step(state: AgentState, config: AgentConfig) -> dict:
    # Deliberatamente non serializza state.visited: il dedup resta nel controller.
    messages = [
        {"role": "system", "content": "Scegli il prossimo passo usando solo query e osservazione aggregata."},
        {"role": "user", "content": f"Query: {state['query']}\nCandidati noti: {len(state['candidates'])}"},
    ]
    try:
        observation = await reason(state["llm_url"], messages)
    except httpx.HTTPError as exc:
        raise LLMUnavailableError(state["llm_url"], exc, state["candidates"]) from exc
    found = await semantic_search(_Context(state["deps"]), state["query"], {})
    merged = merge_candidates(state, found, config.impact_threshold)
    merged.update(step_count=state["step_count"] + 1, token_count=state["token_count"] + len(observation.split()))
    return merged


def check_stop(state: AgentState, config: AgentConfig) -> dict:
    reason_value = None
    if state["step_count"] >= config.max_steps:
        reason_value = "step_budget_exhausted"
    elif state["token_count"] >= config.max_tokens:
        reason_value = "token_budget_exhausted"
    elif state["step_count"] > 0 and state["iteration_new_high_impact"] == 0:
        reason_value = "blast_radius_stabilized"
    return {"stop_reason": reason_value}


def _span(state: AgentState, node_name: str):
    deps = state.get("deps")
    provider = getattr(deps, "tracer_provider", None)
    if provider is None:
        return nullcontext()
    return provider.get_tracer(__name__).start_as_current_span(f"orchestrator.graph.{node_name}")


def _observed_sync(name, function):
    def call(state):
        with _span(state, name) as span:
            update = function(state)
            if update.get("stop_reason") and span is not None:
                span.add_event("orchestrator.stop", {"reason": update["stop_reason"]})
            return update

    return call


def _observed_async(name, function):
    async def call(state):
        with _span(state, name):
            return await function(state)

    return call


class _Context:
    def __init__(self, deps):
        self.deps = deps


def build_graph(config: AgentConfig):
    graph = StateGraph(AgentState)
    graph.add_node("classify_pattern", _observed_sync("classify_pattern", classify_pattern))
    graph.add_node("make_plan", _observed_sync("make_plan", make_plan))
    graph.add_node("react_step", _observed_async("react_step", lambda state: react_step(state, config)))
    graph.add_node("check_stop", _observed_sync("check_stop", lambda state: check_stop(state, config)))
    graph.add_edge(START, "classify_pattern")
    graph.add_conditional_edges(
        "classify_pattern", lambda state: state["pattern"], {"plan_and_solve": "make_plan", "react": "react_step"}
    )
    graph.add_edge("make_plan", "react_step")
    graph.add_edge("react_step", "check_stop")
    graph.add_conditional_edges("check_stop", lambda state: END if state["stop_reason"] else "react_step")
    return graph.compile()


async def run_agent(query: str, deps: Deps, llm_url: str, config: AgentConfig | None = None) -> AgentState:
    actual_config = config or AgentConfig()
    return await build_graph(actual_config).ainvoke(initial_state(query, deps, llm_url))
