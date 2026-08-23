"""PydanticAI contracts for the bounded CPG navigation agent."""

import asyncio
from dataclasses import dataclass

from eci_core.retrieval.v1 import retrieval_pb2
from pydantic import BaseModel
from pydantic_ai import Agent, RunContext, Tool

from orchestrator import retrieval_client


class NodeResult(BaseModel):
    node_id: str
    name: str = ""
    node_type: str = ""
    source_text: str = ""
    impact_score: float = 0.0


class SourceNotAvailable(Exception):
    pass


class SummarizationNotYetAvailable(Exception):
    pass


@dataclass(frozen=True)
class Deps:
    retrieval_addr: str
    security_context: object


def _node(value) -> NodeResult:
    return NodeResult(
        node_id=value.node_id,
        name=value.name,
        node_type=value.node_type,
        source_text=value.source_text,
        impact_score=value.scores.impact_score,
    )


async def get_node(ctx: RunContext[Deps], node_id: str) -> NodeResult:
    value = await asyncio.to_thread(
        retrieval_client.get_node, ctx.deps.retrieval_addr, ctx.deps.security_context, node_id
    )
    return _node(value)


async def get_callers(ctx: RunContext[Deps], node_id: str, depth: int = 1) -> list[NodeResult]:
    values = await asyncio.to_thread(
        retrieval_client.expand_neighbors,
        ctx.deps.retrieval_addr,
        ctx.deps.security_context,
        node_id,
        [retrieval_pb2.EDGE_TYPE_CALLS],
        retrieval_pb2.TRAVERSAL_DIRECTION_REVERSE,
        depth,
    )
    return [_node(value) for value in values]


async def get_callees(ctx: RunContext[Deps], node_id: str, depth: int = 1) -> list[NodeResult]:
    values = await asyncio.to_thread(
        retrieval_client.expand_neighbors,
        ctx.deps.retrieval_addr,
        ctx.deps.security_context,
        node_id,
        [retrieval_pb2.EDGE_TYPE_CALLS],
        retrieval_pb2.TRAVERSAL_DIRECTION_FORWARD,
        depth,
    )
    return [_node(value) for value in values]


async def expand_dependencies(
    ctx: RunContext[Deps], node_id: str, edge_types: list[str], depth: int = 1
) -> list[NodeResult]:
    allowed = {
        "IMPORTS": retrieval_pb2.EDGE_TYPE_IMPORTS,
        "DEPENDS_ON": retrieval_pb2.EDGE_TYPE_DEPENDS_ON,
        "EXTENDS": retrieval_pb2.EDGE_TYPE_EXTENDS,
        "IMPLEMENTS": retrieval_pb2.EDGE_TYPE_IMPLEMENTS,
    }
    edges = [allowed[name] for name in edge_types if name in allowed]
    values = await asyncio.to_thread(
        retrieval_client.expand_neighbors,
        ctx.deps.retrieval_addr,
        ctx.deps.security_context,
        node_id,
        edges,
        retrieval_pb2.TRAVERSAL_DIRECTION_FORWARD,
        depth,
    )
    return [_node(value) for value in values]


async def semantic_search(ctx: RunContext[Deps], query: str, filters: dict) -> list[NodeResult]:
    # The current contract exposes HybridSearch, not a vector-only RPC.
    # Authenticated SecurityContext is supplied from Deps; filters cannot replace it.
    values = await asyncio.to_thread(
        retrieval_client.hybrid_search,
        ctx.deps.retrieval_addr,
        query,
        ctx.deps.security_context,
    )
    return [_node(value) for value in values]


async def read_source(ctx: RunContext[Deps], node_id: str) -> str:
    # GetNode currently ignores include_source_text (server.go); never claim
    # hydration that the backend did not perform.
    value = await asyncio.to_thread(
        retrieval_client.get_node,
        ctx.deps.retrieval_addr,
        ctx.deps.security_context,
        node_id,
        True,
    )
    if not value.source_text:
        raise SourceNotAvailable(node_id)
    return value.source_text


async def summarize_subgraph(ctx: RunContext[Deps], node_ids: list[str]) -> str:
    raise SummarizationNotYetAvailable(",".join(node_ids))


AGENT_TOOLS = [
    Tool(get_node), Tool(get_callers), Tool(get_callees),
    Tool(expand_dependencies), Tool(semantic_search), Tool(read_source),
    Tool(summarize_subgraph),
]


# The fake used before T5.3 does not implement structured tool calls.  This
# Agent is nevertheless the authoritative, runtime-validated tool registry;
# LangGraph owns deterministic routing and invokes the same functions.
TOOL_AGENT = Agent(deps_type=Deps, tools=AGENT_TOOLS, defer_model_check=True)
