"""Tool agentici T5.1: adattatori sottili delle RPC RetrievalEngine."""

import asyncio
from dataclasses import dataclass
from typing import Any

from eci_core.retrieval.v1 import retrieval_pb2
from pydantic_ai import RunContext, Tool

from orchestrator.errors import SourceTextNotYetAvailable, SummarizationNotYetAvailable
from orchestrator.retrieval_client import expand_neighbors, get_node as rpc_get_node, hybrid_search

NodeResult = retrieval_pb2.RetrievedNode


@dataclass(frozen=True)
class Deps:
    retrieval_addr: str
    security_context: retrieval_pb2.SecurityContext
    tracer_provider: Any = None


async def get_node(ctx: RunContext[Deps], node_id: str) -> NodeResult:
    return await asyncio.to_thread(
        rpc_get_node, ctx.deps.retrieval_addr, ctx.deps.security_context, node_id, ctx.deps.tracer_provider
    )


async def _expand(ctx, node_id, edge_types, direction, depth):
    if depth < 1:
        raise ValueError("depth deve essere >= 1")
    return await asyncio.to_thread(
        expand_neighbors,
        ctx.deps.retrieval_addr,
        ctx.deps.security_context,
        node_id,
        edge_types,
        direction,
        depth,
        ctx.deps.tracer_provider,
    )


async def get_callers(ctx: RunContext[Deps], node_id: str, depth: int) -> list[NodeResult]:
    return await _expand(
        ctx, node_id, [retrieval_pb2.EDGE_TYPE_CALLS], retrieval_pb2.TRAVERSAL_DIRECTION_REVERSE, depth
    )


async def get_callees(ctx: RunContext[Deps], node_id: str, depth: int) -> list[NodeResult]:
    return await _expand(
        ctx, node_id, [retrieval_pb2.EDGE_TYPE_CALLS], retrieval_pb2.TRAVERSAL_DIRECTION_FORWARD, depth
    )


async def expand_dependencies(
    ctx: RunContext[Deps], node_id: str, edge_types: list[str], depth: int
) -> list[NodeResult]:
    values = []
    for name in edge_types:
        value = retrieval_pb2.EdgeType.Value("EDGE_TYPE_" + name.upper())
        if value == retrieval_pb2.EDGE_TYPE_UNSPECIFIED:
            raise ValueError(f"edge type non supportato: {name}")
        values.append(value)
    return await _expand(ctx, node_id, values, retrieval_pb2.TRAVERSAL_DIRECTION_REVERSE, depth)


async def semantic_search(
    ctx: RunContext[Deps], query: str, filters: dict[str, Any], entry_node_id: str = ""
) -> list[NodeResult]:
    """HybridSearch reale; il backend non espone una gamba vector-only."""
    repos = filters.get("repos", [])
    return await asyncio.to_thread(
        hybrid_search,
        ctx.deps.retrieval_addr,
        query,
        ctx.deps.security_context,
        ctx.deps.tracer_provider,
        entry_node_id,
        repos,
    )


async def read_source(ctx: RunContext[Deps], node_id: str) -> str:
    raise SourceTextNotYetAvailable(
        f"source_text per {node_id!r} non disponibile: GetNode non effettua hydration OpenSearch"
    )


async def summarize_subgraph(ctx: RunContext[Deps], node_ids: list[str]) -> str:
    raise SummarizationNotYetAvailable(f"RAPTOR non disponibile per {len(node_ids)} nodi prima di T5.5")


AGENT_TOOLS = [
    Tool(get_node),
    Tool(get_callers),
    Tool(get_callees),
    Tool(expand_dependencies),
    Tool(semantic_search),
    Tool(read_source),
    Tool(summarize_subgraph),
]
