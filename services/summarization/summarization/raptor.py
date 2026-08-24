"""Bottom-up structural RAPTOR summarization with content-addressed caching."""

from __future__ import annotations

from enum import IntEnum
from typing import Protocol, Sequence

from opentelemetry import trace
from opentelemetry.trace import Tracer
from pydantic import BaseModel, ConfigDict, Field

SHA256_PATTERN = r"^[0-9a-f]{64}$"


class SummaryLevel(IntEnum):
    METHOD = 0
    CLASS = 1
    MODULE = 2
    REPO = 3


class SummaryNode(BaseModel):
    model_config = ConfigDict(frozen=True)
    node_id: str = Field(min_length=1)
    level: SummaryLevel
    ast_hash: str = Field(pattern=SHA256_PATTERN)
    source: str = ""
    child_ids: tuple[str, ...] = ()


class SummaryCacheKey(BaseModel):
    model_config = ConfigDict(frozen=True)
    ast_hash: str = Field(pattern=SHA256_PATTERN)
    logic_fingerprint: str = Field(pattern=SHA256_PATTERN)


class ChildSummary(BaseModel):
    model_config = ConfigDict(frozen=True)
    node_id: str
    level: SummaryLevel
    summary: str


class SummaryResult(BaseModel):
    model_config = ConfigDict(frozen=True)
    root_id: str
    summaries: dict[str, str]
    cache_hits: int
    cache_misses: int


class SummaryCache(Protocol):
    def get(self, key: SummaryCacheKey) -> str | None: ...
    def put(self, key: SummaryCacheKey, summary: str) -> None: ...


class SummaryModel(Protocol):
    def summarize(
        self, node: SummaryNode, child_summaries: tuple[ChildSummary, ...]
    ) -> str: ...


class InvalidHierarchyError(ValueError):
    """The requested CONTAINS hierarchy is not a valid tree/DAG."""


class SummaryCacheError(RuntimeError):
    """The semantic cache failed; do not silently regenerate or claim a hit."""


class SummaryModelError(RuntimeError):
    """The summary model failed or returned an invalid summary."""


class RaptorSummarizer:
    def __init__(
        self,
        cache: SummaryCache,
        model: SummaryModel,
        logic_fingerprint: str,
        *,
        tracer: Tracer | None = None,
    ) -> None:
        self._cache = cache
        self._model = model
        self._fingerprint = SummaryCacheKey(
            ast_hash="0" * 64, logic_fingerprint=logic_fingerprint
        ).logic_fingerprint
        self._tracer = tracer or trace.get_tracer(__name__)

    def summarize(
        self, nodes: Sequence[SummaryNode], root_id: str
    ) -> SummaryResult:
        by_id = {node.node_id: node for node in nodes}
        if len(by_id) != len(nodes):
            raise InvalidHierarchyError("duplicate node_id")
        if root_id not in by_id:
            raise InvalidHierarchyError(f"root '{root_id}' does not exist")
        order = self._postorder(by_id, root_id)
        summaries: dict[str, str] = {}
        hits = misses = 0
        with self._tracer.start_as_current_span("summarization.raptor") as span:
            for node in order:
                key = SummaryCacheKey(
                    ast_hash=node.ast_hash, logic_fingerprint=self._fingerprint
                )
                cached = self._get(key)
                if cached is not None:
                    if not cached.strip():
                        raise SummaryCacheError("cache returned an empty summary")
                    summaries[node.node_id] = cached
                    hits += 1
                    span.add_event(
                        "summarization.cache_hit",
                        {"summarization.level": node.level.name.lower()},
                    )
                    continue
                children = tuple(
                    ChildSummary(
                        node_id=child_id,
                        level=by_id[child_id].level,
                        summary=summaries[child_id],
                    )
                    for child_id in node.child_ids
                )
                summary = self._generate(node, children)
                self._put(key, summary)
                summaries[node.node_id] = summary
                misses += 1
                span.add_event(
                    "summarization.cache_miss",
                    {"summarization.level": node.level.name.lower()},
                )
            span.set_attribute("summarization.node_count", len(order))
            span.set_attribute("summarization.cache_hits", hits)
            span.set_attribute("summarization.cache_misses", misses)
        return SummaryResult(
            root_id=root_id,
            summaries=summaries,
            cache_hits=hits,
            cache_misses=misses,
        )

    @staticmethod
    def _postorder(
        nodes: dict[str, SummaryNode], root_id: str
    ) -> list[SummaryNode]:
        visiting: set[str] = set()
        visited: set[str] = set()
        order: list[SummaryNode] = []

        def visit(node_id: str) -> None:
            if node_id in visiting:
                raise InvalidHierarchyError("cycle in CONTAINS hierarchy")
            if node_id in visited:
                return
            node = nodes[node_id]
            visiting.add(node_id)
            for child_id in node.child_ids:
                child = nodes.get(child_id)
                if child is None:
                    raise InvalidHierarchyError(f"child '{child_id}' does not exist")
                if child.level >= node.level:
                    raise InvalidHierarchyError(
                        f"invalid levels: {node.node_id} cannot contain {child.node_id}"
                    )
                visit(child_id)
            visiting.remove(node_id)
            visited.add(node_id)
            order.append(node)

        visit(root_id)
        return order

    def _get(self, key: SummaryCacheKey) -> str | None:
        try:
            return self._cache.get(key)
        except Exception as exc:
            raise SummaryCacheError("semantic cache get failed") from exc

    def _put(self, key: SummaryCacheKey, summary: str) -> None:
        try:
            self._cache.put(key, summary)
        except Exception as exc:
            raise SummaryCacheError("semantic cache put failed") from exc

    def _generate(
        self, node: SummaryNode, children: tuple[ChildSummary, ...]
    ) -> str:
        try:
            summary = self._model.summarize(node, children)
        except Exception as exc:
            raise SummaryModelError("summary model failed") from exc
        if not summary.strip():
            raise SummaryModelError("summary model returned empty output")
        return summary
