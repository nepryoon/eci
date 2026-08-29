"""Bottom-up structural RAPTOR summarization with content-addressed caching."""

from __future__ import annotations

import unicodedata
from collections.abc import Sequence
from enum import IntEnum
from hashlib import sha256
from struct import pack
from typing import Protocol

from eci_core.retrieval.v1.retrieval_pb2 import SecurityContext
from opentelemetry import metrics, trace
from opentelemetry.trace import Tracer
from pydantic import BaseModel, ConfigDict, Field

SHA256_PATTERN = r"^[0-9a-f]{64}$"
ACL_SCOPE_VERSION = b"eci-acl-scope-v1"
SUMMARY_VIEW_VERSION = b"eci-summary-view-v1"
MAX_SCOPE_VALUES = 128
MAX_SCOPE_VALUE_BYTES = 256

_visibility_counter = metrics.get_meter(__name__).create_counter(
    "eci_summarization_visibility_total",
    description="RAPTOR node visibility outcomes by bounded level and outcome.",
)


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
    tenant_id: str = Field(min_length=1)
    repo: str = Field(min_length=1)
    acl_group: str = Field(min_length=1)


class SummaryCacheKey(BaseModel):
    model_config = ConfigDict(frozen=True)
    ast_hash: str = Field(pattern=SHA256_PATTERN)
    logic_fingerprint: str = Field(pattern=SHA256_PATTERN)
    acl_scope: str = Field(pattern=SHA256_PATTERN)


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


class SummaryAuthorizationError(PermissionError):
    """The authenticated scope cannot read the requested summary view."""


def acl_scope_fingerprint(security_context: SecurityContext) -> str:
    """Return ADR-0012's canonical fingerprint or fail closed."""

    tenant = security_context.tenant_id
    user = security_context.user_id
    repos = _normalize_scope_values(security_context.allowed_repos)
    groups = _normalize_scope_values(security_context.acl_groups)
    if not _valid_scope_value(tenant) or not _valid_scope_value(user):
        raise SummaryAuthorizationError("not authorized")
    if repos is None or groups is None:
        raise SummaryAuthorizationError("not authorized")

    encoded = bytearray(ACL_SCOPE_VERSION)
    _append_text(encoded, tenant)
    _append_list(encoded, repos)
    _append_list(encoded, groups)
    return sha256(encoded).hexdigest()


def _normalize_scope_values(values: Sequence[str]) -> tuple[str, ...] | None:
    if not values or len(values) > MAX_SCOPE_VALUES:
        return None
    if any(not _valid_scope_value(value) for value in values):
        return None
    return tuple(sorted(set(values)))


def _valid_scope_value(value: str) -> bool:
    return (
        bool(value)
        and value.strip() == value
        and len(value.encode("utf-8")) <= MAX_SCOPE_VALUE_BYTES
        and not any(unicodedata.category(char) == "Cc" for char in value)
    )


def _append_text(encoded: bytearray, value: str) -> None:
    raw = value.encode("utf-8")
    encoded.extend(pack(">I", len(raw)))
    encoded.extend(raw)


def _append_list(encoded: bytearray, values: Sequence[str]) -> None:
    encoded.extend(pack(">I", len(values)))
    for value in values:
        _append_text(encoded, value)


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
            ast_hash="0" * 64,
            logic_fingerprint=logic_fingerprint,
            acl_scope="0" * 64,
        ).logic_fingerprint
        self._tracer = tracer or trace.get_tracer(__name__)

    def summarize(
        self,
        nodes: Sequence[SummaryNode],
        root_id: str,
        security_context: SecurityContext,
    ) -> SummaryResult:
        acl_scope = acl_scope_fingerprint(security_context)
        repos = frozenset(_normalize_scope_values(security_context.allowed_repos) or ())
        groups = frozenset(_normalize_scope_values(security_context.acl_groups) or ())
        by_id = {node.node_id: node for node in nodes}
        if len(by_id) != len(nodes):
            raise InvalidHierarchyError("duplicate node_id")
        if root_id not in by_id:
            raise InvalidHierarchyError("root does not exist")
        order = self._postorder(by_id, root_id)
        if not self._authorized(
            by_id[root_id], security_context.tenant_id, repos, groups
        ):
            raise SummaryAuthorizationError("not authorized")
        summaries: dict[str, str] = {}
        visible: dict[str, bool] = {}
        complete: dict[str, bool] = {}
        effective_hashes: dict[str, str] = {}
        hits = misses = 0
        with self._tracer.start_as_current_span("summarization.raptor") as span:
            for node in order:
                if not self._authorized(
                    node, security_context.tenant_id, repos, groups
                ):
                    visible[node.node_id] = False
                    complete[node.node_id] = False
                    self._observe_visibility(span, node.level, "denied")
                    continue

                visible[node.node_id] = True
                child_ids = tuple(
                    child_id for child_id in node.child_ids if visible[child_id]
                )
                is_complete = all(
                    visible[child_id] and complete[child_id]
                    for child_id in node.child_ids
                )
                complete[node.node_id] = is_complete
                effective_hash = (
                    node.ast_hash
                    if is_complete
                    else self._effective_view_hash(node, visible, effective_hashes)
                )
                effective_hashes[node.node_id] = effective_hash
                key = SummaryCacheKey(
                    ast_hash=effective_hash,
                    logic_fingerprint=self._fingerprint,
                    acl_scope=acl_scope,
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
                    self._observe_visibility(
                        span, node.level, "full" if is_complete else "restricted"
                    )
                    continue
                children = tuple(
                    ChildSummary(
                        node_id=child_id,
                        level=by_id[child_id].level,
                        summary=summaries[child_id],
                    )
                    for child_id in child_ids
                )
                model_node = (
                    node
                    if is_complete
                    else node.model_copy(
                        update={
                            "source": "",
                            "child_ids": child_ids,
                            "ast_hash": effective_hash,
                        }
                    )
                )
                summary = self._generate(model_node, children)
                self._put(key, summary)
                summaries[node.node_id] = summary
                misses += 1
                span.add_event(
                    "summarization.cache_miss",
                    {"summarization.level": node.level.name.lower()},
                )
                self._observe_visibility(
                    span, node.level, "full" if is_complete else "restricted"
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
    def _authorized(
        node: SummaryNode,
        tenant_id: str,
        repos: frozenset[str],
        groups: frozenset[str],
    ) -> bool:
        return (
            _valid_scope_value(node.tenant_id)
            and _valid_scope_value(node.repo)
            and _valid_scope_value(node.acl_group)
            and node.tenant_id == tenant_id
            and node.repo in repos
            and node.acl_group in groups
        )

    @staticmethod
    def _effective_view_hash(
        node: SummaryNode,
        visible: dict[str, bool],
        effective_hashes: dict[str, str],
    ) -> str:
        encoded = bytearray(SUMMARY_VIEW_VERSION)
        _append_text(encoded, node.ast_hash)
        for child_id in node.child_ids:
            _append_text(encoded, child_id)
            child_visible = visible[child_id]
            encoded.append(1 if child_visible else 0)
            if child_visible:
                _append_text(encoded, effective_hashes[child_id])
        return sha256(encoded).hexdigest()

    @staticmethod
    def _observe_visibility(span, level: SummaryLevel, outcome: str) -> None:
        attributes = {
            "summarization.level": level.name.lower(),
            "summarization.outcome": outcome,
        }
        span.add_event("summarization.visibility", attributes)
        _visibility_counter.add(1, attributes)

    @staticmethod
    def _postorder(nodes: dict[str, SummaryNode], root_id: str) -> list[SummaryNode]:
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
                    raise InvalidHierarchyError("child does not exist")
                if child.level >= node.level:
                    raise InvalidHierarchyError("invalid hierarchy levels")
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

    def _generate(self, node: SummaryNode, children: tuple[ChildSummary, ...]) -> str:
        try:
            summary = self._model.summarize(node, children)
        except Exception as exc:
            raise SummaryModelError("summary model failed") from exc
        if not summary.strip():
            raise SummaryModelError("summary model returned empty output")
        return summary
