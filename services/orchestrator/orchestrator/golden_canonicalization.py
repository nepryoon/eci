"""Deterministic, golden-independent canonicalization for golden evaluation."""

from __future__ import annotations

import re
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
VERBOSE_RE = re.compile(
    r"^([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\s+(.+)$", re.DOTALL
)
NODE_TYPES = frozenset(
    {"File", "Module", "Package", "Class", "Interface", "Method", "Function"}
)
EMPTY_SENTINEL = "__EMPTY__"


class CanonicalizationError(ValueError):
    """Fail-closed error raised when the structured symbol source is unavailable."""


class FactKind(StrEnum):
    callers = "callers"
    methods = "methods"
    implementations = "implementations"
    contains = "contains"
    node_type = "node_type"


class IssueCode(StrEnum):
    semantic_error = "semantic_error"
    unqualified_symbol = "unqualified_symbol"
    verbose_fact = "verbose_fact"
    unexpected_fact = "unexpected_fact"
    missing_fact = "missing_fact"
    ambiguous_canonicalization = "ambiguous_canonicalization"


class ResolutionStatus(StrEnum):
    exact = "exact"
    unique = "unique"
    ambiguous = "ambiguous"
    not_found = "not_found"


class StructuredFact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: FactKind
    value: str = Field(min_length=1)

    def flattened(self) -> str:
        return f"{self.kind.value}={self.value}"


class StructuredReply(BaseModel):
    model_config = ConfigDict(extra="forbid")
    facts: tuple[StructuredFact, ...]
    citations: tuple[str, ...] = ()


class ResolutionScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    repository: str = Field(min_length=1)


class Resolution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: ResolutionStatus
    canonical_identifier: str | None = None
    candidates: tuple[str, ...] = ()


class SymbolResolver(Protocol):
    def resolve(self, identifier: str, *, scope: ResolutionScope) -> Resolution: ...


class EvaluationIssue(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    code: IssueCode
    fact_index: int | None = None
    kind: FactKind
    raw_value: str | None = None
    canonical_value: str | None = None
    detail: str


class CanonicalizedFact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    fact_index: int
    raw_fact: StructuredFact
    canonical_fact: StructuredFact | None
    resolution: Resolution
    primary_issue: EvaluationIssue | None = None
    raw_was_canonical: bool = False


class FactComparison(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    exact_canonical_fact_recall: float
    semantic_entity_recall: float | None
    canonicalization_coverage: float
    exact_matched_count: int
    semantic_matched_count: int
    expected_count: int
    resolved_actual_count: int
    actual_count: int
    exact_matched_facts: tuple[str, ...]
    semantic_matched_facts: tuple[str, ...]
    unexpected_facts: tuple[str, ...]
    missing_facts: tuple[str, ...]
    issues: tuple[EvaluationIssue, ...]
    semantic_metric_limitations: tuple[str, ...] = ()


class InMemorySymbolResolver:
    """Exact leaf-name resolver over a deterministic structured symbol set."""

    def __init__(self, canonical_identifiers: list[str] | tuple[str, ...] | set[str]):
        identifiers = sorted(set(canonical_identifiers))
        invalid = [
            identifier
            for identifier in identifiers
            if not IDENTIFIER_RE.fullmatch(identifier)
        ]
        if invalid:
            raise ValueError(f"invalid canonical identifiers: {invalid}")
        self._identifiers = tuple(identifiers)
        by_leaf: dict[str, list[str]] = {}
        for identifier in identifiers:
            by_leaf.setdefault(identifier.rsplit(".", 1)[-1], []).append(identifier)
        self._by_leaf = {leaf: tuple(values) for leaf, values in by_leaf.items()}

    def resolve(self, identifier: str, *, scope: ResolutionScope) -> Resolution:
        del scope
        if identifier in self._identifiers:
            return Resolution(
                status=ResolutionStatus.exact,
                canonical_identifier=identifier,
                candidates=(identifier,),
            )
        candidates = self._by_leaf.get(identifier, ())
        if len(candidates) == 1:
            return Resolution(
                status=ResolutionStatus.unique,
                canonical_identifier=candidates[0],
                candidates=candidates,
            )
        if len(candidates) > 1:
            return Resolution(status=ResolutionStatus.ambiguous, candidates=candidates)
        return Resolution(status=ResolutionStatus.not_found)


def load_sample_repo_symbols(repo_root: Path) -> InMemorySymbolResolver:
    """Build the fixture symbol table from Go declarations, never golden data."""

    fixture = repo_root / "tests" / "fixtures" / "sample-repo"
    symbols: set[str] = set()
    type_pattern = re.compile(
        r"^\s*type\s+([A-Za-z_][A-Za-z0-9_]*)\s+(?:struct|interface)\b", re.MULTILINE
    )
    method_pattern = re.compile(
        r"^\s*func\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s+\*?([A-Za-z_][A-Za-z0-9_]*)\s*\)"
        r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(",
        re.MULTILINE,
    )
    function_pattern = re.compile(
        r"^\s*func\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.MULTILINE
    )
    interface_start_pattern = re.compile(
        r"^\s*type\s+([A-Za-z_][A-Za-z0-9_]*)\s+interface\s*\{"
    )
    interface_method_pattern = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(")
    source_files = sorted(fixture.glob("*.go"))
    if not source_files:
        raise FileNotFoundError(f"fixture repo mancante o vuoto: {fixture}")
    for source_file in source_files:
        source = source_file.read_text(encoding="utf-8")
        symbols.update(type_pattern.findall(source))
        symbols.update(
            f"{receiver}.{method}"
            for receiver, method in method_pattern.findall(source)
        )
        symbols.update(function_pattern.findall(source))
        interface_name: str | None = None
        brace_depth = 0
        for line in source.splitlines():
            if interface_name is None:
                start = interface_start_pattern.match(line)
                if start is None:
                    continue
                interface_name = start.group(1)
                brace_depth = line.count("{") - line.count("}")
                continue
            method = interface_method_pattern.match(line)
            if method is not None:
                symbols.add(f"{interface_name}.{method.group(1)}")
            brace_depth += line.count("{") - line.count("}")
            if brace_depth <= 0:
                interface_name = None
    return InMemorySymbolResolver(symbols)


def structured_facts(facts: dict[str, list[str] | str]) -> tuple[StructuredFact, ...]:
    result: list[StructuredFact] = []
    for raw_kind, values in facts.items():
        kind = FactKind(raw_kind)
        if isinstance(values, str):
            result.append(StructuredFact(kind=kind, value=values))
        elif values:
            result.extend(StructuredFact(kind=kind, value=value) for value in values)
        else:
            result.append(StructuredFact(kind=kind, value=EMPTY_SENTINEL))
    return tuple(result)


class DeterministicCanonicalizer:
    def __init__(self, symbols: SymbolResolver):
        self._symbols = symbols

    def canonicalize(
        self,
        facts: tuple[StructuredFact, ...],
        *,
        scope: ResolutionScope,
    ) -> tuple[CanonicalizedFact, ...]:
        return tuple(
            self._canonicalize_one(index, fact, scope)
            for index, fact in enumerate(facts)
        )

    def _resolve(self, identifier: str, scope: ResolutionScope) -> Resolution:
        try:
            return self._symbols.resolve(identifier, scope=scope)
        except Exception as exc:
            raise CanonicalizationError(
                f"symbol resolver failed for {identifier}"
            ) from exc

    def _canonicalize_one(
        self, index: int, fact: StructuredFact, scope: ResolutionScope
    ) -> CanonicalizedFact:
        if fact.value == EMPTY_SENTINEL:
            return self._resolved(
                index, fact, fact.value, ResolutionStatus.exact, raw_was_canonical=True
            )
        if fact.kind is FactKind.node_type:
            return self._canonicalize_node_type(index, fact)
        if fact.kind is FactKind.callers:
            return self._canonicalize_relation(index, fact, scope)
        verbose = VERBOSE_RE.fullmatch(fact.value)
        if verbose:
            identifier = verbose.group(1)
            resolution = self._resolve(identifier, scope)
            return self._from_resolution(
                index, fact, resolution, IssueCode.verbose_fact
            )
        if not IDENTIFIER_RE.fullmatch(fact.value):
            return self._failed(
                index, fact, ResolutionStatus.not_found, IssueCode.semantic_error
            )
        resolution = self._resolve(fact.value, scope)
        issue = (
            IssueCode.unqualified_symbol
            if resolution.status is ResolutionStatus.unique
            else None
        )
        return self._from_resolution(index, fact, resolution, issue)

    def _canonicalize_node_type(
        self, index: int, fact: StructuredFact
    ) -> CanonicalizedFact:
        if fact.value in NODE_TYPES:
            return self._resolved(
                index, fact, fact.value, ResolutionStatus.exact, raw_was_canonical=True
            )
        verbose = VERBOSE_RE.fullmatch(fact.value)
        if verbose and verbose.group(1) in NODE_TYPES:
            return self._resolved(
                index,
                fact,
                verbose.group(1),
                ResolutionStatus.exact,
                issue=IssueCode.verbose_fact,
            )
        return self._failed(
            index, fact, ResolutionStatus.not_found, IssueCode.semantic_error
        )

    def _canonicalize_relation(
        self, index: int, fact: StructuredFact, scope: ResolutionScope
    ) -> CanonicalizedFact:
        parts = fact.value.split(" <- ")
        if len(parts) != 2 or not all(IDENTIFIER_RE.fullmatch(part) for part in parts):
            return self._failed(
                index, fact, ResolutionStatus.not_found, IssueCode.semantic_error
            )
        resolutions = tuple(self._resolve(part, scope) for part in parts)
        if any(item.status is ResolutionStatus.ambiguous for item in resolutions):
            candidates = tuple(
                sorted(
                    {candidate for item in resolutions for candidate in item.candidates}
                )
            )
            return self._failed(
                index,
                fact,
                ResolutionStatus.ambiguous,
                IssueCode.ambiguous_canonicalization,
                candidates=candidates,
            )
        if any(item.status is ResolutionStatus.not_found for item in resolutions):
            return self._failed(
                index, fact, ResolutionStatus.not_found, IssueCode.semantic_error
            )
        canonical_value = " <- ".join(
            item.canonical_identifier or "" for item in resolutions
        )
        status = (
            ResolutionStatus.unique
            if any(item.status is ResolutionStatus.unique for item in resolutions)
            else ResolutionStatus.exact
        )
        issue = (
            IssueCode.unqualified_symbol if status is ResolutionStatus.unique else None
        )
        return self._resolved(
            index,
            fact,
            canonical_value,
            status,
            issue=issue,
            raw_was_canonical=status is ResolutionStatus.exact,
        )

    def _from_resolution(
        self,
        index: int,
        fact: StructuredFact,
        resolution: Resolution,
        issue: IssueCode | None,
    ) -> CanonicalizedFact:
        if resolution.status is ResolutionStatus.ambiguous:
            return self._failed(
                index,
                fact,
                resolution.status,
                issue
                if issue is IssueCode.verbose_fact
                else IssueCode.ambiguous_canonicalization,
                candidates=resolution.candidates,
            )
        if resolution.status is ResolutionStatus.not_found:
            return self._failed(
                index, fact, resolution.status, IssueCode.semantic_error
            )
        return self._resolved(
            index,
            fact,
            resolution.canonical_identifier or fact.value,
            resolution.status,
            issue=issue,
            candidates=resolution.candidates,
            raw_was_canonical=issue is None
            and resolution.status is ResolutionStatus.exact,
        )

    @staticmethod
    def _resolved(
        index: int,
        fact: StructuredFact,
        canonical_value: str,
        status: ResolutionStatus,
        *,
        issue: IssueCode | None = None,
        candidates: tuple[str, ...] = (),
        raw_was_canonical: bool = False,
    ) -> CanonicalizedFact:
        canonical = StructuredFact(kind=fact.kind, value=canonical_value)
        primary = None
        if issue is not None:
            primary = EvaluationIssue(
                code=issue,
                fact_index=index,
                kind=fact.kind,
                raw_value=fact.value,
                canonical_value=canonical_value,
                detail=f"{issue.value}: {fact.value}",
            )
        return CanonicalizedFact(
            fact_index=index,
            raw_fact=fact,
            canonical_fact=canonical,
            resolution=Resolution(
                status=status,
                canonical_identifier=canonical_value,
                candidates=candidates or (canonical_value,),
            ),
            primary_issue=primary,
            raw_was_canonical=raw_was_canonical,
        )

    @staticmethod
    def _failed(
        index: int,
        fact: StructuredFact,
        status: ResolutionStatus,
        issue: IssueCode,
        *,
        candidates: tuple[str, ...] = (),
    ) -> CanonicalizedFact:
        return CanonicalizedFact(
            fact_index=index,
            raw_fact=fact,
            canonical_fact=None,
            resolution=Resolution(status=status, candidates=candidates),
            primary_issue=EvaluationIssue(
                code=issue,
                fact_index=index,
                kind=fact.kind,
                raw_value=fact.value,
                detail=f"{issue.value}: {fact.value}",
            ),
        )


def compare_facts(
    actual: tuple[CanonicalizedFact, ...], expected: tuple[StructuredFact, ...]
) -> FactComparison:
    expected_flat = tuple(item.flattened() for item in expected)
    exact_actual = tuple(
        item.canonical_fact.flattened()
        for item in actual
        if item.canonical_fact is not None and item.raw_was_canonical
    )
    semantic_actual = tuple(
        item.canonical_fact.flattened()
        for item in actual
        if item.canonical_fact is not None
    )
    exact_matches = tuple(item for item in expected_flat if item in exact_actual)
    semantic_matches = tuple(item for item in expected_flat if item in semantic_actual)
    unexpected = tuple(item for item in semantic_actual if item not in expected_flat)
    missing = tuple(item for item in expected_flat if item not in semantic_actual)
    issues: list[EvaluationIssue] = [
        item.primary_issue for item in actual if item.primary_issue is not None
    ]
    for flattened in unexpected:
        kind, value = flattened.split("=", 1)
        issues.append(
            EvaluationIssue(
                code=IssueCode.unexpected_fact,
                kind=FactKind(kind),
                canonical_value=value,
                detail=f"unexpected canonical fact: {flattened}",
            )
        )
    for flattened in missing:
        kind, value = flattened.split("=", 1)
        issues.append(
            EvaluationIssue(
                code=IssueCode.missing_fact,
                kind=FactKind(kind),
                canonical_value=value,
                detail=f"missing canonical fact: {flattened}",
            )
        )
    expected_count = len(expected_flat)
    resolved = sum(item.canonical_fact is not None for item in actual)
    return FactComparison(
        exact_canonical_fact_recall=len(exact_matches) / expected_count
        if expected_count
        else 1.0,
        semantic_entity_recall=len(semantic_matches) / expected_count
        if expected_count
        else 1.0,
        canonicalization_coverage=resolved / len(actual) if actual else 1.0,
        exact_matched_count=len(exact_matches),
        semantic_matched_count=len(semantic_matches),
        expected_count=expected_count,
        resolved_actual_count=resolved,
        actual_count=len(actual),
        exact_matched_facts=exact_matches,
        semantic_matched_facts=semantic_matches,
        unexpected_facts=unexpected,
        missing_facts=missing,
        issues=tuple(issues),
    )
