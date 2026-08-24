"""Deterministic checks for structured code-answer claims."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

from opentelemetry import trace
from opentelemetry.trace import Tracer
from pydantic import BaseModel, ConfigDict, Field, model_validator
from tree_sitter_language_pack import get_parser


class SymbolClaim(BaseModel):
    model_config = ConfigDict(frozen=True)
    node_id: str = Field(min_length=1)


class RelationClaim(BaseModel):
    model_config = ConfigDict(frozen=True)
    source_id: str = Field(min_length=1)
    target_id: str = Field(min_length=1)
    edge_type: str = Field(min_length=1)
    max_depth: int = Field(default=1, ge=1)


class CitationClaim(BaseModel):
    model_config = ConfigDict(frozen=True)
    node_id: str = Field(min_length=1)
    repo: str = Field(min_length=1)
    path: str = Field(min_length=1)
    start_line: int = Field(ge=1)
    end_line: int = Field(ge=1)
    commit_sha: str = Field(min_length=1)

    @model_validator(mode="after")
    def valid_range(self) -> CitationClaim:
        if self.end_line < self.start_line:
            raise ValueError("end_line must be greater than or equal to start_line")
        return self


class SnippetClaim(BaseModel):
    model_config = ConfigDict(frozen=True)
    language: str = Field(min_length=1)
    source: str = Field(min_length=1)


class CandidateAnswer(BaseModel):
    model_config = ConfigDict(frozen=True)
    answer: str = Field(min_length=1)
    symbol_claims: tuple[SymbolClaim, ...] = ()
    relation_claims: tuple[RelationClaim, ...] = ()
    citations: tuple[CitationClaim, ...] = ()
    snippets: tuple[SnippetClaim, ...] = ()


class SymbolEvidence(BaseModel):
    model_config = ConfigDict(frozen=True)
    node_id: str = Field(min_length=1)
    repo: str = Field(min_length=1)
    path: str = Field(min_length=1)
    start_line: int = Field(ge=1)
    end_line: int = Field(ge=1)
    commit_sha: str = Field(min_length=1)


class VerificationIssue(BaseModel):
    model_config = ConfigDict(frozen=True)
    code: Literal[
        "symbol-hallucination",
        "relation-nonexistent",
        "stale-citation",
        "syntax-invalid",
    ]
    stage: Literal["symbol", "relation", "citation", "syntax"]
    detail: str


class VerificationResult(BaseModel):
    model_config = ConfigDict(frozen=True)
    outcome: Literal["approved", "corrected", "regenerated", "degraded"]
    issues: tuple[VerificationIssue, ...]
    corrected_citations: tuple[CitationClaim, ...]
    feedback: tuple[str, ...]
    attempt: int


@dataclass(frozen=True)
class VerificationConfig:
    max_regenerations: int = 2
    max_relation_depth: int = 4

    def __post_init__(self) -> None:
        if self.max_regenerations not in (2, 3):
            raise ValueError("max_regenerations must be 2 or 3")
        if self.max_relation_depth < 1:
            raise ValueError("max_relation_depth must be positive")


class EvidenceStore(Protocol):
    def symbol(self, node_id: str) -> SymbolEvidence | None: ...

    def relation_exists(
        self, source_id: str, target_id: str, edge_type: str, max_depth: int
    ) -> bool: ...


class SyntaxVerifier(Protocol):
    def is_valid(self, language: str, source: str) -> bool: ...


class UnsupportedLanguageError(ValueError):
    """The configured Tree-sitter language pack has no requested grammar."""


class TreeSitterSyntaxVerifier:
    """Re-parse source with a real Tree-sitter grammar."""

    def is_valid(self, language: str, source: str) -> bool:
        try:
            parser = get_parser(language)
        except (LookupError, ValueError) as exc:
            raise UnsupportedLanguageError(language) from exc
        tree = parser.parse(source.encode("utf-8"))
        return not tree.root_node.has_error


class VerificationBackendError(RuntimeError):
    """A deterministic oracle was unavailable; verification fails closed."""


class Verifier:
    def __init__(
        self,
        store: EvidenceStore,
        syntax: SyntaxVerifier,
        config: VerificationConfig | None = None,
        *,
        tracer: Tracer | None = None,
    ) -> None:
        self._store = store
        self._syntax = syntax
        self._config = config or VerificationConfig()
        self._tracer = tracer or trace.get_tracer(__name__)

    def verify(self, candidate: CandidateAnswer, *, attempt: int = 0) -> VerificationResult:
        if attempt < 0:
            raise ValueError("attempt must not be negative")
        with self._tracer.start_as_current_span("verification.verify") as span:
            span.set_attribute("verification.attempt", attempt)
            issues: list[VerificationIssue] = []
            feedback: list[str] = []
            evidence: dict[str, SymbolEvidence | None] = {}

            for claim in candidate.symbol_claims:
                found = self._symbol(claim.node_id)
                evidence[claim.node_id] = found
                if found is None:
                    issues.append(self._issue("symbol-hallucination", "symbol", claim.node_id))
                    feedback.append(f"symbol '{claim.node_id}' does not exist")

            for claim in candidate.relation_claims:
                depth = min(claim.max_depth, self._config.max_relation_depth)
                if not self._relation(claim, depth):
                    detail = f"{claim.source_id} -[{claim.edge_type}]-> {claim.target_id}"
                    issues.append(self._issue("relation-nonexistent", "relation", detail))
                    feedback.append(f"relation '{detail}' does not exist within depth {depth}")

            corrected: list[CitationClaim] = []
            for citation in candidate.citations:
                current = evidence.get(citation.node_id)
                if citation.node_id not in evidence:
                    current = self._symbol(citation.node_id)
                    evidence[citation.node_id] = current
                if current is None:
                    issues.append(self._issue("symbol-hallucination", "citation", citation.node_id))
                    feedback.append(f"citation symbol '{citation.node_id}' does not exist")
                    continue
                replacement = self._citation(current)
                corrected.append(replacement)
                if citation != replacement:
                    issues.append(self._issue("stale-citation", "citation", citation.node_id))

            for snippet in candidate.snippets:
                try:
                    valid = self._syntax.is_valid(snippet.language, snippet.source)
                    detail = snippet.language
                except UnsupportedLanguageError:
                    valid = False
                    detail = "unsupported-language"
                if not valid:
                    issues.append(self._issue("syntax-invalid", "syntax", detail))
                    feedback.append(f"snippet for language '{snippet.language}' is syntax-invalid")

            substantial = any(i.code != "stale-citation" for i in issues)
            if substantial:
                outcome = "degraded" if attempt >= self._config.max_regenerations else "regenerated"
            elif issues:
                outcome = "corrected"
            else:
                outcome = "approved"
            for issue in issues:
                span.add_event(
                    "verification.issue",
                    {"verification.issue_code": issue.code, "verification.stage": issue.stage},
                )
            span.set_attribute("verification.outcome", outcome)
            span.set_attribute("verification.issue_count", len(issues))
            return VerificationResult(
                outcome=outcome,
                issues=tuple(issues),
                corrected_citations=tuple(corrected),
                feedback=tuple(feedback),
                attempt=attempt,
            )

    def _symbol(self, node_id: str) -> SymbolEvidence | None:
        try:
            return self._store.symbol(node_id)
        except Exception as exc:
            raise VerificationBackendError("symbol check failed") from exc

    def _relation(self, claim: RelationClaim, depth: int) -> bool:
        try:
            return self._store.relation_exists(
                claim.source_id, claim.target_id, claim.edge_type, depth
            )
        except Exception as exc:
            raise VerificationBackendError("relation check failed") from exc

    @staticmethod
    def _citation(evidence: SymbolEvidence) -> CitationClaim:
        return CitationClaim(**evidence.model_dump())

    @staticmethod
    def _issue(code: str, stage: str, detail: str) -> VerificationIssue:
        return VerificationIssue(code=code, stage=stage, detail=detail)  # type: ignore[arg-type]
