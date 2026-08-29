"""Deterministic checks for structured code-answer claims."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import uuid4

from eci_core.retrieval.v1.retrieval_pb2 import SecurityContext
from opentelemetry import metrics, trace
from opentelemetry.trace import Tracer
from pydantic import BaseModel, ConfigDict, Field, model_validator
from tree_sitter_language_pack import get_parser

from verification.audit import (
    AuditSink,
    CitationAccess,
    VerificationAuditError,
    VerificationAuditEvent,
)

MAX_SCOPE_VALUES = 128
MAX_SCOPE_VALUE_BYTES = 256
MAX_NODE_ID_LENGTH = 1024

_citation_access_counter = metrics.get_meter(__name__).create_counter(
    "eci_verification_citation_access_total",
    description="Citation authorization decisions with bounded outcome labels.",
)
_audit_append_counter = metrics.get_meter(__name__).create_counter(
    "eci_verification_audit_append_total",
    description="Mandatory verification audit append outcomes.",
)


class SymbolClaim(BaseModel):
    model_config = ConfigDict(frozen=True)
    node_id: str = Field(min_length=1, max_length=MAX_NODE_ID_LENGTH)


class RelationClaim(BaseModel):
    model_config = ConfigDict(frozen=True)
    source_id: str = Field(min_length=1, max_length=MAX_NODE_ID_LENGTH)
    target_id: str = Field(min_length=1, max_length=MAX_NODE_ID_LENGTH)
    edge_type: str = Field(min_length=1)
    max_depth: int = Field(default=1, ge=1)


class CitationClaim(BaseModel):
    model_config = ConfigDict(frozen=True)
    node_id: str = Field(min_length=1, max_length=MAX_NODE_ID_LENGTH)
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
    node_id: str = Field(min_length=1, max_length=MAX_NODE_ID_LENGTH)
    tenant_id: str = Field(min_length=1)
    repo: str = Field(min_length=1)
    acl_group: str = Field(min_length=1)
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
        "citation-inaccessible",
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


@dataclass(frozen=True)
class AuthorizationScope:
    tenant_id: str
    user_id: str
    allowed_repos: tuple[str, ...]
    acl_groups: tuple[str, ...]
    trace_id: str


class VerificationAuthorizationError(PermissionError):
    """Authenticated metadata is absent or invalid; verification fails closed."""


class EvidenceStore(Protocol):
    def symbol(
        self, node_id: str, scope: AuthorizationScope
    ) -> SymbolEvidence | None: ...

    def relation_exists(
        self,
        source_id: str,
        target_id: str,
        edge_type: str,
        max_depth: int,
        scope: AuthorizationScope,
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
        audit: AuditSink,
        config: VerificationConfig | None = None,
        *,
        tracer: Tracer | None = None,
    ) -> None:
        self._store = store
        self._syntax = syntax
        self._audit = audit
        self._config = config or VerificationConfig()
        self._tracer = tracer or trace.get_tracer(__name__)

    def verify(
        self,
        candidate: CandidateAnswer,
        security_context: SecurityContext,
        *,
        attempt: int = 0,
    ) -> VerificationResult:
        if attempt < 0:
            raise ValueError("attempt must not be negative")
        scope = self._authorization_scope(security_context)
        with self._tracer.start_as_current_span("verification.verify") as span:
            span.set_attribute("verification.attempt", attempt)
            issues: list[VerificationIssue] = []
            feedback: list[str] = []
            evidence: dict[str, SymbolEvidence | None] = {}
            citation_access: list[CitationAccess] = []

            for claim in candidate.symbol_claims:
                found = self._symbol(claim.node_id, scope)
                evidence[claim.node_id] = found
                if found is None:
                    issues.append(self._issue("symbol-hallucination", "symbol", claim.node_id))
                    feedback.append(f"symbol '{claim.node_id}' does not exist")

            for claim in candidate.relation_claims:
                depth = min(claim.max_depth, self._config.max_relation_depth)
                endpoints_authorized = True
                for node_id in (claim.source_id, claim.target_id):
                    if node_id not in evidence:
                        evidence[node_id] = self._symbol(node_id, scope)
                    if evidence[node_id] is None:
                        endpoints_authorized = False
                if not endpoints_authorized or not self._relation(claim, depth, scope):
                    detail = f"{claim.source_id} -[{claim.edge_type}]-> {claim.target_id}"
                    issues.append(self._issue("relation-nonexistent", "relation", detail))
                    feedback.append(f"relation '{detail}' does not exist within depth {depth}")

            corrected: list[CitationClaim] = []
            for citation in candidate.citations:
                current = evidence.get(citation.node_id)
                if citation.node_id not in evidence:
                    current = self._symbol(citation.node_id, scope)
                    evidence[citation.node_id] = current
                if current is None:
                    issues.append(
                        self._issue(
                            "citation-inaccessible", "citation", "citation unavailable"
                        )
                    )
                    feedback.append("citation is unavailable")
                    citation_access.append(
                        CitationAccess(
                            node_id=citation.node_id, decision="inaccessible"
                        )
                    )
                    _citation_access_counter.add(1, {"decision": "inaccessible"})
                    continue
                citation_access.append(
                    CitationAccess(node_id=citation.node_id, decision="authorized")
                )
                _citation_access_counter.add(1, {"decision": "authorized"})
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
            result = VerificationResult(
                outcome=outcome,
                issues=tuple(issues),
                corrected_citations=tuple(corrected),
                feedback=tuple(feedback),
                attempt=attempt,
            )
            event = VerificationAuditEvent(
                event_id=uuid4(),
                recorded_at=datetime.now(UTC),
                trace_id=scope.trace_id,
                tenant_id=scope.tenant_id,
                user_id=scope.user_id,
                allowed_repos=scope.allowed_repos,
                acl_groups=scope.acl_groups,
                attempt=attempt,
                outcome=outcome,
                issue_codes=tuple(issue.code for issue in issues),
                citation_access=tuple(citation_access),
            )
            try:
                self._audit.append(event)
            except Exception:  # noqa: BLE001 -- an audit backend may raise any SDK error
                span.set_attribute("verification.audit_status", "failed")
                span.add_event("verification.audit_failure")
                _audit_append_counter.add(1, {"outcome": "failed"})
                raise VerificationAuditError("audit append failed") from None
            span.set_attribute("verification.audit_status", "written")
            _audit_append_counter.add(1, {"outcome": "written"})
            return result

    def _symbol(
        self, node_id: str, scope: AuthorizationScope
    ) -> SymbolEvidence | None:
        try:
            found = self._store.symbol(node_id, scope)
        except Exception as exc:
            raise VerificationBackendError("symbol check failed") from exc
        if found is None or not self._authorized(found, scope):
            return None
        return found

    def _relation(
        self, claim: RelationClaim, depth: int, scope: AuthorizationScope
    ) -> bool:
        try:
            return self._store.relation_exists(
                claim.source_id, claim.target_id, claim.edge_type, depth, scope
            )
        except Exception as exc:
            raise VerificationBackendError("relation check failed") from exc

    @staticmethod
    def _citation(evidence: SymbolEvidence) -> CitationClaim:
        return CitationClaim(
            node_id=evidence.node_id,
            repo=evidence.repo,
            path=evidence.path,
            start_line=evidence.start_line,
            end_line=evidence.end_line,
            commit_sha=evidence.commit_sha,
        )

    @staticmethod
    def _authorized(evidence: SymbolEvidence, scope: AuthorizationScope) -> bool:
        return (
            evidence.tenant_id == scope.tenant_id
            and evidence.repo in scope.allowed_repos
            and evidence.acl_group in scope.acl_groups
        )

    @staticmethod
    def _authorization_scope(context: SecurityContext) -> AuthorizationScope:
        tenant = context.tenant_id
        user = context.user_id
        trace_id = context.trace_id
        repos = Verifier._normalize_scope_values(context.allowed_repos)
        groups = Verifier._normalize_scope_values(context.acl_groups)
        if not all(Verifier._valid_scope_value(v) for v in (tenant, user, trace_id)):
            raise VerificationAuthorizationError("not authorized")
        if repos is None or groups is None:
            raise VerificationAuthorizationError("not authorized")
        return AuthorizationScope(
            tenant_id=tenant,
            user_id=user,
            allowed_repos=repos,
            acl_groups=groups,
            trace_id=trace_id,
        )

    @staticmethod
    def _normalize_scope_values(values: tuple[str, ...] | list[str]) -> tuple[str, ...] | None:
        if not values or len(values) > MAX_SCOPE_VALUES:
            return None
        if any(not Verifier._valid_scope_value(value) for value in values):
            return None
        return tuple(sorted(set(values)))

    @staticmethod
    def _valid_scope_value(value: str) -> bool:
        return (
            bool(value)
            and value.strip() == value
            and len(value.encode("utf-8")) <= MAX_SCOPE_VALUE_BYTES
            and not any(unicodedata.category(char) == "Cc" for char in value)
        )

    @staticmethod
    def _issue(code: str, stage: str, detail: str) -> VerificationIssue:
        return VerificationIssue(code=code, stage=stage, detail=detail)  # type: ignore[arg-type]
