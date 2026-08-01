"""Discriminated CodeNode dispatch — SPEC-003 §2.

datamodel-code-generator non mappa nativamente su `domain` il vincolo
if/then/const di hybrid-graph.json (`ext` collassa a `dict[str, Any] | None`
nel modello generato in models.py — verificato eseguendo il generator).
Fallback esplicitamente previsto dalla SPEC: tre modelli, uno per dominio,
più un dispatcher che seleziona il modello giusto in base a
`payload["domain"]`. Ognuno stringe `ext` al tipo concreto e lo rende
obbligatorio (a differenza del campo opzionale nel modello base generato).
"""

from __future__ import annotations

from typing import Literal

from eci_core.models import (
    CodeExtension,
    CodeNode,
    DocExtension,
    DomainEnum,
    LegalExtension,
)


class CodeNodeCode(CodeNode):
    domain: Literal[DomainEnum.code] = DomainEnum.code
    ext: CodeExtension


class CodeNodeDoc(CodeNode):
    domain: Literal[DomainEnum.doc] = DomainEnum.doc
    ext: DocExtension


class CodeNodeLegal(CodeNode):
    domain: Literal[DomainEnum.legal] = DomainEnum.legal
    ext: LegalExtension


_DISPATCH = {
    DomainEnum.code.value: CodeNodeCode,
    DomainEnum.doc.value: CodeNodeDoc,
    DomainEnum.legal.value: CodeNodeLegal,
}


def parse_code_node(payload: dict) -> CodeNodeCode | CodeNodeDoc | CodeNodeLegal:
    domain = payload.get("domain")
    model = _DISPATCH.get(domain)
    if model is None:
        raise ValueError(f"unsupported domain for CodeNode dispatch: {domain!r}")
    return model.model_validate(payload)
