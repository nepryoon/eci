"""Tests for eci_core.code_node — SPEC-003 §3 scenari 2-3, §4 edge case.

datamodel-code-generator non mappa nativamente l'if/then/const di `domain` su
`ext` (verificato: collassa a `dict[str, Any] | None`), quindi qui si esercita
il fallback manuale previsto dalla SPEC: CodeNodeCode/Doc/Legal + dispatcher
parse_code_node().
"""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "jsonschema"


def load(name):
    return json.loads((FIXTURES / name).read_text())


def test_domain_code_valid_ext_is_typed_and_accessible():
    from eci_core.code_node import parse_code_node

    node = parse_code_node(load("codenode_code_valid.json"))

    assert node.ext.node_type.value == "Method"
    assert (
        node.ext.symbol_id
        == "scip-java maven acme orders 1.0 OrderService#charge()."
    )


def test_domain_legal_with_code_extension_fields_is_rejected():
    from eci_core.code_node import parse_code_node

    with pytest.raises(ValidationError):
        parse_code_node(load("codenode_legal_ext_mismatch.json"))


def test_domain_code_without_ext_is_rejected():
    from eci_core.code_node import parse_code_node

    with pytest.raises(ValidationError):
        parse_code_node(load("codenode_code_missing_ext.json"))


def test_domain_outside_enum_is_rejected_explicitly():
    from eci_core.code_node import parse_code_node

    with pytest.raises(ValueError, match="finance"):
        parse_code_node(load("codenode_domain_invalid.json"))
