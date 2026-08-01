"""Tests for eci_core.models — SPEC-003 §3 scenario 1, §7 round-trip."""

import json
from pathlib import Path

import jsonschema
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "jsonschema"
SCHEMA_PATH = REPO_ROOT / "contracts" / "jsonschema" / "hybrid-graph.json"


def test_models_module_importable():
    from eci_core import models

    assert hasattr(models, "CodeNode")
    assert hasattr(models, "CodeRelation")


@pytest.mark.parametrize(
    "fixture_name",
    [
        "codenode_code_valid.json",
        "codenode_doc_valid.json",
        "codenode_legal_valid.json",
    ],
)
def test_generated_model_roundtrip_revalidates_against_schema(fixture_name):
    from eci_core.code_node import parse_code_node

    payload = json.loads((FIXTURES / fixture_name).read_text())
    node = parse_code_node(payload)

    serialized = json.loads(node.model_dump_json(exclude_none=True))

    schema = json.loads(SCHEMA_PATH.read_text())
    jsonschema.validate(instance=serialized, schema=schema)
