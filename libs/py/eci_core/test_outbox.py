"""Tests for eci_core.outbox — SPEC-003 §3 scenario 5."""

import json
from pathlib import Path

import jsonschema
import pytest
from pydantic import ValidationError

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "jsonschema"
SCHEMA_PATH = REPO_ROOT / "contracts" / "jsonschema" / "outbox-event.json"


def load(name):
    return json.loads((FIXTURES / name).read_text())


def test_valid_outbox_event_passes_schema_and_model():
    from eci_core.outbox import OutboxEvent

    payload = load("outbox_event_valid.json")
    schema = json.loads(SCHEMA_PATH.read_text())

    jsonschema.validate(instance=payload, schema=schema)
    event = OutboxEvent.model_validate(payload)
    assert event.aggregate_type.value == "CodeNode"


def test_outbox_event_missing_aggregate_id_fails_schema_and_model():
    from eci_core.outbox import OutboxEvent

    payload = load("outbox_event_missing_aggregate_id.json")
    schema = json.loads(SCHEMA_PATH.read_text())

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=payload, schema=schema)

    with pytest.raises(ValidationError):
        OutboxEvent.model_validate(payload)
