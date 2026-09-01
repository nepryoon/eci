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


@pytest.mark.parametrize(
    "aggregate_type", ["CodeNode", "CodeRelation", "CodeChunk", "CodeEmbedding"]
)
def test_every_materialized_aggregate_accepts_delete(aggregate_type):
    from eci_core.outbox import OutboxEvent

    payload = {
        "id": "11111111-1111-1111-1111-111111111111",
        "aggregate_type": aggregate_type,
        "aggregate_id": "entity-id",
        "event_type": "DELETE",
        "event_sequence": 42,
        "payload": {},
        "created_at": "2025-01-01T00:00:00Z",
    }
    schema = json.loads(SCHEMA_PATH.read_text())

    jsonschema.validate(instance=payload, schema=schema)
    assert OutboxEvent.model_validate(payload).aggregate_type.value == aggregate_type


@pytest.mark.parametrize("sequence", [0, 9223372036854775808])
def test_event_sequence_matches_positive_postgres_bigint(sequence):
    from eci_core.outbox import OutboxEvent

    payload = load("outbox_event_valid.json")
    payload["event_sequence"] = sequence
    schema = json.loads(SCHEMA_PATH.read_text())
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=payload, schema=schema)
    with pytest.raises(ValidationError):
        OutboxEvent.model_validate(payload)
