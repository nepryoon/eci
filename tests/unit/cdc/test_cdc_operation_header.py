"""Static contract tests for SPEC-071's authoritative CDC operation header."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PLACEMENT = (
    "trace_id:header:trace_id,id:header:event_id,"
    "event_type:header:event_type,event_sequence:header:event_sequence"
)


class CDCOperationHeaderTests(unittest.TestCase):
    def test_compose_connector_promotes_only_bounded_metadata(self) -> None:
        connector = json.loads(
            (ROOT / "deploy/compose/debezium-outbox-connector.json").read_text()
        )["config"]
        self.assertEqual(
            connector["transforms.outbox.table.fields.additional.placement"],
            PLACEMENT,
        )
        self.assertNotIn("payload:header", PLACEMENT)
        self.assertNotIn("aggregate_id:header", PLACEMENT)

    def test_helm_connector_uses_the_identical_placement(self) -> None:
        template = (ROOT / "deploy/k8s/eci-platform/templates/cdc.yaml").read_text()
        self.assertEqual(template.count(f'"{PLACEMENT}"'), 1)


if __name__ == "__main__":
    unittest.main()
