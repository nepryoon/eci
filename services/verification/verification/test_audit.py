from datetime import UTC, datetime, timedelta

import pytest

from verification.audit import (
    CitationAccess,
    MinioWormAuditSink,
    VerificationAuditEvent,
    WormAuditConfigurationError,
    canonical_event_bytes,
)

NOW = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)


def event():
    return VerificationAuditEvent(
        event_id="018f47e2-9ad4-7c18-a123-123456789abc",
        recorded_at=NOW,
        trace_id="trace-123",
        tenant_id="tenant-a",
        user_id="alice",
        allowed_repos=("repo-b", "repo-a"),
        acl_groups=("ops", "dev"),
        attempt=0,
        outcome="regenerated",
        issue_codes=("citation-inaccessible",),
        citation_access=(CitationAccess(node_id="node-a", decision="inaccessible"),),
    )


class FakeWrite:
    version_id = "v1"


class FakeClient:
    def __init__(self, *, exists=False, bad_retention=False):
        self.exists = exists
        self.bad_retention = bad_retention
        self.made = []
        self.puts = []

    def bucket_exists(self, bucket):
        return self.exists

    def make_bucket(self, bucket, object_lock=False):
        self.made.append((bucket, object_lock))
        self.exists = True

    def get_object_lock_config(self, bucket):
        return object()

    def put_object(self, bucket, key, data, length, **kwargs):
        payload = data.read()
        self.puts.append((bucket, key, payload, length, kwargs))
        return FakeWrite()

    def get_object_retention(self, bucket, key, version_id=None):
        retention = self.puts[0][4]["retention"]
        return None if self.bad_retention else retention


def test_canonical_event_bytes_are_stable_sorted_and_minimal():
    raw = canonical_event_bytes(event())
    assert raw == canonical_event_bytes(event())
    assert raw.startswith(b'{"acl_groups":["dev","ops"],')
    assert b"repo-a" in raw
    assert b"query" not in raw and b"answer" not in raw and b"source" not in raw


def test_minio_sink_creates_locked_bucket_and_verifies_compliance_retention():
    client = FakeClient()
    sink = MinioWormAuditSink(client, "eci-audit", retention_days=30, now=lambda: NOW)
    sink.initialize()
    receipt = sink.append(event())

    assert client.made == [("eci-audit", True)]
    assert receipt.version_id == "v1"
    assert receipt.retain_until == NOW + timedelta(days=30)
    assert receipt.object_name.startswith("audit/v1/2026/08/29/")
    assert receipt.payload_sha256 in receipt.object_name
    retention = client.puts[0][4]["retention"]
    assert retention.mode == "COMPLIANCE"
    assert retention.retain_until_date == NOW + timedelta(days=30)


def test_existing_bucket_must_expose_object_lock_configuration():
    class Unlocked(FakeClient):
        def get_object_lock_config(self, bucket):
            raise RuntimeError("ObjectLockConfigurationNotFoundError")

    with pytest.raises(WormAuditConfigurationError, match="Object Lock"):
        MinioWormAuditSink(Unlocked(exists=True), "eci-audit", now=lambda: NOW).initialize()


def test_retention_mismatch_fails_closed():
    sink = MinioWormAuditSink(FakeClient(bad_retention=True), "eci-audit", now=lambda: NOW)
    sink.initialize()
    with pytest.raises(WormAuditConfigurationError, match="retention"):
        sink.append(event())


@pytest.mark.parametrize("days", [0, -1])
def test_invalid_retention_is_rejected(days):
    with pytest.raises(ValueError, match="retention_days"):
        MinioWormAuditSink(FakeClient(), "eci-audit", retention_days=days)

