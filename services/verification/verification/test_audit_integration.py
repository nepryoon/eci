import os
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO

import pytest
from minio import Minio
from minio.commonconfig import COMPLIANCE
from minio.error import S3Error
from minio.retention import Retention

from verification.audit import (
    CitationAccess,
    MinioWormAuditSink,
    VerificationAuditEvent,
    canonical_event_bytes,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("ECI_RUN_MINIO_INTEGRATION") != "1",
        reason="set ECI_RUN_MINIO_INTEGRATION=1 with MinIO at ECI_MINIO_ENDPOINT",
    ),
]


def test_minio_compliance_object_is_versioned_immutable_and_readable():
    endpoint = os.getenv("ECI_MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("ECI_MINIO_ACCESS_KEY", "eci")
    secret_key = os.getenv("ECI_MINIO_SECRET_KEY", "eci-dev-only-minio")
    bucket = os.getenv("ECI_MINIO_AUDIT_BUCKET", "eci-verification-audit-test")
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    sink = MinioWormAuditSink(client, bucket, retention_days=1)
    sink.initialize()
    event = VerificationAuditEvent(
        event_id="018f47e2-9ad4-7c18-a123-123456789abc",
        recorded_at=datetime.now(UTC), trace_id="integration-trace",
        tenant_id="tenant-a", user_id="alice", allowed_repos=("repo-a",),
        acl_groups=("dev",), attempt=0, outcome="approved", issue_codes=(),
        citation_access=(CitationAccess(node_id="A", decision="authorized"),),
    )

    receipt = sink.append(event)
    retention = client.get_object_retention(bucket, receipt.object_name, version_id=receipt.version_id)
    assert retention is not None and retention.mode == "COMPLIANCE"
    assert retention.retain_until_date >= event.recorded_at

    response = client.get_object(bucket, receipt.object_name, version_id=receipt.version_id)
    try:
        assert response.read() == canonical_event_bytes(event)
    finally:
        response.close()
        response.release_conn()

    tampered = b'{"tampered":true}'
    replacement = client.put_object(
        bucket,
        receipt.object_name,
        BytesIO(tampered),
        len(tampered),
        retention=Retention(COMPLIANCE, receipt.retain_until),
    )
    assert replacement.version_id != receipt.version_id
    assert sha256(tampered).hexdigest() not in receipt.object_name

    with pytest.raises(S3Error) as deleted:
        client.remove_object(bucket, receipt.object_name, version_id=receipt.version_id)
    assert deleted.value.code in {"AccessDenied", "InvalidRequest", "MethodNotAllowed"}

    response = client.get_object(bucket, receipt.object_name, version_id=receipt.version_id)
    try:
        assert response.read() == canonical_event_bytes(event)
    finally:
        response.close()
        response.release_conn()
