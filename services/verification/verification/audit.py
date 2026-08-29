"""Immutable, canonical audit events for deterministic verification decisions."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from io import BytesIO
from typing import Any, Literal, Protocol
from uuid import UUID

from minio.commonconfig import COMPLIANCE
from minio.retention import Retention
from pydantic import BaseModel, ConfigDict, Field, field_validator


class CitationAccess(BaseModel):
    model_config = ConfigDict(frozen=True)
    node_id: str = Field(min_length=1, max_length=1024)
    decision: Literal["authorized", "inaccessible"]


class VerificationAuditEvent(BaseModel):
    model_config = ConfigDict(frozen=True)
    schema_version: Literal["eci.verification.audit.v1"] = "eci.verification.audit.v1"
    event_id: UUID
    recorded_at: datetime
    trace_id: str = Field(min_length=1, max_length=256)
    tenant_id: str = Field(min_length=1, max_length=256)
    user_id: str = Field(min_length=1, max_length=256)
    allowed_repos: tuple[str, ...] = Field(min_length=1, max_length=128)
    acl_groups: tuple[str, ...] = Field(min_length=1, max_length=128)
    attempt: int = Field(ge=0)
    outcome: Literal["approved", "corrected", "regenerated", "degraded"]
    issue_codes: tuple[str, ...]
    citation_access: tuple[CitationAccess, ...]

    @field_validator("recorded_at")
    @classmethod
    def utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("recorded_at must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("allowed_repos", "acl_groups")
    @classmethod
    def canonical_scope(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))


class AuditReceipt(BaseModel):
    model_config = ConfigDict(frozen=True)
    object_name: str
    version_id: str = Field(min_length=1)
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    retain_until: datetime


class AuditSink(Protocol):
    def append(self, event: VerificationAuditEvent) -> AuditReceipt: ...


class VerificationAuditError(RuntimeError):
    """The mandatory audit decision could not be durably appended."""


class WormAuditConfigurationError(RuntimeError):
    """The audit backend does not provide the required WORM guarantees."""


def canonical_event_bytes(event: VerificationAuditEvent) -> bytes:
    """Serialize an allow-listed event with stable ordering and no whitespace."""

    return json.dumps(
        event.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class InMemoryAuditSink:
    """Observable unit-test sink; production wiring must use a WORM sink."""

    def __init__(self) -> None:
        self.events: list[VerificationAuditEvent] = []
        self.serialized_events: list[str] = []

    def append(self, event: VerificationAuditEvent) -> AuditReceipt:
        payload = canonical_event_bytes(event)
        digest = sha256(payload).hexdigest()
        self.events.append(event)
        self.serialized_events.append(payload.decode("utf-8"))
        return AuditReceipt(
            object_name=f"memory/{event.event_id}-{digest}.json",
            version_id="memory-v1",
            payload_sha256=digest,
            retain_until=event.recorded_at + timedelta(days=1),
        )


class MinioWormAuditSink:
    """Append canonical events under S3 Object Lock COMPLIANCE retention."""

    def __init__(
        self,
        client: Any,
        bucket: str,
        retention_days: int = 365,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if not bucket:
            raise ValueError("bucket must not be empty")
        if retention_days < 1:
            raise ValueError("retention_days must be at least 1")
        self._client = client
        self._bucket = bucket
        self._retention_days = retention_days
        self._now = now or (lambda: datetime.now(UTC))
        self._initialized = False

    def initialize(self) -> None:
        try:
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket, object_lock=True)
            # A bucket created without Object Lock rejects this API. Object Lock
            # cannot be enabled later, so there is intentionally no fallback.
            self._client.get_object_lock_config(self._bucket)
        except Exception as exc:
            raise WormAuditConfigurationError(
                "audit bucket must have Object Lock enabled"
            ) from exc
        self._initialized = True

    def append(self, event: VerificationAuditEvent) -> AuditReceipt:
        if not self._initialized:
            raise WormAuditConfigurationError("audit sink is not initialized")
        now = self._now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise WormAuditConfigurationError("audit clock must be timezone-aware")
        now = now.astimezone(UTC)
        retain_until = now + timedelta(days=self._retention_days)
        payload = canonical_event_bytes(event)
        digest = sha256(payload).hexdigest()
        date_path = event.recorded_at.astimezone(UTC).strftime("%Y/%m/%d")
        object_name = f"audit/v1/{date_path}/{event.event_id}-{digest}.json"
        retention = Retention(COMPLIANCE, retain_until)
        try:
            result = self._client.put_object(
                self._bucket,
                object_name,
                BytesIO(payload),
                len(payload),
                content_type="application/json",
                retention=retention,
            )
            version_id = result.version_id
            actual = self._client.get_object_retention(
                self._bucket, object_name, version_id=version_id
            )
        except Exception as exc:
            raise WormAuditConfigurationError("WORM audit append failed") from exc
        if (
            not version_id
            or actual is None
            or actual.mode != COMPLIANCE
            or actual.retain_until_date is None
            or actual.retain_until_date.astimezone(UTC) < retain_until
        ):
            raise WormAuditConfigurationError(
                "server did not confirm COMPLIANCE retention"
            )
        return AuditReceipt(
            object_name=object_name,
            version_id=version_id,
            payload_sha256=digest,
            retain_until=actual.retain_until_date.astimezone(UTC),
        )

