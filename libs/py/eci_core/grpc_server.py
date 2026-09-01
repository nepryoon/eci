"""Fail-closed authenticated gRPC server boundary for ECI Python runtimes."""

from __future__ import annotations

import json
import http.client
import math
import os
import re
import socket
import ssl
import threading
import time
import unicodedata
import urllib.parse
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Protocol

import grpc
from google.protobuf.message import DecodeError

from eci_core.retrieval.v1.retrieval_pb2 import SecurityContext

METADATA_KEY = "eci-security-context-bin"
MAX_METADATA_BYTES = 8 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
MAX_SCOPE_VALUES = 128
MAX_SCOPE_VALUE_BYTES = 256

_SERVICE_PATTERN = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_DURATION_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)(ms|s)$")
_KNOWN_REASONS = {
    "allow",
    "unknown_action",
    "missing_tenant",
    "missing_user",
    "empty_repo_scope",
    "empty_acl_scope",
    "policy_denied",
}
_current_subject: ContextVar[SecurityContext | None] = ContextVar(
    "eci_authenticated_security_context", default=None
)


@dataclass(frozen=True)
class Decision:
    allow: bool
    reason: str


class AuthorizationUnavailable(RuntimeError):
    """The fixed policy decision point did not return a trustworthy decision."""


class DecisionClient(Protocol):
    def decide(
        self,
        context: grpc.ServicerContext,
        subject: SecurityContext,
        method: str,
    ) -> Decision: ...


def security_context() -> SecurityContext:
    """Return the authenticated call-local subject, never a process default."""

    subject = _current_subject.get()
    if subject is None:
        raise RuntimeError("security context is available only inside an authenticated RPC")
    result = SecurityContext()
    result.CopyFrom(subject)
    return result


def _valid_scope_value(value: str) -> bool:
    return (
        bool(value)
        and value.strip() == value
        and len(value.encode("utf-8")) <= MAX_SCOPE_VALUE_BYTES
        and not any(unicodedata.category(char) == "Cc" for char in value)
    )


def _valid_subject(subject: SecurityContext) -> bool:
    repos = list(subject.allowed_repos)
    groups = list(subject.acl_groups)
    return (
        _valid_scope_value(subject.tenant_id)
        and _valid_scope_value(subject.user_id)
        and 0 < len(repos) <= MAX_SCOPE_VALUES
        and 0 < len(groups) <= MAX_SCOPE_VALUES
        and len(set(repos)) == len(repos)
        and len(set(groups)) == len(groups)
        and all(_valid_scope_value(item) for item in repos)
        and all(_valid_scope_value(item) for item in groups)
    )


def _extract_subject(metadata) -> SecurityContext | None:
    values = [item.value for item in metadata if item.key == METADATA_KEY]
    if len(values) != 1 or not isinstance(values[0], bytes):
        return None
    raw = values[0]
    if not raw or len(raw) > MAX_METADATA_BYTES:
        return None
    subject = SecurityContext()
    try:
        subject.ParseFromString(raw)
    except DecodeError:
        return None
    return subject if _valid_subject(subject) else None


class AuthenticatedServerInterceptor(grpc.ServerInterceptor):
    """Authenticate metadata and authorize every supported RPC shape."""

    def __init__(self, authorizer: DecisionClient) -> None:
        if authorizer is None:
            raise ValueError("authorizer is required")
        self._authorizer = authorizer

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        metadata = tuple(handler_call_details.invocation_metadata or ())
        method = handler_call_details.method

        def authenticate(context: grpc.ServicerContext) -> SecurityContext:
            subject = _extract_subject(metadata)
            if subject is None:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "authentication required")
            try:
                decision = self._authorizer.decide(context, subject, method)
            # The PEP must convert every PDP/client failure into the same
            # non-disclosing fail-closed status, including an unexpected
            # implementation error from an injected DecisionClient.
            except Exception:  # noqa: BLE001
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "authorization service unavailable",
                )
            if not isinstance(decision, Decision):
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "authorization service unavailable",
                )
            if not decision.allow:
                context.abort(grpc.StatusCode.PERMISSION_DENIED, "authorization denied")
            return subject

        if handler.unary_unary:
            behavior = handler.unary_unary

            def unary_unary(request, context):
                token = _current_subject.set(authenticate(context))
                try:
                    return behavior(request, context)
                finally:
                    _current_subject.reset(token)

            return grpc.unary_unary_rpc_method_handler(
                unary_unary,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        if handler.unary_stream:
            behavior = handler.unary_stream

            def unary_stream(request, context):
                token = _current_subject.set(authenticate(context))
                try:
                    yield from behavior(request, context)
                finally:
                    _current_subject.reset(token)

            return grpc.unary_stream_rpc_method_handler(
                unary_stream,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        if handler.stream_unary:
            behavior = handler.stream_unary

            def stream_unary(request_iterator, context):
                token = _current_subject.set(authenticate(context))
                try:
                    return behavior(request_iterator, context)
                finally:
                    _current_subject.reset(token)

            return grpc.stream_unary_rpc_method_handler(
                stream_unary,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        if handler.stream_stream:
            behavior = handler.stream_stream

            def stream_stream(request_iterator, context):
                token = _current_subject.set(authenticate(context))
                try:
                    yield from behavior(request_iterator, context)
                finally:
                    _current_subject.reset(token)

            return grpc.stream_stream_rpc_method_handler(
                stream_stream,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        # A method without a recognized behavior must never bypass the PEP.
        return grpc.unary_unary_rpc_method_handler(
            lambda _request, context: context.abort(
                grpc.StatusCode.UNIMPLEMENTED, "unsupported RPC shape"
            ),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


def _parse_timeout(raw: str) -> float:
    match = _DURATION_PATTERN.fullmatch(raw)
    if not match:
        raise ValueError("invalid OPA configuration")
    value = float(match.group(1))
    if match.group(2) == "ms":
        value /= 1000
    if not 0 < value <= 2:
        raise ValueError("invalid OPA configuration")
    return value


def _parse_bool(raw: str) -> bool:
    if raw == "true":
        return True
    if raw == "false":
        return False
    raise ValueError("invalid OPA configuration")


class OPAClient:
    """Strict client for the fixed ECI OPA decision document."""

    def __init__(
        self,
        endpoint: str,
        decision_path: str,
        service: str,
        timeout_seconds: float,
        allow_insecure_http: bool = False,
    ) -> None:
        try:
            parsed = urllib.parse.urlsplit(endpoint)
            _ = parsed.port
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid OPA configuration") from exc
        if (
            not _SERVICE_PATTERN.fullmatch(service)
            or not 0 < timeout_seconds <= 2
            or parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or (parsed.scheme == "http" and not allow_insecure_http)
            or not decision_path.startswith("/")
            or "//" in decision_path
            or any(char in decision_path for char in "?#")
        ):
            raise ValueError("invalid OPA configuration")
        self._base_url = endpoint.rstrip("/")
        self._parsed_endpoint = parsed
        self._decision_path = decision_path
        self._timeout = timeout_seconds

    def _request(
        self,
        method: str,
        path: str,
        timeout: float,
        body: bytes | None = None,
    ) -> tuple[int, bytes]:
        """Execute one OPA exchange under a true total wall-clock budget.

        ``http.client``'s socket timeout, like urllib's, is per blocking
        operation. A peer can otherwise drip one byte before every timeout and
        occupy an authorization worker indefinitely. A monotonic timer closes
        (and first shuts down) the live socket at the absolute deadline, which
        bounds DNS/connect, headers, and the complete body together.
        """

        if not math.isfinite(timeout) or timeout <= 0:
            raise TimeoutError("OPA request deadline exceeded")
        parsed = self._parsed_endpoint
        connection_type = (
            http.client.HTTPSConnection
            if parsed.scheme == "https"
            else http.client.HTTPConnection
        )
        kwargs: dict = {"timeout": timeout}
        if parsed.scheme == "https":
            kwargs["context"] = ssl.create_default_context()
        connection = connection_type(parsed.hostname, parsed.port, **kwargs)
        deadline = time.monotonic() + timeout
        # HTTP/1.0 responses without Content-Length transfer ownership of the
        # socket from the connection object to HTTPResponse. Retain the concrete
        # socket as soon as request() connects so the absolute-deadline timer can
        # still interrupt a drip-fed body after getresponse() detaches it.
        active_socket: list[socket.socket | None] = [None]

        def expire() -> None:
            sock = active_socket[0] or connection.sock
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            connection.close()

        timer = threading.Timer(timeout, expire)
        timer.daemon = True
        timer.start()
        try:
            headers = {"Accept": "application/json"}
            if body is not None:
                headers["Content-Type"] = "application/json"
            connection.request(method, path, body=body, headers=headers)
            active_socket[0] = connection.sock
            response = connection.getresponse()
            raw = response.read(MAX_RESPONSE_BYTES + 1)
            if time.monotonic() >= deadline:
                raise TimeoutError("OPA request deadline exceeded")
            return response.status, raw
        finally:
            timer.cancel()
            connection.close()

    @classmethod
    def from_environment(cls, service: str) -> OPAClient:
        try:
            timeout = _parse_timeout(os.getenv("OPA_TIMEOUT", "100ms"))
            allow_insecure = _parse_bool(
                os.getenv("OPA_ALLOW_INSECURE_HTTP", "false")
            )
            return cls(
                endpoint=os.getenv("OPA_URL", ""),
                decision_path=os.getenv(
                    "OPA_DECISION_PATH", "/v1/data/eci/authz/decision"
                ),
                service=service,
                timeout_seconds=timeout,
                allow_insecure_http=allow_insecure,
            )
        except ValueError as exc:
            raise ValueError("invalid OPA configuration") from exc

    def preflight(self) -> None:
        try:
            status, raw = self._request("GET", "/health", self._timeout)
            if status != 200 or len(raw) > MAX_RESPONSE_BYTES:
                raise AuthorizationUnavailable("OPA health check failed")
        except (OSError, http.client.HTTPException) as exc:
            raise AuthorizationUnavailable("OPA health check failed") from exc

    def decide(
        self,
        context: grpc.ServicerContext,
        subject: SecurityContext,
        method: str,
    ) -> Decision:
        timeout = self._timeout
        try:
            remaining = context.time_remaining()
        except (AttributeError, TypeError):
            remaining = None
        if remaining is not None:
            if not isinstance(remaining, (int, float)) or not math.isfinite(remaining) or remaining <= 0:
                raise AuthorizationUnavailable("OPA decision deadline exceeded")
            timeout = min(timeout, remaining)
        body = json.dumps(
            {
                "input": {
                    "subject": {
                        "tenant_id": subject.tenant_id,
                        "user_id": subject.user_id,
                        "allowed_repos": list(subject.allowed_repos),
                        "acl_groups": list(subject.acl_groups),
                    },
                    "action": method,
                }
            },
            separators=(",", ":"),
        ).encode()
        try:
            status, raw = self._request("POST", self._decision_path, timeout, body)
            if status != 200:
                raise AuthorizationUnavailable("OPA decision failed")
        except (OSError, http.client.HTTPException) as exc:
            raise AuthorizationUnavailable("OPA decision failed") from exc
        if len(raw) > MAX_RESPONSE_BYTES:
            raise AuthorizationUnavailable("OPA decision failed")
        try:
            decoded = json.loads(raw)
            result = decoded["result"]
            allow = result["allow"]
            reason = result["reason"]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise AuthorizationUnavailable("OPA decision failed") from exc
        if not isinstance(allow, bool) or not isinstance(reason, str):
            raise AuthorizationUnavailable("OPA decision failed")
        normalized = reason if reason in _KNOWN_REASONS else "policy_denied"
        if allow and normalized != "allow":
            raise AuthorizationUnavailable("OPA decision failed")
        if not allow and normalized == "allow":
            normalized = "policy_denied"
        return Decision(allow=allow, reason=normalized)
