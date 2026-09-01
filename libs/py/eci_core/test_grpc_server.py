from __future__ import annotations

import json
import threading
import time
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar

import grpc
import pytest

from eci_core.grpc_server import (
    METADATA_KEY,
    AuthenticatedServerInterceptor,
    AuthorizationUnavailable,
    Decision,
    OPAClient,
    security_context,
)
from eci_core.retrieval.v1.retrieval_pb2 import SecurityContext


class Authorizer:
    def __init__(self, decision=None, error=None):
        self.decision = decision or Decision(True, "allow")
        self.error = error
        self.calls = []

    def decide(self, context, subject, method):
        self.calls.append((subject, method))
        if self.error:
            raise self.error
        return self.decision


def valid_subject(**changes):
    values = {
        "tenant_id": "tenant-a",
        "user_id": "user-a",
        "allowed_repos": ["repo-a"],
        "acl_groups": ["developers"],
    }
    values.update(changes)
    return SecurityContext(**values)


def start_server(authorizer):
    observed = []

    def unary(request, context):
        subject = security_context()
        observed.append(subject)
        return subject.tenant_id.encode()

    def stream(request, context):
        subject = security_context()
        observed.append(subject)
        yield b"progress"
        yield subject.tenant_id.encode()

    handlers = grpc.method_handlers_generic_handler(
        "test.Authenticated",
        {
            "Unary": grpc.unary_unary_rpc_method_handler(
                unary, request_deserializer=lambda value: value, response_serializer=lambda value: value
            ),
            "Stream": grpc.unary_stream_rpc_method_handler(
                stream, request_deserializer=lambda value: value, response_serializer=lambda value: value
            ),
        },
    )
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=2),
        interceptors=(AuthenticatedServerInterceptor(authorizer),),
    )
    server.add_generic_rpc_handlers((handlers,))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    unary_call = channel.unary_unary(
        "/test.Authenticated/Unary",
        request_serializer=lambda value: value,
        response_deserializer=lambda value: value,
    )
    stream_call = channel.unary_stream(
        "/test.Authenticated/Stream",
        request_serializer=lambda value: value,
        response_deserializer=lambda value: value,
    )
    return server, channel, unary_call, stream_call, observed


def auth_metadata(subject=None):
    subject = subject or valid_subject()
    return ((METADATA_KEY, subject.SerializeToString(deterministic=True)),)


def test_unary_and_stream_receive_only_authenticated_metadata_context():
    authorizer = Authorizer()
    server, channel, unary, stream, observed = start_server(authorizer)
    try:
        assert unary(b"request-body-is-not-authority", metadata=auth_metadata()) == b"tenant-a"
        assert list(stream(b"ignored", metadata=auth_metadata())) == [b"progress", b"tenant-a"]
    finally:
        channel.close()
        server.stop(0).wait()

    assert len(authorizer.calls) == 2
    assert [call[1] for call in authorizer.calls] == [
        "/test.Authenticated/Unary",
        "/test.Authenticated/Stream",
    ]
    assert [item.tenant_id for item in observed] == ["tenant-a", "tenant-a"]
    with pytest.raises(RuntimeError, match="authenticated RPC"):
        security_context()


@pytest.mark.parametrize(
    "metadata",
    [
        (),
        ((METADATA_KEY, b"\xff\xff\xff"),),
        ((METADATA_KEY, valid_subject().SerializeToString()),) * 2,
        ((METADATA_KEY, b"x" * 8193),),
        auth_metadata(valid_subject(allowed_repos=[])),
        auth_metadata(valid_subject(acl_groups=["developers", "developers"])),
        auth_metadata(valid_subject(tenant_id=" tenant-a")),
    ],
)
def test_invalid_metadata_fails_before_authorizer_and_handler(metadata):
    authorizer = Authorizer()
    server, channel, unary, _, observed = start_server(authorizer)
    try:
        with pytest.raises(grpc.RpcError) as caught:
            unary(b"ignored", metadata=metadata)
        assert caught.value.code() == grpc.StatusCode.UNAUTHENTICATED
        assert caught.value.details() == "authentication required"
    finally:
        channel.close()
        server.stop(0).wait()
    assert authorizer.calls == []
    assert observed == []


@pytest.mark.parametrize(
    ("authorizer", "code", "detail"),
    [
        (Authorizer(Decision(False, "policy_denied")), grpc.StatusCode.PERMISSION_DENIED, "authorization denied"),
        (
            Authorizer(error=AuthorizationUnavailable("sensitive PDP detail")),
            grpc.StatusCode.UNAVAILABLE,
            "authorization service unavailable",
        ),
    ],
)
def test_deny_and_pdp_failure_are_fail_closed_and_non_disclosing(authorizer, code, detail):
    server, channel, unary, _, observed = start_server(authorizer)
    try:
        with pytest.raises(grpc.RpcError) as caught:
            unary(b"ignored", metadata=auth_metadata())
        assert caught.value.code() == code
        assert caught.value.details() == detail
    finally:
        channel.close()
        server.stop(0).wait()
    assert observed == []


class OPAHandler(BaseHTTPRequestHandler):
    response_body: ClassVar = {"result": {"allow": True, "reason": "allow"}}
    response_status = 200
    captured: ClassVar[list] = []

    def do_GET(self):
        if self.path == "/health":
            self.send_response(self.response_status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(self.response_body).encode())
            return
        self.send_error(404)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        type(self).captured.append(json.loads(raw))
        self.send_response(self.response_status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if isinstance(self.response_body, bytes):
            self.wfile.write(self.response_body)
        else:
            self.wfile.write(json.dumps(self.response_body).encode())

    def log_message(self, format, *args):
        pass


@pytest.fixture
def opa_server():
    OPAHandler.response_body = {"result": {"allow": True, "reason": "allow"}}
    OPAHandler.response_status = 200
    OPAHandler.captured = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), OPAHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class DeadlineContext:
    def time_remaining(self):
        return 1.0


def test_opa_client_sends_only_subject_and_method(monkeypatch, opa_server):
    monkeypatch.setenv("OPA_URL", opa_server)
    monkeypatch.setenv("OPA_ALLOW_INSECURE_HTTP", "true")
    monkeypatch.setenv("OPA_TIMEOUT", "100ms")
    client = OPAClient.from_environment("orchestrator")
    client.preflight()
    decision = client.decide(DeadlineContext(), valid_subject(trace_id="private-trace"), "/service/Ask")
    assert decision == Decision(True, "allow")
    assert OPAHandler.captured == [
        {
            "input": {
                "action": "/service/Ask",
                "subject": {
                    "tenant_id": "tenant-a",
                    "user_id": "user-a",
                    "allowed_repos": ["repo-a"],
                    "acl_groups": ["developers"],
                },
            }
        }
    ]


@pytest.mark.parametrize(
    ("url", "allow_http", "timeout"),
    [
        ("http://opa.internal:8181", "false", "100ms"),
        ("https://user:pass@opa.internal", "false", "100ms"),
        ("https://opa.internal?target=evil", "false", "100ms"),
        ("https://opa.internal", "false", "0s"),
        ("https://opa.internal", "false", "3s"),
    ],
)
def test_opa_configuration_rejects_unsafe_or_unbounded_values(monkeypatch, url, allow_http, timeout):
    monkeypatch.setenv("OPA_URL", url)
    monkeypatch.setenv("OPA_ALLOW_INSECURE_HTTP", allow_http)
    monkeypatch.setenv("OPA_TIMEOUT", timeout)
    with pytest.raises(ValueError, match="invalid OPA configuration"):
        OPAClient.from_environment("orchestrator")


def test_opa_invalid_and_oversized_responses_fail_closed(monkeypatch, opa_server):
    monkeypatch.setenv("OPA_URL", opa_server)
    monkeypatch.setenv("OPA_ALLOW_INSECURE_HTTP", "true")
    client = OPAClient.from_environment("orchestrator")

    for body in [
        {"result": {"allow": True, "reason": "policy_denied"}},
        {"result": {"allow": "yes", "reason": "allow"}},
        b"x" * (64 * 1024 + 1),
    ]:
        OPAHandler.response_body = body
        with pytest.raises(AuthorizationUnavailable):
            client.decide(DeadlineContext(), valid_subject(), "/service/Ask")


def test_opa_redirect_is_not_followed(monkeypatch, opa_server):
    monkeypatch.setenv("OPA_URL", opa_server)
    monkeypatch.setenv("OPA_ALLOW_INSECURE_HTTP", "true")
    client = OPAClient.from_environment("orchestrator")
    OPAHandler.response_status = 302
    with pytest.raises(AuthorizationUnavailable):
        client.preflight()


def test_opa_body_drip_cannot_extend_total_rpc_deadline(monkeypatch):
    class DripHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            payload = json.dumps({"result": {"allow": True, "reason": "allow"}}).encode()
            try:
                for byte in payload:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.02)  # each byte is faster than the old socket timeout
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), DripHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("OPA_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("OPA_ALLOW_INSECURE_HTTP", "true")
    monkeypatch.setenv("OPA_TIMEOUT", "120ms")
    client = OPAClient.from_environment("orchestrator")
    started = time.monotonic()
    try:
        with pytest.raises(AuthorizationUnavailable):
            client.decide(DeadlineContext(), valid_subject(), "/service/Ask")
    finally:
        elapsed = time.monotonic() - started
        server.shutdown()
        server.server_close()
        thread.join()
    assert elapsed < 0.4
