import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from orchestrator.golden_canonicalization import InMemorySymbolResolver
from orchestrator.golden_eval import (
    LOGIC_FINGERPRINT,
    PROMPT_CONTRACT_VERSION,
    _build_messages,
    run_golden_eval,
)


def dataset(tmp_path):
    path = tmp_path / "golden.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": "g1",
                    "query": "q1",
                    "expected_facts": {"callers": ["B <- A"]},
                    "scope_note": "s",
                },
                {
                    "id": "g2",
                    "query": "q2",
                    "expected_facts": {"callers": []},
                    "scope_note": "s",
                },
                {
                    "id": "g3",
                    "query": "q3",
                    "expected_facts": {"node_type": "Class"},
                    "scope_note": "s",
                },
            ]
        )
    )
    return path


@pytest.fixture
def server():
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.append(body)
            question = body["messages"][1]["content"].splitlines()[0]
            if question == "Question: q3":
                self.send_response(503)
                self.end_headers()
                return
            content = {
                "Question: q1": {
                    "facts": [{"kind": "callers", "value": "B <- A"}],
                    "citations": ["tests/fixtures/sample-repo/order_service.go"],
                },
                "Question: q2": {
                    "facts": [{"kind": "callers", "value": "__EMPTY__"}],
                    "citations": ["tests/fixtures/sample-repo/main.go"],
                },
            }[question]
            payload = json.dumps(
                {
                    "choices": [
                        {"message": {"content": f"```json\n{json.dumps(content)}\n```"}}
                    ],
                    "usage": {"prompt_tokens": 2, "completion_tokens": 3},
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}", seen
    httpd.shutdown()
    thread.join()


def test_real_http_run_continues_after_error_and_writes_atomic_artifacts(
    tmp_path, server
):
    url, seen = server
    output = tmp_path / "result.jsonl"
    summary = run_golden_eval(
        dataset(tmp_path),
        url,
        "real-model",
        output,
        symbols=InMemorySymbolResolver(["A", "B"]),
    )
    records = [json.loads(line) for line in output.read_text().splitlines()]
    assert [record["query_id"] for record in records] == ["g1", "g2", "g3"]
    assert records[0]["matched_facts"] == ["callers=B <- A"]
    assert records[1]["matched_facts"] == ["callers=__EMPTY__"]
    assert records[1]["passed"] is True
    assert records[2]["error"] == "HTTPStatusError"
    assert seen[0]["temperature"] == 0
    assert any(
        "Repository context" in message["content"] for message in seen[0]["messages"]
    )
    assert any(
        "order_service.go" in message["content"] for message in seen[0]["messages"]
    )
    assert summary["fact_recall"] == pytest.approx(2 / 3)
    assert summary["pass_rate"] == pytest.approx(2 / 3)
    assert summary["exact_canonical_fact_recall"] == pytest.approx(2 / 3)
    assert summary["semantic_entity_recall"] == pytest.approx(2 / 3)
    assert summary["prompt_contract_version"] == PROMPT_CONTRACT_VERSION
    assert summary["logic_fingerprint"] == LOGIC_FINGERPRINT
    assert json.loads(output.with_suffix(".jsonl.summary.json").read_text()) == summary


def test_validation_fake_guard_and_existing_output_happen_before_requests(
    tmp_path, server
):
    url, seen = server
    output = tmp_path / "result.jsonl"
    with pytest.raises(ValueError, match="require-real"):
        run_golden_eval(dataset(tmp_path), url, "vllm-fake", output, require_real=True)
    output.write_text("keep")
    with pytest.raises(FileExistsError):
        run_golden_eval(dataset(tmp_path), url, "real", output)
    assert seen == []


def test_invalid_dataset_fails_before_network(tmp_path, server):
    url, seen = server
    bad = tmp_path / "bad.json"
    bad.write_text('[{"id":"x"}]')
    with pytest.raises(ValueError):
        run_golden_eval(bad, url, "real", tmp_path / "out")
    unsupported = tmp_path / "unsupported.json"
    unsupported.write_text(
        json.dumps(
            [
                {
                    "id": "x",
                    "query": "q",
                    "expected_facts": {"golden_only_kind": ["secret"]},
                    "scope_note": "s",
                }
            ]
        )
    )
    with pytest.raises(ValueError):
        run_golden_eval(unsupported, url, "real", tmp_path / "out")
    assert seen == []


def test_prompt_contract_is_fixed_and_does_not_observe_expected_or_scope_note():
    first = type(
        "Entry",
        (),
        {
            "query": "same",
            "expected_facts": {"callers": ["Secret"]},
            "scope_note": "leak A",
        },
    )
    second = type(
        "Entry",
        (),
        {
            "query": "same",
            "expected_facts": {"methods": ["Other"]},
            "scope_note": "leak B",
        },
    )

    assert _build_messages(first, "repository") == _build_messages(second, "repository")
    rendered = json.dumps(_build_messages(first, "repository"), ensure_ascii=False)
    assert "Secret" not in rendered
    assert "leak A" not in rendered
    assert "canonical identifiers" in rendered


def test_canonical_scalar_fact_is_normalized(tmp_path, server):
    url, _ = server
    path = tmp_path / "scalar.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": "g10",
                    "query": "q1",
                    "expected_facts": {"callers": ["B <- A"]},
                    "scope_note": "s",
                }
            ]
        )
    )
    output = tmp_path / "out.jsonl"
    summary = run_golden_eval(
        path, url, "real", output, symbols=InMemorySymbolResolver(["A", "B"])
    )
    assert summary["fact_recall"] == 1.0


def test_negative_expectation_is_counted_and_empty_answer_passes(tmp_path, server):
    url, _ = server
    path = tmp_path / "negative.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": "g2",
                    "query": "q2",
                    "expected_facts": {"callers": []},
                    "scope_note": "s",
                }
            ]
        )
    )
    output = tmp_path / "negative.jsonl"
    summary = run_golden_eval(path, url, "real", output)
    record = json.loads(output.read_text().splitlines()[0])
    assert record["matched_facts"] == ["callers=__EMPTY__"]
    assert summary["fact_recall"] == 1.0


def test_negative_expectation_fails_when_model_invents_fact(tmp_path):
    path = tmp_path / "negative.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": "g2",
                    "query": "q2",
                    "expected_facts": {"callers": []},
                    "scope_note": "s",
                }
            ]
        )
    )
    output = tmp_path / "invented.jsonl"

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "facts": [
                                            {
                                                "kind": "callers",
                                                "value": "InventedCaller",
                                            }
                                        ],
                                        "citations": [
                                            "tests/fixtures/sample-repo/order_service.go"
                                        ],
                                    }
                                )
                            }
                        }
                    ],
                    "usage": {"prompt_tokens": 2, "completion_tokens": 3},
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever)
    thread.start()
    try:
        summary = run_golden_eval(
            path, f"http://127.0.0.1:{httpd.server_port}", "real", output
        )
    finally:
        httpd.shutdown()
        thread.join()

    record = json.loads(output.read_text().splitlines()[0])
    assert record["matched_facts"] == []
    assert record["unexpected_facts"] == ["callers=InventedCaller"]
    assert record["passed"] is False
    assert summary["fact_recall"] == 0.0
    assert summary["pass_rate"] == 0.0


def test_legacy_reply_shape_is_rejected_without_canonicalization(tmp_path):
    path = tmp_path / "legacy.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": "g1",
                    "query": "q1",
                    "expected_facts": {"contains": ["main"]},
                    "scope_note": "must not enter the prompt",
                }
            ]
        )
    )
    output = tmp_path / "legacy.jsonl"

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {"facts": {"contains": ["main"]}, "citations": []}
                                )
                            }
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever)
    thread.start()
    try:
        summary = run_golden_eval(
            path, f"http://127.0.0.1:{httpd.server_port}", "real", output
        )
    finally:
        httpd.shutdown()
        thread.join()

    record = json.loads(output.read_text().splitlines()[0])
    assert record["error"] == "ValidationError"
    assert record["canonicalized_facts"] == []
    assert summary["exact_canonical_fact_recall"] == 0.0
    assert summary["semantic_entity_recall"] == 0.0


def test_fact_and_citation_correctness_are_independent(tmp_path):
    path = tmp_path / "citation.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": "g1",
                    "query": "q1",
                    "expected_facts": {
                        "callers": ["OrderService.Validate <- OrderService.Process"]
                    },
                    "scope_note": "s",
                }
            ]
        )
    )
    output = tmp_path / "citation.jsonl"
    citation = "tests/fixtures/sample-repo/order_service.go"

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "facts": [
                                            {"kind": "callers", "value": citation}
                                        ],
                                        "citations": [citation],
                                    }
                                )
                            }
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever)
    thread.start()
    try:
        run_golden_eval(path, f"http://127.0.0.1:{httpd.server_port}", "real", output)
    finally:
        httpd.shutdown()
        thread.join()

    record = json.loads(output.read_text().splitlines()[0])
    assert record["citations"] == [citation]
    assert record["matched_facts"] == []
    assert [issue["code"] for issue in record["evaluation_issues"]] == [
        "semantic_error",
        "missing_fact",
    ]


def test_raw_equality_cannot_credit_an_identifier_absent_from_symbol_table(tmp_path):
    path = tmp_path / "unknown.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": "g1",
                    "query": "q1",
                    "expected_facts": {"contains": ["Unknown"]},
                    "scope_note": "s",
                }
            ]
        )
    )
    output = tmp_path / "unknown.jsonl"

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "facts": [
                                            {"kind": "contains", "value": "Unknown"}
                                        ],
                                        "citations": [],
                                    }
                                )
                            }
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever)
    thread.start()
    try:
        summary = run_golden_eval(
            path,
            f"http://127.0.0.1:{httpd.server_port}",
            "real",
            output,
            symbols=InMemorySymbolResolver([]),
        )
    finally:
        httpd.shutdown()
        thread.join()

    record = json.loads(output.read_text().splitlines()[0])
    assert record["matched_facts"] == ["contains=Unknown"]
    assert record["exact_canonical_matched_facts"] == []
    assert record["passed"] is False
    assert summary["fact_recall"] == 1.0
    assert summary["exact_canonical_fact_recall"] == 0.0
    assert summary["semantic_entity_recall"] == 0.0


def test_resolver_outage_marks_semantic_metric_unavailable(tmp_path, server):
    class FailingResolver:
        def resolve(self, identifier, *, scope):
            raise ConnectionError(f"unavailable: {identifier} {scope.repository}")

    url, _ = server
    output = tmp_path / "resolver-outage.jsonl"
    summary = run_golden_eval(
        dataset(tmp_path), url, "real", output, symbols=FailingResolver()
    )
    records = [json.loads(line) for line in output.read_text().splitlines()]

    assert records[0]["error"] == "CanonicalizationError"
    assert records[0]["semantic_entity_recall"] is None
    assert records[0]["semantic_metric_limitations"] == [
        {
            "code": "symbol_resolver_unavailable",
            "query_id": "g1",
            "detail": "symbol resolver failed for B",
        }
    ]
    assert summary["semantic_entity_recall"] is None
    assert summary["semantic_metric_limitations"] == [
        {
            "code": "symbol_resolver_unavailable",
            "query_id": "g1",
            "detail": "symbol resolver failed for B",
        }
    ]
