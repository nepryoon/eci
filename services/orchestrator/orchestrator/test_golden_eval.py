import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from orchestrator.golden_eval import run_golden_eval


def dataset(tmp_path):
    path = tmp_path / "golden.json"
    path.write_text(json.dumps([
        {"id": "g1", "query": "q1", "expected_facts": {"facts": ["A calls B"]}, "scope_note": "s"},
        {"id": "g2", "query": "q2", "expected_facts": {"facts": ["missing"]}, "scope_note": "s"},
    ]))
    return path


@pytest.fixture
def server():
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.append(body)
            if body["messages"][0]["content"] == "q2":
                self.send_response(503)
                self.end_headers()
                return
            payload = json.dumps({"choices": [{"message": {"content": "A calls B"}}], "usage": {"prompt_tokens": 2, "completion_tokens": 3}}).encode()
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


def test_real_http_run_continues_after_error_and_writes_atomic_artifacts(tmp_path, server):
    url, seen = server
    output = tmp_path / "result.jsonl"
    summary = run_golden_eval(dataset(tmp_path), url, "real-model", output)
    records = [json.loads(line) for line in output.read_text().splitlines()]
    assert [record["query_id"] for record in records] == ["g1", "g2"]
    assert records[0]["matched_facts"] == ["A calls B"]
    assert records[1]["error"] == "HTTPStatusError"
    assert seen[0]["temperature"] == 0
    assert summary["fact_recall"] == 0.5
    assert json.loads(output.with_suffix(".jsonl.summary.json").read_text()) == summary


def test_validation_fake_guard_and_existing_output_happen_before_requests(tmp_path, server):
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
    assert seen == []
