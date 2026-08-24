import pytest
from pydantic import ValidationError

from summarization.raptor import (
    InvalidHierarchyError,
    RaptorSummarizer,
    SummaryCacheError,
    SummaryLevel,
    SummaryModelError,
    SummaryNode,
)

H = {
    name: char * 64
    for name, char in zip(
        ("m", "s", "c", "o", "r", "m2", "c2", "o2", "r2"), "123456789"
    )
}
FINGERPRINT = "a" * 64


class Cache:
    def __init__(self):
        self.values = {}
        self.puts = []

    def get(self, key):
        return self.values.get((key.ast_hash, key.logic_fingerprint))

    def put(self, key, summary):
        self.values[(key.ast_hash, key.logic_fingerprint)] = summary
        self.puts.append(key.ast_hash)


class Model:
    def __init__(self):
        self.calls = []

    def summarize(self, node, children):
        self.calls.append((node.node_id, tuple(child.node_id for child in children)))
        return f"summary:{node.node_id}:" + ",".join(child.summary for child in children)


def tree(hashes=H):
    return [
        SummaryNode(node_id="method", level=SummaryLevel.METHOD, ast_hash=hashes["m"], source="code"),
        SummaryNode(node_id="stable", level=SummaryLevel.METHOD, ast_hash=hashes["s"]),
        SummaryNode(node_id="class", level=SummaryLevel.CLASS, ast_hash=hashes["c"], child_ids=("method", "stable")),
        SummaryNode(node_id="module", level=SummaryLevel.MODULE, ast_hash=hashes["o"], child_ids=("class",)),
        SummaryNode(node_id="repo", level=SummaryLevel.REPO, ast_hash=hashes["r"], child_ids=("module",)),
    ]


def test_bottom_up_then_second_run_is_all_cache_hits():
    cache, model = Cache(), Model()
    gate = RaptorSummarizer(cache, model, FINGERPRINT)
    first = gate.summarize(tree(), "repo")
    second = gate.summarize(tree(), "repo")
    assert [call[0] for call in model.calls] == ["method", "stable", "class", "module", "repo"]
    assert first.cache_misses == 5
    assert second.cache_hits == 5
    assert len(model.calls) == 5


def test_selective_invalidation_regenerates_only_changed_path():
    cache, model = Cache(), Model()
    RaptorSummarizer(cache, model, FINGERPRINT).summarize(tree(), "repo")
    changed = dict(H, m=H["m2"], c=H["c2"], o=H["o2"], r=H["r2"])
    result = RaptorSummarizer(cache, model, FINGERPRINT).summarize(tree(changed), "repo")
    assert result.cache_misses == 4
    assert result.cache_hits == 1
    assert len(model.calls) == 9


def test_child_order_is_preserved():
    nodes = [
        SummaryNode(node_id="a", level=SummaryLevel.METHOD, ast_hash="1" * 64),
        SummaryNode(node_id="b", level=SummaryLevel.METHOD, ast_hash="2" * 64),
        SummaryNode(node_id="c", level=SummaryLevel.CLASS, ast_hash="3" * 64, child_ids=("b", "a")),
    ]
    model = Model()
    RaptorSummarizer(Cache(), model, FINGERPRINT).summarize(nodes, "c")
    assert model.calls[-1] == ("c", ("b", "a"))


@pytest.mark.parametrize("nodes,root", [([], "x"), (tree() + [tree()[0]], "repo")])
def test_missing_root_and_duplicate_ids_fail(nodes, root):
    with pytest.raises(InvalidHierarchyError):
        RaptorSummarizer(Cache(), Model(), FINGERPRINT).summarize(nodes, root)


def test_cycle_missing_child_and_invalid_level_fail_before_model():
    cases = [
        [SummaryNode(node_id="a", level=SummaryLevel.CLASS, ast_hash="1"*64, child_ids=("b",)), SummaryNode(node_id="b", level=SummaryLevel.METHOD, ast_hash="2"*64, child_ids=("a",))],
        [SummaryNode(node_id="a", level=SummaryLevel.CLASS, ast_hash="1"*64, child_ids=("missing",))],
        [SummaryNode(node_id="a", level=SummaryLevel.METHOD, ast_hash="1"*64, child_ids=("b",)), SummaryNode(node_id="b", level=SummaryLevel.CLASS, ast_hash="2"*64)],
    ]
    for nodes in cases:
        model = Model()
        with pytest.raises(InvalidHierarchyError):
            RaptorSummarizer(Cache(), model, FINGERPRINT).summarize(nodes, "a")
        assert model.calls == []


def test_empty_model_output_and_cache_failure_fail_closed():
    class Empty(Model):
        def summarize(self, node, children):
            return " "
    with pytest.raises(SummaryModelError):
        RaptorSummarizer(Cache(), Empty(), FINGERPRINT).summarize(tree(), "repo")
    class Broken(Cache):
        def get(self, key):
            raise RuntimeError("down")
    with pytest.raises(SummaryCacheError):
        RaptorSummarizer(Broken(), Model(), FINGERPRINT).summarize(tree(), "repo")


def test_invalid_hash_is_rejected():
    with pytest.raises(ValidationError):
        SummaryNode(node_id="x", level=SummaryLevel.METHOD, ast_hash="bad")
