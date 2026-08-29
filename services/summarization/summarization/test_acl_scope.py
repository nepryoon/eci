import pytest

from eci_core.retrieval.v1.retrieval_pb2 import SecurityContext
from summarization.raptor import (
    RaptorSummarizer,
    SummaryAuthorizationError,
    SummaryLevel,
    SummaryNode,
    acl_scope_fingerprint,
)

FINGERPRINT = "a" * 64
CANONICAL_SCOPE = "c94ba9d3ff0d97a5fd8414abbcbad8c01bdc54c35436a9a69496e0b0d184eafa"


def security_context(*, groups=("engineering", "readers"), repos=("repo-b", "repo-a"), user="user-a"):
    return SecurityContext(
        tenant_id="tenant-a",
        user_id=user,
        allowed_repos=repos,
        acl_groups=groups,
        trace_id="trace-must-not-affect-scope",
    )


class Cache:
    def __init__(self):
        self.values = {}
        self.gets = []
        self.puts = []

    def get(self, key):
        self.gets.append(key)
        return self.values.get((key.ast_hash, key.logic_fingerprint, key.acl_scope))

    def put(self, key, summary):
        self.puts.append(key)
        self.values[(key.ast_hash, key.logic_fingerprint, key.acl_scope)] = summary


class Model:
    def __init__(self):
        self.calls = []

    def summarize(self, node, children):
        self.calls.append((node.node_id, node.source, tuple(child.node_id for child in children)))
        return f"summary:{node.node_id}:" + ",".join(child.summary for child in children)


def node(node_id, level, char, *, children=(), source="", group="engineering", tenant="tenant-a", repo="repo-a"):
    return SummaryNode(
        node_id=node_id,
        level=level,
        ast_hash=char * 64,
        source=source,
        child_ids=children,
        tenant_id=tenant,
        repo=repo,
        acl_group=group,
    )


def mixed_tree(*, secret_group="secret"):
    return [
        node("visible", SummaryLevel.METHOD, "1", source="VISIBLE_SOURCE"),
        node("secret", SummaryLevel.METHOD, "2", source="TOP_SECRET", group=secret_group),
        node("class", SummaryLevel.CLASS, "3", children=("visible", "secret"), source="CLASS_CONTAINS_TOP_SECRET"),
        node("repo", SummaryLevel.REPO, "4", children=("class",), source="REPO_CONTAINS_TOP_SECRET"),
    ]


def test_acl_scope_cross_language_vector_is_canonical_and_user_independent():
    first = security_context()
    second = security_context(
        groups=("readers", "engineering", "readers"),
        repos=("repo-a", "repo-b", "repo-a"),
        user="another-user",
    )
    assert acl_scope_fingerprint(first) == CANONICAL_SCOPE
    assert acl_scope_fingerprint(second) == CANONICAL_SCOPE


def test_restricted_summary_never_exposes_denied_subtree_to_model_cache_or_result():
    cache, model = Cache(), Model()
    result = RaptorSummarizer(cache, model, FINGERPRINT).summarize(
        mixed_tree(), "repo", security_context()
    )

    assert set(result.summaries) == {"visible", "class", "repo"}
    assert all(call[0] != "secret" for call in model.calls)
    assert all("TOP_SECRET" not in call[1] for call in model.calls)
    assert next(call for call in model.calls if call[0] == "class") == (
        "class",
        "",
        ("visible",),
    )
    assert next(call for call in model.calls if call[0] == "repo")[1] == ""
    assert all(key.acl_scope == CANONICAL_SCOPE for key in cache.gets + cache.puts)
    assert not hasattr(result, "denied_nodes")


def test_acl_only_visibility_change_invalidates_aggregate_without_ast_change():
    cache, model = Cache(), Model()
    summarizer = RaptorSummarizer(cache, model, FINGERPRINT)
    full = summarizer.summarize(
        mixed_tree(secret_group="engineering"), "repo", security_context()
    )
    calls_after_full = len(model.calls)
    restricted = summarizer.summarize(mixed_tree(), "repo", security_context())

    assert full.cache_misses == 4
    assert restricted.cache_hits == 1  # visible leaf only
    assert restricted.cache_misses == 2  # class + repo restricted views
    assert len(model.calls) == calls_after_full + 2
    class_keys = [key.ast_hash for key in cache.puts if key.ast_hash != "1" * 64 and key.ast_hash != "2" * 64]
    assert len(set(class_keys)) == 4  # full/restricted class and root hashes differ


@pytest.mark.parametrize(
    "ctx,nodes",
    [
        (SecurityContext(), mixed_tree()),
        (security_context(groups=()), mixed_tree()),
        (security_context(), mixed_tree()[0:3] + [node("repo", SummaryLevel.REPO, "4", children=("class",), tenant="tenant-b")]),
    ],
)
def test_invalid_context_or_denied_root_fails_before_cache_and_model(ctx, nodes):
    cache, model = Cache(), Model()
    with pytest.raises(SummaryAuthorizationError, match="not authorized"):
        RaptorSummarizer(cache, model, FINGERPRINT).summarize(nodes, "repo", ctx)
    assert cache.gets == []
    assert cache.puts == []
    assert model.calls == []
