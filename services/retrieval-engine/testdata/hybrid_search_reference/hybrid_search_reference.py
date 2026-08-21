"""
hybrid_graph_vector_search
Estrazione congiunta Neo4j (graph traversal tipizzato) + Qdrant (vector search),
fusione RRF (k=60, coerente col Modulo 1), scoring con prossimita topologica.
Dipendenze reali: neo4j (driver ufficiale), qdrant-client.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from neo4j import GraphDatabase, Driver
from neo4j.exceptions import Neo4jError
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client import models as qmodels

logger = logging.getLogger("eci.hybrid_search")

RRF_K: int = 60  # invariante dal Modulo 1


@dataclass
class Provenance:
    repo: str
    path: str
    start_line: int
    end_line: int
    commit: str


@dataclass
class RetrievedNode:
    node_id: str
    domain: str
    source: str                       # "graph" | "vector" | "fused"
    vector_score: Optional[float] = None
    hop_distance: Optional[int] = None
    graph_rank: Optional[int] = None
    vector_rank: Optional[int] = None
    rrf_score: float = 0.0
    combined_score: float = 0.0
    provenance: Optional[Provenance] = None
    payload: dict[str, Any] = field(default_factory=dict)


class HybridSearchError(RuntimeError):
    pass


def _embed_query(query: str, embedder) -> list[float]:
    """Embedding della query. `embedder` espone .encode(str)->list[float]."""
    try:
        vec = embedder.encode(query)
        return list(vec)
    except Exception as exc:  # noqa: BLE001
        raise HybridSearchError(f"Embedding fallito: {exc}") from exc


def _vector_search(
    qdrant: QdrantClient,
    collection: str,
    query_vector: list[float],
    domain: Optional[str],
    repo: Optional[str],
    limit: int,
) -> list[RetrievedNode]:
    """Vector search su Qdrant con filtri di payload (domain/repo)."""
    must: list[qmodels.FieldCondition] = []
    if domain:
        must.append(qmodels.FieldCondition(
            key="domain", match=qmodels.MatchValue(value=domain)))
    if repo:
        must.append(qmodels.FieldCondition(
            key="repo", match=qmodels.MatchValue(value=repo)))
    query_filter = qmodels.Filter(must=must) if must else None

    try:
        resp = qdrant.query_points(
            collection_name=collection,
            query=query_vector,
            query_filter=query_filter,
            limit=limit,
            with_payload=True,
        ).points
    except (UnexpectedResponse, ValueError) as exc:
        raise HybridSearchError(f"Qdrant query fallita: {exc}") from exc

    out: list[RetrievedNode] = []
    for rank, p in enumerate(resp, start=1):
        payload = p.payload or {}
        prov = None
        if all(k in payload for k in ("repo", "path", "start_line", "end_line", "commit")):
            prov = Provenance(
                repo=payload["repo"], path=payload["path"],
                start_line=int(payload["start_line"]),
                end_line=int(payload["end_line"]), commit=payload["commit"],
            )
        out.append(RetrievedNode(
            node_id=str(payload.get("node_id", p.id)),
            domain=str(payload.get("domain", "code")),
            source="vector", vector_score=float(p.score),
            vector_rank=rank, provenance=prov, payload=payload,
        ))
    return out


# Traversata inversa tipizzata con limite di profondita e pruning DISTINCT.
# Restituisce nodi distinti (non cammini) -> abilita Pruning Var-Expand di Neo4j.
_GRAPH_TRAVERSAL_CYPHER = """
MATCH (seed:CodeNode {id: $entry_node_id})
MATCH path = (seed)<-[r:CALLS|IMPLEMENTS|EXTENDS|OVERRIDES|DEPENDS_ON|IMPORTS*1..%d]-(dep:CodeNode)
WHERE ($domain IS NULL OR dep.domain = $domain)
  AND ($repo   IS NULL OR dep.repo   = $repo)
WITH DISTINCT dep, min(length(path)) AS hop_distance
RETURN dep.id            AS node_id,
       dep.domain        AS domain,
       dep.repo          AS repo,
       dep.path          AS path,
       dep.start_line    AS start_line,
       dep.end_line      AS end_line,
       dep.commit        AS commit,
       hop_distance      AS hop_distance
ORDER BY hop_distance ASC
LIMIT $graph_limit
"""


def _graph_traversal(
    driver: Driver,
    entry_node_id: str,
    max_depth: int,
    domain: Optional[str],
    repo: Optional[str],
    graph_limit: int,
) -> list[RetrievedNode]:
    """Traversata inversa da entry_node_id, bounded a max_depth, pruning DISTINCT."""
    if max_depth < 1:
        raise ValueError("max_depth deve essere >= 1")
    cypher = _GRAPH_TRAVERSAL_CYPHER % int(max_depth)  # depth inline: literal, non user input
    try:
        with driver.session() as session:
            records = session.run(
                cypher,
                entry_node_id=entry_node_id,
                domain=domain, repo=repo, graph_limit=graph_limit,
            ).data()
    except Neo4jError as exc:
        raise HybridSearchError(f"Neo4j traversal fallito: {exc}") from exc

    out: list[RetrievedNode] = []
    for rank, rec in enumerate(records, start=1):
        prov = None
        if rec.get("path") and rec.get("commit") is not None:
            prov = Provenance(
                repo=rec.get("repo", ""), path=rec["path"],
                start_line=int(rec.get("start_line") or 0),
                end_line=int(rec.get("end_line") or 0), commit=rec["commit"],
            )
        out.append(RetrievedNode(
            node_id=str(rec["node_id"]), domain=str(rec.get("domain", "code")),
            source="graph", hop_distance=int(rec["hop_distance"]),
            graph_rank=rank, provenance=prov,
        ))
    return out


def _rrf_fuse(
    graph_nodes: list[RetrievedNode],
    vector_nodes: list[RetrievedNode],
    k: int = RRF_K,
) -> dict[str, RetrievedNode]:
    """Reciprocal Rank Fusion, dedup per node_id. score = sum 1/(k + rank)."""
    fused: dict[str, RetrievedNode] = {}

    def _merge(n: RetrievedNode, rank: Optional[int]) -> None:
        if rank is None:
            return
        contrib = 1.0 / (k + rank)
        if n.node_id not in fused:
            fused[n.node_id] = RetrievedNode(
                node_id=n.node_id, domain=n.domain, source="fused",
                vector_score=n.vector_score, hop_distance=n.hop_distance,
                graph_rank=n.graph_rank, vector_rank=n.vector_rank,
                provenance=n.provenance, payload=dict(n.payload),
            )
        else:
            ex = fused[n.node_id]
            candidates = [x for x in (ex.hop_distance, n.hop_distance) if x is not None]
            ex.hop_distance = min(candidates) if candidates else None
            ex.vector_score = ex.vector_score if ex.vector_score is not None else n.vector_score
            ex.graph_rank = ex.graph_rank if ex.graph_rank is not None else n.graph_rank
            ex.vector_rank = ex.vector_rank if ex.vector_rank is not None else n.vector_rank
            if ex.provenance is None and n.provenance is not None:
                ex.provenance = n.provenance
        fused[n.node_id].rrf_score += contrib

    for n in graph_nodes:
        _merge(n, n.graph_rank)
    for n in vector_nodes:
        _merge(n, n.vector_rank)
    return fused


def _apply_topological_proximity(
    fused: dict[str, RetrievedNode],
    beta: float = 0.15,
) -> list[RetrievedNode]:
    """Scoring combinato: RRF + boost di prossimita topologica al nodo di impatto."""
    results: list[RetrievedNode] = []
    for n in fused.values():
        prox = 1.0 / (1 + n.hop_distance) if n.hop_distance is not None else 0.0
        n.combined_score = n.rrf_score + beta * prox
        results.append(n)
    results.sort(key=lambda x: x.combined_score, reverse=True)
    return results


def hybrid_graph_vector_search(
    query: str,
    entry_node_id: str,
    max_depth: int,
    *,
    driver: Driver,
    qdrant: QdrantClient,
    embedder: Any,
    collection: str = "code_nodes",
    domain: Optional[str] = "code",
    repo: Optional[str] = None,
    vector_limit: int = 50,
    graph_limit: int = 200,
    top_k: int = 25,
    beta: float = 0.15,
) -> list[RetrievedNode]:
    """
    Estrazione ibrida Neo4j + Qdrant per impact analysis.

    Fasi: (1) embedding query; (2) vector search Qdrant con filtri payload
    domain/repo (seed-finding); (3) traversata tipizzata inversa su Neo4j da
    entry_node_id, bounded a max_depth con pruning DISTINCT; (4) fusione RRF
    (k=60); (5) dedup per node_id + scoring con prossimita topologica.

    Ritorna: lista di RetrievedNode ordinata per combined_score, con provenance.
    """
    if not query or not entry_node_id:
        raise ValueError("query ed entry_node_id sono obbligatori")

    # (1) embedding
    qvec = _embed_query(query, embedder)

    # (2) vector search (seed-finding) — degrada senza abortire in caso di errore
    try:
        vector_nodes = _vector_search(
            qdrant, collection, qvec, domain, repo, vector_limit)
    except HybridSearchError as exc:
        logger.warning("Vector search degradata: %s", exc)
        vector_nodes = []

    # (3) graph traversal (obbligatorio per impact analysis)
    graph_nodes = _graph_traversal(
        driver, entry_node_id, max_depth, domain, repo, graph_limit)

    if not graph_nodes and not vector_nodes:
        raise HybridSearchError("Nessun risultato da grafo e vettoriale.")

    # (4) fusione RRF (k=60)
    fused = _rrf_fuse(graph_nodes, vector_nodes, k=RRF_K)

    # (5) scoring combinato con prossimita topologica + dedup (gia per node_id)
    ranked = _apply_topological_proximity(fused, beta=beta)
    return ranked[:top_k]
