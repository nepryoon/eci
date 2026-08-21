"""
run_reference — driver di sottoprocesso per il test di parità dello
scenario 8 (SPEC-041 §7). NON fa parte della reference D5 copiata verbatim
(hybrid_search_reference.py, accanto a questo file): è harness di test
scritto per questa SPEC, che costruisce i client reali (neo4j, qdrant, un
embedder HTTP verso embedder-fake) e chiama `hybrid_graph_vector_search`
esattamente come documentato dall'ADD, poi stampa il risultato come JSON su
stdout — un record per RetrievedNode, nello stesso ordine ritornato dalla
funzione, così il test Go può confrontarlo col proprio output senza un
fixture JSON precalcolato (che potrebbe divergere silenziosamente dalla
reference se questa cambiasse in futuro).

Uso:
  python run_reference.py --neo4j-uri bolt://... --neo4j-user neo4j \
    --neo4j-password ... --qdrant-host ... --qdrant-grpc-port ... \
    --collection code_embeddings --embedder-base-url http://... \
    --query "..." --entry-node-id ... --max-depth 2 [--domain code]
    [--repo local] [--vector-limit 50] [--graph-limit 200] [--top-k 25]
    [--beta 0.15]
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from dataclasses import asdict

from neo4j import GraphDatabase
from qdrant_client import QdrantClient

from hybrid_search_reference import hybrid_graph_vector_search, HybridSearchError


class HTTPEmbedder:
    """Stesso contratto di embedclient Go: POST {base_url}/embed
    {"inputs": text} -> [[float, ...]], ritorna il primo vettore."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def encode(self, text: str) -> list[float]:
        body = json.dumps({"inputs": text}).encode("utf-8")
        req = urllib.request.Request(
            self.base_url + "/embed",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            vectors = json.loads(resp.read().decode("utf-8"))
        return vectors[0]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--neo4j-uri", required=True)
    p.add_argument("--neo4j-user", required=True)
    p.add_argument("--neo4j-password", required=True)
    p.add_argument("--qdrant-host", required=True)
    p.add_argument("--qdrant-grpc-port", type=int, required=True)
    p.add_argument("--collection", required=True)
    p.add_argument("--embedder-base-url", required=True)
    p.add_argument("--query", required=True)
    p.add_argument("--entry-node-id", required=True)
    p.add_argument("--max-depth", type=int, required=True)
    p.add_argument("--domain", default="code")
    p.add_argument("--repo", default=None)
    p.add_argument("--vector-limit", type=int, default=50)
    p.add_argument("--graph-limit", type=int, default=200)
    p.add_argument("--top-k", type=int, default=25)
    p.add_argument("--beta", type=float, default=0.15)
    args = p.parse_args()

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    qdrant = QdrantClient(host=args.qdrant_host, grpc_port=args.qdrant_grpc_port, prefer_grpc=True)
    embedder = HTTPEmbedder(args.embedder_base_url)

    try:
        ranked = hybrid_graph_vector_search(
            args.query,
            args.entry_node_id,
            args.max_depth,
            driver=driver,
            qdrant=qdrant,
            embedder=embedder,
            collection=args.collection,
            domain=args.domain,
            repo=args.repo,
            vector_limit=args.vector_limit,
            graph_limit=args.graph_limit,
            top_k=args.top_k,
            beta=args.beta,
        )
    except HybridSearchError as exc:
        print(json.dumps({"error": str(exc)}))
        return 1
    finally:
        driver.close()
        qdrant.close()

    out = []
    for n in ranked:
        d = asdict(n)
        d.pop("payload", None)  # non serve al confronto, e provenance già copre repo/path
        out.append(d)
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
