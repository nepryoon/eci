"""Client gRPC verso retrieval-engine (SPEC-018 §2): riuso diretto di
`eci_core.grpc_client.SecurityContextInterceptor` +
`opentelemetry.instrumentation.grpc` — esattamente la combinazione già
verificata funzionante end-to-end contro un server Go reale nello
scaffold di interoperabilità di SPEC-012
(`eci_core/examples/interop_client.py`), qui applicata per la prima
volta a un uso applicativo reale invece che a una prova di
interoperabilità."""

import grpc
from eci_core.grpc_client import SecurityContextInterceptor
from eci_core.retrieval.v1 import retrieval_pb2, retrieval_pb2_grpc
from opentelemetry.instrumentation.grpc import client_interceptor
from opentelemetry.instrumentation.grpc import (
    intercept_channel as otel_intercept_channel,
)

from orchestrator.errors import RetrievalUnavailableError

# SecurityContext placeholder di sviluppo (SPEC-018 §2): propagato
# correttamente (via l'interceptor) ma non enforced da nessuna parte
# della catena — stesso principio "plumbing, non enforcement" di
# T0.8/T1.4.
DEFAULT_TENANT_ID = "eci-dev-tenant"
DEFAULT_USER_ID = "eci-dev-user"

_RPC_TIMEOUT_SECONDS = 10.0


def build_security_context() -> retrieval_pb2.SecurityContext:
    return retrieval_pb2.SecurityContext(tenant_id=DEFAULT_TENANT_ID, user_id=DEFAULT_USER_ID)


def _build_channel(security_context: retrieval_pb2.SecurityContext, addr: str, tracer_provider=None):
    channel = grpc.insecure_channel(addr)
    if tracer_provider is not None:
        # Stesso ordine di composizione di eci_core/examples/interop_client.py:
        # le due famiglie di interceptor (otel "grpcext" vs
        # grpc.UnaryUnaryClientInterceptor standard) si compongono
        # impilando i wrapper di canale, non passandole insieme a un'API
        # unica.
        channel = otel_intercept_channel(channel, client_interceptor(tracer_provider=tracer_provider))
    return grpc.intercept_channel(channel, SecurityContextInterceptor(security_context))


def _sanitize_for_fulltext(query_text: str) -> str:
    """`query_text` diventa direttamente una query Lucene lato
    retrieval-engine (`CALL db.index.fulltext.queryNodes`, SPEC-016 §2)
    — un `?`/`*` finale non è testo libero per Lucene, è un operatore di
    wildcard a carattere singolo/multiplo. Verificato empiricamente
    contro un Neo4j reale: `'Chi chiama Validate?'` produce ZERO match
    full-text (compreso su `Validate` stesso, che pure esiste per nome),
    perché `Validate?` richiede un termine di 9 caratteri, non 8. Una
    domanda in linguaggio naturale che termina con `?` — il caso comune
    per `eci ask` — deve poter comunque fare match sul nome; la
    punteggiatura finale non ha un significato di ricerca voluto
    dall'utente. Solo i caratteri finali sono ripuliti (non un
    escaping Lucene completo, fuori scope qui): non altera un
    identificatore che termina legittimamente con quei caratteri (raro,
    non un caso reale in questo dataset)."""
    return query_text.rstrip("?!.,;:")


def hybrid_search(
    addr: str,
    query_text: str,
    security_context: retrieval_pb2.SecurityContext,
    tracer_provider=None,
    entry_node_id: str = "",
    repos: list[str] | None = None,
) -> list:
    """Chiama HybridSearch su retrieval-engine. Ritorna la lista di
    RetrievedNode (protobuf) — tutti gli altri campi della request
    restano a zero-value (SPEC-018 §2: retrieval-engine applica i propri
    default documentati, l'orchestrator non li duplica). Un errore di
    connessione/RPC diventa RetrievalUnavailableError con host/porta nel
    messaggio (SPEC-018 §3 scenario 4), mai una traceback grpc grezza
    propagata al chiamante."""
    channel = _build_channel(security_context, addr, tracer_provider)
    stub = retrieval_pb2_grpc.RetrievalEngineStub(channel)
    request = retrieval_pb2.HybridSearchRequest(
        security_context=security_context,
        query_text=_sanitize_for_fulltext(query_text),
        entry_node_id=entry_node_id,
        repos=repos or [],
    )
    try:
        response = stub.HybridSearch(request, timeout=_RPC_TIMEOUT_SECONDS)
    except grpc.RpcError as e:
        raise RetrievalUnavailableError(addr, e) from e
    finally:
        channel.close()

    return list(response.nodes)


def find_callers(
    addr: str,
    security_context: retrieval_pb2.SecurityContext,
    node_id: str,
    tracer_provider=None,
) -> list:
    """ExpandNeighbors(direction=REVERSE, edge_types=[CALLS]) su
    `node_id` — CHI chiama `node_id`, non chi `node_id` chiama
    (SPEC-018 §2, euristica "chi chiama"). Parametri fissi e deliberati,
    non derivati dalla request: questa funzione ha un solo scopo, non un
    ExpandNeighbors generico riesposto."""
    channel = _build_channel(security_context, addr, tracer_provider)
    stub = retrieval_pb2_grpc.RetrievalEngineStub(channel)
    request = retrieval_pb2.ExpandNeighborsRequest(
        security_context=security_context,
        node_id=node_id,
        edge_types=[retrieval_pb2.EDGE_TYPE_CALLS],
        direction=retrieval_pb2.TRAVERSAL_DIRECTION_REVERSE,
    )
    try:
        response = stub.ExpandNeighbors(request, timeout=_RPC_TIMEOUT_SECONDS)
    except grpc.RpcError as e:
        raise RetrievalUnavailableError(addr, e) from e
    finally:
        channel.close()

    return list(response.neighbors)


def get_node(addr, security_context, node_id, tracer_provider=None):
    channel = _build_channel(security_context, addr, tracer_provider)
    stub = retrieval_pb2_grpc.RetrievalEngineStub(channel)
    try:
        response = stub.GetNode(
            retrieval_pb2.GetNodeRequest(security_context=security_context, node_id=node_id),
            timeout=_RPC_TIMEOUT_SECONDS,
        )
    except grpc.RpcError as e:
        raise RetrievalUnavailableError(addr, e) from e
    finally:
        channel.close()
    return response.node


def expand_neighbors(addr, security_context, node_id, edge_types, direction, depth, tracer_provider=None):
    channel = _build_channel(security_context, addr, tracer_provider)
    stub = retrieval_pb2_grpc.RetrievalEngineStub(channel)
    request = retrieval_pb2.ExpandNeighborsRequest(
        security_context=security_context,
        node_id=node_id,
        edge_types=edge_types,
        direction=direction,
        depth=depth,
    )
    try:
        response = stub.ExpandNeighbors(request, timeout=_RPC_TIMEOUT_SECONDS)
    except grpc.RpcError as e:
        raise RetrievalUnavailableError(addr, e) from e
    finally:
        channel.close()
    return list(response.neighbors)
