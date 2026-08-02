"""Client gRPC di prova per la verifica di interoperabilità cross-linguaggio
SPEC-012 §3 scenari 3/4: usa SecurityContextInterceptor (questa SPEC) e
opentelemetry-instrumentation-grpc contro un server Go reale
(tests/interop/spec-012-secctx-py-go/server, che usa l'interceptor SERVER
di SPEC-011). Non fa parte della suite pytest — script di verifica manuale
una tantum, eseguito con `python -m eci_core.examples.interop_client`
mentre il server Go gira su 127.0.0.1:50052 (vedi report SPEC-012 §10).
"""

import grpc
from opentelemetry.instrumentation.grpc import client_interceptor
from opentelemetry.instrumentation.grpc import (
    intercept_channel as otel_intercept_channel,
)

from eci_core.grpc_client import SecurityContextInterceptor
from eci_core.observability import init_tracing
from eci_core.retrieval.v1 import retrieval_pb2, retrieval_pb2_grpc


def main() -> None:
    provider = init_tracing("orchestrator-interop-client")
    tracer = provider.get_tracer(__name__)

    security_context = retrieval_pb2.SecurityContext(
        tenant_id="tenant-py-42",
        user_id="user-py-7",
        allowed_repos=["repo-a", "repo-b"],
        acl_groups=["group-x"],
    )

    channel = grpc.insecure_channel("127.0.0.1:50052")
    # opentelemetry-instrumentation-grpc usa il proprio wrapper
    # intercept_channel (interceptor "grpcext", non grpc.UnaryUnaryClientInterceptor
    # standard) — non componibile con grpc.intercept_channel nella stessa
    # chiamata: le due famiglie di interceptor si compongono impilando i
    # rispettivi wrapper di canale, non passandole insieme a un'unica API.
    channel = otel_intercept_channel(channel, client_interceptor(tracer_provider=provider))
    channel = grpc.intercept_channel(channel, SecurityContextInterceptor(security_context))
    stub = retrieval_pb2_grpc.RetrievalEngineStub(channel)

    print(
        f"[py-client] invio SecurityContext: tenant_id={security_context.tenant_id!r} "
        f"user_id={security_context.user_id!r} allowed_repos={list(security_context.allowed_repos)} "
        f"acl_groups={list(security_context.acl_groups)}"
    )

    with tracer.start_as_current_span("interop_get_node") as span:
        trace_id = span.get_span_context().trace_id
        print(f"[py-client] trace_id inviato: {trace_id:032x}")
        response = stub.GetNode(retrieval_pb2.GetNodeRequest(node_id="interop-node-1"))

    print(f"[py-client] risposta ricevuta: node_id={response.node.node_id!r}")
    provider.shutdown()


if __name__ == "__main__":
    main()
