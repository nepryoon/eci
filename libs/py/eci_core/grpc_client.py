"""Interceptor gRPC client che allega SecurityContext (tipo proto già
generato da SPEC-002, riusato — non ridefinito) ai metadata di ogni
chiamata unary-unary in uscita, sotto la chiave binaria
'eci-security-context-bin' — stessa convenzione di SPEC-011 (lato
server, Go). Fase 1 è "no security" per design (vedi ADD): questo
modulo è solo plumbing (propagazione), non enforcement."""

import collections

import grpc

from eci_core.retrieval.v1.retrieval_pb2 import SecurityContext

METADATA_KEY = "eci-security-context-bin"


class _ClientCallDetails(
    collections.namedtuple(
        "_ClientCallDetails",
        (
            "method",
            "timeout",
            "metadata",
            "credentials",
            "wait_for_ready",
            "compression",
        ),
    ),
    grpc.ClientCallDetails,
):
    """ClientCallDetails è un ABC senza implementazione concreta fornita
    da grpc: serve una sottoclasse namedtuple per costruirne una nuova
    con i metadata aumentati (pattern standard degli interceptor client
    gRPC Python, client_call_details è immutabile)."""


class SecurityContextInterceptor(grpc.UnaryUnaryClientInterceptor):
    """Allega un SecurityContext (tipo proto già generato da SPEC-002)
    serializzato come protobuf ai metadata di ogni chiamata
    unary-unary in uscita, sotto la chiave binaria
    'eci-security-context-bin' — stessa convenzione di SPEC-011 (lato
    server, Go). Costruito con un SecurityContext fisso al momento
    della creazione (walking skeleton "no security": nessuna logica
    di autenticazione reale, solo plumbing — vedi §5)."""

    def __init__(self, security_context: SecurityContext) -> None:
        # Serializzato subito: un SecurityContext non serializzabile è un
        # bug altrove (§4) — l'eccezione si propaga qui, alla costruzione,
        # non silenziosamente più tardi a ogni chiamata.
        self._serialized = security_context.SerializeToString()

    def intercept_unary_unary(self, continuation, client_call_details, request):
        metadata = list(client_call_details.metadata or [])
        metadata.append((METADATA_KEY, self._serialized))
        new_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(new_details, request)
