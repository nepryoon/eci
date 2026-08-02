"""Config loader condiviso — equivalente Python di EnvOrDefault (Go,
SPEC-011) / env_or_default (Rust, SPEC-010)."""

import os


def env_or_default(key: str, default: str) -> str:
    """os.environ.get(key, default) — stringa vuota impostata
    esplicitamente è un valore, non un'assenza (comportamento nativo
    di os.environ.get, stesso principio di Rust/Go SPEC-010/011)."""
    return os.environ.get(key, default)
