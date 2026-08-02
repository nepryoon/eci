"""SPEC-012 §3 scenario 2 — env_or_default rispetta default/override,
incluso il caso di stringa vuota impostata esplicitamente (valore, non
assenza)."""

from eci_core.config import env_or_default


def test_env_or_default_returns_default_when_unset(monkeypatch):
    monkeypatch.delenv("ECI_TEST_VAR", raising=False)
    assert env_or_default("ECI_TEST_VAR", "fallback") == "fallback"


def test_env_or_default_returns_override_when_set(monkeypatch):
    monkeypatch.setenv("ECI_TEST_VAR", "explicit-value")
    assert env_or_default("ECI_TEST_VAR", "fallback") == "explicit-value"


def test_env_or_default_treats_explicit_empty_string_as_value(monkeypatch):
    monkeypatch.setenv("ECI_TEST_VAR", "")
    assert env_or_default("ECI_TEST_VAR", "fallback") == ""
