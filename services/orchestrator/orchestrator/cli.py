"""CLI `eci ask "<query>"` (SPEC-018 §2) — installata come
`[project.scripts]` in pyproject.toml: comando reale dopo
`pip install -e .`, non solo un `python -m` da ricordare."""

import argparse
import json
import sys
from pathlib import Path

from eci_core.config import env_or_default
from eci_core.observability import init_tracing

from orchestrator.ask import run_ask
from orchestrator.errors import LLMUnavailableError, RetrievalUnavailableError
from orchestrator.golden_eval import run_golden_eval

DEFAULT_RETRIEVAL_ADDR = "localhost:50053"
DEFAULT_VLLM_URL = "http://localhost:8001"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eci")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ask_parser = subparsers.add_parser("ask", help='Interroga il grafo ed elabora una risposta con fonti')
    ask_parser.add_argument("query", nargs="?", default=None, help="Domanda in linguaggio naturale")

    eval_parser = subparsers.add_parser("eval-golden", help="Esegue il golden dataset")
    eval_parser.add_argument("--dataset", type=Path, required=True)
    eval_parser.add_argument("--base-url", required=True)
    eval_parser.add_argument("--model", required=True)
    eval_parser.add_argument("--output", type=Path, required=True)
    eval_parser.add_argument("--force", action="store_true")
    eval_parser.add_argument("--require-real", action="store_true")

    return parser


def _format_sources(nodes) -> str:
    """Sezione "Fonti" (SPEC-018 §2/§4): solo i campi realmente
    disponibili — un RetrievedNode con provenance parzialmente vuota non
    stampa placeholder "N/A" per i campi mancanti, li omette."""
    lines = ["Fonti:"]
    for n in nodes:
        parts = []
        if n.name:
            parts.append(n.name)
        if n.node_type:
            parts.append(f"tipo={n.node_type}")
        if n.node_id:
            parts.append(f"id={n.node_id}")
        if n.provenance.repo:
            parts.append(f"repo={n.provenance.repo}")
        if n.provenance.path:
            parts.append(f"path={n.provenance.path}")
        lines.append("- " + ", ".join(parts))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "eval-golden":
        try:
            summary = run_golden_eval(
                args.dataset,
                args.base_url,
                args.model,
                args.output,
                force=args.force,
                require_real=args.require_real,
            )
        except (OSError, ValueError) as exc:
            print(f"errore: {exc}", file=sys.stderr)
            return 2
        print(json.dumps(summary, sort_keys=True))
        return 0

    if args.query is None:
        # Query CLI assente (SPEC-018 §4): errore d'uso esplicito via
        # argparse (usage + exit 2), nessuna chiamata a retrieval-engine
        # con query_text="".
        parser.error('argomento "query" mancante — uso: eci ask "<query>"')
        return 2  # pragma: no cover

    if not args.query.strip():
        # Query CLI vuota (SPEC-018 §4, stesso trattamento di "assente"):
        # stesso principio, valore diverso da None.
        print('errore: query vuota — uso: eci ask "<query>"', file=sys.stderr)
        return 2

    retrieval_addr = env_or_default("ORCHESTRATOR_RETRIEVAL_ADDR", DEFAULT_RETRIEVAL_ADDR)
    vllm_url = env_or_default("ORCHESTRATOR_VLLM_URL", DEFAULT_VLLM_URL)

    provider = init_tracing("orchestrator")
    try:
        result = run_ask(args.query, retrieval_addr, vllm_url, tracer_provider=provider)
    except RetrievalUnavailableError as e:
        print(f"errore: {e}", file=sys.stderr)
        return 1
    except LLMUnavailableError as e:
        if e.sources:
            print(_format_sources(e.sources))
            print()
        print(f"errore: {e}", file=sys.stderr)
        return 1
    finally:
        provider.shutdown()

    if not result.nodes:
        print(f"nessun risultato trovato nel grafo per: {result.query_text!r}")
        return 0

    print(result.answer)
    print()
    print(_format_sources(result.nodes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
