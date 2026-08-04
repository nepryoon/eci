"""Costruzione del prompt per vllm-fake (SPEC-018 §2): vllm-fake echeggia
letteralmente solo l'ULTIMO messaggio role="user" (SPEC-017 §2/§3
scenario 4) — l'intero contesto va quindi nel messaggio utente, non nel
system."""

SYSTEM_MESSAGE = (
    "Sei un assistente che risponde a domande sul codice usando SOLO il "
    "contesto fornito nel messaggio successivo."
)


def build_messages(query_text: str, nodes: list) -> list[dict]:
    """`nodes`: RetrievedNode (protobuf) — un messaggio system + un
    messaggio user col template esatto di SPEC-018 §2."""
    lines = [f"Domanda: {query_text}", "", "Contesto recuperato dal grafo:"]
    for n in nodes:
        lines.append(f"- {n.name} ({n.node_type}, id={n.node_id})")
    lines.append("")
    lines.append("Rispondi basandoti solo sul contesto sopra, citando i nomi dei nodi usati.")
    user_content = "\n".join(lines)

    return [
        {"role": "system", "content": SYSTEM_MESSAGE},
        {"role": "user", "content": user_content},
    ]
