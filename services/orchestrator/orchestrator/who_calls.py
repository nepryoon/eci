"""Euristica "chi chiama" (SPEC-018 §2): elenco chiuso di frasi, non un
classificatore NLP. `HybridSearch` (full-text solo sul nome, SPEC-016
§2) non può mai rispondere a una domanda sui chiamanti di un nodo —
verificato empiricamente contro un Neo4j reale che la query letterale
"Chi chiama Validate?" non trova nulla via full-text (il `?` finale è un
wildcard Lucene a un carattere, e anche ripulito il testo troverebbe
solo `Validate` per nome, mai i suoi chiamanti). Questo modulo rileva
quando la domanda riguarda i chiamanti, così `orchestrator.ask` può
aggiungere il passo di traversata (`ExpandNeighbors`, REVERSE su CALLS)
che la sola ricerca testuale non può fare."""

WHO_CALLS_PHRASES = ("chi chiama", "who calls", "chiamanti di")


def is_who_calls_query(query_text: str) -> bool:
    lowered = query_text.lower()
    return any(phrase in lowered for phrase in WHO_CALLS_PHRASES)
