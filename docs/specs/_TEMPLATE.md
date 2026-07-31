## 2. Workflow "Spec-to-Code"

**Granularità (regola fissa):** 1 task del task-tree = 1 SPEC = 1 PR = 1 modulo/package + 1 file di test omonimo. Dimensione target: 200–600 righe prodotte, ≤1 giornata. Se una SPEC supera 8 scenari Given/When/Then o tocca più di un servizio → si spezza. Le SPEC sono l'unità di memoria del progetto: il codice si rigenera, le SPEC no.

**Formato esatto della SPEC** (`docs/specs/_TEMPLATE.md`):

```markdown
# SPEC-NNN — <titolo>
Stato: draft | approved | implemented | verified
Task-tree: T<fase>.<n> · Servizio: services/<nome> · ADD: §<riferimenti puntuali>
Contratti: contracts/<file usati>

## 1. Obiettivo — 2-3 frasi, cosa esiste dopo che prima non esisteva.
## 2. Interfaccia — firma ESATTA: RPC del proto / funzione pubblica / CLI /
     schema evento. Nessuna prosa: codice o schema.
## 3. Comportamento — scenari numerati Given/When/Then (diventano i test 1:1).
## 4. Errori & edge case — tabella: condizione → comportamento atteso.
## 5. Non-goals — cosa NON implementare (àncora anti-scope-creep per l'AI).
## 6. Vincoli dall'ADD — citazioni puntuali (es. "sink idempotente, M1 §2.2.4").
## 7. Test plan — unit / integration (testcontainers: quali container) /
     proprietà (es. idempotenza: doppio replay ⇒ stato identico).
## 8. Osservabilità — span e metriche da esporre (nomi esatti).
## 9. Criteri di accettazione — checklist eseguibile, comandi inclusi.
```

**Prompt esatto per chiedermi una SPEC** (in questa chat):

```
Scrivi SPEC-<NNN> per il task T<x.y> del task-tree.
Template: docs/specs/_TEMPLATE.md. Fonti: ADD §<...>, contracts/<...>.
Vincoli aggiuntivi: <...>.
Output: solo il contenuto del file docs/specs/SPEC-<NNN>-<slug>.md. Nessun codice.
```

**Prompt esatto per Claude Code** (implementazione):

```
Implementa docs/specs/SPEC-<NNN>-<slug>.md.
1) Leggi CLAUDE.md e la SPEC. 2) Scrivi PRIMA i test da §3 e §7 e verificane il
fallimento. 3) Implementa fino a `task lint && task test` verdi. 4) Stato SPEC →
implemented, annota le deviazioni. Non toccare file fuori da services/<nome> e tests/.
```

Ciclo completo: (1) SPEC qui → (2) tua review/approvazione (`approved`) → (3) Claude Code implementa → (4) tua review della PR con la SPEC a fianco → (5) merge, stato `verified`. Il punto (2) è dove spendi il giudizio: rivedere 100 righe di SPEC costa un decimo di rivedere 600 righe di codice sbagliato.

---
