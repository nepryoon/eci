# SPEC-052 — Harness golden provider-neutral per vLLM reale (T5.6-prep)
Stato: implemented
Task-tree: preparazione T5.6 · Servizio: services/orchestrator · ADD: Modulo 4 §1.4-1.5
Contratti: API OpenAI-compatible `/v1/chat/completions`; `contracts/` invariati

## 1. Obiettivo
Costruire il runner riproducibile della prima eval golden senza richiedere subito una GPU. Lo stesso runner opera contro fake o endpoint vLLM reale; soltanto un run reale potrà completare T5.6.

## 2. Interfaccia
```bash
eci eval-golden --dataset tests/golden/queries_v0.json --base-url URL --model MODEL --output results.jsonl
```

## 3. Comportamento
1. Valida tutte le entry prima di inviare richieste. 2. Esegue in ordine stabile con temperature 0. 3. Registra risposta, fonti, token e latenza. 4. Calcola fact recall senza LLM judge. 5. Un errore produce record esplicito e il run continua. 6. Output JSONL è atomico. 7. Secret e prompt non entrano nei log. 8. `--require-real` rifiuta il modello fake.

## 4. Errori & edge case
Dataset/schema invalido: fail prima delle chiamate. Output esistente: rifiutato salvo `--force`. Endpoint irraggiungibile: record error tipizzato.

## 5. Non-goals
Provisioning GPU, benchmark throughput, Ragas, modifica del golden, dichiarare T5.6 verificata.

## 6. Vincoli dall'ADD
Modello on-prem OpenAI-compatible; verifica deterministica; artefatti riproducibili; nessun dato sensibile nei log.

## 7. Test plan
Server HTTP reale in-process; dataset temporaneo; successi, errori, atomicità, fake guard e fact matching.

## 8. Osservabilità
Summary JSON con pass rate, fact recall, error count, p50/p95 latency; nessun source nei log.

## 9. Criteri di accettazione
- [x] Scenari 1-8 verdi.
- [ ] `task build`, `task lint`, `task test`, `task guard` verdi.
- [ ] Run fake etichettato non-reale; criteri GPU T5.6 ancora aperti.

## 10. Deviazioni
Il task T5.6 è diviso in harness software e run GPU differito per minimizzare il costo di provisioning; questa SPEC non costituisce verifica del modello reale.
Il fact matcher iniziale è deterministico e letterale (case-insensitive): misura la
presenza dei fatti golden, non equivalenza semantica né qualità stilistica.
