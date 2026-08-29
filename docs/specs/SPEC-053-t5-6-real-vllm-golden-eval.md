# SPEC-053 — Verifica T5.6: prima golden eval con vLLM reale
Stato: verified
Task-tree: T5.6 · Servizio: services/orchestrator · ADD: Modulo 2 §3.1-3.4, Modulo 4 §1.4 e §3.3
Contratti: API OpenAI-compatible `/v1/chat/completions`; `contracts/` invariati

## 1. Obiettivo
Chiudere formalmente T5.6 registrando il primo run reale del golden dataset con
Qwen3-Coder-30B-A3B FP8 servito da vLLM su una singola GPU dev. La verifica
separa la validità dell'esecuzione dalla qualità delle risposte e conserva il
risultato misurato come baseline immutabile `baseline-v1`.

T5.6 mantiene la definizione del task-tree: **"vLLM reale
(Qwen3-Coder-30B-A3B FP8 su 1 GPU dev) + prima eval sul golden dataset"**.
Non viene aggiunta retroattivamente alcuna soglia di qualità.

## 2. Interfaccia e record immutabile

```bash
eci eval-golden \
  --dataset tests/golden/queries_v0.json \
  --base-url http://127.0.0.1:8000 \
  --model eci-qwen3-coder-30b-a3b-fp8 \
  --output results.jsonl \
  --require-real
```

Record ufficiale `baseline-v1`, copiato senza reinterpretazione da
`results.jsonl.summary.json`:

```json
{
  "error_count": 0,
  "fact_recall": 0.4,
  "is_real": true,
  "latency_p50_ms": 1799.4427140802145,
  "latency_p95_ms": 3604.616958182305,
  "pass_rate": 0.5,
  "query_count": 10,
  "success_count": 10
}
```

Alias di reporting, senza variazione numerica: `pass_rate = 0.50` e
`fact_recall = 0.40`.

## 3. Ambiente ed esecuzione

| Voce | Valore verificato |
|---|---|
| Run ID | `20260828T211053Z` |
| Data UTC | 28 agosto 2026, `21:10:53Z`–`21:13:35Z` |
| Durata | 162 s |
| ECI commit | `32cd8e17643358a7a4307c92dfb3a025d59045f4` |
| Dataset | `tests/golden/queries_v0.json`, 10 query |
| GPU | NVIDIA L40S, 46,068 MiB osservati, compute capability 8.9 |
| Modello | `Qwen/Qwen3-Coder-30B-A3B-Instruct-FP8` |
| Model revision | `dcaee4d4dfc5ee71ad501f01f530e5652438fde0` |
| Serving | vLLM 0.28.0, tensor parallel size 1, max model length 4096 |
| Runtime | PyTorch 2.13.0+cu129, CUDA runtime 12.9 |
| Risultato richieste | 10 completate su 10, 0 errori |
| Execution gate | **PASS** |

Il Pod GPU è stato cancellato dopo il run. La verifica di questa SPEC e della PR
è interamente CPU-only e non richiede di ricrearlo.

Scenari verificati:

1. **Given** il checkpoint FP8 reale e non un fake, **when** vLLM espone
   `/v1/chat/completions`, **then** il catalogo modelli e lo smoke request
   confermano il serving del modello atteso.
2. **Given** il commit ECI e il dataset fissati, **when** l'harness esegue in
   ordine le dieci query, **then** produce dieci record e nessun errore di
   trasporto, HTTP, schema o parsing.
3. **Given** dieci record senza errori, **when** viene valutato l'execution gate,
   **then** il gate è `PASS`.
4. **Given** gli exact match prodotti dall'harness, **when** viene scritto il
   summary, **then** le metriche ufficiali sono `pass_rate=0.50` e
   `fact_recall=0.40`.
5. **Given** il risultato qualitativo inferiore alla perfezione, **when** T5.6
   viene chiuso, **then** il task è `verified` per i criteri originali e non per
   una soglia introdotta dopo il run.
6. **Given** gli artefatti originali, **when** si esegue `sha256sum -c
   SHA256SUMS`, **then** ogni file dell'estrazione è integro.

## 4. Validità dell'esecuzione e qualità della risposta

| Dimensione | Esito | Interpretazione |
|---|---|---|
| Execution validity | **PASS** | modello reale, GPU reale, 10/10 richieste completate, 0 runtime error |
| Answer quality | baseline misurata | `pass_rate=0.50`, `fact_recall=0.40`; non è una dichiarazione di qualità production-ready |

L'execution gate misura che l'esperimento reale sia stato eseguito e abbia
prodotto output valutabile. `pass_rate` e `fact_recall` misurano invece l'exact
quality rispetto al golden. Le due dimensioni non sono intercambiabili.

### Post-hoc qualitative analysis

La tabella seguente è una **post-hoc qualitative analysis** manuale. Non modifica
`passed`, `matched_facts`, `pass_rate` o `fact_recall`, non costituisce una nuova
metrica ufficiale e non viene usata per promuovere retroattivamente alcun record.

| Query | Expected | Actual | Classificazione qualitativa post-hoc |
|---|---|---|---|
| `g01` | `callers=OrderService.Validate <- OrderService.Process` | `callers=tests/fixtures/sample-repo/order_service.go` | vero errore semantico: fact confuso con citation |
| `g04` | `methods=OrderService.Process`, `methods=OrderService.Validate` | `methods=Process`, `methods=Validate` | entità plausibili ma simboli non fully-qualified; mismatch di canonicalizzazione |
| `g06` | `contains=OrderService`, `contains=OrderService.Process`, `contains=OrderService.Validate` | `contains=OrderService`, `contains=Process`, `contains=Validate` | un exact match e due simboli non fully-qualified; mismatch di canonicalizzazione |
| `g07` | `contains=Notifier`, `contains=EmailNotifier`, `contains=EmailNotifier.Notify` | frasi descrittive su `Notifier`, `EmailNotifier` e `Notify` | output verboso/non canonico; il metodo è inoltre non fully-qualified |
| `g09` | `contains=main` | `contains=main istanzia OrderService e chiama Process` | entità incorporata in testo verboso/non canonico |

In sintesi qualitativa: un failure (`g01`) è semantico; quattro failure (`g04`,
`g06`, `g07`, `g09`) sono prevalentemente di formato/canonicalizzazione. La
baseline ufficiale resta comunque **0.50 / 0.40**.

## 5. Evidenze auditabili

Gli artefatti versionati sono in
`artifacts/t5.6/20260828T211053Z/`. I file primari sono:

- `README.md`, `manifest.json`, `SHA256SUMS`;
- `results.jsonl`, `results.jsonl.summary.json`, `queries_v0.json`;
- `golden-dataset.sha256`, `golden-harness.sha256`;
- `model-id.txt`, `model-revision.txt`, `model-metadata.json`;
- `gpu-name.txt`, `gpu-memory-mib.txt`, `gpu.csv`, `torch-gpu.json`;
- `vllm-version.txt`, `vllm-freeze.txt`, `vllm-command.txt`, `vllm.log`,
  `vllm-models.json`;
- `harness-test.log`, `eval-stdout.log`, `eval-exit-code.txt`;
- `eci-commit.txt`, `eci-git-status.txt`.

L'estrazione completa è mantenuta per permettere l'audit del contesto di
esecuzione. Il `.tar.gz` duplicato, checkpoint, cache modello, secret e token non
sono versionati. La scansione degli artefatti non ha rilevato credenziali; il
solo indicatore relativo a Hugging Face registra `false`, non un token.

`run_t56_gpu.sh` dentro l'archivio è la copia **storica e byte-immutabile** dello
script effettivamente eseguito; non è il punto di ingresso supportato per nuove
esecuzioni ed è versionato senza bit eseguibile. Il wrapper mantenuto
`scripts/run-t56-gpu.sh` ammette esclusivamente vLLM `0.28.0` e rifiuta prima di
qualsiasi operazione GPU ogni override di `VLLM_VERSION`. Questa separazione
impedisce che un riuso futuro registri una versione richiesta diversa da quella
runtime senza riscrivere la provenienza del run del 28 agosto 2026.

Verifica integrità:

```bash
(cd artifacts/t5.6/20260828T211053Z && sha256sum -c SHA256SUMS)
sha256sum tests/golden/queries_v0.json
sha256sum services/orchestrator/orchestrator/golden_eval.py
```

I due ultimi digest devono coincidere rispettivamente con
`golden-dataset.sha256` e `golden-harness.sha256` dopo aver ignorato il path
assoluto registrato sul Pod.

## 6. Non-goals

- Correggere `tests/golden/queries_v0.json` o adattarlo al modello.
- Modificare i record del run o ricalcolare manualmente le metriche.
- Introdurre fuzzy matching, embedding similarity o un LLM judge.
- Rilanciare una GPU, cambiare modello o ricreare il Pod cancellato.
- Dichiarare la baseline production-ready.
- Cambiare ADD o contratti.

## 7. Vincoli dall'ADD

- Modulo 2 §3.1-3.4: il gate finale e gli oracoli di correttezza restano
  deterministici; tecniche probabilistiche non sostituiscono la verifica exact.
- Modulo 4 §1.4: Qwen3-Coder-30B-A3B FP8 è il modello primario vLLM on-prem e
  una L40S 48 GB è un target single-GPU previsto per il checkpoint FP8.
- Modulo 4 §3.3: golden dataset e tassi deterministici sono metriche offline;
  metriche LLM-as-judge sono distinte e non entrano in questa baseline.

## 8. Test plan e osservabilità

La verifica del run usa soltanto gli artefatti: checksum, exit code, summary,
record JSONL, manifest, metadati di modello/GPU e log del serving. La latenza è
registrata per query in `results.jsonl` e aggregata come p50/p95 nel summary.
Nessun test della PR contatta vLLM, Runpod o servizi esterni.

## 9. Criteri di accettazione

- [x] Run reale etichettato `is_real=true` su NVIDIA L40S.
- [x] Modello e revision FP8 registrati; vLLM e runtime registrati.
- [x] 10/10 richieste completate, 0 errori, execution gate `PASS`.
- [x] `pass_rate=0.50` e `fact_recall=0.40` registrati senza modifica.
- [x] Cinque failure riportati; analisi manuale marcata post-hoc.
- [x] Evidenze integre e auditabili; nessun secret versionato.
- [x] Runner storico byte-identico; wrapper mantenuto fail-fast su versioni vLLM non supportate.
- [x] Golden dataset, harness, ADD e contratti invariati.
- [x] `task build`, `task lint`, `task test`, `task guard` verdi sul branch PR.

## 10. Esito

T5.6 è **verified** perché i criteri originali richiedevano un vLLM reale con il
modello FP8 su una GPU dev e la prima eval sul golden dataset, entrambi eseguiti
e documentati. `baseline-v1` è immutabile: ogni miglioramento successivo segue
la sequenza `baseline → failure analysis → deterministic improvement →
regression → nuova eval` e produce nuove metriche, senza riscrivere queste.
