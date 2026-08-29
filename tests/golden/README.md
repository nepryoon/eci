# golden

`queries_v0.json` è il corpus golden immutato. Le attese sono utilizzate solo
dal comparator, dopo la canonicalizzazione dell'output del modello.

`t5_6_failure_taxonomy_v1.json` contiene esclusivamente le classificazioni di
regressione per i cinque failure reali T5.6. Gli output sotto test provengono
direttamente dall'artefatto immutabile
`artifacts/t5.6/20260828T211053Z/results.jsonl`; questo file non fornisce simboli
o suggerimenti al resolver.
