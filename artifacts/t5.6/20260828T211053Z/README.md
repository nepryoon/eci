# ECI T5.6 real-vLLM evaluation evidence

- Run ID: `20260828T211053Z`
- Execution gate: **PASS**
- Started: `2026-08-28T21:10:53Z`
- Completed: `2026-08-28T21:13:35Z`
- Duration: `162 seconds`
- ECI commit: `32cd8e17643358a7a4307c92dfb3a025d59045f4`
- Model: `Qwen/Qwen3-Coder-30B-A3B-Instruct-FP8`
- Model revision: `dcaee4d4dfc5ee71ad501f01f530e5652438fde0`
- Served model name: `eci-qwen3-coder-30b-a3b-fp8`
- vLLM: `0.28.0`
- GPU: `NVIDIA L40S`
- Max model length: `4096`
- Golden query count: `10`
- Pass rate: `0.5`
- Fact recall: `0.4`

The execution gate passes only when the real-model run completes all ten queries
without transport, HTTP, schema, or parsing errors. Pass rate and fact recall are
reported as measured quality outcomes; they are not silently converted into an
acceptance threshold absent from the current T5.6 task definition.
