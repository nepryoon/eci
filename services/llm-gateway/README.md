# llm-gateway

Reverse proxy OpenAI-compatible di SPEC-049. Configurazione esempio:

```bash
LLM_GATEWAY_ROUTES='fake=http://localhost:8001|vllm-fake,real=http://vllm:8000|Qwen/Qwen3-Coder-30B-A3B' \
LLM_GATEWAY_DEFAULT='http://localhost:8001|vllm-fake' \
LLM_GATEWAY_TIMEOUT=15s go run .
```

Espone `POST /v1/chat/completions` e `GET /healthz`. Le generazioni non sono
ritentate: timeout e cancellazione vengono propagati, mentre il circuit breaker
apre su errori di rete e risposte 5xx. Lo streaming viene copiato e flushato per
ogni chunk letto dall'upstream.

```bash
go test -count=1 ./...
```
