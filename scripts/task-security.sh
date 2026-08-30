#!/usr/bin/env bash
# Deterministic CPU adversarial security surface (SPEC-061/SPEC-068).
set -euo pipefail

(cd services/api-gateway && go test ./internal/authn ./internal/edge)
(cd libs/go && go test ./eci/accessscope ./eci/authz ./eci/secctx ./eci/securitylabels)
(cd services/retrieval-engine && go test ./internal/securityfilter ./internal/impactanalysis)
services/orchestrator/.venv/bin/python -m pip install -q -e libs/py -e 'services/orchestrator[test]'
services/verification/.venv/bin/python -m pip install -q -e libs/py -e 'services/verification[test]'
services/summarization/.venv/bin/python -m pip install -q -e libs/py -e 'services/summarization[test]'
services/orchestrator/.venv/bin/python -m pytest \
  services/orchestrator/orchestrator/test_graph.py \
  services/orchestrator/orchestrator/test_intent.py
services/verification/.venv/bin/python -m pytest \
  services/verification/verification/test_verifier.py \
  services/verification/verification/test_audit.py
services/summarization/.venv/bin/python -m pytest \
  services/summarization/summarization/test_acl_scope.py
python3 tests/unit/security_config/test_security_config.py
