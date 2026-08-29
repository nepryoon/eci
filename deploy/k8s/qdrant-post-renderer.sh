#!/usr/bin/env bash
set -euo pipefail

python3 -c '
import re
import sys

source = "docker.io/qdrant/qdrant:v1.19.0-unprivileged"
target = source + "@sha256:a0e04fe623cb064502cd869cefc1dc7ce359d8edd481063b5bd351c0a0a2c91e"
rendered = sys.stdin.read()
matches = len(re.findall(r"(?m)^\s*image:\s*\"?" + re.escape(source) + r"\"?\s*$", rendered))
if matches != 1:
    raise SystemExit(f"qdrant post-renderer expected exactly one runtime image, found {matches}")
rendered = rendered.replace(source, target)
for line in rendered.splitlines():
    if re.match(r"^\s*image:\s*", line) and "@sha256:" not in line:
        raise SystemExit(f"qdrant post-renderer rejected mutable image: {line.strip()}")
sys.stdout.write(rendered)
'
