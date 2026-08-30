#!/usr/bin/env bash
set -euo pipefail

# Literal target is intentional: never accept a caller-controlled cluster name.
kind delete cluster --name eci-dev
