#!/usr/bin/env bash

# Verify that a reusable eci-dev cluster is backed by the exact immutable node
# image and Kubernetes release selected by SPEC-062. The caller supplies the
# command paths so the policy remains deterministic and unit-testable.
eci_verify_existing_kind_cluster() {
  local expected_image="$1"
  local expected_version="$2"
  local nodes_output node image_id repo_digests version_json server_version
  local -a nodes=()

  if ! nodes_output="$("$KIND_BIN" get nodes --name eci-dev)"; then
    echo "cannot enumerate nodes for existing kind cluster eci-dev" >&2
    return 1
  fi
  mapfile -t nodes <<<"$nodes_output"
  if [[ ${#nodes[@]} -eq 0 || -z "${nodes[0]:-}" ]]; then
    echo "existing kind cluster eci-dev has no nodes" >&2
    return 1
  fi

  for node in "${nodes[@]}"; do
    if ! image_id="$("$DOCKER_BIN" inspect --format '{{.Image}}' "$node")" || [[ -z "$image_id" ]]; then
      echo "cannot resolve container image for kind node $node" >&2
      return 1
    fi
    if ! repo_digests="$("$DOCKER_BIN" image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$image_id")"; then
      echo "cannot inspect immutable image digest for kind node $node" >&2
      return 1
    fi
    if ! grep -Fxq "$expected_image" <<<"$repo_digests"; then
      echo "kind node $node does not use the pinned image digest $expected_image; recreate eci-dev explicitly" >&2
      return 1
    fi
  done

  if ! version_json="$("$KUBECTL_BIN" --context kind-eci-dev version -o json)"; then
    echo "cannot query Kubernetes server version for kind-eci-dev" >&2
    return 1
  fi
  server_version="$(printf '%s\n' "$version_json" | awk '
    {
      line = $0
      if (!in_server) {
        position = index(line, "\"serverVersion\"")
        if (position == 0) next
        in_server = 1
        line = substr(line, position)
      }
      if (match(line, /\"gitVersion\"[[:space:]]*:[[:space:]]*\"[^\"]+\"/)) {
        value = substr(line, RSTART, RLENGTH)
        sub(/^\"gitVersion\"[[:space:]]*:[[:space:]]*\"/, "", value)
        sub(/\"$/, "", value)
        print value
        exit
      }
    }
  ')"
  if [[ "$server_version" != "v$expected_version" ]]; then
    echo "kind-eci-dev Kubernetes server version ${server_version:-unknown} does not match required v$expected_version; recreate eci-dev explicitly" >&2
    return 1
  fi
}
