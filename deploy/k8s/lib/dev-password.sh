#!/usr/bin/env bash

# Resolve the disposable dev-cluster password without silently rotating a
# persisted database credential. PostgreSQL's CloudNativePG bootstrap Secret is
# initialization-only, so an existing value is authoritative until an explicit
# credential-rotation workflow changes both the role and every consumer.
eci_resolve_dev_password() {
  if [[ "$#" -ne 2 ]]; then
    echo "eci_resolve_dev_password requires stored and requested values" >&2
    return 2
  fi

  local stored_password="$1"
  local requested_password="$2"

  if [[ -n "$stored_password" ]]; then
    if [[ -n "$requested_password" && "$requested_password" != "$stored_password" ]]; then
      echo "ECI_DEV_PASSWORD differs from the existing cluster credential; explicit credential rotation is required" >&2
      return 1
    fi
    printf '%s' "$stored_password"
    return 0
  fi

  if [[ -n "$requested_password" ]]; then
    printf '%s' "$requested_password"
  else
    openssl rand -hex 18
  fi
}
