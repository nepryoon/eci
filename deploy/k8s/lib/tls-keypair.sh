#!/usr/bin/env bash

# Return success only when both files are parseable and the certificate's
# public key is derived from the supplied private key. No private material is
# printed or persisted beyond the caller-provided path.
eci_tls_private_key_matches_certificate() {
  if [[ $# -ne 2 || ! -s "$1" || ! -s "$2" ]]; then
    return 1
  fi

  local certificate_public_key private_public_key
  certificate_public_key="$(openssl x509 -in "$1" -pubkey -noout 2>/dev/null)" || return 1
  private_public_key="$(openssl pkey -in "$2" -pubout 2>/dev/null)" || return 1
  [[ -n "$certificate_public_key" && "$certificate_public_key" == "$private_public_key" ]]
}
