#!/usr/bin/env bash
set -euo pipefail

patterns=(
  '^[[:space:]]*ODDSFOX_ENABLE_LIVE_TRADING=YES[[:space:]]*$'
  'PRIVATE_KEY=[^<[:space:]]+'
  'private_key[[:space:]]*=[[:space:]]*"[^"]+"'
  'CLOB_API_SECRET=[^<[:space:]]+'
  'CLOB_API_PASSPHRASE=[^<[:space:]]+'
  '-----BEGIN (EC |RSA )?PRIVATE KEY-----'
)

for pattern in "${patterns[@]}"; do
  if rg --hidden --glob '!.git/**' --glob '!scripts/secret_scan.sh' \
    --glob '!docs/**' --glob '!README.md' --glob '!SECURITY.md' \
    --glob '!COMPLIANCE.md' --glob '!openapi/**' \
    --line-number --regexp "$pattern"; then
    echo "possible secret matched pattern: $pattern" >&2
    exit 1
  fi
done
