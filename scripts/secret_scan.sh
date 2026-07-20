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
  if git grep --line-number --extended-regexp -e "$pattern" -- . \
    ':(exclude)scripts/secret_scan.sh' \
    ':(exclude)docs/**' \
    ':(exclude)README.md' \
    ':(exclude)SECURITY.md' \
    ':(exclude)COMPLIANCE.md' \
    ':(exclude)openapi/**'; then
    echo "possible secret matched pattern: $pattern" >&2
    exit 1
  fi
done
