#!/usr/bin/env bash
set -euo pipefail

command -v rg >/dev/null || {
  echo "ripgrep is required for the dependency audit" >&2
  exit 1
}

# sqlx's derive proc-macro declares optional MySQL support, so Cargo.lock must
# contain rsa even though it is absent from every compiled oddsfox-exec graph.
# Refuse the documented exception if that ever stops being true.
if cargo tree --locked --all-features --target all -i rsa@0.9.10 2>&1 \
  | rg --quiet 'oddsfox-execution'; then
  echo "RUSTSEC-2023-0071 is reachable and may no longer be ignored" >&2
  exit 1
fi

cargo audit --ignore RUSTSEC-2023-0071 "$@"
