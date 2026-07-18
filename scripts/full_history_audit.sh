#!/usr/bin/env bash
set -euo pipefail

command -v rg >/dev/null || {
  echo "ripgrep is required for the public-history audit" >&2
  exit 1
}
command -v gitleaks >/dev/null || {
  echo "gitleaks is required for the public-history audit" >&2
  exit 1
}
command -v cargo-deny >/dev/null || {
  echo "cargo-deny is required for the public-history audit" >&2
  exit 1
}
command -v cargo-audit >/dev/null || {
  echo "cargo-audit is required for the public-history audit" >&2
  exit 1
}

gitleaks git . --config .gitleaks.toml --redact --log-opts="--all"
gitleaks dir . --config .gitleaks.toml --redact
bash scripts/secret_scan.sh

if git log --all --format='%H' \
  -G '^version https://git-lfs.github.com/spec/v1$' -- . | rg --quiet .; then
  echo "Git LFS pointers exist in reachable history and require a separate object audit" >&2
  exit 1
fi

bash scripts/cargo_audit.sh
cargo deny --locked check
