#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
if [[ "$mode" != "fast" && "$mode" != "full" ]]; then
  echo "usage: $0 {fast|full}" >&2
  exit 2
fi

cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo run --locked -- openapi --check openapi/oddsfox-execution-v1.json

if [[ "$mode" == "fast" ]]; then
  bash scripts/secret_scan.sh
  exit 0
fi

bash scripts/full_history_audit.sh

revision="$(git rev-parse HEAD)"
docker buildx build \
  --platform linux/amd64 \
  --build-arg "VCS_REF=$revision" \
  --load \
  --tag oddsfox-execution:paper-ci \
  .
docker buildx build \
  --platform linux/amd64 \
  --target live-local \
  --build-arg "VCS_REF=$revision" \
  --load \
  --tag oddsfox-execution:live-local-ci \
  .
bash scripts/assert_paper_only_image.sh oddsfox-execution:paper-ci "$revision"
bash scripts/assert_live_local_image.sh oddsfox-execution:live-local-ci "$revision"

docker run --rm -d --name oddsfox-execution-ci \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --tmpfs /var/lib/oddsfox:rw,noexec,nosuid,size=64m,uid=10001,gid=10001 \
  -p 8787:8787 \
  -v "$PWD/config/container-paper.toml:/etc/oddsfox/oddsfox.toml:ro" \
  -v "$PWD/config/risk-policy.example.json:/etc/oddsfox/risk-policy.json:ro" \
  oddsfox-execution:paper-ci
cleanup() {
  docker logs oddsfox-execution-ci || true
  docker stop oddsfox-execution-ci >/dev/null || true
}
trap cleanup EXIT
for _ in $(seq 1 30); do
  if curl --fail --silent http://127.0.0.1:8787/health/ready; then
    exit 0
  fi
  sleep 1
done
exit 1
