#!/usr/bin/env bash
set -euo pipefail

image="${1:?usage: scripts/assert_paper_only_image.sh IMAGE}"

mode_label="$(
  docker inspect \
    --format '{{ index .Config.Labels "io.oddsfox.execution-mode" }}' \
    "$image"
)"
if [[ "$mode_label" != "paper-only" ]]; then
  echo "image is missing the paper-only execution-mode label" >&2
  exit 1
fi

live_enable="Y"
live_enable="${live_enable}ES"
set +e
output="$(
  docker run --rm \
    --read-only \
    --cap-drop ALL \
    --security-opt no-new-privileges:true \
    --tmpfs /var/lib/oddsfox:rw,noexec,nosuid,size=64m,uid=10001,gid=10001 \
    --env ODDSFOX_ENABLE_LIVE_TRADING="$live_enable" \
    --volume "$PWD/config/container-paper.toml:/etc/oddsfox/oddsfox.toml:ro" \
    --volume "$PWD/config/risk-policy.example.json:/etc/oddsfox/risk-policy.json:ro" \
    "$image" \
    serve \
    --config /etc/oddsfox/oddsfox.toml \
    --risk-policy /etc/oddsfox/risk-policy.json \
    --mode live 2>&1
)"
status=$?
set -e

if (( status == 0 )); then
  echo "paper-only image unexpectedly accepted live mode" >&2
  exit 1
fi
if [[ "$output" != *"live mode requires a binary built with the live feature"* ]]; then
  echo "paper-only image failed for an unexpected reason:" >&2
  echo "$output" >&2
  exit 1
fi

echo "verified: image cannot enter live mode"
