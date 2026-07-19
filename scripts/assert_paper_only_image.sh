#!/usr/bin/env bash
set -euo pipefail

image="${1:?usage: scripts/assert_paper_only_image.sh IMAGE [EXPECTED_REVISION]}"
expected_revision="${2:-}"

platform="$(
  docker inspect \
    --format '{{ .Os }}/{{ .Architecture }}' \
    "$image"
)"
if [[ "$platform" != "linux/amd64" ]]; then
  echo "paper image platform is $platform, expected linux/amd64" >&2
  exit 1
fi

runtime_user="$(docker inspect --format '{{ .Config.User }}' "$image")"
if [[ "$runtime_user" != "10001:10001" ]]; then
  echo "paper image user is $runtime_user, expected 10001:10001" >&2
  exit 1
fi

mode_label="$(
  docker inspect \
    --format '{{ index .Config.Labels "io.oddsfox.execution-mode" }}' \
    "$image"
)"
if [[ "$mode_label" != "paper-only" ]]; then
  echo "image is missing the paper-only execution-mode label" >&2
  exit 1
fi

if [[ -n "$expected_revision" ]]; then
  revision="$(
    docker inspect \
      --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' \
      "$image"
  )"
  if [[ "$revision" != "$expected_revision" ]]; then
    echo "paper image revision is $revision, expected $expected_revision" >&2
    exit 1
  fi
fi

expected_capabilities='{"schema_version":"oddsfox.capabilities.v1","modes":["paper"],"signer":null}'
capabilities="$(docker run --rm "$image" capabilities)"
if [[ "$capabilities" != "$expected_capabilities" ]]; then
  echo "paper image reported unexpected capabilities: $capabilities" >&2
  exit 1
fi

for license_file in LICENSE THIRD_PARTY_NOTICES.md; do
  if ! docker run --rm \
    --entrypoint /usr/bin/test \
    "$image" \
    -r "/usr/share/licenses/oddsfox-execution/$license_file"; then
    echo "image is missing readable $license_file licensing material" >&2
    exit 1
  fi
done

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
