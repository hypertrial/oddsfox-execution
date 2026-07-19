#!/usr/bin/env bash
set -euo pipefail

image="${1:?usage: scripts/assert_live_local_image.sh IMAGE [EXPECTED_REVISION]}"
expected_revision="${2:-}"

platform="$(
  docker inspect \
    --format '{{ .Os }}/{{ .Architecture }}' \
    "$image"
)"
if [[ "$platform" != "linux/amd64" ]]; then
  echo "live-local image platform is $platform, expected linux/amd64" >&2
  exit 1
fi

runtime_user="$(docker inspect --format '{{ .Config.User }}' "$image")"
if [[ "$runtime_user" != "10001:10001" ]]; then
  echo "live-local image user is $runtime_user, expected 10001:10001" >&2
  exit 1
fi

mode_label="$(
  docker inspect \
    --format '{{ index .Config.Labels "io.oddsfox.execution-mode" }}' \
    "$image"
)"
if [[ "$mode_label" != "live-local" ]]; then
  echo "image is missing the live-local execution-mode label" >&2
  exit 1
fi

if [[ -n "$expected_revision" ]]; then
  revision="$(
    docker inspect \
      --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' \
      "$image"
  )"
  if [[ "$revision" != "$expected_revision" ]]; then
    echo "live-local image revision is $revision, expected $expected_revision" >&2
    exit 1
  fi
fi

expected_capabilities='{"schema_version":"oddsfox.capabilities.v1","modes":["paper","live"],"signer":"local_file"}'
capabilities="$(docker run --rm "$image" capabilities)"
if [[ "$capabilities" != "$expected_capabilities" ]]; then
  echo "live-local image reported unexpected capabilities: $capabilities" >&2
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

echo "verified: image supports only the local-file live signer"
