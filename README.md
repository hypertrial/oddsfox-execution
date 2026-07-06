# oddsfox-live

Local real-time backend for OddsFox dashboards.

## Part Of OddsFox

`oddsfox-live` serves the JSON/SSE API between hosted graph artifacts and
`oddsfox-dash`. It reads `graph_snapshot.json` and `knockout_artifacts.json`
from `oddsfox-graph` builds, merges live token state, and exposes the stable API
documented in [docs/api.md](docs/api.md).

For the full source-to-dashboard flow, see the
[OddsFox System Overview](https://github.com/hypertrial/oddsfox-pipeline/blob/main/docs/system-overview.md)
and
[Operator Runbook](https://github.com/hypertrial/oddsfox-pipeline/blob/main/docs/operator-runbook.md).

## Run

```bash
go run . -assets "<polymarket_asset_id_1>,<polymarket_asset_id_2>"
go run . -knockout-artifact "$ODDSFOX_DATA_DIR/artifacts/current/knockout_artifacts.json"
go run . -artifact-dir "$ODDSFOX_DATA_DIR/artifacts" -replay-dir "$ODDSFOX_DATA_DIR/replay"
```

The server listens on `http://127.0.0.1:8787` by default.

Artifact-dir mode reads:

```text
/artifacts/releases/<UTC_BUILD_ID>/...
/artifacts/current/knockout_artifacts.json
/artifacts/current/graph_snapshot.json
```

`/artifacts/current` should be an atomic symlink update performed by the
artifact builder. The server polls it with `-artifact-reload-interval` (default
`60s`). Missing graph JSON returns an empty graph with a warning instead of
crashing.

Relevant flags/env:

- `-artifact-dir` / `ODDSFOX_ARTIFACT_DIR`
- `-graph-artifact` / `ODDSFOX_GRAPH_ARTIFACT`
- `-knockout-artifact` / `ODDSFOX_KNOCKOUT_ARTIFACT`
- `-artifact-reload-interval` / `ODDSFOX_ARTIFACT_RELOAD_INTERVAL`

## API

See [docs/api.md](docs/api.md) for response fields, query parameters, SSE
events, and artifact reload behavior.

- `GET /api/v0/health`
- `GET /api/v0/subscriptions`
- `POST /api/v0/subscriptions` with `{ "asset_ids": ["..."] }`
- `GET /api/v0/graph/snapshot`
- `GET /api/v0/knockout/snapshot`
- `GET /api/v0/knockout/timeseries?stage=winner&metric=stage_probability`
- `GET /api/v0/stream`
- `GET /api/v0/replay/events?limit=100`

`oddsfox-live` subscribes to Polymarket's public market WebSocket, keeps local
token state, exposes JSON/SSE for `oddsfox-dash`, and writes replayable JSONL
events under `replay/`. When graph artifacts are loaded,
`/api/v0/graph/snapshot` returns hosted graph nodes, logic edges, conditionals,
violations, metadata, and live token state in one payload.

## Docker

From the OddsFox workspace root:

```bash
cd oddsfox-pipeline
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml build live
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml up live
```

The compose example bind-mounts `$ODDSFOX_DATA_DIR/artifacts` at `/artifacts`
and `$ODDSFOX_DATA_DIR/replay` at `/replay`.
