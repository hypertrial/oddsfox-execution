# oddsfox-live

Local real-time backend for OddsFox dashboards.

## Run

```bash
go run . -assets "<polymarket_asset_id_1>,<polymarket_asset_id_2>"
go run . -knockout-artifact ../oddsfox-graph/output/wc2026/knockout_artifacts.json
```

The server listens on `http://127.0.0.1:8787` by default.

## API

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
events under `replay/`.
