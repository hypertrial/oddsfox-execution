# oddsfox-live API

`oddsfox-live` exposes a local JSON/SSE API for `oddsfox-dash` and other
operators. The default base URL is `http://127.0.0.1:8787`.

All JSON timestamps are UTC. All endpoints allow CORS. Unless noted, successful
responses use `200 OK` and `Content-Type: application/json`.

## Artifact Inputs

Artifact-dir mode reads the current release from:

```text
/artifacts/current/graph_snapshot.json
/artifacts/current/knockout_artifacts.json
```

Start with `-artifact-dir /artifacts` or `ODDSFOX_ARTIFACT_DIR=/artifacts`.
`oddsfox-live` polls the directory with `-artifact-reload-interval`, default
`60s`. Missing `graph_snapshot.json` returns an empty graph with a warning.
Missing or unconfigured `knockout_artifacts.json` makes knockout endpoints
return `404 Not Found`.

## Endpoints

### GET `/api/v0/health`

Returns server and WebSocket connection health.

Response fields: `status`, `version`, `connected`, `last_error`,
`subscriptions`, and `assets`.

Example:

```json
{
  "status": "ok",
  "version": "v0.1.0",
  "connected": true,
  "last_error": "",
  "subscriptions": 128,
  "assets": 128
}
```

### GET `/api/v0/subscriptions`

Returns the active Polymarket asset subscriptions.

Response fields: `asset_ids`.

Example:

```json
{
  "asset_ids": ["123", "456"]
}
```

### POST `/api/v0/subscriptions`

Adds Polymarket asset subscriptions.

Request body:

```json
{
  "asset_ids": ["123", "456"]
}
```

Response fields: `asset_ids` for the full active set and `added` for new IDs.

Errors: invalid JSON returns `400 Bad Request`; methods other than `GET`,
`POST`, and `OPTIONS` return `405 Method Not Allowed`.

### GET `/api/v0/graph/snapshot`

Returns the live graph snapshot. When a graph artifact is loaded, artifact
nodes, edges, conditionals, violations, metadata, and live token state are
merged into one payload.

Top-level fields: `version`, `updated_at`, `assets`, `metadata`, `nodes`,
`edges`, `conditionals`, `violations`, and `warnings`.

Example:

```json
{
  "version": "v0.1.0",
  "updated_at": "2026-07-06T12:00:00Z",
  "assets": [],
  "metadata": {
    "version": "v1",
    "built_at": "2026-07-06T11:58:00Z",
    "source_manifest": "build_manifest.json",
    "counts": {"nodes": 96, "logic_edges": 144}
  },
  "nodes": [],
  "edges": [],
  "conditionals": [],
  "violations": [],
  "warnings": []
}
```

### GET `/api/v0/knockout/snapshot`

Returns the current WC2026 knockout probability snapshot.

Top-level fields: `version`, `updated_at`, `competition`, `stages`, `slots`,
`teams`, `team_probabilities`, `match_results`, `sources`, and `warnings`.

Errors: returns `404 Not Found` when no knockout artifact is configured.

### GET `/api/v0/knockout/timeseries`

Returns hourly probability history for a knockout stage.

Query parameters:

| Parameter | Default | Notes |
| --- | --- | --- |
| `stage` | `winner` | Target stage key. |
| `metric` | `stage_probability` | Use `stage_probability` or `conditional_probability`. |
| `team_id` | unset | Restrict to one team. |
| `limit_teams` | service default | Limit the number of teams returned. |

Top-level fields: `version`, `updated_at`, `competition`, `stage_key`,
`metric`, `from_stage`, `hours`, `series`, `result_markers`, `sources`, and
`warnings`.

Errors: returns `404 Not Found` when no knockout artifact is configured.

### GET `/api/v0/replay/events`

Returns recent live events from the replay log, falling back to in-memory
events.

Query parameters:

| Parameter | Default | Notes |
| --- | --- | --- |
| `limit` | `100` | Values above `1000` are capped to `1000`. |

Response fields: `events`.

Each event has `id`, `received_at`, `type`, optional `asset_id`, optional
`market`, and `payload`.

### GET `/api/v0/stream`

Opens a Server-Sent Events stream with `Content-Type: text/event-stream`.

SSE events:

| Event | Meaning |
| --- | --- |
| `snapshot` | Sent immediately after connect with the current graph snapshot. |
| `event` | Sent for each live market event. |
| `knockout_snapshot` | Sent after sports or token updates when knockout artifacts are configured. |
| `: ping` | Comment heartbeat sent every 15 seconds. |

Errors: if the HTTP writer does not support streaming, returns
`500 Internal Server Error`.
