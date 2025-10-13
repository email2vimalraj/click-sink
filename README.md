# Click Sink

A lightweight Kafka → ClickHouse sink written in Go with a small web UI. It consumes messages from Kafka and writes them to ClickHouse using a schema-driven field mapping. The UI lets you configure multiple pipelines, validate connectivity, infer a mapping from samples, and start/stop pipelines. A background worker can run pipelines across multiple nodes using Postgres for coordination.

## Quick start

1. Launch infra with Docker (Kafka, ClickHouse, Postgres):

```bash
docker compose up -d
```

2. Start the UI using Postgres store (recommended):

```bash
go run ./cmd/click-sink ui --store=pg --pg-dsn="postgres://sink:sink@localhost:5432/click_sink?sslmode=disable" --listen=:8081
```

3. Open the UI at http://localhost:8081 and create a pipeline. Fill Kafka and ClickHouse configs, use “Validate” to check connectivity, click “Sample” to infer a mapping, then save the mapping.

4. Start a worker to run pipelines according to desired state:

```bash
go run ./cmd/click-sink worker --store=pg --pg-dsn="postgres://sink:sink@localhost:5432/click_sink?sslmode=disable" --interval=5s --lease-ttl=20s
```

In the UI Run tab, set desired state to "started" and adjust replicas. Workers will acquire per-replica slots and run consumers.

## Features

- Multi-pipeline management (CRUD)
- Split configuration for Kafka and ClickHouse with validation endpoints
- Mapping editor with sample-based inference and type coercions
- ClickHouse table inspection/creation helpers
- Desired state API with replicas
- Distributed worker with Postgres-backed slot leases for replicas
- Assignments endpoint to inspect current slot → worker leases

## REST API

The OpenAPI 3.1 spec lives at `docs/openapi.yaml`. Highlights:

- `GET /api/pipelines` list, `POST /api/pipelines` create
- `GET/PUT/DELETE /api/pipelines/{id}` manage a pipeline
- `GET/PUT /api/pipelines/{id}/{kafka-config|clickhouse-config}` configs
- `GET/PUT /api/pipelines/{id}/mapping` mapping
- `GET /api/pipelines/{id}/sample` infer mapping from Kafka samples
- `GET /api/pipelines/{id}/status` runtime stats (UI-managed runs)
- `GET|PUT|PATCH /api/pipelines/{id}/state` desired state + replicas
- `GET /api/pipelines/{id}/assignments` current active slot leases

## Notes on scaling

- Horizontal scaling: set `replicas` in desired state; each replica corresponds to one lease slot persisted in Postgres.
- Workers attempt to acquire free slots and renew them periodically. If a worker dies, leases expire after `lease-ttl`, allowing others to take over.
- Vertical scaling (multiple members per process) is supported by the worker acquiring more than one slot per pipeline when available. Fine-grained limits per worker instance can be added later.

## Development

- Build:

```bash
go build ./...
```

- Lint (vet):

```bash
go vet ./...
```

- Run UI with filesystem store (local/dev only):

```bash
go run ./cmd/click-sink ui --store=fs --listen=:8081
```

The filesystem store is best-effort and not suitable for multiple nodes. Prefer Postgres.
