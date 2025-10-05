# click-sink

A lightweight Kafka → ClickHouse sink written in Go. No Kubernetes or NATS required. Runs as a simple CLI.

Features:

- Create a pipeline via YAML config
- Configure Kafka brokers/topics/groups
- Auto-detect schema from Kafka messages (JSON) with recommended ClickHouse types
- User-defined field→column mapping and override destination data types
- Rate-limited batch inserts into ClickHouse

## Quick start

1. Install Go 1.21+

2. Create a config file (see `examples/config.yaml`).

3. Detect a schema from a Kafka topic and generate a draft mapping:

```bash
click-sink detect --config examples/config.yaml --sample 500 > mapping.yaml
```

4. Review and edit `mapping.yaml` if needed, then run the pipeline:

```bash
click-sink run --config examples/config.yaml --mapping mapping.yaml
```

## Local dev with Docker Compose

Spin up local Kafka + ClickHouse:

```bash
docker compose up -d
```

Create the `events` topic and seed a few JSON messages:

```bash
# Topic is auto-created by init-kafka service, but you can double check:
docker compose exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Seed from host (requires kafka console tools + jq installed), or run your own producer
bash scripts/seed-kafka.sh
```

Detect schema and run the sink:

```bash
./click-sink detect --config examples/config.yaml --sample 50 > mapping.yaml
./click-sink run --config examples/config.yaml --mapping mapping.yaml
```

Inspect inserted data in ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM default.events LIMIT 10"
```

Kafka UI (Redpanda Console):

- Open http://localhost:8080 to browse topics and messages.
- Default broker in UI is kafka:29092 (container network broker).

Troubleshooting detect producing empty mapping:

- Open the UI and confirm messages exist in the `events` topic.
- Re-seed: `bash scripts/seed-kafka-in-docker.sh`
- Increase timeout/samples: `./click-sink detect --config examples/config.yaml --sample 200 --timeout 30s > mapping.yaml`

## Configuration

See `examples/config.yaml` for a complete example. Key sections:

- kafka: brokers, topic, groupId, security
- clickhouse: dsn, database, table, batch size, insert rate limit
- mapping: path to YAML mapping defining field→column and types

## Build

```bash
go build -o click-sink ./cmd/click-sink
```

## Notes

- Currently supports JSON messages (one object per message). Avro/Protobuf could be added later.
- If a message fails to parse or map, it’s logged and skipped (best-effort mode). Future work could add DLQ.
