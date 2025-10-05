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
