# click-sink API

Base URL: http://localhost:8081

- Content-Type: application/json
- CORS: enabled (no auth; local dev)

## Pipelines

- GET `/api/pipelines`

  - List pipelines: [{ id, name, description, running, lastErr, started, totalRows, createdAt, updatedAt }]

- POST `/api/pipelines`

  - Create pipeline
  - Body: { name: string, description?: string }
  - Returns: { id, name, description, createdAt, updatedAt }

- GET `/api/pipelines/{id}`

  - Get pipeline metadata/status

- PUT `/api/pipelines/{id}`

  - Update name/description
  - Body: { name?: string, description?: string }

- DELETE `/api/pipelines/{id}`
  - Delete pipeline and its stored files

## Pipeline Configuration

- GET `/api/pipelines/{id}/kafka-config`

  - Returns KafkaConfig

- PUT `/api/pipelines/{id}/kafka-config`

  - Save KafkaConfig

- GET `/api/pipelines/{id}/clickhouse-config`

  - Returns ClickHouseConfig

- PUT `/api/pipelines/{id}/clickhouse-config`
  - Save ClickHouseConfig

Schema (typical):

- KafkaConfig: { brokers: string[], topic: string, groupID: string, securityProtocol?, saslUsername?, saslPassword?, saslMechanism? }
- ClickHouseConfig: { dsn: string, database?, table?, batchSize?, batchFlushInterval?, insertRatePerSec? }

## Validation

- POST `/api/pipelines/{id}/validate/kafka`

  - Validate Kafka connectivity using saved config

- POST `/api/pipelines/{id}/validate/clickhouse`

  - Validate ClickHouse connectivity using saved config

- POST `/api/pipelines/{id}/validate/clickhouse-table`
  - Body: { table: string, columns?: [{ name, type }], create?: boolean }
  - Checks if the table exists (and optionally creates it)

Global (inline) validation endpoints:

- POST `/api/validate/kafka`

  - Body: KafkaConfig

- POST `/api/validate/kafka/sample`

  - Body: { kafka: KafkaConfig, limit?: number }
  - Returns { mappingYAML, fields: [{ fieldPath, column, type }] }

- POST `/api/validate/clickhouse`

  - Body: ClickHouseConfig

- POST `/api/validate/clickhouse/table`
  - Body: { clickhouse: ClickHouseConfig, table: string, columns?, create? }

## ClickHouse Browsing (per-pipeline)

- GET `/api/pipelines/{id}/clickhouse/databases`

  - Returns { databases: string[] }

- GET `/api/pipelines/{id}/clickhouse/tables?db={db}`

  - Returns { tables: string[] }

- GET `/api/pipelines/{id}/clickhouse/schema?db={db}&table={table}`
  - Returns { columns: [{ name, type }] }

## Mapping

- GET `/api/pipelines/{id}/mapping`

  - Returns Mapping (empty columns array when none)

- PUT `/api/pipelines/{id}/mapping`

  - Save Mapping

- GET `/api/pipelines/{id}/sample?limit={n}`
  - Infers fields from sampled Kafka messages (using saved cfg)

Mapping shape:

- Mapping: { columns: [{ fieldPath: string, column: string, type: string }] }

## Running Pipelines

- GET `/api/pipelines/{id}/status`

  - Returns pipeline status: { running, started, lastErr, totalRows, lastBatch, lastBatchAt }

- POST `/api/pipelines/{id}/start`

  - Starts the pipeline (requires KafkaConfig, ClickHouseConfig, Mapping)

- POST `/api/pipelines/{id}/stop`
  - Stops the running pipeline

---

For a machine-readable description, see `docs/openapi.yaml` (OpenAPI 3.1). You can load it into Swagger UI, Redoc, or Insomnia/Postman.
