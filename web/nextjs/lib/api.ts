const BASE = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8081";

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`${BASE}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
    cache: "no-store",
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export type Status = {
  running: boolean;
  started: string;
  lastErr: string;
  totalRows: number;
  lastBatch: number;
  lastBatchAt: string;
};

export type Config = any;
export type Mapping = {
  columns: { fieldPath: string; column: string; type: string }[];
};

export const api = {
  status: () => json<Status>("/api/status"),
  getConfig: () => json<Config>("/api/config"),
  saveConfig: (cfg: Config) =>
    json("/api/config", { method: "POST", body: JSON.stringify(cfg) }),
  getMapping: () => json<Mapping>("/api/mapping"),
  saveMapping: (m: Mapping) =>
    json("/api/mapping", { method: "POST", body: JSON.stringify(m) }),
  sample: (limit = 100) =>
    json<{ fieldPath: string; column: string; type: string }[]>(
      `/api/sample?limit=${limit}`
    ),
  start: () => json("/api/start", { method: "POST" }),
  stop: () => json("/api/stop", { method: "POST" }),
  // Multi-pipeline APIs
  listPipelines: () => json<any[]>("/api/pipelines"),
  createPipeline: (name: string, description?: string) =>
    json<{ id: string; name: string; description?: string }>("/api/pipelines", {
      method: "POST",
      body: JSON.stringify({ name, description }),
    }),
  getPipeline: (id: string) => json<any>(`/api/pipelines/${id}`),
  updatePipeline: (id: string, body: { name?: string; description?: string }) =>
    json<any>(`/api/pipelines/${id}`, {
      method: "PUT",
      body: JSON.stringify(body),
    }),
  deletePipeline: (id: string) =>
    json<any>(`/api/pipelines/${id}`, { method: "DELETE" }),
  getPipelineConfig: (id: string) =>
    json<Config>(`/api/pipelines/${id}/config`),
  savePipelineConfig: (id: string, cfg: Config) =>
    json(`/api/pipelines/${id}/config`, {
      method: "PUT",
      body: JSON.stringify(cfg),
    }),
  // Split config endpoints
  getKafkaConfig: (id: string) =>
    json<any>(`/api/pipelines/${id}/kafka-config`),
  saveKafkaConfig: (id: string, cfg: any) =>
    json(`/api/pipelines/${id}/kafka-config`, {
      method: "PUT",
      body: JSON.stringify(cfg),
    }),
  getClickHouseConfig: (id: string) =>
    json<any>(`/api/pipelines/${id}/clickhouse-config`),
  saveClickHouseConfig: (id: string, cfg: any) =>
    json(`/api/pipelines/${id}/clickhouse-config`, {
      method: "PUT",
      body: JSON.stringify(cfg),
    }),
  // Validation endpoints
  validateKafka: (id: string) =>
    json(`/api/pipelines/${id}/validate/kafka`, { method: "POST" }),
  validateKafkaSample: (kafkaCfg: any, limit = 100) =>
    json<{
      fields: { fieldPath: string; column: string; type: string }[];
      mappingYAML?: string;
    }>(`/api/validate/kafka/sample`, {
      method: "POST",
      body: JSON.stringify({ kafka: kafkaCfg, limit }),
    }),
  validateClickHouse: (id: string) =>
    json(`/api/pipelines/${id}/validate/clickhouse`, { method: "POST" }),
  validateClickHouseTable: (
    id: string,
    req: {
      table: string;
      columns?: { name: string; type: string }[];
      create?: boolean;
    }
  ) =>
    json(`/api/pipelines/${id}/validate/clickhouse-table`, {
      method: "POST",
      body: JSON.stringify(req),
    }),
  // ClickHouse browse APIs for mapping stage
  listDatabases: (id: string) =>
    json<{ databases: string[] }>(`/api/pipelines/${id}/clickhouse/databases`),
  listTables: (id: string, db?: string) =>
    json<{ tables: string[] }>(
      `/api/pipelines/${id}/clickhouse/tables${
        db ? `?db=${encodeURIComponent(db)}` : ""
      }`
    ),
  getTableSchema: (id: string, db: string, table: string) =>
    json<{ columns: { name: string; type: string }[] }>(
      `/api/pipelines/${id}/clickhouse/schema?db=${encodeURIComponent(
        db
      )}&table=${encodeURIComponent(table)}`
    ),
  getPipelineMapping: (id: string) =>
    json<Mapping>(`/api/pipelines/${id}/mapping`),
  savePipelineMapping: (id: string, m: Mapping) =>
    json(`/api/pipelines/${id}/mapping`, {
      method: "PUT",
      body: JSON.stringify(m),
    }),
  pipelineStatus: (id: string) => json<Status>(`/api/pipelines/${id}/status`),
  pipelineSample: (id: string, limit = 100) =>
    json<{ fieldPath: string; column: string; type: string }[]>(
      `/api/pipelines/${id}/sample?limit=${limit}`
    ),
  pipelineStart: (id: string) =>
    json(`/api/pipelines/${id}/start`, { method: "POST" }),
  pipelineStop: (id: string) =>
    json(`/api/pipelines/${id}/stop`, { method: "POST" }),
};
